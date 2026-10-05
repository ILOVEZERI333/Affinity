package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

const (
	joinPath = "/join"

	// peerAPIPortOffset is added to a node's Raft port to get its peer API
	// port: Raft's TCP transport owns the Raft port, so Join and handoff RPCs
	// are served on the next one.
	peerAPIPortOffset = 1
)

// NotLeaderError is returned when a request that only the Raft leader can
// service reaches a follower. LeaderAddr is the leader's Raft address, or ""
// if no leader is currently known.
type NotLeaderError struct {
	LeaderAddr string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderAddr == "" {
		return "not the raft leader (no leader known)"
	}
	return "not the raft leader (leader is " + e.LeaderAddr + ")"
}

type joinRequest struct {
	NodeID string `json:"nodeID"`
	Addr   string `json:"addr"`
}

type notLeaderResponse struct {
	Leader string `json:"leader"`
}

// Server exposes RPC endpoints other worker nodes use to talk to this node:
// cluster join requests (answered by the Raft leader, redirected by
// followers) and task handoff when the hash ring reassigns targets. Failure
// detection is Raft's own heartbeats; see RaftNode.monitorDeadVoters.
type Server struct {
	Addr string

	raft *RaftNode
	ring *HashRing
}

// NewServer constructs a peer server backed by the given Raft node and hash
// ring, both of which the RPC handlers below read/mutate.
func NewServer(addr string, raftNode *RaftNode, ring *HashRing) *Server {
	return &Server{Addr: addr, raft: raftNode, ring: ring}
}

// ListenAndServe binds Addr and blocks, serving peer RPCs until ctx is
// cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+joinPath, s.handleJoin)

	srv := &http.Server{Addr: s.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}

func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req joinRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil || req.NodeID == "" || req.Addr == "" {
		http.Error(w, "nodeID and addr are required", http.StatusBadRequest)
		return
	}

	err := s.Join(r.Context(), req.NodeID, req.Addr)
	var nl *NotLeaderError
	switch {
	case err == nil:
		w.WriteHeader(http.StatusOK)
	case errors.As(err, &nl):
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMisdirectedRequest)
		_ = json.NewEncoder(w).Encode(notLeaderResponse{Leader: nl.LeaderAddr})
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// Join handles a new node's request to join the cluster. Non-leaders respond
// with a NotLeaderError carrying the current leader's address.
func (s *Server) Join(ctx context.Context, nodeID, addr string) error {
	if s.raft == nil {
		return errors.New("raft node not initialized")
	}
	if !s.raft.IsLeader() {
		return &NotLeaderError{LeaderAddr: s.raft.LeaderAddr()}
	}
	err := s.raft.Join(nodeID, addr)
	if errors.Is(err, ErrNotLeader) {
		// Lost leadership between the check above and the AddVoter call.
		return &NotLeaderError{LeaderAddr: s.raft.LeaderAddr()}
	}
	return err
}

// TaskHandoff receives a set of target keys this node is now responsible
// for, typically sent by the leader after a ring rebalance.
func (s *Server) TaskHandoff(ctx context.Context, targets []string) error {
	// TODO: hand targets off to the local scraper.Engine's schedule.
	return nil
}

// PeerAPIAddr maps a node's Raft address to the address of its peer API.
func PeerAPIAddr(raftAddr string) (string, error) {
	host, port, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", err
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return "", fmt.Errorf("invalid port in %q: %w", raftAddr, err)
	}
	return net.JoinHostPort(host, strconv.Itoa(p+peerAPIPortOffset)), nil
}

// JoinCluster asks the cluster to add (nodeID, selfAddr) as a voter. It tries
// each of peerAddrs (Raft addresses, normally from PEER_ADDRS) and follows
// not-leader redirects, returning nil as soon as the leader accepts. Joining
// is idempotent, so it is safe to call repeatedly.
func JoinCluster(ctx context.Context, peerAddrs []string, nodeID, selfAddr string) error {
	body, err := json.Marshal(joinRequest{NodeID: nodeID, Addr: selfAddr})
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 5 * time.Second}

	queue := append([]string(nil), peerAddrs...)
	tried := map[string]bool{selfAddr: true}
	var lastErr error
	for len(queue) > 0 {
		addr := queue[0]
		queue = queue[1:]
		if tried[addr] {
			continue
		}
		tried[addr] = true

		leader, err := postJoin(ctx, client, addr, body)
		if err == nil {
			return nil
		}
		lastErr = err
		if leader != "" {
			queue = append(queue, leader)
		}
	}
	if lastErr == nil {
		return errors.New("no peers to join")
	}
	return lastErr
}

// postJoin sends one join request. On a not-leader response it returns the
// leader's Raft address (possibly "") along with the error.
func postJoin(ctx context.Context, client *http.Client, raftAddr string, body []byte) (leader string, err error) {
	apiAddr, err := PeerAPIAddr(raftAddr)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+apiAddr+joinPath, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return "", nil
	case http.StatusMisdirectedRequest:
		var nl notLeaderResponse
		_ = json.NewDecoder(resp.Body).Decode(&nl)
		return nl.Leader, &NotLeaderError{LeaderAddr: nl.Leader}
	default:
		return "", fmt.Errorf("join via %s: %s", raftAddr, resp.Status)
	}
}
