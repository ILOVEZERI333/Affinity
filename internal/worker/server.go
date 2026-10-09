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
	"sync"
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

	handoffMu    sync.Mutex
	handoffEpoch uint64   // highest ring epoch a handoff has been accepted at
	owned        []string // targets this node currently owns
	sink         func(targets []string)
}

// SetTargetSink registers the callback that applies an accepted assignment,
// normally scraper.Engine.SetTargets.
func (s *Server) SetTargetSink(fn func(targets []string)) {
	s.handoffMu.Lock()
	defer s.handoffMu.Unlock()
	s.sink = fn
}

// Epoch returns the highest ring epoch this node has accepted a handoff at. The
// delivery outbox stamps its writes with it, which is how a node that has missed
// a rebalance is fenced out.
func (s *Server) Epoch() uint64 {
	s.handoffMu.Lock()
	defer s.handoffMu.Unlock()
	return s.handoffEpoch
}

// Owned returns a copy of the targets this node currently owns.
func (s *Server) Owned() []string {
	s.handoffMu.Lock()
	defer s.handoffMu.Unlock()
	return append([]string(nil), s.owned...)
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
	mux.HandleFunc("POST "+handoffPath, s.handleHandoff)
	mux.HandleFunc("POST "+outboxPath, s.handleOutbox)

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

func (s *Server) handleHandoff(w http.ResponseWriter, r *http.Request) {
	var req handoffRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "invalid handoff body", http.StatusBadRequest)
		return
	}
	switch err := s.TaskHandoff(r.Context(), req.Epoch, req.Targets); {
	case err == nil:
		w.WriteHeader(http.StatusOK)
	case errors.Is(err, ErrStaleEpoch):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// TaskHandoff replaces the set of targets this node owns, as sent by the
// leader after a ring change. The set is the complete assignment, so any target
// not in it stops being scraped here.
//
// Fencing: a handoff stamped with an epoch lower than one already accepted is
// rejected. Epochs only grow (every membership change bumps one), so a stale or
// deposed leader that still holds an old view of the ring cannot take work back.
// Equal epochs are accepted so the leader can safely re-send (idempotent).
func (s *Server) TaskHandoff(ctx context.Context, epoch uint64, targets []string) error {
	s.handoffMu.Lock()
	defer s.handoffMu.Unlock()
	if epoch < s.handoffEpoch {
		return ErrStaleEpoch
	}
	s.handoffEpoch = epoch
	s.owned = append([]string(nil), targets...)
	if s.sink != nil {
		s.sink(s.owned)
	}
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
