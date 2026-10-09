package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/affinity/distriscrape/internal/scraper"
)

const (
	outboxPath = "/outbox"

	// outboxHops bounds how many leaders a write chases before giving up; the
	// caller's own backoff handles the rest.
	outboxHops     = 3
	outboxHopDelay = 150 * time.Millisecond
)

// ErrNoLeader is returned when an outbox write cannot find a leader to commit
// it, e.g. mid-election. The engine treats it like any failed poll and retries.
var ErrNoLeader = errors.New("no raft leader to commit outbox write")

type outboxResponse struct {
	State scraper.DeliveryState `json:"state"`
}

// applyOutbox commits an outbox command through Raft. Leader only.
func (n *RaftNode) applyOutbox(cmd command) (scraper.DeliveryState, error) {
	data, err := json.Marshal(cmd)
	if err != nil {
		return "", err
	}
	resp, err := n.apply(data, raftTimeout)
	if err != nil {
		return "", err
	}
	state, _ := resp.(scraper.DeliveryState)
	return state, nil
}

// OutboxClient is the scraper.Outbox backed by the Raft-replicated outbox.
//
// Writes are commands in the Raft log: the leader applies them directly and
// followers forward them to the leader over the peer API. Once a write returns
// it is committed, so a successor leader sees it. HighWater reads this node's
// applied state and may lag the leader by a moment; Enqueue is the authoritative
// check (it is evaluated by the leader against the committed state).
type OutboxClient struct {
	node   *RaftNode
	epoch  func() uint64
	client *http.Client
}

var _ scraper.Outbox = (*OutboxClient)(nil)

// NewOutboxClient returns an Outbox over node. epoch reports the ring epoch of
// this worker's latest accepted handoff (Server.Epoch); it stamps every write
// so a worker that missed a rebalance is fenced out by the FSM.
func NewOutboxClient(node *RaftNode, epoch func() uint64) *OutboxClient {
	return &OutboxClient{node: node, epoch: epoch, client: &http.Client{Timeout: raftTimeout + 2*time.Second}}
}

func (c *OutboxClient) HighWater(target string) (float64, bool) {
	return c.node.fsm.HighWater(target)
}

func (c *OutboxClient) Baseline(ctx context.Context, target string, number float64) error {
	_, err := c.submit(ctx, command{
		Type:   cmdOutboxBaseline,
		Target: target,
		Number: strconv.FormatFloat(number, 'f', -1, 64),
	})
	return err
}

func (c *OutboxClient) Enqueue(ctx context.Context, ch scraper.Chapter) (scraper.DeliveryState, error) {
	return c.submit(ctx, chapterCommand(cmdOutboxEnqueue, ch))
}

func (c *OutboxClient) Ack(ctx context.Context, ch scraper.Chapter) error {
	_, err := c.submit(ctx, chapterCommand(cmdOutboxAck, ch))
	return err
}

func chapterCommand(typ string, ch scraper.Chapter) command {
	return command{Type: typ, Target: ch.Target, Number: ch.Number, URL: ch.URL, Title: ch.Title}
}

// submit commits cmd, locally if this node leads and otherwise through the
// leader.
func (c *OutboxClient) submit(ctx context.Context, cmd command) (scraper.DeliveryState, error) {
	cmd.Epoch = c.epoch()
	hint := ""
	for hop := range outboxHops {
		if hop > 0 {
			// Leadership hints go stale mid-election; give it a moment to settle.
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(time.Duration(hop) * outboxHopDelay):
			}
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if c.node.IsLeader() {
			state, err := c.node.applyOutbox(cmd)
			if errors.Is(err, ErrNotLeader) {
				continue
			}
			return state, err
		}
		addr := hint
		if addr == "" {
			addr = c.node.LeaderAddr()
		}
		if addr == "" {
			return "", ErrNoLeader
		}
		state, err := c.forward(ctx, addr, cmd)
		var nl *NotLeaderError
		if errors.As(err, &nl) {
			hint = nl.LeaderAddr
			continue
		}
		return state, err
	}
	return "", ErrNoLeader
}

// forward posts cmd to the leader's peer API.
func (c *OutboxClient) forward(ctx context.Context, raftAddr string, cmd command) (scraper.DeliveryState, error) {
	apiAddr, err := PeerAPIAddr(raftAddr)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(cmd)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+apiAddr+outboxPath, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var out outboxResponse
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return "", fmt.Errorf("decode outbox response: %w", err)
		}
		return out.State, nil
	case http.StatusMisdirectedRequest:
		var nl notLeaderResponse
		_ = json.NewDecoder(resp.Body).Decode(&nl)
		return "", &NotLeaderError{LeaderAddr: nl.Leader}
	case http.StatusConflict:
		return "", ErrStaleEpoch
	default:
		return "", fmt.Errorf("outbox via %s: %s", raftAddr, resp.Status)
	}
}

// handleOutbox is the leader side of a forwarded outbox write.
func (s *Server) handleOutbox(w http.ResponseWriter, r *http.Request) {
	var cmd command
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&cmd); err != nil || cmd.Target == "" {
		http.Error(w, "invalid outbox command", http.StatusBadRequest)
		return
	}
	switch cmd.Type {
	case cmdOutboxBaseline, cmdOutboxEnqueue, cmdOutboxAck:
	default:
		http.Error(w, "unknown outbox command type", http.StatusBadRequest)
		return
	}
	if s.raft == nil {
		http.Error(w, "raft node not initialized", http.StatusInternalServerError)
		return
	}

	notLeader := func() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMisdirectedRequest)
		_ = json.NewEncoder(w).Encode(notLeaderResponse{Leader: s.raft.LeaderAddr()})
	}
	if !s.raft.IsLeader() {
		notLeader()
		return
	}
	state, err := s.raft.applyOutbox(cmd)
	switch {
	case err == nil:
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(outboxResponse{State: state})
	case errors.Is(err, ErrNotLeader):
		notLeader()
	case errors.Is(err, ErrStaleEpoch):
		http.Error(w, err.Error(), http.StatusConflict)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
