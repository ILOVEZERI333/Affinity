package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	handoffPath = "/handoff"

	// DefaultPlaceInterval is how often the leader re-evaluates placement.
	DefaultPlaceInterval = time.Second

	// resyncInterval is how long the leader trusts that a worker still holds
	// what it was last sent. A worker that restarted comes back with no targets
	// but is still a voter, so the leader would never notice; periodically
	// re-sending the (idempotent) assignment heals that.
	resyncInterval = 10 * time.Second
)

// ErrStaleEpoch is returned by a handoff whose ring epoch is older than one the
// receiving node has already accepted. It is the fence that stops a deposed
// leader (or one working from an outdated membership view) from overwriting
// newer assignments.
var ErrStaleEpoch = errors.New("stale ring epoch")

// handoffRequest is the full set of targets a node owns at a given ring epoch.
// It is a complete replacement, not a delta: anything the node scrapes that is
// not listed is dropped, which is how an old owner "stops scraping keys it no
// longer owns".
type handoffRequest struct {
	Epoch   uint64   `json:"epoch"`
	Targets []string `json:"targets"`
}

// FileTargets returns a target source that reads one target ID per line from
// path (the operator's mounted ConfigMap). Blank lines and "#" comments are
// skipped. The file is re-read on every call, so edits to the ConfigMap flow
// through to placement without a restart.
func FileTargets(path string) func() ([]string, error) {
	return func() ([]string, error) {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var out []string
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			out = append(out, line)
		}
		return out, nil
	}
}

type pushedState struct {
	targets []string
	at      time.Time
}

// Placer runs on every node but only acts on the Raft leader. Each tick it
// builds the ring from the current voters, computes target -> owner, and pushes
// whatever differs from what it last told each worker.
//
// It is deliberately stateless across leadership changes: a new leader starts
// with an empty "pushed" map, so it re-sends every assignment once. That makes
// failover correct without replicating placement state through Raft (the ring
// is a pure function of voters + target list, which every node can derive).
type Placer struct {
	raft     *RaftNode
	ring     *HashRing
	targets  func() ([]string, error)
	push     func(ctx context.Context, raftAddr string, req handoffRequest) error
	interval time.Duration

	pushed map[string]pushedState // node ID -> last successfully pushed set
}

// NewPlacer wires a Placer to a Raft node and ring. targets supplies the
// current target list (see FileTargets).
func NewPlacer(raftNode *RaftNode, ring *HashRing, targets func() ([]string, error)) *Placer {
	client := &http.Client{Timeout: 3 * time.Second}
	return &Placer{
		raft:    raftNode,
		ring:    ring,
		targets: targets,
		push: func(ctx context.Context, addr string, req handoffRequest) error {
			return postHandoff(ctx, client, addr, req)
		},
		interval: DefaultPlaceInterval,
		pushed:   map[string]pushedState{},
	}
}

// Run reconciles placement on an interval until ctx is cancelled.
func (p *Placer) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.Reconcile(ctx)
		}
	}
}

// Reconcile performs one placement pass. Not safe for concurrent use; Run calls
// it from a single goroutine.
func (p *Placer) Reconcile(ctx context.Context) {
	if !p.raft.IsLeader() {
		// Only the leader writes placement. Forget what we pushed so that if we
		// become leader later we re-send everything instead of trusting stale
		// bookkeeping.
		p.pushed = map[string]pushedState{}
		return
	}

	members := p.raft.Members()
	p.syncRing(members)

	targets, err := p.targets()
	if err != nil {
		// Keep the current assignments rather than treating an unreadable file
		// as "no targets", which would make every worker stop scraping.
		log.Printf("placement: read targets: %v", err)
		return
	}

	desired := p.ring.Assign(targets)
	epoch := p.raft.Epoch()

	// Forget bookkeeping for nodes that left the ring.
	for id := range p.pushed {
		if _, ok := desired[id]; !ok {
			delete(p.pushed, id)
		}
	}

	var shrinking, growing []string
	for node, want := range desired {
		prev, ok := p.pushed[node]
		if ok && sameSet(prev.targets, want) && time.Since(prev.at) < resyncInterval {
			continue
		}
		// A node whose new set is a subset of its old one only gives work up.
		if ok && isSubset(want, prev.targets) {
			shrinking = append(shrinking, node)
		} else {
			growing = append(growing, node)
		}
	}
	sort.Strings(shrinking)
	sort.Strings(growing)

	// Phase A: tell losers to drop first. Phase B: only then tell gainers to
	// pick up. If a loser can't be reached we hold back the gainers for this
	// tick: briefly scraping nothing is safe, scraping twice is not. An
	// unreachable loser that is really dead gets evicted from the ring within
	// DeadVoterTimeout, after which it no longer blocks anything.
	allDropped := true
	for _, node := range shrinking {
		if !p.pushTo(ctx, node, members[node], epoch, desired[node]) {
			allDropped = false
		}
	}
	if !allDropped {
		return
	}
	for _, node := range growing {
		p.pushTo(ctx, node, members[node], epoch, desired[node])
	}
}

func (p *Placer) pushTo(ctx context.Context, node, raftAddr string, epoch uint64, targets []string) bool {
	err := p.push(ctx, raftAddr, handoffRequest{Epoch: epoch, Targets: targets})
	if err != nil {
		log.Printf("placement: handoff to %s (epoch %d): %v", node, epoch, err)
		return false
	}
	if prev, ok := p.pushed[node]; !ok || !sameSet(prev.targets, targets) {
		log.Printf("placement: %s now owns %d target(s) at epoch %d", node, len(targets), epoch)
	}
	p.pushed[node] = pushedState{targets: targets, at: time.Now()}
	return true
}

// syncRing makes the ring's members equal the Raft voters.
func (p *Placer) syncRing(members map[string]string) {
	for _, existing := range p.ring.Members() {
		if _, ok := members[existing]; !ok {
			p.ring.RemoveNode(existing)
		}
	}
	for id := range members {
		p.ring.AddNode(id)
	}
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	return isSubset(a, b)
}

// isSubset reports whether every element of a is in b.
func isSubset(a, b []string) bool {
	in := make(map[string]struct{}, len(b))
	for _, s := range b {
		in[s] = struct{}{}
	}
	for _, s := range a {
		if _, ok := in[s]; !ok {
			return false
		}
	}
	return true
}

// postHandoff sends one handoff to the node whose Raft address is raftAddr.
func postHandoff(ctx context.Context, client *http.Client, raftAddr string, req handoffRequest) error {
	apiAddr, err := PeerAPIAddr(raftAddr)
	if err != nil {
		return err
	}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+apiAddr+handoffPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return nil
	case http.StatusConflict:
		return ErrStaleEpoch
	default:
		return fmt.Errorf("handoff via %s: %s", raftAddr, resp.Status)
	}
}
