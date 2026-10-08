package worker

import (
	"sort"
	"sync"
	"time"
)

// NodeHealth is a peer's liveness state as seen by this node's detector.
type NodeHealth int

const (
	// Alive: heartbeat seen within the suspect timeout.
	Alive NodeHealth = iota
	// Suspect: heartbeat is late. The node may be slow (a potential zombie)
	// rather than dead, so no ownership changes happen yet.
	Suspect
	// Dead: no heartbeat for the dead timeout. Safe to rebalance away from.
	Dead
)

func (h NodeHealth) String() string {
	switch h {
	case Alive:
		return "alive"
	case Suspect:
		return "suspect"
	default:
		return "dead"
	}
}

// FailureDetector tracks the last heartbeat from each peer and classifies
// them by elapsed time. It is a supplement to Raft, not a substitute: only
// the Raft leader should act on a Dead verdict, and the action (ring
// removal) must go through the replicated log.
//
// Timeouts are sized against the Phase 1 failover SLO (10s total): with
// DeadAfter around 5s, detection leaves headroom for rebalance + handoff.
type FailureDetector struct {
	SuspectAfter time.Duration
	DeadAfter    time.Duration

	now func() time.Time

	mu       sync.Mutex
	lastSeen map[string]time.Time
	state    map[string]NodeHealth
}

// NewFailureDetector builds a detector. now may be nil (uses time.Now) and
// exists so tests can drive the clock.
func NewFailureDetector(suspectAfter, deadAfter time.Duration, now func() time.Time) *FailureDetector {
	if now == nil {
		now = time.Now
	}
	return &FailureDetector{
		SuspectAfter: suspectAfter,
		DeadAfter:    deadAfter,
		now:          now,
		lastSeen:     make(map[string]time.Time),
		state:        make(map[string]NodeHealth),
	}
}

// Track starts monitoring nodeID, treating "now" as its first heartbeat so
// a freshly added peer isn't instantly declared dead.
func (d *FailureDetector) Track(nodeID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.lastSeen[nodeID]; !ok {
		d.lastSeen[nodeID] = d.now()
		d.state[nodeID] = Alive
	}
}

// Forget stops monitoring nodeID (e.g. after it was removed from the ring).
func (d *FailureDetector) Forget(nodeID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.lastSeen, nodeID)
	delete(d.state, nodeID)
}

// Heartbeat records a heartbeat from nodeID. A suspect node that heartbeats
// returns to Alive; a Dead node does too, but its ownership has already been
// revoked, so fencing (not the detector) keeps it from committing stale work.
func (d *FailureDetector) Heartbeat(nodeID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastSeen[nodeID] = d.now()
	d.state[nodeID] = Alive
}

// Transition describes a node changing health state.
type Transition struct {
	NodeID string
	From   NodeHealth
	To     NodeHealth
}

// Check re-evaluates every tracked node against the clock and returns the
// state changes since the previous Check, sorted by node ID.
func (d *FailureDetector) Check() []Transition {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	var out []Transition
	for id, seen := range d.lastSeen {
		elapsed := now.Sub(seen)
		next := Alive
		switch {
		case elapsed >= d.DeadAfter:
			next = Dead
		case elapsed >= d.SuspectAfter:
			next = Suspect
		}
		if prev := d.state[id]; prev != next {
			d.state[id] = next
			out = append(out, Transition{NodeID: id, From: prev, To: next})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// Health returns the last computed state for nodeID.
func (d *FailureDetector) Health(nodeID string) (NodeHealth, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	h, ok := d.state[nodeID]
	return h, ok
}
