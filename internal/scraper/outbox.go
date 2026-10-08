package scraper

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
)

// ParseChapterNumber parses a chapter number, rejecting NaN and infinities:
// ParseFloat accepts "Inf" and "1e999", and a non-finite mark would silence a
// target forever (everything compares <= +Inf) and break JSON snapshots.
func ParseChapterNumber(s string) (float64, error) {
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, fmt.Errorf("chapter number %q is not finite", s)
	}
	return n, nil
}

// DeliveryState is where a chapter notification stands in the outbox.
type DeliveryState string

const (
	// StatePending means the chapter was recorded but its webhook has not been
	// acknowledged. After a timeout the real outcome is unknown, so a pending
	// chapter may or may not have reached the channel.
	StatePending DeliveryState = "pending"
	// StateDelivered means the webhook was acknowledged and the target's
	// high-water mark covers the chapter.
	StateDelivered DeliveryState = "delivered"
)

// Outbox is the durable record of what has been announced. The idempotency key
// is (target, chapter number).
//
//	Enqueue -> pending   (before the POST; safe to repeat)
//	POST webhook
//	Ack     -> delivered (after a 2xx; advances the high-water mark)
//
// Enqueueing a chapter that is already covered returns StateDelivered, which is
// how a worker that timed out, was deposed, or lost the target to a neighbour
// finds out the notification already went out, instead of assuming it failed.
type Outbox interface {
	// HighWater returns the highest delivered chapter number for target. It may
	// lag the authoritative state; Enqueue is the authoritative check.
	HighWater(target string) (float64, bool)
	// Baseline records the newest existing chapter of a never-seen target
	// without announcing anything. It does not move an existing mark.
	Baseline(ctx context.Context, target string, number float64) error
	Enqueue(ctx context.Context, ch Chapter) (DeliveryState, error)
	Ack(ctx context.Context, ch Chapter) error
}

// OutboxEntry is a chapter awaiting webhook acknowledgement.
type OutboxEntry struct {
	Target string `json:"target"`
	Number string `json:"number"`
	URL    string `json:"url,omitempty"`
	Title  string `json:"title,omitempty"`
}

// OutboxSnapshot is the serializable form of an OutboxState.
type OutboxSnapshot struct {
	HighWater map[string]float64 `json:"highWater,omitempty"`
	Pending   []OutboxEntry      `json:"pending,omitempty"`
}

// OutboxState is the outbox state machine. It is deterministic and does no I/O,
// so the Raft FSM applies committed commands to it on every node and a
// single-node LocalOutbox wraps it directly: one set of rules for both.
//
// Only pending entries are stored. A delivered chapter is represented by the
// high-water mark alone, which keeps the replicated state bounded: Enqueue
// treats any number at or below the mark as delivered.
type OutboxState struct {
	mu        sync.RWMutex
	highWater map[string]float64
	pending   map[string]OutboxEntry
}

// NewOutboxState returns an empty state.
func NewOutboxState() *OutboxState {
	return &OutboxState{
		highWater: make(map[string]float64),
		pending:   make(map[string]OutboxEntry),
	}
}

func pendingKey(target, number string) string { return target + "\x00" + number }

// HighWater returns the highest delivered chapter number for target.
func (s *OutboxState) HighWater(target string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.highWater[target]
	return n, ok
}

// Baseline sets the mark for a target that has none. A non-finite number is
// ignored.
func (s *OutboxState) Baseline(target string, number float64) {
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.highWater[target]; !ok {
		s.highWater[target] = number
	}
}

// Enqueue records ch as pending unless it is already covered by the mark.
func (s *OutboxState) Enqueue(ch Chapter) (DeliveryState, error) {
	n, err := ParseChapterNumber(ch.Number)
	if err != nil {
		return "", fmt.Errorf("outbox: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if hw, ok := s.highWater[ch.Target]; ok && n <= hw {
		return StateDelivered, nil
	}
	key := pendingKey(ch.Target, ch.Number)
	if _, ok := s.pending[key]; !ok {
		s.pending[key] = OutboxEntry{Target: ch.Target, Number: ch.Number, URL: ch.URL, Title: ch.Title}
	}
	return StatePending, nil
}

// Ack marks ch delivered: the mark advances and the pending entry is dropped.
// Acking a chapter that was never enqueued still advances the mark.
func (s *OutboxState) Ack(ch Chapter) error {
	n, err := ParseChapterNumber(ch.Number)
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	hw, ok := s.highWater[ch.Target]
	if !ok || n > hw {
		hw = n
		s.highWater[ch.Target] = n
	}
	delete(s.pending, pendingKey(ch.Target, ch.Number))
	// Anything at or below the mark is covered now; a lower entry left behind
	// (its chapter was skipped or renumbered) would sit in every snapshot forever.
	for k, e := range s.pending {
		if e.Target != ch.Target {
			continue
		}
		if en, err := ParseChapterNumber(e.Number); err != nil || en <= hw {
			delete(s.pending, k)
		}
	}
	return nil
}

// IsPending reports whether (target, number) is awaiting acknowledgement.
func (s *OutboxState) IsPending(target, number string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.pending[pendingKey(target, number)]
	return ok
}

// Pending returns the undelivered entries, ordered by target then insertion key.
func (s *OutboxState) Pending() []OutboxEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]OutboxEntry, 0, len(s.pending))
	for _, e := range s.pending {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Target != out[j].Target {
			return out[i].Target < out[j].Target
		}
		return out[i].Number < out[j].Number
	})
	return out
}

// Snapshot returns a copy of the state.
func (s *OutboxState) Snapshot() OutboxSnapshot {
	snap := OutboxSnapshot{Pending: s.Pending()}
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap.HighWater = make(map[string]float64, len(s.highWater))
	for t, n := range s.highWater {
		snap.HighWater[t] = n
	}
	return snap
}

// Restore replaces the state with snap.
func (s *OutboxState) Restore(snap OutboxSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.highWater = make(map[string]float64, len(snap.HighWater))
	for t, n := range snap.HighWater {
		s.highWater[t] = n
	}
	s.pending = make(map[string]OutboxEntry, len(snap.Pending))
	for _, e := range snap.Pending {
		s.pending[pendingKey(e.Target, e.Number)] = e
	}
}

// LocalOutbox is an in-process Outbox for a single node (and for tests). It
// gives the engine the same semantics as the Raft-backed one, minus replication.
type LocalOutbox struct{ *OutboxState }

// NewLocalOutbox returns an empty in-process outbox.
func NewLocalOutbox() *LocalOutbox { return &LocalOutbox{NewOutboxState()} }

var _ Outbox = (*LocalOutbox)(nil)

func (o *LocalOutbox) Baseline(_ context.Context, target string, number float64) error {
	o.OutboxState.Baseline(target, number)
	return nil
}

func (o *LocalOutbox) Enqueue(_ context.Context, ch Chapter) (DeliveryState, error) {
	return o.OutboxState.Enqueue(ch)
}

func (o *LocalOutbox) Ack(_ context.Context, ch Chapter) error { return o.OutboxState.Ack(ch) }
