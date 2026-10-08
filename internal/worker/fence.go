package worker

import (
	"errors"
	"sync"
)

// ErrStaleEpoch is returned when a write carries a ring epoch that doesn't
// match the current one, i.e. it came from a zombie owner whose targets were
// reassigned.
var ErrStaleEpoch = errors.New("stale ring epoch: ownership has moved")

// EpochFence implements the fencing token from the Phase 1 invariants. The
// ring epoch increases on every membership change; any state-changing write
// (outbox commit, high-water advance) must present the epoch under which the
// writer was granted ownership, and is rejected if the epoch has advanced.
//
// In production the epoch lives in the Raft FSM so every node agrees on it;
// this type is the local check applied on the commit path.
type EpochFence struct {
	mu    sync.RWMutex
	epoch uint64
}

// Current returns the current ring epoch.
func (f *EpochFence) Current() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.epoch
}

// Advance bumps the epoch and returns the new value. Call it whenever ring
// membership changes.
func (f *EpochFence) Advance() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.epoch++
	return f.epoch
}

// Observe raises the epoch to e if e is newer (used when applying a
// replicated epoch). Epochs never move backwards.
func (f *EpochFence) Observe(e uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e > f.epoch {
		f.epoch = e
	}
}

// Check returns ErrStaleEpoch unless e is the current epoch. A writer whose
// epoch is *ahead* of ours is also rejected: this node is the stale one and
// must catch up before it can validate writes.
func (f *EpochFence) Check(e uint64) error {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if e != f.epoch {
		return ErrStaleEpoch
	}
	return nil
}
