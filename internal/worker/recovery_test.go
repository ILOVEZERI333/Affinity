package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func targetsN(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("series-%d", i)
	}
	return out
}

func TestHashRingBasics(t *testing.T) {
	r := NewHashRing(64)
	if _, ok := r.Get("x"); ok {
		t.Fatal("empty ring should have no owner")
	}
	r.AddNode("w-0")
	r.AddNode("w-1")
	r.AddNode("w-1") // idempotent
	if got := r.Members(); len(got) != 2 || got[0] != "w-0" || got[1] != "w-1" {
		t.Fatalf("members = %v", got)
	}
	a, _ := r.Get("series-1")
	b, _ := r.Get("series-1")
	if a != b {
		t.Fatal("Get must be deterministic")
	}
	r.RemoveNode("w-0")
	r.RemoveNode("w-0") // idempotent
	if o, _ := r.Get("series-1"); o != "w-1" {
		t.Fatalf("owner after removal = %q", o)
	}
}

func TestFailureDetectorTransitions(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1000, 0)}
	d := NewFailureDetector(2*time.Second, 5*time.Second, clk.now)
	d.Track("w-1")

	if tr := d.Check(); len(tr) != 0 {
		t.Fatalf("fresh node should not transition: %v", tr)
	}
	clk.advance(3 * time.Second)
	tr := d.Check()
	if len(tr) != 1 || tr[0].To != Suspect {
		t.Fatalf("want Suspect, got %v", tr)
	}
	d.Heartbeat("w-1") // slow, not dead: recovers
	if h, _ := d.Health("w-1"); h != Alive {
		t.Fatalf("health after heartbeat = %v", h)
	}
	clk.advance(6 * time.Second)
	tr = d.Check()
	if len(tr) != 1 || tr[0].To != Dead || tr[0].From != Alive {
		t.Fatalf("want Alive->Dead, got %v", tr)
	}
	if tr := d.Check(); len(tr) != 0 {
		t.Fatalf("Dead should be reported once, got %v", tr)
	}
	d.Forget("w-1")
	if _, ok := d.Health("w-1"); ok {
		t.Fatal("forgotten node should be untracked")
	}
}

// Scenario: worker-1 dies. Only its targets move; everyone else keeps theirs.
func TestRecoverFromFailureMovesOnlyDeadNodesTargets(t *testing.T) {
	ring := NewHashRing(64)
	for _, n := range []string{"w-0", "w-1", "w-2"} {
		ring.AddNode(n)
	}
	targets := targetsN(200)
	before := Assignments(ring, targets)
	var owned int
	for _, o := range before {
		if o == "w-1" {
			owned++
		}
	}
	if owned == 0 {
		t.Fatal("test setup: w-1 owns nothing")
	}

	var fence EpochFence
	rec := RecoverFromFailure(ring, &fence, targets, "w-1")

	if rec.Epoch != 1 || fence.Current() != 1 {
		t.Fatalf("epoch = %d / %d, want 1", rec.Epoch, fence.Current())
	}
	if len(rec.Moves) != owned {
		t.Fatalf("moves = %d, want %d (only w-1's targets)", len(rec.Moves), owned)
	}
	for _, m := range rec.Moves {
		if m.From != "w-1" || m.To == "w-1" || m.To == "" {
			t.Fatalf("bad move %+v", m)
		}
	}
	// Invariant: every target has exactly one live owner.
	for tgt, o := range Assignments(ring, targets) {
		if o == "" || o == "w-1" {
			t.Fatalf("target %s owner = %q", tgt, o)
		}
	}
}

func TestRecoverLastNodeLeavesTargetsUnowned(t *testing.T) {
	ring := NewHashRing(8)
	ring.AddNode("w-0")
	var fence EpochFence
	rec := RecoverFromFailure(ring, &fence, targetsN(5), "w-0")
	if len(rec.Moves) != 5 {
		t.Fatalf("moves = %d", len(rec.Moves))
	}
	for _, m := range rec.Moves {
		if m.To != "" {
			t.Fatalf("expected unowned, got %+v", m)
		}
	}
}

// Scenario: zombie worker-1 wakes up after its targets were reassigned.
func TestFenceRejectsZombie(t *testing.T) {
	var f EpochFence
	f.Advance() // epoch 1: w-1 granted ownership
	zombieEpoch := f.Current()

	f.Advance() // w-1 declared dead; epoch 2
	if err := f.Check(zombieEpoch); err != ErrStaleEpoch {
		t.Fatalf("zombie write err = %v, want ErrStaleEpoch", err)
	}
	if err := f.Check(f.Current()); err != nil {
		t.Fatalf("current-epoch write rejected: %v", err)
	}
	f.Observe(1) // epochs never go backwards
	if f.Current() != 2 {
		t.Fatalf("epoch regressed to %d", f.Current())
	}
}

func TestServerHeartbeatFeedsDetector(t *testing.T) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	d := NewFailureDetector(time.Second, 2*time.Second, clk.now)
	d.Track("w-1")
	s := NewServer(":0", nil, NewHashRing(1), d)
	clk.advance(1500 * time.Millisecond)
	if err := s.Heartbeat(context.Background(), "w-1"); err != nil {
		t.Fatal(err)
	}
	clk.advance(1500 * time.Millisecond)
	if tr := d.Check(); len(tr) != 1 || tr[0].To != Suspect {
		t.Fatalf("heartbeat should have reset the timer; got %v", tr)
	}
}

func TestLivenessHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	LivenessHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
}
