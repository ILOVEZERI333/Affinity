package worker

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"
)

func TestHashRingBasics(t *testing.T) {
	r := NewHashRing(64)
	if _, ok := r.Get("x"); ok {
		t.Fatal("empty ring should own nothing")
	}
	r.AddNode("a")
	r.AddNode("a") // idempotent
	r.AddNode("b")
	r.AddNode("c")
	if got := r.Members(); fmt.Sprint(got) != "[a b c]" {
		t.Fatalf("members = %v", got)
	}

	// Deterministic: same key, same owner.
	o1, _ := r.Get("solo-leveling")
	o2, _ := r.Get("solo-leveling")
	if o1 != o2 {
		t.Fatal("Get is not deterministic")
	}

	// Two rings built in different insertion orders agree (every node must
	// compute the same placement).
	r2 := NewHashRing(64)
	r2.AddNode("c")
	r2.AddNode("a")
	r2.AddNode("b")
	for i := 0; i < 200; i++ {
		k := fmt.Sprintf("target-%d", i)
		a, _ := r.Get(k)
		b, _ := r2.Get(k)
		if a != b {
			t.Fatalf("rings disagree on %s: %s vs %s", k, a, b)
		}
	}
}

func TestHashRingMinimalMovementOnRemove(t *testing.T) {
	r := NewHashRing(64)
	for _, n := range []string{"a", "b", "c", "d"} {
		r.AddNode(n)
	}
	const keys = 1000
	before := map[string]string{}
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("t-%d", i)
		before[k], _ = r.Get(k)
	}
	r.RemoveNode("d")
	moved := 0
	for k, old := range before {
		now, _ := r.Get(k)
		if old != "d" && now != old {
			t.Fatalf("key %s moved %s -> %s although its owner survived", k, old, now)
		}
		if old == "d" {
			moved++
			if now == "d" {
				t.Fatalf("key %s still owned by removed node", k)
			}
		}
	}
	// With 4 nodes roughly a quarter of keys belong to d; allow wide slack.
	if moved < keys/10 || moved > keys*2/5 {
		t.Fatalf("expected ~25%% of keys to move, got %d/%d", moved, keys)
	}
}

func TestAssignCoversEveryTargetOnce(t *testing.T) {
	r := NewHashRing(64)
	r.AddNode("a")
	r.AddNode("b")
	r.AddNode("idle") // may own nothing, must still appear
	targets := []string{"t1", "t2", "t3", "t4", "t5"}
	got := r.Assign(targets)
	var all []string
	for _, ts := range got {
		all = append(all, ts...)
	}
	sort.Strings(all)
	if fmt.Sprint(all) != fmt.Sprint(targets) {
		t.Fatalf("assignment %v does not partition %v", got, targets)
	}
	if len(got) != 3 {
		t.Fatalf("expected an entry for each of 3 members, got %v", got)
	}
}

func TestTaskHandoffFencesStaleEpoch(t *testing.T) {
	s := NewServer("", nil, nil)
	var sunk []string
	s.SetTargetSink(func(ts []string) { sunk = ts })

	if err := s.TaskHandoff(context.Background(), 5, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err := s.TaskHandoff(context.Background(), 4, []string{"x"}); err != ErrStaleEpoch {
		t.Fatalf("stale handoff: got %v, want ErrStaleEpoch", err)
	}
	if fmt.Sprint(s.Owned()) != "[a b]" || fmt.Sprint(sunk) != "[a b]" {
		t.Fatalf("stale handoff changed state: owned=%v sunk=%v", s.Owned(), sunk)
	}
	// Same epoch re-send is accepted (leader anti-entropy).
	if err := s.TaskHandoff(context.Background(), 5, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(s.Owned()) != "[a]" {
		t.Fatalf("owned = %v", s.Owned())
	}
}

// ownedOnce reports whether every target is owned by exactly one of nodes.
func ownedOnce(nodes []*testNode, targets []string) bool {
	count := map[string]int{}
	for _, n := range nodes {
		if n == nil {
			continue
		}
		for _, t := range n.server.Owned() {
			count[t]++
		}
	}
	for _, t := range targets {
		if count[t] != 1 {
			return false
		}
	}
	return len(count) == len(targets)
}

func TestPlacementRebalancesOnMembershipChange(t *testing.T) {
	ports := freePorts(t, 3)
	ids := []string{"w-0", "w-1", "w-2"}
	nodes := make([]*testNode, 3)
	for i := range nodes {
		nodes[i] = startNode(t, ids[i], ports[i], t.TempDir(), i == 0)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			if n != nil {
				n.stop()
			}
		}
	})
	peers := []string{nodes[0].addr, nodes[1].addr, nodes[2].addr}
	eventually(t, 10*time.Second, "w-0 leader", nodes[0].raft.IsLeader)
	joinRetry(t, peers, ids[1], nodes[1].addr)
	joinRetry(t, peers, ids[2], nodes[2].addr)

	targets := make([]string, 12)
	for i := range targets {
		targets[i] = fmt.Sprintf("manhwa-%d", i)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range nodes {
		p := NewPlacer(n.raft, n.ring, func() ([]string, error) { return targets, nil })
		p.interval = 100 * time.Millisecond
		go p.Run(ctx)
	}

	eventually(t, 15*time.Second, "every target owned by exactly one node", func() bool {
		return ownedOnce(nodes, targets)
	})

	// Kill a follower (so the leader that placed work survives and the test
	// stays about rebalancing, not election). Its targets must move.
	victim := -1
	for i, n := range nodes {
		if !n.raft.IsLeader() {
			victim = i
			break
		}
	}
	nodes[victim].stop()
	nodes[victim] = nil
	eventually(t, 20*time.Second, "victim evicted and its targets reassigned exactly once", func() bool {
		for _, n := range nodes {
			if n != nil && len(n.raft.Voters()) != 2 {
				return false
			}
		}
		return ownedOnce(nodes, targets)
	})
}
