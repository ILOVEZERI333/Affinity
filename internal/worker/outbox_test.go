package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/affinity/distriscrape/internal/scraper"
)

func fsmApply(t *testing.T, f *ringFSM, cmd command) any {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return f.Apply(&raft.Log{Data: data})
}

func TestFSMOutboxFencesStaleEpoch(t *testing.T) {
	f := &ringFSM{outbox: scraper.NewOutboxState()}
	fsmApply(t, f, command{Type: cmdBumpEpoch})
	fsmApply(t, f, command{Type: cmdBumpEpoch}) // replicated epoch is now 2

	enq := command{Type: cmdOutboxEnqueue, Target: "t", Number: "4", Epoch: 1}
	if got := fsmApply(t, f, enq); got != any(ErrStaleEpoch) {
		t.Fatalf("stale enqueue = %v, want ErrStaleEpoch", got)
	}
	if got := fsmApply(t, f, command{Type: cmdOutboxAck, Target: "t", Number: "4", Epoch: 1}); got != any(ErrStaleEpoch) {
		t.Fatalf("stale ack = %v, want ErrStaleEpoch", got)
	}
	if _, ok := f.HighWater("t"); ok || len(f.outbox.Pending()) != 0 {
		t.Fatal("a fenced write changed state")
	}

	// At the current epoch (or ahead of it) writes go through.
	enq.Epoch = 2
	if got := fsmApply(t, f, command{Type: cmdOutboxBaseline, Target: "t", Number: "3", Epoch: 2}); got != any(scraper.StateDelivered) {
		t.Fatalf("baseline = %v", got)
	}
	if got := fsmApply(t, f, enq); got != any(scraper.StatePending) {
		t.Fatalf("enqueue = %v", got)
	}
	if got := fsmApply(t, f, command{Type: cmdOutboxAck, Target: "t", Number: "4", Epoch: 3}); got != any(scraper.StateDelivered) {
		t.Fatalf("ack = %v", got)
	}
	if hw, _ := f.HighWater("t"); hw != 4 {
		t.Fatalf("mark = %v", hw)
	}
}

func TestFSMSnapshotCarriesOutbox(t *testing.T) {
	f := &ringFSM{outbox: scraper.NewOutboxState()}
	fsmApply(t, f, command{Type: cmdBumpEpoch})
	fsmApply(t, f, command{Type: cmdOutboxBaseline, Target: "t", Number: "7", Epoch: 1})
	fsmApply(t, f, command{Type: cmdOutboxEnqueue, Target: "t", Number: "8", URL: "u", Epoch: 1})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}

	g := &ringFSM{outbox: scraper.NewOutboxState()}
	if err := g.Restore(sink.reader()); err != nil {
		t.Fatal(err)
	}
	if g.Epoch() != 1 {
		t.Fatalf("epoch = %d", g.Epoch())
	}
	if hw, _ := g.HighWater("t"); hw != 7 {
		t.Fatalf("restored mark = %v", hw)
	}
	if p := g.outbox.Pending(); len(p) != 1 || p[0].Number != "8" || p[0].URL != "u" {
		t.Fatalf("restored pending = %+v", p)
	}
}

// A snapshot from before Phase 8 has no outbox section; it must still restore.
func TestFSMRestoresPhase5Snapshot(t *testing.T) {
	g := &ringFSM{outbox: scraper.NewOutboxState()}
	if err := g.Restore(readCloser(`{"epoch":5}`)); err != nil {
		t.Fatal(err)
	}
	if g.Epoch() != 5 {
		t.Fatalf("epoch = %d", g.Epoch())
	}
}

func TestOutboxReplicatesAndForwardsThroughLeader(t *testing.T) {
	ports := freePorts(t, 2)
	n0 := startNode(t, "w-0", ports[0], t.TempDir(), true)
	n1 := startNode(t, "w-1", ports[1], t.TempDir(), false)
	t.Cleanup(func() { n0.stop(); n1.stop() })
	peers := []string{n0.addr, n1.addr}

	eventually(t, 10*time.Second, "w-0 leads", n0.raft.IsLeader)
	joinRetry(t, peers, "w-1", n1.addr)
	eventually(t, 10*time.Second, "epoch 1 on both nodes", func() bool {
		return n0.raft.Epoch() == 1 && n1.raft.Epoch() == 1
	})

	ctx := context.Background()
	// The follower's client forwards to the leader; the leader's applies locally.
	follower := NewOutboxClient(n1.raft, n1.raft.Epoch)
	leader := NewOutboxClient(n0.raft, n0.raft.Epoch)

	if err := follower.Baseline(ctx, "solo", 2); err != nil {
		t.Fatalf("forwarded baseline: %v", err)
	}
	eventually(t, 10*time.Second, "mark replicated to both nodes", func() bool {
		a, _ := leader.HighWater("solo")
		b, _ := follower.HighWater("solo")
		return a == 2 && b == 2
	})

	ch := scraper.Chapter{Target: "solo", Number: "3", URL: "u"}
	if st, err := follower.Enqueue(ctx, ch); err != nil || st != scraper.StatePending {
		t.Fatalf("enqueue = %s, %v", st, err)
	}
	// The retry after a timeout sees the same pending entry rather than a new one.
	if st, err := leader.Enqueue(ctx, ch); err != nil || st != scraper.StatePending {
		t.Fatalf("repeat enqueue = %s, %v", st, err)
	}
	if got := len(n0.raft.fsm.outbox.Pending()); got != 1 {
		t.Fatalf("pending entries = %d, want 1", got)
	}

	if err := follower.Ack(ctx, ch); err != nil {
		t.Fatalf("ack: %v", err)
	}
	eventually(t, 10*time.Second, "ack replicated, pending cleared", func() bool {
		a, _ := leader.HighWater("solo")
		b, _ := follower.HighWater("solo")
		return a == 3 && b == 3 &&
			len(n0.raft.fsm.outbox.Pending()) == 0 && len(n1.raft.fsm.outbox.Pending()) == 0
	})
	// Asking again is how a worker resolves "did my webhook land?": delivered.
	if st, err := leader.Enqueue(ctx, ch); err != nil || st != scraper.StateDelivered {
		t.Fatalf("enqueue after ack = %s, %v", st, err)
	}

	// A worker that missed a rebalance (epoch behind the cluster) is fenced.
	zombie := NewOutboxClient(n1.raft, func() uint64 { return 0 })
	err := zombie.Ack(ctx, scraper.Chapter{Target: "solo", Number: "9"})
	if !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("zombie ack = %v, want ErrStaleEpoch", err)
	}
	if hw, _ := leader.HighWater("solo"); hw != 3 {
		t.Fatalf("zombie moved the mark to %v", hw)
	}
}

// memSink is an in-memory raft.SnapshotSink.
type memSink struct{ bytes.Buffer }

func (m *memSink) ID() string            { return "mem" }
func (m *memSink) Cancel() error         { return nil }
func (m *memSink) Close() error          { return nil }
func (m *memSink) reader() io.ReadCloser { return io.NopCloser(bytes.NewReader(m.Bytes())) }
func readCloser(s string) io.ReadCloser  { return io.NopCloser(bytes.NewReader([]byte(s))) }

func TestFSMAcceptsStaleAckOnlyForPendingChapter(t *testing.T) {
	f := &ringFSM{outbox: scraper.NewOutboxState()}
	fsmApply(t, f, command{Type: cmdBumpEpoch})
	fsmApply(t, f, command{Type: cmdBumpEpoch})
	fsmApply(t, f, command{Type: cmdOutboxBaseline, Target: "t", Number: "3", Epoch: 2})
	fsmApply(t, f, command{Type: cmdOutboxEnqueue, Target: "t", Number: "4", Epoch: 2})

	// A rebalance landed between the POST and the Ack: the ack still records it.
	if got := fsmApply(t, f, command{Type: cmdOutboxAck, Target: "t", Number: "4", Epoch: 1}); got != any(scraper.StateDelivered) {
		t.Fatalf("stale ack of a pending chapter = %v", got)
	}
	// But a stale writer cannot ack something that was never pending.
	if got := fsmApply(t, f, command{Type: cmdOutboxAck, Target: "t", Number: "9", Epoch: 1}); got != any(ErrStaleEpoch) {
		t.Fatalf("stale ack of an unknown chapter = %v", got)
	}
	if hw, _ := f.HighWater("t"); hw != 4 {
		t.Fatalf("mark = %v", hw)
	}
}
