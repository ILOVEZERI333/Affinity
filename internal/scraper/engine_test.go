package scraper

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestParseChapters(t *testing.T) {
	e := NewEngine("")
	got, err := e.ParseChapters("t", []byte(`[
		{"number": 9, "title": "Nine", "url": "u9"},
		{"number": "10.5", "title": "Ten five"},
		{"number": "extra"}
	]`))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Number != "9" || got[1].Number != "10.5" {
		t.Fatalf("got %+v (non-numeric entry should be skipped)", got)
	}
	if _, err := e.ParseChapters("t", []byte(`not json`)); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestDetectNewNumericAndBaseline(t *testing.T) {
	e := NewEngine("")
	chs := func(nums ...string) []Chapter {
		var out []Chapter
		for _, n := range nums {
			out = append(out, Chapter{Target: "t", Number: n})
		}
		return out
	}

	ctx := context.Background()
	detect := func(c []Chapter) ([]Chapter, bool) {
		fresh, base, err := e.DetectNew(ctx, "t", c)
		if err != nil {
			t.Fatal(err)
		}
		return fresh, base
	}

	// First sight: baseline silently.
	if fresh, base := detect(chs("1", "2", "9")); !base || len(fresh) != 0 {
		t.Fatalf("expected baseline, got fresh=%v base=%v", fresh, base)
	}
	// "10" > "9" numerically (lexicographically it would be smaller).
	fresh, base := detect(chs("9", "10", "10.5"))
	if base || len(fresh) != 2 || fresh[0].Number != "10" || fresh[1].Number != "10.5" {
		t.Fatalf("fresh = %+v", fresh)
	}
	// Detection alone doesn't advance the mark.
	if again, _ := detect(chs("9", "10", "10.5")); len(again) != 2 {
		t.Fatalf("DetectNew mutated state: %+v", again)
	}
	if err := e.Outbox.Ack(ctx, Chapter{Target: "t", Number: "10"}); err != nil {
		t.Fatal(err)
	}
	if again, _ := detect(chs("9", "10", "10.5")); len(again) != 1 || again[0].Number != "10.5" {
		t.Fatalf("after MarkSeen: %+v", again)
	}
}

type fixture struct {
	mu       sync.Mutex
	chapters string
	hooks    int
	hookCode int
	source   *httptest.Server
	webhook  *httptest.Server
}

// clock is a settable time source for the engine's backoff.
type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newFixture(t *testing.T) (*Engine, *fixture) {
	f := &fixture{chapters: `[{"number":"1"},{"number":"2"}]`, hookCode: 204}
	f.source = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		fmt.Fprint(w, f.chapters)
	}))
	f.webhook = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.hooks++
		w.WriteHeader(f.hookCode)
	}))
	t.Cleanup(func() { f.source.Close(); f.webhook.Close() })
	e := NewEngine(f.webhook.URL)
	e.SourceURL = f.source.URL + "/series/" + TargetPlaceholder
	e.SetTargets([]string{"solo"})
	return e, f
}

func (f *fixture) set(chapters string, hookCode int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.chapters, f.hookCode = chapters, hookCode
}

func (f *fixture) hookCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hooks
}

func TestPollAnnouncesOnlyNewChapters(t *testing.T) {
	e, f := newFixture(t)
	ctx := context.Background()

	e.pollAll(ctx) // baseline
	if f.hookCount() != 0 {
		t.Fatalf("baseline announced %d chapters", f.hookCount())
	}
	e.pollAll(ctx) // nothing new
	if f.hookCount() != 0 {
		t.Fatal("announced with nothing new")
	}
	f.set(`[{"number":"1"},{"number":"2"},{"number":"3"}]`, 204)
	e.pollAll(ctx)
	e.pollAll(ctx) // already marked: no repeat
	if f.hookCount() != 1 {
		t.Fatalf("expected exactly 1 webhook, got %d", f.hookCount())
	}
}

func TestWebhookFailureKeepsChapterPendingAndBacksOff(t *testing.T) {
	e, f := newFixture(t)
	clk := &clock{t: time.Unix(1000, 0)}
	e.now = clk.now
	ctx := context.Background()
	e.pollAll(ctx) // baseline at 2

	// Scrape succeeds, webhook fails: the chapter stays pending, not delivered.
	f.set(`[{"number":"1"},{"number":"2"},{"number":"3"}]`, 500)
	e.pollAll(ctx)
	if f.hookCount() != 1 {
		t.Fatalf("expected 1 attempt, got %d", f.hookCount())
	}
	if p := e.Outbox.(*LocalOutbox).Pending(); len(p) != 1 || p[0].Number != "3" {
		t.Fatalf("pending = %+v, want chapter 3", p)
	}

	// Inside the backoff window the target is not polled at all.
	e.pollAll(ctx)
	if f.hookCount() != 1 {
		t.Fatalf("polled during backoff (%d attempts)", f.hookCount())
	}

	// Delays double: 5s after the first failure, 10s after the second.
	clk.advance(5 * time.Second)
	e.pollAll(ctx)
	if f.hookCount() != 2 {
		t.Fatalf("expected a retry after the first delay, got %d", f.hookCount())
	}
	clk.advance(5 * time.Second)
	e.pollAll(ctx)
	if f.hookCount() != 2 {
		t.Fatalf("second delay should be 10s, but retried after 5s (%d)", f.hookCount())
	}

	// Recovery delivers once, clears the pending entry, then stays silent.
	f.set(f.chapters, 204)
	clk.advance(5 * time.Second)
	e.pollAll(ctx)
	e.pollAll(ctx)
	if f.hookCount() != 3 {
		t.Fatalf("expected delivery on recovery then silence, got %d total", f.hookCount())
	}
	if p := e.Outbox.(*LocalOutbox).Pending(); len(p) != 0 {
		t.Fatalf("pending after delivery = %+v", p)
	}
}

func TestBackoffDelayIsCapped(t *testing.T) {
	e := NewEngine("")
	if got := e.backoffDelay(1); got != 5*time.Second {
		t.Fatalf("first delay = %s", got)
	}
	if got := e.backoffDelay(3); got != 20*time.Second {
		t.Fatalf("third delay = %s", got)
	}
	if got := e.backoffDelay(60); got != 10*time.Minute {
		t.Fatalf("delay not capped at 10m: %s", got)
	}
}

// laggingOutbox models a follower whose local copy of the high-water mark has
// not yet caught up with chapters another node already delivered.
type laggingOutbox struct {
	Outbox
	stale float64
}

func (l laggingOutbox) HighWater(string) (float64, bool) { return l.stale, true }

func TestAlreadyDeliveredChapterIsNotReposted(t *testing.T) {
	e, f := newFixture(t)
	ctx := context.Background()
	local := NewLocalOutbox()
	e.Outbox = laggingOutbox{Outbox: local, stale: 2}

	// The previous owner delivered chapter 3 and acked it in the shared outbox.
	f.set(`[{"number":"1"},{"number":"2"},{"number":"3"}]`, 204)
	_ = local.Baseline(ctx, "solo", 2)
	if err := local.Ack(ctx, Chapter{Target: "solo", Number: "3"}); err != nil {
		t.Fatal(err)
	}

	// Our view says 3 is new; Enqueue is the authoritative check and says no.
	e.pollAll(ctx)
	if f.hookCount() != 0 {
		t.Fatalf("re-announced a delivered chapter (%d webhooks)", f.hookCount())
	}
}

func TestMovedTargetKeepsHistory(t *testing.T) {
	// Two engines share one outbox, as two workers do through Raft.
	shared := NewLocalOutbox()
	a, f := newFixture(t)
	a.Outbox = shared
	ctx := context.Background()
	a.pollAll(ctx) // baseline at 2
	f.set(`[{"number":"1"},{"number":"2"},{"number":"3"}]`, 204)
	a.pollAll(ctx)
	if f.hookCount() != 1 {
		t.Fatalf("owner A sent %d webhooks", f.hookCount())
	}

	// The ring moves the target to B, which shares the outbox but has no local
	// memory. It must neither re-baseline (missing a release) nor repeat 3.
	b := NewEngine(f.webhook.URL)
	b.SourceURL = f.source.URL + "/series/" + TargetPlaceholder
	b.Outbox = shared
	b.SetTargets([]string{"solo"})
	b.pollAll(ctx)
	if f.hookCount() != 1 {
		t.Fatalf("new owner repeated a delivered chapter (%d)", f.hookCount())
	}
	f.set(`[{"number":"1"},{"number":"2"},{"number":"3"},{"number":"4"}]`, 204)
	b.pollAll(ctx)
	if f.hookCount() != 2 {
		t.Fatalf("new owner missed chapter 4 (%d webhooks)", f.hookCount())
	}
}

func TestNoAnnounceAfterLosingOwnership(t *testing.T) {
	e, f := newFixture(t)
	ctx := context.Background()
	e.pollAll(ctx) // baseline
	f.set(`[{"number":"1"},{"number":"2"},{"number":"3"}]`, 204)

	e.SetTargets(nil) // ring moved the target away
	e.pollAll(ctx)
	if f.hookCount() != 0 {
		t.Fatalf("announced for a target no longer owned (%d)", f.hookCount())
	}
}

// failingOutbox fails Enqueue the way a leaderless cluster does.
type failingOutbox struct{ Outbox }

func (failingOutbox) Enqueue(context.Context, Chapter) (DeliveryState, error) {
	return "", fmt.Errorf("no leader")
}

func TestOutboxFailureDoesNotBackOff(t *testing.T) {
	e, f := newFixture(t)
	ctx := context.Background()
	e.pollAll(ctx) // baseline at 2
	f.set(`[{"number":"1"},{"number":"2"},{"number":"3"}]`, 204)

	e.Outbox = failingOutbox{e.Outbox}
	e.pollAll(ctx)
	if e.backingOff("solo") {
		t.Fatal("a cluster-wide outbox failure put the target into backoff")
	}
}
