package scraper

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
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

	// First sight: baseline silently.
	if fresh, base := e.DetectNew("t", chs("1", "2", "9")); !base || len(fresh) != 0 {
		t.Fatalf("expected baseline, got fresh=%v base=%v", fresh, base)
	}
	// "10" > "9" numerically (lexicographically it would be smaller).
	fresh, base := e.DetectNew("t", chs("9", "10", "10.5"))
	if base || len(fresh) != 2 || fresh[0].Number != "10" || fresh[1].Number != "10.5" {
		t.Fatalf("fresh = %+v", fresh)
	}
	// Detection alone doesn't advance the mark.
	if again, _ := e.DetectNew("t", chs("9", "10", "10.5")); len(again) != 2 {
		t.Fatalf("DetectNew mutated state: %+v", again)
	}
	e.MarkSeen(Chapter{Target: "t", Number: "10"})
	if again, _ := e.DetectNew("t", chs("9", "10", "10.5")); len(again) != 1 || again[0].Number != "10.5" {
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

func TestWebhookFailureKeepsChapterNew(t *testing.T) {
	e, f := newFixture(t)
	ctx := context.Background()
	e.pollAll(ctx) // baseline at 2

	// Scrape succeeds, webhook fails: chapter must stay unseen and be retried.
	f.set(`[{"number":"1"},{"number":"2"},{"number":"3"}]`, 500)
	e.pollAll(ctx)
	e.pollAll(ctx)
	if f.hookCount() != 2 {
		t.Fatalf("expected a retry each poll (2 attempts), got %d", f.hookCount())
	}
	f.set(f.chapters, 204)
	e.pollAll(ctx)
	e.pollAll(ctx)
	if f.hookCount() != 3 {
		t.Fatalf("expected delivery on recovery then silence, got %d total", f.hookCount())
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
