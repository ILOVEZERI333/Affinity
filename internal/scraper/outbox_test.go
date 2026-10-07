package scraper

import (
	"context"
	"testing"
)

func TestOutboxStateLifecycle(t *testing.T) {
	s := NewOutboxState()
	ch := func(n string) Chapter { return Chapter{Target: "t", Number: n, URL: "u" + n} }

	s.Baseline("t", 5)
	s.Baseline("t", 99) // a mark that exists never moves
	if hw, _ := s.HighWater("t"); hw != 5 {
		t.Fatalf("baseline moved an existing mark: %v", hw)
	}

	// Covered chapters are delivered without ever becoming pending.
	if st, _ := s.Enqueue(ch("5")); st != StateDelivered {
		t.Fatalf("chapter at the mark: %s", st)
	}
	if st, _ := s.Enqueue(ch("6")); st != StatePending {
		t.Fatalf("new chapter: %s", st)
	}
	// Enqueue is idempotent: same key, still one entry.
	if st, _ := s.Enqueue(ch("6")); st != StatePending || len(s.Pending()) != 1 {
		t.Fatalf("repeat enqueue: %s, %d pending", st, len(s.Pending()))
	}

	if err := s.Ack(ch("6")); err != nil {
		t.Fatal(err)
	}
	if hw, _ := s.HighWater("t"); hw != 6 || len(s.Pending()) != 0 {
		t.Fatalf("after ack: mark %v, pending %v", hw, s.Pending())
	}
	if st, _ := s.Enqueue(ch("6")); st != StateDelivered {
		t.Fatalf("enqueue after ack: %s", st)
	}
	// The mark never moves backwards.
	_ = s.Ack(ch("2"))
	if hw, _ := s.HighWater("t"); hw != 6 {
		t.Fatalf("mark regressed to %v", hw)
	}
	if _, err := s.Enqueue(ch("x")); err == nil {
		t.Fatal("non-numeric chapter accepted")
	}
}

func TestOutboxStateSnapshotRoundTrip(t *testing.T) {
	s := NewOutboxState()
	s.Baseline("a", 3)
	_, _ = s.Enqueue(Chapter{Target: "a", Number: "4", URL: "u", Title: "T"})

	r := NewOutboxState()
	r.Restore(s.Snapshot())
	if hw, ok := r.HighWater("a"); !ok || hw != 3 {
		t.Fatalf("restored mark = %v %v", hw, ok)
	}
	p := r.Pending()
	if len(p) != 1 || p[0].Number != "4" || p[0].URL != "u" || p[0].Title != "T" {
		t.Fatalf("restored pending = %+v", p)
	}
	// Restore replaces rather than merges.
	r.Restore(OutboxSnapshot{})
	if _, ok := r.HighWater("a"); ok || len(r.Pending()) != 0 {
		t.Fatal("restore did not replace state")
	}
}

func TestLocalOutboxSatisfiesOutbox(t *testing.T) {
	var o Outbox = NewLocalOutbox()
	ctx := context.Background()
	if err := o.Baseline(ctx, "t", 1); err != nil {
		t.Fatal(err)
	}
	if st, err := o.Enqueue(ctx, Chapter{Target: "t", Number: "2"}); err != nil || st != StatePending {
		t.Fatalf("enqueue = %s, %v", st, err)
	}
}

func TestOutboxRejectsNonFiniteNumbers(t *testing.T) {
	s := NewOutboxState()
	for _, n := range []string{"Inf", "-Inf", "NaN", "1e999"} {
		if _, err := s.Enqueue(Chapter{Target: "t", Number: n}); err == nil {
			t.Errorf("Enqueue accepted %q", n)
		}
		if err := s.Ack(Chapter{Target: "t", Number: n}); err == nil {
			t.Errorf("Ack accepted %q", n)
		}
	}
	e := NewEngine("")
	got, _ := e.ParseChapters("t", []byte(`[{"number":"Inf"},{"number":"3"}]`))
	if len(got) != 1 || got[0].Number != "3" {
		t.Fatalf("ParseChapters kept a non-finite chapter: %+v", got)
	}
}

func TestAckPrunesLowerPendingEntries(t *testing.T) {
	s := NewOutboxState()
	s.Baseline("t", 1)
	_, _ = s.Enqueue(Chapter{Target: "t", Number: "3"})
	_, _ = s.Enqueue(Chapter{Target: "t", Number: "4"})
	_, _ = s.Enqueue(Chapter{Target: "other", Number: "2"})
	_ = s.Ack(Chapter{Target: "t", Number: "4"}) // chapter 3 was skipped upstream
	if p := s.Pending(); len(p) != 1 || p[0].Target != "other" {
		t.Fatalf("pending = %+v, want only the other target's entry", p)
	}
}
