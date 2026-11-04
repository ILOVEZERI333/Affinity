package scraper

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/affinity/distriscrape/internal/metrics"
)

// scrapeCount is how many polls have been timed with the given result.
func scrapeCount(t *testing.T, result string) uint64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.ScrapeDuration.WithLabelValues(result).(prometheus.Metric).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram().GetSampleCount()
}

// The registry is process-global, so assert on deltas, not absolutes.
func TestPollRecordsMetrics(t *testing.T) {
	e, f := newFixture(t)
	clk := &clock{t: time.Unix(1000, 0)}
	e.now = clk.now
	ctx := context.Background()

	okBefore := scrapeCount(t, metrics.ResultOK)
	errBefore := scrapeCount(t, metrics.ResultError)
	failBefore := testutil.ToFloat64(metrics.WebhookFailures)
	sentBefore := testutil.ToFloat64(metrics.ChaptersAnnounced)

	if !e.LastPoll().IsZero() {
		t.Fatal("LastPoll set before any poll")
	}
	e.pollAll(ctx) // baseline: a successful poll
	if got := e.LastPoll(); !got.Equal(clk.t) {
		t.Fatalf("LastPoll = %v, want %v", got, clk.t)
	}

	// Source fine, webhook failing: a failed poll and a webhook failure.
	f.set(`[{"number":"1"},{"number":"2"},{"number":"3"}]`, 500)
	e.pollAll(ctx)

	// Recovery: a successful poll that announces the pending chapter.
	f.set(f.chapters, 204)
	clk.advance(time.Minute) // past the retry backoff
	e.pollAll(ctx)

	if d := scrapeCount(t, metrics.ResultOK) - okBefore; d != 2 {
		t.Errorf("ok polls = %d, want 2", d)
	}
	if d := scrapeCount(t, metrics.ResultError) - errBefore; d != 1 {
		t.Errorf("error polls = %d, want 1", d)
	}
	if d := testutil.ToFloat64(metrics.WebhookFailures) - failBefore; d != 1 {
		t.Errorf("webhook failures = %v, want 1", d)
	}
	if d := testutil.ToFloat64(metrics.ChaptersAnnounced) - sentBefore; d != 1 {
		t.Errorf("chapters announced = %v, want 1", d)
	}
}

func TestShutdownIsNotAScrapeFailure(t *testing.T) {
	e, _ := newFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	errBefore := scrapeCount(t, metrics.ResultError)
	failBefore := testutil.ToFloat64(metrics.WebhookFailures)
	observeScrape(time.Millisecond, context.Canceled)
	e.pollAll(ctx)
	if d := scrapeCount(t, metrics.ResultError) - errBefore; d != 0 {
		t.Errorf("cancelled poll counted as %d error(s)", d)
	}
	if d := testutil.ToFloat64(metrics.WebhookFailures) - failBefore; d != 0 {
		t.Errorf("cancelled poll counted as %v webhook failure(s)", d)
	}
}
