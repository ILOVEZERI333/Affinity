package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsCarryClusterAndNodeLabels(t *testing.T) {
	m := New("prod", "w-0")
	m.RingRebalances.Inc()
	m.WebhookFailures.WithLabelValues("t1").Inc()
	m.Gauge("custom", "test gauge", func() float64 { return 7 })

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`distriscrape_ring_rebalances_total{cluster="prod",node="w-0"} 1`,
		`distriscrape_webhook_failures_total{cluster="prod",node="w-0",target="t1"} 1`,
		`distriscrape_custom{cluster="prod",node="w-0"} 7`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in:\n%s", want, body)
		}
	}
}
