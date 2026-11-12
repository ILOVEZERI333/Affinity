package metrics

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
)

// Every metric the dashboard queries must be exposed by the worker, otherwise a
// stat tile silently shows "-".
func TestDashboardQueriesReferenceExposedMetrics(t *testing.T) {
	raw, err := os.ReadFile("../../dashboard/lib/queries.json")
	if err != nil {
		t.Fatal(err)
	}
	var queries map[string]string
	if err := json.Unmarshal(raw, &queries); err != nil {
		t.Fatal(err)
	}

	m := New("c", "n")
	// Vec series only appear once a child exists.
	m.ReleaseToWebhook.WithLabelValues("t").Observe(1)
	m.WebhookFailures.WithLabelValues("t").Inc()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()

	name := regexp.MustCompile(`distriscrape_[a-z_]+`)
	for tile, q := range queries {
		found := name.FindAllString(q, -1)
		if len(found) == 0 {
			t.Errorf("%s: query references no distriscrape metric: %s", tile, q)
		}
		for _, n := range found {
			base := strings.TrimSuffix(n, "_bucket")
			if !strings.Contains(body, "\n"+base+"{") && !strings.Contains(body, "\n"+base+"_bucket{") {
				t.Errorf("%s: metric %q is not exposed by the worker", tile, base)
			}
		}
	}
}
