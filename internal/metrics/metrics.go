// Package metrics holds the worker's Prometheus instruments. One *M per
// process; every series carries cluster and node labels via the registerer.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type M struct {
	Registry *prometheus.Registry
	reg      prometheus.Registerer

	ReleaseToWebhook *prometheus.HistogramVec // first detection -> webhook accepted
	PollDuration     *prometheus.HistogramVec // one target: fetch, parse, notify
	CycleDuration    prometheus.Histogram     // one pass over all owned targets
	WebhookFailures  *prometheus.CounterVec
	RingRebalances   prometheus.Counter
	UnownedTargets   prometheus.Gauge
	HTTPRequests     *prometheus.CounterVec
	HTTPDuration     *prometheus.HistogramVec
	ProbeFailures    prometheus.Gauge // consecutive failures of the synthetic probe
}

// New builds an M whose series all carry cluster and node labels.
func New(cluster, node string) *M {
	r := prometheus.NewRegistry()
	reg := prometheus.WrapRegistererWith(prometheus.Labels{"cluster": cluster, "node": node}, r)
	buckets := []float64{.05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300}
	m := &M{
		Registry: r,
		reg:      reg,
		ReleaseToWebhook: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "distriscrape_release_to_webhook_seconds", Help: "Time from first detecting a chapter to the webhook accepting it.", Buckets: buckets,
		}, []string{"target"}),
		PollDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "distriscrape_poll_duration_seconds", Help: "Duration of one target poll.", Buckets: buckets,
		}, []string{"target"}),
		CycleDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "distriscrape_scrape_cycle_duration_seconds", Help: "Duration of one pass over all owned targets.", Buckets: buckets,
		}),
		WebhookFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "distriscrape_webhook_failures_total", Help: "Failed webhook posts.",
		}, []string{"target"}),
		RingRebalances: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "distriscrape_ring_rebalances_total", Help: "Ring epoch changes seen by the placing leader.",
		}),
		UnownedTargets: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "distriscrape_unowned_targets", Help: "Targets with no confirmed owner (leader only).",
		}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "distriscrape_http_requests_total", Help: "Peer API requests.",
		}, []string{"path", "code"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "distriscrape_http_request_duration_seconds", Help: "Peer API request latency.", Buckets: prometheus.DefBuckets,
		}, []string{"path"}),
		ProbeFailures: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "distriscrape_probe_consecutive_failures", Help: "Consecutive failures of the synthetic health probe.",
		}),
	}
	reg.MustRegister(m.ReleaseToWebhook, m.PollDuration, m.CycleDuration, m.WebhookFailures,
		m.RingRebalances, m.UnownedTargets, m.HTTPRequests, m.HTTPDuration, m.ProbeFailures)
	return m
}

// Gauge registers a gauge whose value is read at scrape time.
func (m *M) Gauge(name, help string, fn func() float64) {
	m.reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "distriscrape_" + name, Help: help}, fn))
}

// Handler serves the registry in Prometheus text format.
func (m *M) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
