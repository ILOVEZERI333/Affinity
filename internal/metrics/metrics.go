// Package metrics holds the worker's Prometheus instruments. They live in one
// leaf package so the scraper engine and the cluster code can both record
// without importing each other.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry is the worker's private registry, served at /metrics. It is not the
// global default so tests and the operator (which has its own) stay isolated.
var Registry = newRegistry()

func newRegistry() *prometheus.Registry {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return r
}

// Result label values for ScrapeDuration.
const (
	ResultOK    = "ok"
	ResultError = "error"
)

var (
	// ScrapeDuration times one full poll of one target (fetch, parse, detect,
	// notify). Labelled by outcome only: target IDs are user-supplied and would
	// make the series count grow with the target list.
	ScrapeDuration = promauto.With(Registry).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "affinity_scrape_duration_seconds",
		Help:    "Duration of one poll of one target, by result.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 10), // 50ms .. ~25s
	}, []string{"result"})

	// WebhookFailures counts webhook POSTs that did not get a 2xx. The chapter
	// stays pending and is retried, so one chapter can fail more than once.
	WebhookFailures = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "affinity_webhook_failures_total",
		Help: "Webhook deliveries that failed (the chapter stays pending and is retried).",
	})

	// ChaptersAnnounced counts chapters delivered and acknowledged.
	ChaptersAnnounced = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "affinity_chapters_announced_total",
		Help: "Chapters delivered to the webhook and acknowledged in the outbox.",
	})

	// RingRebalances counts membership changes the leader applied to the ring.
	// Only the leader's counter moves; sum across pods for the cluster total.
	RingRebalances = promauto.With(Registry).NewCounter(prometheus.CounterOpts{
		Name: "affinity_ring_rebalances_total",
		Help: "Hash ring membership changes applied by this node while leader.",
	})
)

// Handler serves Registry in the Prometheus text format.
func Handler() http.Handler {
	return promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
}
