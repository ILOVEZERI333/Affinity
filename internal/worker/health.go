package worker

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/affinity/distriscrape/internal/metrics"
	"github.com/affinity/distriscrape/internal/status"
)

// NodeStatus is served at /status; see package status for the fields.
type NodeStatus = status.NodeStatus

// CollectStatus snapshots a node. lastPoll is normally scraper.Engine.LastPoll.
func CollectStatus(n *RaftNode, s *Server, lastPoll func() time.Time) NodeStatus {
	st := NodeStatus{
		Node:     n.NodeID,
		Leader:   n.IsLeader(),
		LeaderID: n.LeaderID(),
		Term:     n.Term(),
		Epoch:    n.Epoch(),
		Voters:   len(n.Voters()),
		Owned:    s.Owned(),
	}
	if t := lastPoll(); !t.IsZero() {
		st.LastPoll = &t
	}
	return st
}

// LivenessHandler serves the Kubernetes liveness probe (mechanism #1 in the
// Phase 9 order). It deliberately checks only that the process can answer:
// it must not depend on Raft or peers, or a cluster-wide hiccup would make
// Kubernetes restart healthy pods.
func LivenessHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

// HealthHandler serves everything on the worker's health port: /healthz
// (liveness, see LivenessHandler), /readyz (readiness), /status (JSON
// NodeStatus) and /metrics (Prometheus).
func HealthHandler(node func() NodeStatus) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/healthz", LivenessHandler())
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !node().Ready() {
			http.Error(w, "no raft leader known", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(node())
	})
	mux.Handle("/metrics", metrics.Handler())
	return mux
}

// RegisterNodeMetrics exposes this node's cluster state as gauges that are read
// at scrape time, so they can never go stale. Call once per registry.
func RegisterNodeMetrics(reg prometheus.Registerer, node func() NodeStatus) {
	gauge := func(name, help string, f func(NodeStatus) float64) {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help},
			func() float64 { return f(node()) }))
	}
	gauge("affinity_raft_is_leader", "1 if this node is the Raft leader.", func(s NodeStatus) float64 {
		if s.Leader {
			return 1
		}
		return 0
	})
	gauge("affinity_raft_term", "Current Raft term.", func(s NodeStatus) float64 { return float64(s.Term) })
	gauge("affinity_ring_epoch", "Replicated hash ring epoch.", func(s NodeStatus) float64 { return float64(s.Epoch) })
	gauge("affinity_raft_voters", "Voters in this node's view of the cluster.", func(s NodeStatus) float64 { return float64(s.Voters) })
	gauge("affinity_owned_targets", "Targets this node is currently scraping.", func(s NodeStatus) float64 { return float64(len(s.Owned)) })
}
