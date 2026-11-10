package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/affinity/distriscrape/internal/metrics"
)

const (
	statusPath = "/status"

	// ProbeInterval and ProbeFailureLimit: four failures in a row (one minute)
	// mark a node unhealthy.
	ProbeInterval     = 15 * time.Second
	ProbeFailureLimit = 4
)

// NodeStatus is one node's view of the cluster, served on /status. The operator
// derives the CR conditions from every node's copy; Handoffs is only filled on
// the leader.
type NodeStatus struct {
	Node         string            `json:"node"`
	IsLeader     bool              `json:"isLeader"`
	LeaderID     string            `json:"leaderID"`
	Term         uint64            `json:"term"`
	Epoch        uint64            `json:"epoch"`        // replicated ring epoch
	HandoffEpoch uint64            `json:"handoffEpoch"` // epoch of the last accepted handoff
	Voters       []string          `json:"voters"`
	Healthy      bool              `json:"healthy"`
	Owned        []string          `json:"owned"`
	Owners       map[string]string `json:"owners"` // target -> confirmed owner, from the Raft log
	Handoffs     []HandoffRecord   `json:"handoffs,omitempty"`
}

// Ready is the readiness rule: a leader is known and the synthetic probe passes.
func (s NodeStatus) Ready() bool { return s.LeaderID != "" && s.Healthy }

// Prober runs a synthetic check on an interval and counts consecutive failures.
type Prober struct {
	check   func(ctx context.Context) error
	fails   atomic.Int32
	metrics *metrics.M
}

func NewProber(check func(ctx context.Context) error, m *metrics.M) *Prober {
	return &Prober{check: check, metrics: m}
}

// Healthy is false once ProbeFailureLimit checks have failed in a row.
func (p *Prober) Healthy() bool { return p.fails.Load() < ProbeFailureLimit }

// Run checks immediately, then every interval, until ctx is cancelled.
func (p *Prober) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := p.check(cctx)
		cancel()
		if err != nil {
			p.metrics.ProbeFailures.Set(float64(p.fails.Add(1)))
		} else {
			p.fails.Store(0)
			p.metrics.ProbeFailures.Set(0)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// SelfCheck is the probe action: ask this node's own peer API for /status and
// require that it answers and knows a leader. It exercises the API server, the
// Raft handle and the FSM, and reports back through the response.
func SelfCheck(apiBindAddr string) func(ctx context.Context) error {
	_, port, _ := net.SplitHostPort(apiBindAddr)
	url := "http://" + net.JoinHostPort("localhost", port) + statusPath
	client := &http.Client{Timeout: 4 * time.Second}
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		var st NodeStatus
		if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
			return err
		}
		if st.LeaderID == "" {
			return errors.New("no leader known")
		}
		return nil
	}
}

// Status builds this node's current view.
func (s *Server) Status() NodeStatus {
	st := NodeStatus{
		Node:         s.raft.NodeID,
		IsLeader:     s.raft.IsLeader(),
		LeaderID:     s.raft.LeaderID(),
		Epoch:        s.raft.Epoch(),
		HandoffEpoch: s.Epoch(),
		Voters:       s.raft.Voters(),
		Healthy:      s.Prober == nil || s.Prober.Healthy(),
		Owned:        s.Owned(),
		Owners:       s.raft.Owners(),
	}
	st.Term, _ = strconv.ParseUint(s.raft.Stats()["term"], 10, 64)
	if st.IsLeader && s.Placer != nil {
		st.Handoffs = s.Placer.Handoffs()
	}
	return st
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.Status())
}

// SetMetrics installs the shared instruments and registers the gauges that are
// read from Raft and handoff state at scrape time.
func (s *Server) SetMetrics(m *metrics.M) {
	s.Metrics = m
	stat := func(key string) func() float64 {
		return func() float64 {
			v, _ := strconv.ParseFloat(s.raft.Stats()[key], 64)
			return v
		}
	}
	m.Gauge("raft_term", "Current Raft term.", stat("term"))
	m.Gauge("raft_commit_index", "Raft commit index.", stat("commit_index"))
	m.Gauge("raft_applied_index", "Raft applied index.", stat("applied_index"))
	m.Gauge("raft_last_log_index", "Raft last log index.", stat("last_log_index"))
	m.Gauge("raft_is_leader", "1 if this node is the Raft leader.", func() float64 {
		if s.raft.IsLeader() {
			return 1
		}
		return 0
	})
	m.Gauge("raft_voters", "Voters in the Raft configuration.", func() float64 { return float64(len(s.raft.Voters())) })
	m.Gauge("ring_epoch", "Replicated ring epoch.", func() float64 { return float64(s.raft.Epoch()) })
	m.Gauge("handoff_epoch", "Ring epoch of the last accepted handoff; lag behind ring_epoch marks a stale node.", func() float64 { return float64(s.Epoch()) })
	m.Gauge("targets_owned", "Targets this node currently owns.", func() float64 { return float64(len(s.Owned())) })
}

// instrument counts requests and times them, labelled by route pattern so the
// label set stays bounded.
func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rw, r)
		path := r.Pattern
		s.Metrics.HTTPRequests.WithLabelValues(path, fmt.Sprint(rw.code)).Inc()
		s.Metrics.HTTPDuration.WithLabelValues(path).Observe(time.Since(start).Seconds())
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) {
	w.code = c
	w.ResponseWriter.WriteHeader(c)
}

// HealthHandler serves the worker's port-8080 endpoints: liveness (independent
// of Raft), readiness, and Prometheus metrics.
func HealthHandler(ready func() bool, metricsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/healthz", LivenessHandler())
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", metricsHandler)
	return mux
}
