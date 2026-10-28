package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/affinity/distriscrape/internal/metrics"
)

func TestHealthHandlerReadinessFollowsLeader(t *testing.T) {
	st := NodeStatus{Node: "w-1"}
	h := HealthHandler(func() NodeStatus { return st })
	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec
	}

	// Liveness must not care about Raft; readiness must.
	if c := get("/healthz").Code; c != http.StatusOK {
		t.Fatalf("/healthz with no leader = %d, want 200", c)
	}
	if c := get("/readyz").Code; c != http.StatusServiceUnavailable {
		t.Fatalf("/readyz with no leader = %d, want 503", c)
	}

	st.LeaderID = "w-0" // a follower that knows its leader
	if c := get("/readyz").Code; c != http.StatusOK {
		t.Fatalf("/readyz as follower with leader = %d, want 200", c)
	}
	st.LeaderID, st.Leader = "", true // the leader itself
	if c := get("/readyz").Code; c != http.StatusOK {
		t.Fatalf("/readyz as leader = %d, want 200", c)
	}
}

func TestHealthHandlerStatusJSON(t *testing.T) {
	polled := time.Date(2026, 10, 28, 15, 4, 5, 0, time.UTC)
	want := NodeStatus{
		Node: "w-1", Leader: true, LeaderID: "w-1", Term: 3, Epoch: 2, Voters: 3,
		Owned: []string{"solo-leveling"}, LastPoll: &polled,
	}
	rec := httptest.NewRecorder()
	HealthHandler(func() NodeStatus { return want }).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))

	body := rec.Body.String()
	// The operator decodes these exact keys.
	for _, key := range []string{`"node"`, `"leader"`, `"leaderID"`, `"term"`, `"epoch"`, `"voters"`, `"owned"`, `"lastPoll"`} {
		if !strings.Contains(body, key) {
			t.Errorf("/status body missing %s: %s", key, body)
		}
	}
	var got NodeStatus
	if err := json.NewDecoder(strings.NewReader(body)).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Node != "w-1" || !got.Leader || got.Term != 3 || got.Epoch != 2 || got.Voters != 3 ||
		len(got.Owned) != 1 || got.Owned[0] != "solo-leveling" || got.LastPoll == nil || !got.LastPoll.Equal(polled) {
		t.Fatalf("round trip lost data: %+v", got)
	}
}

func TestHealthHandlerServesMetrics(t *testing.T) {
	rec := httptest.NewRecorder()
	HealthHandler(func() NodeStatus { return NodeStatus{} }).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d", rec.Code)
	}
	for _, name := range []string{"affinity_webhook_failures_total", "affinity_ring_rebalances_total", "affinity_chapters_announced_total"} {
		if !strings.Contains(rec.Body.String(), name) {
			t.Errorf("/metrics missing %s", name)
		}
	}
}

func TestNodeMetricsReadLiveState(t *testing.T) {
	st := NodeStatus{Leader: true, Term: 4, Epoch: 7, Voters: 3, Owned: []string{"a", "b"}}
	reg := prometheus.NewRegistry()
	RegisterNodeMetrics(reg, func() NodeStatus { return st })

	value := func(name string) float64 {
		t.Helper()
		families, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range families {
			if f.GetName() == name {
				return f.GetMetric()[0].GetGauge().GetValue()
			}
		}
		t.Fatalf("metric %s not found", name)
		return 0
	}
	if value("affinity_raft_is_leader") != 1 || value("affinity_raft_term") != 4 || value("affinity_ring_epoch") != 7 ||
		value("affinity_raft_voters") != 3 || value("affinity_owned_targets") != 2 {
		t.Fatal("gauges do not match the initial status")
	}
	st.Leader, st.Owned = false, nil // gauges are read at scrape time, not cached
	if value("affinity_raft_is_leader") != 0 || value("affinity_owned_targets") != 0 {
		t.Fatal("gauges are stale after the status changed")
	}
}

func TestLoggerStampsNodeAndLiveTerm(t *testing.T) {
	var buf bytes.Buffer
	term := uint64(1)
	log := NewLogger(&buf, "w-2", func() uint64 { return term })

	log.Info("first", "target", "solo-leveling")
	term = 2
	log.With("epoch", 9).Warn("second") // derived loggers keep the stamps

	var lines []map[string]any
	for _, raw := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			t.Fatalf("not JSON: %q: %v", raw, err)
		}
		lines = append(lines, m)
	}
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2", len(lines))
	}
	if lines[0]["node"] != "w-2" || lines[0]["term"] != float64(1) || lines[0]["target"] != "solo-leveling" || lines[0]["msg"] != "first" {
		t.Errorf("line 1 = %v", lines[0])
	}
	if lines[1]["node"] != "w-2" || lines[1]["term"] != float64(2) || lines[1]["epoch"] != float64(9) || lines[1]["level"] != slog.LevelWarn.String() {
		t.Errorf("line 2 = %v", lines[1])
	}
}

func TestSyncRingCountsRebalancesButNotInitialFill(t *testing.T) {
	p := &Placer{ring: NewHashRing(8)}
	rebalances := func() float64 { return testutil.ToFloat64(metrics.RingRebalances) }
	start := rebalances()

	p.syncRing(map[string]string{"a": "", "b": ""}) // a new leader filling an empty ring
	if d := rebalances() - start; d != 0 {
		t.Fatalf("initial fill counted %v rebalance(s)", d)
	}
	p.syncRing(map[string]string{"a": "", "b": ""}) // nothing changed
	if d := rebalances() - start; d != 0 {
		t.Fatalf("no-op sync counted %v rebalance(s)", d)
	}
	p.syncRing(map[string]string{"a": "", "b": "", "c": ""}) // join
	if d := rebalances() - start; d != 1 {
		t.Fatalf("join: %v rebalance(s), want 1", d)
	}
	p.syncRing(map[string]string{"a": "", "c": ""}) // leave
	if d := rebalances() - start; d != 2 {
		t.Fatalf("leave: %v rebalance(s), want 2", d)
	}
}

func TestCollectStatusFromRealNode(t *testing.T) {
	port := freePorts(t, 1)[0]
	n := startNode(t, "solo", port, t.TempDir(), true)
	defer n.stop()

	eventually(t, 10*time.Second, "node becomes leader", n.raft.IsLeader)
	if err := n.server.TaskHandoff(context.Background(), n.raft.Epoch(), []string{"x", "y"}); err != nil {
		t.Fatal(err)
	}

	polled := time.Unix(1000, 0)
	st := CollectStatus(n.raft, n.server, func() time.Time { return polled })
	if st.Node != "solo" || !st.Leader || st.LeaderID != "solo" || !st.Ready() {
		t.Errorf("identity/leadership wrong: %+v", st)
	}
	if st.Term == 0 {
		t.Error("Term() = 0 on an elected leader")
	}
	if st.Voters != 1 || len(st.Owned) != 2 {
		t.Errorf("voters=%d owned=%v", st.Voters, st.Owned)
	}
	if st.LastPoll == nil || !st.LastPoll.Equal(polled) {
		t.Errorf("LastPoll = %v", st.LastPoll)
	}

	// Before the engine has polled, the field is omitted rather than zero.
	if st := CollectStatus(n.raft, n.server, func() time.Time { return time.Time{} }); st.LastPoll != nil {
		t.Errorf("LastPoll = %v before any poll, want nil", st.LastPoll)
	}
}
