package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1 "github.com/affinity/distriscrape/api/v1"
	"github.com/affinity/distriscrape/internal/status"
)

func node(name string, leader bool, term uint64, owned ...string) status.NodeStatus {
	return status.NodeStatus{Node: name, Leader: leader, Term: term, Owned: owned}
}

func TestObserveFindsLeaderAndTargetOwnership(t *testing.T) {
	spec := []string{"a", "b", "c"}
	polled := time.Unix(2000, 0)
	older := time.Unix(1000, 0)
	w0 := node("w-0", true, 5, "a")
	w0.LastPoll = &older
	w1 := node("w-1", false, 5, "b", "c")
	w1.LastPoll = &polled

	obs := observe(spec, []status.NodeStatus{w0, w1})
	if obs.Leader != "w-0" || obs.Term != 5 || obs.Reports != 2 {
		t.Errorf("leader/term/reports = %q/%d/%d", obs.Leader, obs.Term, obs.Reports)
	}
	if len(obs.Unassigned) != 0 || len(obs.Duplicated) != 0 {
		t.Errorf("unassigned=%v duplicated=%v, want none", obs.Unassigned, obs.Duplicated)
	}
	if obs.LastSync == nil || !obs.LastSync.Equal(polled) {
		t.Errorf("LastSync = %v, want the newest poll %v", obs.LastSync, polled)
	}

	// c is owned by nobody, b by two workers (the Phase 6 invariant, broken).
	obs = observe(spec, []status.NodeStatus{node("w-0", true, 5, "a", "b"), node("w-1", false, 5, "b")})
	if strings.Join(obs.Unassigned, ",") != "c" || strings.Join(obs.Duplicated, ",") != "b" {
		t.Errorf("unassigned=%v duplicated=%v", obs.Unassigned, obs.Duplicated)
	}
}

func TestObserveStaleLeaderClaimLosesToHigherTerm(t *testing.T) {
	obs := observe(nil, []status.NodeStatus{node("old", true, 3), node("new", true, 4)})
	if obs.Leader != "new" || obs.Term != 4 {
		t.Errorf("leader = %q at term %d, want new at 4", obs.Leader, obs.Term)
	}
	// Order of reports must not matter.
	obs = observe(nil, []status.NodeStatus{node("new", true, 4), node("old", true, 3)})
	if obs.Leader != "new" {
		t.Errorf("leader = %q, want new", obs.Leader)
	}
}

func TestObserveIgnoresTargetsRemovedFromSpec(t *testing.T) {
	// A worker still owning a target that was just deleted from the spec is
	// transient, not a violation of the spec's own targets.
	obs := observe([]string{"a"}, []status.NodeStatus{node("w-0", true, 1, "a", "removed")})
	if len(obs.Unassigned)+len(obs.Duplicated) != 0 {
		t.Errorf("unassigned=%v duplicated=%v", obs.Unassigned, obs.Duplicated)
	}
}

func scraperWith(gen int64, targets ...string) *infrav1.DistributedScraper {
	return &infrav1.DistributedScraper{
		ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "default", Generation: gen},
		Spec:       infrav1.DistributedScraperSpec{TargetList: targets, WorkerNodes: 2},
	}
}

func condition(t *testing.T, s *infrav1.DistributedScraper, typ string) metav1.Condition {
	t.Helper()
	c := meta.FindStatusCondition(s.Status.Conditions, typ)
	if c == nil {
		t.Fatalf("condition %s not set", typ)
	}
	return *c
}

func TestConditionsReadyWhenEverythingHolds(t *testing.T) {
	s := scraperWith(4, "a", "b")
	obs := observe(s.Spec.TargetList, []status.NodeStatus{node("w-0", true, 2, "a"), node("w-1", false, 2, "b")})
	if !setObservedConditions(s, obs, true) {
		t.Fatal("not ready")
	}
	for _, typ := range []string{ConditionLeaderElected, ConditionTargetsAssigned, ConditionReady} {
		c := condition(t, s, typ)
		if c.Status != metav1.ConditionTrue || c.ObservedGeneration != 4 {
			t.Errorf("%s = %+v, want True at generation 4", typ, c)
		}
	}
	if m := condition(t, s, ConditionLeaderElected).Message; !strings.Contains(m, "w-0") || !strings.Contains(m, "term 2") {
		t.Errorf("LeaderElected message = %q", m)
	}
}

func TestConditionsExplainWhatIsMissing(t *testing.T) {
	cases := []struct {
		name         string
		reports      []status.NodeStatus
		workersReady bool
		leader       metav1.ConditionStatus
		targets      metav1.ConditionStatus
		readyReason  string
		targetReason string
	}{
		{
			name:    "no worker answered",
			reports: nil, workersReady: false,
			leader: metav1.ConditionUnknown, targets: metav1.ConditionUnknown,
			readyReason: "WorkersNotReady", targetReason: "NoWorkerReports",
		},
		{
			name:         "workers answered but no leader",
			reports:      []status.NodeStatus{node("w-0", false, 1, "a", "b")},
			workersReady: true,
			leader:       metav1.ConditionFalse, targets: metav1.ConditionTrue,
			readyReason: "NoLeader", targetReason: "AllTargetsAssigned",
		},
		{
			name:         "leader but a target is unowned",
			reports:      []status.NodeStatus{node("w-0", true, 1, "a")},
			workersReady: true,
			leader:       metav1.ConditionTrue, targets: metav1.ConditionFalse,
			readyReason: "TargetsNotAssigned", targetReason: "TargetsUnassigned",
		},
		{
			name:         "leader but a target is double-owned",
			reports:      []status.NodeStatus{node("w-0", true, 1, "a", "b"), node("w-1", false, 1, "b")},
			workersReady: true,
			leader:       metav1.ConditionTrue, targets: metav1.ConditionFalse,
			readyReason: "TargetsNotAssigned", targetReason: "TargetsDuplicated",
		},
		{
			name:         "all good but pods not ready",
			reports:      []status.NodeStatus{node("w-0", true, 1, "a", "b")},
			workersReady: false,
			leader:       metav1.ConditionTrue, targets: metav1.ConditionTrue,
			readyReason: "WorkersNotReady", targetReason: "AllTargetsAssigned",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := scraperWith(1, "a", "b")
			if setObservedConditions(s, observe(s.Spec.TargetList, tc.reports), tc.workersReady) {
				t.Fatal("reported Ready")
			}
			if got := condition(t, s, ConditionLeaderElected).Status; got != tc.leader {
				t.Errorf("LeaderElected = %s, want %s", got, tc.leader)
			}
			if c := condition(t, s, ConditionTargetsAssigned); c.Status != tc.targets || c.Reason != tc.targetReason {
				t.Errorf("TargetsAssigned = %s/%s, want %s/%s", c.Status, c.Reason, tc.targets, tc.targetReason)
			}
			if c := condition(t, s, ConditionReady); c.Status != metav1.ConditionFalse || c.Reason != tc.readyReason {
				t.Errorf("Ready = %s/%s, want False/%s", c.Status, c.Reason, tc.readyReason)
			}
		})
	}
}

func TestConditionMessageStaysShort(t *testing.T) {
	s := scraperWith(1, "a", "b", "c", "d", "e", "f", "g", "h")
	setObservedConditions(s, observe(s.Spec.TargetList, []status.NodeStatus{node("w-0", true, 1)}), true)
	msg := condition(t, s, ConditionTargetsAssigned).Message
	if !strings.Contains(msg, "a, b, c, d, e, and 3 more") {
		t.Errorf("message = %q", msg)
	}
}

func TestNextPhase(t *testing.T) {
	for _, tc := range []struct {
		prev  infrav1.ClusterPhase
		ready bool
		want  infrav1.ClusterPhase
	}{
		{"", false, infrav1.ClusterPhaseProvisioning},
		{infrav1.ClusterPhasePending, false, infrav1.ClusterPhaseProvisioning},
		{infrav1.ClusterPhaseProvisioning, false, infrav1.ClusterPhaseProvisioning},
		{infrav1.ClusterPhaseProvisioning, true, infrav1.ClusterPhaseRunning},
		{infrav1.ClusterPhaseRunning, false, infrav1.ClusterPhaseDegraded}, // regressed
		{infrav1.ClusterPhaseDegraded, false, infrav1.ClusterPhaseDegraded},
		{infrav1.ClusterPhaseDegraded, true, infrav1.ClusterPhaseRunning}, // recovered
	} {
		if got := nextPhase(tc.prev, tc.ready); got != tc.want {
			t.Errorf("nextPhase(%q, %v) = %q, want %q", tc.prev, tc.ready, got, tc.want)
		}
	}
}

func TestFetchStatusDecodesWorkerJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		// The shape worker.HealthHandler emits.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node": "w-1", "leader": true, "leaderID": "w-1", "term": 6, "epoch": 3, "voters": 3,
			"owned": []string{"a"}, "lastPoll": "2026-10-28T15:04:05Z",
		})
	}))
	defer srv.Close()

	st, err := fetchStatus(context.Background(), srv.Client(), srv.URL+"/status")
	if err != nil {
		t.Fatal(err)
	}
	if st.Node != "w-1" || !st.Leader || st.Term != 6 || len(st.Owned) != 1 || st.LastPoll == nil || st.LastPoll.Year() != 2026 {
		t.Errorf("decoded %+v", st)
	}
	if _, err := fetchStatus(context.Background(), srv.Client(), srv.URL+"/nope"); err == nil {
		t.Error("a 404 must be an error, not an empty status")
	}
}

func TestWorkerStatusURL(t *testing.T) {
	s := scraperWith(1)
	s.Name, s.Namespace = "manhwa", "media"
	want := "http://manhwa-workers-2.manhwa-workers.media.svc.cluster.local:8080/status"
	if got := workerStatusURL(s, 2); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

type fakePoller struct{ reports []status.NodeStatus }

func (f fakePoller) Poll(context.Context, *infrav1.DistributedScraper) []status.NodeStatus {
	return f.reports
}

// Drives Reconcile end to end against a fake API server: what the workers
// report must land in the CR's status, and the phase must track readiness.
func TestReconcileWritesObservedStatus(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := infrav1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	cr := scraperWith(1, "a", "b")
	cr.Spec.WebhookDestination = "https://example.com/hook"
	sts := desiredStatefulSet(cr)
	sts.Status.ReadyReplicas = 2

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cr, sts).WithStatusSubresource(&infrav1.DistributedScraper{}).Build()
	r := &DistributedScraperReconciler{Client: c, Scheme: scheme}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: "x", Namespace: "default"}}
	reconcile := func() ctrl.Result {
		t.Helper()
		res, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	get := func() infrav1.DistributedScraper {
		t.Helper()
		var got infrav1.DistributedScraper
		if err := c.Get(context.Background(), req.NamespacedName, &got); err != nil {
			t.Fatal(err)
		}
		return got
	}

	// Workers up but nobody reachable yet: still coming up.
	if res := reconcile(); res.RequeueAfter != convergingRequeue {
		t.Errorf("requeue while converging = %v, want %v", res.RequeueAfter, convergingRequeue)
	}
	if got := get(); got.Status.Phase != infrav1.ClusterPhaseProvisioning || got.Status.LeaderNode != "" {
		t.Errorf("phase=%q leader=%q before any report", got.Status.Phase, got.Status.LeaderNode)
	}

	// Leader elected and every target owned once: Running.
	polled := time.Unix(5000, 0)
	w0 := node("x-workers-0", true, 3, "a")
	w0.LastPoll = &polled
	r.Workers = fakePoller{[]status.NodeStatus{w0, node("x-workers-1", false, 3, "b")}}
	if res := reconcile(); res.RequeueAfter != steadyRequeue {
		t.Errorf("requeue when ready = %v, want %v", res.RequeueAfter, steadyRequeue)
	}
	got := get()
	if got.Status.Phase != infrav1.ClusterPhaseRunning || got.Status.LeaderNode != "x-workers-0" || got.Status.ReadyWorkers != 2 {
		t.Errorf("status = phase %q leader %q ready %d", got.Status.Phase, got.Status.LeaderNode, got.Status.ReadyWorkers)
	}
	if got.Status.LastSyncTime == nil || !got.Status.LastSyncTime.Time.Equal(polled) {
		t.Errorf("LastSyncTime = %v, want %v", got.Status.LastSyncTime, polled)
	}
	if c := condition(t, &got, ConditionReady); c.Status != metav1.ConditionTrue {
		t.Errorf("Ready = %+v", c)
	}

	// The leader is lost: the cluster was Running, so it is now Degraded.
	r.Workers = fakePoller{[]status.NodeStatus{node("x-workers-1", false, 3, "a", "b")}}
	reconcile()
	got = get()
	if got.Status.Phase != infrav1.ClusterPhaseDegraded || got.Status.LeaderNode != "" {
		t.Errorf("after leader loss: phase=%q leader=%q", got.Status.Phase, got.Status.LeaderNode)
	}
	if c := condition(t, &got, ConditionLeaderElected); c.Status != metav1.ConditionFalse {
		t.Errorf("LeaderElected = %+v", c)
	}
}
