package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	infrav1 "github.com/affinity/distriscrape/api/v1"
	"github.com/affinity/distriscrape/internal/status"
)

// Condition types set on the CR (WorkersReady is in resources.go).
const (
	ConditionLeaderElected   = "LeaderElected"
	ConditionTargetsAssigned = "TargetsAssigned"
	ConditionReady           = "Ready"
)

const (
	// pollTimeout bounds one worker's /status request so a hung pod cannot
	// stall a reconcile.
	pollTimeout = 2 * time.Second

	// Nothing we watch changes when a leader is elected or a target moves, so
	// observation is by polling: quickly while converging, slowly once steady.
	convergingRequeue = 10 * time.Second
	steadyRequeue     = 30 * time.Second
)

// WorkerPoller asks the workers of a DistributedScraper what they see. Pods it
// cannot reach are simply absent from the result.
type WorkerPoller interface {
	Poll(ctx context.Context, scraper *infrav1.DistributedScraper) []status.NodeStatus
}

// HTTPPoller reads GET /status from each worker ordinal over the headless
// Service's stable DNS names.
type HTTPPoller struct {
	Client *http.Client
}

func (p HTTPPoller) Poll(ctx context.Context, scraper *infrav1.DistributedScraper) []status.NodeStatus {
	client := p.Client
	if client == nil {
		client = &http.Client{}
	}
	n := int(scraper.Spec.WorkerNodes)
	results := make([]*status.NodeStatus, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if st, err := fetchStatus(ctx, client, workerStatusURL(scraper, i)); err == nil {
				results[i] = st
			}
		}()
	}
	wg.Wait()

	out := make([]status.NodeStatus, 0, n)
	for _, st := range results {
		if st != nil {
			out = append(out, *st)
		}
	}
	return out
}

func workerStatusURL(scraper *infrav1.DistributedScraper, ordinal int) string {
	svc := workersResourceName(scraper)
	return fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:%d/status", svc, ordinal, svc, scraper.Namespace, workerHealthPort)
}

func fetchStatus(ctx context.Context, client *http.Client, url string) (*status.NodeStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, pollTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status returned %s", resp.Status)
	}
	var st status.NodeStatus
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&st); err != nil {
		return nil, err
	}
	return &st, nil
}

// observation is what the workers' reports add up to.
type observation struct {
	Reports  int // workers that answered
	Leader   string
	Term     uint64
	LastSync *time.Time

	Unassigned []string // spec targets no reporting worker owns
	Duplicated []string // spec targets more than one worker claims
}

// observe folds the workers' reports into one picture.
//
// If several nodes claim leadership (a deposed leader that has not yet noticed),
// the claim with the highest term wins: Raft terms only grow, so it is the
// newer one.
func observe(spec []string, reports []status.NodeStatus) observation {
	obs := observation{Reports: len(reports)}
	owners := map[string]int{}
	for _, r := range reports {
		if r.Leader && (obs.Leader == "" || r.Term > obs.Term) {
			obs.Leader, obs.Term = r.Node, r.Term
		}
		if r.LastPoll != nil && (obs.LastSync == nil || r.LastPoll.After(*obs.LastSync)) {
			t := *r.LastPoll
			obs.LastSync = &t
		}
		for _, t := range r.Owned {
			owners[t]++
		}
	}
	for _, t := range spec {
		switch c := owners[t]; {
		case c == 0:
			obs.Unassigned = append(obs.Unassigned, t)
		case c > 1:
			obs.Duplicated = append(obs.Duplicated, t)
		}
	}
	sort.Strings(obs.Unassigned)
	sort.Strings(obs.Duplicated)
	return obs
}

// setObservedConditions sets LeaderElected, TargetsAssigned and Ready, and
// returns whether the cluster is Ready. workersReady is the WorkersReady
// condition's verdict, decided by the caller.
func setObservedConditions(scraper *infrav1.DistributedScraper, obs observation, workersReady bool) bool {
	set := func(typ string, st metav1.ConditionStatus, reason, msg string) {
		meta.SetStatusCondition(&scraper.Status.Conditions, metav1.Condition{
			Type: typ, Status: st, Reason: reason, Message: msg, ObservedGeneration: scraper.Generation,
		})
	}

	leaderElected := obs.Leader != ""
	switch {
	case obs.Reports == 0:
		set(ConditionLeaderElected, metav1.ConditionUnknown, "NoWorkerReports", "No worker answered /status")
	case leaderElected:
		set(ConditionLeaderElected, metav1.ConditionTrue, "LeaderElected", fmt.Sprintf("%s leads at term %d", obs.Leader, obs.Term))
	default:
		set(ConditionLeaderElected, metav1.ConditionFalse, "NoLeader", "Workers answered but none is the Raft leader")
	}

	assigned := obs.Reports > 0 && len(obs.Unassigned) == 0 && len(obs.Duplicated) == 0
	switch {
	case obs.Reports == 0:
		set(ConditionTargetsAssigned, metav1.ConditionUnknown, "NoWorkerReports", "No worker answered /status")
	case len(obs.Duplicated) > 0:
		// The Phase 6 invariant (each target on exactly one worker) is broken.
		set(ConditionTargetsAssigned, metav1.ConditionFalse, "TargetsDuplicated", "Owned by more than one worker: "+joinCapped(obs.Duplicated))
	case len(obs.Unassigned) > 0:
		set(ConditionTargetsAssigned, metav1.ConditionFalse, "TargetsUnassigned", "Owned by no worker: "+joinCapped(obs.Unassigned))
	default:
		set(ConditionTargetsAssigned, metav1.ConditionTrue, "AllTargetsAssigned", "Every target is owned by exactly one worker")
	}

	ready := workersReady && leaderElected && assigned
	switch {
	case ready:
		set(ConditionReady, metav1.ConditionTrue, "ClusterReady", "Workers ready, leader elected, all targets assigned")
	case !workersReady:
		set(ConditionReady, metav1.ConditionFalse, "WorkersNotReady", "Waiting for worker pods to become ready")
	case !leaderElected:
		set(ConditionReady, metav1.ConditionFalse, "NoLeader", "Waiting for a Raft leader")
	default:
		set(ConditionReady, metav1.ConditionFalse, "TargetsNotAssigned", "Waiting for every target to have exactly one owner")
	}
	return ready
}

// joinCapped lists up to five names so a condition message stays short.
func joinCapped(names []string) string {
	const limit = 5
	out := ""
	for i, n := range names {
		if i == limit {
			return out + fmt.Sprintf(", and %d more", len(names)-limit)
		}
		if i > 0 {
			out += ", "
		}
		out += n
	}
	return out
}

// nextPhase is Running when Ready; otherwise Degraded if the cluster had been
// up (it regressed), else Provisioning (it is still coming up).
func nextPhase(prev infrav1.ClusterPhase, ready bool) infrav1.ClusterPhase {
	switch {
	case ready:
		return infrav1.ClusterPhaseRunning
	case prev == infrav1.ClusterPhaseRunning || prev == infrav1.ClusterPhaseDegraded:
		return infrav1.ClusterPhaseDegraded
	default:
		return infrav1.ClusterPhaseProvisioning
	}
}

var (
	clusterReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "affinity_cluster_ready",
		Help: "1 if the DistributedScraper's Ready condition is True.",
	}, []string{"namespace", "name"})
	clusterReadyWorkers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "affinity_cluster_ready_workers",
		Help: "Ready worker pods of the DistributedScraper.",
	}, []string{"namespace", "name"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(clusterReady, clusterReadyWorkers)
}

func recordClusterMetrics(scraper *infrav1.DistributedScraper, ready bool) {
	v := 0.0
	if ready {
		v = 1
	}
	clusterReady.WithLabelValues(scraper.Namespace, scraper.Name).Set(v)
	clusterReadyWorkers.WithLabelValues(scraper.Namespace, scraper.Name).Set(float64(scraper.Status.ReadyWorkers))
}

func forgetClusterMetrics(namespace, name string) {
	clusterReady.DeleteLabelValues(namespace, name)
	clusterReadyWorkers.DeleteLabelValues(namespace, name)
}
