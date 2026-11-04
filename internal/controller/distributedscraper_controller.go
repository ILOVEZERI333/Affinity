package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	infrav1 "github.com/affinity/distriscrape/api/v1"
	"github.com/affinity/distriscrape/internal/worker"
)

const (
	statusPollBase = 2 * time.Second
	statusPollMax  = 5 * time.Minute
	statusRefresh  = 15 * time.Second
)

// lastStatusUpdate is when the operator last wrote status after reaching at
// least one worker, per cluster.
var lastStatusUpdate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Name: "distriscrape_operator_last_status_update_timestamp_seconds",
	Help: "Unix time of the last successful status update.",
}, []string{"cluster"})

func init() { ctrlmetrics.Registry.MustRegister(lastStatusUpdate) }

// DistributedScraperReconciler reconciles a DistributedScraper object.
type DistributedScraperReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// FetchStatus asks one worker's peer API for /status. Defaults to HTTP.
	FetchStatus func(ctx context.Context, url string) (*worker.NodeStatus, error)

	mu    sync.Mutex
	fails map[string]int // unreachable-in-a-row count per CR, for exponential backoff
}

func httpFetchStatus(ctx context.Context, url string) (*worker.NodeStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %s", resp.Status)
	}
	var st worker.NodeStatus
	return &st, json.NewDecoder(resp.Body).Decode(&st)
}

// pollWorkers asks every expected worker for /status concurrently; nil marks
// one that did not answer.
func (r *DistributedScraperReconciler) pollWorkers(ctx context.Context, scraper *infrav1.DistributedScraper) []*worker.NodeStatus {
	fetch := r.FetchStatus
	if fetch == nil {
		fetch = httpFetchStatus
	}
	urls := peerAPIURLs(scraper)
	out := make([]*worker.NodeStatus, len(urls))
	var wg sync.WaitGroup
	for i, u := range urls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if st, err := fetch(ctx, u); err == nil {
				out[i] = st
			}
		}()
	}
	wg.Wait()
	return out
}

// nextPoll is how long to wait before reconciling again: a steady refresh when
// workers answer, exponential backoff (capped) when none do.
func (r *DistributedScraperReconciler) nextPoll(key string, reached bool) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fails == nil {
		r.fails = map[string]int{}
	}
	if reached {
		delete(r.fails, key)
		return statusRefresh
	}
	n := r.fails[key]
	r.fails[key] = n + 1
	d := statusPollBase << min(n, 8)
	return min(d, statusPollMax)
}

// +kubebuilder:rbac:groups=infra.tony,resources=distributedscrapers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=infra.tony,resources=distributedscrapers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services;configmaps,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives the cluster state for a single DistributedScraper toward
// what's declared in its Spec.
func (r *DistributedScraperReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var scraper infrav1.DistributedScraper
	if err := r.Get(ctx, req.NamespacedName, &scraper); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if scraper.Status.Phase == "" {
		scraper.Status.Phase = infrav1.ClusterPhasePending
	}
	scraper.Status.Phase = infrav1.ClusterPhaseProvisioning

	if err := r.reconcileConfigMap(ctx, &scraper); err != nil {
		logger.Error(err, "reconcile ConfigMap failed")
		return ctrl.Result{}, err
	}
	if err := r.reconcileService(ctx, &scraper); err != nil {
		logger.Error(err, "reconcile Service failed")
		return ctrl.Result{}, err
	}
	if err := r.reconcileStatefulSet(ctx, &scraper); err != nil {
		logger.Error(err, "reconcile StatefulSet failed")
		return ctrl.Result{}, err
	}

	scraper.Status.Phase = infrav1.ClusterPhaseRunning

	readyWorkers := int32(0)
	sts := &appsv1.StatefulSet{}
	stsKey := types.NamespacedName{Name: workersResourceName(&scraper), Namespace: scraper.Namespace}
	if err := r.Get(ctx, stsKey, sts); err == nil {
		readyWorkers = sts.Status.ReadyReplicas
	} else if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	scraper.Status.ReadyWorkers = readyWorkers

	setWorkersReadyCondition(&scraper, readyWorkers)

	statuses := r.pollWorkers(ctx, &scraper)
	conds, leader := computeConditions(scraper.Spec.TargetList, scraper.Spec.WorkerNodes, statuses)
	applyConditions(&scraper, conds)
	reached := false
	for _, st := range statuses {
		reached = reached || st != nil
	}
	if reached {
		scraper.Status.LeaderNode = leader
	}

	if err := r.patchStatus(ctx, &scraper); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}

	if reached {
		lastStatusUpdate.WithLabelValues(req.String()).SetToCurrentTime()
	}
	logger.Info("status updated", "action", "status", "status", "ok", "cluster", req.String(), "reached", reached, "leader", leader)
	return ctrl.Result{RequeueAfter: r.nextPoll(req.String(), reached)}, nil
}

func (r *DistributedScraperReconciler) reconcileConfigMap(ctx context.Context, scraper *infrav1.DistributedScraper) error {
	desired := desiredConfigMap(scraper)
	if err := controllerutil.SetControllerReference(scraper, desired, r.Scheme); err != nil {
		return err
	}

	existing := &corev1.ConfigMap{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	existing.Labels = desired.Labels
	existing.Data = desired.Data
	return r.Update(ctx, existing)
}

func (r *DistributedScraperReconciler) reconcileService(ctx context.Context, scraper *infrav1.DistributedScraper) error {
	desired := desiredService(scraper)
	if err := controllerutil.SetControllerReference(scraper, desired, r.Scheme); err != nil {
		return err
	}

	existing := &corev1.Service{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	existing.Labels = desired.Labels
	existing.Spec.Selector = desired.Spec.Selector
	existing.Spec.Ports = desired.Spec.Ports
	return r.Update(ctx, existing)
}

func (r *DistributedScraperReconciler) reconcileStatefulSet(ctx context.Context, scraper *infrav1.DistributedScraper) error {
	desired := desiredStatefulSet(scraper)
	if err := controllerutil.SetControllerReference(scraper, desired, r.Scheme); err != nil {
		return err
	}

	existing := &appsv1.StatefulSet{}
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}

	existing.Labels = desired.Labels
	existing.Spec.Replicas = desired.Spec.Replicas
	existing.Spec.ServiceName = desired.Spec.ServiceName
	existing.Spec.Selector = desired.Spec.Selector
	existing.Spec.Template = desired.Spec.Template
	return r.Update(ctx, existing)
}

func setWorkersReadyCondition(scraper *infrav1.DistributedScraper, ready int32) {
	desired := scraper.Spec.WorkerNodes
	cond := metav1.Condition{
		Type:               ConditionWorkersReady,
		ObservedGeneration: scraper.Generation,
	}
	if ready >= desired {
		cond.Status = metav1.ConditionTrue
		cond.Reason = "AllWorkersReady"
		cond.Message = "All worker pods are ready"
	} else {
		cond.Status = metav1.ConditionFalse
		cond.Reason = "WorkersNotReady"
		cond.Message = "Waiting for worker pods to become ready"
	}
	meta.SetStatusCondition(&scraper.Status.Conditions, cond)
}

func (r *DistributedScraperReconciler) patchStatus(ctx context.Context, scraper *infrav1.DistributedScraper) error {
	return r.Status().Update(ctx, scraper)
}

// SetupWithManager wires this reconciler into the controller-runtime manager,
// registering watches for the resources it owns.
func (r *DistributedScraperReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&infrav1.DistributedScraper{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}
