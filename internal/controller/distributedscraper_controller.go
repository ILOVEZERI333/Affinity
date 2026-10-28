package controller

import (
	"context"

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

	infrav1 "github.com/affinity/distriscrape/api/v1"
	"github.com/affinity/distriscrape/internal/status"
)

// DistributedScraperReconciler reconciles a DistributedScraper object.
type DistributedScraperReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Workers is asked what the worker pods report (leader, owned targets).
	// Nil skips observation: the observed conditions then say Unknown.
	Workers WorkerPoller
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
			forgetClusterMetrics(req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	prevPhase := scraper.Status.Phase

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

	readyWorkers := int32(0)
	sts := &appsv1.StatefulSet{}
	stsKey := types.NamespacedName{Name: workersResourceName(&scraper), Namespace: scraper.Namespace}
	if err := r.Get(ctx, stsKey, sts); err == nil {
		readyWorkers = sts.Status.ReadyReplicas
	} else if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	scraper.Status.ReadyWorkers = readyWorkers

	workersReady := setWorkersReadyCondition(&scraper, readyWorkers)

	// What the workers themselves report is the only way to know who leads and
	// whether every target has an owner; the StatefulSet cannot tell us.
	var reports []status.NodeStatus
	if r.Workers != nil {
		reports = r.Workers.Poll(ctx, &scraper)
	}
	obs := observe(scraper.Spec.TargetList, reports)
	ready := setObservedConditions(&scraper, obs, workersReady)
	scraper.Status.LeaderNode = obs.Leader
	if obs.LastSync != nil {
		t := metav1.NewTime(*obs.LastSync)
		scraper.Status.LastSyncTime = &t
	}
	scraper.Status.Phase = nextPhase(prevPhase, ready)
	if scraper.Status.Phase != prevPhase {
		logger.Info("phase changed", "from", prevPhase, "to", scraper.Status.Phase,
			"readyWorkers", readyWorkers, "leader", obs.Leader,
			"unassigned", len(obs.Unassigned), "duplicated", len(obs.Duplicated))
	}
	recordClusterMetrics(&scraper, ready)

	if err := r.patchStatus(ctx, &scraper); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}

	if !ready {
		return ctrl.Result{RequeueAfter: convergingRequeue}, nil
	}
	return ctrl.Result{RequeueAfter: steadyRequeue}, nil
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

// setWorkersReadyCondition sets WorkersReady and reports whether it is True.
func setWorkersReadyCondition(scraper *infrav1.DistributedScraper, ready int32) bool {
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
	return cond.Status == metav1.ConditionTrue
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
