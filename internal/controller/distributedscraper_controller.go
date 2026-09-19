package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1 "github.com/affinity/distriscrape/api/v1"
)

// DistributedScraperReconciler reconciles a DistributedScraper object.
type DistributedScraperReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=infra.tony,resources=distributedscrapers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=infra.tony,resources=distributedscrapers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services;configmaps,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives the cluster state for a single DistributedScraper toward
// what's declared in its Spec.
func (r *DistributedScraperReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = ctx
	_ = req

	// TODO: fetch the DistributedScraper; if it's gone, return nil (already
	// cleaned up via owner references).
	//
	// var scraper infrav1.DistributedScraper
	// if err := r.Get(ctx, req.NamespacedName, &scraper); err != nil { ... }

	// TODO: reconcile the target ConfigMap (one entry per Spec.TargetList item).

	// TODO: reconcile the headless Service backing the worker StatefulSet.

	// TODO: reconcile the worker StatefulSet, scaling replicas to match
	// Spec.WorkerNodes and detecting drift.

	// TODO: refresh Status (Phase, LeaderNode, ReadyWorkers, LastSyncTime)
	// from the StatefulSet/pod state and call r.Status().Update(ctx, &scraper).

	return ctrl.Result{}, nil
}

// SetupWithManager wires this reconciler into the controller-runtime manager,
// registering watches for the resources it owns.
func (r *DistributedScraperReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&infrav1.DistributedScraper{}).
		// TODO: .Owns(&appsv1.StatefulSet{}).Owns(&corev1.Service{}).Owns(&corev1.ConfigMap{})
		Complete(r)
}
