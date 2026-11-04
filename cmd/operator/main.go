package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	infrav1 "github.com/affinity/distriscrape/api/v1"
	"github.com/affinity/distriscrape/internal/controller"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(infrav1.AddToScheme(scheme))
}

func main() {
	var metricsAddr, probeAddr string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address the Prometheus /metrics endpoint binds to. \"0\" disables it.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address the /healthz and /readyz endpoints bind to.")
	// JSON logs by default; pass --zap-devel for human-readable ones.
	zapOpts := zap.Options{}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))
	setupLog := ctrl.Log.WithName("setup")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		// TODO: set LeaderElection once this operator itself needs to run with
		// >1 replica (Phase 11).
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err := (&controller.DistributedScraperReconciler{
		Client:  mgr.GetClient(),
		Scheme:  mgr.GetScheme(),
		Workers: controller.HTTPPoller{},
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "DistributedScraper")
		os.Exit(1)
	}

	// Both are "the process is serving": the operator has no dependency whose
	// absence should take it out of rotation (Phase 11 revisits this with HA).
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager", "metrics", metricsAddr, "probes", probeAddr)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
