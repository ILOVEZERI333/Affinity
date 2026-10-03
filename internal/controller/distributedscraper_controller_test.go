package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	infrav1 "github.com/affinity/distriscrape/api/v1"
)

func TestDesiredConfigMapTargets(t *testing.T) {
	scraper := &infrav1.DistributedScraper{
		ObjectMeta: metav1.ObjectMeta{Name: "manhwa-tracker", Namespace: "default"},
		Spec: infrav1.DistributedScraperSpec{
			TargetList: []string{"solo-leveling", "omniscient-reader"},
		},
	}
	cm := desiredConfigMap(scraper)
	if cm.Name != "manhwa-tracker-targets" {
		t.Fatalf("configmap name: got %q", cm.Name)
	}
	want := "solo-leveling\nomniscient-reader\n"
	if cm.Data[targetsConfigKey] != want {
		t.Fatalf("targets.yaml: got %q want %q", cm.Data[targetsConfigKey], want)
	}
}

func TestDesiredStatefulSetReplicasAndNaming(t *testing.T) {
	scraper := &infrav1.DistributedScraper{
		ObjectMeta: metav1.ObjectMeta{Name: "manhwa-tracker", Namespace: "default"},
		Spec: infrav1.DistributedScraperSpec{
			WorkerNodes:        3,
			SyncInterval:       "1m",
			WebhookDestination: "https://example.com/hook",
		},
	}
	sts := desiredStatefulSet(scraper)
	if sts.Name != "manhwa-tracker-workers" || sts.Spec.ServiceName != "manhwa-tracker-workers" {
		t.Fatalf("unexpected sts naming: %s / %s", sts.Name, sts.Spec.ServiceName)
	}
	if *sts.Spec.Replicas != 3 {
		t.Fatalf("replicas: got %d", *sts.Spec.Replicas)
	}
	env := sts.Spec.Template.Spec.Containers[0].Env
	var syncInterval, webhook string
	for _, e := range env {
		switch e.Name {
		case "SYNC_INTERVAL":
			syncInterval = e.Value
		case "WEBHOOK_URL":
			webhook = e.Value
		}
	}
	if syncInterval != "1m" || webhook != "https://example.com/hook" {
		t.Fatalf("env: sync=%q webhook=%q", syncInterval, webhook)
	}
}

func TestDesiredServiceHeadless(t *testing.T) {
	scraper := &infrav1.DistributedScraper{
		ObjectMeta: metav1.ObjectMeta{Name: "x", Namespace: "default"},
	}
	svc := desiredService(scraper)
	if svc.Spec.ClusterIP != "None" {
		t.Fatalf("expected headless service, clusterIP=%q", svc.Spec.ClusterIP)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != workerPort {
		t.Fatalf("unexpected ports: %+v", svc.Spec.Ports)
	}
}
