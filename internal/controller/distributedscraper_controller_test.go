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

func TestDesiredStatefulSetPeerAddrsAndRaftPVC(t *testing.T) {
	scraper := &infrav1.DistributedScraper{
		ObjectMeta: metav1.ObjectMeta{Name: "manhwa-tracker", Namespace: "default"},
		Spec: infrav1.DistributedScraperSpec{
			WorkerNodes: 3,
		},
	}
	sts := desiredStatefulSet(scraper)

	var peerAddrs string
	for _, e := range sts.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "PEER_ADDRS" {
			peerAddrs = e.Value
		}
	}
	want := "manhwa-tracker-workers-0.manhwa-tracker-workers.default.svc.cluster.local:7946," +
		"manhwa-tracker-workers-1.manhwa-tracker-workers.default.svc.cluster.local:7946," +
		"manhwa-tracker-workers-2.manhwa-tracker-workers.default.svc.cluster.local:7946"
	if peerAddrs != want {
		t.Fatalf("PEER_ADDRS: got %q want %q", peerAddrs, want)
	}

	if len(sts.Spec.VolumeClaimTemplates) != 1 || sts.Spec.VolumeClaimTemplates[0].Name != raftDataVolumeName {
		t.Fatalf("expected a %q PVC template, got %+v", raftDataVolumeName, sts.Spec.VolumeClaimTemplates)
	}

	mounts := sts.Spec.Template.Spec.Containers[0].VolumeMounts
	if len(mounts) != 2 ||
		mounts[0].Name != raftDataVolumeName || mounts[0].MountPath != raftDataMountPath ||
		mounts[1].Name != targetsVolumeName || mounts[1].MountPath != targetsMountPath {
		t.Fatalf("unexpected volume mounts: %+v", mounts)
	}

	// The targets mount must be backed by the operator's ConfigMap, and the
	// worker told where to find the file, or the leader has nothing to place.
	vols := sts.Spec.Template.Spec.Volumes
	if len(vols) != 1 || vols[0].Name != targetsVolumeName || vols[0].ConfigMap == nil ||
		vols[0].ConfigMap.Name != configMapName(scraper) {
		t.Fatalf("expected targets volume backed by ConfigMap %q, got %+v", configMapName(scraper), vols)
	}
	var targetsFile string
	for _, e := range sts.Spec.Template.Spec.Containers[0].Env {
		if e.Name == "TARGETS_FILE" {
			targetsFile = e.Value
		}
	}
	if targetsFile != targetsMountPath+"/"+targetsConfigKey {
		t.Fatalf("TARGETS_FILE = %q", targetsFile)
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
	if len(svc.Spec.Ports) != 2 || svc.Spec.Ports[0].Port != workerPort || svc.Spec.Ports[1].Port != workerAPIPort {
		t.Fatalf("unexpected ports: %+v", svc.Spec.Ports)
	}
}
