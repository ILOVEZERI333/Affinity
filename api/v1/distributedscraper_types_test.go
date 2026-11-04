package v1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestDeepCopyIsIndependent(t *testing.T) {
	now := metav1.Now()
	orig := &DistributedScraper{
		Spec: DistributedScraperSpec{TargetList: []string{"a", "b"}, WorkerNodes: 3},
		Status: DistributedScraperStatus{
			Phase:        ClusterPhaseRunning,
			LastSyncTime: &now,
			Conditions:   []metav1.Condition{{Type: "Ready"}},
		},
	}
	cp := orig.DeepCopy()
	cp.Spec.TargetList[0] = "changed"
	cp.Status.Conditions[0].Type = "changed"
	*cp.Status.LastSyncTime = metav1.Time{}
	if orig.Spec.TargetList[0] != "a" || orig.Status.Conditions[0].Type != "Ready" || orig.Status.LastSyncTime.IsZero() {
		t.Fatal("DeepCopy shares memory with the original")
	}
}

func TestSchemeRegistersKinds(t *testing.T) {
	sch := runtime.NewScheme()
	if err := AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	for _, o := range []runtime.Object{&DistributedScraper{}, &DistributedScraperList{}} {
		if gvks, _, err := sch.ObjectKinds(o); err != nil || len(gvks) == 0 {
			t.Fatalf("%T not registered: %v", o, err)
		}
	}
}
