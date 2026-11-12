package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/affinity/distriscrape/internal/worker"
)

func ns(node, leader string, healthy bool, owners map[string]string) *worker.NodeStatus {
	return &worker.NodeStatus{Node: node, LeaderID: leader, Healthy: healthy, Owners: owners}
}

func status(conds []metav1.Condition, typ string) metav1.ConditionStatus {
	for _, c := range conds {
		if c.Type == typ {
			return c.Status
		}
	}
	return ""
}

func TestComputeConditions(t *testing.T) {
	targets := []string{"a", "b"}
	all := map[string]string{"a": "w-0", "b": "w-1"}
	tests := []struct {
		name                    string
		statuses                []*worker.NodeStatus
		leader, assigned, ready metav1.ConditionStatus
	}{
		{"all unreachable", []*worker.NodeStatus{nil, nil, nil},
			metav1.ConditionUnknown, metav1.ConditionUnknown, metav1.ConditionUnknown},
		{"healthy", []*worker.NodeStatus{ns("w-0", "w-0", true, all), ns("w-1", "w-0", true, all), ns("w-2", "w-0", true, all)},
			metav1.ConditionTrue, metav1.ConditionTrue, metav1.ConditionTrue},
		{"two thirds agree, one lags", []*worker.NodeStatus{ns("w-0", "w-0", true, all), ns("w-1", "w-0", true, all), ns("w-2", "", true, nil)},
			metav1.ConditionTrue, metav1.ConditionTrue, metav1.ConditionTrue},
		{"mid election", []*worker.NodeStatus{ns("w-0", "", true, all), ns("w-1", "", true, all), ns("w-2", "", true, all)},
			metav1.ConditionFalse, metav1.ConditionFalse, metav1.ConditionFalse},
		{"target unowned does not block ready", []*worker.NodeStatus{ns("w-0", "w-0", true, map[string]string{"a": "w-0"}), ns("w-1", "w-0", true, nil), ns("w-2", "w-0", true, nil)},
			metav1.ConditionTrue, metav1.ConditionFalse, metav1.ConditionTrue},
		{"minority healthy", []*worker.NodeStatus{ns("w-0", "w-0", true, all), ns("w-1", "w-0", false, all), ns("w-2", "w-0", false, all)},
			metav1.ConditionTrue, metav1.ConditionTrue, metav1.ConditionFalse},
		{"unreachable nodes do not break the vote", []*worker.NodeStatus{ns("w-0", "w-0", true, all), ns("w-1", "w-0", true, all), nil},
			metav1.ConditionTrue, metav1.ConditionTrue, metav1.ConditionTrue},
		{"split vote has no leader", []*worker.NodeStatus{ns("w-0", "w-0", true, all), ns("w-1", "w-1", true, all), ns("w-2", "w-2", true, all)},
			metav1.ConditionFalse, metav1.ConditionFalse, metav1.ConditionFalse},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conds, _ := computeConditions(targets, 3, tt.statuses)
			for typ, want := range map[string]metav1.ConditionStatus{
				ConditionLeaderElected: tt.leader, ConditionTargetsAssigned: tt.assigned, ConditionReady: tt.ready} {
				if got := status(conds, typ); got != want {
					t.Errorf("%s = %q, want %q", typ, got, want)
				}
			}
		})
	}
}

func TestComputeConditionsZeroWorkers(t *testing.T) {
	conds, leader := computeConditions(nil, 0, []*worker.NodeStatus{ns("w-0", "w-0", true, nil)})
	if leader != "" || status(conds, ConditionLeaderElected) != metav1.ConditionFalse {
		t.Fatalf("workers=0 must not elect a leader: leader=%q conds=%v", leader, conds)
	}
}
