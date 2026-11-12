package controller

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	infrav1 "github.com/affinity/distriscrape/api/v1"
	"github.com/affinity/distriscrape/internal/worker"
)

const (
	ConditionReady           = "Ready"
	ConditionLeaderElected   = "LeaderElected"
	ConditionTargetsAssigned = "TargetsAssigned"
)

func cond(typ, reason, msg string, st metav1.ConditionStatus) metav1.Condition {
	return metav1.Condition{Type: typ, Status: st, Reason: reason, Message: msg}
}

// computeConditions derives the three observability conditions from the nodes'
// /status answers. statuses has one entry per expected worker, nil for a worker
// that could not be reached. If none answered, everything is Unknown.
//
//	LeaderElected    at least two thirds of workers agree on one leader
//	Ready            a leader exists and more than half of workers are healthy
//	TargetsAssigned  every target has a confirmed owner in the leader's log view
func computeConditions(targets []string, workers int32, statuses []*worker.NodeStatus) (conds []metav1.Condition, leader string) {
	reached := 0
	healthy := 0
	votes := map[string]int{}
	byNode := map[string]*worker.NodeStatus{}
	for _, st := range statuses {
		if st == nil {
			continue
		}
		reached++
		byNode[st.Node] = st
		if st.Healthy {
			healthy++
		}
		if st.LeaderID != "" {
			votes[st.LeaderID]++
		}
	}
	if reached == 0 {
		msg := "no worker answered /status"
		return []metav1.Condition{
			cond(ConditionLeaderElected, "Unreachable", msg, metav1.ConditionUnknown),
			cond(ConditionTargetsAssigned, "Unreachable", msg, metav1.ConditionUnknown),
			cond(ConditionReady, "Unreachable", msg, metav1.ConditionUnknown),
		}, ""
	}

	best, bestVotes := "", 0
	for id, n := range votes {
		if n > bestVotes {
			best, bestVotes = id, n
		}
	}
	elected := workers > 0 && best != "" && bestVotes*3 >= int(workers)*2
	leaderExists := best != "" && bestVotes > 0

	le := cond(ConditionLeaderElected, "NoQuorumOnLeader", fmt.Sprintf("%d of %d workers agree on a leader, need two thirds", bestVotes, workers), metav1.ConditionFalse)
	if elected {
		le = cond(ConditionLeaderElected, "LeaderAgreed", fmt.Sprintf("%d of %d workers agree on %s", bestVotes, workers, best), metav1.ConditionTrue)
		leader = best
	}

	ta := cond(ConditionTargetsAssigned, "NoLeader", "no agreed leader to read assignments from", metav1.ConditionFalse)
	if ls, ok := byNode[best]; elected && ok {
		var missing []string
		for _, t := range targets {
			if ls.Owners[t] == "" {
				missing = append(missing, t)
			}
		}
		if len(missing) == 0 {
			ta = cond(ConditionTargetsAssigned, "AllOwned", "every target has a confirmed owner", metav1.ConditionTrue)
		} else {
			ta = cond(ConditionTargetsAssigned, "Unowned", fmt.Sprintf("%d of %d targets have no confirmed owner", len(missing), len(targets)), metav1.ConditionFalse)
		}
	}

	rd := cond(ConditionReady, "NotReady", fmt.Sprintf("%d of %d workers healthy (need more than half), leader=%t", healthy, workers, leaderExists), metav1.ConditionFalse)
	if leaderExists && elected && healthy*2 > int(workers) {
		rd = cond(ConditionReady, "Ready", fmt.Sprintf("%d of %d workers healthy with a leader", healthy, workers), metav1.ConditionTrue)
	}
	return []metav1.Condition{le, ta, rd}, leader
}

// applyConditions merges conds into the scraper's status, stamped with the
// current generation.
func applyConditions(scraper *infrav1.DistributedScraper, conds []metav1.Condition) {
	for _, c := range conds {
		c.ObservedGeneration = scraper.Generation
		meta.SetStatusCondition(&scraper.Status.Conditions, c)
	}
}
