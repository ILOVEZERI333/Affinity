// Package status defines what a worker reports about itself at GET /status.
// It is a leaf package so the operator can read it without linking Raft.
package status

import "time"

// NodeStatus is one worker's own view of the cluster. The operator polls it
// from every pod to fill the CR's status and conditions, so the JSON field
// names are a contract between worker and operator.
type NodeStatus struct {
	Node     string `json:"node"`
	Leader   bool   `json:"leader"`   // this node holds Raft leadership
	LeaderID string `json:"leaderID"` // who this node believes the leader is; "" if none
	Term     uint64 `json:"term"`
	Epoch    uint64 `json:"epoch"` // ring epoch (bumped on every membership change)
	Voters   int    `json:"voters"`
	// Owned is the targets this node is scraping, as last assigned by the leader.
	Owned []string `json:"owned"`
	// LastPoll is when the last pass over Owned finished; nil before the first.
	LastPoll *time.Time `json:"lastPoll,omitempty"`
}

// Ready reports whether the node knows a leader, i.e. it is part of a working
// cluster. This is the readiness signal, not the liveness one: an election or
// a partition makes a pod unready without getting it restarted.
func (s NodeStatus) Ready() bool { return s.Leader || s.LeaderID != "" }
