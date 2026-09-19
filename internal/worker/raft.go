package worker

import (
	"context"
	"io"
	"time"

	"github.com/hashicorp/raft"
)

// RaftNode wraps leader election and consensus for the worker cluster. The
// leader is the single writer responsible for placing targets on the
// HashRing and pushing assignments out to followers.
type RaftNode struct {
	raft *raft.Raft
	fsm  *ringFSM

	NodeID   string
	BindAddr string
}

// RaftNodeConfig carries what's needed to stand up a Raft peer.
type RaftNodeConfig struct {
	NodeID   string
	BindAddr string
	// DataDir is where the log store / stable store / snapshot store persist
	// state. Use an emptyDir or PVC mount in the worker Pod spec.
	DataDir string
	// Bootstrap is true only for the node that initializes a brand-new
	// cluster (normally the StatefulSet's ordinal-0 pod).
	Bootstrap bool
}

// NewRaftNode constructs and starts a Raft peer using the given config.
func NewRaftNode(cfg RaftNodeConfig) (*RaftNode, error) {
	// TODO:
	//   1. build raft.Config from cfg (LocalID = cfg.NodeID)
	//   2. construct transport (raft.NewTCPTransport on cfg.BindAddr)
	//   3. construct log/stable stores (raft-boltdb) and snapshot store rooted at cfg.DataDir
	//   4. construct the FSM (see ringFSM below) and call raft.NewRaft(...)
	//   5. if cfg.Bootstrap, call raft.BootstrapCluster with the initial config
	return &RaftNode{NodeID: cfg.NodeID, BindAddr: cfg.BindAddr}, nil
}

// Join adds a peer (nodeID at addr) to the cluster's voter configuration.
// Only the current leader can service this; callers should forward the
// request if IsLeader() is false.
func (n *RaftNode) Join(nodeID, addr string) error {
	// TODO: n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	return nil
}

// Leave removes a peer from the cluster's voter configuration, e.g. after
// detecting it via a failed heartbeat.
func (n *RaftNode) Leave(nodeID string) error {
	// TODO: n.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	return nil
}

// IsLeader reports whether this node currently holds Raft leadership.
func (n *RaftNode) IsLeader() bool {
	// TODO: return n.raft.State() == raft.Leader
	return false
}

// LeaderCh notifies observers of leadership changes for as long as ctx is
// alive, mirroring raft.Raft's LeaderCh but scoped to a context so callers
// can clean up.
func (n *RaftNode) LeaderCh(ctx context.Context) <-chan bool {
	ch := make(chan bool)
	// TODO: forward from n.raft.LeaderCh() until ctx.Done(), then close(ch).
	return ch
}

// Apply proposes a command (e.g. "target X moved to node Y") to the
// replicated log. Only meaningful when IsLeader() is true.
func (n *RaftNode) Apply(cmd []byte, timeout time.Duration) error {
	// TODO: n.raft.Apply(cmd, timeout).Error()
	return nil
}

// ringFSM applies committed log entries (hash-ring membership / assignment
// changes) to local in-memory state on every node, including followers.
type ringFSM struct {
	ring *HashRing
}

var _ raft.FSM = (*ringFSM)(nil)

// Apply is invoked once a log entry has been committed by a majority of the
// cluster.
func (f *ringFSM) Apply(log *raft.Log) interface{} {
	// TODO: decode log.Data (target/node assignment command) and mutate f.ring.
	return nil
}

// Snapshot returns an FSMSnapshot used to: support log compaction, so a
// slow follower or a follower that stops for a while doesn't need to have
// the entire log replayed.
func (f *ringFSM) Snapshot() (raft.FSMSnapshot, error) {
	// TODO: serialize f.ring's current membership into a snapshot.
	return nil, nil
}

// Restore is used to restore an FSM from a snapshot, e.g. on node restart.
func (f *ringFSM) Restore(snapshot io.ReadCloser) error {
	// TODO: deserialize snapshot into f.ring, replacing current state.
	return nil
}
