package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const (
	raftMaxPool      = 3
	raftTimeout      = 10 * time.Second
	raftSnapshotKeep = 2

	// DefaultDeadVoterTimeout is how long the leader waits after a voter's
	// heartbeats start failing before it removes that voter. It has to be
	// longer than a typical StatefulSet pod restart (so a rolling restart
	// doesn't evict and re-add every pod) but short enough to leave room for
	// rebalancing inside the 10s failover SLO once Phase 6 lands.
	DefaultDeadVoterTimeout = 5 * time.Second

	deadVoterCheckInterval = time.Second

	cmdBumpEpoch = "bump_epoch"
)

// ErrNotLeader is returned by operations that only the Raft leader can
// service. Callers should forward the request to LeaderAddr().
var ErrNotLeader = errors.New("not the raft leader")

// RaftNode wraps leader election and consensus for the worker cluster. The
// leader is the single writer responsible for placing targets on the
// HashRing and pushing assignments out to followers.
type RaftNode struct {
	raft *raft.Raft
	fsm  *ringFSM

	logStore *raftboltdb.BoltStore

	deadVoterTimeout time.Duration
	cancelMonitor    context.CancelFunc
	monitorDone      chan struct{}

	NodeID   string
	BindAddr string
	// AdvertiseAddr is the address other nodes use to reach this node. It is
	// also the address stored in the Raft configuration, so it must be stable
	// across restarts (a headless-Service DNS name, not a pod IP).
	AdvertiseAddr string
}

// RaftNodeConfig carries what's needed to stand up a Raft peer.
type RaftNodeConfig struct {
	NodeID   string
	BindAddr string
	// AdvertiseAddr is this node's stable, dialable address (host:port). It is
	// what peers store in the Raft configuration and use to reach us.
	AdvertiseAddr string
	// DataDir is where the log store / stable store / snapshot store persist
	// state. Use an emptyDir or PVC mount in the worker Pod spec.
	DataDir string
	// Bootstrap is true only for the node that initializes a brand-new
	// cluster (normally the StatefulSet's ordinal-0 pod). It is ignored when
	// DataDir already holds Raft state, so a restart never re-bootstraps.
	Bootstrap bool
	// DeadVoterTimeout overrides DefaultDeadVoterTimeout when non-zero.
	DeadVoterTimeout time.Duration
}

// NewRaftNode constructs and starts a Raft peer using the given config.
func NewRaftNode(cfg RaftNodeConfig) (*RaftNode, error) {
	if cfg.AdvertiseAddr == "" {
		return nil, errors.New("raft: AdvertiseAddr is required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: create data dir: %w", err)
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.NodeID)

	listener, err := net.Listen("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("raft: listen on %s: %w", cfg.BindAddr, err)
	}
	trans := raft.NewNetworkTransport(&advertisedStreamLayer{
		Listener:  listener,
		advertise: stringAddr(cfg.AdvertiseAddr),
	}, raftMaxPool, raftTimeout, os.Stderr)

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
	if err != nil {
		_ = trans.Close()
		return nil, fmt.Errorf("raft: open bolt store: %w", err)
	}
	snapshots, err := raft.NewFileSnapshotStore(cfg.DataDir, raftSnapshotKeep, os.Stderr)
	if err != nil {
		_ = logStore.Close()
		_ = trans.Close()
		return nil, fmt.Errorf("raft: open snapshot store: %w", err)
	}

	fsm := &ringFSM{}
	r, err := raft.NewRaft(rc, fsm, logStore, logStore, snapshots, trans)
	if err != nil {
		_ = logStore.Close()
		_ = trans.Close()
		return nil, fmt.Errorf("raft: new raft: %w", err)
	}

	if cfg.Bootstrap {
		hasState, err := raft.HasExistingState(logStore, logStore, snapshots)
		if err != nil {
			_ = r.Shutdown().Error()
			_ = logStore.Close()
			return nil, fmt.Errorf("raft: check existing state: %w", err)
		}
		if !hasState {
			f := r.BootstrapCluster(raft.Configuration{Servers: []raft.Server{{
				Suffrage: raft.Voter,
				ID:       rc.LocalID,
				Address:  raft.ServerAddress(cfg.AdvertiseAddr),
			}}})
			if err := f.Error(); err != nil {
				_ = r.Shutdown().Error()
				_ = logStore.Close()
				return nil, fmt.Errorf("raft: bootstrap: %w", err)
			}
		}
	}

	timeout := cfg.DeadVoterTimeout
	if timeout == 0 {
		timeout = DefaultDeadVoterTimeout
	}
	monitorCtx, cancel := context.WithCancel(context.Background())
	n := &RaftNode{
		raft:             r,
		fsm:              fsm,
		logStore:         logStore,
		deadVoterTimeout: timeout,
		cancelMonitor:    cancel,
		monitorDone:      make(chan struct{}),
		NodeID:           cfg.NodeID,
		BindAddr:         cfg.BindAddr,
		AdvertiseAddr:    cfg.AdvertiseAddr,
	}
	go n.monitorDeadVoters(monitorCtx)
	return n, nil
}

// Shutdown stops the dead-voter monitor and the Raft instance, then closes
// the on-disk stores.
func (n *RaftNode) Shutdown() error {
	n.cancelMonitor()
	<-n.monitorDone
	err := n.raft.Shutdown().Error()
	if cerr := n.logStore.Close(); err == nil {
		err = cerr
	}
	return err
}

// Join adds a peer (nodeID at addr) to the cluster's voter configuration.
// Only the current leader can service this; callers should forward the
// request if IsLeader() is false. Joining a node that is already a voter at
// the same address is a no-op.
func (n *RaftNode) Join(nodeID, addr string) error {
	if !n.IsLeader() {
		return ErrNotLeader
	}
	servers, err := n.servers()
	if err != nil {
		return err
	}
	for _, s := range servers {
		if string(s.ID) == nodeID && string(s.Address) == addr && s.Suffrage == raft.Voter {
			return nil
		}
	}
	if err := n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, raftTimeout).Error(); err != nil {
		return fmt.Errorf("add voter %s: %w", nodeID, err)
	}
	return n.bumpEpoch()
}

// Leave removes a peer from the cluster's voter configuration, e.g. after
// detecting it via a failed heartbeat. Removing a node that isn't a member is
// a no-op.
func (n *RaftNode) Leave(nodeID string) error {
	if !n.IsLeader() {
		return ErrNotLeader
	}
	servers, err := n.servers()
	if err != nil {
		return err
	}
	found := false
	for _, s := range servers {
		if string(s.ID) == nodeID {
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	if err := n.raft.RemoveServer(raft.ServerID(nodeID), 0, raftTimeout).Error(); err != nil {
		return fmt.Errorf("remove server %s: %w", nodeID, err)
	}
	return n.bumpEpoch()
}

// IsLeader reports whether this node currently holds Raft leadership.
func (n *RaftNode) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// LeaderAddr returns the advertised address of the current leader, or "" if
// there is none (mid-election, or this node is partitioned away).
func (n *RaftNode) LeaderAddr() string {
	addr, _ := n.raft.LeaderWithID()
	return string(addr)
}

// IsMember reports whether this node's local view of the Raft configuration
// lists it as a voter. A node evicted while it was down still believes it is a
// member until it hears from the leader, so callers pair this with
// LeaderAddr() to decide whether to rejoin.
func (n *RaftNode) IsMember() bool {
	servers, err := n.servers()
	if err != nil {
		return false
	}
	for _, s := range servers {
		if string(s.ID) == n.NodeID && s.Suffrage == raft.Voter {
			return true
		}
	}
	return false
}

// Voters returns the node IDs of the current voters. Per the Phase 5 design,
// these are the members of the hash ring.
func (n *RaftNode) Voters() []string {
	servers, err := n.servers()
	if err != nil {
		return nil
	}
	ids := make([]string, 0, len(servers))
	for _, s := range servers {
		if s.Suffrage == raft.Voter {
			ids = append(ids, string(s.ID))
		}
	}
	return ids
}

// Epoch returns the replicated ring epoch. It increases by one on every
// membership change, on every node that has applied the corresponding entry.
func (n *RaftNode) Epoch() uint64 {
	return n.fsm.Epoch()
}

// LeaderCh notifies observers of leadership changes for as long as ctx is
// alive, mirroring raft.Raft's LeaderCh but scoped to a context so callers
// can clean up.
func (n *RaftNode) LeaderCh(ctx context.Context) <-chan bool {
	ch := make(chan bool)
	go func() {
		defer close(ch)
		src := n.raft.LeaderCh()
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-src:
				if !ok {
					return
				}
				select {
				case ch <- v:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch
}

// Apply proposes a command (e.g. "target X moved to node Y") to the
// replicated log. Only meaningful when IsLeader() is true.
func (n *RaftNode) Apply(cmd []byte, timeout time.Duration) error {
	f := n.raft.Apply(cmd, timeout)
	if err := f.Error(); err != nil {
		return err
	}
	if err, ok := f.Response().(error); ok {
		return err
	}
	return nil
}

func (n *RaftNode) servers() ([]raft.Server, error) {
	f := n.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return nil, err
	}
	return f.Configuration().Servers, nil
}

func (n *RaftNode) bumpEpoch() error {
	cmd, err := json.Marshal(command{Type: cmdBumpEpoch})
	if err != nil {
		return err
	}
	return n.Apply(cmd, raftTimeout)
}

// monitorDeadVoters removes voters from the cluster once the leader has been
// unable to heartbeat them for DeadVoterTimeout. Raft never evicts a silent
// voter on its own, and since ring members are the voters, a dead voter would
// otherwise keep owning targets that nobody scrapes.
//
// Only the leader acts. Followers keep no state: a new leader's replication
// goroutines emit fresh FailedHeartbeatObservations for any peer that is
// still down, so the timer restarts from the moment it takes over.
func (n *RaftNode) monitorDeadVoters(ctx context.Context) {
	defer close(n.monitorDone)

	obsCh := make(chan raft.Observation, 64)
	observer := raft.NewObserver(obsCh, false, func(o *raft.Observation) bool {
		switch o.Data.(type) {
		case raft.FailedHeartbeatObservation, raft.ResumedHeartbeatObservation:
			return true
		}
		return false
	})
	n.raft.RegisterObserver(observer)
	defer n.raft.DeregisterObserver(observer)

	downSince := map[raft.ServerID]time.Time{}
	ticker := time.NewTicker(deadVoterCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case o := <-obsCh:
			switch v := o.Data.(type) {
			case raft.FailedHeartbeatObservation:
				if _, tracked := downSince[v.PeerID]; !tracked {
					since := v.LastContact
					if since.IsZero() {
						since = time.Now()
					}
					downSince[v.PeerID] = since
				}
			case raft.ResumedHeartbeatObservation:
				delete(downSince, v.PeerID)
			}
		case <-ticker.C:
			if !n.IsLeader() {
				clear(downSince)
				continue
			}
			for id, since := range downSince {
				if string(id) == n.NodeID || time.Since(since) < n.deadVoterTimeout {
					continue
				}
				if err := n.Leave(string(id)); err != nil {
					log.Printf("raft: removing dead voter %s: %v", id, err)
					continue
				}
				log.Printf("raft: removed dead voter %s (no heartbeat for %s)", id, time.Since(since).Round(time.Millisecond))
				delete(downSince, id)
			}
		}
	}
}

// advertisedStreamLayer is a TCP stream layer whose Addr() reports a stable
// DNS name instead of the listener's bound address. Raft hands leaders'
// addresses to clients via Addr(), and it must match the address stored in the
// Raft configuration, which can't be a pod IP.
type advertisedStreamLayer struct {
	net.Listener
	advertise net.Addr
}

var _ raft.StreamLayer = (*advertisedStreamLayer)(nil)

func (s *advertisedStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

func (s *advertisedStreamLayer) Addr() net.Addr { return s.advertise }

type stringAddr string

func (a stringAddr) Network() string { return "tcp" }
func (a stringAddr) String() string  { return string(a) }

// command is the envelope for entries in the replicated log.
type command struct {
	Type string `json:"type"`
}

// ringFSM applies committed log entries to local in-memory state on every
// node, including followers. Phase 5 replicates only the ring epoch; the
// membership itself lives in Raft's own configuration. Phase 6 adds target
// assignments (and mutates ring) here.
type ringFSM struct {
	ring *HashRing

	mu    sync.RWMutex
	epoch uint64
}

var _ raft.FSM = (*ringFSM)(nil)

// Epoch returns the current ring epoch.
func (f *ringFSM) Epoch() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.epoch
}

// Apply is invoked once a log entry has been committed by a majority of the
// cluster.
func (f *ringFSM) Apply(l *raft.Log) interface{} {
	var cmd command
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		return fmt.Errorf("decode command: %w", err)
	}
	switch cmd.Type {
	case cmdBumpEpoch:
		f.mu.Lock()
		defer f.mu.Unlock()
		f.epoch++
		return f.epoch
	default:
		return fmt.Errorf("unknown command type %q", cmd.Type)
	}
}

// fsmState is the serialized form of the FSM used for snapshots.
type fsmState struct {
	Epoch uint64 `json:"epoch"`
}

// Snapshot returns an FSMSnapshot used to: support log compaction, so a
// slow follower or a follower that stops for a while doesn't need to have
// the entire log replayed.
func (f *ringFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &fsmSnapshot{state: fsmState{Epoch: f.epoch}}, nil
}

// Restore is used to restore an FSM from a snapshot, e.g. on node restart.
func (f *ringFSM) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()
	var state fsmState
	if err := json.NewDecoder(snapshot).Decode(&state); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.epoch = state.Epoch
	return nil
}

type fsmSnapshot struct {
	state fsmState
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.state); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
