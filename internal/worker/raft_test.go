package worker

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

type testNode struct {
	id       string
	addr     string // raft address
	dataDir  string
	raft     *RaftNode
	cancelAP context.CancelFunc
}

// freePorts returns n non-overlapping base ports: each returned port p has
// both p and p+1 free, since a node uses p for Raft and p+1 for its peer API.
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	var ports []int
	for len(ports) < n {
		l1, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		p := l1.Addr().(*net.TCPAddr).Port
		l2, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", p+1))
		l1.Close()
		if err != nil {
			continue
		}
		l2.Close()
		overlaps := false
		for _, q := range ports {
			if p-q >= -1 && p-q <= 1 {
				overlaps = true
			}
		}
		if !overlaps {
			ports = append(ports, p)
		}
	}
	return ports
}

func startNode(t *testing.T, id string, port int, dataDir string, bootstrap bool) *testNode {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	n, err := NewRaftNode(RaftNodeConfig{
		NodeID:           id,
		BindAddr:         addr,
		AdvertiseAddr:    addr,
		DataDir:          dataDir,
		Bootstrap:        bootstrap,
		DeadVoterTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
	apiCtx, cancel := context.WithCancel(context.Background())
	api, _ := PeerAPIAddr(addr)
	go NewServer(api, n, NewHashRing(8)).ListenAndServe(apiCtx)
	return &testNode{id: id, addr: addr, dataDir: dataDir, raft: n, cancelAP: cancel}
}

func (n *testNode) stop() {
	n.cancelAP()
	_ = n.raft.Shutdown()
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, what)
}

func joinRetry(t *testing.T, peers []string, id, addr string) {
	t.Helper()
	eventually(t, 15*time.Second, "join "+id, func() bool {
		return JoinCluster(context.Background(), peers, id, addr) == nil
	})
}

func TestClusterFormationFailoverAndDeadVoterRemoval(t *testing.T) {
	ports := freePorts(t, 3)
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	ids := []string{"w-0", "w-1", "w-2"}

	nodes := make([]*testNode, 3)
	nodes[0] = startNode(t, ids[0], ports[0], dirs[0], true)
	nodes[1] = startNode(t, ids[1], ports[1], dirs[1], false)
	nodes[2] = startNode(t, ids[2], ports[2], dirs[2], false)
	t.Cleanup(func() {
		for _, n := range nodes {
			if n != nil {
				n.stop()
			}
		}
	})
	peers := []string{nodes[0].addr, nodes[1].addr, nodes[2].addr}

	// Formation: ordinal 0 bootstraps, the others join through the peer API.
	eventually(t, 10*time.Second, "w-0 becomes leader", nodes[0].raft.IsLeader)
	joinRetry(t, peers, ids[1], nodes[1].addr)
	joinRetry(t, peers, ids[2], nodes[2].addr)

	eventually(t, 10*time.Second, "3 voters and epoch 2 on every node", func() bool {
		for _, n := range nodes {
			if len(n.raft.Voters()) != 3 || n.raft.Epoch() != 2 {
				return false
			}
		}
		return true
	})

	// Joining again is idempotent: no membership change, no epoch bump.
	joinRetry(t, peers, ids[1], nodes[1].addr)
	if e := nodes[0].raft.Epoch(); e != 2 {
		t.Fatalf("idempotent join bumped epoch to %d", e)
	}

	// Kill the leader: the survivors elect a new one, which then evicts the
	// dead voter after DeadVoterTimeout.
	leaderIdx := -1
	for i, n := range nodes {
		if n.raft.IsLeader() {
			leaderIdx = i
		}
	}
	if leaderIdx < 0 {
		t.Fatal("no leader found")
	}
	dead := nodes[leaderIdx]
	dead.stop()
	nodes[leaderIdx] = nil

	var survivors []*testNode
	for _, n := range nodes {
		if n != nil {
			survivors = append(survivors, n)
		}
	}
	eventually(t, 20*time.Second, "new leader evicts dead voter (2 voters, epoch 3)", func() bool {
		for _, n := range survivors {
			if len(n.raft.Voters()) != 2 || n.raft.Epoch() != 3 {
				return false
			}
		}
		return true
	})
	leaders := 0
	for _, n := range survivors {
		if n.raft.IsLeader() {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader among survivors, got %d", leaders)
	}

	// The evicted node restarts on its old data dir (so it must not
	// re-bootstrap even though it is ordinal 0 when leaderIdx == 0) and
	// rejoins, bumping the epoch again.
	port := ports[leaderIdx]
	restarted := startNode(t, dead.id, port, dead.dataDir, leaderIdx == 0)
	nodes[leaderIdx] = restarted
	joinRetry(t, peers, restarted.id, restarted.addr)
	eventually(t, 15*time.Second, "3 voters and epoch 4 after rejoin", func() bool {
		for _, n := range nodes {
			if len(n.raft.Voters()) != 3 || n.raft.Epoch() != 4 {
				return false
			}
		}
		return true
	})
}
