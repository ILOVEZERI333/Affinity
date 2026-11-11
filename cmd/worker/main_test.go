package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/affinity/distriscrape/internal/worker"
)

func TestLoadConfigDefaultsAndOverrides(t *testing.T) {
	t.Setenv("POD_NAME", "scraper-0")
	t.Setenv("PEER_ADDRS", "scraper-0.svc:7946,scraper-1.svc:7946")
	c := loadConfig()
	if c.NodeID != "scraper-0" || !c.Bootstrap || c.SyncInterval != 30*time.Second ||
		c.DeadVoterTimeout != worker.DefaultDeadVoterTimeout || len(c.PeerAddrs) != 2 ||
		c.AdvertiseAddr != "scraper-0.svc:7946" {
		t.Fatalf("unexpected defaults: %+v", c)
	}

	t.Setenv("SYNC_INTERVAL", "5m")
	t.Setenv("DEAD_VOTER_TIMEOUT", "2s")
	t.Setenv("CLUSTER_ID", "prod")
	c = loadConfig()
	if c.SyncInterval != 5*time.Minute || c.DeadVoterTimeout != 2*time.Second || c.ClusterID != "prod" {
		t.Fatalf("overrides ignored: %+v", c)
	}
}

func TestBootstrapFromEnv(t *testing.T) {
	cases := []struct {
		bootstrap, pod string
		want           bool
	}{
		{"true", "w-3", true},
		{"false", "w-0", false},
		{"", "w-0", true},
		{"", "w-1", false},
		{"", "", false},
	}
	for _, c := range cases {
		t.Setenv("BOOTSTRAP", c.bootstrap)
		t.Setenv("NODE_ID", "")
		t.Setenv("POD_NAME", c.pod)
		if got := bootstrapFromEnv(); got != c.want {
			t.Errorf("BOOTSTRAP=%q pod=%q: got %v want %v", c.bootstrap, c.pod, got, c.want)
		}
	}
}

func TestAdvertiseAddrFor(t *testing.T) {
	peers := []string{"w-0.svc:7946", "w-1.svc:7946"}
	if got := advertiseAddrFor("w-1", ":7946", peers); got != "w-1.svc:7946" {
		t.Errorf("got %q", got)
	}
	// "w-1" must not match "w-10": the dot in the prefix guards that.
	if got := advertiseAddrFor("w-10", ":7946", peers); got != "localhost:7946" {
		t.Errorf("got %q", got)
	}
	if got := advertiseAddrFor("x", "bad", nil); got != "bad" {
		t.Errorf("got %q", got)
	}
}

// A node that never finds a leader must stop scraping once it has been
// leaderless longer than DeadVoterTimeout (zombie handling, Phase 9).
func TestMaintainMembershipStopsScrapingWhenLeaderless(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()

	n, err := worker.NewRaftNode(worker.RaftNodeConfig{
		NodeID: "lone", BindAddr: addr, AdvertiseAddr: addr,
		DataDir: t.TempDir(), DeadVoterTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()

	var stopped atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go maintainMembership(ctx, n, Config{NodeID: "lone", AdvertiseAddr: addr, DeadVoterTimeout: time.Second},
		func() { stopped.Store(true) })

	deadline := time.Now().Add(9 * time.Second)
	for !stopped.Load() {
		if time.Now().After(deadline) {
			t.Fatal("stopScraping never called for a leaderless node")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
