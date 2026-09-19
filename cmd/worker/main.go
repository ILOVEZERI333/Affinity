package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/affinity/distriscrape/internal/scraper"
	"github.com/affinity/distriscrape/internal/worker"
)

// Config is read from the environment. The operator sets these on the
// worker StatefulSet's pod template from the DistributedScraper spec.
type Config struct {
	NodeID       string // usually the pod name (ordinal-stable via StatefulSet)
	BindAddr     string // this node's peer RPC address
	PeerAddrs    []string
	Bootstrap    bool // true for pod ordinal 0 only
	DataDir      string
	SyncInterval time.Duration
	WebhookURL   string
}

func loadConfig() Config {
	interval, err := time.ParseDuration(getenv("SYNC_INTERVAL", "30s"))
	if err != nil {
		log.Fatalf("invalid SYNC_INTERVAL: %v", err)
	}

	var peers []string
	if raw := os.Getenv("PEER_ADDRS"); raw != "" {
		peers = strings.Split(raw, ",")
	}

	return Config{
		NodeID:       getenv("NODE_ID", os.Getenv("POD_NAME")),
		BindAddr:     getenv("BIND_ADDR", ":7946"),
		PeerAddrs:    peers,
		Bootstrap:    os.Getenv("BOOTSTRAP") == "true",
		DataDir:      getenv("DATA_DIR", "/data/raft"),
		SyncInterval: interval,
		WebhookURL:   os.Getenv("WEBHOOK_URL"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	cfg := loadConfig()
	if cfg.NodeID == "" {
		log.Fatal("NODE_ID (or POD_NAME) must be set")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	raftNode, err := worker.NewRaftNode(worker.RaftNodeConfig{
		NodeID:    cfg.NodeID,
		BindAddr:  cfg.BindAddr,
		DataDir:   cfg.DataDir,
		Bootstrap: cfg.Bootstrap,
	})
	if err != nil {
		log.Fatalf("failed to start raft node: %v", err)
	}

	ring := worker.NewHashRing(64)
	ring.AddNode(cfg.NodeID)

	peerServer := worker.NewServer(cfg.BindAddr, raftNode, ring)
	engine := scraper.NewEngine(cfg.WebhookURL)

	// TODO: once raftNode.IsLeader() is meaningful, only the leader should
	// place cfg.PeerAddrs on the ring and call peerServer.Join for each; on
	// followers, wait for TaskHandoff RPCs to populate engine.SetTargets.
	_ = cfg.PeerAddrs

	go func() {
		if err := peerServer.ListenAndServe(ctx); err != nil {
			log.Printf("peer server stopped: %v", err)
		}
	}()

	go engine.Run(ctx, cfg.SyncInterval)

	log.Printf("worker %s ready (bind=%s)", cfg.NodeID, cfg.BindAddr)
	<-ctx.Done()
	log.Println("worker shutting down")
}
