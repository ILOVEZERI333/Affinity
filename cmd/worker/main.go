package main

import (
	"context"
	"log"
	"net"
	"net/http"
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
	NodeID           string // usually the pod name (ordinal-stable via StatefulSet)
	BindAddr         string // Raft transport listen address
	AdvertiseAddr    string // stable address peers use to reach this node's Raft transport
	APIBindAddr      string // peer API (join/handoff) listen address
	PeerAddrs        []string
	Bootstrap        bool // true for pod ordinal 0 only
	DataDir          string
	TargetsFile      string // newline-separated target IDs (operator's mounted ConfigMap)
	SyncInterval     time.Duration
	DeadVoterTimeout time.Duration
	WebhookURL       string
	SourceURL        string // chapter-list endpoint, "{target}" is substituted
}

func loadConfig() Config {
	interval, err := time.ParseDuration(getenv("SYNC_INTERVAL", "30s"))
	if err != nil {
		log.Fatalf("invalid SYNC_INTERVAL: %v", err)
	}

	deadVoterTimeout, err := time.ParseDuration(getenv("DEAD_VOTER_TIMEOUT", worker.DefaultDeadVoterTimeout.String()))
	if err != nil {
		log.Fatalf("invalid DEAD_VOTER_TIMEOUT: %v", err)
	}

	var peers []string
	if raw := os.Getenv("PEER_ADDRS"); raw != "" {
		peers = strings.Split(raw, ",")
	}

	nodeID := getenv("NODE_ID", os.Getenv("POD_NAME"))
	bindAddr := getenv("BIND_ADDR", ":7946")

	return Config{
		NodeID:           nodeID,
		BindAddr:         bindAddr,
		AdvertiseAddr:    getenv("ADVERTISE_ADDR", advertiseAddrFor(nodeID, bindAddr, peers)),
		APIBindAddr:      getenv("API_BIND_ADDR", ":7947"),
		PeerAddrs:        peers,
		Bootstrap:        bootstrapFromEnv(),
		DataDir:          getenv("DATA_DIR", "/data/raft"),
		TargetsFile:      getenv("TARGETS_FILE", "/etc/affinity/targets.yaml"),
		SyncInterval:     interval,
		DeadVoterTimeout: deadVoterTimeout,
		WebhookURL:       os.Getenv("WEBHOOK_URL"),
		SourceURL:        os.Getenv("SOURCE_URL"),
	}
}

// advertiseAddrFor finds this node's own entry in PEER_ADDRS (the operator
// builds each as "{pod}.{svc}.{ns}.svc.cluster.local:{port}"), which is its
// stable headless-Service DNS name. Outside Kubernetes there is no such
// entry, so fall back to localhost on the bind port.
func advertiseAddrFor(nodeID, bindAddr string, peers []string) string {
	for _, p := range peers {
		if strings.HasPrefix(p, nodeID+".") {
			return p
		}
	}
	_, port, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return bindAddr
	}
	return net.JoinHostPort("localhost", port)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// bootstrapFromEnv honors BOOTSTRAP when set; otherwise ordinal-0 pods bootstrap
// (StatefulSet pod names end with "-0").
func bootstrapFromEnv() bool {
	switch os.Getenv("BOOTSTRAP") {
	case "true":
		return true
	case "false":
		return false
	default:
		podName := getenv("NODE_ID", os.Getenv("POD_NAME"))
		return strings.HasSuffix(podName, "-0")
	}
}

// maintainMembership keeps this node in the cluster. A node with no known
// leader is either brand new or was evicted as a dead voter while it was down,
// so it asks the cluster to (re)add it. Joining is idempotent. The leader
// never needs to join: it already is the cluster.
func maintainMembership(ctx context.Context, n *worker.RaftNode, cfg Config) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if n.IsLeader() || n.LeaderAddr() != "" {
			continue
		}
		if err := worker.JoinCluster(ctx, cfg.PeerAddrs, cfg.NodeID, cfg.AdvertiseAddr); err != nil {
			log.Printf("join cluster: %v", err)
			continue
		}
		log.Printf("joined cluster as %s", cfg.NodeID)
	}
}

func main() {
	cfg := loadConfig()
	if cfg.NodeID == "" {
		log.Fatal("NODE_ID (or POD_NAME) must be set")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	raftNode, err := worker.NewRaftNode(worker.RaftNodeConfig{
		NodeID:           cfg.NodeID,
		BindAddr:         cfg.BindAddr,
		AdvertiseAddr:    cfg.AdvertiseAddr,
		DataDir:          cfg.DataDir,
		Bootstrap:        cfg.Bootstrap,
		DeadVoterTimeout: cfg.DeadVoterTimeout,
	})
	if err != nil {
		log.Fatalf("failed to start raft node: %v", err)
	}
	defer func() {
		if err := raftNode.Shutdown(); err != nil {
			log.Printf("raft shutdown: %v", err)
		}
	}()

	// The ring starts empty: on the leader, the placer fills it from the Raft
	// voters each tick. Followers never consult it.
	ring := worker.NewHashRing(64)

	peerServer := worker.NewServer(cfg.APIBindAddr, raftNode, ring)
	engine := scraper.NewEngine(cfg.WebhookURL)
	engine.SourceURL = cfg.SourceURL
	// Delivery state (pending notifications and high-water marks) is replicated
	// through Raft, so it follows a target when the ring moves it and survives
	// this node dying. Writes are fenced by the epoch of our latest handoff.
	engine.Outbox = worker.NewOutboxClient(raftNode, peerServer.Epoch)

	// Accepted handoffs replace the engine's target set. Every node does this;
	// only the leader runs the placer that sends them.
	peerServer.SetTargetSink(engine.SetTargets)
	placer := worker.NewPlacer(raftNode, ring, worker.FileTargets(cfg.TargetsFile))
	go placer.Run(ctx)

	go func() {
		if err := peerServer.ListenAndServe(ctx); err != nil {
			log.Printf("peer server stopped: %v", err)
		}
	}()

	// Kubernetes liveness probe target (httpGet /healthz). It intentionally
	// ignores Raft state: failure *detection* of peers is Raft's job, and tying
	// the probe to it would make Kubernetes restart healthy pods during an election.
	healthSrv := &http.Server{
		Addr:              getenv("HEALTH_ADDR", ":8080"),
		Handler:           worker.LivenessHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("health server stopped: %v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		_ = healthSrv.Close()
	}()

	go engine.Run(ctx, cfg.SyncInterval)
	go maintainMembership(ctx, raftNode, cfg)

	log.Printf("worker %s ready (bind=%s)", cfg.NodeID, cfg.BindAddr)
	<-ctx.Done()
	log.Println("worker shutting down")
}
