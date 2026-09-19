package worker

import (
	"context"
	"net"
)

// Server exposes RPC endpoints other worker nodes use to talk to this node:
// heartbeats for failure detection, cluster join requests forwarded to the
// Raft leader, and task handoff when the hash ring reassigns targets.
type Server struct {
	Addr string

	raft *RaftNode
	ring *HashRing

	listener net.Listener
}

// NewServer constructs a peer server backed by the given Raft node and hash
// ring, both of which the RPC handlers below read/mutate.
func NewServer(addr string, raftNode *RaftNode, ring *HashRing) *Server {
	return &Server{Addr: addr, raft: raftNode, ring: ring}
}

// ListenAndServe binds Addr and blocks, serving peer RPCs until ctx is
// cancelled.
func (s *Server) ListenAndServe(ctx context.Context) error {
	// TODO: choose a wire format (gRPC recommended, given raft already pulls
	// in protobuf-adjacent deps) and register Heartbeat/Join/TaskHandoff
	// handlers, then serve on Addr until ctx.Done().
	return nil
}

// Heartbeat answers a liveness probe from another node.
func (s *Server) Heartbeat(ctx context.Context, fromNodeID string) error {
	// TODO: record last-seen time for fromNodeID for failure detection.
	return nil
}

// Join handles a new node's request to join the cluster. Non-leaders should
// respond with a redirect to the current leader.
func (s *Server) Join(ctx context.Context, nodeID, addr string) error {
	if s.raft == nil || !s.raft.IsLeader() {
		// TODO: return a "not leader" error carrying the current leader's address.
		return nil
	}
	return s.raft.Join(nodeID, addr)
}

// TaskHandoff receives a set of target keys this node is now responsible
// for, typically sent by the leader after a ring rebalance.
func (s *Server) TaskHandoff(ctx context.Context, targets []string) error {
	// TODO: hand targets off to the local scraper.Engine's schedule.
	return nil
}
