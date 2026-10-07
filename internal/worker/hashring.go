package worker

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strconv"
	"sync"
)

// ringEntry is one virtual-node position on the ring.
type ringEntry struct {
	hash uint64
	node string
}

// HashRing maps scrape keys (target IDs) onto worker nodes via consistent
// hashing, so that adding or removing a node only reshuffles the keys owned
// by its immediate neighbors on the ring.
//
// Picture a circle of 2^64 positions. Every node is dropped onto the circle
// many times (virtual nodes) at positions derived from hash(nodeID + "#" + i).
// A key is hashed onto the same circle and owned by the first node position
// found walking clockwise. Removing a node frees only the arcs it owned, and
// those keys fall to whichever node is next clockwise; every other key stays put.
type HashRing struct {
	mu          sync.RWMutex
	virtualNode int // replicas per real node placed on the ring

	entries []ringEntry         // sorted by (hash, node)
	nodes   map[string]struct{} // real node IDs currently on the ring
}

// NewHashRing constructs an empty ring. virtualNodes controls how many
// positions each real node occupies on the ring (higher = smoother
// distribution, more memory/CPU per lookup).
func NewHashRing(virtualNodes int) *HashRing {
	if virtualNodes < 1 {
		virtualNodes = 1
	}
	return &HashRing{
		virtualNode: virtualNodes,
		nodes:       make(map[string]struct{}),
	}
}

// hashKey turns a string into a ring position. SHA-256 (truncated) is far
// slower than FNV but mixes similar inputs like "w-0#1" and "w-0#2" well, which
// is what keeps the virtual nodes evenly spread. Lookups are rare, so speed is
// irrelevant here.
func hashKey(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

// AddNode places nodeID's virtual replicas onto the ring. Adding a node that is
// already present is a no-op.
func (r *HashRing) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[nodeID]; ok {
		return
	}
	r.nodes[nodeID] = struct{}{}
	for i := 0; i < r.virtualNode; i++ {
		r.entries = append(r.entries, ringEntry{hash: hashKey(nodeID + "#" + strconv.Itoa(i)), node: nodeID})
	}
	// Ties on hash (astronomically rare) are broken by node ID so every node in
	// the cluster builds the exact same ring.
	sort.Slice(r.entries, func(i, j int) bool {
		if r.entries[i].hash != r.entries[j].hash {
			return r.entries[i].hash < r.entries[j].hash
		}
		return r.entries[i].node < r.entries[j].node
	})
}

// RemoveNode removes all of nodeID's virtual replicas from the ring. Callers
// are responsible for reassigning any keys that were owned by nodeID.
func (r *HashRing) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[nodeID]; !ok {
		return
	}
	delete(r.nodes, nodeID)
	kept := r.entries[:0]
	for _, e := range r.entries {
		if e.node != nodeID {
			kept = append(kept, e)
		}
	}
	r.entries = kept
}

// Get returns the node responsible for key: the first node whose hash is
// greater than or equal to hash(key), wrapping around the ring.
func (r *HashRing) Get(key string) (nodeID string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.entries) == 0 {
		return "", false
	}
	h := hashKey(key)
	i := sort.Search(len(r.entries), func(i int) bool { return r.entries[i].hash >= h })
	if i == len(r.entries) {
		i = 0 // walked past the last position: wrap to the first
	}
	return r.entries[i].node, true
}

// Members returns the distinct set of real node IDs currently on the ring,
// sorted.
func (r *HashRing) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for n := range r.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Assign computes target -> owner for every target and returns it grouped by
// owner. Every ring member appears in the result, with an empty slice if it
// owns nothing, so callers can tell a node "you own nothing" explicitly.
// Each node's targets are sorted so results compare equal across calls.
func (r *HashRing) Assign(targets []string) map[string][]string {
	out := make(map[string][]string)
	for _, m := range r.Members() {
		out[m] = []string{}
	}
	for _, t := range targets {
		if owner, ok := r.Get(t); ok {
			out[owner] = append(out[owner], t)
		}
	}
	for _, ts := range out {
		sort.Strings(ts)
	}
	return out
}
