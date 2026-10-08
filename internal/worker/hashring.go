package worker

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// HashRing maps scrape keys (target IDs) onto worker nodes via consistent
// hashing, so that adding or removing a node only reshuffles the keys owned
// by its immediate neighbors on the ring.
type HashRing struct {
	mu          sync.RWMutex
	virtualNode int // replicas per real node placed on the ring

	sortedHashes []uint32
	hashToNode   map[uint32]string
	nodes        map[string]struct{}
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
		hashToNode:  make(map[uint32]string),
		nodes:       make(map[string]struct{}),
	}
}

func hashKey(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// AddNode places nodeID's virtual replicas onto the ring. Adding a node that
// is already present is a no-op.
func (r *HashRing) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.nodes[nodeID]; ok {
		return
	}
	r.nodes[nodeID] = struct{}{}
	for i := 0; i < r.virtualNode; i++ {
		h := hashKey(nodeID + "#" + strconv.Itoa(i))
		// On the (rare) 32-bit collision the first node keeps the slot; the
		// ring stays deterministic because nodes are added in the same order
		// on every replica via the Raft log.
		if _, taken := r.hashToNode[h]; taken {
			continue
		}
		r.hashToNode[h] = nodeID
		r.sortedHashes = append(r.sortedHashes, h)
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool { return r.sortedHashes[i] < r.sortedHashes[j] })
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
	kept := r.sortedHashes[:0]
	for _, h := range r.sortedHashes {
		if r.hashToNode[h] == nodeID {
			delete(r.hashToNode, h)
			continue
		}
		kept = append(kept, h)
	}
	r.sortedHashes = kept
}

// Get returns the node responsible for key: the first node whose hash is
// greater than or equal to hash(key), wrapping around the ring.
func (r *HashRing) Get(key string) (nodeID string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sortedHashes) == 0 {
		return "", false
	}
	h := hashKey(key)
	i := sort.Search(len(r.sortedHashes), func(i int) bool { return r.sortedHashes[i] >= h })
	if i == len(r.sortedHashes) {
		i = 0
	}
	return r.hashToNode[r.sortedHashes[i]], true
}

// Members returns the distinct set of real node IDs currently on the ring,
// sorted for deterministic output.
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
