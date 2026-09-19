package worker

import "sync"

// HashRing maps scrape keys (target IDs) onto worker nodes via consistent
// hashing, so that adding or removing a node only reshuffles the keys owned
// by its immediate neighbors on the ring.
type HashRing struct {
	mu          sync.RWMutex
	virtualNode int // replicas per real node placed on the ring

	// TODO: backing structure for the ring, e.g.:
	//   sortedHashes []uint32
	//   hashToNode   map[uint32]string
}

// NewHashRing constructs an empty ring. virtualNodes controls how many
// positions each real node occupies on the ring (higher = smoother
// distribution, more memory/CPU per lookup).
func NewHashRing(virtualNodes int) *HashRing {
	return &HashRing{
		virtualNode: virtualNodes,
	}
}

// AddNode places nodeID's virtual replicas onto the ring.
func (r *HashRing) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// TODO: hash nodeID+i for i in [0, virtualNode), insert into sorted ring.
}

// RemoveNode removes all of nodeID's virtual replicas from the ring. Callers
// are responsible for reassigning any keys that were owned by nodeID.
func (r *HashRing) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// TODO: remove all ring entries pointing at nodeID.
}

// Get returns the node responsible for key: the first node whose hash is
// greater than or equal to hash(key), wrapping around the ring.
func (r *HashRing) Get(key string) (nodeID string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// TODO: hash key, binary-search the sorted ring for the successor node.
	return "", false
}

// Members returns the distinct set of real node IDs currently on the ring.
func (r *HashRing) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	// TODO: dedupe virtual node entries down to real node IDs.
	return nil
}
