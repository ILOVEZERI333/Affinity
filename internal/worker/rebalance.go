package worker

import "sort"

// Move records a target changing owner during a rebalance.
type Move struct {
	Target string
	From   string // empty if the target was previously unowned
	To     string // empty if no live node can take it
}

// Assignments computes target -> owner for the current ring.
func Assignments(ring *HashRing, targets []string) map[string]string {
	out := make(map[string]string, len(targets))
	for _, t := range targets {
		owner, _ := ring.Get(t) // "" when the ring is empty
		out[t] = owner
	}
	return out
}

// DiffAssignments returns the targets whose owner differs between old and
// next, sorted by target ID. These are exactly the handoffs the leader must
// push (TaskHandoff) and the old owners must stop scraping.
func DiffAssignments(old, next map[string]string) []Move {
	var moves []Move
	for t, to := range next {
		if from := old[t]; from != to {
			moves = append(moves, Move{Target: t, From: from, To: to})
		}
	}
	sort.Slice(moves, func(i, j int) bool { return moves[i].Target < moves[j].Target })
	return moves
}

// Recovery is the outcome of removing a dead node.
type Recovery struct {
	Epoch uint64 // new ring epoch; handoffs and later writes must carry it
	Moves []Move
}

// RecoverFromFailure removes deadNode from the ring, advances the fencing
// epoch, and returns the handoffs needed to reassign its targets. Because
// the ring is consistent-hashed, only targets the dead node owned move
// (to its ring neighbors); everything else keeps its owner.
//
// Only the Raft leader should call this, and the result should be committed
// through the Raft log so all nodes apply the same removal and epoch.
func RecoverFromFailure(ring *HashRing, fence *EpochFence, targets []string, deadNode string) Recovery {
	before := Assignments(ring, targets)
	ring.RemoveNode(deadNode)
	epoch := fence.Advance()
	return Recovery{Epoch: epoch, Moves: DiffAssignments(before, Assignments(ring, targets))}
}
