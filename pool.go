//go:build unix

// Pool: the finite set of nodes a session owns, and per-job allocation.
//
// Allocate carves a SubPool out of the pool to satisfy a job's PoolSpec, all or
// nothing: either every node spec is matched to a distinct free node or the pool
// is left untouched and an error wrapping ErrAllocation is returned. Free gives
// a SubPool's nodes back. Matching honors each spec's hard Required resources
// (NodeSpec.SatisfiedBy) and computes a maximum bipartite matching, so it accepts
// a request whenever some assignment of distinct nodes to specs exists rather
// than rejecting a feasible one because a flexible spec greedily took a node a
// pickier spec needed. OS matching is not yet enforced -- v1 pools are
// single-OS -- and is future work once nodes carry an OS.

package torx

import (
	"cmp"
	"fmt"
	"slices"
	"sync"
)

// Pool is the finite set of nodes a session owns. It is safe for concurrent use.
type Pool struct {
	mu      sync.Mutex
	free    []*Node
	inUse   map[*Node]struct{}
	total   int
	maxUsed int
}

// NewPool returns a Pool over the given nodes, all initially free.
func NewPool(nodes []*Node) *Pool {
	return &Pool{
		free:  append([]*Node(nil), nodes...),
		inUse: make(map[*Node]struct{}, len(nodes)),
		total: len(nodes),
	}
}

// SubPool is the set of nodes allocated to one job.
type SubPool struct {
	nodes []*Node
	freed bool
}

// Nodes returns the allocated nodes, ordered to match the PoolSpec that
// requested them: Nodes()[i] satisfies the spec's i-th NodeSpec.
func (s *SubPool) Nodes() []*Node {
	return append([]*Node(nil), s.nodes...)
}

// Size is the number of allocated nodes.
func (s *SubPool) Size() int { return len(s.nodes) }

// Allocate carves a SubPool satisfying spec out of the free nodes, atomically:
// on failure the pool is unchanged and the error wraps ErrAllocation.
func (p *Pool) Allocate(spec PoolSpec) (*SubPool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	assigned, ok := matchSpecs(p.free, spec.Nodes)
	if !ok {
		return nil, Wrap(ErrAllocation, "pool: allocate",
			fmt.Errorf("cannot satisfy %d node spec(s); %d of %d free", len(spec.Nodes), len(p.free), p.total))
	}

	claimed := make(map[*Node]struct{}, len(assigned))
	for _, n := range assigned {
		claimed[n] = struct{}{}
		p.inUse[n] = struct{}{}
	}
	kept := make([]*Node, 0, len(p.free)-len(assigned))
	for _, n := range p.free {
		if _, taken := claimed[n]; !taken {
			kept = append(kept, n)
		}
	}
	p.free = kept
	if len(p.inUse) > p.maxUsed {
		p.maxUsed = len(p.inUse)
	}
	return &SubPool{nodes: assigned}, nil
}

// CanAllocate reports whether spec could be satisfied from the currently free
// nodes, without consuming any.
func (p *Pool) CanAllocate(spec PoolSpec) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := matchSpecs(p.free, spec.Nodes)
	return ok
}

// CanEverFit reports whether spec could be satisfied by the pool's nodes if all
// were free -- so the scheduler can fail a job that can never run instead of
// waiting for it forever.
func (p *Pool) CanEverFit(spec PoolSpec) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	all := make([]*Node, 0, p.total)
	all = append(all, p.free...)
	for n := range p.inUse {
		all = append(all, n)
	}
	_, ok := matchSpecs(all, spec.Nodes)
	return ok
}

// Free returns a SubPool's nodes to the pool. It is idempotent.
func (p *Pool) Free(sub *SubPool) {
	if sub == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if sub.freed {
		return
	}
	sub.freed = true
	for _, n := range sub.nodes {
		delete(p.inUse, n)
		p.free = append(p.free, n)
	}
}

// Evict removes a SubPool's nodes from circulation without returning them to the
// free set. Use it for a job that could not confirm its node was left clean -- a
// service that may still be running, a port still held, data not removed -- so a
// possibly-dirty node cannot be handed to a later job. Evicted nodes count as
// neither free nor in use, shrinking the pool's capacity. Like Free it is
// idempotent for a given SubPool, and the two are mutually exclusive.
func (p *Pool) Evict(sub *SubPool) {
	if sub == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if sub.freed {
		return
	}
	sub.freed = true
	for _, n := range sub.nodes {
		delete(p.inUse, n)
	}
}

// Size is the total number of nodes in the pool.
func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.total
}

// Available is the number of free nodes.
func (p *Pool) Available() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.free)
}

// InUse is the number of allocated nodes.
func (p *Pool) InUse() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.inUse)
}

// MaxUsed is the high-water mark of simultaneously allocated nodes, for cluster
// utilization reporting.
func (p *Pool) MaxUsed() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxUsed
}

// matchSpecs assigns a distinct candidate node to every spec, or reports that no
// such assignment exists. It computes a maximum bipartite matching between specs
// and the candidates that satisfy them (augmenting-path search, ample for these
// pool sizes), so it succeeds whenever a feasible assignment exists -- a greedy
// pass can wrongly reject one by letting a flexible spec claim a node a pickier
// spec is the only taker for. Specs are processed most-constrained first, which
// does not affect whether a full matching is found but steers which feasible
// matching is chosen toward the intuitive one (a picky spec keeps its scarce
// node). The result is in spec order: assigned[i] satisfies specs[i]. It does
// not mutate candidates.
func matchSpecs(candidates []*Node, specs []NodeSpec) ([]*Node, bool) {
	// specOfCand[j] is the spec index currently holding candidate j, or -1.
	specOfCand := make([]int, len(candidates))
	for j := range specOfCand {
		specOfCand[j] = -1
	}
	candOfSpec := make([]int, len(specs))
	for i := range candOfSpec {
		candOfSpec[i] = -1
	}

	matched := 0
	for _, si := range constraintOrder(specs) {
		seen := make([]bool, len(candidates))
		if augmentSpec(si, specs, candidates, specOfCand, candOfSpec, seen) {
			matched++
		}
	}
	if matched != len(specs) {
		return nil, false
	}
	assigned := make([]*Node, len(specs))
	for i := range specs {
		assigned[i] = candidates[candOfSpec[i]]
	}
	return assigned, true
}

// augmentSpec tries to match spec si to a candidate, rerouting earlier matches
// along an augmenting path when the candidates si can use are already taken. It
// returns whether si ended up matched, updating specOfCand (candidate -> spec)
// and candOfSpec (spec -> candidate) in place. seen guards against revisiting a
// candidate within one search.
func augmentSpec(si int, specs []NodeSpec, candidates []*Node, specOfCand, candOfSpec []int, seen []bool) bool {
	for j, node := range candidates {
		if seen[j] || !specs[si].SatisfiedBy(node.Resources()) {
			continue
		}
		seen[j] = true
		// Take j if it is free, or if the spec currently holding it can move to
		// another candidate.
		if specOfCand[j] == -1 || augmentSpec(specOfCand[j], specs, candidates, specOfCand, candOfSpec, seen) {
			specOfCand[j] = si
			candOfSpec[si] = j
			return true
		}
	}
	return false
}

// constraintOrder returns spec indices ordered by decreasing constraint, so the
// pickiest specs are considered first. This is a preference heuristic for
// choosing among feasible matchings, not a correctness mechanism: matchSpecs
// finds a full assignment whenever one exists regardless of this order.
func constraintOrder(specs []NodeSpec) []int {
	order := make([]int, len(specs))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		return cmp.Compare(specScore(specs[b]), specScore(specs[a]))
	})
	return order
}

// specScore is a rough measure of how constrained a spec is: more required
// labels and set quantities rank higher. It only orders the preference search,
// so its coarseness does not affect which requests are satisfiable.
func specScore(s NodeSpec) int {
	score := len(s.Required.Labels)
	if _, ok := s.Required.CPUs.Get(); ok {
		score++
	}
	if _, ok := s.Required.MemoryMB.Get(); ok {
		score++
	}
	return score
}
