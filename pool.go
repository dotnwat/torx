// Pool: the finite set of nodes a session owns, and per-job allocation.
//
// Allocate carves a SubPool out of the pool to satisfy a job's PoolSpec, all or
// nothing: either every node spec is matched to a distinct free node or the pool
// is left untouched and an error wrapping ErrAllocation is returned. Free gives
// a SubPool's nodes back. Matching honors each spec's hard Required resources
// (NodeSpec.SatisfiedBy) and assigns the most-constrained specs first, so a
// flexible spec does not claim a node a pickier one needs. OS matching is not
// yet enforced -- v1 pools are single-OS -- and is future work once nodes carry
// an OS.
package torx

import (
	"fmt"
	"sort"
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

// matchSpecs assigns a distinct candidate node to each spec, most-constrained
// first, so a flexible spec does not claim a node a pickier one needs. It
// returns the assignment in spec order (assigned[i] satisfies specs[i]) and
// whether every spec was matched. It does not mutate candidates.
func matchSpecs(candidates []*Node, specs []NodeSpec) ([]*Node, bool) {
	assigned := make([]*Node, len(specs))
	used := make([]bool, len(candidates))
	for _, si := range constraintOrder(specs) {
		spec := specs[si]
		found := -1
		for i, node := range candidates {
			if used[i] {
				continue
			}
			if spec.SatisfiedBy(node.Resources()) {
				found = i
				break
			}
		}
		if found < 0 {
			return nil, false
		}
		used[found] = true
		assigned[si] = candidates[found]
	}
	return assigned, true
}

// constraintOrder returns spec indices ordered by decreasing constraint, so the
// pickiest specs are matched first.
func constraintOrder(specs []NodeSpec) []int {
	order := make([]int, len(specs))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return specScore(specs[order[a]]) > specScore(specs[order[b]])
	})
	return order
}

// specScore is a rough measure of how constrained a spec is: more required
// labels and set quantities rank higher.
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
