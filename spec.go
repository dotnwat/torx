//go:build unix

// Resource specifications: what a job asks of the nodes it allocates.
//
// A NodeSpec states the hard Required resources a node must provide, plus an
// ordered list of Preferred resource sets to try best-first; a PoolSpec is the
// multiset of node specs making up a whole job's request. Matching here is
// intentionally simple -- richer, heterogeneous allocation builds on these
// types without changing them. These are pure value types with no other torx
// dependencies.

package torx

// OS is the operating-system family a node runs. The empty value means
// unspecified; the allocator decides how to match it.
type OS string

const (
	Linux  OS = "linux"
	Darwin OS = "darwin"
)

// Optional is a value that may be unset. The zero value is unset. It models an
// "unspecified" field without the aliasing hazards of a pointer.
type Optional[T any] struct {
	value T
	set   bool
}

// Some returns an Optional holding v.
func Some[T any](v T) Optional[T] {
	return Optional[T]{value: v, set: true}
}

// Get returns the held value and whether it is set.
func (o Optional[T]) Get() (T, bool) {
	return o.value, o.set
}

// Labels is a set of free-form capability tags (e.g. "nvme", "bare-metal").
// The nil set is a valid empty set.
type Labels map[string]struct{}

// NewLabels builds a Labels set from tags.
func NewLabels(tags ...string) Labels {
	l := make(Labels, len(tags))
	for _, t := range tags {
		l[t] = struct{}{}
	}
	return l
}

// Has reports whether tag is present.
func (l Labels) Has(tag string) bool {
	_, ok := l[tag]
	return ok
}

// Subset reports whether every tag in l is also present in of.
func (l Labels) Subset(of Labels) bool {
	for t := range l {
		if !of.Has(t) {
			return false
		}
	}
	return true
}

// Resources describes the resources a node offers, or that a spec requires. An
// unset CPUs or MemoryMB means "unspecified": ignored as a requirement, and
// "unknown" when describing a node's capacity.
type Resources struct {
	CPUs     Optional[float64]
	MemoryMB Optional[int]
	Labels   Labels
}

// NodeSpec is the requirement for a single node. Only Required is checked by
// SatisfiedBy; OS and Preferred are matched by the allocator. Preferred lists
// progressively nicer resource sets, best first; an allocator may honor them
// but is only obligated to meet Required. Role is a human-facing label used in
// reporting.
type NodeSpec struct {
	OS        OS
	Required  Resources
	Preferred []Resources
	Role      string
}

// SatisfiedBy reports whether capacity meets this spec's hard Required
// resources. An unset requirement acts as a wildcard.
func (s NodeSpec) SatisfiedBy(capacity Resources) bool {
	if req, ok := s.Required.CPUs.Get(); ok {
		have, present := capacity.CPUs.Get()
		if !present || have < req {
			return false
		}
	}
	if req, ok := s.Required.MemoryMB.Get(); ok {
		have, present := capacity.MemoryMB.Get()
		if !present || have < req {
			return false
		}
	}
	return s.Required.Labels.Subset(capacity.Labels)
}

// PoolSpec is a whole job's node request: a multiset of NodeSpecs.
type PoolSpec struct {
	Nodes []NodeSpec
}

// Homogeneous builds a request for count identical nodes. A non-positive count
// yields an empty request.
func Homogeneous(count int, spec NodeSpec) PoolSpec {
	if count <= 0 {
		return PoolSpec{}
	}
	nodes := make([]NodeSpec, count)
	for i := range nodes {
		nodes[i] = spec
	}
	return PoolSpec{Nodes: nodes}
}

// Size returns the number of nodes requested.
func (p PoolSpec) Size() int {
	return len(p.Nodes)
}
