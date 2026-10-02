// Package linearize checks whether a history of operations on a shared
// object is linearizable: whether some total order of the operations, one
// that keeps every operation that returned before another began ahead of it,
// is a legal sequential run of a model of the object.
//
// Linearizability is what a system promises when it says an operation takes
// effect at one instant between its call and its return: a register whose
// reads see the last write, a key-value store that is "strongly consistent",
// an object store with read-after-write consistency. A job records each
// operation its clients issue -- what was asked, what came back, and when
// the call went out and the answer arrived, on one clock -- and Check
// searches for an order that explains every answer.
//
// An operation whose outcome is unknown -- it timed out, or its connection
// dropped -- may have taken effect or not, at any time after its call. Give
// it [Pending] as its return time and an output the model accepts in any
// state: the search may then place it anywhere after its call, or after
// everything else, where it explains nothing. The search relies on the
// model accepting such an operation in every state, and tries it only where
// it changes the state: anywhere else, it might as well come last.
//
// Check implements the algorithm of Wing and Gong, with Lowe's memoization
// of the configurations already explored (G. Lowe, "Testing for
// linearizability", 2017), as the Porcupine and Knossos checkers do. The
// problem is NP-complete, and the search can take exponential time on long
// histories with many concurrent operations. Two habits keep it fast: split a
// history into independent parts first, one per key of a key-value store
// ([Partition]), since a history is linearizable exactly when each part is;
// and bound each search with a context, and its memory with [CheckWithin],
// which turns a search that runs too long into an [Unknown] result rather
// than a hang. A model that says which unanswered operations are the same
// ([Model].Same) saves the search from trying them in every order.
package linearize

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
)

// Pending is the return time of an operation that never returned, or whose
// answer was lost: it may have taken effect at any time after its call.
const Pending int64 = math.MaxInt64

// Op is one operation in a history: what was called, what it returned, and
// when. Call and Return are read on one clock, in any unit, with Call <=
// Return; Return is Pending for an operation with no answer.
type Op[I, O any] struct {
	// Client is who issued the operation, for reports. Check does not
	// require one client's operations not to overlap.
	Client int
	Call   int64
	Return int64
	Input  I
	Output O
}

// Model is a sequential specification of an object: its initial state, and
// how one operation moves it from a state to the next. States must be
// comparable, since the search remembers the states it has been in.
type Model[S comparable, I, O any] struct {
	// Init returns the object's state before any operation.
	Init func() S
	// Step applies an operation with input in to state s. It returns
	// whether out is a legal output of that operation in s, and the state
	// after it. Step must be deterministic. An operation with an unknown
	// outcome should be legal in every state, taking effect as it would
	// have had it succeeded.
	Step func(s S, in I, out O) (ok bool, next S)
	// Same, if set, reports whether two operations that never returned
	// have the same effect in every state -- two deletes of one key, two
	// writes of one value -- so that the search need only try them in the
	// order of their calls. Histories with many operations whose answers
	// were lost search much faster with it.
	Same func(in1 I, out1 O, in2 I, out2 O) bool
}

// Outcome is the verdict of a check.
type Outcome int

const (
	// Ok means the history is linearizable.
	Ok Outcome = iota
	// Illegal means no order of the operations explains the history.
	Illegal
	// Unknown means the search was stopped, by its context or its
	// Limits, before it decided.
	Unknown
)

func (o Outcome) String() string {
	switch o {
	case Ok:
		return "ok"
	case Illegal:
		return "illegal"
	case Unknown:
		return "unknown"
	}
	return fmt.Sprintf("Outcome(%d)", int(o))
}

// Result is the verdict of a check, and for an illegal history, how far the
// search got.
type Result[S comparable] struct {
	Outcome Outcome
	// For an illegal history: Longest is the longest sequence of
	// operations, as indices into the history, that the search could
	// linearize; State is the model's state after it; and Stuck is the
	// operation that could not be placed after it before it returned --
	// the first operation, in return order, missing from Longest. The
	// operations that may follow Longest and could not are the ones that
	// contradict the history there: Stuck, and whatever was concurrent with
	// it.
	Longest []int
	State   S
	Stuck   int
	// Explored is how many configurations -- sets of operations
	// linearized, with the state they leave -- the search visited.
	Explored int
}

// ErrBadOp is returned by Check for an operation that returns before its
// call.
var ErrBadOp = errors.New("linearize: operation returns before its call")

// ErrLimit is returned, with an Unknown result, by a search that reached
// one of its Limits.
var ErrLimit = errors.New("linearize: the search reached its limit")

// Limits bound what a search may spend. A search that reaches one stops,
// with an Unknown result and ErrLimit.
type Limits struct {
	// Configurations is how many configurations the search may remember.
	// Each costs memory in proportion to the length of the history -- a
	// bit per operation, and the model's state -- and a search can visit
	// millions a second, so a long search with no bound can exhaust memory
	// before its context ends. Zero is no bound.
	Configurations int
}

// entry is a call or a return in the history, linked in time order.
type entry struct {
	op         int
	call       bool
	match      *entry // a call's return
	prev, next *entry
}

// Check reports whether ops is a linearizable history of model m. It returns
// a result with Outcome Unknown, and ctx's error, when ctx ends first.
func Check[S comparable, I, O any](ctx context.Context, m Model[S, I, O], ops []Op[I, O]) (Result[S], error) {
	return CheckWithin(ctx, m, ops, Limits{})
}

// CheckWithin is Check, bounded by lim as well as ctx.
func CheckWithin[S comparable, I, O any](ctx context.Context, m Model[S, I, O], ops []Op[I, O], lim Limits) (Result[S], error) {
	for i, op := range ops {
		if op.Return < op.Call {
			return Result[S]{Outcome: Unknown, Stuck: i}, fmt.Errorf("%w: op %d calls at %d, returns at %d", ErrBadOp, i, op.Call, op.Return)
		}
	}
	head := link(ops)
	n := len(ops)
	// after[i] is the operation, never returned and the same as i, called
	// last before i: i is tried only once it is linearized.
	after := make([]int, n)
	for i := range after {
		after[i] = -1
	}
	if m.Same != nil {
		var pending []int
		for i, op := range ops {
			if op.Return == Pending {
				pending = append(pending, i)
			}
		}
		slices.SortStableFunc(pending, func(a, b int) int { return cmp.Compare(ops[a].Call, ops[b].Call) })
		for k, i := range pending {
			for _, j := range slices.Backward(pending[:k]) {
				if m.Same(ops[j].Input, ops[j].Output, ops[i].Input, ops[i].Output) {
					after[i] = j
					break
				}
			}
		}
	}

	type frame struct {
		call  *entry
		state S
	}
	var (
		stack      []frame
		state      = m.Init()
		linearized = newBitset(n)
		seen       = cache[S]{}
		best       = Result[S]{Outcome: Illegal, State: state, Stuck: -1}
		bestDepth  = -1
		steps      int
	)
	seen.add(linearized, state)
	e := head.next
	for head.next != nil {
		if steps++; steps&0xfff == 0 && ctx.Err() != nil {
			return Result[S]{Outcome: Unknown, Explored: seen.size}, ctx.Err()
		}
		if lim.Configurations > 0 && seen.size > lim.Configurations {
			return Result[S]{Outcome: Unknown, Explored: seen.size}, ErrLimit
		}
		if e.call {
			op := &ops[e.op]
			if j := after[e.op]; j >= 0 && !linearized.has(j) {
				// The same operation, called before, is not placed yet:
				// any order with this one first is as well tried with
				// that one first.
				e = e.next
				continue
			}
			ok, next := m.Step(state, op.Input, op.Output)
			if ok && op.Return == Pending && next == state {
				// An operation that never returned, and would change
				// nothing here, is as well placed last, where it is legal
				// too: try the orders that leave it for later only.
				e = e.next
				continue
			}
			if ok {
				linearized.set(e.op)
				if seen.add(linearized, next) {
					stack = append(stack, frame{e, state})
					state = next
					lift(e)
					e = head.next
					continue
				}
				linearized.clear(e.op)
			}
			e = e.next
			continue
		}
		// The returns of operations that never returned come after every
		// other: every operation that did return is linearized, and the
		// rest can follow them in any order, legal in every state.
		if ops[e.op].Return == Pending {
			return Result[S]{Outcome: Ok, Explored: seen.size}, nil
		}
		// A return of an operation not yet linearized: nothing placed
		// after the operations linearized so far can come ahead of it.
		// Remember how far this branch got, and back up.
		if len(stack) > bestDepth {
			bestDepth = len(stack)
			best.Longest = best.Longest[:0]
			for _, f := range stack {
				best.Longest = append(best.Longest, f.call.op)
			}
			best.State, best.Stuck = state, e.op
		}
		if len(stack) == 0 {
			best.Explored = seen.size
			return best, nil
		}
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		state = f.state
		linearized.clear(f.call.op)
		unlift(f.call)
		e = f.call.next
	}
	return Result[S]{Outcome: Ok, Explored: seen.size}, nil
}

// link lays the history out as a list of calls and returns in time order,
// behind a sentinel head. A call and a return at the same time are taken as
// concurrent: the call comes first.
func link[I, O any](ops []Op[I, O]) *entry {
	type ev struct {
		t    int64
		call bool
		op   int
	}
	evs := make([]ev, 0, 2*len(ops))
	for i, op := range ops {
		evs = append(evs, ev{op.Call, true, i}, ev{op.Return, false, i})
	}
	slices.SortStableFunc(evs, func(a, b ev) int {
		if a.t != b.t {
			if a.t < b.t {
				return -1
			}
			return 1
		}
		if a.call != b.call {
			if a.call {
				return -1
			}
			return 1
		}
		return 0
	})
	head := &entry{op: -1}
	calls := make([]*entry, len(ops))
	prev := head
	for _, v := range evs {
		e := &entry{op: v.op, call: v.call, prev: prev}
		if v.call {
			calls[v.op] = e
		} else {
			calls[v.op].match = e
		}
		prev.next = e
		prev = e
	}
	return head
}

// lift takes a call and its return out of the list.
func lift(e *entry) {
	e.prev.next = e.next
	if e.next != nil {
		e.next.prev = e.prev
	}
	r := e.match
	r.prev.next = r.next
	if r.next != nil {
		r.next.prev = r.prev
	}
}

// unlift puts back a call and its return that lift took out.
func unlift(e *entry) {
	r := e.match
	r.prev.next = r
	if r.next != nil {
		r.next.prev = r
	}
	e.prev.next = e
	if e.next != nil {
		e.next.prev = e
	}
}

// Partition splits a history into the histories of independent objects, by
// the key each operation's input names: the keys of a key-value store, say.
// A history is linearizable exactly when each part is, and the parts are
// far cheaper to check one at a time.
func Partition[K comparable, I, O any](ops []Op[I, O], key func(I) K) map[K][]Op[I, O] {
	parts := map[K][]Op[I, O]{}
	for _, op := range ops {
		k := key(op.Input)
		parts[k] = append(parts[k], op)
	}
	return parts
}

// bitset is the set of operations linearized.
type bitset []uint64

func newBitset(n int) bitset { return make(bitset, (n+63)/64) }

func (b bitset) set(i int)      { b[i/64] |= 1 << (i % 64) }
func (b bitset) clear(i int)    { b[i/64] &^= 1 << (i % 64) }
func (b bitset) has(i int) bool { return b[i/64]&(1<<(i%64)) != 0 }

func (b bitset) hash() uint64 {
	h := uint64(0xcbf29ce484222325)
	for _, w := range b {
		h ^= w
		h *= 0x100000001b3
		h ^= h >> 29
	}
	return h
}

// cache is the configurations the search has visited.
type cache[S comparable] struct {
	m    map[uint64][]config[S]
	size int
}

type config[S comparable] struct {
	ops   bitset
	state S
}

// add records a configuration, and reports whether it is new.
func (c *cache[S]) add(ops bitset, state S) bool {
	if c.m == nil {
		c.m = map[uint64][]config[S]{}
	}
	h := ops.hash()
	for _, cf := range c.m[h] {
		if cf.state == state && slices.Equal(cf.ops, ops) {
			return false
		}
	}
	c.m[h] = append(c.m[h], config[S]{slices.Clone(ops), state})
	c.size++
	return true
}

// Weakening is how Minimize may weaken an operation: an operation weakened
// constrains a history no more than it did.
type Weakening int

const (
	// Keep says the operation cannot be weakened: it stays as it is, and
	// is never in a core.
	Keep Weakening = iota
	// Drop says the operation can be left out altogether, because it
	// changes nothing: a read, a write whose precondition failed.
	Drop
	// Replace says the operation can be replaced by a weaker one: a
	// conditional write that succeeded by the same write made
	// unconditionally, or any operation by one whose outcome is unknown,
	// with a Return of Pending.
	Replace
)

// Minimize shrinks an illegal history to a core that explains why: a few
// of its operations that, with every other operation weakened, still make
// it illegal, and of which none can be weakened as well. It returns the
// core's operations as indices into ops, in order. A history's violation
// is often a handful of operations among thousands -- a write, the read
// that saw it, and the read after that did not -- and the core is those.
//
// Weaken says how each operation may be weakened, and returns the weaker
// operation for Replace. Weakening only ever removes constraints, so a
// weakened history that is still illegal is illegal for a reason among the
// operations it kept whole. (Leaving out every operation instead would not
// do: a read of a value whose write was left out is illegal on its own.)
// Every operation left pending widens the search, so the fewer Replace
// leaves pending, the faster Minimize runs: a conditional write that
// succeeded is better replaced by an unconditional one, which constrains no
// more and keeps its return, than by one whose outcome is unknown.
//
// Minimize first leaves out what can be dropped, and then weakens the rest,
// each time weakening halves of the operations still kept, then quarters,
// and so on down to single operations (Zeller's delta debugging), so it
// runs Check some multiple of len(ops) times at worst. Proving a history
// illegal can take far longer than finding an order for a legal one, so
// each check gets at most a hundred times the time and the configurations
// the check of ops took, and at least a second and a million
// configurations; a history whose check runs out of either, or of ctx,
// counts as legal, and the core may then be larger than it need be.
// Minimize returns every index, and an error, if ops is not illegal or ctx
// ends before Check finds it is.
func Minimize[S comparable, I, O any](ctx context.Context, m Model[S, I, O], ops []Op[I, O], weaken func(Op[I, O]) (Op[I, O], Weakening)) ([]int, error) {
	weak := make([]Op[I, O], len(ops))
	how := make([]Weakening, len(ops))
	var droppable, replaceable, all []int
	for i, op := range ops {
		weak[i], how[i] = weaken(op)
		switch how[i] {
		case Drop:
			droppable = append(droppable, i)
		case Replace:
			replaceable = append(replaceable, i)
		}
		all = append(all, i)
	}
	budget := time.Duration(math.MaxInt64)
	var lim Limits
	explored := 0
	// illegal checks ops with every operation weakened but those in keep.
	illegal := func(keep []int) bool {
		var sub []Op[I, O]
		k := 0
		for i := range ops {
			switch {
			case how[i] == Keep:
				sub = append(sub, ops[i])
			case k < len(keep) && keep[k] == i:
				sub = append(sub, ops[i])
				k++
			case how[i] == Replace:
				sub = append(sub, weak[i])
			}
			for k < len(keep) && keep[k] <= i {
				k++
			}
		}
		cctx, cancel := context.WithTimeout(ctx, budget)
		defer cancel()
		res, _ := CheckWithin(cctx, m, sub, lim)
		explored = res.Explored
		return res.Outcome == Illegal
	}
	start := time.Now()
	if !illegal(all) {
		if err := ctx.Err(); err != nil {
			return all, err
		}
		return all, errors.New("linearize: the history is not illegal")
	}
	budget = max(time.Second, 100*time.Since(start))
	lim.Configurations = max(1<<20, 100*explored)
	// Leave out what can be left out first, which leaves the search no
	// wider; then weaken the rest.
	droppable = ddmin(ctx, droppable, func(sub []int) bool { return illegal(merge(sub, replaceable)) })
	replaceable = ddmin(ctx, replaceable, func(sub []int) bool { return illegal(merge(droppable, sub)) })
	return merge(droppable, replaceable), nil
}

// ddmin shrinks set, while illegal holds of it, to a subset of which no
// element can be removed with illegal still holding.
func ddmin(ctx context.Context, set []int, illegal func([]int) bool) []int {
	for chunk := max(len(set)/2, 1); len(set) > 0; {
		removed := false
		for start := 0; start < len(set) && ctx.Err() == nil; {
			end := min(start+chunk, len(set))
			rest := append(slices.Clone(set[:start]), set[end:]...)
			if illegal(rest) {
				set, removed = rest, true
				continue // the next chunk now starts where this one did
			}
			start = end
		}
		if ctx.Err() != nil || chunk == 1 && !removed {
			break
		}
		if chunk > 1 {
			chunk /= 2
		}
	}
	return set
}

// merge merges two sorted sets of indices.
func merge(a, b []int) []int {
	out := append(slices.Clone(a), b...)
	slices.Sort(out)
	return out
}
