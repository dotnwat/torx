// Package listappend checks a history of transactions over lists for the
// anomalies isolation levels forbid, after Elle (Kingsbury and Alvaro,
// "Elle: Inferring Isolation Anomalies from Experimental Observations").
//
// Each key holds a list. A transaction reads keys, getting their whole
// lists, and appends to keys, each value appended to a key unique to it.
// Because a list read shows every append that came before, in order, the
// reads of a key reveal the order its versions were written in -- the
// longest read is the version order, and every other read must be a
// prefix of it -- and from that order every dependency between
// transactions follows:
//
//   - ww: T2 appended the element right after T1's (T1 -> T2).
//   - wr: T2 read a list whose last element T1 appended (T1 -> T2).
//   - rw: T1 read a list that T2's append came right after: T2 overwrote
//     what T1 read (T1 -> T2), an anti-dependency.
//
// A committed append that no read saw came after every version one did,
// so it follows the last of them the same ways. And, optionally, realtime: T1 committed before T2 began (T1 -> T2).
// A cycle of dependencies is an anomaly; which cycles a level forbids is
// the level's definition. Serializability forbids every cycle;
// snapshot isolation allows only those with two rw edges in a row, so it
// forbids G0 (a cycle of ww), G1c (of ww and wr), G-single (exactly one
// rw), and G-nonadjacent (rw edges, no two adjacent). Check reports every
// kind it finds, each with a cycle as its witness; Forbidden says which a
// level forbids.
//
// Check also reports what needs no cycle: an aborted transaction's append
// read (G1a), a read of an intermediate state (G1b), reads of one key that
// disagree on its order or with the order a transaction appended to it
// in, an element read twice in a list, a transaction whose read lacks its
// own appends (internal) or shows others' differently than its earlier
// read did (nonrepeatable, which read committed allows), and -- with
// Realtime -- a committed append missing from a read that began after it
// committed.
package listappend

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// Status is what became of a transaction.
type Status int

const (
	// Committed: the transaction committed, and its reads are what it
	// read.
	Committed Status = iota
	// Aborted: the transaction did not commit, and none of its appends
	// took effect.
	Aborted
	// Unknown: the outcome is in doubt; its appends may have taken effect.
	// Its reads are not used.
	Unknown
)

// Mop is one operation of a transaction: a read of Key, which returned
// Read, or an append of Value to Key.
type Mop struct {
	Append bool
	Key    string
	Value  string
	Read   []string
}

// Txn is one transaction.
type Txn struct {
	// ID names the transaction in reports.
	ID int
	// Process is who ran it: transactions of a process run one at a time.
	Process int
	// Call and Return are when it began and when its outcome was known,
	// on one clock; used only with Realtime.
	Call, Return int64
	Status       Status
	Mops         []Mop
}

// Options configure Check.
type Options struct {
	// Realtime adds an edge from each committed transaction to every
	// transaction that began after it returned -- the order strict
	// serializability adds -- and reports a committed append missing from
	// a read that began after it committed.
	Realtime bool
	// Process adds an edge from each transaction to its process's next.
	Process bool
}

// Edge kinds.
const (
	WW       = "ww"
	WR       = "wr"
	RW       = "rw"
	Realtime = "realtime"
	Process  = "process"
)

// Anomaly is one anomaly found.
type Anomaly struct {
	// Kind is the anomaly: "G0", "G1a", "G1b", "G1c", "G-single",
	// "G-nonadjacent", "G2", "incompatible-order", "duplicate",
	// "internal", "nonrepeatable", "lost".
	Kind string
	// Txns are the transactions involved, by ID; for a cycle, in cycle
	// order, with Edges[i] the kind of the edge from Txns[i] to the next.
	Txns  []int
	Edges []string
	Key   string
	// Detail says what was seen.
	Detail string
}

// Forbidden returns the anomaly kinds a level forbids: "serializable",
// "strict-serializable", "snapshot-isolation", or "read-committed".
func Forbidden(level string) []string {
	base := []string{"G0", "G1a", "G1b", "G1c", "incompatible-order", "duplicate", "internal"}
	switch level {
	case "read-committed":
		return base
	case "snapshot-isolation":
		return append(base, "nonrepeatable", "G-single", "G-nonadjacent")
	case "serializable", "strict-serializable":
		return append(base, "nonrepeatable", "G-single", "G-nonadjacent", "G2", "lost")
	}
	return nil
}

type edge struct {
	to   int
	kind string
}

type checker struct {
	txns   []Txn
	index  map[int]int // ID -> position
	opts   Options
	writer map[string]map[string]int // key -> value -> position of the txn that appended it
	// appended is what each transaction appended to each key, in the order
	// it did: key -> position of the txn -> values.
	appended map[string]map[int][]string
	order    map[string][]string // key -> version order
	adj      [][]edge
	out      []Anomaly
}

// Check checks a history.
func Check(txns []Txn, opts Options) []Anomaly {
	c := &checker{txns: txns, index: map[int]int{}, opts: opts, writer: map[string]map[string]int{}, appended: map[string]map[int][]string{}, order: map[string][]string{}}
	for i, t := range txns {
		c.index[t.ID] = i
	}
	c.appends()
	c.internal()
	c.orders()
	c.reads()
	c.adj = make([][]edge, len(txns))
	c.edges()
	c.cycles()
	return c.out
}

func (c *checker) report(a Anomaly) { c.out = append(c.out, a) }

// appends records who appended each value, and what each transaction
// appended.
func (c *checker) appends() {
	for i, t := range c.txns {
		for _, m := range t.Mops {
			if !m.Append {
				continue
			}
			if c.writer[m.Key] == nil {
				c.writer[m.Key] = map[string]int{}
				c.appended[m.Key] = map[int][]string{}
			}
			c.appended[m.Key][i] = append(c.appended[m.Key][i], m.Value)
			if j, ok := c.writer[m.Key][m.Value]; ok && j != i {
				c.report(Anomaly{Kind: "duplicate", Key: m.Key, Txns: []int{c.txns[j].ID, t.ID},
					Detail: fmt.Sprintf("value %s appended to %s twice", m.Value, m.Key)})
			}
			c.writer[m.Key][m.Value] = i
		}
	}
}

// internal checks each committed transaction's reads against its own
// appends and its earlier reads of the key. A read must end with what the
// transaction has appended to the key so far, or it is "internal": no
// level lets a transaction miss its own writes. What comes before that is
// what others appended, and a later read that shows it differently than an
// earlier one did is "nonrepeatable": read committed allows it, another
// transaction having committed in between; the levels above do not.
func (c *checker) internal() {
	for _, t := range c.txns {
		if t.Status != Committed {
			continue
		}
		own := map[string][]string{}    // what the txn has appended to each key so far
		others := map[string][]string{} // what its last read of each key showed of others' appends
		read := map[string]bool{}
		for _, m := range t.Mops {
			if m.Append {
				own[m.Key] = append(own[m.Key], m.Value)
				continue
			}
			mine := own[m.Key]
			if len(m.Read) < len(mine) || !slices.Equal(m.Read[len(m.Read)-len(mine):], mine) {
				c.report(Anomaly{Kind: "internal", Key: m.Key, Txns: []int{t.ID},
					Detail: fmt.Sprintf("txn %d appended %v to %s, then read %v", t.ID, mine, m.Key, m.Read)})
				continue
			}
			rest := m.Read[:len(m.Read)-len(mine)]
			if read[m.Key] && !slices.Equal(rest, others[m.Key]) {
				c.report(Anomaly{Kind: "nonrepeatable", Key: m.Key, Txns: []int{t.ID},
					Detail: fmt.Sprintf("txn %d read %s as %v of others' appends, having read %v", t.ID, m.Key, rest, others[m.Key])})
			}
			read[m.Key] = true
			others[m.Key] = rest
		}
	}
}

// external returns a committed transaction's external reads: each key's
// first read, if it came before any append of the key.
func external(t Txn) []Mop {
	var out []Mop
	seen := map[string]bool{}
	for _, m := range t.Mops {
		if seen[m.Key] {
			continue
		}
		seen[m.Key] = true
		if !m.Append {
			out = append(out, m)
		}
	}
	return out
}

// orders infers each key's version order from the longest read of it,
// and reports reads that are not prefixes of it.
func (c *checker) orders() {
	reads := map[string][][]string{}
	who := map[string][]int{}
	for _, t := range c.txns {
		if t.Status != Committed {
			continue
		}
		for _, m := range t.Mops {
			if !m.Append {
				reads[m.Key] = append(reads[m.Key], m.Read)
				who[m.Key] = append(who[m.Key], t.ID)
			}
		}
	}
	for k, rs := range reads {
		longest := 0
		for i, r := range rs {
			if len(r) > len(rs[longest]) {
				longest = i
			}
			dup := map[string]bool{}
			for _, v := range r {
				if dup[v] {
					c.report(Anomaly{Kind: "duplicate", Key: k, Txns: []int{who[k][i]},
						Detail: fmt.Sprintf("txn %d read %s with %s twice: %v", who[k][i], k, v, r)})
				}
				dup[v] = true
			}
		}
		order := rs[longest]
		for i, r := range rs {
			if len(r) > len(order) || !slices.Equal(order[:len(r)], r) {
				c.report(Anomaly{Kind: "incompatible-order", Key: k, Txns: []int{who[k][i], who[k][longest]},
					Detail: fmt.Sprintf("txn %d read %s as %v; txn %d as %v", who[k][i], k, r, who[k][longest], order)})
			}
		}
		c.order[k] = order
	}
}

// reads checks what each read saw of what others appended, whether or not
// its transaction had appended to the key by then: no aborted appends, no
// intermediate states, no appends nobody made, none out of the order
// their transaction made them in or without those it made before, and
// with Realtime nothing committed before it began missing.
func (c *checker) reads() {
	for i, t := range c.txns {
		if t.Status != Committed {
			continue
		}
		// Every read is checked, each element once: one that follows the
		// transaction's own append of the key shows what others appended
		// as much as one that came before it.
		checked := map[[2]string]bool{}
		first := map[string]bool{}
		type appender struct {
			key string
			txn int
		}
		disordered := map[appender]bool{}
		for _, m := range t.Mops {
			if m.Append {
				continue
			}
			for _, v := range m.Read {
				if checked[[2]string{m.Key, v}] {
					continue
				}
				checked[[2]string{m.Key, v}] = true
				w, ok := c.writer[m.Key][v]
				if !ok {
					c.report(Anomaly{Kind: "G1a", Key: m.Key, Txns: []int{t.ID},
						Detail: fmt.Sprintf("txn %d read %s, which no transaction appended to %s", t.ID, v, m.Key)})
					continue
				}
				if c.txns[w].Status == Aborted {
					c.report(Anomaly{Kind: "G1a", Key: m.Key, Txns: []int{c.txns[w].ID, t.ID},
						Detail: fmt.Sprintf("txn %d read %s, appended to %s by aborted txn %d", t.ID, v, m.Key, c.txns[w].ID)})
				}
			}
			// A list holds a transaction's appends in the order it made
			// them, none there without those before it.
			var theirs map[int][]string
			for _, v := range m.Read {
				if w, ok := c.writer[m.Key][v]; ok && w != i && len(c.appended[m.Key][w]) > 1 {
					if theirs == nil {
						theirs = map[int][]string{}
					}
					theirs[w] = append(theirs[w], v)
				}
			}
			for _, w := range slices.Sorted(maps.Keys(theirs)) {
				got, made := theirs[w], c.appended[m.Key][w]
				n := min(len(got), len(made))
				if !slices.Equal(got[:n], made[:n]) && !disordered[appender{m.Key, w}] {
					disordered[appender{m.Key, w}] = true
					c.report(Anomaly{Kind: "incompatible-order", Key: m.Key, Txns: []int{c.txns[w].ID, t.ID},
						Detail: fmt.Sprintf("txn %d read %s as %v, with %v of what txn %d appended, in order, as %v", t.ID, m.Key, m.Read, got, c.txns[w].ID, made)})
				}
			}
			// The rest is checked in the transaction's first read of the
			// key.
			if first[m.Key] {
				continue
			}
			first[m.Key] = true
			// What others wrote ends before the transaction's own appends.
			others := m.Read
			for len(others) > 0 {
				if w, ok := c.writer[m.Key][others[len(others)-1]]; !ok || w != i {
					break
				}
				others = others[:len(others)-1]
			}
			if len(others) > 0 {
				last := others[len(others)-1]
				if w, ok := c.writer[m.Key][last]; ok {
					if mine := c.appended[m.Key][w]; mine[len(mine)-1] != last {
						c.report(Anomaly{Kind: "G1b", Key: m.Key, Txns: []int{c.txns[w].ID, t.ID},
							Detail: fmt.Sprintf("txn %d read %s ending in %s, an intermediate append of txn %d, which went on to append %v", t.ID, m.Key, last, c.txns[w].ID, mine)})
					}
				}
			}
			if c.opts.Realtime {
				have := map[string]bool{}
				for _, v := range m.Read {
					have[v] = true
				}
				for v, w := range c.writer[m.Key] {
					wt := c.txns[w]
					if wt.Status == Committed && wt.Return < t.Call && !have[v] && wt.ID != t.ID {
						c.report(Anomaly{Kind: "lost", Key: m.Key, Txns: []int{wt.ID, t.ID},
							Detail: fmt.Sprintf("txn %d committed %s to %s before txn %d began, which read %v", wt.ID, v, m.Key, t.ID, m.Read)})
					}
				}
			}
		}
	}
}

// edges builds the dependency graph over committed transactions, and
// transactions in doubt whose appends some read saw.
func (c *checker) edges() {
	add := func(from, to int, kind string) {
		if from == to {
			return
		}
		if !slices.ContainsFunc(c.adj[from], func(e edge) bool { return e.to == to && e.kind == kind }) {
			c.adj[from] = append(c.adj[from], edge{to, kind})
		}
	}
	for k, order := range c.order {
		for i := 1; i < len(order); i++ {
			a, ok1 := c.writer[k][order[i-1]]
			b, ok2 := c.writer[k][order[i]]
			if ok1 && ok2 {
				add(a, b, WW)
			}
		}
	}
	latest := map[string][]int{} // key -> the transactions that read its whole version order
	for i, t := range c.txns {
		if t.Status != Committed {
			continue
		}
		for _, m := range external(t) {
			if len(m.Read) > 0 {
				if w, ok := c.writer[m.Key][m.Read[len(m.Read)-1]]; ok {
					add(w, i, WR)
				}
			}
			order := c.order[m.Key]
			if len(m.Read) < len(order) && slices.Equal(order[:len(m.Read)], m.Read) {
				if w, ok := c.writer[m.Key][order[len(m.Read)]]; ok {
					add(i, w, RW)
				}
			}
			if slices.Equal(order, m.Read) {
				latest[m.Key] = append(latest[m.Key], i)
			}
		}
	}
	// A committed append no read saw came after every version one did, so
	// its writer follows the writer of the last of those (ww) and whoever
	// read it (rw); a read of an earlier version gets there through them.
	// Without these a history nobody read the end of -- write skew with no
	// later reader -- would have no anti-dependencies at all.
	observed := map[string]map[string]bool{}
	for k, order := range c.order {
		observed[k] = map[string]bool{}
		for _, v := range order {
			observed[k][v] = true
		}
	}
	for i, t := range c.txns {
		if t.Status != Committed {
			continue
		}
		for _, m := range t.Mops {
			if !m.Append || observed[m.Key][m.Value] {
				continue
			}
			if order := c.order[m.Key]; len(order) > 0 {
				if w, ok := c.writer[m.Key][order[len(order)-1]]; ok {
					add(w, i, WW)
				}
			}
			for _, reader := range latest[m.Key] {
				add(reader, i, RW)
			}
		}
	}
	if c.opts.Realtime {
		// Each transaction to those that began after it returned -- only to
		// the first of them, those that began before any of them returned,
		// since the rest follow from those -- so the edges stay few.
		for i, t := range c.txns {
			if t.Status != Committed {
				continue
			}
			// Link t to the transactions that began after it returned and
			// before any other that also began after it returned had.
			var next []int
			first := int64(-1)
			for j, u := range c.txns {
				if u.Call > t.Return && u.Status == Committed {
					if first < 0 || u.Return < first {
						first = u.Return
					}
					next = append(next, j)
				}
			}
			for _, j := range next {
				if c.txns[j].Call <= first {
					add(i, j, Realtime)
				}
			}
		}
	}
	if c.opts.Process {
		last := map[int]int{}
		for i, t := range c.txns {
			if p, ok := last[t.Process]; ok {
				add(p, i, Process)
			}
			last[t.Process] = i
		}
	}
}

// cycles finds the cycles each level cares about, one witness of each kind
// per strongly connected component.
func (c *checker) cycles() {
	all := func(string) bool { return true }
	noRW := func(k string) bool { return k != RW }
	wwOnly := func(k string) bool { return k == WW }
	for _, comp := range c.sccs(all) {
		in := map[int]bool{}
		for _, v := range comp {
			in[v] = true
		}
		found := false
		if cyc, kinds := c.cycleWithin(comp, in, wwOnly); cyc != nil {
			c.reportCycle("G0", cyc, kinds)
			found = true
		} else if cyc, kinds := c.cycleWithin(comp, in, noRW); cyc != nil {
			c.reportCycle("G1c", cyc, kinds)
			found = true
		}
		if cyc, kinds := c.gSingle(comp, in); cyc != nil {
			c.reportCycle("G-single", cyc, kinds)
			found = true
		} else if cyc, kinds := c.nonadjacent(comp, in); cyc != nil {
			c.reportCycle("G-nonadjacent", cyc, kinds)
			found = true
		}
		if !found {
			if cyc, kinds := c.cycleWithin(comp, in, all); cyc != nil {
				c.reportCycle("G2", cyc, kinds)
			}
		}
	}
}

func (c *checker) reportCycle(kind string, cyc []int, kinds []string) {
	ids := make([]int, len(cyc))
	var b strings.Builder
	for i, v := range cyc {
		ids[i] = c.txns[v].ID
		fmt.Fprintf(&b, "%d -%s-> ", ids[i], kinds[i])
	}
	fmt.Fprintf(&b, "%d", ids[0])
	c.report(Anomaly{Kind: kind, Txns: ids, Edges: kinds, Detail: b.String()})
}

// sccs returns the strongly connected components with more than one
// transaction, over edges whose kinds ok accepts (Tarjan's algorithm,
// iterative).
func (c *checker) sccs(ok func(string) bool) [][]int {
	n := len(c.txns)
	index := make([]int, n)
	low := make([]int, n)
	on := make([]bool, n)
	for i := range index {
		index[i] = -1
	}
	var out [][]int
	var st []int
	next := 0
	type frame struct{ v, e int }
	for root := range n {
		if index[root] >= 0 {
			continue
		}
		call := []frame{{root, 0}}
		index[root], low[root] = next, next
		next++
		st = append(st, root)
		on[root] = true
		for len(call) > 0 {
			f := &call[len(call)-1]
			if f.e < len(c.adj[f.v]) {
				e := c.adj[f.v][f.e]
				f.e++
				if !ok(e.kind) {
					continue
				}
				w := e.to
				if index[w] < 0 {
					index[w], low[w] = next, next
					next++
					st = append(st, w)
					on[w] = true
					call = append(call, frame{w, 0})
				} else if on[w] {
					low[f.v] = min(low[f.v], index[w])
				}
				continue
			}
			v := f.v
			call = call[:len(call)-1]
			if len(call) > 0 {
				p := call[len(call)-1].v
				low[p] = min(low[p], low[v])
			}
			if low[v] == index[v] {
				var comp []int
				for {
					w := st[len(st)-1]
					st = st[:len(st)-1]
					on[w] = false
					comp = append(comp, w)
					if w == v {
						break
					}
				}
				if len(comp) > 1 {
					slices.Sort(comp)
					out = append(out, comp)
				}
			}
		}
	}
	return out
}

// path finds a shortest path from src to dst inside the component, over
// edges ok accepts, returning its vertices (src first, dst excluded) and
// edge kinds.
func (c *checker) path(src, dst int, in map[int]bool, ok func(string) bool) ([]int, []string) {
	type hop struct {
		prev int
		kind string
	}
	seen := map[int]hop{src: {-1, ""}}
	q := []int{src}
	for len(q) > 0 {
		v := q[0]
		q = q[1:]
		for _, e := range c.adj[v] {
			if !ok(e.kind) || !in[e.to] {
				continue
			}
			if e.to == dst {
				// Walk back.
				verts := []int{v}
				kinds := []string{e.kind}
				for x := v; seen[x].prev >= 0; x = seen[x].prev {
					verts = append(verts, seen[x].prev)
					kinds = append(kinds, seen[x].kind)
				}
				slices.Reverse(verts)
				slices.Reverse(kinds)
				return verts, kinds
			}
			if _, ok := seen[e.to]; !ok {
				seen[e.to] = hop{v, e.kind}
				q = append(q, e.to)
			}
		}
	}
	return nil, nil
}

// cycleWithin finds a short cycle inside the component over edges ok
// accepts.
func (c *checker) cycleWithin(comp []int, in map[int]bool, ok func(string) bool) ([]int, []string) {
	var best []int
	var bestKinds []string
	for _, v := range comp {
		cyc, kinds := c.path(v, v, in, ok)
		if cyc != nil && (best == nil || len(cyc) < len(best)) {
			best, bestKinds = cyc, kinds
			if len(best) == 2 {
				break
			}
		}
	}
	return best, bestKinds
}

// gSingle finds a cycle with exactly one rw edge: an rw edge a -> b with a
// path from b back to a without rw edges.
func (c *checker) gSingle(comp []int, in map[int]bool) ([]int, []string) {
	noRW := func(k string) bool { return k != RW }
	for _, a := range comp {
		for _, e := range c.adj[a] {
			if e.kind != RW || !in[e.to] {
				continue
			}
			back, kinds := c.path(e.to, a, in, noRW)
			if back != nil {
				return append([]int{a}, back...), append([]string{RW}, kinds...)
			}
		}
	}
	return nil, nil
}

// nonadjacent finds a cycle with rw edges, no two of them in a row: a
// cycle in the graph of (transaction, arrived by rw) states, where an rw
// edge cannot leave a state arrived at by one.
func (c *checker) nonadjacent(comp []int, in map[int]bool) ([]int, []string) {
	type state struct {
		v  int
		rw bool
	}
	for _, start := range comp {
		for _, startRW := range []bool{false, true} {
			s0 := state{start, startRW}
			type hop struct {
				prev state
				kind string
			}
			seen := map[state]hop{}
			q := []state{s0}
			for len(q) > 0 {
				s := q[0]
				q = q[1:]
				for _, e := range c.adj[s.v] {
					if !in[e.to] || (e.kind == RW && s.rw) {
						continue
					}
					ns := state{e.to, e.kind == RW}
					if ns == s0 {
						// Rebuild the walk; keep it if it has an rw edge and
						// visits no transaction twice.
						verts := []int{s.v}
						kinds := []string{e.kind}
						for x := s; x != s0; x = seen[x].prev {
							verts = append(verts, seen[x].prev.v)
							kinds = append(kinds, seen[x].kind)
						}
						slices.Reverse(verts)
						slices.Reverse(kinds)
						if slices.Contains(kinds, RW) && distinct(verts) {
							return verts, kinds
						}
						continue
					}
					if _, ok := seen[ns]; !ok {
						seen[ns] = hop{s, e.kind}
						q = append(q, ns)
					}
				}
			}
		}
	}
	return nil, nil
}

func distinct(vs []int) bool {
	seen := map[int]bool{}
	for _, v := range vs {
		if seen[v] {
			return false
		}
		seen[v] = true
	}
	return true
}
