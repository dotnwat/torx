// Package prefixcheck checks copies of a primary's history -- the backups,
// replicas, and restores of a database, a follower's copy of a leader's
// log -- against what the primary committed and what it acknowledged as
// copied.
//
// A primary commits entries one after another, each at a position of its
// own, 1, 2, ...; a system that replicates or backs it up asynchronously
// makes copies that lag it, and promises three things of them, which Check
// holds a history to:
//
//   - not-a-prefix: a copy holds a prefix of a sequence the primary
//     committed -- every entry it holds was committed, at the position it
//     holds it at, and none is missing before the last -- not a state the
//     primary never had;
//   - lost-acked: a copy of the latest state holds every entry a sync
//     acknowledged before the copy was begun -- what a "sync and wait", a
//     replication ack, or a completed backup says is safe;
//   - went-back: a copy holds every entry of any copy of its group that
//     was done before it began: copies of the latest state do not go back
//     in time, nor do the reads of one follower.
//
// A primary that fails over starts a branch of history: a new primary
// restored from a copy carries on from the copy's sequence, which is the
// new branch's first entries, and what the old primary committed after
// them is not on it. Branches are numbered from 0, in the order they
// began; each knows its entries at the positions the job learned them.
// An entry whose outcome is in doubt -- its commit's answer lost, the
// primary gone -- may stand at any one position a branch does not know,
// on its own branch or a later one, unless the branch knows it at
// another.
//
// What stands at a position a branch does not know, the copies say, and
// those that only that branch could hold must say the same: two that
// differ there are not both prefixes of what it committed. Which of them
// is not, the check cannot tell, so it reports the two together, once
// for the position, as not-a-prefix.
//
// A copy more than one branch could hold says nothing itself, since which
// branch it is of is not known, but is held to what the others say: one
// that differs from them on every branch it could be of is of none, and
// is reported with the copies it differs from. Such copies are not held
// to one another, and that is a limit of the check: three that differ at
// a position two branches do not know cannot all be prefixes, and pass.
// To tell would take trying each copy on each of its branches.
//
// A copy already known to be wrong for a reason of the caller's -- an
// issue found before, allowed so a run finds others -- is checked on its
// own and its anomalies carry the reason, but no other copy is compared
// to it, so it hides nothing else. Nor is one compared to a copy found
// not to be a prefix, which is reported once.
package prefixcheck

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// History is what a job recorded.
type History[ID comparable] struct {
	// Branches are what each branch of the primary's history committed:
	// position -> entry, from 1, as far as the job knows. A branch begun
	// by a failover holds the entries it began with at positions 1 to the
	// base's length.
	Branches []map[int]ID
	// Doubt are the entries whose outcome is in doubt, with the branch
	// each was committed on, if at all.
	Doubt map[ID]int
	Acks  []Ack
	// Copies are the copies the job read. Their IDs name them in the
	// anomalies.
	Copies []Copy[ID]
}

// Ack is an acknowledgement that the copies hold positions 1 through
// Prefix of a branch: a sync that returned at Return. An Ack whose
// Prefix is 0 says nothing.
type Ack struct {
	Branch int
	Prefix int
	Return time.Time
}

// Copy is one copy of the primary's history the job read: its entries in
// order, and when it was begun and done.
type Copy[ID comparable] struct {
	ID           int
	Seq          []ID
	Call, Return time.Time
	// Latest says the copy is of the latest state, which every ack before
	// Call must be in, and which is in the group "latest".
	Latest bool
	// Group, if set, names copies that must never go back: each holds
	// every copy of the group done before it began. A follower's reads,
	// say. A copy that names the group "latest" without being Latest is
	// held to the copies of the latest state, and not to the acks.
	Group string
	// Known, if set, is why the copy is already known to be wrong. It is
	// checked on its own, its anomalies carry Known, and no other copy is
	// compared to it.
	Known string
}

// The kinds of Anomaly.
const (
	NotAPrefix = "not-a-prefix"
	LostAcked  = "lost-acked"
	WentBack   = "went-back"
)

// Anomaly is something a copy contradicts.
type Anomaly[ID comparable] struct {
	Kind string
	// Copies are the copies concerned, by ID: the copy, and for
	// went-back, the earlier copy it does not hold. A not-a-prefix with
	// more than one is of copies that differ where their branch knows
	// neither's entry: the copy, and the one that held another entry
	// there first -- one for each branch the copy follows, if it follows
	// several, the latest branch's first.
	Copies []int
	// Known is the Known of a copy concerned, if any.
	Known string
	// Branch and At are, for not-a-prefix, the branch the copy follows
	// longest and the first position where it does not; for one of
	// copies that differ, the copy's branch -- the latest it follows --
	// and the position it differs at from that branch's copy.
	Branch, At int
	// Missing are, for lost-acked and went-back, the entries the copy
	// lacks.
	Missing []ID
	// Ack is, for lost-acked, the latest-ending ack the copy contradicts.
	Ack  *Ack
	Text string
}

func (a Anomaly[ID]) String() string { return a.Kind + ": " + a.Text }

type checker[ID comparable] struct {
	h      History[ID]
	byID   map[int]*Copy[ID]
	branch map[int]int // copy -> the branch it is a prefix of, -1 if none
	// known is, by branch, where the branch knows each of its entries,
	// made when first asked for.
	known []map[ID]int
	out   []Anomaly[ID]
}

// Check checks h and returns what its copies contradict.
func Check[ID comparable](h History[ID]) []Anomaly[ID] {
	c := &checker[ID]{h: h, byID: map[int]*Copy[ID]{}, branch: map[int]int{}, known: make([]map[ID]int, len(h.Branches))}
	for i := range h.Copies {
		c.byID[h.Copies[i].ID] = &h.Copies[i]
	}
	c.prefixes()
	c.acks()
	c.groups()
	return c.out
}

func (c *checker[ID]) add(a Anomaly[ID]) {
	for _, id := range a.Copies {
		if k := c.byID[id].Known; k != "" {
			a.Known = k
			break
		}
	}
	c.out = append(c.out, a)
}

// Consistent returns the first position, from 1, at which seq does not
// follow branch b: where b knows another entry, or where b does not know
// the position and the entry there cannot stand at it -- it is not one in
// doubt on b or an earlier branch, b knows it at another position, or seq
// holds it already, and it was committed once if at all. It returns 0 if
// seq follows b throughout.
func (h History[ID]) Consistent(seq []ID, b int) int {
	var known map[ID]int
	leaves, _ := h.follow(seq, b, func() map[ID]int {
		if known == nil {
			known = h.positions(b)
		}
		return known
	})
	return leaves
}

// positions is where branch b knows each of its entries: entry ->
// position.
func (h History[ID]) positions(b int) map[ID]int {
	at := make(map[ID]int, len(h.Branches[b]))
	for pos, id := range h.Branches[b] {
		at[id] = pos
	}
	return at
}

// follow is Consistent: it returns the first position at which seq leaves
// branch b, or 0, and the positions before it at which seq holds an entry
// in doubt where b knows of none. known returns b's positions; it is
// asked only once seq holds an entry in doubt at such a position, which
// most never do.
func (h History[ID]) follow(seq []ID, b int, known func() map[ID]int) (leaves int, doubts []int) {
	// The entries in doubt seq holds where b knows of none. No other can
	// be held twice: an entry stands where b knows it, which is one
	// position, or at one of these.
	var held map[ID]bool
	for i, id := range seq {
		pos := i + 1
		if want, ok := h.Branches[b][pos]; ok {
			if want != id {
				return pos, doubts
			}
			continue
		}
		if db, ok := h.Doubt[id]; !ok || db > b {
			return pos, doubts
		}
		if _, elsewhere := known()[id]; elsewhere || held[id] {
			return pos, doubts
		}
		if held == nil {
			held = map[ID]bool{}
		}
		held[id] = true
		doubts = append(doubts, pos)
	}
	return 0, doubts
}

// BranchOf is the latest branch seq is a prefix of, or -1.
func (h History[ID]) BranchOf(seq []ID) int {
	for b := range slices.Backward(h.Branches) {
		if h.Consistent(seq, b) == 0 {
			return b
		}
	}
	return -1
}

// follow is History.follow, with the branch's positions kept from one
// call to the next.
func (c *checker[ID]) follow(seq []ID, b int) (leaves int, doubts []int) {
	return c.h.follow(seq, b, func() map[ID]int { return c.knownOn(b) })
}

func (c *checker[ID]) knownOn(b int) map[ID]int {
	if c.known[b] == nil {
		c.known[b] = c.h.positions(b)
	}
	return c.known[b]
}

// claim is the entry a copy holds at a position its branch knows no entry
// for: what the branch is taken to have committed there.
type claim[ID comparable] struct {
	id   ID
	copy int
	// disputed says a copy that held another entry there was reported.
	disputed bool
}

// place is a branch a copy follows, and the positions at which the copy
// holds an entry in doubt where the branch knows of none.
type place struct {
	branch int
	doubts []int
}

// undecided is a copy more than one branch could hold, and the branches,
// the latest first.
type undecided[ID comparable] struct {
	cp     *Copy[ID]
	places []place
}

// difference is an entry a copy holds where its branch's copies hold
// another.
type difference[ID comparable] struct {
	branch, at int
	id         ID
}

func (c *checker[ID]) prefixes() {
	// claims are, by branch and position, the first claim made there.
	claims := make([]map[int]*claim[ID], len(c.h.Branches))
	// differs is where on a branch a copy first holds another entry than
	// was claimed, and the claim; 0 if nowhere.
	differs := func(cp *Copy[ID], pl place) (int, *claim[ID]) {
		for _, pos := range pl.doubts {
			if was, ok := claims[pl.branch][pos]; ok && was.id != cp.Seq[pos-1] {
				return pos, was
			}
		}
		return 0, nil
	}
	var several []undecided[ID]
	for i := range c.h.Copies {
		cp := &c.h.Copies[i]
		// The branches the copy follows, the latest first: that one is
		// its branch.
		var places []place
		for x := range slices.Backward(c.h.Branches) {
			if leaves, doubts := c.follow(cp.Seq, x); leaves == 0 {
				places = append(places, place{x, doubts})
			}
		}
		if len(places) == 0 {
			c.branch[cp.ID] = -1
			c.notAPrefix(*cp)
			continue
		}
		b := places[0].branch
		c.branch[cp.ID] = b
		switch {
		case cp.Known != "":
			// Known to be wrong, and compared to no other.
			continue
		case len(places) > 1:
			// It may differ from the copies of one of its branches and
			// be of another: it claims nothing, and is held, below, to
			// what the others have.
			several = append(several, undecided[ID]{cp, places})
			continue
		}
		pos, was := differs(cp, places[0])
		if pos == 0 {
			if claims[b] == nil {
				claims[b] = map[int]*claim[ID]{}
			}
			for _, pos := range places[0].doubts {
				if _, ok := claims[b][pos]; !ok {
					claims[b][pos] = &claim[ID]{id: cp.Seq[pos-1], copy: cp.ID}
				}
			}
			continue
		}
		// One of the two is not a prefix. It claims nothing further, and
		// the difference is reported once, however many copies share it.
		if !was.disputed {
			was.disputed = true
			c.add(Anomaly[ID]{Kind: NotAPrefix, Copies: []int{cp.ID, was.copy}, Branch: b, At: pos,
				Text: fmt.Sprintf("copy %d holds %v at position %d, where copy %d holds %v: branch %d, the only branch either follows, committed one entry there at most, and knows of neither",
					cp.ID, cp.Seq[pos-1], pos, was.copy, was.id, b)})
		}
	}

	// A copy more than one branch could hold is of one of them, and holds
	// what that one's copies do. One that differs from the copies of every
	// branch it follows is of none of them.
	told := map[difference[ID]]bool{}
	for _, u := range several {
		copies, at := []int{u.cp.ID}, 0
		var parts []string
		for i, pl := range u.places {
			pos, was := differs(u.cp, pl)
			if pos == 0 {
				copies = nil // it could be of this one
				break
			}
			if i == 0 {
				at = pos
			}
			copies = append(copies, was.copy)
			parts = append(parts, fmt.Sprintf("%v at position %d of branch %d, where copy %d holds %v",
				u.cp.Seq[pos-1], pos, pl.branch, was.copy, was.id))
		}
		if copies == nil {
			continue
		}
		d := difference[ID]{u.places[0].branch, at, u.cp.Seq[at-1]}
		if told[d] {
			continue
		}
		told[d] = true
		c.add(Anomaly[ID]{Kind: NotAPrefix, Copies: copies, Branch: d.branch, At: at,
			Text: fmt.Sprintf("copy %d holds %s: it follows no other branch, and each committed one entry at most where it knows of none",
				u.cp.ID, strings.Join(parts, ", and "))})
	}
}

// notAPrefix reports a copy no branch committed: the branch it follows
// longest, and where it leaves it.
func (c *checker[ID]) notAPrefix(cp Copy[ID]) {
	best, at := 0, 0
	for x := range c.h.Branches {
		if n, _ := c.follow(cp.Seq, x); n > at {
			best, at = x, n
		}
	}
	id := cp.Seq[at-1]
	want, known := c.h.Branches[best][at]
	text := fmt.Sprintf("copy %d holds %d entries, which no branch committed in that order: at position %d it holds %v, ",
		cp.ID, len(cp.Seq), at, id)
	first := slices.Index(cp.Seq, id) + 1
	elsewhere, placed := c.knownOn(best)[id]
	switch {
	case known:
		text += fmt.Sprintf("where branch %d committed %v", best, want)
	case first < at:
		text += fmt.Sprintf("which it holds at position %d too", first)
	case placed:
		text += fmt.Sprintf("which branch %d committed at position %d", best, elsewhere)
	default:
		text += fmt.Sprintf("where branch %d knows of nothing that could be there", best)
	}
	c.add(Anomaly[ID]{Kind: NotAPrefix, Copies: []int{cp.ID}, Branch: best, At: at, Text: text})
}

func (c *checker[ID]) acks() {
	for _, cp := range c.h.Copies {
		if !cp.Latest {
			continue
		}
		var worst *Ack
		for i := range c.h.Acks {
			a := &c.h.Acks[i]
			if a.Prefix == 0 || !a.Return.Before(cp.Call) {
				continue
			}
			if len(cp.Seq) >= a.Prefix {
				if leaves, _ := c.follow(cp.Seq[:a.Prefix], a.Branch); leaves == 0 {
					continue
				}
			}
			if worst == nil || a.Prefix > worst.Prefix || (a.Prefix == worst.Prefix && a.Return.After(worst.Return)) {
				worst = a
			}
		}
		if worst == nil {
			continue
		}
		have := map[ID]bool{}
		for _, id := range cp.Seq {
			have[id] = true
		}
		var missing []ID
		for pos := 1; pos <= worst.Prefix; pos++ {
			if id, ok := c.h.Branches[worst.Branch][pos]; ok && !have[id] {
				missing = append(missing, id)
			}
		}
		text := fmt.Sprintf("copy %d holds %d entries, but an ack that returned at %s covered the first %d of branch %d",
			cp.ID, len(cp.Seq), worst.Return.Format("15:04:05.000"), worst.Prefix, worst.Branch)
		if len(missing) > 0 {
			text += fmt.Sprintf("; it lacks %d of them, the first %v", len(missing), missing[0])
		}
		c.add(Anomaly[ID]{Kind: LostAcked, Copies: []int{cp.ID}, Missing: missing, Ack: worst, Text: text})
	}
}

// groups checks that each copy of a group holds every copy of the group
// done before it began.
func (c *checker[ID]) groups() {
	groups := map[string][]*Copy[ID]{}
	for i := range c.h.Copies {
		cp := &c.h.Copies[i]
		// A copy known to be wrong, or found not to be a prefix, has been
		// reported; compared to the others, it would only report itself
		// again.
		if cp.Known != "" || c.branch[cp.ID] < 0 {
			continue
		}
		if cp.Latest {
			groups["latest"] = append(groups["latest"], cp)
		}
		// A copy may name the group "latest" itself; Latest has put it
		// there already if it is both.
		if cp.Group != "" && (cp.Group != "latest" || !cp.Latest) {
			groups[cp.Group] = append(groups[cp.Group], cp)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(groups)) {
		cps := groups[name]
		slices.SortStableFunc(cps, func(a, b *Copy[ID]) int { return a.Call.Compare(b.Call) })
		done := slices.Clone(cps)
		slices.SortStableFunc(done, func(a, b *Copy[ID]) int { return a.Return.Compare(b.Return) })
		// ahead are the copies done so far that no other of them holds:
		// one, the longest, while those done agree, and one for each way
		// they went once two that overlapped each hold what the other does
		// not. A copy that holds these holds every copy done.
		var ahead []*Copy[ID]
		i := 0
		for _, cp := range cps {
			for ; i < len(done) && done[i].Return.Before(cp.Call); i++ {
				ahead = furthest(ahead, done[i])
			}
			var have map[ID]bool
			for _, prev := range ahead {
				if isPrefix(prev.Seq, cp.Seq) {
					continue
				}
				if have == nil {
					have = map[ID]bool{}
					for _, id := range cp.Seq {
						have[id] = true
					}
				}
				var missing []ID
				for _, id := range prev.Seq {
					if !have[id] {
						missing = append(missing, id)
					}
				}
				text := fmt.Sprintf("copy %d of group %s holds %d entries, though copy %d, done before it began, held %d",
					cp.ID, name, len(cp.Seq), prev.ID, len(prev.Seq))
				if len(missing) > 0 {
					text += fmt.Sprintf("; it lacks %d of them, the first %v", len(missing), missing[0])
				}
				c.add(Anomaly[ID]{Kind: WentBack, Copies: []int{cp.ID, prev.ID}, Missing: missing, Text: text})
			}
		}
	}
}

// furthest adds cp, a copy now done, to ahead, the copies done before it
// that no other of them holds: it takes the place of those it holds, and
// is left out if one of them holds it.
func furthest[ID comparable](ahead []*Copy[ID], cp *Copy[ID]) []*Copy[ID] {
	for _, a := range ahead {
		if isPrefix(cp.Seq, a.Seq) {
			return ahead
		}
	}
	ahead = slices.DeleteFunc(ahead, func(a *Copy[ID]) bool { return isPrefix(a.Seq, cp.Seq) })
	return append(ahead, cp)
}

func isPrefix[ID comparable](a, b []ID) bool {
	return len(a) <= len(b) && slices.Equal(a, b[:len(a)])
}
