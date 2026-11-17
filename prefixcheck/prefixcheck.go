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
// primary gone -- may stand at any position a branch does not know, on
// its own branch or a later one.
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
	// say.
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
	// went-back, the earlier copy it does not hold.
	Copies []int
	// Known is the Known of a copy concerned, if any.
	Known string
	// Branch and At are, for not-a-prefix, the branch the copy follows
	// longest and the first position where it does not.
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
	out    []Anomaly[ID]
}

// Check checks h and returns what its copies contradict.
func Check[ID comparable](h History[ID]) []Anomaly[ID] {
	c := &checker[ID]{h: h, byID: map[int]*Copy[ID]{}, branch: map[int]int{}}
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
// follow branch b: where b knows another entry, or b does not know the
// position and the entry there is not one in doubt on b or an earlier
// branch. It returns 0 if seq follows b throughout.
func (h History[ID]) Consistent(seq []ID, b int) int {
	for i, id := range seq {
		pos := i + 1
		if want, ok := h.Branches[b][pos]; ok {
			if want != id {
				return pos
			}
			continue
		}
		if db, ok := h.Doubt[id]; !ok || db > b {
			return pos
		}
	}
	return 0
}

// BranchOf is the latest branch seq is a prefix of, or -1.
func (h History[ID]) BranchOf(seq []ID) int {
	for b := len(h.Branches) - 1; b >= 0; b-- {
		if h.Consistent(seq, b) == 0 {
			return b
		}
	}
	return -1
}

func (c *checker[ID]) prefixes() {
	for _, cp := range c.h.Copies {
		b := c.h.BranchOf(cp.Seq)
		c.branch[cp.ID] = b
		if b >= 0 {
			continue
		}
		// Name the branch it follows longest, and where it leaves it.
		best, at := 0, 0
		for x := range c.h.Branches {
			if n := c.h.Consistent(cp.Seq, x); n > at {
				best, at = x, n
			}
		}
		want, known := c.h.Branches[best][at]
		text := fmt.Sprintf("copy %d holds %d entries, which no branch committed in that order: at position %d it holds %v, where branch %d ",
			cp.ID, len(cp.Seq), at, cp.Seq[at-1], best)
		if known {
			text += fmt.Sprintf("committed %v", want)
		} else {
			text += "knows of nothing that could be there"
		}
		c.add(Anomaly[ID]{Kind: NotAPrefix, Copies: []int{cp.ID}, Branch: best, At: at, Text: text})
	}
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
			if len(cp.Seq) >= a.Prefix && c.h.Consistent(cp.Seq[:a.Prefix], a.Branch) == 0 {
				continue
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
		if cp.Group != "" && cp.Group != "latest" {
			groups[cp.Group] = append(groups[cp.Group], cp)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(groups)) {
		cps := groups[name]
		slices.SortStableFunc(cps, func(a, b *Copy[ID]) int { return a.Call.Compare(b.Call) })
		done := slices.Clone(cps)
		slices.SortStableFunc(done, func(a, b *Copy[ID]) int { return a.Return.Compare(b.Return) })
		var longest *Copy[ID]
		i := 0
		for _, cp := range cps {
			for i < len(done) && done[i].Return.Before(cp.Call) {
				if longest == nil || len(done[i].Seq) > len(longest.Seq) {
					longest = done[i]
				}
				i++
			}
			if longest == nil || isPrefix(longest.Seq, cp.Seq) {
				continue
			}
			have := map[ID]bool{}
			for _, id := range cp.Seq {
				have[id] = true
			}
			var missing []ID
			for _, id := range longest.Seq {
				if !have[id] {
					missing = append(missing, id)
				}
			}
			c.add(Anomaly[ID]{Kind: WentBack, Copies: []int{cp.ID, longest.ID}, Missing: missing,
				Text: fmt.Sprintf("copy %d of group %s holds %d entries, though copy %d, done before it began, held %d",
					cp.ID, name, len(cp.Seq), longest.ID, len(longest.Seq))})
		}
	}
}

func isPrefix[ID comparable](a, b []ID) bool {
	return len(a) <= len(b) && slices.Equal(a, b[:len(a)])
}
