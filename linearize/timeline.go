package linearize

import (
	"fmt"
	"math"
)

// Timeline places operations on the line Check orders them by, when a
// system orders them by timestamps of its own -- the commit timestamp it
// gives each write, the snapshot timestamp it reads each read at -- rather
// than by when clients called and heard back. Checked on a timeline, a
// history is linearizable exactly when it is serializable in the system's
// timestamp order: when every read holds the writes with timestamps before
// its snapshot, and no others. That is what a multi-version database
// promises of its snapshot reads -- Kudu, CockroachDB, YugabyteDB, TiDB,
// etcd at a revision -- and it holds of reads in the past as of reads at
// the present: a read at a snapshot taken an hour ago must hold just what
// one taken then did. A real-time check cannot see a violation there.
//
// A write the system answered sits at its timestamp, so the search cannot
// move it; a read sits at its snapshot, between the writes before it and
// the write at it, or after the write at it if the system's snapshots are
// inclusive. A write whose outcome is unknown may take effect at any
// timestamp the system could have given it, or never: give it the least
// such timestamp -- past the timestamp its client had seen, say, as systems
// that propagate timestamps promise -- and it returns at Pending.
//
// The reads at one snapshot see one state: a write of unknown outcome
// landed before all of them or before none. Placed on their own they are
// merely concurrent, and the search could land the write between two of
// them, so give the reads at each snapshot one [Op.Group] -- ts-Base+1,
// say, which is never zero.
//
// Timestamps are any int64s the system orders operations by, at least
// Base and at most MaxOffset past it. Placing one outside that range
// panics, so that a mis-set Base is loud rather than a wrong verdict.
type Timeline struct {
	// Base is at or before every timestamp placed.
	Base int64
	// Inclusive says a snapshot at t holds the write at t too; otherwise
	// it holds the writes before t only, as Kudu's do.
	Inclusive bool
}

// MaxOffset is the furthest past Base a timestamp may lie: beyond it, the
// places run into Pending and past the end of int64.
const MaxOffset int64 = (math.MaxInt64 - 3) / 2

// Write places a write the system committed at timestamp ts.
func (l Timeline) Write(ts int64) (call, ret int64) {
	at := 2*l.offset(ts) + 1
	return at, at
}

// Read places a read at snapshot timestamp ts.
func (l Timeline) Read(ts int64) (call, ret int64) {
	at := 2 * l.offset(ts)
	if l.Inclusive {
		at += 2
	}
	return at, at
}

// Unknown places a write whose outcome is unknown, which the system would
// have committed at timestamp ts or later.
func (l Timeline) Unknown(ts int64) (call, ret int64) {
	call, _ = l.Write(ts)
	return call, Pending
}

// offset is how far past Base ts lies, within MaxOffset.
func (l Timeline) offset(ts int64) int64 {
	if ts < l.Base {
		panic(fmt.Sprintf("linearize: timestamp %d is before the timeline's base %d", ts, l.Base))
	}
	d := ts - l.Base
	if d < 0 || d > MaxOffset {
		panic(fmt.Sprintf("linearize: timestamp %d is more than MaxOffset past the timeline's base %d", ts, l.Base))
	}
	return d
}
