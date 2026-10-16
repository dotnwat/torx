package linearize

import "testing"

// TestTimeline checks histories placed by timestamp: a snapshot holds the
// writes before it, and the one at it only if snapshots are inclusive; a
// write of unknown outcome lands at most once, at or after its least
// timestamp, and before all the reads at a snapshot or none of them; and
// real time does not enter into it.
func TestTimeline(t *testing.T) {
	write := func(l Timeline, ts int64, v int) Op[regIn, regOut] {
		call, ret := l.Write(ts)
		return w(0, call, ret, v)
	}
	read := func(l Timeline, ts int64, v int) Op[regIn, regOut] {
		call, ret := l.Read(ts)
		return r(1, call, ret, v)
	}
	unknown := func(l Timeline, ts int64, v int) Op[regIn, regOut] {
		call, ret := l.Unknown(ts)
		o := w(2, call, ret, v)
		o.Output.unknown = true
		return o
	}
	// snapshot places reads at one snapshot in one group, as the reads a
	// system served at one snapshot are.
	snapshot := func(l Timeline, ts int64, vs ...int) []Op[regIn, regOut] {
		var ops []Op[regIn, regOut]
		for i, v := range vs {
			call, ret := l.Read(ts)
			op := r(10+i, call, ret, v)
			op.Group = ts - l.Base + 1
			ops = append(ops, op)
		}
		return ops
	}
	excl := Timeline{Base: 100}
	incl := Timeline{Base: 100, Inclusive: true}
	for _, tc := range []struct {
		name string
		ops  []Op[regIn, regOut]
		want Outcome
	}{
		{"exclusive snapshot misses the write at it",
			[]Op[regIn, regOut]{write(excl, 110, 1), read(excl, 110, 0)}, Ok},
		{"exclusive snapshot holding the write at it",
			[]Op[regIn, regOut]{write(excl, 110, 1), read(excl, 110, 1)}, Illegal},
		{"exclusive snapshot after the write",
			[]Op[regIn, regOut]{write(excl, 110, 1), read(excl, 111, 1)}, Ok},
		{"inclusive snapshot holds the write at it",
			[]Op[regIn, regOut]{write(incl, 110, 1), read(incl, 110, 1)}, Ok},
		{"inclusive snapshot missing the write at it",
			[]Op[regIn, regOut]{write(incl, 110, 1), read(incl, 110, 0)}, Illegal},
		{"inclusive snapshot before the write",
			[]Op[regIn, regOut]{write(incl, 110, 1), read(incl, 109, 0)}, Ok},
		{"a read in the past must not see a later write",
			[]Op[regIn, regOut]{write(excl, 110, 1), write(excl, 120, 2), read(excl, 115, 2)}, Illegal},
		{"a read in the past sees what was then",
			[]Op[regIn, regOut]{write(excl, 110, 1), write(excl, 120, 2), read(excl, 115, 1)}, Ok},
		{"unknown write seen",
			[]Op[regIn, regOut]{unknown(excl, 105, 1), read(excl, 120, 1)}, Ok},
		{"unknown write never landing",
			[]Op[regIn, regOut]{unknown(excl, 105, 1), read(excl, 120, 0)}, Ok},
		{"unknown write seen before its least timestamp",
			[]Op[regIn, regOut]{unknown(excl, 105, 1), read(excl, 103, 1)}, Illegal},
		{"unknown write seen, then not",
			[]Op[regIn, regOut]{unknown(excl, 105, 1), read(excl, 120, 1), read(excl, 130, 0)}, Illegal},
		{"unknown write landing between two snapshots",
			[]Op[regIn, regOut]{unknown(excl, 105, 1), read(excl, 110, 0), read(excl, 120, 1)}, Ok},
		{"exclusive snapshot at an unknown write's least timestamp misses it",
			[]Op[regIn, regOut]{unknown(excl, 105, 1), read(excl, 105, 1)}, Illegal},
		{"inclusive snapshot at an unknown write's least timestamp may hold it",
			[]Op[regIn, regOut]{unknown(incl, 105, 1), read(incl, 105, 1)}, Ok},
		{"two writes at one timestamp land in either order",
			[]Op[regIn, regOut]{write(excl, 110, 1), write(excl, 110, 2), read(excl, 111, 1)}, Ok},
		{"two writes at one timestamp, both past the exclusive snapshot at it",
			[]Op[regIn, regOut]{write(excl, 110, 1), write(excl, 110, 2), read(excl, 110, 1)}, Illegal},
		{"reads at one snapshot agree on an unknown write",
			append([]Op[regIn, regOut]{unknown(excl, 105, 1)}, snapshot(excl, 110, 0, 1)...), Illegal},
		{"reads at one inclusive snapshot agree on an unknown write",
			append([]Op[regIn, regOut]{unknown(incl, 105, 1)}, snapshot(incl, 110, 0, 1)...), Illegal},
		{"reads at one snapshot all holding an unknown write",
			append([]Op[regIn, regOut]{unknown(excl, 105, 1)}, snapshot(excl, 110, 1, 1)...), Ok},
		{"reads at one snapshot all missing an unknown write that landed later",
			append(append([]Op[regIn, regOut]{unknown(excl, 105, 1)}, snapshot(excl, 110, 0, 0)...), read(excl, 120, 1)), Ok},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := check(t, tc.ops).Outcome; got != tc.want {
				t.Fatalf("outcome = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestTimelineBounds checks the ends of the range a timeline places: at
// MaxOffset past Base a write still sits short of Pending and an inclusive
// read still past the write, so a snapshot there holding it is legal; a
// timestamp before Base or past MaxOffset panics.
func TestTimelineBounds(t *testing.T) {
	l := Timeline{Base: 10, Inclusive: true}
	top := l.Base + MaxOffset
	wc, wr := l.Write(top)
	rc, rr := l.Read(top)
	if wc >= Pending || rc <= wc {
		t.Fatalf("places at the bound: write %d, inclusive read %d, Pending %d", wc, rc, Pending)
	}
	if got := check(t, []Op[regIn, regOut]{w(0, wc, wr, 1), r(1, rc, rr, 1)}).Outcome; got != Ok {
		t.Errorf("an inclusive snapshot at the bound holding the write at it = %v, want %v", got, Ok)
	}
	for _, ts := range []int64{l.Base - 1, top + 1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Write(%d) on a timeline based at %d did not panic", ts, l.Base)
				}
			}()
			l.Write(ts)
		}()
	}
}
