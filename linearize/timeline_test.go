package linearize

import "testing"

// TestTimeline checks histories placed by timestamp: a snapshot holds the
// writes before it, and the one at it only if snapshots are inclusive; a
// write of unknown outcome lands at most once, at or after its least
// timestamp; and real time does not enter into it.
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := check(t, tc.ops).Outcome; got != tc.want {
				t.Fatalf("outcome = %v, want %v", got, tc.want)
			}
		})
	}
}
