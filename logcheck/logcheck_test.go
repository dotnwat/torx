package logcheck

import (
	"context"
	"slices"
	"testing"

	"github.com/dotnwat/torx/linearize"
)

// acked is an append of value under key, acked at off over [call, ret].
func acked(op int, key, value string, call, ret int64, off uint64) Append {
	return Append{Value: value, Key: key, Attempts: []Attempt{{Op: op, Call: call, Return: ret, Outcome: OK, Offset: off}}}
}

// unknown is an append of value under key whose answer was lost.
func unknown(op int, key, value string, call int64) Append {
	return Append{Value: value, Key: key, Attempts: []Attempt{{Op: op, Call: call, Outcome: Unknown}}}
}

func refused(op int, key, value string, call, ret int64) Append {
	return Append{Value: value, Key: key, Attempts: []Attempt{{Op: op, Call: call, Return: ret, Outcome: Failed}}}
}

func after(a Append, off uint64) Append {
	a.AfterKey = &off
	return a
}

func e(off uint64, key, value string) Entry { return Entry{Offset: off, Key: key, Value: value} }

func kinds(as []Anomaly) []string {
	var out []string
	for _, a := range as {
		out = append(out, a.Kind)
	}
	slices.Sort(out)
	return out
}

func TestCheck(t *testing.T) {
	for _, tc := range []struct {
		name    string
		appends []Append
		log     []Entry
		want    []string
	}{{
		name: "clean",
		appends: []Append{
			acked(0, "a", "x", 0, 10, 1), after(acked(1, "a", "y", 11, 20, 3), 1),
			acked(2, "b", "z", 12, 15, 2), unknown(3, "b", "w", 30), refused(4, "a", "v", 40, 41),
		},
		log: []Entry{e(1, "a", "x"), e(2, "b", "z"), e(3, "a", "y"), e(4, "b", "w")},
	}, {
		name:    "lost",
		appends: []Append{acked(0, "a", "x", 0, 10, 1), acked(1, "a", "y", 11, 12, 2)},
		log:     []Entry{e(1, "a", "x")},
		want:    []string{"lost"},
	}, {
		name:    "phantom",
		appends: []Append{acked(0, "a", "x", 0, 10, 1)},
		log:     []Entry{e(1, "a", "x"), e(2, "a", "zz")},
		want:    []string{"phantom"},
	}, {
		name:    "two acked at one offset",
		appends: []Append{acked(0, "a", "x", 0, 10, 1), acked(1, "a", "y", 0, 10, 1)},
		log:     []Entry{e(1, "a", "x"), e(2, "a", "y")},
		want:    []string{"ack-mismatch"},
	}, {
		name:    "acked under another key",
		appends: []Append{acked(0, "a", "x", 0, 10, 1)},
		log:     []Entry{e(1, "b", "x")},
		want:    []string{"ack-mismatch"},
	}, {
		name:    "condition",
		appends: []Append{acked(0, "a", "x", 0, 10, 1), acked(1, "a", "y", 11, 12, 2), after(acked(2, "a", "z", 13, 14, 3), 1)},
		log:     []Entry{e(1, "a", "x"), e(2, "a", "y"), e(3, "a", "z")},
		want:    []string{"condition"},
	}, {
		name: "condition on the log",
		appends: []Append{acked(0, "a", "x", 0, 10, 1), acked(1, "b", "y", 11, 12, 2),
			func() Append { a := acked(2, "a", "z", 13, 14, 3); one := uint64(1); a.AfterLog = &one; return a }()},
		log:  []Entry{e(1, "a", "x"), e(2, "b", "y"), e(3, "a", "z")},
		want: []string{"condition"},
	}, {
		name:    "realtime order",
		appends: []Append{acked(0, "a", "x", 0, 10, 2), acked(1, "b", "y", 20, 30, 1)},
		log:     []Entry{e(1, "b", "y"), e(2, "a", "x")},
		want:    []string{"realtime-order"},
	}, {
		name:    "an unknown append landing before one acked before it was sent",
		appends: []Append{acked(0, "a", "x", 0, 10, 2), unknown(1, "b", "y", 20)},
		log:     []Entry{e(1, "b", "y"), e(2, "a", "x")},
		want:    []string{"realtime-order"},
	}, {
		name:    "refused, yet there",
		appends: []Append{refused(0, "a", "x", 0, 10)},
		log:     []Entry{e(1, "a", "x")},
		want:    []string{"failed-visible"},
	}, {
		name: "resent with no idempotency key, twice",
		appends: []Append{{Value: "x", Key: "a", Attempts: []Attempt{
			{Op: 0, Call: 0, Outcome: Unknown}, {Op: 1, Call: 5, Return: 10, Outcome: OK, Offset: 2},
		}}},
		log: []Entry{e(1, "a", "x"), e(2, "a", "x")},
	}, {
		name: "resent under an idempotency key, twice",
		appends: []Append{{Value: "x", Key: "a", Idempotent: true, Attempts: []Attempt{
			{Op: 0, Call: 0, Outcome: Unknown}, {Op: 1, Call: 5, Return: 10, Outcome: OK, Offset: 2},
		}}},
		log:  []Entry{e(1, "a", "x"), e(2, "a", "x")},
		want: []string{"duplicate"},
	}, {
		name:    "sent once, twice",
		appends: []Append{acked(0, "a", "x", 0, 10, 2)},
		log:     []Entry{e(1, "a", "x"), e(2, "a", "x")},
		want:    []string{"duplicate"},
	}, {
		// A refused attempt cannot have put the value there.
		name: "refused, then acked, twice",
		appends: []Append{{Value: "x", Key: "a", Attempts: []Attempt{
			{Op: 0, Call: 0, Return: 1, Outcome: Failed}, {Op: 1, Call: 5, Return: 10, Outcome: OK, Offset: 2},
		}}},
		log:  []Entry{e(1, "a", "x"), e(2, "a", "x")},
		want: []string{"duplicate"},
	}, {
		// The second copy follows the first, whatever the condition.
		name: "conditional, resent, twice",
		appends: []Append{acked(0, "a", "x", 0, 1, 1), after(Append{Value: "v", Key: "a", Attempts: []Attempt{
			{Op: 1, Call: 2, Outcome: Unknown}, {Op: 2, Call: 5, Return: 10, Outcome: OK, Offset: 4},
		}}, 1)},
		log:  []Entry{e(1, "a", "x"), e(2, "a", "v"), e(4, "a", "v")},
		want: []string{"condition"},
	}, {
		// An acked append is where its ack put it, not at an earlier copy.
		name: "realtime order against an ack, with an earlier copy",
		appends: []Append{{Value: "d", Key: "a", Attempts: []Attempt{
			{Op: 0, Call: 0, Outcome: Unknown}, {Op: 1, Call: 20, Return: 30, Outcome: OK, Offset: 7},
		}}, acked(2, "b", "x", 40, 45, 5)},
		log:  []Entry{e(2, "a", "d"), e(5, "b", "x"), e(7, "a", "d")},
		want: []string{"realtime-order"},
	}, {
		name:    "two entries at one offset",
		appends: []Append{acked(0, "a", "x", 0, 10, 5), acked(1, "b", "y", 0, 10, 5)},
		log:     []Entry{e(5, "a", "x"), e(5, "b", "y")},
		want:    []string{"ack-mismatch", "collision"},
	}, {
		// A refused attempt put nothing in the log: the append was sent
		// when its first attempt not refused was, after y's ack.
		name: "realtime order from the first attempt not refused",
		appends: []Append{acked(0, "b", "y", 0, 10, 2), {Value: "x", Key: "a", Attempts: []Attempt{
			{Op: 1, Call: 1, Return: 2, Outcome: Failed}, {Op: 2, Call: 20, Return: 30, Outcome: OK, Offset: 1},
		}}},
		log:  []Entry{e(1, "a", "x"), e(2, "b", "y")},
		want: []string{"realtime-order"},
	}, {
		// An append acked twice was in the log at the higher offset by the
		// later ack.
		name: "realtime order against a later ack",
		appends: []Append{{Value: "d", Key: "a", Attempts: []Attempt{
			{Op: 0, Call: 0, Return: 10, Outcome: OK, Offset: 1}, {Op: 1, Call: 0, Return: 12, Outcome: OK, Offset: 3},
		}}, acked(2, "b", "x", 20, 25, 2)},
		log:  []Entry{e(1, "a", "d"), e(2, "b", "x"), e(3, "a", "d")},
		want: []string{"realtime-order"},
	}, {
		name:    "an unknown append under another key",
		appends: []Append{unknown(0, "a", "x", 0)},
		log:     []Entry{e(1, "b", "x")},
		want:    []string{"misfiled"},
	}, {
		// One attempt at x had been sent when y was acked, so one copy of
		// x may precede y; the second is the resend's, sent after.
		name: "a resend's copy before an append acked before the resend",
		appends: []Append{{Value: "x", Key: "a", Attempts: []Attempt{
			{Op: 0, Call: 0, Outcome: Unknown}, {Op: 1, Call: 100, Outcome: Unknown},
		}}, acked(2, "b", "y", 10, 20, 3)},
		log:  []Entry{e(1, "a", "x"), e(2, "a", "x"), e(3, "b", "y")},
		want: []string{"realtime-order"},
	}, {
		// The ack says the resend, sent at 100, placed offset 1, which
		// precedes y, acked by 60.
		name: "an ack naming an offset the log held before the attempt was sent",
		appends: []Append{{Value: "x", Key: "a", Attempts: []Attempt{
			{Op: 0, Call: 0, Outcome: Unknown}, {Op: 1, Call: 100, Return: 110, Outcome: OK, Offset: 1},
		}}, acked(2, "b", "y", 50, 60, 2)},
		log:  []Entry{e(1, "a", "x"), e(2, "b", "y"), e(3, "a", "x")},
		want: []string{"realtime-order"},
	}, {
		// The first attempt was acked at 3, so the copy at 1 is the
		// resend's, sent at 100, after that ack.
		name: "a resend's copy before the first attempt's, acked before the resend",
		appends: []Append{{Value: "x", Key: "a", Attempts: []Attempt{
			{Op: 0, Call: 0, Return: 10, Outcome: OK, Offset: 3}, {Op: 1, Call: 100, Outcome: Unknown},
		}}},
		log:  []Entry{e(1, "a", "x"), e(3, "a", "x")},
		want: []string{"realtime-order"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			got := Check(tc.appends, tc.log)
			for _, a := range got {
				t.Log(a)
			}
			if !slices.Equal(kinds(got), tc.want) {
				t.Fatalf("found %v, want %v", kinds(got), tc.want)
			}
		})
	}
}

func TestCheckRejectsOffsetZero(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("an entry at offset 0 was accepted")
		}
	}()
	Check([]Append{acked(0, "a", "x", 0, 10, 0)}, []Entry{e(0, "a", "x")})
}

func read(op int, key string, call, ret int64, off uint64, value string) Read {
	return Read{Op: op, Key: key, Call: call, Return: ret, Offset: off, Value: value}
}

func TestCheckReads(t *testing.T) {
	appends := []Append{
		acked(0, "a", "x", 0, 10, 1),
		acked(1, "a", "y", 11, 12, 2),
		unknown(2, "a", "z", 30), // lands at 4
		acked(3, "b", "w", 12, 15, 3),
	}
	log := []Entry{e(1, "a", "x"), e(2, "a", "y"), e(3, "b", "w"), e(4, "a", "z")}
	for _, tc := range []struct {
		name  string
		reads []Read
		want  []string
	}{{
		name: "clean",
		reads: []Read{
			read(10, "a", 5, 6, 0, ""),        // before x took effect
			read(11, "a", 5, 11, 1, "x"),      // x
			read(12, "a", 11, 12, 1, "x"),     // concurrent with y
			read(13, "a", 13, 14, 2, "y"),     // y
			read(14, "a", 31, 32, 2, "y"),     // z not yet
			read(15, "a", 40, 41, 4, "z"),     // z
			read(16, "b", 16, 17, 3, "w"),     // another key
			read(17, "c", 1, 2, 0, ""),        // no entries at all
			read(18, "a", 35, 36, 4, "z"),     // z seen once ...
			read(19, "a", 37, 38, 4, "z"),     // ... and after
			read(20, "a", 12, 13, 2, "y"),     // y concurrent with its ack
			read(21, "b", 100, 101, 3, "w"),   // long after
			read(22, "a", 100, 101, 4, "z"),   // z long after
			read(23, "a", 11, 12, 2, "y"),     // y within y's own call
			read(24, "a", 10, 10, 1, "x"),     // at x's return
			read(25, "b", 0, 1, 0, ""),        // w not yet
			read(26, "a", 0, 100, 1, "x"),     // a long read
			read(27, "a", 31, 100, 4, "z"),    // a long read of z
			read(28, "a", 31, 100, 2, "y"),    // a long read of y
			read(29, "b", 13, 15, 3, "w"),     // w within its call
			read(30, "a", 9, 10, 1, "x"),      // x within its call
			read(31, "a", 12, 30, 2, "y"),     // y up to z's call
			read(32, "a", 13, 1000, 4, "z"),   // z, read long
			read(33, "a", 2000, 2001, 4, "z"), // z at the end
		},
	}, {
		name:  "stale",
		reads: []Read{read(10, "a", 20, 21, 1, "x")},
		want:  []string{"nonlinearizable-read"},
	}, {
		name:  "new then old",
		reads: []Read{read(10, "a", 40, 41, 4, "z"), read(11, "a", 42, 43, 2, "y")},
		want:  []string{"nonlinearizable-read"},
	}, {
		name:  "nothing after something",
		reads: []Read{read(10, "a", 20, 21, 0, "")},
		want:  []string{"nonlinearizable-read"},
	}, {
		name:  "dirty",
		reads: []Read{read(10, "a", 20, 21, 9, "q")},
		want:  []string{"dirty-read"},
	}, {
		name:  "the right offset, the wrong value",
		reads: []Read{read(10, "a", 20, 21, 2, "q")},
		want:  []string{"dirty-read"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			got, undecided := CheckReads(context.Background(), appends, log, tc.reads, linearize.Limits{})
			if len(undecided) > 0 {
				t.Fatalf("undecided: %v", undecided)
			}
			for _, a := range got {
				t.Log(a)
			}
			if !slices.Equal(kinds(got), tc.want) {
				t.Fatalf("found %v, want %v", kinds(got), tc.want)
			}
		})
	}
}

// A duplicate's later copy may have landed any time after the append was
// sent, so a read may see the key's entries move back to it.
func TestCheckReadsDuplicate(t *testing.T) {
	appends := []Append{
		{Value: "x", Key: "a", Attempts: []Attempt{{Op: 0, Call: 0, Outcome: Unknown}, {Op: 1, Call: 5, Return: 10, Outcome: OK, Offset: 1}}},
		acked(2, "a", "y", 11, 12, 2),
	}
	log := []Entry{e(1, "a", "x"), e(2, "a", "y"), e(3, "a", "x")}
	reads := []Read{read(10, "a", 13, 14, 2, "y"), read(11, "a", 20, 21, 3, "x")}
	got, _ := CheckReads(context.Background(), appends, log, reads, linearize.Limits{})
	if len(got) > 0 {
		t.Fatalf("found %v", got)
	}
}

// An entry no client appended is not in the key's order the search uses:
// the reads of what came after it are placed as if it were not there, and
// a read of it is left out.
func TestCheckReadsPhantom(t *testing.T) {
	appends := []Append{acked(0, "a", "x", 0, 10, 1), acked(1, "a", "y", 20, 30, 3)}
	log := []Entry{e(1, "a", "x"), e(2, "a", "ph"), e(3, "a", "y")}
	reads := []Read{read(10, "a", 11, 12, 1, "x"), read(11, "a", 15, 16, 2, "ph"), read(12, "a", 31, 32, 3, "y")}
	got, undecided := CheckReads(context.Background(), appends, log, reads, linearize.Limits{})
	if len(got) > 0 || len(undecided) > 0 {
		t.Fatalf("found %v, undecided %v", got, undecided)
	}
	got, _ = CheckReads(context.Background(), appends, log, []Read{read(13, "a", 31, 32, 1, "x")}, linearize.Limits{})
	if !slices.Equal(kinds(got), []string{"nonlinearizable-read"}) {
		t.Fatalf("a stale read beside a phantom: found %v", kinds(got))
	}
}

// explain names the operations nearest the one it could not place,
// whatever the clock's unit, and does not run off the end of a history
// whose stuck operation never returned.
func TestExplainNearest(t *testing.T) {
	appends := []Append{acked(0, "a", "x", 0, 1, 1), acked(1, "a", "y", 2, 3, 2)}
	log := []Entry{e(1, "a", "x"), e(2, "a", "y")}
	var reads []Read
	for i := range 20 {
		reads = append(reads, read(10+i, "a", int64(100+10*i), int64(101+10*i), 2, "y"))
	}
	reads = append(reads, read(50, "a", 1000, 1001, 1, "x")) // stale, long after
	got, _ := CheckReads(context.Background(), appends, log, reads, linearize.Limits{})
	if len(got) != 1 || got[0].Kind != "nonlinearizable-read" {
		t.Fatalf("found %v", got)
	}
	if got[0].Ops[0] != 50 || len(got[0].Ops) != 9 || !slices.Contains(got[0].Ops, 29) || slices.Contains(got[0].Ops, 10) {
		t.Errorf("ops %v: want the stale read, then the eight reads nearest it", got[0].Ops)
	}
}

// An ack names the copy it is for: the retry's ack at 2 says nothing of
// when the copy at 1 landed, which the log's order does -- before the copy
// at 2, so by its ack.
func TestCheckReadsAckedCopy(t *testing.T) {
	appends := []Append{{Value: "x", Key: "a", Attempts: []Attempt{
		{Op: 0, Call: 0, Outcome: Unknown}, {Op: 1, Call: 5, Return: 10, Outcome: OK, Offset: 2},
	}}}
	log := []Entry{e(1, "a", "x"), e(2, "a", "x")}
	got, _ := CheckReads(context.Background(), appends, log, []Read{read(10, "a", 20, 21, 1, "x")}, linearize.Limits{})
	if !slices.Equal(kinds(got), []string{"nonlinearizable-read"}) {
		t.Fatalf("found %v", got)
	}
}

// A refused attempt put nothing in the log: the value cannot be read
// before the attempt that did was sent.
func TestCheckReadsRefusedAttempt(t *testing.T) {
	appends := []Append{{Value: "x", Key: "a", Attempts: []Attempt{
		{Op: 0, Call: 1, Return: 2, Outcome: Failed}, {Op: 1, Call: 20, Return: 30, Outcome: OK, Offset: 1},
	}}}
	log := []Entry{e(1, "a", "x")}
	got, _ := CheckReads(context.Background(), appends, log, []Read{read(10, "a", 5, 6, 1, "x")}, linearize.Limits{})
	if !slices.Equal(kinds(got), []string{"nonlinearizable-read"}) {
		t.Fatalf("found %v", got)
	}
}

// The log is one sequence: an entry landed by the time any entry after it
// was acked, and no earlier than any entry before it was sent, whatever
// their keys.
func TestCheckReadsAcrossKeys(t *testing.T) {
	// x to a, its answer lost, at 1; y to b, acked at 2 by 10: x was there
	// by 10, and a read of a at 20 that finds nothing is stale.
	appends := []Append{unknown(0, "a", "x", 0), acked(1, "b", "y", 5, 10, 2)}
	log := []Entry{e(1, "a", "x"), e(2, "b", "y")}
	got, _ := CheckReads(context.Background(), appends, log, []Read{read(10, "a", 20, 21, 0, "")}, linearize.Limits{})
	if !slices.Equal(kinds(got), []string{"nonlinearizable-read"}) {
		t.Fatalf("a read that missed an entry bounded by a later key's ack: found %v", got)
	}
	// y to b, sent at 50, at 1; x to a, its answer lost, at 2: x landed
	// after 50, and a read of a at 20 cannot have seen it.
	appends = []Append{acked(0, "b", "y", 50, 60, 1), unknown(1, "a", "x", 0)}
	log = []Entry{e(1, "b", "y"), e(2, "a", "x")}
	got, _ = CheckReads(context.Background(), appends, log, []Read{read(10, "a", 20, 21, 2, "x")}, linearize.Limits{})
	if !slices.Equal(kinds(got), []string{"nonlinearizable-read"}) {
		t.Fatalf("a read that saw an entry bounded by an earlier key's call: found %v", got)
	}
}

// A read that returned an entry saw it landed, which bounds every entry
// before it, under any key: a read of b that saw offset 2 says offset 1,
// under a, had landed too.
func TestCheckReadsReadAsEvidence(t *testing.T) {
	appends := []Append{unknown(0, "a", "x", 0), unknown(1, "b", "y", 0)}
	log := []Entry{e(1, "a", "x"), e(2, "b", "y")}
	reads := []Read{read(10, "b", 20, 21, 2, "y"), read(11, "a", 30, 31, 0, "")}
	got, _ := CheckReads(context.Background(), appends, log, reads, linearize.Limits{})
	if !slices.Equal(kinds(got), []string{"nonlinearizable-read"}) {
		t.Fatalf("found %v", got)
	}
	if len(got) > 0 && got[0].Ops[0] != 11 {
		t.Errorf("blamed op %d, want the read of a that found nothing", got[0].Ops[0])
	}
}

// A copy from a resend landed no earlier than the resend was sent: the
// second copy of x is the attempt at 100's, and cannot have been read at
// 50.
func TestCheckReadsResendCopy(t *testing.T) {
	appends := []Append{{Value: "x", Key: "a", Attempts: []Attempt{
		{Op: 0, Call: 0, Outcome: Unknown}, {Op: 1, Call: 100, Outcome: Unknown},
	}}}
	log := []Entry{e(1, "a", "x"), e(2, "a", "x")}
	got, _ := CheckReads(context.Background(), appends, log, []Read{read(10, "a", 50, 51, 2, "x")}, linearize.Limits{})
	if !slices.Equal(kinds(got), []string{"nonlinearizable-read"}) {
		t.Fatalf("found %v", got)
	}
	got, _ = CheckReads(context.Background(), appends, log, []Read{read(10, "a", 50, 51, 1, "x"), read(11, "a", 200, 201, 2, "x")}, linearize.Limits{})
	if len(got) > 0 {
		t.Fatalf("the first copy read at 50 and the second at 200: found %v", got)
	}
	// Acked, the resend's copy is the one its ack named, whichever that is.
	appends = []Append{{Value: "x", Key: "a", Attempts: []Attempt{
		{Op: 0, Call: 0, Outcome: Unknown}, {Op: 1, Call: 100, Return: 110, Outcome: OK, Offset: 1},
	}}}
	got, _ = CheckReads(context.Background(), appends, log, []Read{read(10, "a", 50, 51, 1, "x")}, linearize.Limits{})
	if !slices.Equal(kinds(got), []string{"nonlinearizable-read"}) {
		t.Fatalf("the resend's copy, at 1 by its ack, read at 50: found %v", got)
	}
	got, _ = CheckReads(context.Background(), appends, log, []Read{read(10, "a", 120, 121, 1, "x"), read(11, "a", 200, 201, 2, "x")}, linearize.Limits{})
	if len(got) > 0 {
		t.Fatalf("the resend's copy read at 120 and the first attempt's at 200: found %v", got)
	}
}
