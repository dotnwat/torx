package prefixcheck

import (
	"slices"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }

// seq is a branch committing ids at positions 1, 2, ...
func seq(ids ...string) map[int]string {
	m := map[int]string{}
	for i, id := range ids {
		m[i+1] = id
	}
	return m
}

func kinds(as []Anomaly[string]) []string {
	var out []string
	for _, a := range as {
		out = append(out, a.Kind)
	}
	return out
}

func TestPrefixes(t *testing.T) {
	h := History[string]{
		Branches: []map[int]string{seq("a", "b", "c", "d")},
		Copies: []Copy[string]{
			{ID: 0, Seq: nil},
			{ID: 1, Seq: []string{"a", "b"}},
			{ID: 2, Seq: []string{"a", "b", "c", "d"}},
		},
	}
	if as := Check(h); len(as) != 0 {
		t.Fatalf("prefixes: %v", as)
	}
	// An entry out of place, one never committed, and one after a gap.
	h.Copies = []Copy[string]{
		{ID: 0, Seq: []string{"a", "c"}},
		{ID: 1, Seq: []string{"a", "b", "x"}},
		{ID: 2, Seq: []string{"b"}},
	}
	as := Check(h)
	if !slices.Equal(kinds(as), []string{NotAPrefix, NotAPrefix, NotAPrefix}) {
		t.Fatalf("anomalies: %v", as)
	}
	if as[0].At != 2 || as[1].At != 3 || as[2].At != 1 {
		t.Fatalf("positions: %d %d %d", as[0].At, as[1].At, as[2].At)
	}
}

func TestDoubt(t *testing.T) {
	// c's commit was in doubt; the branch knows d at 4, nothing at 3.
	h := History[string]{
		Branches: []map[int]string{{1: "a", 2: "b", 4: "d"}},
		Doubt:    map[string]int{"c": 0},
		Copies: []Copy[string]{
			{ID: 0, Seq: []string{"a", "b", "c"}},
			{ID: 1, Seq: []string{"a", "b", "c", "d"}},
		},
	}
	if as := Check(h); len(as) != 0 {
		t.Fatalf("an entry in doubt at an unknown position: %v", as)
	}
	// In doubt, but not where the branch knows another entry.
	h.Copies = []Copy[string]{{ID: 0, Seq: []string{"a", "c"}}}
	if as := Check(h); !slices.Equal(kinds(as), []string{NotAPrefix}) {
		t.Fatalf("an entry in doubt where another is known: %v", as)
	}
	// In doubt on a later branch: not on this one.
	h = History[string]{
		Branches: []map[int]string{{1: "a"}, {1: "a"}},
		Doubt:    map[string]int{"z": 1},
		Copies:   []Copy[string]{{ID: 0, Seq: []string{"a", "z"}}},
	}
	if as := Check(h); len(as) != 0 {
		t.Fatalf("an entry in doubt on the later branch: %v", as)
	}
	if b := h.BranchOf([]string{"a", "z"}); b != 1 {
		t.Fatalf("BranchOf = %d, want 1", b)
	}
}

// An entry in doubt was committed once if at all: a copy that holds it
// twice applied it twice, wherever the branch has room for it.
func TestDoubtTwice(t *testing.T) {
	h := History[string]{
		Branches: []map[int]string{{1: "a"}},
		Doubt:    map[string]int{"x": 0, "y": 0},
		Copies: []Copy[string]{
			{ID: 0, Seq: []string{"a", "x", "y"}},
			{ID: 1, Seq: []string{"a", "x", "x"}},
			{ID: 2, Seq: []string{"a", "x", "y", "x"}},
		},
	}
	as := Check(h)
	if !slices.Equal(kinds(as), []string{NotAPrefix, NotAPrefix}) {
		t.Fatalf("anomalies: %v", as)
	}
	if as[0].Copies[0] != 1 || as[0].At != 3 || as[1].Copies[0] != 2 || as[1].At != 4 {
		t.Fatalf("copies and positions: %+v", as)
	}
	if want := "at position 3 it holds x, which it holds at position 2 too"; !strings.Contains(as[0].Text, want) {
		t.Fatalf("text %q does not say %q", as[0].Text, want)
	}
	// Twice, the second time where a later branch learned its position:
	// in doubt on the branch it was sent to, known on the one that kept it.
	h = History[string]{
		Branches: []map[int]string{{1: "a"}, {1: "a", 3: "x"}},
		Doubt:    map[string]int{"x": 0},
		Copies: []Copy[string]{
			{ID: 0, Seq: []string{"a", "x", "x"}},
			{ID: 1, Seq: []string{"a", "x"}},
		},
	}
	if as := Check(h); !slices.Equal(kinds(as), []string{NotAPrefix}) || as[0].Copies[0] != 0 {
		t.Fatalf("an entry held where it is in doubt and where it is known: %v", as)
	}
}

func TestBranches(t *testing.T) {
	// The primary committed a..d; a failover restored a, b and carried on
	// with x, y.
	h := History[string]{
		Branches: []map[int]string{seq("a", "b", "c", "d"), seq("a", "b", "x", "y")},
		Copies: []Copy[string]{
			{ID: 0, Seq: []string{"a", "b", "c"}},
			{ID: 1, Seq: []string{"a", "b", "x"}},
			{ID: 2, Seq: []string{"a", "b", "c", "y"}},
		},
	}
	as := Check(h)
	if !slices.Equal(kinds(as), []string{NotAPrefix}) || as[0].Copies[0] != 2 {
		t.Fatalf("anomalies: %v", as)
	}
}

func TestLostAcked(t *testing.T) {
	h := History[string]{
		Branches: []map[int]string{seq("a", "b", "c", "d")},
		Acks: []Ack{
			{Branch: 0, Prefix: 2, Return: at(1)},
			{Branch: 0, Prefix: 3, Return: at(3)},
			{Branch: 0, Prefix: 4, Return: at(10)}, // after every copy
		},
		Copies: []Copy[string]{
			{ID: 0, Seq: []string{"a", "b"}, Call: at(2), Return: at(2.5), Latest: true},
			{ID: 1, Seq: []string{"a", "b"}, Call: at(4), Return: at(4.5), Latest: true},
			{ID: 2, Seq: []string{"a"}, Call: at(5), Return: at(5.5)}, // not of the latest state
		},
	}
	as := Check(h)
	// Copy 1 lacks c, acked at 3; and went back from nothing -- copy 0
	// held as much.
	if !slices.Equal(kinds(as), []string{LostAcked}) {
		t.Fatalf("anomalies: %v", as)
	}
	if a := as[0]; a.Copies[0] != 1 || a.Ack.Prefix != 3 || !slices.Equal(a.Missing, []string{"c"}) {
		t.Fatalf("lost-acked: %+v", a)
	}
}

func TestWentBack(t *testing.T) {
	h := History[string]{
		Branches: []map[int]string{seq("a", "b", "c")},
		Copies: []Copy[string]{
			{ID: 0, Seq: []string{"a", "b"}, Call: at(1), Return: at(2), Latest: true},
			{ID: 1, Seq: []string{"a"}, Call: at(1.5), Return: at(2.5), Latest: true}, // overlapped: fine
			{ID: 2, Seq: []string{"a"}, Call: at(3), Return: at(4), Latest: true},     // after 0: went back
			{ID: 3, Seq: []string{"a", "b", "c"}, Call: at(5), Return: at(6), Group: "f"},
			{ID: 4, Seq: []string{"a", "b"}, Call: at(7), Return: at(8), Group: "f"}, // went back
			{ID: 5, Seq: []string{"a"}, Call: at(7), Return: at(8), Group: "g"},      // another group
		},
	}
	as := Check(h)
	if !slices.Equal(kinds(as), []string{WentBack, WentBack}) {
		t.Fatalf("anomalies: %v", as)
	}
	if !slices.Equal(as[0].Copies, []int{4, 3}) || !slices.Equal(as[0].Missing, []string{"c"}) {
		t.Fatalf("group f: %+v", as[0])
	}
	if !slices.Equal(as[1].Copies, []int{2, 0}) || !slices.Equal(as[1].Missing, []string{"b"}) {
		t.Fatalf("latest: %+v", as[1])
	}
}

// Copies that overlapped may each hold what the other does not -- one of
// the old primary, one of the new -- and a copy begun after both is held
// to both, not to the longer.
func TestWentBackDiverged(t *testing.T) {
	h := History[string]{
		Branches: []map[int]string{seq("a", "b", "c"), seq("a", "x", "y", "z")},
		Copies: []Copy[string]{
			{ID: 0, Seq: []string{"a", "b", "c"}, Call: at(1), Return: at(3), Latest: true},
			{ID: 1, Seq: []string{"a", "x"}, Call: at(2), Return: at(4), Latest: true},
			{ID: 2, Seq: []string{"a", "b", "c"}, Call: at(5), Return: at(6), Latest: true}, // lost x
			{ID: 3, Seq: []string{"a", "x", "y"}, Call: at(7), Return: at(8), Latest: true}, // lost b, c
			{ID: 4, Seq: []string{"a"}, Call: at(9), Return: at(10), Latest: true},          // lost both ways
		},
	}
	as := Check(h)
	if !slices.Equal(kinds(as), []string{WentBack, WentBack, WentBack, WentBack}) {
		t.Fatalf("anomalies: %v", as)
	}
	type lost struct {
		copies  []int
		missing []string
	}
	for i, want := range []lost{
		{[]int{2, 1}, []string{"x"}},
		{[]int{3, 0}, []string{"b", "c"}},
		{[]int{4, 0}, []string{"b", "c"}},
		{[]int{4, 3}, []string{"x", "y"}}, // copy 3 took copy 1's place
	} {
		if !slices.Equal(as[i].Copies, want.copies) || !slices.Equal(as[i].Missing, want.missing) {
			t.Fatalf("anomaly %d: %+v, want %+v", i, as[i], want)
		}
	}
	// As long as each other: neither stands for both.
	h = History[string]{
		Branches: []map[int]string{seq("a", "b"), seq("a", "x")},
		Copies: []Copy[string]{
			{ID: 0, Seq: []string{"a", "b"}, Call: at(1), Return: at(3), Latest: true},
			{ID: 1, Seq: []string{"a", "x"}, Call: at(2), Return: at(4), Latest: true},
			{ID: 2, Seq: []string{"a", "b"}, Call: at(5), Return: at(6), Latest: true},
		},
	}
	as = Check(h)
	if !slices.Equal(kinds(as), []string{WentBack}) || !slices.Equal(as[0].Copies, []int{2, 1}) {
		t.Fatalf("copies of one length: %v", as)
	}
}

func TestKnown(t *testing.T) {
	// Copy 1 holds x, a known issue: it is reported with the reason, and
	// copy 2, which holds less, is not compared to it.
	h := History[string]{
		Branches: []map[int]string{seq("a", "b")},
		Copies: []Copy[string]{
			{ID: 0, Seq: []string{"a"}, Call: at(1), Return: at(2), Latest: true},
			{ID: 1, Seq: []string{"a", "x"}, Call: at(3), Return: at(4), Latest: true, Known: "torn"},
			{ID: 2, Seq: []string{"a"}, Call: at(5), Return: at(6), Latest: true},
		},
	}
	as := Check(h)
	if !slices.Equal(kinds(as), []string{NotAPrefix}) || as[0].Known != "torn" {
		t.Fatalf("anomalies: %v", as)
	}
	// Without the reason, the copy is reported, and still not compared to.
	h.Copies[1].Known = ""
	as = Check(h)
	if !slices.Equal(kinds(as), []string{NotAPrefix}) || as[0].Known != "" {
		t.Fatalf("anomalies: %v", as)
	}
}
