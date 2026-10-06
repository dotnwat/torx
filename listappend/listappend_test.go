package listappend

import (
	"slices"
	"testing"
)

func r(k string, vs ...string) Mop { return Mop{Key: k, Read: vs} }
func a(k, v string) Mop            { return Mop{Append: true, Key: k, Value: v} }

func txn(id int, mops ...Mop) Txn {
	return Txn{ID: id, Process: id, Call: int64(id), Return: int64(id), Mops: mops}
}

func inDoubt(id int, mops ...Mop) Txn {
	t := txn(id, mops...)
	t.Status = Unknown
	return t
}

// during sets when a transaction began and returned.
func during(t Txn, call, ret int64) Txn {
	t.Call, t.Return = call, ret
	return t
}

func kinds(as []Anomaly) []string {
	var out []string
	for _, x := range as {
		out = append(out, x.Kind)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func TestAnomalies(t *testing.T) {
	aborted := txn(1, a("x", "1"))
	aborted.Status = Aborted
	for _, tc := range []struct {
		name string
		txns []Txn
		opts Options
		want []string
	}{
		{"serial", []Txn{
			txn(1, r("x"), a("x", "1")),
			txn(2, r("x", "1"), a("x", "2")),
			txn(3, r("x", "1", "2")),
		}, Options{}, nil},
		{"G0", []Txn{
			txn(1, a("x", "1"), a("y", "1")),
			txn(2, a("x", "2"), a("y", "2")),
			txn(3, r("x", "1", "2"), r("y", "2", "1")),
		}, Options{}, []string{"G0"}},
		{"G1c", []Txn{
			txn(1, a("x", "1"), r("y", "2")),
			txn(2, a("y", "2"), r("x", "1")),
		}, Options{}, []string{"G1c"}},
		{"lost update is G-single", []Txn{
			txn(1, r("x"), a("x", "1")),
			txn(2, r("x"), a("x", "2")),
			txn(3, r("x", "1", "2")),
		}, Options{}, []string{"G-single"}},
		{"write skew is G2 only", []Txn{
			txn(1, r("x"), r("y"), a("x", "1")),
			txn(2, r("x"), r("y"), a("y", "1")),
			txn(3, r("x", "1"), r("y", "1")),
		}, Options{}, []string{"G2"}},
		{"write skew nobody read the end of", []Txn{
			txn(1, r("y"), a("x", "1")),
			txn(2, r("x"), a("y", "2")),
		}, Options{}, []string{"G2"}},
		{"lost update nobody read the end of", []Txn{
			// Which of the unread appends came first is unknown, but
			// whichever did, the other read before it and appended after.
			txn(1, r("x"), a("x", "1")),
			txn(2, r("x", "1"), a("x", "2")),
			txn(3, r("x", "1"), a("x", "3")),
		}, Options{}, []string{"G-single"}},
		{"lost update from an empty key nobody read", []Txn{
			txn(1, r("x"), a("x", "1")),
			txn(2, r("x"), a("x", "2")),
		}, Options{}, []string{"G-single"}},
		{"lost update from different versions nobody read the end of", []Txn{
			txn(1, a("x", "0")),
			txn(2, r("x"), a("x", "2")),
			txn(3, r("x", "0"), a("x", "3")),
			txn(4, r("x", "0")),
		}, Options{}, []string{"G-single"}},
		{"blind appends nobody read are no lost update", []Txn{
			txn(1, a("x", "1")),
			txn(2, r("x"), a("x", "2")),
		}, Options{}, nil},
		{"serial, its last appends unread", []Txn{
			txn(1, r("x"), a("x", "1")),
			txn(2, r("x", "1"), a("x", "2"), a("y", "2")),
			txn(3, r("y"), a("z", "3")),
		}, Options{}, nil},
		{"G-nonadjacent", []Txn{
			// T1 -rw-> T2 -wr-> T3 -rw-> T4 -wr-> T1: two rw edges, apart.
			txn(1, r("x"), r("w", "4")),
			txn(2, a("x", "2"), a("y", "2")),
			txn(3, r("y", "2"), r("z")),
			txn(4, a("z", "4"), a("w", "4")),
			txn(5, r("x", "2"), r("z", "4")),
		}, Options{}, []string{"G-nonadjacent"}},
		{"G1a", []Txn{aborted, txn(2, r("x", "1"))}, Options{}, []string{"G1a"}},
		{"in doubt, but read", []Txn{
			// T2 read T1's append to x, so T1 took effect, its append to
			// y included; T2 read y without it.
			inDoubt(1, a("x", "1"), a("y", "1")),
			txn(2, r("x", "1"), r("y")),
		}, Options{}, []string{"G-single"}},
		{"in doubt, and read late", []Txn{
			// T1's client gave up before T2 began, and T3 read T1's
			// append after: it may have landed between the two.
			inDoubt(1, a("x", "1")),
			txn(2, r("x")),
			txn(3, r("x", "1")),
		}, Options{Realtime: true}, nil},
		{"in doubt, and read late after an append", []Txn{
			inDoubt(1, a("x", "1")),
			txn(2, r("x"), a("x", "2")),
			txn(3, r("x", "2", "1")),
		}, Options{Realtime: true}, nil},
		{"in doubt, and began after", []Txn{
			// T1 returned before T2 began, so T2's append, which T3 read,
			// cannot be before T1's.
			during(txn(1, a("x", "1")), 1, 2),
			during(inDoubt(2, a("x", "2")), 3, 4),
			during(txn(3, r("x", "2", "1")), 5, 6),
		}, Options{Realtime: true}, []string{"G1c"}},
		{"in doubt, and read before it began", []Txn{
			during(txn(1, r("x", "2")), 1, 2),
			during(inDoubt(2, a("x", "2")), 5, 6),
		}, Options{Realtime: true}, []string{"G1c"}},
		{"in doubt, began after, and nothing committed after", []Txn{
			// T3 overlaps T1, so no committed transaction began after T1
			// returned to carry its order on: T2 gets T1's edge itself.
			during(txn(1, a("x", "1")), 1, 2),
			during(inDoubt(2, a("x", "2")), 3, 4),
			during(txn(3, r("x", "2", "1")), 1, 6),
		}, Options{Realtime: true}, []string{"G1c"}},
		{"in doubt, and began while another ran", []Txn{
			during(txn(1, a("x", "1")), 1, 4),
			during(inDoubt(2, a("x", "2")), 3, 5),
			during(txn(3, r("x", "2", "1")), 6, 7),
		}, Options{Realtime: true}, nil},
		{"in doubt, and began after, without realtime", []Txn{
			during(txn(1, a("x", "1")), 1, 2),
			during(inDoubt(2, a("x", "2")), 3, 4),
			during(txn(3, r("x", "2", "1")), 5, 6),
		}, Options{}, nil},
		{"in doubt, read, and then missed", []Txn{
			// T2 read T1's append, so it had landed before T3 began.
			inDoubt(1, a("x", "1")),
			txn(2, r("x", "1")),
			txn(3, r("x")),
		}, Options{Realtime: true}, []string{"G-single"}},
		{"in doubt and unread", []Txn{
			inDoubt(1, a("x", "1"), a("y", "1")),
			txn(2, r("x"), r("y")),
		}, Options{Realtime: true}, nil},
		{"G1a after an append of its own", []Txn{aborted, txn(2, a("x", "2"), r("x", "1", "2"))}, Options{}, []string{"G1a"}},
		{"G1a of an append nobody made", []Txn{txn(1, a("x", "1"), r("x", "0", "1"))}, Options{}, []string{"G1a"}},
		{"G1b after an append of its own", []Txn{
			txn(1, a("x", "1"), a("y", "1"), a("x", "2")),
			txn(2, a("x", "3"), r("x", "1", "3"), r("y", "1")),
			txn(3, r("x", "1", "3", "2")),
		}, Options{}, []string{"G0", "G1b"}},
		{"lost after an append of its own", []Txn{
			// T1's append is unread, so after T2's; T1 committed first.
			txn(1, a("x", "1")),
			txn(2, a("x", "2"), r("x", "2")),
		}, Options{Realtime: true}, []string{"G1c", "lost"}},
		{"G1b in a later read", []Txn{
			txn(1, a("x", "1"), a("x", "2")),
			txn(2, r("x"), r("x", "1"), r("x", "1")),
		}, Options{}, []string{"G-single", "G1b", "nonrepeatable"}},
		{"lost in a later read", []Txn{
			txn(1, a("x", "1")),
			txn(2, a("x", "2")),
			txn(3, r("x", "1", "2"), r("x", "1"), r("x", "1")),
		}, Options{Realtime: true}, []string{"G-single", "lost", "nonrepeatable"}},
		{"G1b", []Txn{
			// What T2 read is after T1's first append and before its
			// second, a cycle too.
			txn(1, a("x", "1"), a("x", "2")),
			txn(2, r("x", "1")),
		}, Options{}, []string{"G-single", "G1b"}},
		{"an append without the one its transaction made before", []Txn{
			txn(1, a("x", "1"), a("x", "2")),
			txn(2, r("x", "2")),
		}, Options{}, []string{"G-single", "incompatible-order"}},
		{"a transaction's appends out of order", []Txn{
			txn(1, a("x", "1"), a("x", "2")),
			txn(2, r("x", "2", "1")),
		}, Options{}, []string{"G1b", "incompatible-order"}},
		{"a transaction's appends apart", []Txn{
			txn(1, r("x"), a("x", "1"), a("x", "2")),
			txn(2, r("x"), a("x", "3")),
			txn(3, r("x", "1", "3", "2")),
		}, Options{}, []string{"G-single", "G0"}},
		{"incompatible order", []Txn{
			txn(1, a("x", "1")),
			txn(2, a("x", "2")),
			txn(3, r("x", "1", "2")),
			txn(4, r("x", "2", "1")),
		}, Options{}, []string{"incompatible-order"}},
		{"internal", []Txn{
			txn(1, a("x", "1"), r("x")),
		}, Options{}, []string{"internal"}},
		{"internal after a read", []Txn{
			txn(1, a("x", "1")),
			txn(2, r("x", "1"), a("x", "2"), r("x", "1")),
		}, Options{}, []string{"internal"}},
		{"its own appends, read again", []Txn{
			txn(1, a("x", "1")),
			txn(2, a("x", "2"), r("x", "1", "2"), a("x", "3"), r("x", "1", "2", "3")),
		}, Options{}, nil},
		{"G1c through a later read", []Txn{
			// T1's second read of x is after T2's append; its read of y is
			// not, so T1 read T2's write and T2 read T1's.
			txn(1, a("y", "1"), r("x"), r("x", "2")),
			txn(2, a("x", "2"), r("y", "1")),
		}, Options{}, []string{"G-single", "G1c", "nonrepeatable"}},
		{"nonrepeatable", []Txn{
			// T1 read before T2's append, then after it: a cycle too.
			txn(1, r("x"), r("x", "1")),
			txn(2, a("x", "1")),
		}, Options{}, []string{"G-single", "nonrepeatable"}},
		{"nonrepeatable around an append of its own", []Txn{
			txn(1, a("x", "1")),
			// And a lost update's cycle: T2 read x before T1's append and
			// appended after it.
			txn(2, r("x"), a("y", "2"), a("x", "2"), r("x", "1", "2")),
		}, Options{}, []string{"G-single", "nonrepeatable"}},
		{"lost with realtime", []Txn{
			// T2 began after T1 committed and read what T1 overwrote.
			txn(1, a("x", "1")),
			txn(2, r("x")),
		}, Options{Realtime: true}, []string{"G-single", "lost"}},
		{"stale read without realtime is fine", []Txn{
			txn(1, a("x", "1")),
			txn(2, r("x")),
		}, Options{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Check(tc.txns, tc.opts)
			if k := kinds(got); !slices.Equal(k, tc.want) {
				for _, x := range got {
					t.Logf("%s: %s", x.Kind, x.Detail)
				}
				t.Fatalf("anomalies %v, want %v", k, tc.want)
			}
		})
	}
}

// TestReportedOnce: what several reads of a transaction show alike is
// reported once.
func TestReportedOnce(t *testing.T) {
	got := Check([]Txn{
		txn(1, a("x", "1"), a("x", "2")),
		txn(2, r("x", "1"), r("x", "1"), r("x", "1")),
	}, Options{Realtime: true})
	n := map[string]int{}
	for _, x := range got {
		n[x.Kind]++
	}
	if n["G1b"] != 1 || n["lost"] != 1 {
		t.Fatalf("reported %v", n)
	}
}

func TestForbidden(t *testing.T) {
	si := Forbidden("snapshot-isolation")
	if slices.Contains(si, "G2") || !slices.Contains(si, "G-single") {
		t.Fatalf("snapshot isolation forbids %v", si)
	}
	if ser := Forbidden("serializable"); !slices.Contains(ser, "G2") {
		t.Fatalf("serializability forbids %v", ser)
	}
	// Read committed lets a transaction see what committed between its
	// reads, and nothing lets it miss its own appends.
	rc := Forbidden("read-committed")
	if slices.Contains(rc, "nonrepeatable") || !slices.Contains(rc, "internal") || !slices.Contains(si, "nonrepeatable") {
		t.Fatalf("read committed forbids %v, snapshot isolation %v", rc, si)
	}
}
