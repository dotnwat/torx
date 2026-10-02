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
			// Which of the unread appends came first is unknown, so the
			// ww edge between them is: G2, where a read of both would
			// have shown G-single.
			txn(1, r("x"), a("x", "1")),
			txn(2, r("x", "1"), a("x", "2")),
			txn(3, r("x", "1"), a("x", "3")),
		}, Options{}, []string{"G2"}},
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
		{"G1b", []Txn{
			// What T2 read is after T1's first append and before its
			// second, a cycle too.
			txn(1, a("x", "1"), a("x", "2")),
			txn(2, r("x", "1")),
		}, Options{}, []string{"G-single", "G1b"}},
		{"incompatible order", []Txn{
			txn(1, a("x", "1")),
			txn(2, a("x", "2")),
			txn(3, r("x", "1", "2")),
			txn(4, r("x", "2", "1")),
		}, Options{}, []string{"incompatible-order"}},
		{"internal", []Txn{
			txn(1, a("x", "1"), r("x")),
		}, Options{}, []string{"internal"}},
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

func TestForbidden(t *testing.T) {
	si := Forbidden("snapshot-isolation")
	if slices.Contains(si, "G2") || !slices.Contains(si, "G-single") {
		t.Fatalf("snapshot isolation forbids %v", si)
	}
	if ser := Forbidden("serializable"); !slices.Contains(ser, "G2") {
		t.Fatalf("serializability forbids %v", ser)
	}
}
