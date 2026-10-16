//go:build unix

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/dotnwat/torx/examples/kudu/qa/kudu"
)

// TestCheckHistory checks a history a run recorded, named by
// $KUDU_HISTORY, as the job did: to look into a run's verdict, or time the
// checker on a real history.
func TestCheckHistory(t *testing.T) {
	path := os.Getenv("KUDU_HISTORY")
	if path == "" {
		t.Skip("KUDU_HISTORY names no history")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var hist []op
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<20), 64<<20)
	for sc.Scan() {
		var o op
		if err := json.Unmarshal(sc.Bytes(), &o); err != nil {
			t.Fatal(err)
		}
		hist = append(hist, o)
	}
	start := time.Now()
	byKey, bad := keyHistories(hist)
	for k, xs := range byKey {
		pending := 0
		for _, x := range xs {
			if x.op.Output.Unknown {
				pending++
			}
		}
		if pending > 0 || len(xs) > 2000 {
			t.Logf("key %d: %d ops, %d unknown", k, len(xs), pending)
		}
	}
	kv, unknown := checkKeys(context.Background(), byKey, checkBudget)
	bad = append(bad, kv...)
	bad = append(bad, opChecks(hist, possiblyWritten(hist))...)
	bad = append(bad, txnCheck(hist, -1)...)
	bad = append(bad, diffCheck(hist)...)
	t.Logf("checked %d keys in %v; %d undecided: %v", len(byKey), time.Since(start), len(unknown), unknown)
	for _, a := range bad {
		t.Log(a)
	}
}

// TestKeyHistories checks the per-key model on small histories: a
// snapshot at t holds the writes before t and not the one at t; a write
// whose outcome is unknown may take effect at any later timestamp, but
// once; a refused write says what the row held at its timestamp.
func TestKeyHistories(t *testing.T) {
	w := func(i int, kind, v, code string, ts int64) op {
		return op{Index: i, Kind: "write", Ops: []kudu.Op{{Op: kind, K: 1, V: v}},
			Results: []kudu.OpResult{{Code: code, TS: ts}}, PropBefore: -1}
	}
	unknown := func(i int, kind, v string, prop int64) op {
		return op{Index: i, Kind: "write", Ops: []kudu.Op{{Op: kind, K: 1, V: v}}, Code: "TimedOut", PropBefore: prop}
	}
	r := func(i int, ts int64, v string) op {
		o := op{Index: i, Kind: "read", Read: &kudu.ReadReq{Mode: "snapshot", Keys: []int64{1}}, SnapshotTS: ts, PropBefore: -1}
		if v != "" {
			o.Rows = []row{{K: 1, V: v}}
		}
		return o
	}
	for _, tc := range []struct {
		name string
		hist []op
		ok   bool
	}{
		{"read after write", []op{w(0, "upsert", "a", "OK", 10<<12), r(1, 11<<12, "a")}, true},
		{"snapshot excludes its own timestamp", []op{w(0, "upsert", "a", "OK", 10<<12), r(1, 10<<12, "a")}, false},
		{"snapshot before write", []op{w(0, "upsert", "a", "OK", 10<<12), r(1, 9<<12, "")}, true},
		{"stale read", []op{w(0, "upsert", "a", "OK", 10<<12), w(1, "upsert", "b", "OK", 20<<12), r(2, 30<<12, "a")}, false},
		{"insert refused on an absent row", []op{w(0, "insert", "a", "AlreadyPresent", 10<<12)}, false},
		{"insert refused on a present row", []op{w(0, "upsert", "a", "OK", 10<<12), w(1, "insert", "b", "AlreadyPresent", 20<<12), r(2, 30<<12, "a")}, true},
		{"update of an absent row", []op{w(0, "update", "a", "OK", 10<<12)}, false},
		{"delete then read", []op{w(0, "upsert", "a", "OK", 10<<12), w(1, "delete", "", "OK", 20<<12), r(2, 30<<12, "")}, true},
		{"unknown write seen", []op{unknown(0, "upsert", "a", 5<<12), r(1, 20<<12, "a")}, true},
		{"unknown write seen then unseen", []op{unknown(0, "upsert", "a", 5<<12), r(1, 20<<12, "a"), r(2, 30<<12, "")}, false},
		{"unknown write before its lower bound", []op{unknown(0, "upsert", "a", 25<<12), r(1, 20<<12, "a")}, false},
		{"phantom", []op{r(0, 20<<12, "z")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			byKey, bad := keyHistories(tc.hist)
			if len(bad) > 0 {
				t.Fatalf("anomalies building histories: %v", bad)
			}
			kv, unknown := checkKeys(context.Background(), byKey, 10*time.Second)
			if len(unknown) > 0 {
				t.Fatalf("undecided: %v", unknown)
			}
			if got := len(kv) == 0; got != tc.ok {
				t.Fatalf("legal = %v, want %v: %v", got, tc.ok, kv)
			}
		})
	}
}

// TestDiffCheck checks the diff-scan check on a key written twice: a diff
// scan returns the key, as at its end, exactly when its state changed.
func TestDiffCheck(t *testing.T) {
	writes := []op{
		{Index: 0, Kind: "write", Ops: []kudu.Op{{Op: "upsert", K: 1, V: "a"}}, Results: []kudu.OpResult{{Code: "OK", TS: 10}}},
		{Index: 1, Kind: "write", Ops: []kudu.Op{{Op: "upsert", K: 1, V: "b"}}, Results: []kudu.OpResult{{Code: "OK", TS: 20}}},
		{Index: 2, Kind: "write", Ops: []kudu.Op{{Op: "delete", K: 1}}, Results: []kudu.OpResult{{Code: "OK", TS: 30}}},
	}
	diff := func(s, e int64, rows ...row) op {
		return op{Index: 9, Kind: "read", Read: &kudu.ReadReq{Mode: "diff", StartTS: s, TS: e, Lo: new(int64(0)), Hi: new(int64(2))}, Rows: rows}
	}
	for _, tc := range []struct {
		name string
		scan op
		want string
	}{
		{"changed and returned", diff(15, 25, row{K: 1, V: "b"}), ""},
		{"changed and left out", diff(15, 25), "diff-missing"},
		{"unchanged and left out", diff(21, 29), ""},
		{"unchanged and returned", diff(21, 29, row{K: 1, V: "b"}), "diff-extra"},
		{"returned with a stale value", diff(5, 25, row{K: 1, V: "a"}), "diff-wrong"},
		{"deleted and returned so", diff(25, 35, row{K: 1, Del: true}), ""},
		{"deleted and returned alive", diff(25, 35, row{K: 1, V: "b"}), "diff-wrong"},
		{"absent at both ends, returned deleted", diff(5, 8, row{K: 1, Del: true}), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := diffCheck(append(slices.Clone(writes), tc.scan))
			got := ""
			if len(bad) > 0 {
				got = bad[0].Kind
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q: %v", got, tc.want, bad)
			}
		})
	}
}

func TestMismatched(t *testing.T) {
	ksck := `Checksum Summary
T aaa P p1 (10.77.0.15:20000): Checksum: 1
T aaa P p2 (10.77.0.18:20002): Checksum: 1
T bbb P p1 (10.77.0.15:20000): Checksum: 2
T bbb P p3 (10.77.0.19:20002): Checksum: 3
T ccc P p2 (10.77.0.18:20002): Checksum: 4
`
	if got := mismatched(ksck); !slices.Equal(got, []string{"bbb"}) {
		t.Fatalf("mismatched = %v, want [bbb]", got)
	}
}
