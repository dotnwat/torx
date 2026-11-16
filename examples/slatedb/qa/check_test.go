//go:build unix

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dotnwat/torx/examples/slatedb/qa/slatedb"
)

// h builds histories: each op's times are its index in steps.
type h struct {
	ops []op
	t0  time.Time
}

func (b *h) at(i int) time.Time { return b.t0.Add(time.Duration(i) * time.Millisecond) }

func (b *h) add(o op, call, ret int) {
	o.ID = len(b.ops)
	o.Call, o.Return = b.at(call), b.at(ret)
	if o.Outcome == "" {
		o.Outcome = "ok"
	}
	if o.Proc == "" {
		o.Proc = "w0"
	}
	if o.Launch == 0 {
		o.Launch = 1
	}
	b.ops = append(b.ops, o)
}

func put(k, v string, seq uint64) op {
	var vp *string
	if v != "" {
		vp = new(v)
	}
	return op{Kind: opPut, Writes: []write{{k, vp}}, Durable: true, Seq: seq}
}

func get(k, v string, seq uint64) op {
	r := read{}
	if v != "" {
		r = read{Found: true, Value: v, Seq: seq}
	}
	return op{Kind: opGet, Key: k, Read: map[string]read{k: r}}
}

func scan(kv map[string]read) op { return op{Kind: opScan, Read: kv} }

func kinds(as []anomaly) []string {
	var out []string
	for _, a := range as {
		out = append(out, a.Kind)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func run(t *testing.T, b *h) []string {
	t.Helper()
	as, unknown := newChecker(b.ops).check(context.Background())
	if len(unknown) > 0 {
		t.Fatalf("unchecked: %v", unknown)
	}
	return kinds(as)
}

// A scan that misses a delete's sequence number is a snapshot when the
// delete explains what it read.
func TestScanSnapshotAfterDelete(t *testing.T) {
	b := &h{t0: time.Unix(0, 0)}
	b.add(put("a", "a1", 10), 0, 1)
	b.add(put("b", "b1", 12), 0, 1)
	b.add(put("a", "", 15), 2, 3) // delete a at 15
	b.add(scan(map[string]read{"a": {}, "b": {Found: true, Value: "b1", Seq: 12}}), 4, 5)
	if got := run(t, b); len(got) != 0 {
		t.Fatalf("anomalies %v, want none", got)
	}
}

// A scan that sees a newer write of one key but an older value of another
// is no snapshot.
func TestScanNotSnapshot(t *testing.T) {
	b := &h{t0: time.Unix(0, 0)}
	b.add(put("a", "a1", 10), 0, 1)
	b.add(put("a", "a2", 20), 2, 3)
	b.add(put("b", "b1", 25), 4, 5)
	// A scan concurrent with everything, so linearizability allows it,
	// that sees b at 25 but a at 10.
	b.add(scan(map[string]read{"a": {Found: true, Value: "a1", Seq: 10}, "b": {Found: true, Value: "b1", Seq: 25}}), 0, 6)
	if got := run(t, b); !slices.Equal(got, []string{"scan-not-snapshot"}) {
		t.Fatalf("anomalies %v, want scan-not-snapshot", got)
	}
}

// A write acknowledged durable that a later read does not see is lost.
func TestLostDurableWrite(t *testing.T) {
	b := &h{t0: time.Unix(0, 0)}
	b.add(put("a", "a1", 10), 0, 1)
	b.add(put("a", "a2", 11), 2, 3)
	b.add(get("a", "a1", 10), 4, 5)
	if got := run(t, b); !slices.Equal(got, []string{"nonlinearizable"}) {
		t.Fatalf("anomalies %v, want nonlinearizable", got)
	}
}

// A write that did not await durability may be lost -- unless a later
// write of the same writer was acknowledged durable.
func TestNonDurableWrite(t *testing.T) {
	lost := func(laterDurable bool, launch int) []string {
		b := &h{t0: time.Unix(0, 0)}
		b.add(put("a", "a1", 10), 0, 1)
		nd := put("a", "a2", 11)
		nd.Durable = false
		b.add(nd, 2, 3)
		if laterDurable {
			later := put("b", "b1", 12)
			later.Launch = launch
			b.add(later, 4, 5)
		}
		b.add(get("a", "a1", 10), 6, 7)
		return run(t, b)
	}
	if got := lost(false, 1); len(got) != 0 {
		t.Fatalf("a lost write that never awaited durability: anomalies %v, want none", got)
	}
	if got := lost(true, 1); !slices.Equal(got, []string{"nonlinearizable"}) {
		t.Fatalf("a lost write before a durable one: anomalies %v, want nonlinearizable", got)
	}
	if got := lost(true, 2); len(got) != 0 {
		t.Fatalf("a durable write of another launch makes nothing of the first durable: anomalies %v, want none", got)
	}
}

// A reader that reads a key at one seq and later at an older one went
// back in time; one that later finds it absent did too, unless a delete
// may have come after.
func TestReaderMonotonic(t *testing.T) {
	check := func(second op, deleteAfter bool) []string {
		b := &h{t0: time.Unix(0, 0)}
		b.add(put("a", "a1", 10), 0, 1)
		b.add(put("a", "a2", 20), 2, 3)
		if deleteAfter {
			b.add(put("a", "", 30), 4, 5)
		}
		first := get("a", "a2", 20)
		first.Proc, first.Reader = "r0", true
		b.add(first, 6, 7)
		second.Proc, second.Reader = "r0", true
		b.add(second, 8, 9)
		return run(t, b)
	}
	if got := check(get("a", "a1", 10), false); !slices.Equal(got, []string{"reader-went-back"}) {
		t.Fatalf("older value: anomalies %v, want reader-went-back", got)
	}
	if got := check(get("a", "", 0), false); !slices.Equal(got, []string{"reader-went-back"}) {
		t.Fatalf("absent with no delete after: anomalies %v, want reader-went-back", got)
	}
	if got := check(get("a", "", 0), true); len(got) != 0 {
		t.Fatalf("absent after a delete: anomalies %v, want none", got)
	}
	if got := check(get("a", "a2", 20), false); len(got) != 0 {
		t.Fatalf("same value: anomalies %v, want none", got)
	}
}

// A merge acknowledged durable that a read begun after it does not see is
// lost; reads that disagree on the order of appends disagree.
func TestMerges(t *testing.T) {
	t0 := time.Unix(0, 0)
	at := func(i int) time.Time { return t0.Add(time.Duration(i) * time.Millisecond) }
	app := func(id int, k, v string, call, ret int) txnRecord {
		return txnRecord{ID: id, Client: id, Call: at(call), Return: at(ret), Status: 0,
			Ops: []slatedb.TxnOp{{Op: "append", Key: k, Value: v}}}
	}
	rd := func(id int, k, got string, call, ret int) txnRecord {
		return txnRecord{ID: id, Client: id, Call: at(call), Return: at(ret), Status: 0,
			Ops: []slatedb.TxnOp{{Op: "get", Key: k}}, Results: []*string{&got}}
	}
	ok := []txnRecord{app(0, "m", "a", 0, 1), app(1, "m", "b", 2, 3), rd(2, "m", "a b", 4, 5)}
	if as := checkMerges(ok); len(as) != 0 {
		t.Fatalf("anomalies %v, want none", as)
	}
	lost := []txnRecord{app(0, "m", "a", 0, 1), app(1, "m", "b", 2, 3), rd(2, "m", "a", 4, 5)}
	if as := checkMerges(lost); len(as) == 0 || as[0].Kind != "merge-lost" {
		t.Fatalf("anomalies %v, want merge-lost", as)
	}
	order := []txnRecord{app(0, "m", "a", 0, 1), app(1, "m", "b", 0, 1), rd(2, "m", "a b", 4, 5), rd(3, "m", "b a", 6, 7)}
	if as := checkMerges(order); len(as) == 0 || as[0].Kind != "merge-incompatible-order" {
		t.Fatalf("anomalies %v, want merge-incompatible-order", as)
	}
}

// TestRecheck checks a history a run wrote, named by SLATEDB_HISTORY.
func TestRecheck(t *testing.T) {
	path := os.Getenv("SLATEDB_HISTORY")
	if path == "" {
		t.Skip("SLATEDB_HISTORY names no history")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var ops []op
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		var o op
		if err := json.Unmarshal(sc.Bytes(), &o); err != nil {
			t.Fatal(err)
		}
		ops = append(ops, o)
	}
	as, unknown := newChecker(ops).check(context.Background())
	for _, a := range as {
		t.Errorf("%s %s: %s", a.Kind, a.Key, a.Detail)
	}
	for _, u := range unknown {
		t.Logf("unchecked: %s", u)
	}
}

// TestRecheckTxns checks the transactions a run wrote, named by
// SLATEDB_TXNS, at the isolation SLATEDB_ISOLATION names (ssi by default).
func TestRecheckTxns(t *testing.T) {
	path := os.Getenv("SLATEDB_TXNS")
	if path == "" {
		t.Skip("SLATEDB_TXNS names no transactions")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var recs []txnRecord
	for line := range strings.SplitSeq(strings.TrimSpace(string(b)), "\n") {
		var r txnRecord
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatal(err)
		}
		recs = append(recs, r)
	}
	iso := os.Getenv("SLATEDB_ISOLATION")
	if iso == "" {
		iso = "ssi"
	}
	bad, allowed := checkTxns(recs, iso)
	for _, a := range bad {
		t.Errorf("%s %s: %s", a.Kind, a.Key, a.Detail)
	}
	for _, a := range allowed {
		t.Logf("allowed: %s %s", a.Kind, a.Detail)
	}
}
