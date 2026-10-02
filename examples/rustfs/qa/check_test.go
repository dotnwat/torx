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

	"github.com/dotnwat/torx/linearize"
)

func kop(call, ret int64, f, value, expect string, r result, read string) linearize.Op[keyIn, keyOut] {
	if r == unknown {
		ret = linearize.Pending
	}
	return linearize.Op[keyIn, keyOut]{Call: call, Return: ret, Input: keyIn{f: f, value: value, expect: expect}, Output: keyOut{result: r, read: read}}
}

func TestKeyModel(t *testing.T) {
	for _, tc := range []struct {
		name string
		ops  []linearize.Op[keyIn, keyOut]
		want linearize.Outcome
	}{
		{"read your write", []linearize.Op[keyIn, keyOut]{kop(0, 1, "put", "a", "", done, ""), kop(2, 3, "get", "", "", done, "a")}, linearize.Ok},
		{"lost write", []linearize.Op[keyIn, keyOut]{kop(0, 1, "put", "a", "", done, ""), kop(2, 3, "get", "", "", done, "")}, linearize.Illegal},
		{"create twice", []linearize.Op[keyIn, keyOut]{kop(0, 5, "put-if-absent", "a", "", done, ""), kop(0, 5, "put-if-absent", "b", "", done, "")}, linearize.Illegal},
		{"create once", []linearize.Op[keyIn, keyOut]{kop(0, 5, "put-if-absent", "a", "", done, ""), kop(0, 5, "put-if-absent", "b", "", precondition, "")}, linearize.Ok},
		{"cas twice from one value", []linearize.Op[keyIn, keyOut]{
			kop(0, 1, "put", "a", "", done, ""),
			kop(2, 5, "put-if-match", "b", "a", done, ""), kop(2, 5, "put-if-match", "c", "a", done, ""),
		}, linearize.Illegal},
		{"cas on no object", []linearize.Op[keyIn, keyOut]{
			kop(0, 1, "put", "a", "", done, ""), kop(2, 3, "delete", "", "", done, ""),
			kop(4, 5, "put-if-match", "b", "a", done, ""),
		}, linearize.Illegal},
		{"cas on no object fails", []linearize.Op[keyIn, keyOut]{
			kop(0, 1, "put", "a", "", done, ""), kop(2, 3, "delete", "", "", done, ""),
			kop(4, 5, "put-if-match", "b", "a", noSuchKey, ""),
		}, linearize.Ok},
		{"unanswered delete may land", []linearize.Op[keyIn, keyOut]{
			kop(0, 1, "put", "a", "", done, ""), kop(2, 3, "delete", "", "", unknown, ""),
			kop(4, 5, "list", "", "", done, ""), kop(6, 7, "get", "", "", done, ""),
		}, linearize.Ok},
		{"unanswered delete lands and unlands", []linearize.Op[keyIn, keyOut]{
			kop(0, 1, "put", "a", "", done, ""), kop(2, 3, "delete", "", "", unknown, ""),
			kop(4, 5, "list", "", "", done, ""), kop(6, 7, "get", "", "", done, "a"),
		}, linearize.Illegal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := linearize.Check(context.Background(), keyModel(false, allowed{}), tc.ops)
			if err != nil || res.Outcome != tc.want {
				t.Fatalf("got %v %v, want %v", res.Outcome, err, tc.want)
			}
		})
	}
}

func TestPrune(t *testing.T) {
	ops := []linearize.Op[keyIn, keyOut]{
		kop(0, 0, "put", "a", "", unknown, ""), // seen by the get: returns by its end
		kop(0, 0, "put", "b", "", unknown, ""), // never seen: left out
		kop(5, 9, "get", "", "", done, "a"),
	}
	src := make([]Op, len(ops))
	for i := range src {
		src[i].Value = ops[i].Input.value
	}
	pops, psrc := prune(ops, src)
	if len(pops) != 2 || pops[0].Return != 9 || psrc[1].Value != "" {
		t.Fatalf("pruned to %+v", pops)
	}
}

// TestCheckHistory checks a history a chaos run saved, named by
// RUSTFS_HISTORY, with the keys k0..k(RUSTFS_KEYS-1): how to try the
// checker on a run again without running it.
func TestCheckHistory(t *testing.T) {
	path := os.Getenv("RUSTFS_HISTORY")
	if path == "" {
		t.Skip("RUSTFS_HISTORY names no history")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var ops []Op
	keys := map[string]bool{}
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<24)
	for sc.Scan() {
		var op Op
		if err := json.Unmarshal(sc.Bytes(), &op); err != nil {
			t.Fatal(err)
		}
		ops = append(ops, op)
		if op.Key != "" && op.Process != "nemesis" {
			keys[op.Key] = true
		}
		for k := range op.Listed {
			keys[k] = true
		}
	}
	var ks []string
	for k := range keys {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	start := time.Now()
	for _, a := range checkLinear(context.Background(), keyModel(os.Getenv("RUSTFS_VERSIONED") != "", allowed{ifMatchOnMarker: os.Getenv("RUSTFS_STRICT") == "", deleteIfMatchMissing: os.Getenv("RUSTFS_STRICT") == ""}), ops, ks, 5*time.Minute) {
		t.Errorf("%s %s: %s", a.Kind, a.Key, a.Detail)
		for _, op := range a.Ops {
			t.Logf("   %v-%v %s %s", op.Start.Round(time.Millisecond), op.End.Round(time.Millisecond), op.Process, describe(op))
		}
	}
	for _, a := range checkVersions(ops) {
		t.Errorf("%s %s: %s", a.Kind, a.Key, a.Detail)
	}
	t.Logf("checked %d keys in %v", len(ks), time.Since(start))
}

func TestCheckVersions(t *testing.T) {
	ops := []Op{
		{Process: "c1", F: "put", Key: "k", Value: "a", Version: "v1", Start: 0, End: 1, Outcome: Ok},
		{Process: "c1", F: "put", Key: "k", Value: "b", Version: "v2", Start: 2, End: 3, Outcome: Ok},
		{Process: "c2", F: "put", Key: "k", Value: "c", Start: 2, End: 3, Outcome: Info},
		{Process: "c2", F: "delete", Key: "k", Version: "m", Start: 4, End: 5, Outcome: Ok},
		{Process: "c3", F: "delete", Key: "k", Start: 6, End: 7, Outcome: Info},
	}
	list := func(vs ...VersionEntry) Op {
		return Op{Process: "final", F: "list-versions", Node: "n1", Outcome: Ok, Versions: vs}
	}
	at := func(sec int) string { return time.Unix(int64(sec), 0).UTC().Format(time.RFC3339Nano) }
	v1 := VersionEntry{Key: "k", Version: "v1", Value: "a", Modified: at(1)}
	v2 := VersionEntry{Key: "k", Version: "v2", Value: "b", Modified: at(3)}
	m := VersionEntry{Key: "k", Version: "m", DeleteMarker: true, Modified: at(5)}
	// RustFS lists delete markers before versions, whatever their age;
	// one marker no acknowledged delete made is the unanswered delete's.
	x := VersionEntry{Key: "k", Version: "x", DeleteMarker: true, Modified: at(7)}
	y := VersionEntry{Key: "k", Version: "y", Value: "c", Modified: at(3)}
	if a := checkVersions(append(slices.Clone(ops), list(x, m, y, v2, v1))); len(a) != 0 {
		t.Fatalf("a good list: %+v", a)
	}
	old := m
	old.Modified = at(0)
	for _, tc := range []struct {
		name string
		list Op
		kind string
	}{
		{"lost", list(m, v2), "lost-version"},
		{"order", list(m, v1, v2), "version-order"},
		{"marker older", list(old, v2, v1), "version-order"},
		{"phantom", list(m, v2, v1, VersionEntry{Key: "k", Version: "z", Value: "q"}), "phantom-version"},
		{"wrong value", list(m, VersionEntry{Key: "k", Version: "v2", Value: "a"}, v1), "phantom-version"},
	} {
		got := checkVersions(append(slices.Clone(ops), tc.list))
		if !slices.ContainsFunc(got, func(a anomaly) bool { return a.Kind == tc.kind }) {
			t.Errorf("%s: %+v, want %s", tc.name, got, tc.kind)
		}
	}
}
