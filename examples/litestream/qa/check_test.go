//go:build unix

package main

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dotnwat/torx/examples/litestream/model"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }

// history is a record to check: transactions 1..n committed in order on
// epoch 0, each writing one row.
type history struct {
	txns  []*txnRec
	specs map[uint64]model.Txn
	acks  []ackRec
	obs   []*obsRec
	kills []time.Time
	// snapshots are when Litestream wrote snapshots.
	snapshots []time.Time
}

func newHistory(n int) *history {
	h := &history{specs: map[uint64]model.Txn{}}
	for i := 1; i <= n; i++ {
		uid := uint64(i)
		h.specs[uid] = model.Txn{UID: uid, Ops: []model.Op{{Kind: model.Put, Table: 0, Key: int64(i % 3), Size: 50}}}
		h.txns = append(h.txns, &txnRec{UID: uid, Call: at(float64(i)), Return: at(float64(i) + 0.1), Outcome: committed, N: int64(i)})
	}
	return h
}

// copyOf is a copy holding uids, read at s seconds, whose contents are
// what they wrote.
func (h *history) copyOf(kind string, s float64, uids ...uint64) *obsRec {
	st := model.NewState()
	for _, uid := range uids {
		st.Apply(h.specs[uid])
	}
	o := &obsRec{ID: len(h.obs), Kind: kind, Call: at(s), Return: at(s + 0.5), TXID: "1",
		Report: &model.Report{Integrity: []string{"ok"}, TxLog: uids, Digest: st.Digest(), TableDigests: st.TableDigests()}}
	h.obs = append(h.obs, o)
	return o
}

func (h *history) check(allow ...string) (found, known []anomaly) {
	a := map[string]bool{}
	for _, x := range allow {
		a[x] = true
	}
	return newChecker(h.txns, h.specs, h.acks, h.obs, map[int][]uint64{0: nil}, 0, h.kills, h.snapshots, a).check()
}

func kindsOf(as []anomaly) []string {
	var out []string
	for _, a := range as {
		out = append(out, a.Kind)
	}
	slices.Sort(out)
	return out
}

func TestCheckClean(t *testing.T) {
	h := newHistory(5)
	h.acks = []ackRec{{Epoch: 0, Prefix: 3, Return: at(3.5)}}
	h.copyOf(obsRestore, 4, 1, 2, 3)
	h.copyOf(obsRestore, 6, 1, 2, 3, 4, 5)
	h.copyOf(obsPrimary, 7, 1, 2, 3, 4, 5)
	h.copyOf(obsFinal, 8, 1, 2, 3, 4, 5)
	if found, known := h.check(); len(found)+len(known) != 0 {
		t.Fatalf("a clean history: %v %v", found, known)
	}
}

func TestCheckLostAndBack(t *testing.T) {
	h := newHistory(5)
	h.acks = []ackRec{{Epoch: 0, Prefix: 4, Return: at(4.5)}}
	h.copyOf(obsRestore, 4, 1, 2, 3)         // before the ack: fine
	h.copyOf(obsRestore, 5, 1, 2)            // lost 3 and 4, and went back
	h.copyOf(obsRestore, 6, 1, 2, 3, 5)      // 5 where 4 should be
	o := h.copyOf(obsRestore, 7, 1, 2, 3, 4) // wrong contents
	o.Report.Digest.Sum++
	found, _ := h.check()
	want := []string{"lost-acked", "lost-acked", "not-a-prefix", "restore-went-back", "wrong-contents"}
	if got := kindsOf(found); !slices.Equal(got, want) {
		t.Fatalf("anomalies %v, want %v:\n%v", got, want, found)
	}
}

// A transaction in doubt may stand at a row the primary's log does not
// account for, once: a copy that holds it twice is not a prefix, and the
// anomaly says that is why, not that nothing was committed at the row.
func TestCheckHeldTwice(t *testing.T) {
	h := newHistory(4)
	h.txns[3].Outcome, h.txns[3].N = inDoubt, 0
	h.copyOf(obsRestore, 5, 1, 2, 3, 4) // in doubt, and it committed: fine
	h.copyOf(obsRestore, 6, 1, 2, 3, 4, 4)
	found, _ := h.check()
	var details []string
	for _, a := range found {
		if a.Kind == "not-a-prefix" {
			details = append(details, a.Detail)
		}
	}
	if len(details) != 1 || !strings.Contains(details[0], "at row 5 it holds 4 (") || !strings.HasSuffix(details[0], "which it holds at row 4 too") {
		t.Fatalf("not-a-prefix anomalies %q, want one for the second 4, at row 5; all: %v", details, found)
	}
	// A row the primary's log does account for still says what it holds.
	h = newHistory(4)
	h.copyOf(obsRestore, 5, 1, 2, 4)
	found, _ = h.check()
	if len(found) == 0 || !strings.Contains(found[0].Detail, "where epoch 0 committed 3 (") {
		t.Fatalf("a copy holding 4 at row 3: %v", found)
	}
}

// Two copies that hold different transactions in doubt at one row are not
// both what the primary had, though each alone could be: the anomaly is
// of the two, and says so.
func TestCheckDisagree(t *testing.T) {
	h := newHistory(4)
	h.txns[2].Outcome, h.txns[2].N = inDoubt, 0
	h.txns[3].Outcome, h.txns[3].N = inDoubt, 0
	// Restores that overlapped, so neither went back from the other.
	h.copyOf(obsRestore, 5, 1, 2, 3)
	h.copyOf(obsRestore, 5.2, 1, 2, 4)
	found, known := h.check()
	if len(found) != 1 || len(known) != 0 || found[0].Kind != "not-a-prefix" || !slices.Equal(found[0].Obs, []int{0, 1}) {
		t.Fatalf("anomalies %v, known %v; want one not-a-prefix of the two copies", found, known)
	}
	d := found[0].Detail
	if !strings.Contains(d, "holds 4 (") || !strings.Contains(d, "at row 3, where ") || !strings.Contains(d, "holds 3 (") || !strings.Contains(d, "accounts for neither") {
		t.Fatalf("detail %q does not say what each copy holds at row 3", d)
	}
}

// A copy that two epochs could each hold, as far as it goes, and that
// differs from a copy of each, is of neither: the anomaly names all three.
func TestCheckDisagreeTwoEpochs(t *testing.T) {
	h := &history{specs: map[uint64]model.Txn{}}
	add := func(uid uint64, epoch int, outcome string, n int64) {
		h.specs[uid] = model.Txn{UID: uid, Ops: []model.Op{{Kind: model.Put, Table: 0, Key: int64(uid % 3), Size: 50}}}
		h.txns = append(h.txns, &txnRec{UID: uid, Epoch: epoch, Call: at(float64(uid)), Return: at(float64(uid) + 0.1), Outcome: outcome, N: n})
	}
	// Each epoch committed 1, then a transaction in doubt, then one of
	// its own at row 3; epoch 1 began from a copy holding 1.
	add(1, 0, committed, 1)
	add(2, 0, inDoubt, 0)
	add(3, 0, committed, 3)
	add(4, 1, inDoubt, 0)
	add(5, 1, committed, 3)
	add(6, 0, inDoubt, 0)
	h.copyOf(obsRestore, 10, 1, 2, 3)   // epoch 0's
	h.copyOf(obsRestore, 10.1, 1, 4, 5) // epoch 1's
	h.copyOf(obsRestore, 10.2, 1, 6)    // either's, and neither's
	found, known := newChecker(h.txns, h.specs, h.acks, h.obs, map[int][]uint64{0: nil, 1: {1}}, 1, h.kills, h.snapshots, map[string]bool{}).check()
	if len(found) != 1 || len(known) != 0 || found[0].Kind != "not-a-prefix" || !slices.Equal(found[0].Obs, []int{1, 2, 0}) {
		t.Fatalf("anomalies %v, known %v; want one not-a-prefix of the three copies", found, known)
	}
	d := found[0].Detail
	if !strings.Contains(d, "holds 6 (") || !strings.Contains(d, "at row 2, where ") || !strings.Contains(d, "holds 4 (") || !strings.Contains(d, "nor is it a copy of an earlier epoch") {
		t.Fatalf("detail %q does not say what the copy differs from in each epoch", d)
	}
}

func TestCheckTornCommit(t *testing.T) {
	h := newHistory(4)
	// Transaction 3 was sent as the app was killed, and the app's
	// database did not hold it; the primary committed 4 at row 3.
	tx3 := h.txns[2]
	tx3.Outcome, tx3.N, tx3.Resolved = aborted, 0, true
	h.txns[3].N = 3
	h.kills = []time.Time{tx3.Call.Add(10 * time.Millisecond)}
	h.copyOf(obsRestore, 5, 1, 2, 3)
	h.copyOf(obsRestore, 6, 1, 2, 4)
	found, known := h.check()
	if len(found) == 0 {
		t.Fatal("a copy holding a torn transaction passed")
	}
	found, known = h.check("torn-commit")
	if len(found) != 0 {
		t.Fatalf("with torn-commit allowed: %v", found)
	}
	for _, k := range known {
		if !strings.HasPrefix(k.Kind, "torn-commit: ") {
			t.Fatalf("known %q", k.Kind)
		}
	}
	if len(known) == 0 {
		t.Fatal("the torn copy was not reported as known")
	}
}

// A torn transaction Litestream shipped leaves pages that corrupt copies
// that no longer hold it: those are the known issue too, until a snapshot
// after the kill, for restores of the latest state.
func TestCheckTornDamage(t *testing.T) {
	h := newHistory(5)
	tx3 := h.txns[2]
	tx3.Outcome, tx3.N, tx3.Resolved = aborted, 0, true
	h.txns[3].N, h.txns[4].N = 3, 4
	h.kills = []time.Time{tx3.Call.Add(10 * time.Millisecond)}
	h.snapshots = []time.Time{at(8)}
	h.copyOf(obsRestore, 5, 1, 2, 3) // holds the torn transaction
	broken := h.copyOf(obsRestore, 6, 1, 2, 4, 5)
	broken.Report.Integrity = []string{"*** in database main ***", "2nd reference to page 9"}
	after := h.copyOf(obsRestore, 9, 1, 2, 4, 5) // after the snapshot
	after.Report.Integrity = []string{"*** in database main ***"}
	found, known := h.check("torn-commit")
	if len(found) != 1 || found[0].Kind != "corrupt" || found[0].Obs[0] != after.ID {
		t.Fatalf("anomalies %v; want the corrupt copy after the snapshot alone (known: %v)", found, known)
	}
}

func TestCheckFinal(t *testing.T) {
	h := newHistory(3)
	h.copyOf(obsPrimary, 5, 1, 2, 3)
	h.copyOf(obsFinal, 6, 1, 2)
	found, _ := h.check()
	if !slices.Contains(kindsOf(found), "final-diverges") {
		t.Fatalf("a final restore short of the primary: %v", found)
	}
}
