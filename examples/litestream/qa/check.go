//go:build unix

package main

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/dotnwat/torx/examples/litestream/model"
	"github.com/dotnwat/torx/prefixcheck"
)

// slop is how far apart two clocks of one host may read the same instant:
// the job's, which times the clients, and Litestream's, which stamps the
// LTX files a restore to a time picks by.
const slop = 50 * time.Millisecond

// checker holds the copies of the database the job read to what the
// clients committed and what Litestream acknowledged.
//
// What any asynchronous replica owes its primary -- each copy a prefix of
// what a primary committed, holding every transaction a sync acknowledged
// before it, and never going back -- is prefixcheck's to check, over the
// txlogs. The rest is Litestream's: a copy holds exactly what its
// transactions wrote, a TXID restores the same way every time, a time
// restores to nothing sent after it, and once every fault is healed the
// replica restores to the primary's database.
type checker struct {
	specs  map[uint64]model.Txn
	byUID  map[uint64]*txnRec
	acks   []ackRec
	obs    []*obsRec
	epochs int
	// orders are each epoch's txlog, as the job knows it: row -> uid. An
	// epoch begins with the txlog of the restore it started from.
	orders []map[int]uint64
	hist   prefixcheck.History[uint64]
	// allow are the known issues reported as warnings.
	allow map[string]bool
	// torn are the transactions an app was killed committing, which its
	// database then did not hold, by when; tainted, the copies that hold
	// one, or that Litestream may have built from pages one left.
	torn    map[uint64]time.Time
	tainted map[int]bool

	anomalies []anomaly
	known     []anomaly
}

// newChecker builds a checker of a record. appKills are when the job
// killed an app; snapshots, when Litestream wrote a snapshot to the
// replica.
func newChecker(txns []*txnRec, specs map[uint64]model.Txn, acks []ackRec, obs []*obsRec, bases map[int][]uint64, epochs int, appKills, snapshots []time.Time, allow map[string]bool) *checker {
	c := &checker{specs: specs, byUID: map[uint64]*txnRec{}, acks: acks, obs: obs, epochs: epochs,
		allow: allow, torn: map[uint64]time.Time{}, tainted: map[int]bool{}}
	for _, t := range txns {
		c.byUID[t.UID] = t
		if t.Outcome != aborted || !t.Resolved {
			continue
		}
		for _, k := range appKills {
			if !k.Before(t.Call) && !k.After(t.Return.Add(100*time.Millisecond)) {
				c.torn[t.UID] = k
			}
		}
	}
	// A torn transaction Litestream shipped -- one a copy holds -- leaves
	// its pages in the replica under what the primary writes after it, so
	// a copy that does not hold it may still be built from them: a restore
	// of the latest state until the next snapshot, one to a TXID or a time
	// at any time after, a follower's for the rest of its life.
	var shipped []time.Time
	for _, o := range obs {
		if o.Report == nil {
			continue
		}
		for _, uid := range o.Report.TxLog {
			if k, ok := c.torn[uid]; ok {
				c.tainted[o.ID] = true
				shipped = append(shipped, k)
				break
			}
		}
	}
	for _, k := range shipped {
		healed := time.Time{} // the first snapshot after the kill
		for _, s := range snapshots {
			if s.After(k) && (healed.IsZero() || s.Before(healed)) {
				healed = s
			}
		}
		for _, o := range obs {
			if o.Call.Before(k) || o.Kind == obsPrimary {
				continue
			}
			if !latest(o) || o.Kind == obsFollow || healed.IsZero() || o.Call.Before(healed) {
				c.tainted[o.ID] = true
			}
		}
	}
	c.orders = make([]map[int]uint64, epochs+1)
	for e := range c.orders {
		c.orders[e] = map[int]uint64{}
		for i, uid := range bases[e] {
			c.orders[e][i+1] = uid
		}
	}
	for _, t := range txns {
		if t.Outcome != committed || t.N == 0 {
			continue
		}
		if prev, ok := c.orders[t.Epoch][int(t.N)]; ok && prev != t.UID {
			c.add(anomaly{Kind: "txlog-conflict", Txns: []uint64{prev, t.UID},
				Detail: fmt.Sprintf("epoch %d's app committed both %d and %d at txlog row %d", t.Epoch, prev, t.UID, t.N)})
			continue
		}
		c.orders[t.Epoch][int(t.N)] = t.UID
	}
	c.hist = prefixcheck.History[uint64]{Branches: c.orders, Doubt: map[uint64]int{}}
	for _, t := range txns {
		if t.Outcome == inDoubt {
			c.hist.Doubt[t.UID] = t.Epoch
		}
	}
	for _, a := range acks {
		if a.Err == "" {
			c.hist.Acks = append(c.hist.Acks, prefixcheck.Ack{Branch: a.Epoch, Prefix: int(a.Prefix), Return: a.Return})
		}
	}
	for _, o := range obs {
		if o.Report == nil {
			continue
		}
		cp := prefixcheck.Copy[uint64]{ID: o.ID, Seq: o.Report.TxLog, Call: o.Call, Return: o.Return, Latest: latest(o), Known: c.knownFor(o.ID)}
		if o.Kind == obsFollow {
			cp.Group = fmt.Sprintf("follower life %d", o.Life)
		}
		c.hist.Copies = append(c.hist.Copies, cp)
	}
	return c
}

// knownFor is the known issue, allowed, that explains what is wrong with
// copy o, if one does: it holds a torn transaction, or it is a follower's,
// read after one of the follower's polls failed.
func (c *checker) knownFor(id int) string {
	if id < 0 || id >= len(c.obs) {
		return ""
	}
	o := c.obs[id]
	switch {
	case c.allow["torn-commit"] && c.tainted[o.ID]:
		return "torn-commit"
	case c.allow["follow-regress"] && o.Kind == obsFollow && o.FollowErrors > 0:
		return "follow-regress"
	}
	return ""
}

// skip reports whether to leave copy o out of the checks that compare
// copies: a known issue explains it, so comparing it would report the
// same issue again and hide the others.
func (c *checker) skip(o *obsRec) bool { return c.knownFor(o.ID) != "" }

func (c *checker) add(a anomaly) {
	for _, id := range a.Obs {
		if k := c.knownFor(id); k != "" {
			a.Kind = k + ": " + a.Kind
			c.known = append(c.known, a)
			return
		}
	}
	c.anomalies = append(c.anomalies, a)
}

func (c *checker) describe(uid uint64) string {
	t := c.byUID[uid]
	if t == nil {
		return fmt.Sprintf("%d (a transaction no client sent)", uid)
	}
	s := fmt.Sprintf("%d (%s", uid, t.Outcome)
	if t.N > 0 {
		s += fmt.Sprintf(" at row %d", t.N)
	}
	return s + fmt.Sprintf(" in epoch %d, sent %s)", t.Epoch, t.Call.Format("15:04:05.000"))
}

// check returns the anomalies, and the known issues it reports as
// warnings.
func (c *checker) check() (anomalies, known []anomaly) {
	for _, a := range prefixcheck.Check(c.hist) {
		c.addPrefix(a)
	}
	c.checkCopies()
	c.checkTXIDs()
	c.checkTimes()
	c.checkFinal()
	return c.anomalies, c.known
}

// addPrefix reports what prefixcheck found, in the job's terms.
func (c *checker) addPrefix(a prefixcheck.Anomaly[uint64]) {
	o := c.obs[a.Copies[0]]
	L := o.Report.TxLog
	switch a.Kind {
	case prefixcheck.NotAPrefix:
		uid := L[a.At-1]
		if len(a.Copies) > 1 {
			// Two copies that differ at a row the epoch's log does not
			// account for: one of them is not what the primary had, and
			// nothing says which.
			other := c.obs[a.Copies[1]]
			theirs := other.Report.TxLog[a.At-1]
			obs := []int{other.ID, o.ID}
			detail := fmt.Sprintf("%s holds %s at row %d, where %s holds %s: epoch %d committed one transaction there at most, and its log accounts for neither",
				c.what(o), c.describe(uid), a.At, c.what(other), c.describe(theirs), a.Branch)
			// A copy an earlier epoch could hold as well is of none of
			// them only if it differs from a copy of each: the rest.
			for _, id := range a.Copies[2:] {
				detail += fmt.Sprintf("; nor is it a copy of an earlier epoch, differing as it does from %s", c.what(c.obs[id]))
				obs = append(obs, id)
			}
			c.add(anomaly{Kind: "not-a-prefix", Obs: obs, Txns: []uint64{theirs, uid}, Detail: detail})
			return
		}
		detail := fmt.Sprintf("%s holds %d transactions, which no primary committed in that order: at row %d it holds %s, ",
			c.what(o), len(L), a.At, c.describe(uid))
		want, known := c.orders[a.Branch][a.At]
		switch first := slices.Index(L, uid) + 1; {
		case known && want != uid:
			detail += fmt.Sprintf("where epoch %d committed %s", a.Branch, c.describe(want))
		case first < a.At:
			// A transaction in doubt, held twice: the row is one it could
			// stand at, had it not stood at another already.
			detail += fmt.Sprintf("which it holds at row %d too", first)
		default:
			detail += fmt.Sprintf("where epoch %d committed nothing", a.Branch)
		}
		c.add(anomaly{Kind: "not-a-prefix", Obs: []int{o.ID}, Detail: detail})
	case prefixcheck.LostAcked:
		detail := fmt.Sprintf("%s holds %d transactions, but a sync -wait that returned at %s acknowledged the %d epoch %d had committed when it was called",
			c.what(o), len(L), a.Ack.Return.Format("15:04:05.000"), a.Ack.Prefix, a.Ack.Branch)
		if len(a.Missing) > 0 {
			detail += fmt.Sprintf("; it lacks %d of them, first %s", len(a.Missing), c.describe(a.Missing[0]))
		}
		c.add(anomaly{Kind: "lost-acked", Obs: []int{o.ID}, Txns: first(a.Missing, 10), Detail: detail})
	case prefixcheck.WentBack:
		before := c.obs[a.Copies[1]]
		kind, what := "restore-went-back", "a restore of the latest state"
		if o.Kind == obsFollow {
			kind, what = "follower-went-back", "the follower"
		}
		c.add(anomaly{Kind: kind, Obs: []int{before.ID, o.ID}, Txns: first(a.Missing, 10),
			Detail: fmt.Sprintf("%s, %s, holds %d transactions, though %s, before it, held %d", what, c.what(o), len(L), c.what(before), len(before.Report.TxLog))})
	default:
		c.add(anomaly{Kind: a.Kind, Obs: a.Copies, Detail: a.Text})
	}
}

// checkCopies checks each copy on its own: readable, intact, holding no
// transaction that did not commit, and holding exactly what its
// transactions wrote.
func (c *checker) checkCopies() {
	type todo struct {
		o *obsRec
		e int
	}
	var replay []todo
	for _, o := range c.obs {
		r := o.Report
		if r == nil && o.TXID != "" && o.Kind != obsFollow {
			// Litestream restored it, and SQLite cannot read it.
			c.add(anomaly{Kind: "unreadable", Obs: []int{o.ID}, Detail: fmt.Sprintf("%s: %s", c.what(o), o.Err)})
		}
		if r == nil {
			continue
		}
		if !r.Intact() {
			var d []string
			if len(r.Integrity) > 0 && r.Integrity[0] != "ok" {
				d = append(d, "integrity_check: "+strings.Join(first(r.Integrity, 5), "; "))
			}
			if len(r.BadValues) > 0 {
				d = append(d, "values: "+strings.Join(first(r.BadValues, 5), "; "))
			}
			if len(r.TxLogGaps) > 0 {
				d = append(d, fmt.Sprintf("txlog skips rows %v", first(r.TxLogGaps, 5)))
			}
			c.add(anomaly{Kind: "corrupt", Obs: []int{o.ID}, Detail: fmt.Sprintf("%s: %s", c.what(o), strings.Join(d, "\n"))})
		}
		var bad []uint64
		for _, uid := range r.TxLog {
			if t := c.byUID[uid]; t != nil && (t.Outcome == aborted || t.Outcome == rolledBack) {
				bad = append(bad, uid)
			}
		}
		if len(bad) > 0 {
			c.add(anomaly{Kind: "holds-aborted", Obs: []int{o.ID}, Txns: first(bad, 10),
				Detail: fmt.Sprintf("%s holds %d transactions that did not commit, first %s", c.what(o), len(bad), c.describe(bad[0]))})
		}
		replay = append(replay, todo{o, c.hist.BranchOf(r.TxLog)})
	}
	// Replay each epoch's copies shortest first, so one replay serves all.
	sort.SliceStable(replay, func(a, b int) bool {
		if replay[a].e != replay[b].e {
			return replay[a].e < replay[b].e
		}
		return len(replay[a].o.Report.TxLog) < len(replay[b].o.Report.TxLog)
	})
	var st *model.State
	var seq []uint64
	for _, x := range replay {
		L := x.o.Report.TxLog
		if st == nil || !isPrefix(seq, L) {
			st, seq = model.NewState(), nil
		}
		for _, uid := range L[len(seq):] {
			st.Apply(c.specs[uid])
		}
		seq = L
		if want, got := st.Digest(), x.o.Report.Digest; want != got {
			var diff []string
			wantT := st.TableDigests()
			for _, name := range tableNames() {
				w, g := wantT[name], x.o.Report.TableDigests[name]
				if w != g {
					diff = append(diff, fmt.Sprintf("%s holds %d rows, should hold %d", name, g.Rows, w.Rows))
				}
			}
			c.add(anomaly{Kind: "wrong-contents", Obs: []int{x.o.ID},
				Detail: fmt.Sprintf("%s holds %d transactions but not what they wrote: %s", c.what(x.o), len(L), strings.Join(diff, "; "))})
		}
	}
}

func tableNames() []string {
	var out []string
	for t := range model.Tables {
		out = append(out, model.TableName(t))
	}
	return out
}

func first[T any](xs []T, n int) []T {
	if len(xs) > n {
		return xs[:n]
	}
	return xs
}

func (c *checker) what(o *obsRec) string {
	s := fmt.Sprintf("%s #%d", o.Kind, o.ID)
	if o.Kind == obsRestore || o.Kind == obsFinal || o.Kind == obsFailover {
		s += " (" + o.Target.String()
		if o.TXID != "" {
			s += ", restored txid " + o.TXID
		}
		s += ")"
	}
	return s + fmt.Sprintf(" on %s at %s", o.Node, o.Call.Format("15:04:05.000"))
}

// latest reports whether o restores the replica's latest state: what an
// ack before it binds.
//
// A restore to a time is not held to the acks before the time: it can
// restore only to the end of a file stamped before it, and once
// compaction merges the files a time falls among and retention deletes
// them, the latest it can reach may be well before the time.
func latest(o *obsRec) bool {
	return (o.Kind == obsRestore || o.Kind == obsFinal || o.Kind == obsFailover) && o.Target.TXID == 0 && o.Target.Timestamp.IsZero()
}

// isPrefix reports whether a is a prefix of b.
func isPrefix(a, b []uint64) bool {
	return len(a) <= len(b) && slices.Equal(a, b[:len(a)])
}

// checkTXIDs checks that a TXID restores the same way every time, and a
// later TXID to a later state.
func (c *checker) checkTXIDs() {
	byTXID := map[uint64][]*obsRec{}
	var ids []uint64
	for _, o := range c.obs {
		if o.Report == nil || o.Target.TXID == 0 || c.skip(o) {
			continue
		}
		if byTXID[o.Target.TXID] == nil {
			ids = append(ids, o.Target.TXID)
		}
		byTXID[o.Target.TXID] = append(byTXID[o.Target.TXID], o)
	}
	slices.Sort(ids)
	var prev *obsRec
	for _, id := range ids {
		os := byTXID[id]
		for _, o := range os[1:] {
			if !slices.Equal(o.Report.TxLog, os[0].Report.TxLog) || o.Report.Digest != os[0].Report.Digest {
				c.add(anomaly{Kind: "txid-changed", Obs: []int{os[0].ID, o.ID},
					Detail: fmt.Sprintf("txid %d restored to %d transactions (digest %x) at %s, and to %d (digest %x) at %s",
						id, len(os[0].Report.TxLog), os[0].Report.Digest.Sum, os[0].Call.Format("15:04:05.000"),
						len(o.Report.TxLog), o.Report.Digest.Sum, o.Call.Format("15:04:05.000"))})
			}
		}
		if prev != nil && !isPrefix(prev.Report.TxLog, os[0].Report.TxLog) {
			c.add(anomaly{Kind: "txid-went-back", Obs: []int{prev.ID, os[0].ID},
				Detail: fmt.Sprintf("txid %d restored to %d transactions, but txid %d, before it, to %d", id, len(os[0].Report.TxLog), prev.Target.TXID, len(prev.Report.TxLog))})
		}
		prev = os[0]
	}
}

// checkTimes checks that a restore to a time holds no transaction sent
// after it.
//
// Restores to times are not held to each other: one can restore only to
// the end of a file stamped before its time, and compaction and retention
// replace fine files with coarse ones as time goes on, so a restore to a
// time may reach less than an earlier restore to an earlier time did.
func (c *checker) checkTimes() {
	for _, o := range c.obs {
		if o.Report == nil || o.Target.Timestamp.IsZero() || c.skip(o) {
			continue
		}
		T := o.Target.Timestamp
		for _, uid := range o.Report.TxLog {
			if t := c.byUID[uid]; t != nil && t.Call.After(T.Add(slop)) {
				c.add(anomaly{Kind: "time-holds-later", Obs: []int{o.ID}, Txns: []uint64{uid},
					Detail: fmt.Sprintf("%s holds %s, which was sent %s after that time", c.what(o), c.describe(uid), t.Call.Sub(T))})
				break
			}
		}
	}
}

// checkFinal checks the end: the primary holds every transaction its epoch
// committed, and the replica, restored, and the follower hold the
// primary's database exactly.
func (c *checker) checkFinal() {
	var primary, final, follow *obsRec
	for _, o := range c.obs {
		switch o.Kind {
		case obsPrimary:
			primary = o
		case obsFinal:
			final = o
		case obsFollow:
			follow = o
		}
	}
	if primary == nil || primary.Report == nil {
		return
	}
	P := primary.Report.TxLog
	e := c.epochs
	have := map[uint64]bool{}
	for _, uid := range P {
		have[uid] = true
	}
	var lost []uint64
	for _, n := range slices.Sorted(maps.Keys(c.orders[e])) {
		if uid := c.orders[e][n]; !have[uid] {
			lost = append(lost, uid)
		}
	}
	if len(lost) > 0 {
		c.add(anomaly{Kind: "primary-lost", Obs: []int{primary.ID}, Txns: first(lost, 10),
			Detail: fmt.Sprintf("the primary's database lacks %d transactions its app committed, first %s", len(lost), c.describe(lost[0]))})
	}
	if final == nil || final.Report == nil {
		reason := "it was not taken"
		if final != nil {
			reason = final.Err
		}
		c.add(anomaly{Kind: "unrestorable", Detail: "the final restore failed: " + reason})
	} else if !slices.Equal(final.Report.TxLog, P) || final.Report.Digest != primary.Report.Digest {
		c.add(anomaly{Kind: "final-diverges", Obs: []int{primary.ID, final.ID},
			Detail: fmt.Sprintf("once the replica caught up, it restored to %d transactions (digest %x); the primary holds %d (digest %x)",
				len(final.Report.TxLog), final.Report.Digest.Sum, len(P), primary.Report.Digest.Sum)})
	}
	if follow != nil && follow.Report != nil && follow.Call.After(primary.Call) && len(follow.Report.TxLog) >= len(P) &&
		(!slices.Equal(follow.Report.TxLog, P) || follow.Report.Digest != primary.Report.Digest) {
		c.add(anomaly{Kind: "follower-diverges", Obs: []int{primary.ID, follow.ID},
			Detail: fmt.Sprintf("the follower caught up to %d transactions (digest %x); the primary holds %d (digest %x)",
				len(follow.Report.TxLog), follow.Report.Digest.Sum, len(P), primary.Report.Digest.Sum)})
	}
}
