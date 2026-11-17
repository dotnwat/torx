//go:build unix

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/litestream/model"
	"github.com/dotnwat/torx/examples/litestream/qa/litestream"
)

// reproJob is the base of the jobs that each reproduce one finding of
// litestream.chaos, deterministically: one primary, no nemesis, no
// follower, and Litestream's and the app's settings fixed so nothing but
// the job checkpoints the WAL.
type reproJob struct {
	chaosJob
}

func (*reproJob) Matrix() []torx.Params { return []torx.Params{{}} }

func (*reproJob) ResolveParams(p torx.Params) (torx.Params, error) {
	for k := range p {
		if k != paramTrial {
			return nil, fmt.Errorf("unknown parameter %q", k)
		}
	}
	return p, nil
}

func (r *reproJob) Declare(jc *torx.JobContext) {
	r.svc = litestream.New(serviceName, nodes)
	jc.Register(r.svc)
	r.allow = map[string]bool{}
	r.tune = func(s *settings, a *litestream.AppConfig) {
		*s = settings{
			MonitorInterval:    50 * time.Millisecond,
			SyncInterval:       100 * time.Millisecond,
			MinCheckpointPageN: 1_000_000, // Litestream never checkpoints on its own
			CheckpointInterval: time.Hour,
			FollowInterval:     time.Second,
		}
		*a = litestream.AppConfig{PageSize: 4096, AutoVacuum: "none", CachePages: 2000, Autocheckpoint: 0, Synchronous: "NORMAL"}
	}
}

// commit commits txn through the app and returns its txlog row.
func (r *reproJob) commit(ctx context.Context, txn model.Txn) (int64, error) {
	url, epoch := r.appURL()
	rec := &txnRec{UID: txn.UID, Epoch: epoch, Call: time.Now()}
	r.mu.Lock()
	r.specs[txn.UID] = txn
	r.txns = append(r.txns, rec)
	r.mu.Unlock()
	res, err := r.client.Txn(ctx, url, txn)
	r.mu.Lock()
	defer r.mu.Unlock()
	rec.Return = time.Now()
	if err == nil && res.Error != "" {
		err = errors.New(res.Error)
	}
	if err != nil {
		rec.Outcome, rec.Err = inDoubt, err.Error()
		return 0, fmt.Errorf("commit %d: %w", txn.UID, err)
	}
	rec.Outcome, rec.N = committed, res.N
	r.maxN[epoch] = max(r.maxN[epoch], res.N)
	return res.N, nil
}

// put is a transaction that writes one row.
func put(uid uint64, t int, key int64, size int) model.Txn {
	return model.Txn{UID: uid, Ops: []model.Op{{Kind: model.Put, Table: t, Key: key, Size: size}}}
}

// syncUntil runs `litestream sync -wait` until it succeeds: Litestream
// just started may not serve its socket yet.
func (r *reproJob) syncUntil(ctx context.Context) (ackRec, error) {
	deadline := time.Now().Add(time.Minute)
	for {
		err := r.syncOnce(ctx)
		r.mu.Lock()
		a := r.acks[len(r.acks)-1]
		r.mu.Unlock()
		if err == nil {
			return a, nil
		}
		if time.Now().After(deadline) {
			return a, err
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// primaryReport reads the primary's own database.
func (r *reproJob) primaryReport(ctx context.Context) (*obsRec, error) {
	n := r.svc.Nodes()[0]
	o := r.beginObs(obsPrimary, n, litestream.Target{})
	err := r.read(ctx, n, o, r.dbPath(n), true)
	r.endObs(o, err)
	return o, err
}

// restoreOn restores the replica on the standby as t says.
func (r *reproJob) restoreOn(ctx context.Context, t litestream.Target) (*obsRec, error) {
	o := r.restore(ctx, r.svc.Nodes()[1], obsRestore, t)
	if o.Err != "" {
		return o, errors.New(o.Err)
	}
	return o, nil
}

func (r *reproJob) logReport(what string, o *obsRec) {
	rep := o.Report
	integrity := "ok"
	if !rep.Intact() {
		integrity = strings.Join(first(rep.Integrity, 3), "; ")
		if len(rep.BadValues) > 0 {
			integrity += "; " + strings.Join(first(rep.BadValues, 3), "; ")
		}
	}
	last := uint64(0)
	if len(rep.TxLog) > 0 {
		last = rep.TxLog[len(rep.TxLog)-1]
	}
	r.jc.Log("info", fmt.Sprintf("%s: %d transactions, the last %d, %d rows, txid %s, integrity %s", what, len(rep.TxLog), last, rep.Digest.Rows, o.TXID, integrity))
}

// writeRecord keeps what the job did and saw, and the replica as it ends,
// whose LTX files `ltx dump` reads.
func (r *reproJob) writeRecord() {
	r.mu.Lock()
	b, _ := json.MarshalIndent(map[string]any{"acks": r.acks, "copies": r.obs, "txns": r.txns}, "", "  ")
	r.mu.Unlock()
	r.jc.WriteArtifact("record.json", b)
	var tar bytes.Buffer
	if err := r.store.WriteTar(&tar); err == nil {
		r.jc.WriteArtifact("replica.tar", tar.Bytes())
	}
}

// restartGapJob is litestream.restart-gap. Litestream is killed after it
// has synced part of the WAL; the app commits more, checkpoints the WAL
// with nothing holding it open -- as its own autocheckpoint does -- and
// writes a few frames into the WAL from its start, fewer than Litestream
// had synced. Litestream, restarted, takes the WAL's new generation to
// follow on from where it left off, and never ships what the app committed
// while it was down: the replica restores to a database that lacks those
// transactions and fails integrity_check.
type restartGapJob struct{ reproJob }

func (r *restartGapJob) Run(ctx context.Context, jc *torx.JobContext) error {
	defer r.writeRecord()
	uid := uint64(0)
	next := func() uint64 { uid++; return uid }
	// About a page each, so Litestream's position ends ~150 frames in.
	for i := range 150 {
		if _, err := r.commit(ctx, put(next(), 1, int64(i), 3000)); err != nil {
			return err
		}
	}
	a, err := r.syncUntil(ctx)
	if err != nil {
		return err
	}
	jc.Log("info", fmt.Sprintf("Litestream synced the first 150 transactions: replica txid %d", a.ReplicaTXID))
	if err := r.svc.Crash(ctx, lsName(0)); err != nil {
		return err
	}
	jc.Log("info", "killed Litestream")
	for i := range 100 {
		if _, err := r.commit(ctx, put(next(), 2, int64(i), 3000)); err != nil {
			return err
		}
	}
	res, err := r.client.Checkpoint(ctx, r.svc.AppURL(appName(0)), "PASSIVE")
	if err != nil {
		return err
	}
	jc.Log("info", "the app committed 100 more transactions and checkpointed the WAL: "+res)
	for i := range 3 {
		if _, err := r.commit(ctx, put(next(), 0, int64(i), 100)); err != nil {
			return err
		}
	}
	jc.Log("info", "the app committed 3 more, which restarted the WAL")
	if err := r.svc.Restart(ctx, lsName(0)); err != nil {
		return err
	}
	a, err = r.syncUntil(ctx)
	if err != nil {
		return err
	}
	jc.Log("info", fmt.Sprintf("restarted Litestream; sync -wait returned: replica txid %d", a.ReplicaTXID))

	p, err := r.primaryReport(ctx)
	if err != nil {
		return err
	}
	r.logReport("the primary", p)
	o, err := r.restoreOn(ctx, litestream.Target{})
	if err != nil {
		return err
	}
	r.logReport("the replica, restored", o)
	return compare(p, o)
}

// compare says how a restore differs from the primary it should equal.
func compare(primary, restored *obsRec) error {
	var errs []error
	if !restored.Report.Intact() {
		errs = append(errs, fmt.Errorf("the restore is corrupt: %s", strings.Join(append(first(restored.Report.Integrity, 5), first(restored.Report.BadValues, 5)...), "; ")))
	}
	if !slices.Equal(primary.Report.TxLog, restored.Report.TxLog) {
		errs = append(errs, fmt.Errorf("the restore's txlog holds %d transactions, the primary's %d", len(restored.Report.TxLog), len(primary.Report.TxLog)))
	}
	for name, want := range primary.Report.TableDigests {
		if got := restored.Report.TableDigests[name]; got != want {
			errs = append(errs, fmt.Errorf("the restore's table %s holds %d rows, the primary's %d", name, got.Rows, want.Rows))
		}
	}
	return errors.Join(errs...)
}

// tornCommitJob is litestream.torn-commit. The app commits a transaction
// as a writer killed mid-commit leaves it: its frames, commit frame and
// all, in the WAL, but the WAL index not updated (lsapp tear). SQLite does
// not count it committed -- no reader sees it, and the app's next write
// goes over it -- but Litestream ships it: the replica holds a transaction
// the primary never did.
//
// Litestream's own checkpoint then copies into the database only what the
// WAL index counts, and restarts the WAL, and Litestream goes on from the
// restart as if the replica and the database agreed. They do not: the
// torn transaction's pages stay in the replica under every page the
// primary writes afterwards. The torn transaction stores a value large
// enough to take overflow pages, and so does the primary's next one, in
// another table, on the same pages: the replica restores to a row whose
// value is the other table's.
type tornCommitJob struct{ reproJob }

const tornUID = 9001

// tornValue is large enough that a value of it takes overflow pages.
const tornValue = 20000

func (r *tornCommitJob) Declare(jc *torx.JobContext) {
	r.reproJob.Declare(jc)
	base := r.tune
	r.tune = func(s *settings, a *litestream.AppConfig) {
		base(s, a)
		s.CheckpointInterval = time.Second // Litestream checkpoints once the WAL has sat a second
	}
}

// walSalt reads the salt of the WAL's header, which a restart changes.
func (r *tornCommitJob) walSalt(ctx context.Context) (string, error) {
	n := r.svc.Nodes()[0]
	b, err := n.ReadFile(ctx, r.dbPath(n)+"-wal")
	if err != nil {
		return "", err
	}
	if len(b) < 24 {
		return "", nil
	}
	return fmt.Sprintf("%x", b[16:24]), nil
}

func (r *tornCommitJob) Run(ctx context.Context, jc *torx.JobContext) error {
	defer r.writeRecord()
	for i := range 20 {
		if _, err := r.commit(ctx, put(uint64(i+1), 0, int64(i), 200)); err != nil {
			return err
		}
	}
	if _, err := r.syncUntil(ctx); err != nil {
		return err
	}
	// Litestream snapshots the database soon after it starts, and then not
	// for a day: a restore after the torn transaction must come from the
	// LTX files Litestream ships, not a snapshot of the database file.
	deadline := time.Now().Add(30 * time.Second)
	for len(r.store.List(bucket, replicaPath+"/0009/")) == 0 {
		if time.Now().After(deadline) {
			return errors.New("Litestream took no snapshot within 30s")
		}
		time.Sleep(100 * time.Millisecond)
	}
	salt, err := r.walSalt(ctx)
	if err != nil {
		return err
	}
	n := r.svc.Nodes()[0]
	torn := put(tornUID, 1, 1, tornValue)
	b, _ := json.Marshal(torn)
	res, err := n.Exec(ctx, torx.Command(litestream.App, "tear", "-db", r.dbPath(n), "-txn", string(b)))
	if err != nil {
		return err
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("lsapp tear: %s", res.Stderr)
	}
	jc.Log("info", "tore a commit of transaction 9001: "+strings.TrimSpace(string(res.Stdout)))
	a1, err := r.syncUntil(ctx)
	if err != nil {
		return err
	}
	jc.Log("info", fmt.Sprintf("sync -wait returned: replica txid %d", a1.ReplicaTXID))
	url := r.svc.AppURL(appName(0))
	row, err := r.client.Lookup(ctx, url, tornUID)
	if err != nil {
		return err
	}
	jc.Log("info", fmt.Sprintf("the app's txlog holds transaction 9001 at row %d (0: not at all)", row))
	o1, err := r.restoreOn(ctx, litestream.Target{})
	if err != nil {
		return err
	}
	r.logReport("the replica, restored", o1)

	// Litestream checkpoints the WAL a second after its last change, and
	// restarts it.
	deadline = time.Now().Add(30 * time.Second)
	for {
		s, err := r.walSalt(ctx)
		if err != nil {
			return err
		}
		if s != salt {
			break
		}
		if time.Now().After(deadline) {
			return errors.New("Litestream did not checkpoint the WAL within 30s")
		}
		time.Sleep(100 * time.Millisecond)
	}
	jc.Log("info", "Litestream checkpointed the WAL and restarted it")

	n21, err := r.commit(ctx, put(9002, 2, 1, tornValue))
	if err != nil {
		return err
	}
	jc.Log("info", fmt.Sprintf("the app committed transaction 9002 at row %d", n21))
	if _, err := r.syncUntil(ctx); err != nil {
		return err
	}
	// The restore may be too broken for SQLite to read: that is a finding,
	// not a failure of the job.
	o2, err := r.restoreOn(ctx, litestream.Target{})
	if err != nil && o2.TXID == "" {
		return err
	}
	if o2.Report != nil {
		r.logReport("the replica, restored again", o2)
	} else {
		jc.Log("info", fmt.Sprintf("the replica, restored again to txid %s, cannot be read: %s", o2.TXID, o2.Err))
	}
	o3, err := r.restoreOn(ctx, litestream.Target{TXID: a1.ReplicaTXID})
	if err != nil {
		return err
	}
	r.logReport(fmt.Sprintf("the replica, restored to txid %d", a1.ReplicaTXID), o3)
	p, err := r.primaryReport(ctx)
	if err != nil {
		return err
	}
	r.logReport("the primary", p)

	var errs []error
	if row == 0 && slices.Contains(o1.Report.TxLog, tornUID) {
		errs = append(errs, fmt.Errorf("the replica restored to transaction 9001, which the primary's database does not hold"))
	}
	if slices.Contains(o3.Report.TxLog, tornUID) && !slices.Contains(p.Report.TxLog, tornUID) {
		errs = append(errs, fmt.Errorf("txid %d restores to a state the primary never had: transaction 9001 at row %d, where the primary has 9002",
			a1.ReplicaTXID, slices.Index(o3.Report.TxLog, tornUID)+1))
	}
	if o2.Report == nil {
		errs = append(errs, fmt.Errorf("after Litestream's checkpoint and transaction 9002, the replica restores to a database SQLite cannot read: %s", o2.Err))
	} else if err := compare(p, o2); err != nil {
		errs = append(errs, fmt.Errorf("after Litestream's checkpoint and transaction 9002, the replica restores to a database that is not the primary's: %w", err))
	}
	return errors.Join(errs...)
}
