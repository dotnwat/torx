//go:build unix

package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path"
	"strings"
	"time"

	"github.com/dotnwat/torx/diskfault"
	"github.com/dotnwat/torx/examples/litestream/qa/litestream"
	"github.com/dotnwat/torx/nemesis"
	"github.com/dotnwat/torx/objstore"
)

var checkpointModes = []string{"PASSIVE", "FULL", "RESTART", "TRUNCATE"}

func (j *chaosJob) faultList() []nemesis.Fault {
	f := func(name string, inject func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error)) nemesis.Fault {
		return nemesis.Fault{Name: name, Weight: faultWeights[name], Inject: inject}
	}
	return []nemesis.Fault{
		f("crash-litestream", j.crashLitestream),
		f("stop-litestream", j.stopLitestream),
		f("pause-litestream", j.pauseLitestream),
		f("outage", j.outage),
		f("crash-app", j.crashApp),
		f("restart-app", j.restartApp),
		f("app-checkpoint", j.appCheckpoint),
		f("vacuum", j.vacuum),
		f("store-errors", j.storeErrors),
		f("store-slow", j.storeSlow),
		f("store-down", j.storeDown),
		f("store-partition", j.storePartition),
		f("disk-full", j.diskFull),
		f("restore-errors", j.restoreErrors),
		f("failover", j.failover),
	}
}

// restartHeal restarts process name.
func (j *chaosJob) restartHeal(name string) nemesis.Heal {
	return func(ctx context.Context) error { return j.svc.Restart(ctx, name) }
}

func (j *chaosJob) crashLitestream(ctx context.Context, _ *rand.Rand) (nemesis.Heal, string, error) {
	_, p := j.current()
	name := lsName(p)
	if err := j.svc.Crash(ctx, name); err != nil {
		return nil, name, err
	}
	return j.restartHeal(name), name, nil
}

func (j *chaosJob) stopLitestream(ctx context.Context, _ *rand.Rand) (nemesis.Heal, string, error) {
	_, p := j.current()
	name := lsName(p)
	clean, err := j.svc.Terminate(ctx, name)
	target := name
	if err == nil && !clean {
		target += " (killed after its grace)"
	}
	return j.restartHeal(name), target, err
}

func (j *chaosJob) pauseLitestream(ctx context.Context, _ *rand.Rand) (nemesis.Heal, string, error) {
	_, p := j.current()
	name := lsName(p)
	if err := j.svc.Pause(ctx, name); err != nil {
		return nil, name, err
	}
	return func(ctx context.Context) error { return j.svc.Resume(ctx, name) }, name, nil
}

// outage takes Litestream down and, while it is, has the app do what an
// app does with nobody holding its WAL open: checkpoint it, close its last
// connection (which checkpoints and removes the WAL), crash, or vacuum.
func (j *chaosJob) outage(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	e, p := j.current()
	name := lsName(p)
	if err := j.svc.Crash(ctx, name); err != nil {
		return nil, name, err
	}
	heal := j.restartHeal(name)
	acts := []string{name + " down"}
	for range 1 + rng.IntN(3) {
		if !nemesis.Sleep(ctx, time.Duration(100+rng.IntN(900))*time.Millisecond) {
			break
		}
		act, err := j.appAction(ctx, rng, e, p)
		acts = append(acts, act)
		if err != nil {
			return heal, strings.Join(acts, ", "), err
		}
	}
	return heal, strings.Join(acts, ", "), nil
}

// appAction has the app of node p do one of the things that change its
// WAL behind Litestream's back.
func (j *chaosJob) appAction(ctx context.Context, rng *rand.Rand, epoch, p int) (string, error) {
	url := j.svc.AppURL(appName(p))
	switch rng.IntN(5) {
	case 0, 1:
		mode := pick(rng, checkpointModes...)
		if j.allow["restart-gap"] {
			// Only a checkpoint that truncates the WAL leaves Litestream
			// nothing to mistake for where it left off.
			mode = "TRUNCATE"
		}
		res, err := j.client.Checkpoint(ctx, url, mode)
		return "checkpoint " + mode + " " + res, err
	case 2:
		return "reopen", j.client.Reopen(ctx, url)
	case 3:
		return "crash app", j.bounceApp(ctx, epoch, p, false)
	default:
		full := rng.IntN(2) == 0
		if full {
			return "vacuum", j.client.Vacuum(ctx, url, true, 0)
		}
		return "incremental vacuum", j.client.Vacuum(ctx, url, false, 1+rng.IntN(200))
	}
}

// bounceApp crashes, or stops, the app of node p and starts it again.
func (j *chaosJob) bounceApp(ctx context.Context, epoch, p int, graceful bool) error {
	name := appName(p)
	var err error
	if graceful {
		_, err = j.svc.Terminate(ctx, name)
	} else {
		err = j.killApp(ctx, name)
	}
	if err != nil {
		return err
	}
	return j.restartApp0(ctx, epoch, p)
}

func (j *chaosJob) restartApp0(ctx context.Context, epoch, p int) error {
	name := appName(p)
	if err := j.svc.Restart(ctx, name); err != nil {
		return err
	}
	if err := j.svc.WaitApp(ctx, name, time.Minute); err != nil {
		return err
	}
	return j.resolve(ctx, epoch, j.svc.AppURL(name))
}

// killApp kills app process name, and notes when: a transaction it was
// committing then may be torn.
func (j *chaosJob) killApp(ctx context.Context, name string) error {
	j.mu.Lock()
	j.appKills = append(j.appKills, time.Now())
	j.mu.Unlock()
	return j.svc.Crash(ctx, name)
}

func (j *chaosJob) crashApp(ctx context.Context, _ *rand.Rand) (nemesis.Heal, string, error) {
	e, p := j.current()
	name := appName(p)
	if err := j.killApp(ctx, name); err != nil {
		return nil, name, err
	}
	return func(ctx context.Context) error { return j.restartApp0(ctx, e, p) }, name, nil
}

func (j *chaosJob) restartApp(ctx context.Context, _ *rand.Rand) (nemesis.Heal, string, error) {
	e, p := j.current()
	name := appName(p)
	if _, err := j.svc.Terminate(ctx, name); err != nil {
		return nil, name, err
	}
	return func(ctx context.Context) error { return j.restartApp0(ctx, e, p) }, name, nil
}

func (j *chaosJob) appCheckpoint(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	_, p := j.current()
	mode := pick(rng, checkpointModes...)
	res, err := j.client.Checkpoint(ctx, j.svc.AppURL(appName(p)), mode)
	return nil, mode + " " + res, err
}

func (j *chaosJob) vacuum(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	_, p := j.current()
	if rng.IntN(2) == 0 {
		return nil, "full", j.client.Vacuum(ctx, j.svc.AppURL(appName(p)), true, 0)
	}
	n := 1 + rng.IntN(200)
	return nil, fmt.Sprintf("incremental %d", n), j.client.Vacuum(ctx, j.svc.AppURL(appName(p)), false, n)
}

// The kinds of store request a rule picks among.
var opSets = [][]objstore.Op{
	nil, // all
	{objstore.OpPut, objstore.OpPart, objstore.OpComplete},
	{objstore.OpDelete, objstore.OpDeletes},
	{objstore.OpList},
	{objstore.OpGet, objstore.OpHead},
}

func opNames(ops []objstore.Op) string {
	if ops == nil {
		return "all"
	}
	var s []string
	for _, o := range ops {
		s = append(s, string(o))
	}
	return strings.Join(s, "+")
}

// rule adds r to the store and returns how to remove it.
func (j *chaosJob) rule(r objstore.Rule) (nemesis.Heal, string) {
	id := j.store.AddRule(r)
	target := fmt.Sprintf("%s %s of %s p=%.1f", r.Action, opNames(r.Ops), strings.Join(r.Clients, ","), r.Prob)
	if r.Delay > 0 {
		target += " " + r.Delay.String()
	}
	return func(context.Context) error { j.store.RemoveRule(id); return nil }, target
}

func (j *chaosJob) storeErrors(_ context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	_, p := j.current()
	heal, target := j.rule(objstore.Rule{
		Name:    "store-errors",
		Clients: []string{lsName(p)},
		Ops:     pick(rng, opSets...),
		Prob:    pick(rng, 0.2, 0.5, 1.0),
		Action:  pick(rng, objstore.Fail, objstore.FailAfter, objstore.Drop, objstore.DropAfter),
		Status:  pick(rng, 500, 503),
	})
	return heal, target, nil
}

func (j *chaosJob) storeSlow(_ context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	_, p := j.current()
	heal, target := j.rule(objstore.Rule{
		Name:    "store-slow",
		Clients: []string{lsName(p)},
		Ops:     pick(rng, opSets...),
		Prob:    pick(rng, 0.3, 1.0),
		Action:  pick(rng, objstore.Delay, objstore.Stall, objstore.DelayAfter),
		Delay:   pick(rng, 200*time.Millisecond, time.Second, stallMax),
	})
	return heal, target, nil
}

func (j *chaosJob) storeDown(_ context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	_, p := j.current()
	heal, target := j.rule(objstore.Rule{
		Name:    "store-down",
		Clients: []string{lsName(p)},
		Prob:    1,
		Action:  objstore.Delay,
		Delay:   pick(rng, 5*time.Second, 15*time.Second),
	})
	return heal, target, nil
}

// storePartition cuts Litestream off from the store, every request or
// some kinds: they wait, and go through once the partition heals, as a
// network's retransmissions would deliver them.
func (j *chaosJob) storePartition(_ context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	_, p := j.current()
	heal, target := j.rule(objstore.Rule{
		Name:    "store-partition",
		Clients: []string{lsName(p)},
		Ops:     pick(rng, opSets[:2]...),
		Prob:    1,
		Action:  objstore.Block,
	})
	return heal, target, nil
}

// diskFull fills the primary's disk -- the database's, its WAL's, and
// Litestream's LTX files' -- until the heal frees it.
func (j *chaosJob) diskFull(ctx context.Context, _ *rand.Rand) (nemesis.Heal, string, error) {
	if !j.disk {
		return nil, "", nemesis.ErrNoTarget
	}
	_, p := j.current()
	n := j.svc.Nodes()[p]
	dir := path.Dir(j.dbPath(n))
	if err := diskfault.Fill(ctx, n, dir); err != nil {
		return nil, n.Name(), err
	}
	return func(ctx context.Context) error { return diskfault.Free(ctx, n, dir) }, n.Name(), nil
}

func (j *chaosJob) restoreErrors(_ context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	var clients []string
	for i := range nodes {
		clients = append(clients, fmt.Sprintf("restore%d", i), fmt.Sprintf("follow%d", i))
	}
	heal, target := j.rule(objstore.Rule{
		Name:    "restore-errors",
		Clients: clients,
		Ops:     pick(rng, opSets...),
		Prob:    pick(rng, 0.2, 0.5),
		Action:  pick(rng, objstore.Fail, objstore.Drop, objstore.Delay),
		Delay:   time.Second,
	})
	return heal, target, nil
}

// failover loses the primary -- its app and Litestream die, and its disk
// with them -- restores the replica on the standby, and makes the standby
// the primary: the app starts there from the restore, and Litestream
// replicates it to the same replica. The lost node becomes the standby.
func (j *chaosJob) failover(ctx context.Context, _ *rand.Rand) (nemesis.Heal, string, error) {
	e, p := j.current()
	s := 1 - p
	ns := j.svc.Nodes()
	old, nw := ns[p], ns[s]
	target := fmt.Sprintf("%s -> %s", old.Name(), nw.Name())
	for _, name := range []string{appName(p), lsName(p), followName(s)} {
		if !j.svc.Running(name) {
			continue
		}
		crash := j.svc.Crash
		if name == appName(p) {
			crash = j.killApp
		}
		if err := crash(ctx, name); err != nil {
			return nil, target, err
		}
	}
	// A dead primary's store requests held by a fault land before the
	// new one starts: a request in flight forever is another test.
	nemesis.Sleep(ctx, stallMax+time.Second)

	// The standby starts from nothing but the replica.
	dbDir := path.Dir(j.dbPath(nw))
	if err := j.svc.Wipe(ctx, nw, dbDir); err != nil {
		return nil, target, err
	}
	var o *obsRec
	for attempt := 0; ; attempt++ {
		o = j.beginObs(obsFailover, nw, litestream.Target{})
		rctx, cancel := context.WithTimeout(ctx, restoreTimeout)
		res, err := j.svc.Restore(rctx, nw, j.lsConfig(nw), j.dbPath(nw), j.dbPath(nw), litestream.Target{})
		if err == nil {
			o.TXID = res.TXID
			err = j.read(rctx, nw, o, j.dbPath(nw), false)
		}
		cancel()
		j.endObs(o, err)
		if err == nil {
			break
		}
		_ = j.svc.Wipe(ctx, nw, dbDir)
		if attempt == 4 || ctx.Err() != nil {
			// Bring the old primary back, as if it had not been lost.
			restart := func(ctx context.Context) error { return j.startOld(ctx, e, p) }
			return restart, target, fmt.Errorf("restore on %s: %w", nw.Name(), err)
		}
		nemesis.Sleep(ctx, time.Second)
	}
	j.keepIfBroken(ctx, nw, o, j.dbPath(nw))

	j.mu.Lock()
	j.epoch++
	j.primary = s
	j.bases[j.epoch] = o.Report.TxLog
	j.maxN[j.epoch] = int64(len(o.Report.TxLog))
	j.mu.Unlock()
	if err := j.startPrimary(ctx, s); err != nil {
		return nil, target, err
	}
	// The lost node comes back empty, as the standby.
	if err := j.svc.Wipe(ctx, old, path.Dir(j.dbPath(old))); err != nil {
		return nil, target, err
	}
	if err := j.startFollower(ctx, p); err != nil {
		return nil, target, err
	}
	return nil, fmt.Sprintf("%s at %d transactions (txid %s)", target, len(o.Report.TxLog), o.TXID), nil
}

// startOld starts epoch e's primary on node p again.
func (j *chaosJob) startOld(ctx context.Context, e, p int) error {
	for _, name := range []string{appName(p), lsName(p)} {
		if !j.svc.Running(name) {
			if err := j.svc.Restart(ctx, name); err != nil {
				return err
			}
		}
	}
	if err := j.svc.WaitApp(ctx, appName(p), time.Minute); err != nil {
		return err
	}
	return j.resolve(ctx, e, j.svc.AppURL(appName(p)))
}
