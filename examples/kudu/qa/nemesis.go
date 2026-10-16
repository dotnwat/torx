//go:build unix

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/diskfault"
	"github.com/dotnwat/torx/examples/kudu/qa/kudu"
	"github.com/dotnwat/torx/nemesis"
	"github.com/dotnwat/torx/netfault"
	"github.com/dotnwat/torx/resfault"
)

// nemesis builds the job's nemesis: its faults, drawn from the run's, a
// supervisor that restarts what exited on its own before each, and a
// record of every fault and heal.
func (j *chaosJob) nemesis(rng *rand.Rand, admin *kudu.Client) *nemesis.Nemesis {
	all := map[string]func(context.Context, *rand.Rand) (nemesis.Heal, string, error){
		"crash-tserver": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.crash(ctx, rng, kudu.TServer, 1)
		},
		"crash-tservers": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.crash(ctx, rng, kudu.TServer, 2)
		},
		"crash-master": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.crash(ctx, rng, kudu.Master, 1)
		},
		"restart-tserver": j.restartTServer,
		"pause-tserver": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.pause(ctx, rng, kudu.TServer)
		},
		"pause-master": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.pause(ctx, rng, kudu.Master)
		},
		"partition-one":    j.partitionOne,
		"partition-halves": j.partitionHalves,
		"partition-client": j.partitionClient,
		"step-down": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.stepDown(ctx, rng, admin)
		},
		"move-replica": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.moveReplica(ctx, rng, admin)
		},
		"eio": j.eio,
		"disk-full-wal": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.diskFull(ctx, rng, true)
		},
		"disk-full-data": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.diskFull(ctx, rng, false)
		},
		"freeze": j.freeze,
	}
	n := &nemesis.Nemesis{
		Schedule: nemesis.DefaultSchedule,
		Rand:     rng,
		Before:   j.supervise,
		Record: func(e nemesis.Event) {
			j.mu.Lock()
			j.events = append(j.events, e)
			j.mu.Unlock()
		},
	}
	for _, f := range j.faults {
		if inject, ok := all[f]; ok && faultWeights[f] > 0 {
			n.Faults = append(n.Faults, nemesis.Fault{Name: f, Weight: faultWeights[f], Inject: inject})
		}
	}
	return n
}

// up are the servers of a role that run, unpaused, and that no fault
// holds.
func (j *chaosJob) up(role string) []string {
	var out []string
	for _, name := range j.cluster.Procs(role) {
		if j.cluster.Running(name) && !j.cluster.Paused(name) && !j.holds.Held(name) {
			out = append(out, name)
		}
	}
	return out
}

func pick[T any](rng *rand.Rand, xs []T, n int) []T {
	xs = slices.Clone(xs)
	rng.Shuffle(len(xs), func(a, b int) { xs[a], xs[b] = xs[b], xs[a] })
	return xs[:min(n, len(xs))]
}

// restart starts a server the fault stopped, and waits a while for it to
// serve; a server that does not start is the supervisor's.
func (j *chaosJob) restart(ctx context.Context, name string) error {
	if err := j.cluster.Restart(ctx, name); err != nil {
		return err
	}
	return j.cluster.WaitReady(ctx, name, time.Minute)
}

func (j *chaosJob) crash(ctx context.Context, rng *rand.Rand, role string, n int) (nemesis.Heal, string, error) {
	victims := pick(rng, j.up(role), n)
	if len(victims) < n {
		return nil, "", nemesis.ErrNoTarget
	}
	release := j.holds.Hold(victims...)
	var errs []error
	for _, v := range victims {
		errs = append(errs, j.cluster.Crash(ctx, v))
	}
	return nemesis.Released(release, func(ctx context.Context) error {
		var errs []error
		for _, v := range victims {
			errs = append(errs, j.restart(ctx, v))
		}
		return errors.Join(errs...)
	}), strings.Join(victims, ","), errors.Join(errs...)
}

func (j *chaosJob) restartTServer(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	victims := pick(rng, j.up(kudu.TServer), 1)
	if len(victims) == 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	v := victims[0]
	release := j.holds.Hold(v)
	defer release()
	if _, err := j.cluster.Terminate(ctx, v); err != nil {
		return nil, v, err
	}
	return nil, v, j.restart(ctx, v)
}

func (j *chaosJob) pause(ctx context.Context, rng *rand.Rand, role string) (nemesis.Heal, string, error) {
	victims := pick(rng, j.up(role), 1)
	if len(victims) == 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	v := victims[0]
	release := j.holds.Hold(v)
	if err := j.cluster.Pause(ctx, v); err != nil {
		release()
		return nil, v, err
	}
	return nemesis.Released(release, func(ctx context.Context) error { return j.cluster.Resume(ctx, v) }), v, nil
}

// serverNodes are the nodes the servers run on.
func (j *chaosJob) serverNodes() []*torx.Node { return j.cluster.Nodes() }

func (j *chaosJob) allNodes() []*torx.Node {
	return append(slices.Clone(j.cluster.Nodes()), j.client.Nodes()...)
}

func names(nodes []*torx.Node) string {
	var out []string
	for _, n := range nodes {
		out = append(out, n.Name())
	}
	return strings.Join(out, ",")
}

func (j *chaosJob) healNet(ctx context.Context) error { return netfault.Heal(ctx, j.allNodes()...) }

func (j *chaosJob) partitionOne(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	n := pick(rng, j.serverNodes(), 1)[0]
	err := netfault.Isolate(ctx, n, j.allNodes())
	return j.healNet, n.Name(), err
}

func (j *chaosJob) partitionHalves(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	nodes := pick(rng, j.allNodes(), len(j.allNodes()))
	half := 1 + rng.IntN(len(nodes)-1)
	a, b := nodes[:half], nodes[half:]
	err := netfault.Partition(ctx, a, b)
	return j.healNet, names(a) + " | " + names(b), err
}

// partitionClient cuts the clients off from some server nodes, which
// still reach each other: the bridge is the nodes the clients still reach.
func (j *chaosJob) partitionClient(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	servers := pick(rng, j.serverNodes(), len(j.serverNodes()))
	cut := 1 + rng.IntN(max(1, len(servers)-1))
	reach := servers[cut:]
	err := netfault.Partition(ctx, append(slices.Clone(reach), j.client.Node()), servers)
	return j.healNet, "client cut from " + names(servers[:cut]), err
}

// tabletServers maps each tablet server's RPC address to its UUID, as the
// leader master knows them.
func (j *chaosJob) tabletServers(ctx context.Context) (map[string]string, error) {
	out, err := kudu.Tool(ctx, j.client.Node(), "tserver", "list", j.cluster.MasterAddrs(), "-columns=uuid,rpc-addresses", "-format=json")
	if err != nil {
		return nil, err
	}
	var list []struct {
		UUID string `json:"uuid"`
		Addr string `json:"rpc-addresses"`
	}
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		return nil, fmt.Errorf("tserver list: %w: %s", err, out)
	}
	m := map[string]string{}
	for _, t := range list {
		m[t.Addr] = t.UUID
	}
	return m, nil
}

func (j *chaosJob) stepDown(ctx context.Context, rng *rand.Rand, admin *kudu.Client) (nemesis.Heal, string, error) {
	tablets, err := admin.Tablets(ctx, tableName)
	if err != nil || len(tablets) == 0 {
		return nil, "", errors.Join(nemesis.ErrNoTarget, err)
	}
	t := tablets[rng.IntN(len(tablets))]
	args := []string{"tablet", "leader_step_down", j.cluster.MasterAddrs(), t.ID, "-timeout_ms=10000"}
	if rng.IntN(2) == 0 {
		args = append(args, "-abrupt")
	}
	_, err = kudu.Tool(ctx, j.client.Node(), args...)
	return nil, t.ID + " " + strings.Join(args[4:], " "), err
}

func (j *chaosJob) moveReplica(ctx context.Context, rng *rand.Rand, admin *kudu.Client) (nemesis.Heal, string, error) {
	tablets, err := admin.Tablets(ctx, tableName)
	if err != nil || len(tablets) == 0 {
		return nil, "", errors.Join(nemesis.ErrNoTarget, err)
	}
	uuids, err := j.tabletServers(ctx)
	if err != nil {
		return nil, "", errors.Join(nemesis.ErrNoTarget, err)
	}
	t := tablets[rng.IntN(len(tablets))]
	hosting := map[string]bool{}
	for _, r := range t.Replicas {
		hosting[net.JoinHostPort(r.Host, strconv.Itoa(r.Port))] = true
	}
	var from, to []string
	for addr := range uuids {
		if hosting[addr] {
			from = append(from, addr)
		} else if j.cluster.Running(j.cluster.ByAddr(addr)) {
			to = append(to, addr)
		}
	}
	slices.Sort(from)
	slices.Sort(to)
	if len(from) == 0 || len(to) == 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	f, d := from[rng.IntN(len(from))], to[rng.IntN(len(to))]
	target := fmt.Sprintf("%s %s -> %s", t.ID, j.cluster.ByAddr(f), j.cluster.ByAddr(d))
	_, err = kudu.Tool(ctx, j.client.Node(), "tablet", "change_config", "move_replica", j.cluster.MasterAddrs(), t.ID, uuids[f], uuids[d], "-timeout_ms=30000")
	return nil, target, err
}

// diskFault records a disk fault on server name, and returns what marks
// it healed: a server that exits meanwhile, or soon after, may have had
// cause to.
func (j *chaosJob) diskFault(name, fault string) func() {
	j.mu.Lock()
	i := len(j.diskFaults[name])
	j.diskFaults[name] = append(j.diskFaults[name], diskWindow{fault: fault, start: time.Now()})
	j.mu.Unlock()
	return func() {
		j.mu.Lock()
		j.diskFaults[name][i].end = time.Now()
		j.mu.Unlock()
	}
}

// eio fails a fraction of the I/O to one of a tablet server's data
// directories. Kudu marks a directory that fails as failed until the
// server restarts, and fails the tablets with data in it, so the heal
// restarts the server.
func (j *chaosJob) eio(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	victims := pick(rng, j.up(kudu.TServer), 1)
	if len(victims) == 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	v := victims[0]
	dirs := j.cluster.DataDirs(v)
	dir := dirs[rng.IntN(len(dirs))]
	frac := []string{"0.01", "0.1", "1"}[rng.IntN(3)]
	release := j.holds.Hold(v)
	done := j.diskFault(v, "eio")
	from := j.client.Node()
	// The glob first, and only if it took the fraction: the glob's default
	// is every file, the WAL's and the metadata's too.
	err := j.cluster.SetFlag(ctx, from, v, "env_inject_eio_globs", dir+"/*")
	if err == nil {
		err = j.cluster.SetFlag(ctx, from, v, "env_inject_eio", frac)
	}
	heal := func(ctx context.Context) error {
		defer done()
		if j.cluster.Running(v) {
			_ = j.cluster.SetFlag(ctx, from, v, "env_inject_eio", "0")
			if err := j.cluster.Crash(ctx, v); err != nil {
				return err
			}
		}
		return j.restart(ctx, v)
	}
	return nemesis.Released(release, heal), fmt.Sprintf("%s %s at %s", v, dir, frac), err
}

// diskFull fills a tablet server's WAL directory, or one of its data
// directories, and frees it at the heal.
func (j *chaosJob) diskFull(ctx context.Context, rng *rand.Rand, wal bool) (nemesis.Heal, string, error) {
	victims := pick(rng, j.up(kudu.TServer), 1)
	if len(victims) == 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	v := victims[0]
	dir := j.cluster.WALDir(v)
	if !wal {
		dirs := j.cluster.DataDirs(v)
		dir = dirs[rng.IntN(len(dirs))]
	}
	n := j.cluster.Node(v)
	fault := "disk-full-data"
	if wal {
		fault = "disk-full-wal"
	}
	done := j.diskFault(v, fault)
	err := diskfault.Fill(ctx, n, dir)
	return func(ctx context.Context) error {
		defer done()
		return diskfault.Free(ctx, n, dir)
	}, v + " " + dir, err
}

// freeze freezes every process of a server node, its master's and its
// tablet server's alike.
func (j *chaosJob) freeze(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	n := pick(rng, j.serverNodes(), 1)[0]
	var held []string
	for _, name := range j.cluster.Procs("") {
		if j.cluster.Node(name) == n {
			held = append(held, name)
		}
	}
	release := j.holds.Hold(held...)
	if err := resfault.Freeze(ctx, n); err != nil {
		release()
		return nil, n.Name(), err
	}
	return nemesis.Released(release, func(ctx context.Context) error { return resfault.Thaw(ctx, n) }), n.Name(), nil
}

// healAll undoes whatever faults are left once the nemesis is done: it
// heals its own last fault, but a heal can fail.
func (j *chaosJob) healAll(ctx context.Context) {
	if err := j.healNet(ctx); err != nil && j.netFaults() {
		torx.Logf(ctx, "warn", "healing the network: %v", err)
	}
	for _, n := range j.serverNodes() {
		if j.faulting("freeze") {
			_ = resfault.Thaw(ctx, n)
		}
	}
	for _, name := range j.cluster.Procs("") {
		if j.cluster.Paused(name) {
			_ = j.cluster.Resume(ctx, name)
		}
		if strings.HasPrefix(name, kudu.TServer) && (j.faulting("disk-full-wal") || j.faulting("disk-full-data")) {
			for _, d := range append([]string{j.cluster.WALDir(name)}, j.cluster.DataDirs(name)...) {
				_ = diskfault.Free(ctx, j.cluster.Node(name), d)
			}
		}
	}
	j.supervise(ctx)
}

func (j *chaosJob) netFaults() bool {
	return j.faulting("partition-one") || j.faulting("partition-halves") || j.faulting("partition-client")
}
