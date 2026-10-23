//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/diskfault"
	"github.com/dotnwat/torx/examples/nats/qa/nats"
	"github.com/dotnwat/torx/nemesis"
	"github.com/dotnwat/torx/netfault"
	"github.com/dotnwat/torx/resfault"
)

// faultWeights are the faults the nemesis draws from, by name, with their
// weights. Network faults need -netns, disk-full needs a node that can
// mount, freeze needs -cgroups; a run without them leaves them out.
var faultWeights = map[string]int{
	"crash":                4, // kill a server, restart it after the hold
	"crash-minority":       1, // kill as many servers as a majority survives
	"restart":              2, // SIGTERM a server and start it again
	"lame-duck":            1, // put a server in lame-duck mode, start it again once it exits
	"pause":                2, // SIGSTOP a server
	"freeze":               2, // freeze a server's node
	"partition-one":        3, // cut one server off from every other
	"partition-halves":     2, // split the servers in two
	"partition-ring":       1, // each server reaches only its neighbors: overlapping majorities
	"stepdown":             3, // have a stream's leader step down, to a server of the nemesis's choosing or not
	"stepdown-meta":        1, // have the meta leader step down
	"stepdown-consumer":    1, // have the consumer's leader step down
	"scale":                2, // change a stream's replica count, and change it back at the heal
	"scale-cancel":         1, // change a stream's replica count, then cancel the change
	"peer-remove":          1, // remove one of a stream's replicas, which JetStream replaces
	"evacuate":             1, // move one of a stream's replicas to another server
	"disk-full":            1, // fill a server's store
	"server-evacuate":      1, // move every stream's replica off a server
	"consumer-peer-remove": 1, // remove one of the work consumer's replicas, which JetStream replaces
	"consumer-evacuate":    1, // move one of the work consumer's replicas to another server
	"update-config":        1, // change a stream's direct gets, duplicate window, or compression
	"wipe":                 1, // crash a server and lose its store: a replaced disk
}

// offByDefault are faults "all" leaves out; a run names them to inject
// them.
var offByDefault = map[string]bool{
	// A server that comes back with no store has forgotten its Raft
	// votes and log; Raft assumes a server never does.
	"wipe": true,
}

// nemesis builds the job's nemesis: its faults, drawn from the run's, a
// supervisor that restarts what exited on its own before each, and a
// record of every fault and heal.
func (j *chaosJob) nemesis(rng *rand.Rand, admin, sys *nats.Client) *nemesis.Nemesis {
	all := map[string]func(context.Context, *rand.Rand) (nemesis.Heal, string, error){
		"crash": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.crash(ctx, rng, 1, false)
		},
		"crash-minority": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.crash(ctx, rng, (j.nodes-1)/2, false)
		},
		"wipe": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.crash(ctx, rng, 1, true)
		},
		"restart":          j.restartOne,
		"lame-duck":        j.lameDuck,
		"pause":            j.pause,
		"freeze":           j.freeze,
		"partition-one":    j.partitionOne,
		"partition-halves": j.partitionHalves,
		"partition-ring":   j.partitionRing,
		"stepdown": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.stepDown(ctx, rng, admin)
		},
		"stepdown-meta": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return nil, "meta", sys.MetaStepDown(ctx)
		},
		"stepdown-consumer": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return nil, workStream + " > " + consumerName, admin.ConsumerStepDown(ctx, workStream, consumerName)
		},
		"scale": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.scale(ctx, rng, admin, false)
		},
		"scale-cancel": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.scale(ctx, rng, admin, true)
		},
		"peer-remove": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.removePeer(ctx, rng, admin, false)
		},
		"evacuate": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.removePeer(ctx, rng, admin, true)
		},
		"disk-full": j.diskFull,
		"server-evacuate": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			victims := pick(rng, j.up(), 1)
			if len(victims) == 0 {
				return nil, "", nemesis.ErrNoTarget
			}
			return nil, victims[0], sys.EvacuateServer(ctx, victims[0])
		},
		"consumer-peer-remove": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.removeConsumerPeer(ctx, rng, admin, false)
		},
		"consumer-evacuate": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.removeConsumerPeer(ctx, rng, admin, true)
		},
		"update-config": func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.updateConfig(ctx, rng, admin)
		},
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

// up are the servers that run, unpaused, and that no fault holds.
func (j *chaosJob) up() []string {
	var out []string
	for _, name := range j.cluster.Procs() {
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

// supervise restarts every server that exited on its own and that no
// fault holds, and every one that runs with its JetStream shut down or a
// stream stuck on a write error, as an operator would.
func (j *chaosJob) supervise(ctx context.Context) {
	for _, name := range j.cluster.Procs() {
		if j.holds.Held(name) {
			continue
		}
		if !j.cluster.Running(name) {
			if err := j.cluster.Restart(ctx, name); err != nil {
				torx.Logf(ctx, "warn", "supervisor: restarting %s: %v", name, err)
			}
			continue
		}
		if err := j.restartIfBroken(ctx, name); err != nil {
			torx.Logf(ctx, "warn", "supervisor: restarting %s: %v", name, err)
		}
	}
}

func (j *chaosJob) crash(ctx context.Context, rng *rand.Rand, n int, wipe bool) (nemesis.Heal, string, error) {
	victims := pick(rng, j.up(), n)
	if len(victims) < n || n == 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	release := j.holds.Hold(victims...)
	var errs []error
	for _, v := range victims {
		errs = append(errs, j.cluster.Crash(ctx, v))
		if wipe {
			errs = append(errs, j.cluster.Wipe(ctx, v))
		}
	}
	return nemesis.Released(release, func(ctx context.Context) error {
		var errs []error
		for _, v := range victims {
			errs = append(errs, j.restart(ctx, v))
		}
		return errors.Join(errs...)
	}), strings.Join(victims, ","), errors.Join(errs...)
}

func (j *chaosJob) restartOne(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	victims := pick(rng, j.up(), 1)
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

// lameDuck puts a server in lame-duck mode, and starts it again once it
// has exited, as an operator rolling a cluster does.
func (j *chaosJob) lameDuck(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	victims := pick(rng, j.up(), 1)
	if len(victims) == 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	v := victims[0]
	release := j.holds.Hold(v)
	defer release()
	clean, err := j.cluster.LameDuck(ctx, v, time.Minute)
	if err != nil {
		return nil, v, err
	}
	if !clean {
		torx.Logf(ctx, "warn", "%s did not leave lame-duck mode within a minute and was killed", v)
	}
	return nil, v, j.restart(ctx, v)
}

func (j *chaosJob) pause(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	victims := pick(rng, j.up(), 1)
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

// freeze freezes every process of a server's node.
func (j *chaosJob) freeze(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	victims := pick(rng, j.up(), 1)
	if len(victims) == 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	v := victims[0]
	n := j.cluster.Node(v)
	release := j.holds.Hold(v)
	if err := resfault.Freeze(ctx, n); err != nil {
		release()
		return nil, v, err
	}
	return nemesis.Released(release, func(ctx context.Context) error { return resfault.Thaw(ctx, n) }), v, nil
}

func (j *chaosJob) serverNodes() []*torx.Node { return j.cluster.Nodes() }

func (j *chaosJob) serverNames(nodes []*torx.Node) string {
	var out []string
	for _, n := range nodes {
		out = append(out, j.cluster.ByNode(n))
	}
	return strings.Join(out, ",")
}

func (j *chaosJob) healNet(ctx context.Context) error { return netfault.Heal(ctx, j.serverNodes()...) }

func (j *chaosJob) partitionOne(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	n := pick(rng, j.serverNodes(), 1)[0]
	err := netfault.Isolate(ctx, n, j.serverNodes())
	return j.healNet, j.cluster.ByNode(n), err
}

func (j *chaosJob) partitionHalves(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	nodes := pick(rng, j.serverNodes(), len(j.serverNodes()))
	half := 1 + rng.IntN(len(nodes)-1)
	a, b := nodes[:half], nodes[half:]
	err := netfault.Partition(ctx, a, b)
	return j.healNet, j.serverNames(a) + " | " + j.serverNames(b), err
}

// partitionRing lets each server reach only the servers next to it in a
// random ring: every server sees a majority of three, and no two
// majorities are the same.
func (j *chaosJob) partitionRing(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	nodes := pick(rng, j.serverNodes(), len(j.serverNodes()))
	if len(nodes) < 4 {
		return nil, "", nemesis.ErrNoTarget
	}
	var groups [][]*torx.Node
	var desc []string
	for i := range nodes {
		g := []*torx.Node{nodes[i], nodes[(i+1)%len(nodes)]}
		groups = append(groups, g)
		desc = append(desc, j.serverNames(g))
	}
	err := netfault.Partition(ctx, groups...)
	return j.healNet, strings.Join(desc, " | "), err
}

// streams are the streams the job made.
func (j *chaosJob) streams() []string {
	return append([]string{regStream, logStream, ctrStream, batchStream, workStream}, copyStreams...)
}

func (j *chaosJob) stepDown(ctx context.Context, rng *rand.Rand, admin *nats.Client) (nemesis.Heal, string, error) {
	stream := pick(rng, j.streams(), 1)[0]
	si, err := admin.StreamInfo(ctx, stream)
	if err != nil || si.Cluster == nil {
		return nil, stream, errors.Join(nemesis.ErrNoTarget, err)
	}
	var preferred string
	if peers := si.Cluster.Peers(); len(peers) > 1 && rng.IntN(2) == 0 {
		preferred = peers[1+rng.IntN(len(peers)-1)]
	}
	err = admin.StepDown(ctx, stream, preferred)
	return nil, fmt.Sprintf("%s (leader %s, preferred %q)", stream, si.Cluster.Leader, preferred), err
}

// scale changes a stream's replica count; the heal changes it back. With
// cancel, it cancels the change instead, a moment after asking for it.
func (j *chaosJob) scale(ctx context.Context, rng *rand.Rand, admin *nats.Client, cancel bool) (nemesis.Heal, string, error) {
	stream := pick(rng, j.streams(), 1)[0]
	si, err := admin.StreamInfo(ctx, stream)
	if err != nil {
		return nil, stream, errors.Join(nemesis.ErrNoTarget, err)
	}
	cur := int(si.Config["num_replicas"].(float64))
	var to []int
	for _, r := range []int{1, 3, 5} {
		if r != cur && r <= j.nodes {
			to = append(to, r)
		}
	}
	if len(to) == 0 {
		return nil, stream, nemesis.ErrNoTarget
	}
	r := to[rng.IntN(len(to))]
	cfg := si.Config
	cfg["num_replicas"] = r
	target := fmt.Sprintf("%s R%d -> R%d", stream, cur, r)
	if _, err := admin.UpdateStream(ctx, cfg); err != nil {
		return nil, target, err
	}
	restore := func(ctx context.Context) error {
		si, err := admin.StreamInfo(ctx, stream)
		if err != nil {
			return err
		}
		if int(si.Config["num_replicas"].(float64)) == j.replicas {
			return nil
		}
		cfg := si.Config
		cfg["num_replicas"] = j.replicas
		_, err = admin.UpdateStream(ctx, cfg)
		return err
	}
	if cancel {
		nemesis.Sleep(ctx, time.Duration(rng.IntN(2000))*time.Millisecond)
		// A change done before the cancel came is undone at the heal.
		return restore, target + " (cancelled)", admin.CancelMove(ctx, stream)
	}
	return restore, target, nil
}

// removePeer removes one of a stream's replicas, which JetStream places
// anew on another server, or, with evacuate, moves it there.
func (j *chaosJob) removePeer(ctx context.Context, rng *rand.Rand, admin *nats.Client, evacuate bool) (nemesis.Heal, string, error) {
	stream := pick(rng, j.streams(), 1)[0]
	si, err := admin.StreamInfo(ctx, stream)
	if err != nil || si.Cluster == nil {
		return nil, stream, errors.Join(nemesis.ErrNoTarget, err)
	}
	peers := si.Cluster.Peers()
	if len(peers) < 2 || len(peers) >= j.nodes {
		return nil, stream, nemesis.ErrNoTarget
	}
	peer := peers[rng.IntN(len(peers))]
	target := stream + " " + peer
	if evacuate {
		return nil, target, admin.EvacuatePeer(ctx, stream, peer)
	}
	return nil, target, admin.RemovePeer(ctx, stream, peer)
}

// removeConsumerPeer removes one of the work consumer's replicas, or with
// evacuate moves it.
func (j *chaosJob) removeConsumerPeer(ctx context.Context, rng *rand.Rand, admin *nats.Client, evacuate bool) (nemesis.Heal, string, error) {
	ci, err := admin.ConsumerInfo(ctx, workStream, consumerName)
	if err != nil || ci.Cluster == nil {
		return nil, "", errors.Join(nemesis.ErrNoTarget, err)
	}
	peers := ci.Cluster.Peers()
	if len(peers) < 2 {
		return nil, "", nemesis.ErrNoTarget
	}
	peer := peers[rng.IntN(len(peers))]
	return nil, workStream + " > " + consumerName + " " + peer, admin.RemoveConsumerPeer(ctx, workStream, consumerName, peer, evacuate)
}

// updateConfig changes a stream's config as an operator might while it
// runs: turns direct gets on or off, changes how long it remembers message
// ids, or changes its compression, which rewrites its blocks as they fill.
func (j *chaosJob) updateConfig(ctx context.Context, rng *rand.Rand, admin *nats.Client) (nemesis.Heal, string, error) {
	stream := pick(rng, []string{regStream, logStream, ctrStream, batchStream, workStream}, 1)[0]
	si, err := admin.StreamInfo(ctx, stream)
	if err != nil {
		return nil, stream, errors.Join(nemesis.ErrNoTarget, err)
	}
	cfg := si.Config
	var change string
	switch rng.IntN(3) {
	case 0:
		on, _ := cfg["allow_direct"].(bool)
		cfg["allow_direct"] = !on
		change = fmt.Sprintf("allow_direct %v", !on)
	case 1:
		// At least as long as a write and its resends take, so that a resend
		// is always deduplicated.
		w := []int64{int64(30 * time.Second), int64(time.Minute), int64(2 * time.Minute)}[rng.IntN(3)]
		cfg["duplicate_window"] = w
		change = fmt.Sprintf("duplicate_window %v", time.Duration(w))
	default:
		if cfg["storage"] != "file" {
			return nil, stream, nemesis.ErrNoTarget
		}
		to := "none"
		if cfg["compression"] != "s2" {
			to = "s2"
		}
		if to == "s2" && j.allow["compress-truncate"] && j.settings["sync"] == "always" {
			return nil, stream, nemesis.ErrNoTarget
		}
		cfg["compression"] = to
		change = "compression " + to
	}
	_, err = admin.UpdateStream(ctx, cfg)
	return nil, stream + " " + change, err
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

// diskFull fills a server's store, and frees it at the heal. A server
// whose write failed for want of space shuts its JetStream down until it
// restarts, by design, so the heal then restarts it, as its operator
// would.
func (j *chaosJob) diskFull(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	victims := pick(rng, j.up(), 1)
	if len(victims) == 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	v := victims[0]
	dir := j.cluster.StoreDir(v)
	n := j.cluster.Node(v)
	release := j.holds.Hold(v)
	done := j.diskFault(v, "disk-full")
	err := diskfault.Fill(ctx, n, dir)
	return nemesis.Released(release, func(ctx context.Context) error {
		defer done()
		if err := diskfault.Free(ctx, n, dir); err != nil {
			return err
		}
		// The server may yet fail a write it buffered while the disk was
		// full: give it a moment before looking.
		nemesis.Sleep(ctx, time.Second)
		return j.restartIfBroken(ctx, v)
	}), v, err
}

// restartIfBroken restarts server name if it runs with its JetStream
// shut down, which it does by design when a write fails for want of
// space, or with a stream stuck on a write error, which it does not leave
// until it restarts. The second is an anomaly; both are recorded.
func (j *chaosJob) restartIfBroken(ctx context.Context, name string) error {
	if !j.cluster.Running(name) || j.cluster.Paused(name) {
		return nil
	}
	code, body, err := nats.Healthz(ctx, j.cluster.MonitorURL(name), "")
	if err != nil || code == 200 {
		return nil
	}
	var why string
	switch {
	case strings.Contains(body, "not enabled"):
		why = "JetStream shut down"
	case strings.Contains(body, "write error"):
		why = "write error"
		j.note(anomaly{Kind: "stream-write-error", Detail: fmt.Sprintf("%s at %s: %s", name, time.Now().Format(time.RFC3339Nano), strings.TrimSpace(body))})
	default:
		return nil
	}
	torx.Logf(ctx, "info", "%s: %s; restarting it", name, why)
	j.mu.Lock()
	j.restarted = append(j.restarted, name+": "+why)
	j.mu.Unlock()
	if _, err := j.cluster.Terminate(ctx, name); err != nil {
		return err
	}
	return j.restart(ctx, name)
}

// healAll undoes whatever faults are left once the nemesis is done: it
// heals its own last fault, but a heal can fail.
func (j *chaosJob) healAll(ctx context.Context) {
	if j.netFaults() {
		if err := j.healNet(ctx); err != nil {
			torx.Logf(ctx, "warn", "healing the network: %v", err)
		}
	}
	for _, name := range j.cluster.Procs() {
		if j.faulting("freeze") {
			_ = resfault.Thaw(ctx, j.cluster.Node(name))
		}
		if j.cluster.Paused(name) {
			_ = j.cluster.Resume(ctx, name)
		}
		if j.faulting("disk-full") {
			_ = diskfault.Free(ctx, j.cluster.Node(name), j.cluster.StoreDir(name))
		}
	}
	j.supervise(ctx)
}

func (j *chaosJob) netFaults() bool {
	return j.faulting("partition-one") || j.faulting("partition-halves") || j.faulting("partition-ring")
}
