//go:build unix

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/diskfault"
	"github.com/dotnwat/torx/examples/nats/qa/nats"
	"github.com/dotnwat/torx/nemesis"
	"github.com/dotnwat/torx/netfault"
	"github.com/dotnwat/torx/resfault"
	"github.com/nats-io/nats.go/jetstream"
)

// Parameters of nats.chaos.
const (
	paramDuration  = "duration"   // seconds of faults and load
	paramClients   = "clients"    // concurrent clients
	paramKeys      = "keys"       // registers the clients contend on
	paramNodes     = "nodes"      // servers: one on each node
	paramReplicas  = "replicas"   // the streams' replica count
	paramFaults    = "faults"     // comma-separated fault names, or "all" and exclusions ("all,-pause")
	paramConfig    = "config"     // "random": settings drawn from the seed; "default": the server's
	paramValueSize = "value-size" // the largest payload written, in bytes
	paramRate      = "rate"       // operations a second each client issues at most; 0 for no bound
	paramConsumers = "consumers"  // clients fetching from the work stream's consumer
	paramFeatures  = "features"   // the servers' feature flags, "flag=true,flag=false", over the job's
	paramTimeout   = "timeout"    // each client operation's timeout, in milliseconds
	paramDiskSize  = "disk-size"  // MiB of each server's size-limited store, for disk-full
	paramNemeses   = "nemeses"    // nemeses injecting faults at once
	paramStorage   = "storage"    // the streams' storage: "file", "memory", or "" to draw it
	paramSync      = "sync"       // the servers' sync_interval: "always", a duration, "default", or "" to draw it
	paramAllow     = "allow"      // comma-separated known issues reported as warnings (README.md)
	paramTrial     = "trial"      // distinguishes repeated variants; each draws its own seed

	defaultDuration = 60
	// defaultAllow are the known issues a run allows unless it says
	// otherwise: every one there is, so that a run finds others.
	defaultAllow = "stale-direct-read,stale-read,deposed-refusal,flush-error,compress-truncate,copy-stall,snapshot-sources"
)

const (
	// settleTimeout bounds how long, once every fault is healed, the
	// cluster may take to be healthy again.
	settleTimeout = 3 * time.Minute
	// checkBudget bounds the search of each register's history.
	checkBudget = 30 * time.Second
)

// knownIssues are issues of NATS a run can allow, so that a run that keeps
// finding one can find others. README.md describes each.
var knownIssues = map[string]bool{
	// A leader whose flush failed for want of space answers from a store
	// that dropped messages it had applied, and passes conditional writes
	// whose condition is already false (results/nats-findings, finding 2).
	// The checker tells these apart: an anomaly of a write or a read in
	// flight while a disk was full, or soon after.
	"flush-error": true,
	// With the feature flag js_snapshot_sources, on by default from 2.16, a
	// stream that sources others, led by a replica that crashed after its
	// last snapshot, sources again what it had sourced since
	// (results/nats-findings, finding 5). Allowed, the servers run with
	// the flag off.
	"snapshot-sources": true,
	// A stream's leader cut off from its peers keeps answering publishes
	// until it learns it was deposed, and refuses a conditional one from
	// its stale state -- "wrong last sequence" -- that the new leader
	// stores (results/nats-findings, finding 4; the nats.deposed job
	// reproduces it). The checker tells these apart: a refused write the
	// stream holds, sent while a fault silenced a server or soon after.
	"deposed-refusal": true,
	// A mirror or a stream that sources others stops copying, and reports
	// no lag, after it is scaled to one replica and back or its origin
	// loses a replica, until its leader changes (results/nats-findings,
	// finding 3). The job tells it apart: a copy behind once every fault
	// healed that a new leader brings up to date.
	"copy-stall": true,
	// A file stream compressed with S2 corrupts its last block when its
	// store is truncated within that block -- as a replica that syncs
	// always does on every restart after a crash, back to its last
	// snapshot -- and every message in the block becomes unreadable on
	// that replica (results/nats-findings, finding 1). Allowed, a run never
	// draws S2 with sync always.
	"compress-truncate": true,
	// A stream's leader cut off from its peers, or paused, keeps answering
	// reads from what it has until it learns it was deposed, and every
	// server that believes it leads a stream answers a message get: JetStream
	// has no leader leases. The checker tells these apart: a leader read that
	// breaks linearizability while a fault silenced a server, or soon after.
	"stale-read": true,
	// Direct gets are served by any replica, which may be behind its
	// leader, so they may read a register's older value.
	"stale-direct-read": true,
}

// chaosJob is nats.chaos: a randomized test of JetStream's guarantees
// under faults. Clients write registers -- the last message on each of a
// few subjects of a replicated stream -- unconditionally and with
// compare-and-set on the subject's last sequence, append to a log with
// compare-and-set on the stream's, resend writes whose answers were lost
// under the same message id, and read the registers from the stream's
// leader and from any replica, while a nemesis crashes, pauses, partitions,
// and freezes servers, fills their disks, and moves streams' leaders and
// replicas. Once every fault is healed, every replica of each stream is
// read, as its leader in turn, and must hold the same messages; every
// acked write must be there, once, at the sequence it was acked at; no
// refused write may be; every compare-and-set must follow the sequence it
// named; the stream must order writes as real time did; and every read of
// a register from its leader must be linearizable.
type chaosJob struct {
	torx.JobBase
	cluster *nats.Cluster

	duration  time.Duration
	clients   int
	keys      int
	nodes     int
	replicas  int
	faults    []string
	random    bool
	valueSize int
	rate      int
	consumers int
	timeout   time.Duration
	diskSize  int64
	nemeses   int
	storage   string
	sync      string
	allow     map[string]bool

	streamCfg map[string]any // settings every stream is made with
	settings  map[string]any // what was drawn, for the record

	mu     sync.Mutex
	events []nemesis.Event
	notes  []anomaly
	holds  nemesis.Holds
	// diskFaults are each server's disk faults, while and since they were
	// held: a server that exits under one may have had cause to.
	diskFaults map[string][]diskWindow
	// start is when the clients' clock started.
	start time.Time
	// replicas0 is each stream's state on each server that holds it, once
	// every fault healed and before the final reads moved any leader.
	replicas0 map[string][]nats.ReplicaState
	// restarted are the servers the supervisor restarted because they had
	// shut their JetStream down or held a stream stuck on a write error,
	// once for each time, with why.
	restarted []string
}

type diskWindow struct {
	fault      string
	start, end time.Time
}

func (*chaosJob) Matrix() []torx.Params {
	return []torx.Params{{paramDuration: defaultDuration}}
}

func parseFaults(spec string) []string {
	parts := strings.Split(spec, ",")
	if parts[0] != "all" {
		return parts
	}
	faults := slices.DeleteFunc(slices.Sorted(maps.Keys(faultWeights)), func(f string) bool { return offByDefault[f] })
	for _, p := range parts[1:] {
		faults = slices.DeleteFunc(faults, func(f string) bool { return "-"+f == p })
		if f, ok := strings.CutPrefix(p, "+"); ok {
			faults = append(faults, f)
		}
	}
	return faults
}

func (*chaosJob) ResolveParams(p torx.Params) (torx.Params, error) {
	out := torx.Params{
		paramDuration: defaultDuration, paramClients: 8, paramKeys: 8, paramNodes: 5, paramReplicas: 3,
		paramFaults: "all", paramConfig: "random", paramValueSize: 256, paramRate: 100, paramConsumers: 2, paramFeatures: "", paramTimeout: 5_000, paramDiskSize: 256,
		paramNemeses: 1, paramStorage: "", paramSync: "", paramAllow: defaultAllow,
	}
	for k, v := range p {
		switch k {
		case paramDuration, paramClients, paramKeys, paramNodes, paramReplicas, paramValueSize, paramTimeout,
			paramDiskSize, paramNemeses, paramTrial, paramRate, paramConsumers:
			n, ok := intValue(v)
			bounds := map[string][2]int{
				paramDuration: {1, 7200}, paramClients: {1, 64}, paramKeys: {1, 10_000}, paramNodes: {1, 9},
				paramReplicas: {1, 5}, paramValueSize: {0, 1 << 20}, paramTimeout: {100, 600_000},
				paramDiskSize: {16, 8192}, paramNemeses: {1, 4}, paramTrial: {0, math.MaxInt32}, paramRate: {0, 100_000}, paramConsumers: {1, 16},
			}[k]
			if !ok || n < bounds[0] || n > bounds[1] {
				return nil, fmt.Errorf("%s must be an integer in [%d, %d], got %v", k, bounds[0], bounds[1], v)
			}
			out[k] = n
		case paramFaults:
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a string, got %v", k, v)
			}
			for _, f := range parseFaults(s) {
				if _, ok := faultWeights[f]; !ok && f != "none" {
					return nil, fmt.Errorf("unknown fault %q", f)
				}
			}
			out[k] = s
		case paramAllow:
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a string, got %v", k, v)
			}
			for a := range strings.SplitSeq(s, ",") {
				if a != "" && !knownIssues[a] {
					return nil, fmt.Errorf("unknown issue %q to allow", a)
				}
			}
			out[k] = s
		case paramConfig:
			if v != "random" && v != "default" {
				return nil, fmt.Errorf("%s must be random or default, got %v", k, v)
			}
			out[k] = v
		case paramFeatures:
			str, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a string, got %v", k, v)
			}
			if _, err := parseFeatures(str); err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
			out[k] = str
		case paramStorage:
			if v != "" && v != "file" && v != "memory" {
				return nil, fmt.Errorf("%s must be file, memory, or empty, got %v", k, v)
			}
			out[k] = v
		case paramSync:
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a string, got %v", k, v)
			}
			if s != "" && s != "always" && s != "default" {
				if _, err := time.ParseDuration(s); err != nil {
					return nil, fmt.Errorf("%s must be always, default, a duration, or empty: %w", k, err)
				}
			}
			out[k] = s
		default:
			return nil, fmt.Errorf("unknown parameter %q", k)
		}
	}
	n, _ := intValue(out[paramNodes])
	r, _ := intValue(out[paramReplicas])
	if r > n {
		return nil, fmt.Errorf("%d replicas need as many servers, have %d", r, n)
	}
	return out, nil
}

func intValue(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case float64:
		if n == math.Trunc(n) {
			return int(n), true
		}
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil
	}
	return 0, false
}

func (j *chaosJob) Declare(jc *torx.JobContext) {
	p := jc.Params
	j.duration = time.Duration(p.Int(paramDuration, defaultDuration)) * time.Second
	j.clients = p.Int(paramClients, 8)
	j.keys = p.Int(paramKeys, 8)
	j.nodes = p.Int(paramNodes, 5)
	j.replicas = p.Int(paramReplicas, 3)
	j.faults = parseFaults(p.String(paramFaults, "all"))
	j.random = p.String(paramConfig, "random") == "random"
	j.valueSize = p.Int(paramValueSize, 256)
	j.rate = p.Int(paramRate, 100)
	j.consumers = p.Int(paramConsumers, 2)
	j.timeout = time.Duration(p.Int(paramTimeout, 5_000)) * time.Millisecond
	j.diskSize = int64(p.Int(paramDiskSize, 256)) << 20
	j.nemeses = p.Int(paramNemeses, 1)
	j.storage = p.String(paramStorage, "")
	j.sync = p.String(paramSync, "")
	j.allow = map[string]bool{}
	for a := range strings.SplitSeq(p.String(paramAllow, defaultAllow), ",") {
		if a != "" {
			j.allow[a] = true
		}
	}
	j.diskFaults = map[string][]diskWindow{}
	cfg := j.drawConfig(jc.Rand("config"))
	cfg.Prepare = j.prepare
	features := map[string]bool{}
	if j.allow["snapshot-sources"] {
		features["js_snapshot_sources"] = false
	}
	pinned, _ := parseFeatures(p.String(paramFeatures, ""))
	maps.Copy(features, pinned)
	if len(features) > 0 {
		cfg.Server = map[string]any{"feature_flags": features}
		j.settings["features"] = features
	}
	j.cluster = nats.New(clusterName, j.nodes, cfg)
	jc.Register(j.cluster)
}

// drawConfig draws the servers' settings and the streams': how they store
// and sync, compress, and serve direct gets. With config=default, only
// what the job needs.
func (j *chaosJob) drawConfig(rng *rand.Rand) nats.Config {
	pick := func(xs ...string) string { return xs[rng.IntN(len(xs))] }
	cfg := nats.Config{}
	j.streamCfg = map[string]any{"storage": "file", "allow_direct": true}
	storage, sync := j.storage, j.sync
	if j.random {
		if storage == "" {
			storage = pick("file", "file", "file", "memory")
		}
		if sync == "" {
			sync = pick("always", "default", "1s")
		}
		j.streamCfg["allow_direct"] = rng.IntN(3) > 0
		if storage == "file" {
			j.streamCfg["compression"] = pick("none", "s2")
		}
		j.streamCfg["duplicate_window"] = int64(time.Duration([]int{30, 120}[rng.IntN(2)]) * time.Second)
	}
	if storage == "" {
		storage = "file"
	}
	j.streamCfg["storage"] = storage
	switch sync {
	case "", "default":
	case "always":
		cfg.SyncAlways = true
	default:
		cfg.SyncInterval, _ = time.ParseDuration(sync)
	}
	if j.allow["compress-truncate"] && sync == "always" && j.streamCfg["compression"] == "s2" {
		j.streamCfg["compression"] = "none"
	}
	j.settings = map[string]any{"stream": j.streamCfg, "sync": sync, "durable_sources": j.random && rng.IntN(2) == 0}
	return cfg
}

// prepare mounts a size-limited filesystem over a server's store, if the
// run fills disks.
func (j *chaosJob) prepare(ctx context.Context, name string, n *torx.Node) error {
	if !j.faulting("disk-full") {
		return nil
	}
	return j.cluster.Limit(ctx, name, j.cluster.StoreDir(name), j.diskSize)
}

func (j *chaosJob) faulting(f string) bool { return slices.Contains(j.faults, f) }

func (j *chaosJob) Setup(ctx context.Context, jc *torx.JobContext) error {
	// Faults the nodes cannot take are left out, before anything starts:
	// prepare mounts disks only for a run that fills them.
	nodes := j.cluster.Nodes()
	drop := func(why string, fs ...string) {
		for _, f := range fs {
			if j.faulting(f) {
				torx.Logf(ctx, "warn", "leaving out %s: %s", f, why)
				j.faults = slices.DeleteFunc(j.faults, func(x string) bool { return x == f })
			}
		}
	}
	if err := netfault.Check(ctx, nodes); err != nil {
		drop(err.Error(), "partition-one", "partition-halves", "partition-ring")
	}
	if err := diskfault.Check(ctx, nodes[0], nodes[0].Scratch().Root); err != nil {
		drop(err.Error(), "disk-full")
	}
	if err := resfault.Check(nodes[0]); err != nil {
		drop(err.Error(), "freeze")
	}
	if j.nodes < 3 {
		drop("fewer than three servers", "crash-minority", "partition-ring")
	}
	if j.streamCfg["storage"] == "memory" {
		// A memory stream's replica that restarts empty catches up from
		// its peers, but one whose peers restarted too has nothing to
		// catch up from: lost by design.
		drop("memory streams", "crash-minority", "wipe")
	}
	return j.JobBase.Setup(ctx, jc)
}

// streamConfig is the config of stream name, on subjects.
func (j *chaosJob) streamConfig(name string, subjects ...string) map[string]any {
	cfg := maps.Clone(j.streamCfg)
	cfg["name"] = name
	cfg["subjects"] = subjects
	cfg["num_replicas"] = j.replicas
	return cfg
}

// connect connects client name to the cluster, preferring server i mod
// the servers' count.
func (j *chaosJob) connect(name string, i int, sys bool) (*nats.Client, error) {
	procs := j.cluster.Procs()
	var urls []string
	for k := range procs {
		s := procs[(i+k)%len(procs)]
		if sys {
			urls = append(urls, j.cluster.SysURL(s))
		} else {
			urls = append(urls, j.cluster.URL(s))
		}
	}
	return nats.Connect(strings.Join(urls, ","), name)
}

func (j *chaosJob) Run(ctx context.Context, jc *torx.JobContext) error {
	admin, err := j.connect("admin", 0, false)
	if err != nil {
		return err
	}
	defer admin.Close()
	sys, err := j.connect("sys", 0, true)
	if err != nil {
		return err
	}
	defer sys.Close()
	ctr := j.streamConfig(ctrStream, "ctr.>")
	ctr["allow_msg_counter"] = true
	batch := j.streamConfig(batchStream, "batch.>")
	batch["allow_atomic"] = true
	sched := j.streamConfig(schedStream, "sched.>")
	sched["allow_msg_schedules"] = true
	for _, cfg := range []map[string]any{j.streamConfig(regStream, "reg.>"), j.streamConfig(logStream, "log.>"), ctr, batch, j.streamConfig(workStream, "work.>"), sched} {
		if err := createStream(ctx, admin, cfg); err != nil {
			return err
		}
	}
	if err := createConsumer(ctx, admin); err != nil {
		return err
	}
	if err := j.createCopies(ctx, admin); err != nil {
		return err
	}
	torx.Logf(ctx, "info", "config: %v; faults %v", j.settings, j.faults)

	var clients []*nats.Client
	for c := range j.clients {
		cl, err := j.connect(fmt.Sprintf("c%d", c), c, false)
		if err != nil {
			return err
		}
		defer cl.Close()
		clients = append(clients, cl)
	}
	weights := maps.Clone(opWeights)
	if j.streamCfg["allow_direct"] != true {
		delete(weights, "dget")
	}
	w := newWorkload(clients, j.keys, j.valueSize, j.rate, j.timeout, weights)
	j.start = w.start
	cs := &consumers{w: w}
	for c := range j.consumers {
		cl, err := j.connect(fmt.Sprintf("consumer%d", c), j.clients+c, false)
		if err != nil {
			return err
		}
		defer cl.Close()
		cs.clients = append(cs.clients, cl)
	}

	loadCtx, stopLoad := context.WithTimeout(ctx, j.duration)
	defer stopLoad()
	var wg sync.WaitGroup
	for c := range j.clients {
		rng := jc.Rand(fmt.Sprintf("client-%d", c))
		wg.Go(func() { w.run(loadCtx, c, rng) })
	}
	for c := range j.consumers {
		rng := jc.Rand(fmt.Sprintf("consumer-%d", c))
		wg.Go(func() { cs.run(loadCtx, c, rng, false) })
	}
	for i := range j.nemeses {
		nem := j.nemesis(jc.Rand(fmt.Sprintf("nemesis-%d", i)), admin, sys)
		if len(nem.Faults) > 0 {
			wg.Go(func() { nem.Run(loadCtx) })
		}
	}
	wg.Wait()

	// Heal what the nemesis could not, and bring every server back.
	j.healAll(ctx)
	if health, ok := j.settle(ctx, admin); !ok {
		j.note(anomaly{Kind: "no-recovery", Detail: fmt.Sprintf("the cluster was not healthy %v after every fault healed:\n%s", settleTimeout, health)})
	}

	// The consumers deliver what is left, and ack it all.
	isDrained, drainState := j.drain(ctx, cs, jc)
	if !isDrained {
		j.note(anomaly{Kind: "consumer-stuck", Detail: fmt.Sprintf("the consumer did not drain within %v: %s", drainTimeout, drainState)})
	}

	// The copies copy what is left.
	if state, ok := j.caughtUp(ctx, admin); !ok {
		// A new leader starts copying again what a stalled one stopped.
		detail := state + "\n" + j.describeReplicas(ctx, copyStreams)
		for _, s := range copyStreams {
			_ = admin.StepDown(ctx, s, "")
		}
		if state, ok := j.caughtUp(ctx, admin); ok {
			j.note(anomaly{Kind: "copy-stall", Detail: "the copies caught up with their origins only once their leaders changed: " + detail})
		} else {
			j.note(anomaly{Kind: "copies-behind", Detail: "the copies did not catch up with their origins, even under new leaders: " + state + "\n" + j.describeReplicas(ctx, copyStreams)})
		}
	}

	// What each replica holds, before the final reads move the streams'
	// leaders: a copy's new leader copies what it lacks at once.
	j.replicas0 = j.replicaStates(ctx)

	finals := map[string]*final{}
	for _, stream := range j.streams() {
		f, bad := j.finalRead(ctx, admin, stream)
		for _, a := range bad {
			j.note(a)
		}
		if f != nil {
			finals[stream] = f
		}
	}
	return j.check(ctx, jc, w.history(), cs, isDrained, finals)
}

// drainTimeout bounds how long the consumers may take to drain the work
// stream once every fault is healed.
const drainTimeout = 2 * time.Minute

// drain runs the consumers, acking everything, until the consumer has
// nothing left to deliver or to wait for an ack of.
func (j *chaosJob) drain(ctx context.Context, cs *consumers, jc *torx.JobContext) (bool, string) {
	dctx, cancel := context.WithTimeout(ctx, drainTimeout)
	var wg sync.WaitGroup
	for c := range cs.clients {
		rng := jc.Rand(fmt.Sprintf("drain-%d", c))
		wg.Go(func() { cs.run(dctx, c, rng, true) })
	}
	var state string
	err := torx.WaitUntil(dctx, func(ctx context.Context) (bool, error) {
		ok, s := drained(ctx, cs.clients[0])
		if ctx.Err() == nil {
			state = s
		}
		return ok, nil
	}, 2*time.Second)
	cancel()
	wg.Wait()
	return err == nil, state
}

// createConsumer creates the work stream's durable consumer, retrying
// while the cluster elects its leader.
func createConsumer(ctx context.Context, c *nats.Client) error {
	js, err := jetstream.New(c.NC)
	if err != nil {
		return err
	}
	var last error
	err = torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_, last = js.CreateOrUpdateConsumer(rctx, workStream, consumerConfig())
		return last == nil, nil
	}, time.Second)
	if err != nil {
		return fmt.Errorf("create consumer: %w (last: %w)", err, last)
	}
	return nil
}

// createStream creates a stream, retrying while the cluster elects its
// leaders, and waits for the stream to have a leader.
func createStream(ctx context.Context, c *nats.Client, cfg map[string]any) error {
	var last error
	err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := c.CreateStream(rctx, cfg); err != nil {
			last = err
			return false, nil
		}
		si, err := c.StreamInfo(rctx, cfg["name"].(string))
		last = err
		return err == nil && si.Cluster != nil && si.Cluster.Leader != "", nil
	}, time.Second)
	if err != nil {
		return fmt.Errorf("create stream %s: %w (last: %w)", cfg["name"], err, last)
	}
	return nil
}

// settle waits for every server to say every stream and consumer it hosts
// is current, and for each stream to have its replicas, all current. It
// returns what it last saw that was not.
func (j *chaosJob) settle(ctx context.Context, admin *nats.Client) (string, bool) {
	ctx, cancel := context.WithTimeout(ctx, settleTimeout)
	defer cancel()
	var last string
	err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		var not []string
		for _, s := range j.cluster.Procs() {
			code, body, err := nats.Healthz(ctx, j.cluster.MonitorURL(s), "")
			if err != nil || code != 200 {
				not = append(not, fmt.Sprintf("%s: %d %v %s", s, code, err, strings.TrimSpace(body)))
			}
		}
		for _, stream := range j.streams() {
			si, err := admin.StreamInfo(ctx, stream)
			switch {
			case err != nil:
				not = append(not, fmt.Sprintf("%s: %v", stream, err))
			case si.Cluster == nil || si.Cluster.Leader == "":
				not = append(not, fmt.Sprintf("%s: no leader", stream))
			case len(si.Cluster.Peers()) != j.replicas:
				not = append(not, fmt.Sprintf("%s: replicas %v, want %d", stream, si.Cluster.Peers(), j.replicas))
			default:
				for _, r := range si.Cluster.Replicas {
					if !r.Current || r.Offline {
						not = append(not, fmt.Sprintf("%s: replica %s not current (lag %d, active %v, offline %v)", stream, r.Name, r.Lag, r.Active, r.Offline))
					}
				}
			}
		}
		// A pass the deadline cut short says nothing new.
		if ctx.Err() == nil {
			last = strings.Join(not, "\n")
		}
		return len(not) == 0, nil
	}, time.Second)
	return last, err == nil
}

func (j *chaosJob) note(a anomaly) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.notes = append(j.notes, a)
}

// check runs every check of the history, writes the history and what the
// checks found to the results, and fails the job on anything found that
// the run does not allow.
func (j *chaosJob) check(ctx context.Context, jc *torx.JobContext, hist []op, cs *consumers, isDrained bool, finals map[string]*final) error {
	var bad []anomaly
	var unknown []string
	deliveries := cs.history()
	if f := finals[workStream]; f != nil {
		bad = append(bad, checkDeliveries(deliveries, f, isDrained)...)
	}
	for _, stream := range j.streams() {
		f := finals[stream]
		if f == nil || slices.Contains(copyStreams, stream) || stream == schedStream {
			continue
		}
		bad = append(bad, checkStream(hist, stream, f)...)
	}
	if f := finals[batchStream]; f != nil {
		bad = append(bad, checkBatches(hist, f)...)
	}
	if f := finals[schedStream]; f != nil {
		bad = append(bad, checkSchedules(hist, f)...)
	}
	if o, m := finals[regStream], finals[mirrorStream]; o != nil && m != nil {
		bad = append(bad, checkMirror(o, m)...)
	}
	if agg := finals[aggStream]; agg != nil {
		var origins []*final
		for _, s := range aggOrigins {
			if f := finals[s]; f != nil {
				origins = append(origins, f)
			}
		}
		if len(origins) == len(aggOrigins) {
			bad = append(bad, checkSources(origins, agg)...)
		}
	}
	if o, a := finals[ctrStream], finals[ctrAggStream]; o != nil && a != nil {
		bad = append(bad, checkCounterSource(o, a)...)
	}
	if f := finals[ctrStream]; f != nil {
		bad = append(bad, checkCounters(hist, f)...)
		b, u := checkReads(ctx, hist, f, map[string]bool{"cget": true}, checkBudget)
		bad = append(bad, b...)
		unknown = append(unknown, u...)
	}
	if f := finals[regStream]; f != nil {
		b, u := checkReads(ctx, hist, f, map[string]bool{"get": true}, checkBudget)
		bad = append(bad, b...)
		unknown = append(unknown, u...)
		// Direct gets may be stale; each must still be a value the
		// stream holds, and a stale one is its own kind.
		b, _ = checkReads(ctx, hist, f, map[string]bool{"get": true, "dget": true}, checkBudget)
		for _, a := range b {
			if a.Kind == "nonlinearizable-read" {
				a.Kind = "stale-direct-read"
			}
			if !slices.ContainsFunc(bad, func(x anomaly) bool { return x.Detail == a.Detail }) {
				bad = append(bad, a)
			}
		}
	}
	j.mu.Lock()
	bad = append(bad, j.notes...)
	events := slices.Clone(j.events)
	j.mu.Unlock()
	bad = append(bad, j.exitAnomalies()...)
	bad = append(bad, j.logAnomalies(ctx)...)
	bad = j.staleReads(hist, events, bad)

	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	for _, o := range hist {
		_ = enc.Encode(o)
	}
	jc.WriteArtifact("history.ndjson", b.Bytes())
	b.Reset()
	for _, d := range deliveries {
		_ = enc.Encode(d)
	}
	jc.WriteArtifact("deliveries.ndjson", b.Bytes())
	b.Reset()
	for _, e := range events {
		_ = enc.Encode(map[string]any{"fault": e.Fault, "heal": e.Heal, "held": e.Held, "target": e.Target, "start": e.Start, "end": e.End, "error": errString(e.Err)})
	}
	jc.WriteArtifact("nemesis.ndjson", b.Bytes())
	for stream, f := range finals {
		b.Reset()
		for _, seq := range slices.Sorted(maps.Keys(f.bySeq)) {
			_ = enc.Encode(f.bySeq[seq])
		}
		jc.WriteArtifact("final-"+stream+".ndjson", b.Bytes())
	}
	if out, err := json.MarshalIndent(map[string]any{
		"anomalies": bad, "unknown": unknown, "exits": j.cluster.Exits(), "settings": j.settings, "faults": j.faults,
		"restarted": j.restarted, "fetch_errors": cs.fetchErrs, "deliveries": len(deliveries), "replicas": j.replicas0,
	}, "", "  "); err == nil {
		jc.WriteArtifact("check.json", out)
	}

	counts := map[string]int{}
	outcomes := map[string]int{}
	for _, o := range hist {
		counts[o.Kind]++
		outcomes[string(o.Out)]++
	}
	var failing []string
	for _, a := range bad {
		if j.allow[a.Kind] {
			torx.Logf(ctx, "warn", "allowed: %s", a)
			continue
		}
		failing = append(failing, a.String())
	}
	if err := jc.Record(map[string]any{"ops": counts, "outcomes": outcomes, "anomalies": len(bad), "unknown": len(unknown), "faults": len(events)}); err != nil {
		torx.Logf(ctx, "warn", "recording the run's counts: %v", err)
	}
	summary := fmt.Sprintf("%d ops %v %v, %d fault events, %d anomalies (%d allowed), %d undecided",
		len(hist), counts, outcomes, len(events), len(bad), len(bad)-len(failing), len(unknown))
	if len(failing) > 0 {
		jc.SetSummary(fmt.Sprintf("%d anomalies; %s", len(failing), summary))
		if len(failing) > 20 {
			failing = append(failing[:20], fmt.Sprintf("... and %d more (check.json)", len(failing)-20))
		}
		return fmt.Errorf("%d anomalies:\n%s", len(failing), strings.Join(failing, "\n"))
	}
	jc.SetSummary(summary)
	return nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// exitAnomalies are the servers that exited without the nemesis stopping
// them.
func (j *chaosJob) exitAnomalies() []anomaly {
	var out []anomaly
	for _, e := range j.cluster.Exits() {
		kind := "exit"
		j.mu.Lock()
		for _, w := range j.diskFaults[e.Proc] {
			if !e.Time.Before(w.start) && (w.end.IsZero() || e.Time.Before(w.end.Add(5*time.Second))) {
				kind = "exit-under-" + w.fault
			}
		}
		j.mu.Unlock()
		detail := fmt.Sprintf("%s exited at %s with status %d", e.Proc, e.Time.Format(time.RFC3339Nano), e.Code)
		if e.Signal != "" {
			detail += " by " + e.Signal
		}
		out = append(out, anomaly{Kind: kind, Detail: detail + ":\n" + e.Said})
	}
	return out
}

// parseFeatures reads "flag=true,flag=false" into a map.
func parseFeatures(s string) (map[string]bool, error) {
	out := map[string]bool{}
	for kv := range strings.SplitSeq(s, ",") {
		if kv == "" {
			continue
		}
		k, v, ok := strings.Cut(kv, "=")
		b, err := strconv.ParseBool(v)
		if !ok || k == "" || err != nil {
			return nil, fmt.Errorf("%q is not flag=true or flag=false", kv)
		}
		out[k] = b
	}
	return out, nil
}
