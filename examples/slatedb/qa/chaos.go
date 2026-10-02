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
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/slatedb/qa/slatedb"
	"github.com/dotnwat/torx/listappend"
	"github.com/dotnwat/torx/nemesis"
	"github.com/dotnwat/torx/objstore"
)

// Parameters of slatedb.chaos.
const (
	paramDuration   = "duration"    // seconds of faults and load
	paramClients    = "clients"     // concurrent clients
	paramKeys       = "keys"        // keys the clients contend on
	paramFaults     = "faults"      // comma-separated fault names, or "all" and exclusions ("all,-clock")
	paramConfig     = "config"      // "random": settings drawn from the seed; "default": SlateDB's
	paramStandalone = "standalone"  // run the compactor and collector as processes of their own
	paramReader     = "reader"      // run a DbReader, read by the clients too
	paramValueSize  = "value-size"  // the largest value written, in bytes
	paramMaxSkew    = "max-skew"    // the clock faults' largest skew, in milliseconds
	paramIsolation  = "isolation"   // the transactions' isolation: "ssi", "si", or "none" for no transactions
	paramTxnKeys    = "txn-keys"    // keys the transactions contend on
	paramWorkers    = "workers"     // compaction workers of their own; 0 has the compactor run its jobs itself
	paramCollectors = "collectors"  // with standalone, the garbage collectors of their own that run at once
	paramEmbeddedGC = "embedded-gc" // with standalone, keep the writer's own collector as well
	paramAllow      = "allow"       // comma-separated known issues reported as warnings, so a run finds others (README.md)
	paramMerges     = "merges"      // install an append merge operator, and have clients append with it
	paramSegments   = "segments"    // split the keys into segments (RFC 0024) by their first n bytes; 0 for none
	paramClones     = "clones"      // have clients clone the database, and read the clones
	paramTrial      = "trial"       // distinguishes repeated variants; each draws its own seed

	defaultDuration  = 30
	defaultClients   = 8
	defaultKeys      = 16
	defaultValueSize = 512
	defaultTxnKeys   = 8
)

const (
	bucket = "slatedb"
	dbPath = "db"
	// nodes: the two writers' and one for the rest.
	nodes = 3
	// opTimeout bounds each client operation.
	opTimeout = 20 * time.Second
	// storeTimeout is the object-store client's request timeout in every
	// process.
	storeTimeout = 5 * time.Second
	// settleTimeout bounds how long, once every fault is healed, the
	// writer may take to serve again.
	settleTimeout = 180 * time.Second
	// checkBudget bounds the search of each key's history.
	checkBudget = time.Minute
	// defaultMaxSkew bounds the clock faults: below every GC min_age the
	// job configures, as SlateDB requires.
	defaultMaxSkew = 5000
)

// faultWeights are the faults the nemesis draws from, by name, with their
// weights. Faults of a role the run does not have are left out.
var faultWeights = map[string]int{
	"crash-writer":    4, // kill the writer, restart it
	"restart-writer":  2, // close the writer, restart it
	"failover":        4, // pause the writer, open another, resume the first: a zombie
	"failover-live":   3, // open another writer while the first runs
	"crash-failover":  3, // kill the writer, open the other
	"store-errors":    4, // object-store requests fail, before or after taking effect
	"store-slow":      3, // object-store requests are delayed, or land late
	"store-partition": 2, // a process's object-store requests hang
	"crash-aux":       3, // kill the compactor, collector, or reader, restart it
	"pause-aux":       2, // pause the compactor, collector, or reader
	"clock-skew":      2, // restart a process with its clock skewed, within max-skew
	"clock-jump":      2, // step a running process's clock, within max-skew
}

// knownIssues are the issues of SlateDB a run can allow, so that a run
// that keeps finding one can find others. README.md describes each.
var knownIssues = map[string]bool{
	// A standalone compaction worker does not retry object-store errors as
	// every other process does, and its loop ends on one (#49).
	"worker-store-error": true,
}

// offByDefault are faults "all" leaves out; a run names them to inject
// them. Clock skew between processes kills a compaction worker at 20ms
// (#48), and a writer whose clock is behind fails for good (#47); left in,
// they would hide everything else.
var offByDefault = map[string]bool{"clock-skew": true, "clock-jump": true}

// chaosJob is slatedb.chaos: a randomized test of SlateDB's guarantees under
// faults. Clients put, delete, batch, read, and scan a few keys through the
// writer -- and a reader, with reader -- while a nemesis crashes, pauses,
// and fails over the writer, faults the object store under each process,
// crashes and pauses the compactor, collector, and reader, and skews their
// clocks. Every value written is unique, and its bytes derive from its
// name, so every read is checked byte for byte; every read of the writer
// reads durable data only, and every write but a few awaits durability, so
// each key's history must be linearizable, every scan must be a snapshot,
// and a write the writer acknowledged as durable must never be lost. Once
// every fault is healed, the writer reads every key, and a fresh reader
// must read the same.
type chaosJob struct {
	torx.JobBase
	db         *slatedb.Service
	duration   time.Duration
	clients    int
	keys       []string
	faults     []string
	random     bool
	standalone bool
	reader     bool
	valueSize  int
	maxSkew    time.Duration
	isolation  string // "" for no transactions
	txnKeys    int
	workers    int
	merges     bool
	clones     bool
	segments   int
	allow      map[string]bool
	collectors int
	embeddedGC bool
	worker     map[string]any // the workers' options
	reader0    map[string]any // the readers' options

	jc       *torx.JobContext
	store    *objstore.Server
	settings map[string]any
	gc       map[string]any
	compact  map[string]any

	mu         sync.Mutex
	current    string         // the writer the clients use
	launches   map[string]int // process -> launches so far, as the job counts them
	hist       []op
	txnLog     []txnRecord
	mergeLog   []txnRecord // merges and reads of merged keys, each a transaction of one operation
	cloneNames []string    // the clones made
	events     []nemesis.Event
	notes      []anomaly // anomalies seen outside the history
}

func (*chaosJob) Matrix() []torx.Params {
	return []torx.Params{{paramDuration: 20}}
}

func parseFaults(spec string) []string {
	parts := strings.Split(spec, ",")
	if parts[0] != "all" {
		return parts
	}
	faults := slices.DeleteFunc(slices.Sorted(maps.Keys(faultWeights)), func(f string) bool { return offByDefault[f] })
	for _, p := range parts[1:] {
		faults = slices.DeleteFunc(faults, func(f string) bool { return "-"+f == p })
	}
	return faults
}

func (*chaosJob) ResolveParams(p torx.Params) (torx.Params, error) {
	out := torx.Params{
		paramDuration: defaultDuration, paramClients: defaultClients, paramKeys: defaultKeys,
		paramFaults: "all", paramConfig: "random", paramStandalone: false, paramReader: true,
		paramValueSize: defaultValueSize, paramMaxSkew: defaultMaxSkew,
		paramIsolation: "ssi", paramTxnKeys: defaultTxnKeys, paramWorkers: 0,
		paramCollectors: 1, paramEmbeddedGC: false, paramAllow: "worker-store-error",
		paramMerges: true, paramSegments: 0, paramClones: true,
	}
	for k, v := range p {
		switch k {
		case paramDuration, paramClients, paramKeys, paramValueSize, paramTrial, paramMaxSkew, paramTxnKeys, paramWorkers, paramCollectors, paramSegments:
			n, ok := intValue(v)
			bounds := map[string][2]int{
				paramDuration: {1, 3600}, paramClients: {1, 64}, paramKeys: {1, 1000},
				paramValueSize: {16, 1 << 20}, paramTrial: {0, math.MaxInt32}, paramMaxSkew: {0, 3_600_000},
				paramTxnKeys: {1, 100}, paramWorkers: {0, 4}, paramCollectors: {1, 4}, paramSegments: {0, 3},
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
			str, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a string, got %v", k, v)
			}
			for a := range strings.SplitSeq(str, ",") {
				if a != "" && !knownIssues[a] {
					return nil, fmt.Errorf("unknown issue %q to allow", a)
				}
			}
			out[k] = str
		case paramIsolation:
			if v != "ssi" && v != "si" && v != "none" {
				return nil, fmt.Errorf("%s must be ssi, si, or none, got %v", k, v)
			}
			out[k] = v
		case paramConfig:
			if v != "random" && v != "default" {
				return nil, fmt.Errorf("%s must be random or default, got %v", k, v)
			}
			out[k] = v
		case paramStandalone, paramReader, paramEmbeddedGC, paramMerges, paramClones:
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("%s must be a boolean, got %v", k, v)
			}
			out[k] = b
		default:
			return nil, fmt.Errorf("unknown parameter %q", k)
		}
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
	j.db = slatedb.New(serviceName, nodes)
	jc.Register(j.db)
	j.duration = time.Duration(jc.Params.Int(paramDuration, defaultDuration)) * time.Second
	j.clients = jc.Params.Int(paramClients, defaultClients)
	j.keys = nil
	for i := range jc.Params.Int(paramKeys, defaultKeys) {
		j.keys = append(j.keys, fmt.Sprintf("k%03d", i))
	}
	j.faults = parseFaults(jc.Params.String(paramFaults, "all"))
	j.random = jc.Params.String(paramConfig, "random") == "random"
	j.standalone = jc.Params.Bool(paramStandalone, false)
	j.reader = jc.Params.Bool(paramReader, true)
	j.valueSize = jc.Params.Int(paramValueSize, defaultValueSize)
	j.maxSkew = time.Duration(jc.Params.Int(paramMaxSkew, defaultMaxSkew)) * time.Millisecond
	j.isolation = jc.Params.String(paramIsolation, "ssi")
	if j.isolation == "none" {
		j.isolation = ""
	}
	j.txnKeys = jc.Params.Int(paramTxnKeys, defaultTxnKeys)
	j.workers = jc.Params.Int(paramWorkers, 0)
	j.collectors = jc.Params.Int(paramCollectors, 1)
	j.merges = jc.Params.Bool(paramMerges, true)
	j.clones = jc.Params.Bool(paramClones, true)
	j.segments = jc.Params.Int(paramSegments, 0)
	j.allow = map[string]bool{}
	for a := range strings.SplitSeq(jc.Params.String(paramAllow, "worker-store-error"), ",") {
		j.allow[a] = a != ""
	}
	j.embeddedGC = jc.Params.Bool(paramEmbeddedGC, false)
}

// The processes' names.
const (
	writerA   = "w0"
	writerB   = "w1"
	readerP   = "r0"
	compactor = "c0"
)

func (j *chaosJob) Setup(ctx context.Context, jc *torx.JobContext) error {
	j.jc = jc
	if err := j.JobBase.Setup(ctx, jc); err != nil {
		return err
	}
	ns := j.db.Nodes()
	addr, err := ns[0].CallerAddr()
	if err != nil {
		return err
	}
	j.store = objstore.New(objstore.Options{Rand: jc.Rand("store")})
	j.store.CreateBucket(bucket)
	endpoint, err := j.store.Serve(net.JoinHostPort(addr, "0"))
	if err != nil {
		return err
	}
	jc.Defer(func(context.Context) error { return j.store.Close() })
	j.db.SetStore(slatedb.Store{Endpoint: endpoint, Bucket: bucket, Timeout: storeTimeout})
	jc.Log("info", "object store at "+endpoint)

	j.drawSettings(jc.Rand("config"))
	b, _ := json.MarshalIndent(map[string]any{"writer": j.settings, "gc": j.gc, "compactor": j.compact, "worker": j.worker, "reader": j.reader0}, "", "  ")
	jc.WriteArtifact("settings.json", b)
	jc.Log("info", "settings: "+string(b))

	j.launches = map[string]int{}
	j.current = writerA
	if err := j.launch(ctx, writerA, ns[0], slatedb.Spec{Role: slatedb.Writer, DB: dbPath, Settings: j.settings}); err != nil {
		return err
	}
	if err := j.db.WaitReady(ctx, writerA, 0); err != nil {
		return err
	}
	if j.standalone {
		if err := j.launch(ctx, compactor, ns[2], slatedb.Spec{Role: slatedb.Compactor, DB: dbPath, Options: j.compact}); err != nil {
			return err
		}
		for i := range j.collectors {
			if err := j.launch(ctx, collectorName(i), ns[2], slatedb.Spec{Role: slatedb.GC, DB: dbPath, Options: j.gc}); err != nil {
				return err
			}
		}
	}
	if j.reader {
		if err := j.launch(ctx, readerP, ns[2], slatedb.Spec{Role: slatedb.Reader, DB: dbPath, Options: j.readerOptions()}); err != nil {
			return err
		}
	}
	for i := range j.workers {
		if err := j.launch(ctx, workerName(i), ns[2], slatedb.Spec{Role: slatedb.Worker, DB: dbPath, Options: j.worker}); err != nil {
			return err
		}
	}
	for _, p := range j.db.Procs("") {
		if err := j.db.WaitReady(ctx, p, 0); err != nil {
			return err
		}
	}
	return nil
}

func (j *chaosJob) readerOptions() map[string]any {
	return j.reader0
}

func workerName(i int) string    { return fmt.Sprintf("x%d", i) }
func collectorName(i int) string { return fmt.Sprintf("g%d", i) }

// launch launches a process and counts the launch.
func (j *chaosJob) launch(ctx context.Context, name string, n *torx.Node, spec slatedb.Spec) error {
	spec.MergeAppend = j.merges
	spec.SegmentPrefixLen = j.segments
	j.mu.Lock()
	j.launches[name]++
	j.mu.Unlock()
	return j.db.Launch(ctx, name, n, spec)
}

func (j *chaosJob) restart(ctx context.Context, name string) error {
	j.mu.Lock()
	j.launches[name]++
	j.mu.Unlock()
	return j.db.Restart(ctx, name)
}

func (j *chaosJob) launchOf(name string) int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.launches[name]
}

func pick[T any](rng *rand.Rand, xs ...T) T { return xs[rng.IntN(len(xs))] }

// drawSettings draws the writer's settings, and the compactor's and
// collector's options, from rng: small, frequent flushes and compactions,
// so a short run exercises every path; or SlateDB's defaults.
func (j *chaosJob) drawSettings(rng *rand.Rand) {
	j.settings = map[string]any{}
	j.gc = map[string]any{}
	j.compact = map[string]any{}
	if j.random {
		maxSSTs := pick(rng, 4, 8, 16)
		j.settings = map[string]any{
			"flush_interval":         pick(rng, "10ms", "50ms", "100ms", "250ms"),
			"manifest_poll_interval": pick(rng, "100ms", "500ms", "1s"),
			"l0_sst_size_bytes":      pick(rng, 4<<10, 16<<10, 64<<10, 1<<20),
			"l0_max_ssts":            maxSSTs,
			"l0_max_ssts_per_key":    maxSSTs,
			"l0_flush_parallelism":   pick(rng, 1, 2, 4),
			"min_filter_keys":        pick(rng, 1, 1000),
		}
		j.compact = map[string]any{
			"poll_interval":              pick(rng, "100ms", "500ms", "2s"),
			"max_concurrent_compactions": pick(rng, 1, 2, 4),
			"commit_compacted_interval":  pick(rng, "100ms", "1s"),
			"worker": map[string]any{
				"compactions_poll_interval":  pick(rng, "100ms", "1s"),
				"heartbeat_interval":         "1s",
				"max_concurrent_compactions": pick(rng, 1, 4),
			},
			"worker_heartbeat_timeout": "10s",
		}
		// A short checkpoint lifetime has a paused reader's checkpoint
		// expire, and the collector delete what it pinned.
		j.reader0 = pick(rng,
			map[string]any{"manifest_poll_interval": "100ms", "checkpoint_lifetime": "2s"},
			map[string]any{"manifest_poll_interval": "200ms", "checkpoint_lifetime": "5s"},
			map[string]any{"manifest_poll_interval": "1s", "checkpoint_lifetime": "60s"},
		)
		j.worker = map[string]any{
			"compactions_poll_interval":  pick(rng, "100ms", "1s"),
			"heartbeat_interval":         "1s",
			"max_concurrent_compactions": pick(rng, 1, 4),
		}
		dir := func() map[string]any {
			// min_age stays above the clock faults' skew: SlateDB assumes
			// the skew between a collector's clock and the writers' is
			// below it.
			return map[string]any{"interval": pick(rng, "1s", "3s"), "min_age": pick(rng, "10s", "20s", "30s")}
		}
		j.gc = map[string]any{
			"manifest_options":    dir(),
			"wal_options":         dir(),
			"compacted_options":   dir(),
			"compactions_options": dir(),
			// WAL fences are deleted only in dry runs: SlateDB requires a
			// min_age "longer than any writer can run" to delete them, or a
			// writer paused past it comes back unfenced.
			"wal_fence_options": map[string]any{"interval": "3s", "min_age": "10s", "dry_run": true},
			"detach_options":    map[string]any{"interval": pick(rng, "1s", "10s")},
		}
	}
	if j.workers > 0 {
		// The compactor runs no jobs itself; the workers claim them.
		j.compact["worker"] = nil
	}
	if j.standalone {
		j.settings["compactor_options"] = nil
		j.settings["garbage_collector_options"] = nil
		if j.embeddedGC && len(j.gc) > 0 {
			j.settings["garbage_collector_options"] = j.gc
		} else if j.embeddedGC {
			delete(j.settings, "garbage_collector_options")
		}
	} else {
		if len(j.compact) > 0 {
			j.settings["compactor_options"] = j.compact
		}
		if len(j.gc) > 0 {
			j.settings["garbage_collector_options"] = j.gc
		}
	}
}

func (j *chaosJob) Run(ctx context.Context, jc *torx.JobContext) error {
	runCtx, stop := context.WithTimeout(ctx, j.duration)
	defer stop()
	var wg sync.WaitGroup
	for c := range j.clients {
		wg.Go(func() { j.client(ctx, runCtx, c, jc.Rand(fmt.Sprintf("client-%d", c))) })
	}
	var faults []nemesis.Fault
	for _, f := range j.faultList() {
		if slices.Contains(j.faults, f.Name) {
			faults = append(faults, f)
		}
	}
	nem := &nemesis.Nemesis{
		Faults:   faults,
		Schedule: nemesis.DefaultSchedule,
		Rand:     jc.Rand("nemesis"),
		Record: func(ev nemesis.Event) {
			j.mu.Lock()
			j.events = append(j.events, ev)
			j.mu.Unlock()
			msg := fmt.Sprintf("nemesis: %s", ev.Fault)
			if ev.Heal {
				msg += " healed"
			}
			if ev.Target != "" {
				msg += " " + ev.Target
			}
			if ev.Err != nil {
				msg += ": " + ev.Err.Error()
			}
			jc.Log("info", msg)
		},
	}
	nem.Run(runCtx)
	wg.Wait()

	// Heal what the nemesis may have left, and bring the writer back.
	j.store.ClearRules()
	settleCtx, cancel := context.WithTimeout(ctx, settleTimeout)
	defer cancel()
	settleErr := j.settle(settleCtx, jc)
	var final map[string]read
	if settleErr == nil {
		final, settleErr = j.finalReads(settleCtx, jc)
	}
	if settleErr == nil {
		settleErr = j.finalTxn(settleCtx)
	}
	if settleErr == nil {
		settleErr = j.finalMerges(settleCtx)
	}
	if settleErr == nil {
		j.finalClones(settleCtx)
	}
	if settleErr != nil {
		j.note(anomaly{Kind: "unavailable", Detail: "after every fault was healed: " + settleErr.Error()})
	}
	if settleErr == nil && j.reader {
		if err := j.finalReader(settleCtx, jc, final); err != nil {
			j.note(anomaly{Kind: "unavailable", Detail: "a fresh reader: " + err.Error()})
		}
	}
	for _, e := range j.db.Exits() {
		j.note(anomaly{Kind: "exit", Detail: fmt.Sprintf("%s (%s on %s) exited with status %d: %s\n%s", e.Proc, e.Role, e.Node, e.Code, e.Err, e.Said)})
	}
	j.checkAux(ctx)
	return j.report(ctx, jc)
}

// finalClones scans every clone once more: the source has compacted and
// collected since, and a clone that lost a file it shares with the source
// cannot be read.
func (j *chaosJob) finalClones(ctx context.Context) {
	j.mu.Lock()
	names := slices.Clone(j.cloneNames)
	j.mu.Unlock()
	w := j.writer()
	c := slatedb.NewClient(j.db.URL(w))
	defer c.CloseIdle()
	for _, name := range names {
		octx, cancel := context.WithTimeout(ctx, opTimeout)
		if err := j.scanClone(octx, c, w, finalClient, name); err != nil {
			j.note(anomaly{Kind: "clone-unreadable", Detail: fmt.Sprintf("%s: %v", name, err)})
		}
		cancel()
	}
}

// note records an anomaly seen outside the history.
func (j *chaosJob) note(a anomaly) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.notes = append(j.notes, a)
}

// checkAux asks the compactor and collector whether their loops ended.
func (j *chaosJob) checkAux(ctx context.Context) {
	for _, p := range append([]string{compactor}, j.db.Procs(slatedb.GC)...) {
		if !j.db.Running(p) {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		st, err := slatedb.NewClient(j.db.URL(p)).Status(cctx)
		cancel()
		if err == nil && st.Ended != nil {
			j.note(anomaly{Kind: "aux-ended", Detail: fmt.Sprintf("%s's loop %s", p, *st.Ended)})
		}
	}
}

// settle resumes what is paused, restarts what is down, and waits for the
// writer to serve.
func (j *chaosJob) settle(ctx context.Context, jc *torx.JobContext) error {
	for _, p := range j.db.Procs("") {
		if j.db.Paused(p) {
			_ = j.db.Resume(ctx, p)
		}
		if j.db.Running(p) {
			_ = j.db.SetClock(ctx, p, 0)
		}
	}
	w := j.writer()
	for _, p := range j.db.Procs(slatedb.Writer) {
		if p != w && j.db.Running(p) {
			jc.Log("info", "settle: stopping writer "+p)
			_, _ = j.db.Terminate(ctx, p)
		}
	}
	for _, p := range j.db.Procs("") {
		if p == w || j.db.LaunchSpec(p).Role != slatedb.Writer {
			if !j.db.Running(p) {
				jc.Log("info", "settle: restarting "+p)
				if err := j.restart(ctx, p); err != nil {
					return err
				}
			}
		}
	}
	// A compactor, worker, or collector whose loop ended serves on, saying
	// so; note it, and restart it.
	for _, p := range j.db.Procs("") {
		role := j.db.LaunchSpec(p).Role
		if !j.db.Running(p) || role == slatedb.Writer || role == slatedb.Reader {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		st, err := slatedb.NewClient(j.db.URL(p)).Status(cctx)
		cancel()
		if err != nil || st.Ended == nil {
			continue
		}
		if role == slatedb.Worker && j.allow["worker-store-error"] && strings.Contains(*st.Ended, "object store error") {
			jc.Log("warn", fmt.Sprintf("known issue worker-store-error: %s's loop %s", p, *st.Ended))
		} else {
			j.note(anomaly{Kind: "aux-ended", Detail: fmt.Sprintf("%s's loop %s", p, *st.Ended)})
		}
		jc.Log("info", "settle: restarting "+p+", whose loop ended")
		if err := j.db.Crash(ctx, p); err != nil {
			return err
		}
		if err := j.restart(ctx, p); err != nil {
			return err
		}
	}
	for _, p := range j.db.Procs("") {
		if !j.db.Running(p) {
			continue
		}
		if err := j.db.WaitReady(ctx, p, 0); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
	}
	return nil
}

// finalReads reads every key from the writer, retrying failures, and
// records the reads in the history.
func (j *chaosJob) finalReads(ctx context.Context, jc *torx.JobContext) (map[string]read, error) {
	w := j.writer()
	c := slatedb.NewClient(j.db.URL(w))
	defer c.CloseIdle()
	out := map[string]read{}
	for _, k := range j.keys {
		for {
			o := j.begin(-1, w, opGet)
			o.Key = k
			octx, cancel := context.WithTimeout(ctx, opTimeout)
			v, err := c.Get(octx, k, slatedb.Remote)
			cancel()
			o.Return = time.Now()
			if err == nil {
				r := read{Found: v.Found, Value: v.Value, Seq: v.Seq}
				o.Read = map[string]read{k: r}
				o.Outcome = "ok"
				j.end(o)
				out[k] = r
				break
			}
			o.Outcome, o.Err = "fail", err.Error()
			j.end(o)
			if ctx.Err() != nil {
				return nil, fmt.Errorf("reading %s: %w", k, err)
			}
			time.Sleep(time.Second)
		}
	}
	return out, nil
}

// finalReader flushes the writer, opens a fresh reader, and reads every key
// from it: it must read what the writer read.
func (j *chaosJob) finalReader(ctx context.Context, jc *torx.JobContext, final map[string]read) error {
	w := j.writer()
	if err := slatedb.NewClient(j.db.URL(w)).Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	const name = "rf"
	if err := j.launch(ctx, name, j.db.Nodes()[2], slatedb.Spec{Role: slatedb.Reader, DB: dbPath, Options: j.readerOptions()}); err != nil {
		return err
	}
	if err := j.db.WaitReady(ctx, name, 0); err != nil {
		return err
	}
	c := slatedb.NewClient(j.db.URL(name))
	defer c.CloseIdle()
	for _, k := range j.keys {
		o := j.begin(-2, name, opGet)
		o.Key = k
		v, err := c.Get(ctx, k, slatedb.Remote)
		o.Return = time.Now()
		if err != nil {
			o.Outcome, o.Err = "fail", err.Error()
			j.end(o)
			return fmt.Errorf("reading %s: %w", k, err)
		}
		r := read{Found: v.Found, Value: v.Value, Seq: v.Seq}
		o.Read = map[string]read{k: r}
		o.Outcome = "ok"
		o.Reader = true
		j.end(o)
		if want := final[k]; r.Found != want.Found || r.Value != want.Value {
			j.note(anomaly{Kind: "reader-diverges", Key: k, Ops: []int{o.ID},
				Detail: fmt.Sprintf("a fresh reader read %s as %s; the writer, as %s", k, describe(r), describe(want))})
		}
	}
	return nil
}

func describe(r read) string {
	if !r.Found {
		return "absent"
	}
	return fmt.Sprintf("%q (seq %d)", short(r.Value), r.Seq)
}

func short(v string) string {
	name, _, _ := strings.Cut(v, "|")
	return name
}

// writer is the writer the clients use.
func (j *chaosJob) writer() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.current
}

func (j *chaosJob) setWriter(w string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.current = w
}

// other is the writer slot that is not w.
func other(w string) string {
	if w == writerA {
		return writerB
	}
	return writerA
}

// report checks the history, writes it and the anomalies, and fails the
// job if there are any.
func (j *chaosJob) report(ctx context.Context, jc *torx.JobContext) error {
	j.mu.Lock()
	hist := slices.Clone(j.hist)
	notes := slices.Clone(j.notes)
	events := slices.Clone(j.events)
	j.mu.Unlock()

	var lines []byte
	for _, o := range hist {
		b, _ := json.Marshal(o)
		lines = append(append(lines, b...), '\n')
	}
	jc.WriteArtifact("history.jsonl", lines)
	type ev struct {
		Fault  string    `json:"fault"`
		Heal   bool      `json:"heal,omitempty"`
		Target string    `json:"target,omitempty"`
		Start  time.Time `json:"start"`
		End    time.Time `json:"end"`
		Err    string    `json:"error,omitempty"`
	}
	var evs []ev
	for _, e := range events {
		x := ev{Fault: e.Fault, Heal: e.Heal, Target: e.Target, Start: e.Start, End: e.End}
		if e.Err != nil {
			x.Err = e.Err.Error()
		}
		evs = append(evs, x)
	}
	b, _ := json.MarshalIndent(evs, "", "  ")
	jc.WriteArtifact("faults.json", b)
	var sb strings.Builder
	_ = j.store.WriteHistory(&sb)
	jc.WriteArtifact("store-history.jsonl", []byte(sb.String()))

	c := newChecker(hist)
	anomalies, unknown := c.check(ctx)
	anomalies = append(notes, anomalies...)
	txnBad, txnAllowed := j.checkTxns()
	anomalies = append(anomalies, txnBad...)
	anomalies = append(anomalies, j.checkMerges()...)
	for _, a := range txnAllowed {
		jc.Log("info", fmt.Sprintf("allowed at %s: %s %s", j.isolation, a.Kind, a.Detail))
	}
	j.mu.Lock()
	txnLog := slices.Clone(j.txnLog)
	j.mu.Unlock()
	if ml := slices.Clone(j.mergeLog); len(ml) > 0 {
		var b []byte
		for _, t := range ml {
			x, _ := json.Marshal(t)
			b = append(append(b, x...), '\n')
		}
		jc.WriteArtifact("merges.jsonl", b)
	}
	if len(txnLog) > 0 {
		var tl []byte
		for _, t := range txnLog {
			b, _ := json.Marshal(t)
			tl = append(append(tl, b...), '\n')
		}
		jc.WriteArtifact("txns.jsonl", tl)
	}
	counts := map[string]int{}
	for _, o := range hist {
		counts[string(o.Kind)+":"+o.Outcome]++
	}
	objects, size := j.store.Usage()
	tcounts := map[listappend.Status]int{}
	for _, t := range txnLog {
		tcounts[t.Status]++
	}
	summary := fmt.Sprintf("%d ops (%v), %d txns (%d committed, %d aborted, %d unknown), %d faults, %d anomalies, %d keys unchecked; store holds %d objects, %d bytes",
		len(hist), counts, len(txnLog), tcounts[listappend.Committed], tcounts[listappend.Aborted], tcounts[listappend.Unknown],
		len(events), len(anomalies), len(unknown), objects, size)
	jc.SetSummary(summary)
	jc.Log("info", summary)
	for _, u := range unknown {
		jc.Log("warn", "unchecked: "+u)
	}
	if len(anomalies) == 0 {
		return nil
	}
	b, _ = json.MarshalIndent(anomalies, "", "  ")
	jc.WriteArtifact("anomalies.json", b)
	var tarball bytes.Buffer
	if err := j.store.WriteTar(&tarball); err != nil {
		jc.Log("warn", "archiving the bucket: "+err.Error())
	} else {
		jc.WriteArtifact("bucket.tar", tarball.Bytes())
	}
	var kinds []string
	for _, a := range anomalies {
		kinds = append(kinds, a.Kind)
		jc.Log("error", fmt.Sprintf("anomaly %s %s: %s", a.Kind, a.Key, a.Detail))
	}
	slices.Sort(kinds)
	return fmt.Errorf("%d anomalies: %s", len(anomalies), strings.Join(slices.Compact(kinds), ", "))
}

// faultList is every fault, by name; the run keeps those its faults
// parameter names.
func (j *chaosJob) faultList() []nemesis.Fault {
	f := func(name string, inject func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error)) nemesis.Fault {
		return nemesis.Fault{Name: name, Weight: faultWeights[name], Inject: inject}
	}
	return []nemesis.Fault{
		f("crash-writer", j.crashWriter),
		f("restart-writer", j.restartWriter),
		f("failover", func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.failover(ctx, rng, true)
		}),
		f("failover-live", func(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
			return j.failover(ctx, rng, false)
		}),
		f("crash-failover", j.crashFailover),
		f("store-errors", j.storeErrors),
		f("store-slow", j.storeSlow),
		f("store-partition", j.storePartition),
		f("crash-aux", j.crashAux),
		f("pause-aux", j.pauseAux),
		f("clock-skew", j.clockSkew),
		f("clock-jump", j.clockJump),
	}
}

func (j *chaosJob) crashWriter(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	w := j.writer()
	if !j.db.Running(w) {
		return nil, "", nemesis.ErrNoTarget
	}
	if err := j.db.Crash(ctx, w); err != nil {
		return nil, w, err
	}
	return func(ctx context.Context) error {
		if err := j.restart(ctx, w); err != nil {
			return err
		}
		return j.db.WaitReady(ctx, w, 0)
	}, w, nil
}

func (j *chaosJob) restartWriter(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	w := j.writer()
	if !j.db.Running(w) {
		return nil, "", nemesis.ErrNoTarget
	}
	clean, err := j.db.Terminate(ctx, w)
	if err != nil {
		return nil, w, err
	}
	if !clean {
		// Not an anomaly by itself: a writer whose memtables have fallen
		// far behind its WAL flushes them all as it closes.
		j.jc.Log("warn", fmt.Sprintf("writer %s did not close within the grace after SIGTERM", w))
	}
	if err := j.restart(ctx, w); err != nil {
		return nil, w, err
	}
	return nil, w, j.db.WaitReady(ctx, w, 0)
}

// openWriter launches, or relaunches, writer slot w and waits for it to
// open the database.
func (j *chaosJob) openWriter(ctx context.Context, w string) error {
	var err error
	if j.db.Node(w) == nil {
		ns := j.db.Nodes()
		n := ns[0]
		if w == writerB {
			n = ns[1]
		}
		err = j.launch(ctx, w, n, slatedb.Spec{Role: slatedb.Writer, DB: dbPath, Settings: j.settings})
	} else {
		err = j.restart(ctx, w)
	}
	if err != nil {
		return err
	}
	return j.db.WaitReady(ctx, w, 0)
}

// failover opens the other writer while the current one is paused (or,
// live, running), and moves the clients to it. Its heal resumes the old
// writer, which another has fenced, sends it a few writes -- each must
// fail, or be durable in the new writer -- and stops it.
func (j *chaosJob) failover(ctx context.Context, rng *rand.Rand, pause bool) (nemesis.Heal, string, error) {
	old := j.writer()
	next := other(old)
	if !j.db.Running(old) || j.db.Running(next) {
		return nil, "", nemesis.ErrNoTarget
	}
	if pause {
		if err := j.db.Pause(ctx, old); err != nil {
			return nil, old, err
		}
	}
	err := j.openWriter(ctx, next)
	target := old + "->" + next
	if err != nil {
		if pause {
			_ = j.db.Resume(ctx, old)
		}
		return nil, target, err
	}
	j.setWriter(next)
	return func(ctx context.Context) error {
		if pause {
			if err := j.db.Resume(ctx, old); err != nil {
				return err
			}
		}
		j.zombieWrites(ctx, old, rng)
		_, err := j.db.Terminate(ctx, old)
		return err
	}, target, nil
}

// zombieWrites sends writes to a writer another has fenced.
func (j *chaosJob) zombieWrites(ctx context.Context, w string, rng *rand.Rand) {
	c := slatedb.NewClient(j.db.URL(w))
	defer c.CloseIdle()
	for i := range 3 {
		k := pick(rng, j.keys...)
		v := j.value(fmt.Sprintf("z%s.%d.%d", w, j.launchOf(w), i), rng)
		o := j.begin(-3, w, opPut)
		o.Writes = []write{{Key: k, Value: &v}}
		o.Durable = true
		o.Zombie = true
		octx, cancel := context.WithTimeout(ctx, opTimeout)
		seq, err := c.Put(octx, k, &v, true)
		cancel()
		j.finishWrite(o, seq, err)
	}
}

func (j *chaosJob) crashFailover(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	old := j.writer()
	next := other(old)
	if !j.db.Running(old) || j.db.Running(next) {
		return nil, "", nemesis.ErrNoTarget
	}
	if err := j.db.Crash(ctx, old); err != nil {
		return nil, old, err
	}
	target := old + "->" + next
	if err := j.openWriter(ctx, next); err != nil {
		return nil, target, err
	}
	j.setWriter(next)
	return nil, target, nil
}

// storeClients returns the running processes that talk to the object
// store, but for the final reader.
func (j *chaosJob) storeClients() []string {
	var out []string
	for _, p := range j.db.Procs("") {
		if j.db.Running(p) && p != "rf" {
			out = append(out, p)
		}
	}
	return out
}

var storePrefixes = []string{"", dbPath + "/manifest/", dbPath + "/wal/", dbPath + "/compacted/", dbPath + "/compactions/"}

func (j *chaosJob) addRule(rng *rand.Rand, r objstore.Rule) (nemesis.Heal, string) {
	clients := j.storeClients()
	if len(clients) > 0 && rng.IntN(4) > 0 {
		r.Clients = []string{pick(rng, clients...)}
	}
	if r.Prefix == "" {
		r.Prefix = pick(rng, storePrefixes...)
	}
	if rng.IntN(2) == 0 {
		r.Ops = pick(rng,
			[]objstore.Op{objstore.OpPut, objstore.OpComplete},
			[]objstore.Op{objstore.OpGet, objstore.OpHead},
			[]objstore.Op{objstore.OpList},
			[]objstore.Op{objstore.OpDelete, objstore.OpDeletes},
		)
	}
	id := j.store.AddRule(r)
	target := fmt.Sprintf("%s %s p=%.2f clients=%v ops=%v prefix=%q", r.Action, r.Delay, r.Prob, r.Clients, r.Ops, r.Prefix)
	return func(context.Context) error { j.store.RemoveRule(id); return nil }, target
}

func (j *chaosJob) storeErrors(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	r := objstore.Rule{
		Name:   "errors",
		Prob:   pick(rng, 0.05, 0.2, 0.5, 1.0),
		Action: pick(rng, objstore.Fail, objstore.FailAfter, objstore.Drop, objstore.DropAfter),
		Status: pick(rng, 500, 503),
	}
	heal, target := j.addRule(rng, r)
	return heal, target, nil
}

func (j *chaosJob) storeSlow(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	r := objstore.Rule{
		Name:   "slow",
		Prob:   pick(rng, 0.1, 0.5, 1.0),
		Action: pick(rng, objstore.Delay, objstore.Stall, objstore.DelayAfter),
		Delay:  pick(rng, 200*time.Millisecond, time.Second, 3*time.Second, 8*time.Second),
	}
	heal, target := j.addRule(rng, r)
	return heal, target, nil
}

func (j *chaosJob) storePartition(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	clients := j.storeClients()
	if len(clients) == 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	c := pick(rng, clients...)
	id := j.store.AddRule(objstore.Rule{Name: "partition", Clients: []string{c}, Prob: 1, Action: objstore.Hang})
	return func(context.Context) error { j.store.RemoveRule(id); return nil }, c, nil
}

// aux returns the running compactor, collector, and reader processes.
func (j *chaosJob) aux() []string {
	var out []string
	for _, p := range append(append([]string{compactor, readerP}, j.db.Procs(slatedb.Worker)...), j.db.Procs(slatedb.GC)...) {
		if j.db.Running(p) && !j.db.Paused(p) {
			out = append(out, p)
		}
	}
	return out
}

func (j *chaosJob) crashAux(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	procs := j.aux()
	if len(procs) == 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	p := pick(rng, procs...)
	if err := j.db.Crash(ctx, p); err != nil {
		return nil, p, err
	}
	return func(ctx context.Context) error {
		if err := j.restart(ctx, p); err != nil {
			return err
		}
		return j.db.WaitReady(ctx, p, 0)
	}, p, nil
}

func (j *chaosJob) pauseAux(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	procs := j.aux()
	if len(procs) == 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	p := pick(rng, procs...)
	if err := j.db.Pause(ctx, p); err != nil {
		return nil, p, err
	}
	return func(ctx context.Context) error { return j.db.Resume(ctx, p) }, p, nil
}

// skewOf draws a clock skew within max-skew.
func (j *chaosJob) skewOf(rng *rand.Rand) time.Duration {
	return time.Duration(rng.Int64N(int64(2*j.maxSkew))) - j.maxSkew
}

// clockSkew restarts a process with its clock skewed: the process as if on
// a host whose clock is off. SlateDB's own clock is the wall clock when
// the process started plus the time elapsed since, so a host's clock that
// is off shows only across processes, and across restarts. The heal
// restarts it with the clock right.
func (j *chaosJob) clockSkew(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	var procs []string
	for _, p := range j.db.Procs("") {
		if j.db.Running(p) && !j.db.Paused(p) && p != "rf" {
			procs = append(procs, p)
		}
	}
	if len(procs) == 0 || j.maxSkew <= 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	p := pick(rng, procs...)
	off := j.skewOf(rng)
	relaunch := func(ctx context.Context, off time.Duration) error {
		if j.db.Running(p) {
			if err := j.db.Crash(ctx, p); err != nil {
				return err
			}
		}
		spec := j.db.LaunchSpec(p)
		spec.ClockOffset = off
		j.mu.Lock()
		j.launches[p]++
		j.mu.Unlock()
		if err := j.db.Relaunch(ctx, p, spec); err != nil {
			return err
		}
		return j.db.WaitReady(ctx, p, 0)
	}
	if err := relaunch(ctx, off); err != nil {
		return nil, p, err
	}
	return func(ctx context.Context) error { return relaunch(ctx, 0) }, fmt.Sprintf("%s %v", p, off), nil
}

// clockJump steps a running process's clock: a SystemClock that follows
// the host's wall clock, as NTP steps it. The heal steps it back.
func (j *chaosJob) clockJump(ctx context.Context, rng *rand.Rand) (nemesis.Heal, string, error) {
	var procs []string
	for _, p := range j.db.Procs("") {
		if j.db.Running(p) && !j.db.Paused(p) {
			procs = append(procs, p)
		}
	}
	if len(procs) == 0 || j.maxSkew <= 0 {
		return nil, "", nemesis.ErrNoTarget
	}
	p := pick(rng, procs...)
	off := j.skewOf(rng)
	if err := j.db.SetClock(ctx, p, off); err != nil {
		return nil, p, err
	}
	return func(ctx context.Context) error {
		if !j.db.Running(p) {
			return nil
		}
		return j.db.SetClock(ctx, p, 0)
	}, fmt.Sprintf("%s %v", p, off), nil
}
