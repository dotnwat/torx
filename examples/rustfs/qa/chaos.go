//go:build unix

package main

import (
	"context"
	"encoding/json"
	"errors"
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
	"github.com/dotnwat/torx/examples/rustfs/qa/rustfs"
	"github.com/dotnwat/torx/examples/rustfs/qa/s3"
	"github.com/dotnwat/torx/nemesis"
	"github.com/dotnwat/torx/netfault"
	"github.com/dotnwat/torx/resfault"
)

// Parameters of rustfs.chaos.
const (
	paramServers    = "servers"    // cluster size
	paramDrives     = "drives"     // drives per server
	paramDuration   = "duration"   // seconds of faults and load
	paramClients    = "clients"    // concurrent clients
	paramKeys       = "keys"       // keys the clients contend on
	paramFaults     = "faults"     // comma-separated fault names, or "all" and exclusions
	paramFragile    = "fragile"    // drives the drive faults may hit, at most the parity
	paramParity     = "parity"     // the erasure code's parity, "" for RustFS's default for the pool
	paramVersioning = "versioning" // whether the bucket keeps versions
	paramConfig     = "config"     // "random": each server starts with settings of its own; "default"
	paramNemeses    = "nemeses"    // nemeses injecting faults at once, each one fault at a time
	paramMaxSize    = "max-size"   // the largest object written, in KiB
	paramTrial      = "trial"      // distinguishes repeated variants; each draws its own seed
	paramTolerate   = "tolerate"   // comma-separated anomaly kinds reported as warnings, not failures
	paramAllow      = "allow"      // comma-separated known issues the model allows, so a run finds others (README.md)
	paramAudit      = "audit"      // the path of RustFS's dump_versions: sample what each drive holds every second (audit.go)

	defaultServers  = 4
	defaultDrives   = 2
	defaultDuration = 30
	defaultClients  = 8
	defaultKeys     = 8
	defaultMaxSize  = 2 << 10
)

// knownIssues are the issues of RustFS the model can allow, so that a run
// that keeps finding one can find others. README.md describes each.
var knownIssues = map[string]bool{
	// A write with If-Match takes effect on a delete marker, whatever
	// ETag it names.
	"ifmatch-delete-marker": true,
	// In a bucket with versioning, a delete with If-Match of a key never
	// written succeeds, and leaves a delete marker.
	"delete-ifmatch-missing": true,
}

// defaultAllow is the known issues a run allows unless told otherwise.
const defaultAllow = "ifmatch-delete-marker,delete-ifmatch-missing"

const (
	// driveLimit is the size of each drive when the nodes can take disk
	// faults: room for the objects of a run and the trash they leave, and
	// little enough to fill in a moment. A tmpfs takes memory only for what
	// it holds.
	driveLimit = 1 << 30
	// opTimeout bounds each client operation, plus a second per MiB of a
	// write's body.
	opTimeout = 10 * time.Second
	// settleTimeout bounds how long, once every fault is healed, the cluster
	// may take to serve again, and the clients' operations in flight to
	// complete.
	settleTimeout = 120 * time.Second
	// checkBudget bounds the search for an order of each key's operations.
	checkBudget = 2 * time.Minute
)

// schedule is the nemesis's: torx's default, but with a long hold that
// outlasts the 30 seconds a lock of RustFS's lasts without renewal, so that
// a server paused while it holds one loses it.
var schedule = func() nemesis.Schedule {
	s := nemesis.DefaultSchedule
	s.LongHold = 45 * time.Second
	return s
}()

// chaosJob is rustfs.chaos: a randomized test of RustFS's guarantees under
// faults. Clients write, conditionally write, read, delete, and list a few
// keys of one bucket, each request to a server picked at random, while a
// nemesis crashes, pauses, and reconfigures servers, wipes and corrupts
// drives, fills disks, and cuts and degrades the network between them. Every
// value written is unique and its bytes derive from its name, so every read
// is checked byte for byte; once every fault is healed, every server reads
// every key, and each key's history -- a listing is a read of every key --
// must be linearizable: the order S3's read-after-write consistency and its
// preconditions promise. A server that exits on its own is an anomaly too.
type chaosJob struct {
	torx.JobBase
	fs        *rustfs.Service
	servers   int
	drives    int
	duration  time.Duration
	clients   int
	keys      []string
	faults    []string
	allFaults bool
	random    bool
	tolerate  map[string]bool
	diskErr   error
	configs   [][]string
}

// Matrix is one short variant: the chaos job as a smoke test. Hunting for
// bugs is a -params run with longer durations and many trials.
func (*chaosJob) Matrix() []torx.Params {
	return []torx.Params{{paramDuration: 20}}
}

func parseFaults(spec string) ([]string, bool) {
	parts := strings.Split(spec, ",")
	if parts[0] != "all" {
		return parts, false
	}
	faults := slices.Sorted(maps.Keys(faultWeights))
	for _, p := range parts[1:] {
		faults = slices.DeleteFunc(faults, func(f string) bool { return "-"+f == p })
	}
	return faults, true
}

func (*chaosJob) ResolveParams(p torx.Params) (torx.Params, error) {
	out := torx.Params{
		paramServers: defaultServers, paramDrives: defaultDrives, paramDuration: defaultDuration,
		paramClients: defaultClients, paramKeys: defaultKeys, paramFaults: "all", paramFragile: -1,
		paramParity: "", paramVersioning: false, paramConfig: "random", paramNemeses: 1,
		paramMaxSize: defaultMaxSize, paramTolerate: "", paramAllow: defaultAllow, paramAudit: "",
	}
	for k, v := range p {
		switch k {
		case paramServers, paramDrives, paramDuration, paramClients, paramKeys, paramFragile, paramNemeses, paramMaxSize, paramTrial:
			n, ok := intValue(v)
			bounds := map[string][2]int{
				paramServers: {2, 16}, paramDrives: {1, 16}, paramDuration: {1, 3600}, paramClients: {1, 64},
				paramKeys: {1, 1000}, paramFragile: {-1, 16}, paramNemeses: {1, 4}, paramMaxSize: {1, 64 << 10},
				paramTrial: {0, math.MaxInt32},
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
			faults, _ := parseFaults(s)
			for _, f := range faults {
				if _, ok := faultWeights[f]; !ok {
					return nil, fmt.Errorf("unknown fault %q", f)
				}
			}
			out[k] = s
		case paramParity:
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a string, got %v", k, v)
			}
			out[k] = s
		case paramVersioning:
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("%s must be a boolean, got %v", k, v)
			}
			out[k] = b
		case paramConfig:
			if v != "random" && v != "default" {
				return nil, fmt.Errorf("%s must be random or default, got %v", k, v)
			}
			out[k] = v
		case paramTolerate:
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a string, got %v", k, v)
			}
			out[k] = s
		case paramAudit:
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a string, got %v", k, v)
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
	case int64:
		return int(n), true
	case float64:
		if n != math.Trunc(n) {
			return 0, false
		}
		return int(n), true
	}
	return 0, false
}

func (j *chaosJob) Declare(jc *torx.JobContext) {
	j.servers = jc.Params.Int(paramServers, defaultServers)
	j.drives = jc.Params.Int(paramDrives, defaultDrives)
	j.duration = time.Duration(jc.Params.Int(paramDuration, defaultDuration)) * time.Second
	j.clients = jc.Params.Int(paramClients, defaultClients)
	j.keys = nil
	for i := range jc.Params.Int(paramKeys, defaultKeys) {
		j.keys = append(j.keys, "k"+strconv.Itoa(i))
	}
	j.random = jc.Params.String(paramConfig, "random") == "random"
	j.tolerate = map[string]bool{}
	for k := range strings.SplitSeq(jc.Params.String(paramTolerate, ""), ",") {
		if k != "" {
			j.tolerate[k] = true
		}
	}
	j.faults, j.allFaults = parseFaults(jc.Params.String(paramFaults, "all"))
	j.fs = rustfs.New(serviceName, j.servers, j.drives)
	jc.Register(j.fs)
}

// parity is the erasure code's parity for the pool: the parameter's, or
// RustFS's default for a pool of drives drives (default_parity_count).
func parity(param string, drives int) int {
	if p, err := strconv.Atoi(strings.TrimPrefix(param, "EC:")); err == nil {
		return p
	}
	switch {
	case drives == 1:
		return 0
	case drives <= 3:
		return 1
	case drives <= 5:
		return 2
	case drives <= 7:
		return 3
	}
	return 4
}

// Setup draws each server's settings, and limits the drives when the nodes
// can take disk faults, before the cluster starts.
func (j *chaosJob) Setup(ctx context.Context, jc *torx.JobContext) error {
	rng := jc.Rand("config")
	var env []string
	if p := jc.Params.String(paramParity, ""); p != "" {
		env = append(env, "RUSTFS_STORAGE_CLASS_STANDARD=EC:"+strings.TrimPrefix(p, "EC:"))
	}
	j.fs.SetEnv(env...)
	j.configs = nil
	for i := range j.servers {
		var cfg []string
		if j.random {
			cfg = drawConfig(rng)
		}
		j.configs = append(j.configs, cfg)
		j.fs.SetServerEnv(i, cfg...)
		jc.Log("info", fmt.Sprintf("server %d settings: %s", i, strings.Join(cfg, " ")))
	}
	j.diskErr = nil
	for _, n := range j.fs.Nodes() {
		if err := diskfault.Check(ctx, n, n.Scratch().Root); err != nil {
			j.diskErr = err
			break
		}
	}
	if j.diskErr == nil {
		j.fs.SetDriveLimit(driveLimit)
	}
	return j.JobBase.Setup(ctx, jc)
}

// drawConfig draws a server's settings: ones each server may hold apart
// from the others.
func drawConfig(rng *rand.Rand) []string {
	var env []string
	if rng.IntN(2) == 0 {
		env = append(env, "RUSTFS_DURABILITY_MODE="+[]string{"strict", "relaxed", "none"}[rng.IntN(3)])
	}
	return env
}

func (j *chaosJob) Run(ctx context.Context, jc *torx.JobContext) error {
	netOK := netfault.Check(ctx, j.fs.Nodes())
	var resOK error
	for _, n := range j.fs.Nodes() {
		if resOK = resfault.Check(n); resOK != nil {
			break
		}
	}
	var faultNames []string
	for _, f := range j.faults {
		var why error
		switch {
		case netFaults[f]:
			why = netOK
		case diskFaults[f]:
			why = j.diskErr
		case resFaults[f]:
			why = resOK
		}
		switch {
		case why == nil:
			faultNames = append(faultNames, f)
		case !j.allFaults:
			return fmt.Errorf("fault %s needs nodes it can act on (run with -netns, or -cgroups): %w", f, why)
		}
	}
	for what, err := range map[string]error{"network": netOK, "disk": j.diskErr, "resource": resOK} {
		if err != nil {
			jc.Log("info", fmt.Sprintf("no %s faults: %v", what, err))
		}
	}
	if len(faultNames) == 0 {
		return errors.New("no faults to inject")
	}

	const bucket = "chaos"
	if err := j.fs.WaitWritable(ctx, settleTimeout); err != nil {
		return err
	}
	admin, err := j.fs.Client(j.fs.Nodes()[0])
	if err != nil {
		return err
	}
	defer admin.Close()
	if err := admin.CreateBucket(ctx, bucket); err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	if jc.Params.Bool(paramVersioning, false) {
		if err := admin.SetVersioning(ctx, bucket, true); err != nil {
			return fmt.Errorf("enable versioning: %w", err)
		}
	}

	// The fragile drives: as many as the erasure code can lose, less a
	// server's worth so a server down besides leaves the objects readable,
	// each on a server of its own while there are servers enough.
	p := parity(jc.Params.String(paramParity, ""), j.servers*j.drives)
	nFragile := jc.Params.Int(paramFragile, -1)
	if nFragile < 0 {
		nFragile = max(0, min(p-j.drives, p/2))
		if nFragile == 0 && p > 0 {
			nFragile = 1
		}
	}
	if nFragile > p {
		return fmt.Errorf("%d fragile drives are more than the parity, %d, can lose", nFragile, p)
	}
	frng := jc.Rand("fragile")
	var fragile []drive
	for _, i := range frng.Perm(j.servers * j.drives) {
		if len(fragile) == nFragile {
			break
		}
		// Spread across servers first: drive index i%servers is a server.
		n := j.fs.Nodes()[i%j.servers]
		fragile = append(fragile, drive{n, (i / j.servers) % j.drives})
	}
	var fragileNames []string
	for _, d := range fragile {
		fragileNames = append(fragileNames, d.String())
	}
	jc.Log("info", fmt.Sprintf("parity %d; fragile drives: %s", p, strings.Join(fragileNames, " ")))

	h := NewHistory()
	an := &anomalies{}
	maxSize := jc.Params.Int(paramMaxSize, defaultMaxSize) << 10
	w := &workload{
		bucket: bucket, keys: j.keys, h: h, vals: newValues(), timeout: opTimeout, anomalies: an,
		sizes: func(rng *rand.Rand) int {
			// Most objects small enough to be stored inline in their
			// metadata (at most 128KiB a shard), some of one or two
			// erasure blocks (1MiB each), a few larger. RustFS keeps what
			// an overwrite replaces in a trash it empties every five
			// minutes, so the larger the objects, the sooner the trash
			// fills a drive.
			switch r := rng.IntN(20); {
			case r < 14:
				return 64 + rng.IntN(4<<10)
			case r < 19:
				return min(maxSize, 4<<10+rng.IntN(300<<10))
			default:
				return min(maxSize, 300<<10+rng.IntN(max(1, maxSize-300<<10)))
			}
		},
		noMultipart: maxSize < 2*multipartPart+1,
	}
	for _, n := range j.fs.Nodes() {
		w.servers = append(w.servers, n.Name())
	}

	f := &faults{
		fs: j.fs, h: h, jc: jc, bucket: bucket, fragile: fragile, draw: drawConfig,
		frozen: map[string]bool{}, paused: map[string]bool{},
	}
	if !j.random {
		f.draw = func(*rand.Rand) []string { return nil }
	}
	runCtx, stop := context.WithTimeout(ctx, j.duration)
	defer stop()
	var wg sync.WaitGroup
	var conns []*s3.Client
	for i := range j.clients {
		c := &client{w: w, id: i, rng: jc.Rand(fmt.Sprintf("client-%d", i)), seen: map[string]string{}, history: map[string][]string{}}
		for _, n := range j.fs.Nodes() {
			conn, err := j.fs.Client(n)
			if err != nil {
				return err
			}
			c.conns = append(c.conns, conn)
			conns = append(conns, conn)
		}
		wg.Go(func() { c.run(runCtx) })
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	var nwg sync.WaitGroup
	for i := range jc.Params.Int(paramNemeses, 1) {
		stream := "nemesis"
		if i > 0 {
			stream = fmt.Sprintf("nemesis-%d", i)
		}
		m := &nemesis.Nemesis{
			Faults: f.build(faultNames), Schedule: schedule, Rand: jc.Rand(stream),
			Before: f.supervise, Record: f.record,
		}
		nwg.Go(func() { m.Run(runCtx) })
	}
	var aud *auditor
	if dump := jc.Params.String(paramAudit, ""); dump != "" {
		aud = newAuditor(j.fs, h, bucket, j.keys, dump)
		nwg.Go(func() { aud.run(runCtx) })
	}
	<-runCtx.Done()
	nwg.Wait()

	if err := f.healAll(ctx, netOK == nil, j.diskErr == nil, resOK == nil); err != nil {
		an.add(anomaly{Kind: "heal-failed", Detail: err.Error()})
	}
	healed := h.Now()
	settled := make(chan struct{})
	go func() { wg.Wait(); close(settled) }()
	select {
	case <-settled:
	case <-time.After(settleTimeout):
		an.add(anomaly{Kind: "stuck", Detail: fmt.Sprintf("client operations still in flight %v after every fault was healed", settleTimeout)})
		<-settled
	}
	jc.Log("info", fmt.Sprintf("clients settled %v after the heal", h.Now()-healed))

	if err := j.final(ctx, w, jc.Params.Bool(paramVersioning, false)); err != nil {
		an.add(anomaly{Kind: "unavailable", Detail: err.Error()})
	}
	for _, e := range j.fs.Exits() {
		an.add(anomaly{Kind: "unexpected-exit", Node: e.Node, Detail: fmt.Sprintf("exit status %d at %s: %s", e.Code, e.Time.Format(time.RFC3339Nano), e.Said)})
	}
	// What the drives hold at the end, for a run whose drives filled up.
	usage := map[string]map[string]int64{}
	for _, n := range j.fs.Nodes() {
		for d := range j.drives {
			if u, err := j.fs.Usage(ctx, n, d); err == nil {
				usage[drive{n, d}.String()] = u
			}
		}
	}
	ops := h.Ops()
	allow := map[string]bool{}
	for a := range strings.SplitSeq(jc.Params.String(paramAllow, defaultAllow), ",") {
		allow[a] = true
	}
	model := keyModel(jc.Params.Bool(paramVersioning, false),
		allowed{ifMatchOnMarker: allow["ifmatch-delete-marker"], deleteIfMatchMissing: allow["delete-ifmatch-missing"]})
	for _, a := range checkLinear(ctx, model, ops, j.keys, checkBudget) {
		an.add(a)
	}
	for _, a := range checkVersions(ops) {
		an.add(a)
	}
	if aud != nil {
		// What the drives hold after the final reads, heal having had its
		// time.
		aud.sample(ctx)
		for _, a := range aud.anomalies(ops) {
			an.add(a)
		}
		jc.Log("info", fmt.Sprintf("audit: %d samples failed", aud.failed))
	}

	st := stats(ops)
	all := an.all()
	if len(all) > 0 {
		j.archiveDrives(ctx, jc, bucket)
	}
	jc.WriteArtifact("history.ndjson", h.NDJSON())
	report, _ := json.MarshalIndent(map[string]any{
		"anomalies": all, "stats": st, "exits": j.fs.Exits(), "configs": j.configs,
		"faults": faultNames, "fragile": fragileNames, "parity": p, "usage": usage,
		"start": h.Start().Format(time.RFC3339Nano),
	}, "", "  ")
	jc.WriteArtifact("check.json", report)
	var failed []string
	for _, a := range all {
		level := "error"
		if j.tolerate[a.Kind] {
			level = "warn"
		} else {
			failed = append(failed, a.Kind)
		}
		jc.Log(level, fmt.Sprintf("%s: %s %s", a.Kind, a.Key, a.Detail))
	}
	_ = jc.Record(map[string]any{"anomalies": all, "stats": st})
	jc.SetSummary(summary(st, len(all)))
	if len(failed) > 0 {
		return fmt.Errorf("anomalies: %s", strings.Join(slices.Compact(slices.Sorted(slices.Values(failed))), ", "))
	}
	return nil
}

// final waits for every server to serve, and then has every server read
// every key and list the bucket -- and, with versioning, list every version
// -- retrying what does not answer, so the history ends with what each
// server says the cluster holds.
func (j *chaosJob) final(ctx context.Context, w *workload, versioned bool) error {
	ctx, cancel := context.WithTimeout(ctx, settleTimeout)
	defer cancel()
	for _, n := range j.fs.Nodes() {
		if err := j.fs.WaitReady(ctx, n, settleTimeout); err != nil {
			return fmt.Errorf("%s not ready after the heal: %w", n.Name(), err)
		}
	}
	if err := j.fs.WaitWritable(ctx, settleTimeout); err != nil {
		return err
	}
	var errs []error
	for si, n := range j.fs.Nodes() {
		conn, err := j.fs.Client(n)
		if err != nil {
			return err
		}
		fw := *w
		fw.servers = []string{n.Name()}
		c := &client{w: &fw, id: -1 - si, label: "final", rng: rand.New(rand.NewPCG(0, 0)), conns: []*s3.Client{conn},
			seen: map[string]string{}, history: map[string][]string{}}
		reads := append(slices.Clone(j.keys), "")
		if versioned {
			reads = append(reads, "-versions")
		}
		for _, k := range reads {
			f := "get"
			switch k {
			case "":
				f = "list"
			case "-versions":
				f, k = "list-versions", ""
			}
			var op Op
			for {
				if op = c.doKey(ctx, f, 0, k); op.Outcome == Ok || ctx.Err() != nil {
					break
				}
				nemesis.Sleep(ctx, time.Second)
			}
			if op.Outcome != Ok {
				errs = append(errs, fmt.Errorf("final %s of %q via %s: %s %s", f, k, n.Name(), op.Code, op.Err))
			}
		}
		conn.Close()
	}
	return errors.Join(errs...)
}

// archiveDrives attaches each drive's metadata of the bucket -- every
// object's xl.meta and its backup, without the shards -- to the results,
// so a run that found an anomaly keeps what the drives held at its end.
func (j *chaosJob) archiveDrives(ctx context.Context, jc *torx.JobContext, bucket string) {
	for _, n := range j.fs.Nodes() {
		for d := range j.drives {
			res, err := n.Exec(ctx, torx.Command("tar", "-czf", "-", "-C", j.fs.Drive(n, d), "--exclude=part.*", bucket))
			name := fmt.Sprintf("%s-drive%d.tgz", n.Name(), d)
			if err != nil || res.ExitCode != 0 {
				jc.Log("warn", fmt.Sprintf("archive %s: %v %s", name, err, res.Stderr))
				continue
			}
			jc.WriteArtifact(name, res.Stdout)
		}
	}
}

func summary(st map[string]int, anomalies int) string {
	return fmt.Sprintf("%d ops (%d ok, %d info), %d faults; %d anomalies",
		st["ops"], st["ok"], st["info"], st["faults"], anomalies)
}

func stats(ops []Op) map[string]int {
	st := map[string]int{}
	for _, op := range ops {
		if op.Process == "nemesis" {
			if !strings.HasPrefix(op.F, "heal-") && !strings.HasPrefix(op.F, "supervise") {
				st["faults"]++
			}
			continue
		}
		st["ops"]++
		st[string(op.Outcome)]++
		st[op.F+":"+string(op.Outcome)]++
	}
	return st
}
