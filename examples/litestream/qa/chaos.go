//go:build unix

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"net"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/diskfault"
	"github.com/dotnwat/torx/examples/litestream/model"
	"github.com/dotnwat/torx/examples/litestream/qa/litestream"
	"github.com/dotnwat/torx/nemesis"
	"github.com/dotnwat/torx/objstore"
)

// Parameters of litestream.chaos.
const (
	paramDuration = "duration"  // seconds of faults and load
	paramClients  = "clients"   // concurrent clients of the app
	paramKeys     = "keys"      // keys of each table
	paramMaxValue = "max-value" // the largest common value, in bytes; a rare one is up to five times that
	paramFaults   = "faults"    // comma-separated fault names, or "all" and exclusions ("all,-failover")
	paramConfig   = "config"    // "random": settings drawn from the seed; "default": Litestream's and SQLite's
	paramFollow   = "follow"    // run a follower (restore -f) on the standby and read it
	paramRestores = "restores"  // restore the replica on the standby throughout the run
	paramPace     = "pace"      // the most milliseconds a client waits between transactions
	paramAllow    = "allow"     // comma-separated known issues reported as warnings, so a run finds others (README.md)
	paramTrial    = "trial"     // distinguishes repeated variants; each draws its own seed

	defaultDuration = 30
	defaultClients  = 4
	defaultKeys     = 300
	defaultMaxValue = 4096
	defaultPace     = 20
	defaultAllow    = "torn-commit,restart-gap,follow-regress"
)

// knownIssues are the issues of Litestream a run can allow, so that a run
// that keeps finding one can find others. README.md describes each.
var knownIssues = map[string]string{
	// Litestream takes every checksummed commit frame in the WAL file as
	// committed, though SQLite takes only those its WAL index says it
	// wrote: an app killed in the moments between has its transaction
	// replicated, though SQLite discards it and writes the next one over
	// it. The checker reports a copy holding such a transaction as this.
	"torn-commit": "a copy holds a transaction its app was killed committing, which the app's database does not",
	// Litestream restarted after the app's WAL restarted -- checkpointed
	// and written over from the start while it was down -- can miss what
	// the app committed meanwhile, and the replica is corrupt. Allowed,
	// the run sidesteps it: the app does not checkpoint on its own, and
	// while Litestream is down it checkpoints only to truncate the WAL.
	"restart-gap": "sidestepped: no app checkpoints that restart the WAL while Litestream is down",
	// A follower whose poll fails partway applies the poll's files again
	// from where it began it, over newer pages, and its copy goes back,
	// perhaps to a state that never was. The checker reports what it
	// finds wrong with a copy read after a poll of its life failed as this.
	"follow-regress": "a follower's copy, read after one of its polls failed, went back or holds a state that never was",
}

const (
	bucket      = "litestream"
	replicaPath = "db"
	nodes       = 2
	// opTimeout bounds a request to the app: a large transaction, or one
	// that waits out Litestream's checkpoint.
	opTimeout = 60 * time.Second
	// syncTimeout bounds `litestream sync -wait`.
	syncTimeout = 30 * time.Second
	// restoreTimeout bounds a restore and its check.
	restoreTimeout = 2 * time.Minute
	// settleTimeout bounds how long, once every fault is healed, the
	// replica may take to catch up.
	settleTimeout = 3 * time.Minute
	// stallMax is the longest a store fault holds a request, and so how
	// long a failover waits for a dead primary's requests to land.
	stallMax = 3 * time.Second
	// keepBroken is how many broken copies the job keeps.
	keepBroken = 4
	// diskSize is the size of the database's disk, when the nodes can
	// mount one: far more than a run writes, until a fault fills it.
	diskSize = 1 << 30
)

// faultWeights are the faults the nemesis draws from, by name, with their
// weights.
var faultWeights = map[string]int{
	"crash-litestream": 4, // kill Litestream; the app writes on; restart it
	"stop-litestream":  2, // stop Litestream with SIGTERM, its shutdown sync; restart it
	"pause-litestream": 2, // SIGSTOP Litestream, SIGCONT it
	"outage":           4, // Litestream down while the app checkpoints, reopens, crashes, or vacuums
	"crash-app":        3, // kill the app mid-transaction; restart it
	"restart-app":      1, // stop the app with SIGTERM; restart it
	"app-checkpoint":   3, // the app checkpoints the WAL in any mode, under Litestream
	"vacuum":           1, // the app vacuums, in full or incrementally
	"store-errors":     3, // Litestream's store requests fail, before or after they take effect
	"store-slow":       2, // Litestream's store requests are delayed, or land late
	"store-down":       1, // every store request of Litestream's waits seconds
	"store-partition":  2, // Litestream is cut off from the store; its requests go through when it heals
	"disk-full":        2, // the primary's disk, the database's and Litestream's, fills up; with -netns
	"restore-errors":   1, // the restores' and the follower's store requests fail or are delayed
	"failover":         2, // lose the primary; restore the replica on the standby and run there
}

func parseFaults(spec string) []string {
	parts := strings.Split(spec, ",")
	if parts[0] != "all" {
		return parts
	}
	faults := slices.Sorted(maps.Keys(faultWeights))
	for _, p := range parts[1:] {
		faults = slices.DeleteFunc(faults, func(f string) bool { return "-"+f == p })
	}
	return faults
}

// chaosJob is litestream.chaos: a randomized test of Litestream's
// guarantees under faults. An application (lsapp) commits transactions to
// a SQLite database on one node, the primary, which Litestream replicates
// to an object store the job serves, while a nemesis crashes, stops, and
// pauses Litestream, crashes the application, has it checkpoint and vacuum
// under Litestream, faults the object store, and fails the primary over to
// the standby by restoring the replica there. Throughout, the standby
// restores the replica -- its latest state, a TXID, a point in time -- and
// a follower there applies it as it grows.
//
// Every transaction is logged in the database, and every value derives from
// the transaction that wrote it, so every copy -- each restore, the
// follower's, the primary's -- says which transactions it holds, and the
// job checks that it holds exactly what they wrote. A copy must be intact
// and a prefix of what a primary committed; a restore must hold every
// transaction a `litestream sync -wait` returned for before it began, never
// go back, restore one TXID the same way every time, and restore a time to
// no transaction that began after it. Once every fault is healed, the
// replica must restore to the primary's database exactly.
type chaosJob struct {
	torx.JobBase
	svc      *litestream.Service
	duration time.Duration
	clients  int
	keys     int64
	maxValue int
	faults   []string
	random   bool
	follow   bool
	restores bool
	pace     time.Duration
	allow    map[string]bool
	// tune, if set, adjusts the settings drawn before anything starts: a
	// job that reproduces one issue fixes the settings it needs.
	tune func(*settings, *litestream.AppConfig)
	// disk says the database's directory is a disk the job can fill.
	disk bool

	jc       *torx.JobContext
	store    *objstore.Server
	endpoint string
	ls       settings
	app      litestream.AppConfig
	sockID   string
	start    time.Time
	client   *appClient

	mu         sync.Mutex
	epoch      int
	primary    int // the primary's node index
	nextUID    uint64
	specs      map[uint64]model.Txn
	txns       []*txnRec
	maxN       map[int]int64    // epoch -> the highest txlog row known committed
	bases      map[int][]uint64 // epoch -> the txlog it began with
	acks       []ackRec
	obs        []*obsRec
	events     []nemesis.Event
	notes      []anomaly
	followLife int
	kept       int
	restoreN   int
	// appKills are when the job killed an app: a transaction in flight
	// then may have been torn.
	appKills []time.Time
}

func (*chaosJob) Matrix() []torx.Params {
	return []torx.Params{{paramDuration: 20}}
}

func (*chaosJob) ResolveParams(p torx.Params) (torx.Params, error) {
	out := torx.Params{
		paramDuration: defaultDuration, paramClients: defaultClients, paramKeys: defaultKeys,
		paramMaxValue: defaultMaxValue, paramFaults: "all", paramConfig: "random",
		paramFollow: true, paramRestores: true, paramPace: defaultPace, paramAllow: defaultAllow,
	}
	for k, v := range p {
		switch k {
		case paramDuration, paramClients, paramKeys, paramMaxValue, paramPace, paramTrial:
			n, ok := intValue(v)
			bounds := map[string][2]int{
				paramDuration: {1, 3600}, paramClients: {1, 64}, paramKeys: {1, 1 << 20},
				paramMaxValue: {64, 1 << 20}, paramPace: {0, 10000}, paramTrial: {0, math.MaxInt32},
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
				if _, ok := knownIssues[a]; a != "" && !ok {
					return nil, fmt.Errorf("unknown issue %q to allow", a)
				}
			}
			out[k] = s
		case paramConfig:
			if v != "random" && v != "default" {
				return nil, fmt.Errorf("%s must be random or default, got %v", k, v)
			}
			out[k] = v
		case paramFollow, paramRestores:
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
	j.svc = litestream.New(serviceName, nodes)
	jc.Register(j.svc)
	j.duration = time.Duration(jc.Params.Int(paramDuration, defaultDuration)) * time.Second
	j.clients = jc.Params.Int(paramClients, defaultClients)
	j.keys = int64(jc.Params.Int(paramKeys, defaultKeys))
	j.maxValue = jc.Params.Int(paramMaxValue, defaultMaxValue)
	j.faults = parseFaults(jc.Params.String(paramFaults, "all"))
	j.random = jc.Params.String(paramConfig, "random") == "random"
	j.follow = jc.Params.Bool(paramFollow, true)
	j.restores = jc.Params.Bool(paramRestores, true)
	j.pace = time.Duration(jc.Params.Int(paramPace, defaultPace)) * time.Millisecond
	j.allow = map[string]bool{}
	for a := range strings.SplitSeq(jc.Params.String(paramAllow, defaultAllow), ",") {
		j.allow[a] = a != ""
	}
}

// The processes, by node index.
func appName(i int) string    { return fmt.Sprintf("app%d", i) }
func lsName(i int) string     { return fmt.Sprintf("ls%d", i) }
func followName(i int) string { return fmt.Sprintf("follow%d", i) }

// Where things are on a node.
func (j *chaosJob) dir(n *torx.Node) string           { return j.svc.Dir(n) }
func (j *chaosJob) dbPath(n *torx.Node) string        { return path.Join(j.dir(n), "db", "app.db") }
func (j *chaosJob) lsConfig(n *torx.Node) string      { return path.Join(j.dir(n), "replicate.yml") }
func (j *chaosJob) restoreConfig(n *torx.Node) string { return path.Join(j.dir(n), "restore.yml") }
func (j *chaosJob) followConfig(n *torx.Node) string  { return path.Join(j.dir(n), "follow.yml") }
func (j *chaosJob) followDB(n *torx.Node) string      { return path.Join(j.dir(n), "follow", "app.db") }
func (j *chaosJob) restoreDir(n *torx.Node) string    { return path.Join(j.dir(n), "restores") }

// socket is node n's control socket: under /tmp, as a scratch path can be
// longer than a socket's path may be.
func (j *chaosJob) socket(n *torx.Node) string {
	return fmt.Sprintf("/tmp/torx-ls-%s-%s.sock", j.sockID, n.Name())
}

func (j *chaosJob) Setup(ctx context.Context, jc *torx.JobContext) error {
	j.jc = jc
	if err := j.JobBase.Setup(ctx, jc); err != nil {
		return err
	}
	j.specs = map[uint64]model.Txn{}
	j.maxN = map[int]int64{}
	j.bases = map[int][]uint64{0: nil}
	j.client = newAppClient(opTimeout)
	j.sockID = fmt.Sprintf("%08x", jc.Rand("socket").Uint32())
	ns := j.svc.Nodes()
	addr, err := ns[0].CallerAddr()
	if err != nil {
		return err
	}
	j.store = objstore.New(objstore.Options{Rand: jc.Rand("store")})
	j.store.CreateBucket(bucket)
	if j.endpoint, err = j.store.Serve(net.JoinHostPort(addr, "0")); err != nil {
		return err
	}
	jc.Defer(func(context.Context) error { return j.store.Close() })
	jc.Log("info", "object store at "+j.endpoint)

	rng := jc.Rand("config")
	j.ls = drawSettings(rng, j.random)
	j.app = drawApp(rng, j.random)
	if j.allow["restart-gap"] {
		j.app.Autocheckpoint = 0
	}
	if j.tune != nil {
		j.tune(&j.ls, &j.app)
	}
	b, _ := json.MarshalIndent(map[string]any{"litestream": j.ls, "app": j.app}, "", "  ")
	jc.WriteArtifact("settings.json", b)
	jc.Log("info", "settings: "+string(b))

	for i, n := range ns {
		if err := n.WriteFile(ctx, j.lsConfig(n), []byte(replicateYAML(j.dbPath(n), j.socket(n), j.endpoint, lsName(i), j.ls))); err != nil {
			return err
		}
		if err := n.WriteFile(ctx, j.restoreConfig(n), []byte(restoreYAML(j.dbPath(n), j.endpoint, fmt.Sprintf("restore%d", i), "debug", j.ls))); err != nil {
			return err
		}
		if err := n.WriteFile(ctx, j.followConfig(n), []byte(restoreYAML(j.dbPath(n), j.endpoint, fmt.Sprintf("follow%d", i), "info", j.ls))); err != nil {
			return err
		}
		if err := n.Mkdir(ctx, j.restoreDir(n)); err != nil {
			return err
		}
		sock := j.socket(n)
		jc.Defer(func(ctx context.Context) error { return n.Rm(ctx, sock) })
	}
	// Where a node can mount -- a -netns node -- the database's directory,
	// which the WAL and Litestream's LTX files share with it, is a disk
	// the disk-full fault fills.
	j.disk = true
	for _, n := range ns {
		if err := diskfault.Check(ctx, n, j.dir(n)); err != nil {
			jc.Log("info", "no disk faults: "+err.Error())
			j.disk = false
			break
		}
	}
	if j.disk {
		for _, n := range ns {
			if err := j.svc.Limit(ctx, n, path.Dir(j.dbPath(n)), diskSize); err != nil {
				return err
			}
		}
	}
	return j.startPrimary(ctx, 0)
}

// startPrimary starts the app on node i, and then Litestream.
func (j *chaosJob) startPrimary(ctx context.Context, i int) error {
	n := j.svc.Nodes()[i]
	if err := j.svc.LaunchApp(ctx, appName(i), n, j.dbPath(n), j.app); err != nil {
		return err
	}
	if err := j.svc.WaitApp(ctx, appName(i), time.Minute); err != nil {
		return err
	}
	return j.svc.LaunchReplicate(ctx, lsName(i), n, j.lsConfig(n))
}

// current is the primary's epoch and node index.
func (j *chaosJob) current() (epoch, primary int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.epoch, j.primary
}

func (j *chaosJob) appURL() (string, int) {
	e, p := j.current()
	return j.svc.AppURL(appName(p)), e
}

func (j *chaosJob) Run(ctx context.Context, jc *torx.JobContext) error {
	j.start = time.Now()
	runCtx, stop := context.WithTimeout(ctx, j.duration)
	defer stop()

	var clients, aux sync.WaitGroup
	for i := range j.clients {
		rng := jc.Rand(fmt.Sprintf("client%d", i))
		clients.Go(func() { j.runClient(runCtx, i, rng) })
	}
	auxCtx, stopAux := context.WithCancel(ctx)
	defer stopAux()
	aux.Go(func() { j.runSyncer(auxCtx, jc.Rand("syncer")) })
	if j.restores {
		aux.Go(func() { j.runRestorer(auxCtx, jc.Rand("restorer")) })
	}
	if j.follow {
		aux.Go(func() { j.runFollowReader(auxCtx, jc.Rand("follower")) })
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
		Before:   j.supervise,
		Record: func(ev nemesis.Event) {
			j.mu.Lock()
			j.events = append(j.events, ev)
			j.mu.Unlock()
			msg := "nemesis: " + ev.Fault
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
	if len(faults) > 0 {
		nem.Run(runCtx)
	} else {
		<-runCtx.Done()
	}
	clients.Wait()

	// Heal what the nemesis may have left, bring everything back, and have
	// the replica catch up.
	j.store.ClearRules()
	settleCtx, cancel := context.WithTimeout(ctx, settleTimeout)
	defer cancel()
	if err := j.settle(settleCtx); err != nil {
		j.note(anomaly{Kind: "unavailable", Detail: "after every fault was healed: " + err.Error()})
	}
	stopAux()
	aux.Wait()
	if err := j.final(settleCtx); err != nil {
		j.note(anomaly{Kind: "unavailable", Detail: "after every fault was healed: " + err.Error()})
	}
	for _, e := range j.svc.Exits() {
		j.note(anomaly{Kind: "exit", Detail: fmt.Sprintf("%s\n%s", e, e.Tail)})
	}
	j.scanLogs(ctx)
	return j.report(ctx, jc)
}

// scanLogs notes every data race and panic the processes logged, in any
// launch: a race detector's report does not end the process.
func (j *chaosJob) scanLogs(ctx context.Context) {
	for _, n := range j.svc.Nodes() {
		script := `for f in ` + j.dir(n) + `/*.log; do ` +
			`[ -f "$f" ] || continue; ` +
			`c=$(grep -c -e 'WARNING: DATA RACE' -e '^panic: ' -e '^fatal error: ' "$f"); ` +
			`[ "$c" -gt 0 ] && { echo "== $f $c"; grep -m 3 -A 6 -e 'WARNING: DATA RACE' -e '^panic: ' -e '^fatal error: ' "$f"; }; ` +
			`done; true`
		res, err := n.Exec(ctx, torx.Command("sh", "-c", script))
		if err != nil || len(strings.TrimSpace(string(res.Stdout))) == 0 {
			continue
		}
		for chunk := range strings.SplitSeq(strings.TrimSpace(string(res.Stdout)), "== ") {
			if chunk = strings.TrimSpace(chunk); chunk == "" {
				continue
			}
			kind := "panic"
			if strings.Contains(chunk, "DATA RACE") {
				kind = "data-race"
			}
			head, body, _ := strings.Cut(chunk, "\n")
			j.note(anomaly{Kind: kind, Detail: fmt.Sprintf("%s on %s:\n%s", path.Base(strings.Fields(head)[0]), n.Name(), body)})
		}
	}
}

// runClient sends transactions to the primary until ctx ends.
func (j *chaosJob) runClient(ctx context.Context, id int, rng *rand.Rand) {
	for ctx.Err() == nil {
		j.mu.Lock()
		j.nextUID++
		uid := j.nextUID
		j.mu.Unlock()
		txn := genTxn(rng, uid, j.keys, j.maxValue)
		url, epoch := j.appURL()
		rec := &txnRec{UID: uid, Client: id, Epoch: epoch, Call: time.Now()}
		j.mu.Lock()
		j.specs[uid] = txn
		j.txns = append(j.txns, rec)
		j.mu.Unlock()
		octx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
		r, err := j.client.Txn(octx, url, txn)
		cancel()
		j.mu.Lock()
		rec.Return = time.Now()
		switch {
		case err != nil:
			rec.Outcome, rec.Err = inDoubt, err.Error()
		case r.RolledBack:
			rec.Outcome = rolledBack
		case r.Error == "":
			rec.Outcome, rec.N = committed, r.N
		case r.Outcome == committed:
			rec.Outcome, rec.N, rec.Err = committed, r.N, r.Error
		case r.Outcome == aborted:
			rec.Outcome, rec.Err = aborted, r.Error
		default:
			rec.Outcome, rec.Err = inDoubt, r.Error
		}
		if rec.Outcome == committed && rec.N > j.maxN[epoch] && epoch == j.epoch {
			j.maxN[epoch] = rec.N
		}
		j.mu.Unlock()
		if err != nil {
			nemesis.Sleep(ctx, 100*time.Millisecond)
		} else if j.pace > 0 {
			nemesis.Sleep(ctx, time.Duration(rng.Int64N(int64(j.pace))))
		}
	}
}

// resolve asks the app of epoch which of the transactions in doubt it
// committed.
func (j *chaosJob) resolve(ctx context.Context, epoch int, url string) error {
	j.mu.Lock()
	var doubt []*txnRec
	for _, t := range j.txns {
		if t.Epoch == epoch && t.Outcome == inDoubt && !t.Return.IsZero() {
			doubt = append(doubt, t)
		}
	}
	j.mu.Unlock()
	for _, t := range doubt {
		n, err := j.client.Lookup(ctx, url, t.UID)
		if err != nil {
			return fmt.Errorf("resolve %d: %w", t.UID, err)
		}
		j.mu.Lock()
		t.Resolved = true
		if n > 0 {
			t.Outcome, t.N = committed, n
		} else {
			t.Outcome = aborted
		}
		j.mu.Unlock()
	}
	return nil
}

// runSyncer asks Litestream, every so often, to sync the replica and wait
// for it: an ack of everything committed before it was asked.
func (j *chaosJob) runSyncer(ctx context.Context, rng *rand.Rand) {
	for nemesis.Sleep(ctx, time.Duration(200+rng.IntN(1300))*time.Millisecond) {
		j.syncOnce(ctx)
	}
}

func (j *chaosJob) syncOnce(ctx context.Context) error {
	j.mu.Lock()
	e, p := j.epoch, j.primary
	prefix := j.maxN[e]
	j.mu.Unlock()
	if !j.svc.Running(lsName(p)) || j.svc.Paused(lsName(p)) {
		return fmt.Errorf("%s is not running", lsName(p))
	}
	n := j.svc.Nodes()[p]
	a := ackRec{Epoch: e, Prefix: prefix, Call: time.Now()}
	res, err := j.svc.Sync(ctx, n, j.socket(n), j.dbPath(n), syncTimeout)
	a.Return = time.Now()
	if err != nil {
		a.Err = err.Error()
	} else {
		a.TXID, a.ReplicaTXID = res.TXID, res.ReplicaTXID
	}
	j.mu.Lock()
	j.acks = append(j.acks, a)
	j.mu.Unlock()
	return err
}

// runRestorer restores the replica on the standby, every so often: its
// latest state, a TXID it holds, or a time since the run began.
func (j *chaosJob) runRestorer(ctx context.Context, rng *rand.Rand) {
	for nemesis.Sleep(ctx, time.Duration(500+rng.IntN(2500))*time.Millisecond) {
		var t litestream.Target
		switch r := rng.IntN(10); {
		case r < 5:
		case r < 8:
			txids := j.replicaTXIDs()
			if len(txids) == 0 {
				continue
			}
			if rng.IntN(2) == 0 {
				t.TXID = txids[rng.IntN(len(txids))]
			} else {
				t.TXID = 1 + rng.Uint64N(slices.Max(txids))
			}
		default:
			span := time.Since(j.start) - 500*time.Millisecond
			if span <= 0 {
				continue
			}
			t.Timestamp = j.start.Add(time.Duration(rng.Int64N(int64(span)))).Truncate(time.Millisecond)
		}
		_, p := j.current()
		j.restore(ctx, j.svc.Nodes()[1-p], obsRestore, t)
	}
}

// replicaTXIDs are the TXIDs the replica's LTX files end at.
func (j *chaosJob) replicaTXIDs() []uint64 {
	var out []uint64
	for _, o := range j.store.List(bucket, replicaPath+"/") {
		name := path.Base(o.Key)
		_, hi, ok := strings.Cut(strings.TrimSuffix(name, ".ltx"), "-")
		if !ok || !strings.HasSuffix(name, ".ltx") {
			continue
		}
		if v, err := strconv.ParseUint(hi, 16, 64); err == nil {
			out = append(out, v)
		}
	}
	return out
}

// restore restores the replica on n as target says, checks the copy, and
// records it.
func (j *chaosJob) restore(ctx context.Context, n *torx.Node, kind string, t litestream.Target) *obsRec {
	j.mu.Lock()
	j.restoreN++
	k := j.restoreN
	j.mu.Unlock()
	out := path.Join(j.restoreDir(n), fmt.Sprintf("r%d.db", k))
	o := j.beginObs(kind, n, t)
	rctx, cancel := context.WithTimeout(ctx, restoreTimeout)
	defer cancel()
	res, err := j.svc.Restore(rctx, n, j.restoreConfig(n), j.dbPath(n), out, t)
	if err == nil {
		o.TXID = res.TXID
		err = j.read(rctx, n, o, out, false)
	}
	j.endObs(o, err)
	j.keepIfBroken(ctx, n, o, out)
	_, _ = n.Exec(ctx, torx.Command("rm", "-f", out, out+"-wal", out+"-shm", out+"-txid"))
	return o
}

// read reads the copy of the database at db into o.
func (j *chaosJob) read(ctx context.Context, n *torx.Node, o *obsRec, db string, readonly bool) error {
	b, err := j.svc.Verify(ctx, n, db, readonly)
	if err != nil {
		return err
	}
	var r model.Report
	if err := json.Unmarshal(b, &r); err != nil {
		return fmt.Errorf("verify: %w", err)
	}
	o.Report = &r
	return nil
}

func (j *chaosJob) beginObs(kind string, n *torx.Node, t litestream.Target) *obsRec {
	e, _ := j.current()
	return &obsRec{Kind: kind, Target: t, Node: n.Name(), Epoch: e, Call: time.Now()}
}

func (j *chaosJob) endObs(o *obsRec, err error) {
	o.Return = time.Now()
	if err != nil {
		o.Err = err.Error()
	}
	o.Summary = summarize(o.Report)
	j.mu.Lock()
	o.ID = len(j.obs)
	j.obs = append(j.obs, o)
	j.mu.Unlock()
}

// keepIfBroken keeps a copy whose report says it is broken, or that
// Litestream restored but could not be read, for a person to examine.
func (j *chaosJob) keepIfBroken(ctx context.Context, n *torx.Node, o *obsRec, db string) {
	unreadable := o.Report == nil && o.TXID != ""
	if !unreadable && (o.Report == nil || o.Report.Intact()) {
		return
	}
	j.mu.Lock()
	if j.kept >= keepBroken {
		j.mu.Unlock()
		return
	}
	j.kept++
	j.mu.Unlock()
	name := fmt.Sprintf("broken-%d-%s.db", o.ID, o.Kind)
	kept := path.Join(j.dir(n), name)
	if res, err := n.Exec(ctx, torx.Command("cp", db, kept)); err == nil && res.ExitCode == 0 {
		j.svc.AddArtifact(n, torx.Artifact{Name: name, Path: kept})
		o.Kept = name
	}
}

// runFollowReader reads the follower's copy every so often, starting the
// follower once the replica exists.
func (j *chaosJob) runFollowReader(ctx context.Context, rng *rand.Rand) {
	for nemesis.Sleep(ctx, time.Duration(500+rng.IntN(2500))*time.Millisecond) {
		_, p := j.current()
		s := 1 - p
		if !j.svc.Running(followName(s)) {
			continue
		}
		j.readFollower(ctx, s)
	}
}

func (j *chaosJob) readFollower(ctx context.Context, s int) *obsRec {
	n := j.svc.Nodes()[s]
	o := j.beginObs(obsFollow, n, litestream.Target{})
	j.mu.Lock()
	o.Life = j.followLife
	j.mu.Unlock()
	rctx, cancel := context.WithTimeout(ctx, restoreTimeout)
	defer cancel()
	err := j.read(rctx, n, o, j.followDB(n), true)
	if b, terr := j.svc.Tail(rctx, followName(s)); terr == nil {
		o.FollowErrors = strings.Count(string(b), "follow: error applying updates")
	}
	j.endObs(o, err)
	return o
}

// startFollower starts a follower on node s, from nothing.
func (j *chaosJob) startFollower(ctx context.Context, s int) error {
	if !j.follow {
		return nil
	}
	n := j.svc.Nodes()[s]
	dir := path.Dir(j.followDB(n))
	if err := n.Rm(ctx, dir); err != nil {
		return err
	}
	if err := n.Mkdir(ctx, dir); err != nil {
		return err
	}
	j.mu.Lock()
	j.followLife++
	j.mu.Unlock()
	return j.svc.LaunchFollow(ctx, followName(s), n, j.followConfig(n), j.dbPath(n), j.followDB(n), j.ls.FollowInterval)
}

// supervise runs before each fault: it brings back what exited on its own,
// so each fault starts from a whole system, and starts the follower once
// there is a replica to follow.
func (j *chaosJob) supervise(ctx context.Context) {
	e, p := j.current()
	for _, name := range []string{appName(p), lsName(p)} {
		if j.svc.Node(name) != nil && !j.svc.Running(name) {
			j.jc.Log("info", "supervise: restarting "+name)
			if err := j.svc.Restart(ctx, name); err != nil {
				j.jc.Log("warn", fmt.Sprintf("supervise: %s: %v", name, err))
			}
			if name == appName(p) {
				_ = j.svc.WaitApp(ctx, name, time.Minute)
				_ = j.resolve(ctx, e, j.svc.AppURL(name))
			}
		}
	}
	s := 1 - p
	if j.follow && !j.svc.Running(followName(s)) {
		j.mu.Lock()
		acked := slices.ContainsFunc(j.acks, func(a ackRec) bool { return a.Err == "" })
		j.mu.Unlock()
		if acked {
			if err := j.startFollower(ctx, s); err != nil {
				j.jc.Log("warn", fmt.Sprintf("supervise: follower: %v", err))
			}
		}
	}
}

func (j *chaosJob) note(a anomaly) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.notes = append(j.notes, a)
}

// settle resumes what is paused and restarts what is down, and waits for
// the app.
func (j *chaosJob) settle(ctx context.Context) error {
	for _, name := range j.svc.Names() {
		if j.svc.Paused(name) {
			_ = j.svc.Resume(ctx, name)
		}
	}
	j.supervise(ctx)
	e, p := j.current()
	if err := j.svc.WaitApp(ctx, appName(p), time.Minute); err != nil {
		return err
	}
	return j.resolve(ctx, e, j.svc.AppURL(appName(p)))
}

// final has the replica catch up, restores it, and reads the primary's
// database and the follower's copy, which must all agree.
func (j *chaosJob) final(ctx context.Context) error {
	_, p := j.current()
	var err error
	for {
		if err = j.syncOnce(ctx); err == nil || ctx.Err() != nil {
			break
		}
		j.jc.Log("info", "final sync: "+err.Error())
		if !nemesis.Sleep(ctx, time.Second) {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("the replica did not catch up: %w", err)
	}
	pn, sn := j.svc.Nodes()[p], j.svc.Nodes()[1-p]
	po := j.beginObs(obsPrimary, pn, litestream.Target{})
	j.endObs(po, j.read(ctx, pn, po, j.dbPath(pn), true))
	j.restore(ctx, sn, obsFinal, litestream.Target{})
	if !j.follow || !j.svc.Running(followName(1-p)) || po.Report == nil {
		return nil
	}
	// The follower polls; give it time to apply the last of the replica.
	deadline := time.Now().Add(time.Minute)
	for {
		o := j.readFollower(ctx, 1-p)
		if o.Report != nil && len(o.Report.TxLog) >= len(po.Report.TxLog) {
			return nil
		}
		if time.Now().After(deadline) {
			n := -1
			if o.Report != nil {
				n = len(o.Report.TxLog)
			}
			j.note(anomaly{Kind: "follower-behind", Obs: []int{o.ID},
				Detail: fmt.Sprintf("a minute after the replica caught up, the follower holds %d of the primary's %d transactions", n, len(po.Report.TxLog))})
			return nil
		}
		if !nemesis.Sleep(ctx, time.Second) {
			return ctx.Err()
		}
	}
}
