//go:build unix

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/rqlite/qa/rqlite"
)

// Parameters of rqlite.chaos.
const (
	paramDuration = "duration" // seconds of faults and load
	paramClients  = "clients"  // concurrent clients
	paramFaults   = "faults"   // comma-separated fault names, or "all"
	paramTrial    = "trial"    // distinguishes repeated variants; each draws its own seed
	paramQueued   = "queued"   // whether clients also add through the write queue
	paramTolerate = "tolerate" // comma-separated anomaly kinds reported as warnings, not failures

	defaultDuration = 30
	defaultClients  = 6
	maxDuration     = 3600
	maxClients      = 64
)

const (
	opTimeout   = 5 * time.Second // one client operation, after which it is indeterminate
	readWindow  = 100             // a window read covers values within this many of the newest
	fullReadPct = 5               // percent of reads that read the whole table
	failBackoff = 50 * time.Millisecond
	// chaosStopGrace is how long a stopped node gets to exit. rqlited
	// normally exits within milliseconds, but a client's connection that has
	// not yet sent a request holds Go's HTTP server shutdown for five
	// seconds, and a load like this one leaves such connections about; a
	// stop past slowStop is a warning, one past the grace an error.
	chaosStopGrace = 15 * time.Second
	slowStop       = 5 * time.Second
	healTimeout    = 60 * time.Second
	finalTimeout   = 30 * time.Second
)

// chaosJob is rqlite.chaos: a randomized test of rqlite's guarantees under
// faults. Clients add unique values to a table and read them back at every
// consistency level, while a nemesis crashes, stops, and pauses nodes -- the
// leader more often than not -- and forces snapshots, reaps, and leader
// stepdowns. rqlite itself runs a configuration drawn at random, with
// snapshot thresholds low enough that the faults land mid-snapshot. At the end
// every fault is healed, and the history of every operation is checked
// (checkSet) against the final state, which every node's own copy must
// converge on. An rqlited that exits on its own, or does not exit cleanly
// when stopped, is an anomaly too.
//
// Every choice the job makes -- the configuration, the faults and their
// timing, each client's operations -- is drawn from the variant's seed, so a
// failure found under one seed is rerun with -seed and the same selection.
type chaosJob struct {
	torx.JobBase
	db       *rqlite.Service
	nodes    int
	duration time.Duration
	clients  int
	faults   []string
	queued   bool
	tolerate map[string]bool
	config   chaosConfig
}

// knownIssues are the anomalies rqlite v10.3.6 is known to produce under this
// job, which its compiled-in variant tolerates so that a run of the whole
// suite passes on a release with them; a -params run is strict unless it says
// otherwise. README.md describes each.
const knownIssues = "duplicate,shutdown-hang"

// Matrix is one short variant that tolerates the known issues: the chaos job
// as a smoke test. Hunting for bugs is a -params run with longer durations,
// many trials, and nothing tolerated.
func (*chaosJob) Matrix() []torx.Params {
	return []torx.Params{{paramDuration: 20, paramTolerate: knownIssues}}
}

func (*chaosJob) ResolveParams(p torx.Params) (torx.Params, error) {
	return resolveChaosParams(p)
}

func (j *chaosJob) Declare(jc *torx.JobContext) {
	j.nodes = jc.Params.Int(paramNodes, defaultNodes)
	j.duration = time.Duration(jc.Params.Int(paramDuration, defaultDuration)) * time.Second
	j.clients = jc.Params.Int(paramClients, defaultClients)
	j.queued = jc.Params.Bool(paramQueued, true)
	j.tolerate = map[string]bool{}
	for k := range strings.SplitSeq(jc.Params.String(paramTolerate, ""), ",") {
		if k != "" {
			j.tolerate[k] = true
		}
	}
	j.faults = strings.Split(jc.Params.String(paramFaults, "all"), ",")
	if len(j.faults) == 1 && j.faults[0] == "all" {
		j.faults = slices.Sorted(maps.Keys(faultWeights))
	}
	j.db = rqlite.New(serviceName, j.nodes)
	jc.Register(j.db)
}

// Setup draws the configuration rqlite runs with before starting it.
func (j *chaosJob) Setup(ctx context.Context, jc *torx.JobContext) error {
	j.config = drawConfig(jc.Rand("config"))
	jc.Log("info", "rqlited flags: "+strings.Join(j.config.flags(), " "))
	j.db.SetFlags(j.config.flags()...)
	j.db.SetStopGrace(chaosStopGrace)
	return j.JobBase.Setup(ctx, jc)
}

func (j *chaosJob) Run(ctx context.Context, jc *torx.JobContext) error {
	leader, err := j.db.AwaitLeader(ctx)
	if err != nil {
		return err
	}
	if _, err := j.db.Client(leader).Execute(ctx,
		rqlite.Stmt("CREATE TABLE s (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)")); err != nil {
		return err
	}

	h := NewHistory()
	var next atomic.Int64 // the last value handed out; values start at 1
	runCtx, stop := context.WithTimeout(ctx, j.duration)
	defer stop()

	var wg sync.WaitGroup
	for i := range j.clients {
		wg.Go(func() {
			j.client(runCtx, h, jc.Rand(fmt.Sprintf("client-%d", i)), fmt.Sprintf("client-%d", i), &next)
		})
	}
	nem := &nemesis{db: j.db, h: h, jc: jc, rng: jc.Rand("nemesis"), faults: j.faults}
	nem.run(runCtx)
	wg.Wait()

	// Heal everything, then read the final state and wait for every node's
	// own copy to converge on it.
	var anomalies []Anomaly
	anomalies = append(anomalies, nem.anomalies...)
	if err := nem.healAll(ctx); err != nil {
		return fmt.Errorf("healing the cluster after the faults: %w", err)
	}
	if err := j.flushQueues(ctx); err != nil {
		return err
	}
	final, err := j.finalRead(ctx)
	if err != nil {
		return err
	}
	anomalies = append(anomalies, j.converge(ctx, final)...)
	for _, e := range j.db.Exits() {
		anomalies = append(anomalies, Anomaly{Kind: "exit", Severity: sevError,
			Detail: fmt.Sprintf("rqlited on %s exited on its own with status %d at %s %s", e.Node, e.Code, e.Time.Format(time.RFC3339Nano), e.Err)})
	}
	ops := h.Ops()
	anomalies = append(anomalies, checkSet(ops, final)...)

	jc.WriteArtifact("history.ndjson", h.NDJSON())
	errs := 0
	byKind := map[string]int{}
	for i := range anomalies {
		a := &anomalies[i]
		if a.Severity == sevError && j.tolerate[a.Kind] {
			a.Severity, a.Detail = sevWarn, "(tolerated) "+a.Detail
		}
		byKind[a.Kind]++
		if a.Severity == sevError {
			errs++
		}
		jc.Log(a.Severity, fmt.Sprintf("%s: %s", a.Kind, a.Detail))
	}
	if len(anomalies) > 0 {
		b, _ := json.MarshalIndent(anomalies, "", "  ")
		jc.WriteArtifact("anomalies.json", b)
	}

	stats := tally(ops)
	if err := jc.Record(map[string]any{
		"config": j.config, "ops": stats, "final_values": len(final),
		"anomalies": byKind, "exits": j.db.Exits(),
	}); err != nil {
		return err
	}
	jc.SetSummary(fmt.Sprintf("%d faults, %d adds (%d ok, %d info), %d reads; %d values at the end; %d anomalies (%d errors)",
		stats["faults"], stats["add"], stats["add:ok"], stats["add:info"], stats["read"], len(final), len(anomalies), errs))
	if errs > 0 {
		var kinds []string
		for _, a := range anomalies {
			if a.Severity == sevError && !slices.Contains(kinds, a.Kind) {
				kinds = append(kinds, a.Kind)
			}
		}
		slices.Sort(kinds)
		return fmt.Errorf("%d anomalies: %s; see anomalies.json", errs, strings.Join(kinds, ", "))
	}
	return nil
}

// client issues operations until ctx is done: adds of fresh values and reads
// of the newest values, or now and then of all of them, each at a random node
// and consistency level.
func (j *chaosJob) client(ctx context.Context, h *History, rng *rand.Rand, name string, next *atomic.Int64) {
	nodes := j.db.Nodes()
	for ctx.Err() == nil {
		n := nodes[rng.IntN(len(nodes))]
		c := j.db.Client(n)
		op := Op{Process: name, Node: n.Name()}
		octx, cancel := context.WithTimeout(ctx, opTimeout)
		if rng.IntN(100) < 60 {
			op.F = "add"
			op.Value = next.Add(1)
			stmt := rqlite.Stmt("INSERT INTO s(v) VALUES(?)", op.Value)
			op.Start = h.Now()
			var err error
			if j.queued && rng.IntN(100) < 20 {
				op.Mode = "queued"
				err = c.ExecuteQueued(octx, true, stmt)
			} else {
				op.Mode = "execute"
				_, err = c.Execute(octx, stmt)
			}
			op.End = h.Now()
			op.Outcome = classify(err)
			if err != nil {
				op.Err = err.Error()
			}
		} else {
			op.F = "read"
			op.Mode = pickLevel(rng)
			stmt := rqlite.Stmt("SELECT v FROM s")
			if rng.IntN(100) >= fullReadPct {
				op.Lower = max(0, next.Load()-readWindow)
				stmt = rqlite.Stmt("SELECT v FROM s WHERE v > ?", op.Lower)
			}
			op.Start = h.Now()
			res, err := c.Query(octx, op.Mode, stmt)
			op.End = h.Now()
			if err == nil {
				op.Values, err = int64s(res)
			}
			if err != nil {
				op.Outcome, op.Err = Fail, err.Error() // a failed read changes nothing
			} else {
				op.Outcome = Ok
			}
		}
		cancel()
		if ctx.Err() != nil && op.Outcome != Ok && op.F == "read" {
			continue // cut short by the end of the run, not by the cluster
		}
		h.Add(op)
		if op.Outcome == Fail {
			sleep(ctx, failBackoff) // a node that is down refuses at once; do not spin on it
		}
	}
}

// pickLevel picks a read's consistency level, favoring the ones with a
// guarantee to check.
func pickLevel(rng *rand.Rand) string {
	switch p := rng.IntN(100); {
	case p < 45:
		return rqlite.LevelLinearizable
	case p < 70:
		return rqlite.LevelStrong
	case p < 90:
		return rqlite.LevelWeak
	default:
		return rqlite.LevelNone
	}
}

// int64s decodes a one-column result of integers.
func int64s(res rqlite.Result) ([]int64, error) {
	out := make([]int64, 0, len(res.Values))
	for _, row := range res.Values {
		if len(row) != 1 {
			return nil, fmt.Errorf("row has %d columns, want 1", len(row))
		}
		f, ok := row[0].(float64)
		if !ok {
			return nil, fmt.Errorf("value is %T, not a number", row[0])
		}
		out = append(out, int64(f))
	}
	return out, nil
}

// flushQueues waits until every node's write queue has committed what it
// holds. A queued write a client gave up on stays queued, and rqlite retries
// it until it commits, so without the flush it could land after the final
// read and look like a value the cluster invented. Each node's queue runs in
// order, so a waited-for empty write through it returns once everything
// queued before it has committed.
func (j *chaosJob) flushQueues(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, finalTimeout)
	defer cancel()
	for _, n := range j.db.Nodes() {
		var lastErr error
		err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
			lastErr = j.db.Client(n).ExecuteQueued(ctx, true)
			return lastErr == nil, nil
		}, 0)
		if err != nil {
			return fmt.Errorf("flushing %s's write queue: %w; last error: %w", n.Name(), err, lastErr)
		}
	}
	return nil
}

// finalRead reads the whole table at the strong level through the leader,
// retrying until the healed cluster answers.
func (j *chaosJob) finalRead(ctx context.Context) ([]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, finalTimeout)
	defer cancel()
	var final []int64
	var lastErr error
	err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		leader, err := j.db.Leader(ctx)
		if err != nil {
			lastErr = err
			return false, nil
		}
		res, err := j.db.Client(leader).Query(ctx, rqlite.LevelStrong, rqlite.Stmt("SELECT v FROM s ORDER BY v"))
		if err == nil {
			final, err = int64s(res)
		}
		lastErr = err
		return err == nil, nil
	}, 0)
	if err != nil {
		return nil, fmt.Errorf("final read: %w; last error: %w", err, lastErr)
	}
	return final, nil
}

// converge waits for every node's own copy to hold exactly the final values,
// and reports each node whose copy does not by the deadline.
func (j *chaosJob) converge(ctx context.Context, final []int64) []Anomaly {
	var mu sync.Mutex
	var out []Anomaly
	var wg sync.WaitGroup
	for _, n := range j.db.Nodes() {
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, finalTimeout)
			defer cancel()
			var got []int64
			var lastErr error
			err := torx.WaitUntil(cctx, func(ctx context.Context) (bool, error) {
				res, err := j.db.Client(n).Query(ctx, rqlite.LevelNone, rqlite.Stmt("SELECT v FROM s ORDER BY v"))
				if err == nil {
					got, err = int64s(res)
				}
				lastErr = err
				return err == nil && slices.Equal(got, final), nil
			}, 0)
			if err == nil {
				return
			}
			missing, extra := diff(final, got)
			a := anomaly("divergence", sevError, nil, append(missing, extra...),
				"%s's own copy did not converge on the final read: %d value(s) missing, %d extra, of %d (last error: %v)",
				n.Name(), len(missing), len(extra), len(final), lastErr)
			mu.Lock()
			out = append(out, a)
			mu.Unlock()
		})
	}
	wg.Wait()
	return out
}

// diff returns the values of want missing from got and those of got not in
// want, as multisets.
func diff(want, got []int64) (missing, extra []int64) {
	counts := map[int64]int{}
	for _, v := range want {
		counts[v]++
	}
	for _, v := range got {
		counts[v]--
	}
	for _, v := range slices.Sorted(maps.Keys(counts)) {
		for c := counts[v]; c > 0; c-- {
			missing = append(missing, v)
		}
		for c := counts[v]; c < 0; c++ {
			extra = append(extra, v)
		}
	}
	return missing, extra
}

// tally counts a history's operations by function and outcome.
func tally(ops []Op) map[string]int {
	t := map[string]int{}
	for _, op := range ops {
		if op.Process == "nemesis" {
			t["faults"]++
			continue
		}
		t[op.F]++
		t[op.F+":"+string(op.Outcome)]++
	}
	return t
}

// errNoTarget is a fault with no node it can apply to right now.
var errNoTarget = errors.New("no node to apply the fault to")
