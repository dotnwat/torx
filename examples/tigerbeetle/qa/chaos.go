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
	"strings"
	"sync"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/diskfault"
	"github.com/dotnwat/torx/examples/tigerbeetle/qa/tigerbeetle"
	"github.com/dotnwat/torx/netfault"
	"github.com/dotnwat/torx/resfault"
	tb "github.com/tigerbeetle/tigerbeetle-go"
)

// Parameters of tigerbeetle.chaos.
const (
	paramReplicas = "replicas" // cluster size
	paramDuration = "duration" // seconds of faults and load
	paramClients  = "clients"  // concurrent client sessions
	paramAccounts = "accounts" // accounts the transfers move between
	paramFaults   = "faults"   // comma-separated fault names, or "all" and exclusions
	paramConfig   = "config"   // "random": each replica starts with a configuration of its own; "default"
	paramTrial    = "trial"    // distinguishes repeated variants; each draws its own seed
	paramTolerate = "tolerate" // comma-separated anomaly kinds reported as warnings, not failures
	paramStandbys = "standbys" // standbys beside the replicas
	paramNemeses  = "nemeses"  // nemeses injecting faults at once, each one fault at a time
	paramStorage  = "storage"  // "tmpfs": data files on a limited tmpfs, for disk-full, where the nodes can take it; "disk": on the scratch's own disk, for slow-disk

	defaultReplicas = 3
	defaultDuration = 30
	defaultClients  = 6
	defaultAccounts = 12
	maxReplicas     = 6
	maxDuration     = 3600
	maxClients      = 48 // under the 64 sessions a cluster keeps, leaving room for the job's own
)

const (
	// dataLimit is the size of a replica's data directory when the nodes
	// can take disk faults: a data file starts at 1.06GiB, its write-ahead log
	// and client replies written out in full, and this leaves the grid room to
	// grow into for a run, and little enough to fill in a moment.
	dataLimit = 1400 << 20
	// settleTimeout bounds how long, once every fault is healed, the cluster
	// may take to answer again, and the clients' ops in flight to complete.
	settleTimeout = 120 * time.Second
	// lookupChunk is how many ids one lookup asks for: few enough that the
	// transfers found fit in a reply under the smallest request limit a
	// replica may run with.
	lookupChunk = 200
	// changesPage is how many change events one read of them asks for.
	changesPage = 2000
)

// knownIssues are the anomalies TigerBeetle 0.17.9 is known to produce
// under this job. README.md describes each.
const knownIssues = ""

// chaosJob is tigerbeetle.chaos: a randomized test of TigerBeetle's
// guarantees under faults. Clients move money between accounts in batches of
// transfers -- plain, pending, posting and voiding pending ones, balancing,
// linked into chains that stand or fall together, and some that must fail --
// resubmit transfers already sent, and read the accounts and look transfers
// up, while a nemesis crashes, pauses, reconfigures, and recovers replicas,
// fills their disks, and cuts and degrades the network between them. At the
// end every fault is healed and the whole history is replayed through a
// model of TigerBeetle's state machine in the cluster's own serial order --
// the order of the timestamps it assigned -- which every result, read, and
// the final state must agree with. A replica that exits on its own, except on
// a full disk, is an anomaly too.
type chaosJob struct {
	torx.JobBase
	db        *tigerbeetle.Service
	replicas  int
	duration  time.Duration
	clients   int
	accounts  int
	faults    []string
	allFaults bool
	random    bool
	disk      bool // storage=disk
	tolerate  map[string]bool
	diskErr   error
	configs   [][]string
}

// Matrix is one short variant: the chaos job as a smoke test. It leaves out
// the two faults that provoke findings already made (README.md), whose
// variants fail. Hunting for bugs is a -params run with longer durations and
// many trials.
func (*chaosJob) Matrix() []torx.Params {
	return []torx.Params{{paramDuration: 20, paramFaults: "all,-batch-limit,-corrupt-headers", paramTolerate: knownIssues}}
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
		paramReplicas: defaultReplicas, paramDuration: defaultDuration, paramClients: defaultClients,
		paramAccounts: defaultAccounts, paramFaults: "all", paramConfig: "random", paramTolerate: "",
		paramStorage: "tmpfs", paramStandbys: 0, paramNemeses: 1,
	}
	for k, v := range p {
		switch k {
		case paramReplicas, paramDuration, paramClients, paramAccounts, paramTrial, paramStandbys, paramNemeses:
			n, ok := intValue(v)
			bounds := map[string][2]int{
				paramReplicas: {1, maxReplicas}, paramDuration: {1, maxDuration},
				paramClients: {1, maxClients}, paramAccounts: {2, 1000}, paramTrial: {0, math.MaxInt32},
				paramStandbys: {0, 6}, paramNemeses: {1, 4},
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
		case paramStorage:
			if v != "tmpfs" && v != "disk" {
				return nil, fmt.Errorf("%s must be tmpfs or disk, got %v", k, v)
			}
			out[k] = v
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
	j.replicas = jc.Params.Int(paramReplicas, defaultReplicas)
	j.duration = time.Duration(jc.Params.Int(paramDuration, defaultDuration)) * time.Second
	j.clients = jc.Params.Int(paramClients, defaultClients)
	j.accounts = jc.Params.Int(paramAccounts, defaultAccounts)
	j.random = jc.Params.String(paramConfig, "random") == "random"
	j.disk = jc.Params.String(paramStorage, "tmpfs") == "disk"
	j.tolerate = map[string]bool{}
	for k := range strings.SplitSeq(jc.Params.String(paramTolerate, ""), ",") {
		if k != "" {
			j.tolerate[k] = true
		}
	}
	j.faults, j.allFaults = parseFaults(jc.Params.String(paramFaults, "all"))
	j.db = tigerbeetle.NewWithStandbys(serviceName, j.replicas, jc.Params.Int(paramStandbys, 0))
	jc.Register(j.db)
}

// Setup draws each replica's configuration and limits the data directories
// when the nodes can take disk faults, before the cluster starts.
func (j *chaosJob) Setup(ctx context.Context, jc *torx.JobContext) error {
	rng := jc.Rand("config")
	j.db.SetCluster(rng.Uint64())
	if !j.random {
		j.db.SetFlags("--cache-grid=128MiB")
	}
	j.configs = nil
	for i := range len(j.db.Nodes()) {
		var flags []string
		if j.random {
			flags = drawConfig(rng)
		}
		j.configs = append(j.configs, flags)
		j.db.SetReplicaFlags(i, flags...)
		jc.Log("info", fmt.Sprintf("replica %d flags: %s", i, strings.Join(flags, " ")))
	}
	j.diskErr = nil
	if j.disk {
		j.diskErr = errors.New("storage=disk keeps data files off a limited tmpfs")
	}
	for _, n := range j.db.Nodes() {
		if j.diskErr != nil {
			break
		}
		if err := diskfault.Check(ctx, n, n.Scratch().Root); err != nil {
			j.diskErr = err
		}
	}
	if j.diskErr == nil {
		j.db.SetDataLimit(dataLimit)
	}
	return j.JobBase.Setup(ctx, jc)
}

func (j *chaosJob) Run(ctx context.Context, jc *torx.JobContext) error {
	netOK := netfault.Check(ctx, j.db.Nodes())
	var resOK error
	for _, n := range j.db.Nodes() {
		if resOK = resfault.Check(n); resOK != nil {
			break
		}
	}
	// A slow disk needs the data files on a block device, which lifting the
	// throttle on each one, a harmless write, finds out.
	blockOK := resOK
	if blockOK == nil && !j.disk {
		blockOK = errors.New("slow-disk needs storage=disk")
	}
	for _, n := range j.db.Nodes() {
		if blockOK != nil {
			break
		}
		blockOK = resfault.ThrottleIO(n, j.db.DataDir(n), resfault.IOLimit{})
	}
	var faults []string
	for _, f := range j.faults {
		var why error
		switch {
		case netFaults[f]:
			why = netOK
		case diskFaults[f]:
			why = j.diskErr
		case resFaults[f]:
			why = resOK
		case blockFaults[f]:
			why = blockOK
		}
		switch {
		case why == nil:
			faults = append(faults, f)
		case !j.allFaults:
			return fmt.Errorf("fault %s needs nodes it can act on (run with -netns): %w", f, why)
		}
	}
	if netOK != nil {
		jc.Log("info", fmt.Sprintf("no network faults: %v", netOK))
	}
	if j.diskErr != nil {
		jc.Log("info", fmt.Sprintf("no disk faults: %v", j.diskErr))
	}
	if resOK != nil {
		jc.Log("info", fmt.Sprintf("no resource faults: %v", resOK))
	}
	if blockOK != nil {
		jc.Log("info", fmt.Sprintf("no slow disks: %v", blockOK))
	}
	if len(faults) == 0 {
		return errors.New("no faults to inject")
	}

	h := NewHistory()
	b := newBank(jc.Rand("bank"), j.accounts)
	setup, err := j.db.NewClient()
	if err != nil {
		return err
	}
	defer setup.Close()
	res, err := setup.CreateAccounts(b.newAccounts())
	if err != nil {
		return fmt.Errorf("creating accounts: %w", err)
	}
	var initial []acct
	for i, r := range res {
		if r.Status != tb.AccountCreated {
			return fmt.Errorf("creating account %d: %v", i+1, r.Status)
		}
		a := acctFrom(b.newAccounts()[i])
		a.timestamp = r.Timestamp
		initial = append(initial, a)
	}

	// Each nemesis draws from a stream of its own; the first is the one a
	// run with a single nemesis has always used.
	st := &faultState{full: map[string]bool{}, frozen: map[string]bool{}, down: map[string]int{}}
	var nemeses []*nemesis
	for i := range jc.Params.Int(paramNemeses, 1) {
		stream := "nemesis"
		if i > 0 {
			stream = fmt.Sprintf("nemesis-%d", i)
		}
		m := &nemesis{
			db: j.db, h: h, jc: jc, rng: jc.Rand(stream), faults: faults,
			net: netOK == nil, disk: j.diskErr == nil, res: resOK == nil, draw: drawConfig, st: st,
		}
		if !j.random {
			m.draw = func(*rand.Rand) []string { return nil }
		}
		nemeses = append(nemeses, m)
	}
	m := nemeses[0]
	runCtx, stop := context.WithTimeout(ctx, j.duration)
	defer stop()
	var wg sync.WaitGroup
	var clients []*client
	for i := range j.clients {
		c := &client{name: fmt.Sprintf("client-%d", i), process: i, connect: j.db.NewClient}
		clients = append(clients, c)
		rng := jc.Rand(c.name)
		wg.Go(func() { c.run(runCtx, b, h, rng) })
	}
	var nwg sync.WaitGroup
	for _, n := range nemeses {
		nwg.Go(func() { n.run(runCtx) })
	}
	<-runCtx.Done()
	nwg.Wait()

	var anomalies []Anomaly
	if err := m.healAll(ctx); err != nil {
		anomalies = append(anomalies, Anomaly{Kind: "heal-failed", Severity: sevError, Detail: err.Error()})
	}
	healed := h.Now()
	// Every op in flight must complete once the faults are healed.
	settled := make(chan struct{})
	go func() { wg.Wait(); close(settled) }()
	select {
	case <-settled:
	case <-time.After(settleTimeout):
		anomalies = append(anomalies, Anomaly{Kind: "stuck", Severity: sevError,
			Detail: fmt.Sprintf("client ops still in flight %v after every fault was healed", settleTimeout)})
		for _, c := range clients {
			c.Close()
		}
		<-settled
	}
	for _, c := range clients {
		c.Close()
	}
	jc.Log("info", fmt.Sprintf("clients settled %v after the heal", h.Now()-healed))

	final, err := j.finalState(ctx, b, h)
	if err != nil {
		anomalies = append(anomalies, Anomaly{Kind: "unavailable", Severity: sevError, Detail: err.Error()})
	}

	anomalies = append(anomalies, j.exitAnomalies(m, h)...)
	chk := &checker{ops: h.Ops(), initial: initial}
	if final != nil {
		chk.accounts, chk.transfers, chk.expiries = final.accounts, final.transfers, final.expiries
		anomalies = append(anomalies, chk.check()...)
	}

	jc.WriteArtifact("history.ndjson", h.NDJSON())
	report, _ := json.MarshalIndent(map[string]any{
		"anomalies": anomalies, "stats": chk.stats, "exits": j.db.Exits(), "configs": j.configs,
		"faults": faults,
	}, "", "  ")
	jc.WriteArtifact("check.json", report)
	var failed []string
	for i, a := range anomalies {
		if a.Severity == sevError && j.tolerate[a.Kind] {
			anomalies[i].Severity = sevWarn
		}
		jc.Log(string(anomalies[i].Severity), fmt.Sprintf("%s: %s", a.Kind, a.Detail))
		if anomalies[i].Severity == sevError {
			failed = append(failed, a.Kind)
		}
	}
	_ = jc.Record(map[string]any{"anomalies": anomalies, "stats": chk.stats})
	jc.SetSummary(chk.summary())
	if len(failed) > 0 {
		return fmt.Errorf("anomalies: %s", strings.Join(slices.Compact(slices.Sorted(slices.Values(failed))), ", "))
	}
	return nil
}

// finalState is what the cluster holds once every fault is healed: every
// account and every transfer submitted, read by a session of its own, which
// the cluster must answer within settleTimeout.
type finalState struct {
	accounts  []tb.Account
	transfers map[u128]tb.Transfer
	expiries  []expiry
}

func (j *chaosJob) finalState(ctx context.Context, b *bank, h *History) (*finalState, error) {
	c, err := j.db.NewClient()
	if err != nil {
		return nil, err
	}
	type reply struct {
		f   *finalState
		err error
	}
	done := make(chan reply, 1)
	go func() {
		f := &finalState{transfers: map[u128]tb.Transfer{}}
		var ids []tb.Uint128
		seen := map[tb.Uint128]bool{}
		for _, op := range h.Ops() {
			for _, e := range op.Events {
				if !seen[e.ID] {
					seen[e.ID] = true
					ids = append(ids, e.ID)
				}
			}
		}
		for chunk := range slices.Chunk(ids, lookupChunk) {
			ts, err := c.LookupTransfers(chunk)
			if err != nil {
				done <- reply{nil, err}
				return
			}
			for _, t := range ts {
				f.transfers[fromTB(t.ID)] = t
			}
		}
		// The expiries, which the cluster runs on its own, are in the change
		// events, read a page at a time. They go on after the clients stop,
		// until the last pending transfer times out, so the accounts are read
		// between two reads of the change events that find the same ones:
		// the model then knows every expiry the accounts show.
		var after uint64
		expiries := func() error {
			for {
				page, err := c.GetChangeEvents(tb.ChangeEventsFilter{TimestampMin: after + 1, Limit: changesPage})
				if err != nil {
					return err
				}
				for _, e := range page {
					if e.Type == tb.ChangeEventTwoPhaseExpired {
						f.expiries = append(f.expiries, expiry{ts: e.Timestamp, id: fromTB(e.TransferID)})
					}
					after = e.Timestamp
				}
				if len(page) < changesPage {
					return nil
				}
			}
		}
		if err := expiries(); err != nil {
			done <- reply{nil, err}
			return
		}
		for {
			before := after
			accts, err := c.LookupAccounts(b.allIDs())
			if err != nil {
				done <- reply{nil, err}
				return
			}
			if err := expiries(); err != nil {
				done <- reply{nil, err}
				return
			}
			if after == before {
				f.accounts = accts
				break
			}
		}
		done <- reply{f, nil}
	}()
	select {
	case r := <-done:
		c.Close()
		return r.f, r.err
	case <-time.After(settleTimeout):
		c.Close()
		<-done
		return nil, fmt.Errorf("the cluster did not answer the final reads within %v of the heal", settleTimeout)
	case <-ctx.Done():
		c.Close()
		<-done
		return nil, ctx.Err()
	}
}

// exitAnomalies reports the replicas that exited on their own. An exit on
// a full disk, while the nemesis had filled it, is TigerBeetle stopping as
// designed; any other is a crash, and what the replica said last is quoted.
func (j *chaosJob) exitAnomalies(m *nemesis, h *History) []Anomaly {
	var out []Anomaly
	for _, e := range j.db.Exits() {
		if e.Code == tigerbeetle.ExitNoSpaceLeft {
			continue
		}
		kind := "replica-crash"
		if e.Fatal() {
			kind = "replica-fatal"
		}
		out = append(out, Anomaly{Kind: kind, Severity: sevError,
			Detail: fmt.Sprintf("%s (replica %d) exited with status %d at %s: %s", e.Node, e.Replica, e.Code, e.Time.Format(time.RFC3339Nano), e.Said)})
	}
	return out
}
