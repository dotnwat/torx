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
	"strings"
	"sync"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/diskfault"
	"github.com/dotnwat/torx/examples/kudu/qa/kudu"
	"github.com/dotnwat/torx/nemesis"
	"github.com/dotnwat/torx/netfault"
	"github.com/dotnwat/torx/resfault"
)

// Parameters of kudu.chaos.
const (
	paramDuration  = "duration"      // seconds of faults and load
	paramClients   = "clients"       // concurrent clients
	paramKeys      = "keys"          // keys the clients contend on
	paramNodes     = "nodes"         // server nodes: a tablet server on each
	paramMasters   = "masters"       // masters, on the first nodes
	paramReplicas  = "replicas"      // the table's replication factor
	paramBuckets   = "buckets"       // hash buckets in each of the table's three ranges
	paramDataDirs  = "data-dirs"     // data directories per tablet server
	paramFaults    = "faults"        // comma-separated fault names, or "all" and exclusions ("all,-pause")
	paramConfig    = "config"        // "random": flags drawn from the seed; "default": Kudu's
	paramTxns      = "txns"          // enable transactions, and have clients run them
	paramAI        = "ai"            // have clients work a table of a non-unique key too, whose ids Kudu assigns
	paramJVMs      = "jvms"          // kudu-nodes the clients spread over
	paramValueSize = "value-size"    // the largest payload written, in bytes
	paramTimeout   = "timeout"       // each client operation's timeout, in milliseconds
	paramDiskSize  = "disk-size"     // MiB of each tablet server's size-limited directories, for disk-full
	paramNemeses   = "nemeses"       // nemeses injecting faults at once
	paramTSFlags   = "tserver-flags" // comma-separated flag=value pinned on every tablet server, over the drawn ones
	paramMFlags    = "master-flags"  // the same for every master
	paramColumns   = "columns"       // the table's value encoding, payload encoding, and compression, comma-separated, over the drawn ones
	paramAllow     = "allow"         // comma-separated known issues reported as warnings (README.md)
	paramTrial     = "trial"         // distinguishes repeated variants; each draws its own seed

	defaultDuration = 60
)

const (
	tableName = "kv"
	// settleTimeout bounds how long, once every fault is healed, the
	// cluster may take to be healthy again.
	settleTimeout = 4 * time.Minute
	// checkBudget bounds the search of each key's history.
	checkBudget = 30 * time.Second
)

// faultWeights are the faults the nemesis draws from, by name, with their
// weights. Network faults need -netns, disk-full needs a node that can
// mount, freeze needs -cgroups; a run without them leaves them out.
var faultWeights = map[string]int{
	"crash-tserver":    4, // kill a tablet server, restart it after the hold
	"crash-tservers":   1, // kill two tablet servers at once
	"restart-tserver":  2, // SIGTERM a tablet server and start it again
	"pause-tserver":    3, // SIGSTOP a tablet server
	"crash-master":     2, // kill a master
	"pause-master":     1, // SIGSTOP a master
	"partition-one":    3, // cut one server node off from every other node
	"partition-halves": 2, // split the nodes in two
	"partition-client": 1, // cut the clients off from some servers
	"step-down":        3, // have a tablet's leader step down, abruptly or not
	"move-replica":     2, // move a tablet's replica to another tablet server
	"eio":              2, // fail some I/O to one of a tablet server's data directories, then restart it
	"disk-full-wal":    1, // fill a tablet server's WAL directory
	"disk-full-data":   1, // fill one of a tablet server's data directories
	"freeze":           2, // freeze every process of a server node
}

// knownIssues are issues of Kudu a run can allow, so that a run that keeps
// finding one can find others. README.md describes each. An issue the
// checker cannot tell apart from others is sidestepped instead: allowing
// it changes the configuration so it does not happen.
var knownIssues = map[string]bool{
	// The deleted-rowset GC deletes a fully deleted rowset whose deletes
	// are not ancient when it races a DeltaMemStore flush, which takes more
	// than one maintenance thread; snapshot and diff scans lose the rows.
	// Allowed, the tablet servers run with the GC off.
	"deleted-rowset-gc": true,
	// Kudu has no leader leases: a leader deposed by a partition, a freeze,
	// or a pause serves snapshot reads at "now" from what it has, and Kudu
	// documents that such reads may not be repeatable (KUDU-430,
	// KUDU-1188). The checker tells these apart: a key whose history only
	// such reads break.
	"stale-read": true,
	// A tablet server crashes when a data directory fills between the
	// checks of its free space, which are cached, though another of the
	// tablet's directories has room: the flush that hits ENOSPC is not
	// retried elsewhere, and its failure is fatal (KUDU-2404).
	"exit-under-disk-full-data": true,
	// A tablet server's bootstrap replays committed inserts into a table of
	// a non-unique key from a counter it takes from the rows on disk, not
	// the one the leader logged with each write, so after the row of the
	// highest id was deleted a restart renumbers rows, and replicas
	// disagree. Allowed, the anomalies of aiTable it explains are warnings.
	"auto-id-restart": true,
}

// issueKinds are the kinds of anomaly an allowed issue covers, for an issue
// that is not a kind of its own.
var issueKinds = map[string][]string{
	"auto-id-restart": {"auto-id-changed", "auto-id-reused", "auto-id-order", "ai-timestamp-order", "replica-divergence-ai"},
}

// offByDefault are faults "all" leaves out; a run names them to inject
// them.
var offByDefault = map[string]bool{}

// chaosJob is kudu.chaos: a randomized test of Kudu's guarantees under
// faults. Clients write a few contended keys with every kind of row
// operation, insert keys of their own, and run transactions, and read in
// every read mode and at snapshots in the past, while a nemesis crashes,
// pauses, partitions, and freezes servers, fails and fills their disks, and
// moves leaders and replicas. Kudu gives each write a timestamp and each
// snapshot read one, and promises a snapshot at t holds exactly the writes
// before t, on every replica, for as long as the history is kept: so every
// key's history, ordered by Kudu's own timestamps, must be a legal
// sequence of row operations; transactions must be all or nothing in every
// snapshot; and once every fault is healed, every replica must agree.
type chaosJob struct {
	torx.JobBase
	cluster *kudu.Cluster
	client  *kudu.ClientNode

	duration  time.Duration
	clients   int
	keys      int
	nodes     int
	masters   int
	replicas  int
	buckets   int
	dataDirs  int
	faults    []string
	random    bool
	txns      bool
	ai        bool
	jvms      int
	valueSize int
	timeout   time.Duration
	diskSize  int64
	nemeses   int
	allow     map[string]bool

	flags map[string]map[string]string // role -> flags drawn
	table kudu.Table

	mu     sync.Mutex
	events []nemesis.Event
	notes  []anomaly
	holds  nemesis.Holds
	// diskFaults are each server's disk faults, while and since they were
	// held: a server that exits under one may have had cause to.
	diskFaults map[string][]diskWindow
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
	}
	return faults
}

func (*chaosJob) ResolveParams(p torx.Params) (torx.Params, error) {
	out := torx.Params{
		paramDuration: defaultDuration, paramClients: 8, paramKeys: 24, paramNodes: 5, paramMasters: 3,
		paramReplicas: 3, paramBuckets: 3, paramDataDirs: 2, paramFaults: "all", paramConfig: "random",
		paramTxns: true, paramAI: true, paramJVMs: 2, paramValueSize: 512, paramTimeout: 10_000, paramDiskSize: 1024,
		paramNemeses: 1, paramAllow: "deleted-rowset-gc,stale-read,exit-under-disk-full-data,auto-id-restart", paramTSFlags: "", paramMFlags: "", paramColumns: "",
	}
	for k, v := range p {
		switch k {
		case paramDuration, paramClients, paramKeys, paramNodes, paramMasters, paramReplicas, paramBuckets,
			paramDataDirs, paramJVMs, paramValueSize, paramTimeout, paramDiskSize, paramNemeses, paramTrial:
			n, ok := intValue(v)
			bounds := map[string][2]int{
				paramDuration: {1, 7200}, paramClients: {1, 64}, paramKeys: {1, 10_000}, paramNodes: {1, 16},
				paramMasters: {1, 5}, paramReplicas: {1, 7}, paramBuckets: {1, 16}, paramDataDirs: {1, 4},
				paramJVMs: {1, 8}, paramValueSize: {0, 1 << 20}, paramTimeout: {500, 600_000},
				paramDiskSize: {16, 8192}, paramNemeses: {1, 4}, paramTrial: {0, math.MaxInt32},
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
		case paramTSFlags, paramMFlags:
			str, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a string, got %v", k, v)
			}
			if _, err := parseFlags(str); err != nil {
				return nil, fmt.Errorf("%s: %w", k, err)
			}
			out[k] = str
		case paramColumns:
			str, ok := v.(string)
			if !ok || (str != "" && len(strings.Split(str, ",")) != 3) {
				return nil, fmt.Errorf("%s must be \"VALUE_ENCODING,PAYLOAD_ENCODING,COMPRESSION\", got %v", k, v)
			}
			out[k] = str
		case paramConfig:
			if v != "random" && v != "default" {
				return nil, fmt.Errorf("%s must be random or default, got %v", k, v)
			}
			out[k] = v
		case paramTxns, paramAI:
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("%s must be a boolean, got %v", k, v)
			}
			out[k] = b
		default:
			return nil, fmt.Errorf("unknown parameter %q", k)
		}
	}
	m, _ := intValue(out[paramMasters])
	n, _ := intValue(out[paramNodes])
	r, _ := intValue(out[paramReplicas])
	if m > n {
		return nil, fmt.Errorf("%d masters need as many nodes, have %d", m, n)
	}
	if r > n {
		return nil, fmt.Errorf("%d replicas need as many tablet servers, have %d", r, n)
	}
	return out, nil
}

// parseFlags reads "flag=value,flag=value" into a map.
func parseFlags(s string) (map[string]string, error) {
	out := map[string]string{}
	for kv := range strings.SplitSeq(s, ",") {
		if kv == "" {
			continue
		}
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("%q is not flag=value", kv)
		}
		out[strings.TrimLeft(k, "-")] = v
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
	j.keys = p.Int(paramKeys, 24)
	j.nodes = p.Int(paramNodes, 5)
	j.masters = p.Int(paramMasters, 3)
	j.replicas = p.Int(paramReplicas, 3)
	j.buckets = p.Int(paramBuckets, 3)
	j.dataDirs = p.Int(paramDataDirs, 2)
	j.faults = parseFaults(p.String(paramFaults, "all"))
	j.random = p.String(paramConfig, "random") == "random"
	j.txns = p.Bool(paramTxns, true)
	j.ai = p.Bool(paramAI, true)
	j.jvms = p.Int(paramJVMs, 2)
	j.valueSize = p.Int(paramValueSize, 512)
	j.timeout = time.Duration(p.Int(paramTimeout, 10_000)) * time.Millisecond
	j.diskSize = int64(p.Int(paramDiskSize, 1024)) << 20
	j.nemeses = p.Int(paramNemeses, 1)
	j.allow = map[string]bool{}
	for a := range strings.SplitSeq(p.String(paramAllow, "deleted-rowset-gc,stale-read,exit-under-disk-full-data,auto-id-restart"), ",") {
		if a != "" {
			j.allow[a] = true
			for _, k := range issueKinds[a] {
				j.allow[k] = true
			}
		}
	}
	j.drawConfig(jc.Rand("config"))
	if j.allow["deleted-rowset-gc"] {
		j.flags[kudu.TServer]["enable_deleted_rowset_gc"] = "false"
	}
	for role, param := range map[string]string{kudu.TServer: paramTSFlags, kudu.Master: paramMFlags} {
		pinned, _ := parseFlags(p.String(param, ""))
		maps.Copy(j.flags[role], pinned)
	}
	if c := p.String(paramColumns, ""); c != "" {
		parts := strings.Split(c, ",")
		j.table.ValueEncoding, j.table.PayloadEncoding, j.table.Compression = parts[0], parts[1], parts[2]
	}
	j.diskFaults = map[string][]diskWindow{}
	cfg := kudu.Config{
		Masters: j.masters, DataDirs: j.dataDirs, Flags: j.flags["common"],
		MasterFlags: j.flags[kudu.Master], TServerFlags: j.flags[kudu.TServer],
		Prepare: j.prepare,
	}
	j.cluster = kudu.New(clusterName, j.nodes, cfg)
	j.client = kudu.NewClientNode(clientName, j.cluster, j.jvms)
	jc.Register(j.cluster)
	jc.Register(j.client)
}

// drawConfig draws the servers' flags and the table's options: small
// flush thresholds and log segments, so that tablets flush, compact, and
// collect their logs while the clients load them, and followers that fall
// behind need copying; and which encodings and compression the columns
// use. With config=default, only what the job needs.
func (j *chaosJob) drawConfig(rng *rand.Rand) {
	ts := map[string]string{
		// Rereplicate a replica that has been gone a while, so a crashed
		// tablet server's tablets move.
		"follower_unavailable_considered_failed_sec": "20",
	}
	master := map[string]string{
		"tserver_unresponsive_timeout_ms": "10000",
	}
	if j.txns {
		master["txn_manager_enabled"] = "true"
		ts["enable_txn_system_client_init"] = "true"
	}
	common := map[string]string{}
	j.table = kudu.Table{Name: tableName, HashBuckets: j.buckets, RangeSplits: []int64{freshBase, txnBase}, Replicas: j.replicas}
	if j.random {
		pick := func(xs ...string) string { return xs[rng.IntN(len(xs))] }
		ts["flush_threshold_mb"] = pick("1", "4", "64")
		ts["flush_threshold_secs"] = pick("1", "2", "10", "60")
		ts["log_segment_size_mb"] = pick("1", "2", "8")
		ts["log_max_segments_to_retain"] = pick("3", "10", "80")
		ts["maintenance_manager_num_threads"] = pick("1", "2", "4")
		ts["maintenance_manager_polling_interval_ms"] = pick("10", "50", "250")
		ts["budgeted_compaction_target_rowset_size"] = pick("16384", "262144", "33554432")
		ts["cfile_default_block_size"] = pick("1024", "262144")
		ts["deltafile_default_block_size"] = pick("1024", "32768")
		ts["raft_heartbeat_interval_ms"] = pick("100", "250", "500")
		ts["follower_unavailable_considered_failed_sec"] = pick("10", "20", "60")
		// Masters and tablet servers must agree on how to replace a replica.
		common["raft_prepare_replacement_before_eviction"] = pick("true", "false")
		ts["block_manager"] = pick("log", "file")
		ts["tablet_history_max_age_sec"] = pick("60", "300", "900")
		ts["undo_delta_block_gc_init_budget_millis"] = pick("10", "1000")
		master["auto_rebalancing_enabled"] = pick("true", "false")
		if master["auto_rebalancing_enabled"] == "true" {
			// The rebalancer adds a replica before it removes one.
			common["raft_prepare_replacement_before_eviction"] = "true"
			master["auto_rebalancing_interval_seconds"] = "5"
		}
		master["raft_heartbeat_interval_ms"] = pick("100", "500")
		j.table.ValueEncoding = pick("AUTO_ENCODING", "PLAIN_ENCODING", "PREFIX_ENCODING", "DICT_ENCODING")
		j.table.PayloadEncoding = pick("AUTO_ENCODING", "PLAIN_ENCODING", "PREFIX_ENCODING", "DICT_ENCODING")
		j.table.Compression = pick("DEFAULT_COMPRESSION", "NO_COMPRESSION", "SNAPPY", "LZ4", "ZLIB")
	}
	j.flags = map[string]map[string]string{kudu.TServer: ts, kudu.Master: master, "common": common}
}

// prepare mounts a size-limited filesystem over a tablet server's WAL
// directory and its data directories, if the run fills disks.
func (j *chaosJob) prepare(ctx context.Context, name string, n *torx.Node) error {
	if !strings.HasPrefix(name, kudu.TServer) || (!j.faulting("disk-full-wal") && !j.faulting("disk-full-data")) {
		return nil
	}
	for _, d := range append([]string{j.cluster.WALDir(name)}, j.cluster.DataDirs(name)...) {
		if err := j.cluster.Limit(ctx, name, d, j.diskSize); err != nil {
			return err
		}
	}
	return nil
}

func (j *chaosJob) faulting(f string) bool { return slices.Contains(j.faults, f) }

func (j *chaosJob) Setup(ctx context.Context, jc *torx.JobContext) error {
	// Faults the nodes cannot take are left out, before anything starts:
	// prepare mounts disks only for a run that fills them.
	var nodes []*torx.Node
	nodes = append(nodes, j.cluster.Nodes()...)
	nodes = append(nodes, j.client.Nodes()...)
	drop := func(why string, fs ...string) {
		for _, f := range fs {
			if j.faulting(f) {
				torx.Logf(ctx, "warn", "leaving out %s: %s", f, why)
				j.faults = slices.DeleteFunc(j.faults, func(x string) bool { return x == f })
			}
		}
	}
	if err := netfault.Check(ctx, nodes); err != nil {
		drop(err.Error(), "partition-one", "partition-halves", "partition-client")
	}
	if err := diskfault.Check(ctx, nodes[0], nodes[0].Scratch().Root); err != nil {
		drop(err.Error(), "disk-full-wal", "disk-full-data")
	}
	if err := resfault.Check(nodes[0]); err != nil {
		drop(err.Error(), "freeze")
	}
	if j.masters < 2 {
		drop("one master", "crash-master", "pause-master")
	}
	return j.JobBase.Setup(ctx, jc)
}

func (j *chaosJob) Run(ctx context.Context, jc *torx.JobContext) error {
	admin := kudu.NewClient(j.client.URL(), "admin")
	if err := waitTServers(ctx, j.cluster, j.client.Node(), j.nodes); err != nil {
		return err
	}
	res, err := admin.CreateTable(ctx, j.table, 2*time.Minute)
	if err != nil {
		return err
	}
	if err := res.Err(); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	if j.ai {
		res, err := admin.CreateTable(ctx, kudu.Table{Name: aiTable, NonUnique: true, HashBuckets: 2, Replicas: j.replicas}, 2*time.Minute)
		if err != nil {
			return err
		}
		if err := res.Err(); err != nil {
			return fmt.Errorf("create table %s: %w", aiTable, err)
		}
	}
	torx.Logf(ctx, "info", "config: common %v; tserver %v; master %v; table %+v; faults %v", j.flags["common"], j.flags[kudu.TServer], j.flags[kudu.Master], j.table, j.faults)

	urls := j.client.URLs()
	w := &workload{
		keys: j.keys, valueSize: j.valueSize, table: tableName, timeout: j.timeout, txns: j.txns,
		weights: opWeights, seq: make([]int64, j.clients), rng: jc.Rand("workload"),
		ai: j.ai, aiSeen: make([][]row, j.clients),
	}
	for c := range j.clients {
		w.clients = append(w.clients, kudu.NewClient(urls[c%len(urls)], fmt.Sprintf("c%d", c)))
	}

	loadCtx, stopLoad := context.WithTimeout(ctx, j.duration)
	defer stopLoad()
	var wg sync.WaitGroup
	for c := range j.clients {
		rng := jc.Rand(fmt.Sprintf("client-%d", c))
		wg.Go(func() { w.run(loadCtx, c, rng) })
	}
	for i := range j.nemeses {
		nem := j.nemesis(jc.Rand(fmt.Sprintf("nemesis-%d", i)), admin)
		if len(nem.Faults) > 0 {
			wg.Go(func() { nem.Run(loadCtx) })
		}
	}
	wg.Wait()

	// Heal what the nemesis could not, and bring every server back.
	j.healAll(ctx)
	health, healthy := j.settle(ctx)
	if !healthy {
		j.note(anomaly{Kind: "no-recovery", Detail: fmt.Sprintf("the cluster was not healthy %v after every fault healed:\n%s", settleTimeout, lastLines(health, 40))})
	}

	// Read everything once more, through a client that has propagated
	// nothing, and then through every kudu-node at the same snapshot,
	// which reads other replicas.
	final := -1
	finalClients := []*kudu.Client{}
	for i, u := range urls {
		finalClients = append(finalClients, kudu.NewClient(u, fmt.Sprintf("final-%d", i)))
	}
	fw := &workload{keys: j.keys, valueSize: j.valueSize, table: tableName, timeout: time.Minute, clients: finalClients, seq: make([]int64, len(finalClients))}
	var finalTS int64
	for attempt := range 5 {
		o := fw.read(ctx, 0, kudu.ReadReq{Mode: "snapshot", FaultTolerant: true}, nil)
		if o.NodeErr == "" && o.Code == "" && o.SnapshotTS > 0 {
			finalTS = o.SnapshotTS
			break
		}
		torx.Logf(ctx, "warn", "final read %d failed: %s %s %s", attempt, o.Code, o.Error, o.NodeErr)
		time.Sleep(5 * time.Second)
	}
	if finalTS > 0 {
		for c := range finalClients {
			fw.read(ctx, c, kudu.ReadReq{Mode: "snapshot", TS: finalTS + 1, Replica: "closest"}, nil)
			if j.ai {
				fw.read(ctx, c, kudu.ReadReq{Table: aiTable, Mode: "snapshot", TS: finalTS + 1, Replica: "closest"}, nil)
			}
		}
	} else {
		j.note(anomaly{Kind: "no-final-read", Detail: "the cluster could not be read after every fault healed"})
	}

	// Every replica of every tablet must hold the same rows.
	if healthy {
		out, err := kudu.Tool(ctx, j.client.Node(), "cluster", "ksck", j.cluster.MasterAddrs(), "-checksum_scan", "-checksum_timeout_sec=300")
		if err != nil {
			kind := "ksck-failed"
			if bad := mismatched(out); len(bad) > 0 {
				kind = "replica-divergence"
				if j.ai && j.onlyTable(ctx, admin, aiTable, bad) {
					kind = "replica-divergence-ai"
				}
			}
			j.note(anomaly{Kind: kind, Detail: fmt.Sprintf("%v\n%s", err, lastLines(out, 60))})
		}
	}

	// The history the checker reads: the clients', then the final reads.
	hist := w.history()
	for _, o := range fw.history() {
		o.Client += j.clients
		o.Index = len(hist)
		if final < 0 && o.SnapshotTS == finalTS && finalTS > 0 {
			final = o.Index
		}
		hist = append(hist, o)
	}
	return j.check(ctx, jc, hist, final)
}

// check runs every check of the history, writes the history and what the
// checks found to the results, and fails the job on anything found that
// the run does not allow.
func (j *chaosJob) check(ctx context.Context, jc *torx.JobContext, hist []op, final int) error {
	var bad []anomaly
	byKey, noTS := keyHistories(hist)
	bad = append(bad, noTS...)
	kv, unknown := checkKeys(ctx, byKey, checkBudget)
	bad = append(bad, kv...)
	bad = append(bad, opChecks(hist, possiblyWritten(hist))...)
	bad = append(bad, txnCheck(hist, final)...)
	bad = append(bad, diffCheck(hist)...)
	if j.ai {
		keys := make([]int64, aiKeys)
		for i := range keys {
			keys[i] = int64(i)
		}
		tablets, err := kudu.NewClient(j.client.URL(), "layout").Partition(ctx, aiTable, keys)
		if err != nil {
			j.note(anomaly{Kind: "no-layout", Detail: fmt.Sprintf("locating %s's tablets: %v", aiTable, err)})
		}
		bad = append(bad, aiCheck(ctx, hist, tablets, checkBudget)...)
	}
	j.mu.Lock()
	bad = append(bad, j.notes...)
	events := slices.Clone(j.events)
	j.mu.Unlock()
	bad = append(bad, j.exitAnomalies()...)
	bad = j.staleReads(ctx, hist, events, bad)

	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	for _, o := range hist {
		_ = enc.Encode(o)
	}
	jc.WriteArtifact("history.ndjson", b.Bytes())
	b.Reset()
	for _, e := range events {
		_ = enc.Encode(map[string]any{"fault": e.Fault, "heal": e.Heal, "target": e.Target, "start": e.Start, "end": e.End, "error": errString(e.Err)})
	}
	jc.WriteArtifact("nemesis.ndjson", b.Bytes())
	if out, err := json.MarshalIndent(map[string]any{
		"anomalies": bad, "unknown_keys": unknown, "exits": j.cluster.Exits(), "disks": j.diskUsage(ctx),
		"layout": j.layout(ctx),
	}, "", "  "); err == nil {
		jc.WriteArtifact("check.json", out)
	}

	counts := map[string]int{}
	for _, o := range hist {
		counts[o.Kind]++
	}
	var failing []string
	for _, a := range bad {
		if j.allow[a.Kind] {
			torx.Logf(ctx, "warn", "allowed: %s", a)
			continue
		}
		failing = append(failing, a.String())
	}
	jc.Record(map[string]any{"ops": counts, "anomalies": len(bad), "unknown_keys": len(unknown), "faults": len(events)})
	summary := fmt.Sprintf("%d ops (%d writes, %d reads), %d fault events, %d keys checked (%d undecided)",
		len(hist), counts["write"], counts["read"], len(events), len(byKey), len(unknown))
	if len(failing) > 0 {
		jc.SetSummary(fmt.Sprintf("%d anomalies; %s", len(failing), summary))
		if len(failing) > 20 {
			failing = append(failing[:20], fmt.Sprintf("... and %d more (check.json)", len(failing)-20))
		}
		return fmt.Errorf("%d anomalies:\n%s", len(bad), strings.Join(failing, "\n"))
	}
	jc.SetSummary(summary)
	return nil
}

// silencing are the faults that can leave a server a deposed leader that
// does not know it: it stops hearing from its peers, or they from it.
var silencing = map[string]bool{
	"pause-tserver": true, "freeze": true, "partition-one": true, "partition-halves": true, "partition-client": true,
}

// staleReads reclassifies an anomaly of a transaction's snapshot as the
// known issue of stale reads (KUDU-430, KUDU-1188) when its read was at a
// snapshot the server chose and a server that served it was cut off or
// stopped while the read was in flight: a deposed leader serving what it
// has.
func (j *chaosJob) staleReads(ctx context.Context, hist []op, events []nemesis.Event, bad []anomaly) []anomaly {
	type window struct {
		target     string
		start, end time.Time
	}
	var windows []window
	open := map[string]int{}
	for _, e := range events {
		if !silencing[e.Fault] {
			continue
		}
		k := e.Fault + "|" + e.Target
		if !e.Heal {
			open[k] = len(windows)
			windows = append(windows, window{e.Target, e.Start, time.Time{}})
		} else if i, ok := open[k]; ok {
			windows[i].end = e.End
			delete(open, k)
		}
	}
	names := map[string]string{}
	if uuids, err := j.tabletServers(ctx); err == nil {
		for addr, uuid := range uuids {
			names[uuid] = j.cluster.ByAddr(addr)
		}
	}
	silenced := func(server string, from, to time.Time) bool {
		n := j.cluster.Node(server)
		for _, w := range windows {
			end := w.end
			if end.IsZero() {
				end = to
			}
			if (strings.Contains(w.target, server) || (n != nil && strings.Contains(w.target, n.Name()))) &&
				!w.start.After(to) && !end.Before(from) {
				return true
			}
		}
		return false
	}
	for i, a := range bad {
		if !strings.HasPrefix(a.Kind, "txn-") || len(a.Ops) == 0 {
			continue
		}
		o := hist[a.Ops[len(a.Ops)-1]]
		if o.Kind != "read" || o.Read == nil || o.Read.TS != 0 {
			continue
		}
		for _, uuid := range o.Servers {
			if silenced(names[uuid], time.Unix(0, o.Call), time.Unix(0, o.Return)) {
				bad[i].Kind = "stale-read"
				bad[i].Detail = fmt.Sprintf("(%s, at a snapshot %s chose while cut off or stopped) %s", a.Kind, names[uuid], a.Detail)
				break
			}
		}
	}
	return bad
}

// mismatched are the tablets whose replicas ksck's checksum scan found to
// disagree: those with more than one checksum among their replicas' lines,
// "T <tablet> P <replica> (<addr>): Checksum: <sum>".
func mismatched(ksck string) []string {
	sums := map[string]map[string]bool{}
	for line := range strings.SplitSeq(ksck, "\n") {
		f := strings.Fields(line)
		if len(f) >= 7 && f[0] == "T" && f[2] == "P" && f[len(f)-2] == "Checksum:" {
			if sums[f[1]] == nil {
				sums[f[1]] = map[string]bool{}
			}
			sums[f[1]][f[len(f)-1]] = true
		}
	}
	var out []string
	for t, s := range sums {
		if len(s) > 1 {
			out = append(out, t)
		}
	}
	slices.Sort(out)
	return out
}

// onlyTable reports whether every tablet of tablets is one of table's.
func (j *chaosJob) onlyTable(ctx context.Context, admin *kudu.Client, table string, tablets []string) bool {
	ts, err := admin.Tablets(ctx, table)
	if err != nil {
		return false
	}
	ids := map[string]bool{}
	for _, t := range ts {
		ids[t.ID] = true
	}
	for _, t := range tablets {
		if !ids[t] {
			return false
		}
	}
	return true
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

func (j *chaosJob) note(a anomaly) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.notes = append(j.notes, a)
}

// diskWindow is a disk fault on a server: what it was, and when it was
// injected and healed (zero while held).
type diskWindow struct {
	fault      string
	start, end time.Time
}

// exitAnomalies are the servers that exited on their own. Kudu documents
// that a server crashes when the disk of its WAL fills, so an exit then is
// no anomaly; but not when one data directory fails or fills while
// another has room, so an exit then is one of its own kind.
func (j *chaosJob) exitAnomalies() []anomaly {
	var bad []anomaly
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, e := range j.cluster.Exits() {
		kind, walFull := "server-exit", false
		for _, w := range j.diskFaults[e.Proc] {
			if e.Time.Before(w.start) || (!w.end.IsZero() && e.Time.After(w.end.Add(30*time.Second))) {
				continue
			}
			kind = "exit-under-" + w.fault
			walFull = walFull || w.fault == "disk-full-wal"
		}
		// Under a full WAL disk, whatever else was faulted, the crash is
		// the one Kudu documents.
		if walFull {
			continue
		}
		how := fmt.Sprintf("with status %d", e.Code)
		if e.Signal != "" {
			how = "by " + e.Signal
		}
		bad = append(bad, anomaly{Kind: kind, Detail: fmt.Sprintf("%s exited %s at %s:\n%s", e.Proc, how, e.Time.Format(time.RFC3339Nano), e.Said)})
	}
	return bad
}

// layout is what a reader of the history needs to place its reads: which
// tablet server each UUID is, and which tablet each contended key is in.
func (j *chaosJob) layout(ctx context.Context) map[string]any {
	out := map[string]any{}
	if uuids, err := j.tabletServers(ctx); err == nil {
		names := map[string]string{}
		for addr, uuid := range uuids {
			names[uuid] = j.cluster.ByAddr(addr)
		}
		out["servers"] = names
	} else {
		out["servers_error"] = err.Error()
	}
	keys := make([]int64, j.keys)
	for i := range keys {
		keys[i] = int64(i)
	}
	admin := kudu.NewClient(j.client.URL(), "layout")
	if tablets, err := admin.Partition(ctx, tableName, keys); err == nil {
		out["key_tablets"] = tablets
	} else {
		out["key_tablets_error"] = err.Error()
	}
	if tablets, err := admin.Tablets(ctx, tableName); err == nil {
		out["tablets"] = tablets
	}
	return out
}

// diskUsage is what df says of each tablet server's directories, as the
// servers' own nodes see them: a limited one is a mount in the node's
// mount namespace.
func (j *chaosJob) diskUsage(ctx context.Context) map[string]string {
	out := map[string]string{}
	for _, name := range j.cluster.Procs(kudu.TServer) {
		n := j.cluster.Node(name)
		args := append([]string{"-k", j.cluster.WALDir(name)}, j.cluster.DataDirs(name)...)
		res, err := n.Exec(ctx, torx.Command("df", args...))
		if err != nil {
			out[name] = err.Error()
			continue
		}
		out[name] = string(res.Stdout)
	}
	return out
}

// settle waits for ksck to find the cluster healthy, and returns what it
// last said.
func (j *chaosJob) settle(ctx context.Context) (string, bool) {
	ctx, cancel := context.WithTimeout(ctx, settleTimeout)
	defer cancel()
	var last string
	err := torx.WaitUntil(ctx, func(ctx context.Context) (bool, error) {
		j.supervise(ctx)
		out, err := kudu.Tool(ctx, j.client.Node(), "cluster", "ksck", j.cluster.MasterAddrs())
		last = out
		if err != nil {
			last += "\n" + err.Error()
		}
		return err == nil, nil
	}, 3*time.Second)
	return last, err == nil
}

// supervise restarts every server that exited on its own and no fault
// holds down.
func (j *chaosJob) supervise(ctx context.Context) {
	for _, name := range j.cluster.Procs("") {
		if j.cluster.Running(name) || j.holds.Held(name) {
			continue
		}
		torx.Logf(ctx, "info", "supervisor: restarting %s", name)
		if err := j.cluster.Restart(ctx, name); err != nil {
			torx.Logf(ctx, "warn", "supervisor: restarting %s: %v", name, err)
		}
	}
}
