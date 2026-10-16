//go:build unix

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/dotnwat/torx/examples/kudu/qa/kudu"
)

// The table's key space is three ranges, each a range partition of its own
// (hashed into buckets within it): keys the clients contend on, keys each
// inserted once, and keys transactions insert.
const (
	freshBase = int64(1_000_000_000)
	txnBase   = int64(2_000_000_000)
	keyLimit  = int64(3_000_000_000)
	// perClient spaces one client's fresh keys, and its transactions', from
	// the next client's.
	perClient = int64(1_000_000)
	// txnRows is the most rows a transaction inserts; each transaction's
	// keys start at a multiple of it.
	txnRows = 8
)

// op is one operation a client issued, and what came back, as the history
// records it. Times are the worker's wall clock in nanoseconds, the one the
// servers on the same host read their hybrid clocks from.
type op struct {
	Index  int    `json:"i"`
	Client int    `json:"client"`
	Kind   string `json:"kind"` // write, read, begin, commit, rollback
	Call   int64  `json:"call"`
	Return int64  `json:"ret"`

	// Table is the table the operation is on, if not the contended one:
	// aiTable. Target is the value of the row a delete of it means to
	// delete, as a scan saw it.
	Table  string `json:"table,omitempty"`
	Target string `json:"target,omitempty"`

	// A write's operations, and their results, one each; and the
	// transaction a write, commit, or rollback is in.
	Txn     string          `json:"txn,omitempty"`
	Ops     []kudu.Op       `json:"ops,omitempty"`
	Results []kudu.OpResult `json:"results,omitempty"`

	// A read's request and the rows it returned.
	Read       *kudu.ReadReq `json:"read,omitempty"`
	Rows       []row         `json:"rows,omitempty"`
	Servers    []string      `json:"servers,omitempty"`
	SnapshotTS int64         `json:"snapshot_ts,omitempty"`

	// Code and Error are the failure of the operation as a whole: a
	// write's or a read's, a commit's. NodeErr is a call kudu-node did not
	// answer.
	Code    string `json:"code,omitempty"`
	Error   string `json:"error,omitempty"`
	NodeErr string `json:"node_error,omitempty"`

	// PropBefore and PropAfter are the client's propagated timestamp
	// before and after the call; -1 for none yet.
	PropBefore int64 `json:"prop_before"`
	PropAfter  int64 `json:"prop_after"`
}

// row is a row read, compactly: its key, value ("" for null), and whether
// its payload was the one its value implies.
type row struct {
	K      int64  `json:"k"`
	V      string `json:"v"`
	BadPay bool   `json:"bad_payload,omitempty"`
	Server int    `json:"s"`             // the server that served it, in the op's Servers
	Del    bool   `json:"del,omitempty"` // a diff scan's deleted row
	ID     int64  `json:"id,omitempty"`  // its auto-incrementing id, in aiTable
}

// aiTable has a non-unique primary key, k, so Kudu adds an
// auto-incrementing id to it, which the tablet server assigns each row it
// inserts. Every row's value is unique, and names it.
const (
	aiTable = "ai"
	aiKeys  = 4
)

func (o op) ai() bool { return o.Table == aiTable }

// payload is the payload of value v: bytes derived from it, of a length
// derived from it below max, so a read can be checked byte for byte.
func payload(v string, max int) string {
	sum := sha256.Sum256([]byte(v))
	n := int(sum[0])<<8 | int(sum[1])
	if max > 0 {
		n %= max
	}
	h := hex.EncodeToString(sum[:])
	return strings.Repeat(h, n/len(h)+1)[:n]
}

// definite reports whether code says a row operation did not take effect:
// Kudu checked it against the row and refused it.
func definite(code string) bool {
	return code == "AlreadyPresent" || code == "NotFound"
}

// workload issues the clients' operations and records them.
type workload struct {
	clients   []*kudu.Client
	keys      int
	valueSize int
	table     string
	timeout   time.Duration
	txns      bool
	weights   map[string]int
	rng       *rand.Rand // for the sample of old timestamps, under mu

	mu   sync.Mutex
	hist []op
	// recent are timestamps writes were acknowledged at lately, and old a
	// sample of every one so far, for reads in the past to pick from: old
	// ones reach back to the history Kudu keeps, and past it.
	recent []int64
	old    []int64
	acked  int
	// seq numbers each client's values, fresh keys, and transactions.
	seq []int64
	// ai has clients work aiTable too; aiSeen is the rows each client's
	// last scan of it saw, for its deletes to pick from.
	ai     bool
	aiSeen [][]row
}

// opWeights are how often a client issues each kind of operation.
var opWeights = map[string]int{
	"write":         10, // one operation on a contended key
	"batch":         3,  // several, in one flush
	"read-now":      3,  // a snapshot scan of the contended keys, at a timestamp the server picks
	"read-past":     4,  // a snapshot scan at a timestamp a write was acknowledged at, or near one
	"read-ryw":      2,  // a read-your-writes read of one key
	"read-latest":   1,  // a READ_LATEST read of one key
	"insert-fresh":  2,  // an insert of a key no one else writes
	"txn":           2,  // a transaction that inserts a few keys
	"read-txn":      1,  // a snapshot scan of the transactions' keys
	"close-client":  1,  // close the client's Kudu client, as a restart would
	"read-fresh":    1,  // a snapshot scan of the fresh keys
	"read-past-all": 1,  // a snapshot scan of every key, in the past
	"read-diff":     2,  // a diff scan of the contended keys between two timestamps in the past
	"ai-insert":     3,  // an insert into the table of a non-unique key
	"ai-scan":       2,  // a snapshot scan of it, now or in the past
	"ai-delete":     1,  // a delete of a row a scan of it saw, by its key and id
}

func (w *workload) record(o op) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	o.Index = len(w.hist)
	w.hist = append(w.hist, o)
	if o.Kind == "write" && o.Txn == "" {
		for _, r := range o.Results {
			if r.Code == "OK" && r.TS > 0 {
				w.recent = append(w.recent, r.TS)
				// Reservoir sampling keeps old a uniform sample.
				w.acked++
				if len(w.old) < 1024 {
					w.old = append(w.old, r.TS)
				} else if w.rng != nil {
					if i := w.rng.IntN(w.acked); i < len(w.old) {
						w.old[i] = r.TS
					}
				}
			}
		}
		if len(w.recent) > 4096 {
			w.recent = slices.Clone(w.recent[len(w.recent)-2048:])
		}
	}
	return o.Index
}

func (w *workload) history() []op {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.hist)
}

func (w *workload) next(c int) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seq[c]++
	return w.seq[c]
}

func (w *workload) pastTS(rng *rand.Rand) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.recent) == 0 {
		return 0
	}
	ts := w.recent[rng.IntN(len(w.recent))]
	if rng.IntN(4) == 0 {
		ts = w.old[rng.IntN(len(w.old))]
	}
	switch rng.IntN(4) {
	case 0: // exactly the write's: the write is not in the snapshot
	case 1: // just past it
		ts++
	default: // within a few milliseconds either side
		ts += (rng.Int64N(8_000) - 4_000) << 12
	}
	return ts
}

// run issues client c's operations until ctx ends.
func (w *workload) run(ctx context.Context, c int, rng *rand.Rand) {
	kinds, total := []string{}, 0
	for _, k := range slices.Sorted(maps.Keys(w.weights)) {
		if w.weights[k] > 0 && (w.txns || (k != "txn" && k != "read-txn")) && (w.ai || !strings.HasPrefix(k, "ai-")) {
			kinds = append(kinds, k)
			total += w.weights[k]
		}
	}
	for ctx.Err() == nil {
		x := rng.IntN(total)
		var kind string
		for _, k := range kinds {
			if x < w.weights[k] {
				kind = k
				break
			}
			x -= w.weights[k]
		}
		w.issue(ctx, c, rng, kind)
	}
}

var writeOps = []string{"insert", "upsert", "update", "delete", "insert_ignore", "update_ignore", "delete_ignore", "upsert_ignore"}

func (w *workload) rowOp(c int, rng *rand.Rand, k int64) kudu.Op {
	kind := writeOps[rng.IntN(len(writeOps))]
	o := kudu.Op{Op: kind, K: k}
	if !strings.HasPrefix(kind, "delete") {
		o.V = fmt.Sprintf("c%d-%d", c, w.next(c))
		o.P = payload(o.V, w.valueSize)
	}
	return o
}

func (w *workload) issue(ctx context.Context, c int, rng *rand.Rand, kind string) {
	cl := w.clients[c]
	switch kind {
	case "write":
		w.write(ctx, c, "", []kudu.Op{w.rowOp(c, rng, rng.Int64N(int64(w.keys)))})
	case "batch":
		n := 2 + rng.IntN(5)
		var ops []kudu.Op
		for _, k := range rng.Perm(w.keys)[:min(n, w.keys)] {
			ops = append(ops, w.rowOp(c, rng, int64(k)))
		}
		w.write(ctx, c, "", ops)
	case "insert-fresh":
		k := freshBase + int64(c)*perClient + w.next(c)
		v := fmt.Sprintf("c%d-%d", c, w.next(c))
		w.write(ctx, c, "", []kudu.Op{{Op: "insert", K: k, V: v, P: payload(v, w.valueSize)}})
	case "read-now":
		w.read(ctx, c, kudu.ReadReq{Mode: "snapshot", Lo: new(int64(0)), Hi: new(int64(w.keys))}, rng)
	case "read-past":
		ts := w.pastTS(rng)
		if ts == 0 {
			return
		}
		w.read(ctx, c, kudu.ReadReq{Mode: "snapshot", TS: ts, Lo: new(int64(0)), Hi: new(int64(w.keys))}, rng)
	case "read-past-all":
		ts := w.pastTS(rng)
		if ts == 0 {
			return
		}
		w.read(ctx, c, kudu.ReadReq{Mode: "snapshot", TS: ts}, rng)
	case "read-diff":
		a, b := w.pastTS(rng), w.pastTS(rng)
		if a == 0 || a == b {
			return
		}
		w.read(ctx, c, kudu.ReadReq{Mode: "diff", StartTS: min(a, b), TS: max(a, b), Lo: new(int64(0)), Hi: new(int64(w.keys))}, rng)
	case "read-fresh":
		w.read(ctx, c, kudu.ReadReq{Mode: "snapshot", Lo: new(freshBase), Hi: new(txnBase)}, rng)
	case "read-txn":
		w.read(ctx, c, kudu.ReadReq{Mode: "snapshot", Lo: new(txnBase), Hi: new(keyLimit)}, rng)
	case "read-ryw":
		w.read(ctx, c, kudu.ReadReq{Mode: "ryw", Keys: []int64{rng.Int64N(int64(w.keys))}}, rng)
	case "read-latest":
		w.read(ctx, c, kudu.ReadReq{Mode: "latest", Keys: []int64{rng.Int64N(int64(w.keys))}}, rng)
	case "txn":
		w.txn(ctx, c, rng)
	case "close-client":
		_ = cl.Close(ctx)
	case "ai-insert":
		v := fmt.Sprintf("a%d-%d", c, w.next(c))
		w.writeTable(ctx, c, aiTable, "", "", []kudu.Op{{Op: "insert", K: rng.Int64N(aiKeys), V: v}})
	case "ai-scan":
		r := kudu.ReadReq{Table: aiTable, Mode: "snapshot"}
		if rng.IntN(3) == 0 {
			r.TS = w.pastTS(rng)
		}
		o := w.read(ctx, c, r, rng)
		if o.NodeErr == "" && o.Code == "" {
			w.mu.Lock()
			w.aiSeen[c] = o.Rows
			w.mu.Unlock()
		}
	case "ai-delete":
		w.mu.Lock()
		seen := slices.Clone(w.aiSeen[c])
		w.mu.Unlock()
		if len(seen) == 0 {
			return
		}
		// Mostly the newest rows: deleting the row of the highest id is
		// what a restarted tablet's counter must survive.
		slices.SortFunc(seen, func(a, b row) int { return compare(b.ID, a.ID) })
		x := seen[rng.IntN(len(seen))]
		if rng.IntN(2) == 0 {
			x = seen[rng.IntN(min(3, len(seen)))]
		}
		w.writeTable(ctx, c, aiTable, "", x.V, []kudu.Op{{Op: "delete", K: x.K, ID: new(x.ID)}})
	}
}

// write applies ops, in transaction txn if it is not "", and records it.
func (w *workload) write(ctx context.Context, c int, txn string, ops []kudu.Op) op {
	return w.writeTable(ctx, c, "", txn, "", ops)
}

// writeTable applies ops to table, the contended one if "", and records
// it; a delete of aiTable records the value of the row it targets.
func (w *workload) writeTable(ctx context.Context, c int, table, txn, target string, ops []kudu.Op) op {
	cl := w.clients[c]
	o := op{Client: c, Kind: "write", Table: table, Target: target, Txn: txn, Ops: ops, Call: time.Now().UnixNano(), PropBefore: -1, PropAfter: -1}
	if table == "" {
		table = w.table
	}
	cctx, cancel := context.WithTimeout(ctx, w.timeout+15*time.Second)
	resp, err := cl.Write(cctx, table, txn, w.timeout, ops)
	cancel()
	o.Return = time.Now().UnixNano()
	if err != nil {
		o.NodeErr = err.Error()
	} else {
		o.Results, o.Code, o.Error = resp.Results, resp.Code, resp.Error
		o.PropBefore, o.PropAfter = resp.PropagatedBefore, resp.PropagatedAfter
	}
	w.record(o)
	return o
}

// read scans as r says, with the replica selection and fault tolerance
// drawn, and records it.
func (w *workload) read(ctx context.Context, c int, r kudu.ReadReq, rng *rand.Rand) op {
	cl := w.clients[c]
	table := ""
	if r.Table == aiTable {
		table = aiTable
	} else {
		r.Table = w.table
	}
	r.TimeoutMS = w.timeout.Milliseconds()
	if rng != nil {
		if rng.IntN(2) == 0 {
			r.Replica = "closest"
		}
		if r.Mode == "snapshot" && rng.IntN(3) == 0 {
			r.FaultTolerant = true
		}
		if r.Mode == "diff" {
			r.Replica = ""
		}
	}
	o := op{Client: c, Kind: "read", Table: table, Read: &r, Call: time.Now().UnixNano(), PropBefore: -1, PropAfter: -1}
	cctx, cancel := context.WithTimeout(ctx, w.timeout+15*time.Second)
	resp, err := cl.Read(cctx, r)
	cancel()
	o.Return = time.Now().UnixNano()
	if err != nil {
		o.NodeErr = err.Error()
	} else {
		o.Code, o.Error, o.SnapshotTS, o.Servers = resp.Code, resp.Error, resp.SnapshotTS, resp.Servers
		o.PropBefore, o.PropAfter = resp.PropagatedBefore, resp.PropagatedAfter
		for _, x := range resp.Rows {
			rw := row{K: x.K, Server: x.Server, Del: x.Deleted}
			if table == aiTable {
				rw.ID = x.ID
			}
			if x.V != nil {
				rw.V = *x.V
			}
			if table == "" && rw.V != "" && !rw.Del && (x.P == nil || *x.P != payload(rw.V, w.valueSize)) {
				rw.BadPay = true
			}
			rw.BadPay = rw.BadPay || (table == "" && rw.V == "" && x.P != nil)
			o.Rows = append(o.Rows, rw)
		}
	}
	w.record(o)
	return o
}

// txn begins a transaction, inserts a few keys of its own in one or two
// writes, and commits it, or now and then rolls it back.
func (w *workload) txn(ctx context.Context, c int, rng *rand.Rand) {
	cl := w.clients[c]
	b := op{Client: c, Kind: "begin", Call: time.Now().UnixNano(), PropBefore: -1, PropAfter: -1}
	cctx, cancel := context.WithTimeout(ctx, w.timeout+15*time.Second)
	res, err := cl.TxnBegin(cctx, w.timeout)
	cancel()
	b.Return = time.Now().UnixNano()
	if err != nil {
		b.NodeErr = err.Error()
	} else {
		b.Txn, b.Code, b.Error = res.Txn, res.Code, res.Error
	}
	w.record(b)
	if b.Txn == "" {
		return
	}
	base := txnBase + (int64(c)*perClient+w.next(c))*txnRows
	n := 2 + rng.IntN(txnRows-1)
	var ops []kudu.Op
	for i := range n {
		v := fmt.Sprintf("%s#%d", b.Txn, i)
		ops = append(ops, kudu.Op{Op: "insert", K: base + int64(i), V: v, P: payload(v, w.valueSize)})
	}
	split := n
	if rng.IntN(2) == 0 {
		split = 1 + rng.IntN(n-1)
	}
	// An application commits only what it knows it wrote: a write that
	// failed, or whose outcome is unknown, has the transaction rolled back.
	kind := "commit"
	for _, part := range [][]kudu.Op{ops[:split], ops[split:]} {
		if len(part) == 0 {
			continue
		}
		wo := w.write(ctx, c, b.Txn, part)
		if wo.NodeErr != "" || wo.Code != "" || len(wo.Results) != len(part) {
			kind = "rollback"
		}
		for _, r := range wo.Results {
			if r.Code != "OK" {
				kind = "rollback"
			}
		}
	}
	if rng.IntN(5) == 0 {
		kind = "rollback"
	}
	e := op{Client: c, Kind: kind, Txn: b.Txn, Ops: ops, Call: time.Now().UnixNano(), PropBefore: -1, PropAfter: -1}
	cctx, cancel = context.WithTimeout(ctx, 2*w.timeout+30*time.Second)
	if kind == "commit" {
		res, err = cl.TxnCommit(cctx, b.Txn)
	} else {
		res, err = cl.TxnRollback(cctx, b.Txn)
	}
	cancel()
	e.Return = time.Now().UnixNano()
	if err != nil {
		e.NodeErr = err.Error()
	} else {
		e.Code, e.Error = res.Code, res.Error
		e.PropBefore, e.PropAfter = res.PropagatedBefore, res.PropagatedAfter
	}
	w.record(e)
}
