//go:build unix

package main

import (
	"context"
	"fmt"

	"github.com/dotnwat/torx"
)

// NEW in step 3: the benchmark job, with Matrix, ResolveParams, Record, and
// WriteArtifact. NEW in step 5: a second service, the load generator, on
// nodes of its own; Declare, Summary, and Run are where it shows.

// benchJob is kv.bench: a benchmark of kvd under load from a number of
// nodes, each running a number of clients. It declares two services -- the
// server, on a node of its own, and the load generator, on as many nodes as
// the nodes parameter says -- and the framework sums their demand into the
// nodes it allocates the job and hands each service its share, in the order
// they were registered.
//
// It runs once per point of its parameter matrix, and each variant is a job
// of its own -- selected, scheduled, and reported on its own -- with an id
// that spells out its parameters, kv.bench[clients=4,nodes=2,seconds=2].
type benchJob struct {
	torx.JobBase
	db   *Service
	load *Load
}

// Bounds on the parameters, checked by ResolveParams.
const (
	maxClients = 256
	maxSeconds = 60
	maxNodes   = 8
)

// Matrix is the compiled-in parameter space: the cross product of client
// counts and load-node counts. torx.Matrix takes dimensions in sorted name
// order, so the expansion is deterministic whatever order they are written.
func (*benchJob) Matrix() []torx.Params {
	return torx.Matrix(map[string][]any{
		"clients": {4, 16},
		"nodes":   {1, 2},
	})
}

// ResolveParams turns whatever parameters a variant was given -- from the
// matrix above or a -params file -- into the complete, checked set it runs
// with. Discovery calls it before ids are built, so the id names every
// parameter with its resolved value, a mistyped key fails the variant loudly
// rather than silently running a default, and two spellings of the same
// configuration are the same variant.
func (*benchJob) ResolveParams(p torx.Params) (torx.Params, error) {
	for k := range p {
		if k != "clients" && k != "seconds" && k != "nodes" {
			return nil, fmt.Errorf("unknown parameter %q (want clients, nodes, seconds)", k)
		}
	}
	clients, err := intParam(p, "clients", 1, maxClients)
	if err != nil {
		return nil, err
	}
	seconds, err := intParam(p, "seconds", 2, maxSeconds)
	if err != nil {
		return nil, err
	}
	nodes, err := intParam(p, "nodes", 1, maxNodes)
	if err != nil {
		return nil, err
	}
	return torx.Params{"clients": clients, "seconds": seconds, "nodes": nodes}, nil
}

// intParam reads an integer parameter in [1, limit], filling in def when it
// is absent. Parameters arrive as JSON, so a number may be a float64; that is
// accepted when it is whole, and anything else is rejected rather than
// defaulted.
func intParam(p torx.Params, key string, def, limit int) (int, error) {
	v, ok := p[key]
	if !ok {
		return def, nil
	}
	var n int
	switch v := v.(type) {
	case int:
		n = v
	case float64:
		if v != float64(int(v)) {
			return 0, fmt.Errorf("%s must be an integer, got %v", key, v)
		}
		n = int(v)
	default:
		return 0, fmt.Errorf("%s must be an integer, got %v", key, v)
	}
	if n < 1 || n > limit {
		return 0, fmt.Errorf("%s must be in 1..%d, got %d", key, limit, n)
	}
	return n, nil
}

// NEW in step 5: two services in one job.
//
// Declare registers the server and then the load generator, whose node
// count is a parameter: the job's demand depends on the variant. Services
// are bound to nodes in registration order, so the server gets the first
// node of the job's allocation and the load generator the rest.
func (j *benchJob) Declare(jc *torx.JobContext) {
	j.db = New(serviceName)
	j.load = NewLoad(loadName, jc.Params.Int("nodes", 1))
	jc.Register(j.db)
	jc.Register(j.load)
}

// Report is what kvd's load generator prints: what it ran and what it
// measured. It is the benchmark's own schema; torx records it verbatim.
type Report struct {
	Clients   int     `json:"clients"`
	Seconds   float64 `json:"seconds"`
	Ops       int     `json:"ops"`
	OpsPerSec float64 `json:"ops_per_sec"`
	P50Ms     float64 `json:"p50_ms"`
	P99Ms     float64 `json:"p99_ms"`
	Errors    int     `json:"errors"`
}

// NEW in step 5: aggregating across nodes.
//
// Summary is what the job records: every node's report, and the totals a
// reader wants first. Throughput adds up across nodes; percentiles do not,
// so the worst node's p99 stands for the whole rather than an average that
// would mean nothing.
type Summary struct {
	Nodes     []NodeReport `json:"nodes"`
	Ops       int          `json:"ops"`
	OpsPerSec float64      `json:"ops_per_sec"`
	P99Ms     float64      `json:"p99_ms_worst"`
}

// NEW in step 5: the load comes from every load node at once.
//
// Run drives the load from every load node at once and records the result.
// The load generators reach the server at the address the service
// advertises, which on the local pool is the loopback and on a real pool the
// server node's own address -- the reason the service binds every interface
// and advertises n.Addr() rather than 127.0.0.1.
func (j *benchJob) Run(ctx context.Context, jc *torx.JobContext) error {
	clients := jc.Params.Int("clients", 1)
	seconds := jc.Params.Int("seconds", 2)

	reports, err := j.load.Run(ctx, j.db.Addr(), clients, seconds)
	// Each node's raw report is kept beside result.json whatever happens
	// next, so a failed variant still shows what each load generator saw.
	for _, r := range reports {
		if r.Raw != nil {
			jc.WriteArtifact("load-"+r.Node+".json", r.Raw)
		}
	}
	if err != nil {
		return err
	}

	sum := Summary{Nodes: reports}
	for _, r := range reports {
		sum.Ops += r.Ops
		sum.OpsPerSec += r.OpsPerSec
		sum.P99Ms = max(sum.P99Ms, r.P99Ms)
	}
	jc.SetSummary(fmt.Sprintf("%d node(s) x %d clients for %ds: %.0f ops/s, worst p99 %.2f ms",
		len(reports), clients, seconds, sum.OpsPerSec, sum.P99Ms))
	return jc.Record(sum)
}
