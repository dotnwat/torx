//go:build unix

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/dotnwat/torx"
)

// NEW in step 3: the benchmark job, with Matrix, ResolveParams, Record, and
// WriteArtifact.

// benchJob is kv.bench: a benchmark of kvd under a number of clients. It runs
// once per point of its parameter matrix, and each variant is a job of its
// own -- selected, scheduled, and reported on its own -- with an id that
// spells out its parameters, kv.bench[clients=4,seconds=2].
//
// The load comes from kvd's own load generator, run on the node through
// n.Exec: the job asks the node to run a command to completion and gets back
// what it printed and how it exited. What it measured is recorded on the
// result, and what it printed is kept beside the result as an artifact.
type benchJob struct {
	torx.JobBase
	db *Service
}

// Bounds on the parameters, checked by ResolveParams.
const (
	maxClients = 256
	maxSeconds = 60
)

// Matrix is the compiled-in parameter space: one variant per client count.
// torx.Matrix builds the cross product of the dimensions it is given; with
// one dimension that is one variant per value.
func (*benchJob) Matrix() []torx.Params {
	return torx.Matrix(map[string][]any{"clients": {1, 4, 16}})
}

// ResolveParams turns whatever parameters a variant was given -- from the
// matrix above or a -params file -- into the complete, checked set it runs
// with. Discovery calls it before ids are built, so the id names every
// parameter with its resolved value, a mistyped key fails the variant loudly
// rather than silently running a default, and two spellings of the same
// configuration are the same variant.
func (*benchJob) ResolveParams(p torx.Params) (torx.Params, error) {
	for k := range p {
		if k != "clients" && k != "seconds" {
			return nil, fmt.Errorf("unknown parameter %q (want clients, seconds)", k)
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
	return torx.Params{"clients": clients, "seconds": seconds}, nil
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

// Declare registers the kvd service. Parameters are available here too, for
// a job whose service configuration depends on them.
func (j *benchJob) Declare(jc *torx.JobContext) {
	j.db = New(serviceName)
	jc.Register(j.db)
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

// Run drives the load and records the result. The load generator runs on the
// service's node, next to the server; step 5 moves it onto nodes of its own.
func (j *benchJob) Run(ctx context.Context, jc *torx.JobContext) error {
	clients := jc.Params.Int("clients", 1)
	seconds := jc.Params.Int("seconds", 2)
	n := j.db.Nodes()[0]

	cmd := torx.Command(binary, "load",
		"--addr", j.db.Addr(),
		"--clients", strconv.Itoa(clients),
		"--seconds", strconv.Itoa(seconds),
	)
	res, err := n.Exec(ctx, cmd)
	if err != nil {
		return err
	}
	// The raw report is kept beside result.json whatever happens next, so a
	// failed variant still shows what the load generator saw.
	jc.WriteArtifact("load.json", res.Stdout)
	if res.ExitCode != 0 {
		return fmt.Errorf("kvd load exited %d: %s", res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	var rep Report
	if err := json.Unmarshal(res.Stdout, &rep); err != nil {
		return fmt.Errorf("kvd load printed %q: %w", res.Stdout, err)
	}
	if rep.Ops == 0 {
		return fmt.Errorf("kvd load completed no requests")
	}
	jc.SetSummary(fmt.Sprintf("%d clients for %ds: %.0f ops/s, p50 %.2f ms, p99 %.2f ms",
		clients, seconds, rep.OpsPerSec, rep.P50Ms, rep.P99Ms))
	return jc.Record(rep)
}
