//go:build unix

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/dotnwat/torx"
)

// Load is the load generator as a service: it owns a set of nodes and runs
// kvd's load generator on all of them at once, against a server somewhere
// else. It is the second service in the benchmark job, and the reason a
// distributed test framework has nodes at all: the load comes from machines
// of its own, so the server's node is not also the one generating the load,
// and the load scales by adding nodes.
//
// It has nothing to start. The per-node hooks only check that kvd is on
// each node's PATH; the work happens in Run, when the job asks for it. A
// service that is not a long-running process per node -- a one-shot client,
// a rolling operation -- is still a service: it is how a job gets nodes and
// how the framework knows the job's demand. (A service like this could also
// override Start, Wait, Stop, and Clean directly instead of implementing the
// per-node hooks.)
type Load struct {
	*torx.ServiceBase
}

// NewLoad builds a load service named name that needs nodes nodes. The
// framework adds this demand to the other services' when it sizes the job.
func NewLoad(name string, nodes int) *Load {
	l := &Load{}
	l.ServiceBase = torx.NewServiceBase(name, torx.Homogeneous(nodes, torx.NodeSpec{}), l)
	return l
}

// StartNode checks the node is prepared; there is no process to start.
func (l *Load) StartNode(ctx context.Context, n *torx.Node) error { return preflight(ctx, n) }

// WaitNode has nothing to wait for.
func (l *Load) WaitNode(context.Context, *torx.Node) error { return nil }

// StopNode has nothing to stop: each run is a command that ran to completion.
func (l *Load) StopNode(context.Context, *torx.Node) error { return nil }

// CleanNode has nothing to remove: the load generator keeps no state.
func (l *Load) CleanNode(context.Context, *torx.Node) error { return nil }

// NodeReport is one node's report, tagged with the node it came from. Raw is
// exactly what the load generator printed there, for the job to keep as an
// artifact; it is not part of the recorded data.
type NodeReport struct {
	Node string `json:"node"`
	Report
	Raw []byte `json:"-"`
}

// Run runs clients clients for seconds seconds on every node at once,
// against the kvd at addr, and returns each node's report in node order. A
// node whose run failed contributes its error, and its raw output when it
// printed any; the others' reports are returned alongside.
func (l *Load) Run(ctx context.Context, addr string, clients, seconds int) ([]NodeReport, error) {
	nodes := l.Nodes()
	reports := make([]NodeReport, len(nodes))
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Go(func() { reports[i], errs[i] = l.runOn(ctx, n, addr, clients, seconds) })
	}
	wg.Wait()
	return reports, errors.Join(errs...)
}

// runOn runs the load generator on n to completion and parses its report.
// Each generator gets the node's name as its key prefix: every client checks
// it reads back what it wrote, and the first two-node run of this step found
// that two generators with the same key names read back each other's writes.
func (l *Load) runOn(ctx context.Context, n *torx.Node, addr string, clients, seconds int) (NodeReport, error) {
	rep := NodeReport{Node: n.Name()}
	cmd := torx.Command(binary, "load",
		"--addr", addr,
		"--clients", strconv.Itoa(clients),
		"--seconds", strconv.Itoa(seconds),
		"--prefix", n.Name()+"-",
	)
	res, err := n.Exec(ctx, cmd)
	if err != nil {
		return rep, fmt.Errorf("%s: %w", n.Name(), err)
	}
	rep.Raw = res.Stdout
	if res.ExitCode != 0 {
		return rep, fmt.Errorf("%s: kvd load exited %d: %s", n.Name(), res.ExitCode, strings.TrimSpace(string(res.Stderr)))
	}
	if err := json.Unmarshal(res.Stdout, &rep.Report); err != nil {
		return rep, fmt.Errorf("%s: kvd load printed %q: %w", n.Name(), res.Stdout, err)
	}
	if rep.Ops == 0 {
		return rep, fmt.Errorf("%s: kvd load completed no requests", n.Name())
	}
	return rep, nil
}
