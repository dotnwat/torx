//go:build unix

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/rqlite/qa/rqlite"
)

const (
	// clusterRows is how many rows the cluster job writes at the leader and
	// expects to see from a follower.
	clusterRows = 100
	// membershipProbe bounds each reachability probe behind a membership view.
	membershipProbe = 2 * time.Second
)

// clusterJob is rqlite.cluster: a cluster forms, every member is a reachable
// voter with exactly one leader, and a write at the leader is visible from a
// follower at the read consistency level under test. It is parametrized on
// the cluster size and the level, so one job covers every level's guarantee
// and a -params file can run it against other sizes.
type clusterJob struct {
	torx.JobBase
	nodes int
	level string
	db    *rqlite.Service
}

// Matrix is the compiled-in variant set: the default size crossed with every
// read level.
func (j *clusterJob) Matrix() []torx.Params {
	levels := make([]any, 0, len(rqlite.Levels()))
	for _, l := range rqlite.Levels() {
		levels = append(levels, l)
	}
	return torx.Matrix(map[string][]any{paramNodes: {defaultNodes}, paramLevel: levels})
}

// ResolveParams canonicalizes a variant's parameters; see params.go.
func (j *clusterJob) ResolveParams(p torx.Params) (torx.Params, error) {
	return resolveClusterParams(p)
}

// Declare registers a cluster of the requested size. Parameters here have
// passed ResolveParams, so the typed getters' defaults are never reached.
func (j *clusterJob) Declare(jc *torx.JobContext) {
	j.nodes = jc.Params.Int(paramNodes, defaultNodes)
	j.level = jc.Params.String(paramLevel, defaultLevel)
	j.db = rqlite.New(serviceName, j.nodes)
	jc.Register(j.db)
}

// Run checks the membership, writes at the leader, and reads at a follower.
func (j *clusterJob) Run(ctx context.Context, jc *torx.JobContext) error {
	leader, err := j.db.AwaitLeader(ctx)
	if err != nil {
		return err
	}
	view, err := j.db.Client(leader).Nodes(ctx, membershipProbe)
	if err != nil {
		return err
	}
	if err := checkMembership(view, j.nodes); err != nil {
		return err
	}

	stmts := []rqlite.Statement{rqlite.Stmt("CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT NOT NULL)")}
	for i := range clusterRows {
		stmts = append(stmts, rqlite.Stmt("INSERT INTO kv(k, v) VALUES(?, ?)", i, fmt.Sprintf("value-%d", i)))
	}
	if _, err := j.db.Client(leader).Execute(ctx, stmts...); err != nil {
		return err
	}

	// Read at a follower, or at the leader itself when there is no follower.
	reader := leader
	for _, n := range j.db.Nodes() {
		if n.Name() != leader.Name() {
			reader = n
			break
		}
	}
	count := rqlite.Stmt("SELECT COUNT(*) FROM kv")
	if j.level == rqlite.LevelNone && reader != leader {
		// A none read is served from the follower's own copy with no cluster
		// check, so it may trail a write the leader has acknowledged. The
		// guarantee under test is that the copy converges, so the assertion
		// is a bounded poll rather than a single read.
		if err := awaitRows(ctx, j.db.Client(reader), j.level, count, clusterRows); err != nil {
			return fmt.Errorf("reading %s at level %s: %w", reader.Name(), j.level, err)
		}
	} else {
		// Every other level guarantees the write is visible now.
		got, err := j.db.Client(reader).QueryInt(ctx, j.level, count)
		if err != nil {
			return fmt.Errorf("reading %s at level %s: %w", reader.Name(), j.level, err)
		}
		if got != clusterRows {
			return fmt.Errorf("%s at level %s sees %d rows, want %d", reader.Name(), j.level, got, clusterRows)
		}
	}

	if err := jc.Record(map[string]any{
		"nodes": j.nodes, "level": j.level, "rows": clusterRows,
		"leader": leader.Name(), "reader": reader.Name(),
	}); err != nil {
		return err
	}
	jc.SetSummary(fmt.Sprintf("%d-node cluster; %d rows written at %s visible from %s at level=%s",
		j.nodes, clusterRows, leader.Name(), reader.Name(), j.level))
	return nil
}

// checkMembership verifies that a membership view describes a healthy cluster
// of want nodes: every member a reachable voter, and exactly one leader.
func checkMembership(view []rqlite.NodeInfo, want int) error {
	if len(view) != want {
		return fmt.Errorf("cluster has %d members, want %d", len(view), want)
	}
	leaders := 0
	for _, m := range view {
		if !m.Voter {
			return fmt.Errorf("member %s is not a voter", m.ID)
		}
		if !m.Reachable {
			return fmt.Errorf("member %s is unreachable", m.ID)
		}
		if m.Leader {
			leaders++
		}
	}
	if leaders != 1 {
		return fmt.Errorf("cluster has %d leaders, want 1", leaders)
	}
	return nil
}
