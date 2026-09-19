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
	rollingNodes = 3
	rowsPerStep  = 100 // written through the cluster while each node is down
)

// rollingJob is rqlite.rolling: the cluster stays available through a rolling
// restart, with every node stopped gracefully in turn. For each node in order
// the job stops rqlited with SIGTERM -- on which a leader steps down before
// it exits -- waits until the survivors report a leader, writes at it, brings
// the node back, and waits for it to catch up before the next node goes down.
// The stop is the thing under test: a node that ignores the signal or exits
// unclean fails the job. It is the graceful twin of rqlite.failover, which
// crashes the leader outright; the handoff a stepdown gives is recorded the
// way that job records the election a crash forces, so the two sit side by
// side in the results.
type rollingJob struct {
	torx.JobBase
	db *rqlite.Service
}

// Declare registers a three-node cluster, the smallest that keeps a quorum
// with one member down at a time.
func (j *rollingJob) Declare(jc *torx.JobContext) {
	j.db = rqlite.New(serviceName, rollingNodes)
	jc.Register(j.db)
}

// Run restarts every node in turn, writing while each is down, and checks
// that every node ends up with every row.
func (j *rollingJob) Run(ctx context.Context, jc *torx.JobContext) error {
	leader, err := j.db.AwaitLeader(ctx)
	if err != nil {
		return err
	}
	if _, err := j.db.Client(leader).Execute(ctx,
		rqlite.Stmt("CREATE TABLE events (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")); err != nil {
		return err
	}

	var handoffs []time.Duration // from each leader's stop until the survivors report its successor
	var rows int64
	for _, n := range j.db.Nodes() {
		leader, err := j.db.AwaitLeader(ctx)
		if err != nil {
			return err
		}
		wasLeader := leader.Name() == n.Name()
		jc.Log("info", fmt.Sprintf("stopping %s gracefully (leader: %t)", n.Name(), wasLeader))
		stopped := time.Now()
		if err := j.db.Shutdown(ctx, n); err != nil {
			return err
		}
		// The survivors have a leader other than the stopped node: the one that
		// was already leading, or the one a stepdown handed leadership to.
		successor, err := j.db.AwaitNewLeader(ctx, n)
		if err != nil {
			return err
		}
		if wasLeader {
			handoff := time.Since(stopped)
			handoffs = append(handoffs, handoff)
			jc.Log("info", fmt.Sprintf("%s leads %s after the stepdown", successor.Name(), handoff.Round(time.Millisecond)))
		}

		// The cluster keeps taking writes with the node down.
		stmts := make([]rqlite.Statement, 0, rowsPerStep)
		for range rowsPerStep {
			stmts = append(stmts, rqlite.Stmt("INSERT INTO events(note) VALUES(?)", "while "+n.Name()+" was down"))
		}
		if _, err := j.db.Client(successor).Execute(ctx, stmts...); err != nil {
			return err
		}
		rows += rowsPerStep

		// The node comes back as the same member and receives what it missed
		// before the next node goes down, so the cluster is never short of two.
		jc.Log("info", "restarting "+n.Name())
		if err := j.db.Restart(ctx, n); err != nil {
			return err
		}
		if err := j.db.WaitSynced(ctx, n); err != nil {
			return err
		}
	}

	// Every node's own copy converges on every row, and the membership is whole.
	count := rqlite.Stmt("SELECT COUNT(*) FROM events")
	for _, n := range j.db.Nodes() {
		if err := awaitRows(ctx, j.db.Client(n), rqlite.LevelNone, count, rows); err != nil {
			return fmt.Errorf("%s after the rolling restart: %w", n.Name(), err)
		}
	}
	leader, err = j.db.AwaitLeader(ctx)
	if err != nil {
		return err
	}
	view, err := j.db.Client(leader).Nodes(ctx, membershipProbe)
	if err != nil {
		return err
	}
	if err := checkMembership(view, rollingNodes); err != nil {
		return fmt.Errorf("after the rolling restart: %w", err)
	}

	var longest time.Duration
	handoffMS := make([]int64, 0, len(handoffs))
	for _, h := range handoffs {
		handoffMS = append(handoffMS, h.Milliseconds())
		longest = max(longest, h)
	}
	if err := jc.Record(map[string]any{
		"nodes": rollingNodes, "rows": rows, "handoffs_ms": handoffMS,
	}); err != nil {
		return err
	}
	jc.SetSummary(fmt.Sprintf("%d nodes restarted in turn; %d leader handoff(s), longest %s; every node caught up to %d rows",
		rollingNodes, len(handoffs), longest.Round(time.Millisecond), rows))
	return nil
}
