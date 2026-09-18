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
	failoverNodes = 3
	rowsBefore    = 10 // written before the leader is crashed
	rowsAfter     = 10 // written to the successor while the old leader is down
)

// failoverJob is rqlite.failover: the cluster survives losing its leader. The
// job crashes the leader outright, waits for the survivors to elect a
// successor, keeps writing, brings the crashed node back, and checks it
// rejoins as itself and catches up. It is the job the service's Crash and
// Restart exist for: a job drives faults through the service, never by
// reaching around it to the process.
type failoverJob struct {
	torx.JobBase
	db *rqlite.Service
}

// Declare registers a three-node cluster, the smallest that keeps a quorum
// after losing one member.
func (j *failoverJob) Declare(jc *torx.JobContext) {
	j.db = rqlite.New(serviceName, failoverNodes)
	jc.Register(j.db)
}

// Run crashes the leader, verifies the cluster keeps serving, and verifies the
// crashed node returns and catches up.
func (j *failoverJob) Run(ctx context.Context, jc *torx.JobContext) error {
	leader, err := j.db.AwaitLeader(ctx)
	if err != nil {
		return err
	}
	stmts := []rqlite.Statement{rqlite.Stmt("CREATE TABLE events (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")}
	for range rowsBefore {
		stmts = append(stmts, rqlite.Stmt("INSERT INTO events(note) VALUES(?)", "before the crash"))
	}
	if _, err := j.db.Client(leader).Execute(ctx, stmts...); err != nil {
		return err
	}

	jc.Log("info", "crashing leader "+leader.Name())
	if err := j.db.Crash(ctx, leader); err != nil {
		return err
	}
	crashed := time.Now()
	successor, err := j.db.AwaitNewLeader(ctx, leader)
	if err != nil {
		return err
	}
	election := time.Since(crashed)
	jc.Log("info", fmt.Sprintf("%s elected %s after the crash", successor.Name(), election.Round(time.Millisecond)))

	// The cluster keeps taking writes with a member down.
	stmts = stmts[:0]
	for range rowsAfter {
		stmts = append(stmts, rqlite.Stmt("INSERT INTO events(note) VALUES(?)", "after the crash"))
	}
	if _, err := j.db.Client(successor).Execute(ctx, stmts...); err != nil {
		return err
	}
	want := int64(rowsBefore + rowsAfter)
	count := rqlite.Stmt("SELECT COUNT(*) FROM events")
	for _, n := range j.db.Nodes() {
		if n.Name() == leader.Name() {
			continue
		}
		got, err := j.db.Client(n).QueryInt(ctx, rqlite.LevelLinearizable, count)
		if err != nil {
			return fmt.Errorf("reading survivor %s: %w", n.Name(), err)
		}
		if got != want {
			return fmt.Errorf("survivor %s sees %d rows, want %d", n.Name(), got, want)
		}
	}

	// The crashed node comes back as the same member and catches up. A none
	// read after WaitSynced proves the rows reached its own copy, not that a
	// forwarded request found them elsewhere.
	jc.Log("info", "restarting "+leader.Name())
	if err := j.db.Restart(ctx, leader); err != nil {
		return err
	}
	if err := j.db.WaitSynced(ctx, leader); err != nil {
		return err
	}
	got, err := j.db.Client(leader).QueryInt(ctx, rqlite.LevelNone, count)
	if err != nil {
		return fmt.Errorf("reading restarted %s: %w", leader.Name(), err)
	}
	if got != want {
		return fmt.Errorf("restarted %s sees %d rows locally, want %d", leader.Name(), got, want)
	}
	view, err := j.db.Client(successor).Nodes(ctx, membershipProbe)
	if err != nil {
		return err
	}
	if err := checkMembership(view, failoverNodes); err != nil {
		return fmt.Errorf("after the restart: %w", err)
	}

	if err := jc.Record(map[string]any{
		"crashed": leader.Name(), "successor": successor.Name(),
		"election_ms": election.Milliseconds(), "rows": want,
	}); err != nil {
		return err
	}
	jc.SetSummary(fmt.Sprintf("leader %s crashed; %s took over in %s; %s rejoined and caught up to %d rows",
		leader.Name(), successor.Name(), election.Round(time.Millisecond), leader.Name(), want))
	return nil
}
