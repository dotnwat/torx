//go:build unix

package main

import (
	"context"
	"fmt"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/rqlite/qa/rqlite"
)

// smokeJob is rqlite.smoke: one node, one table, a write and a read. It is the
// smallest job that proves the binary runs, the service brings it to
// readiness, and the API answers -- the first thing to run against a new
// build, and the shape every other job here elaborates.
type smokeJob struct {
	torx.JobBase
	db *rqlite.Service
}

// Declare registers a one-node rqlite service.
func (j *smokeJob) Declare(jc *torx.JobContext) {
	j.db = rqlite.New(serviceName, 1)
	jc.Register(j.db)
}

// Run creates a table, inserts two rows in one transaction, and reads the
// count back.
func (j *smokeJob) Run(ctx context.Context, jc *torx.JobContext) error {
	c := j.db.Client(j.db.Nodes()[0])
	if _, err := c.Execute(ctx,
		rqlite.Stmt("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"),
		rqlite.Stmt("INSERT INTO users(name) VALUES(?)", "alice"),
		rqlite.Stmt("INSERT INTO users(name) VALUES(?)", "bob"),
	); err != nil {
		return err
	}
	got, err := c.QueryInt(ctx, rqlite.LevelWeak, rqlite.Stmt("SELECT COUNT(*) FROM users"))
	if err != nil {
		return err
	}
	if got != 2 {
		return fmt.Errorf("users has %d rows, want 2", got)
	}
	jc.SetSummary(fmt.Sprintf("single node up; wrote and read back %d rows", got))
	return nil
}
