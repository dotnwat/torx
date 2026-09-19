//go:build unix

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dotnwat/torx"
	"github.com/dotnwat/torx/examples/rqlite/qa/rqlite"
)

const (
	backupNodes = 3
	backupRows  = 1000 // written before the backup, and expected back after the restore

	// The job's own artifacts: the backup as rqlite restores it, and as a
	// person reads it.
	backupFile = "backup.db"
	dumpFile   = "backup.sql"

	// sqliteHeader opens every SQLite database file.
	sqliteHeader = "SQLite format 3\x00"
)

// backupJob is rqlite.backup: a backup taken from the cluster restores it. The
// job writes rows at the leader and takes a backup there in both forms rqlite
// offers -- the SQLite file a restore loads, and the SQL dump a person can
// read -- and attaches both to its results. It then damages the database,
// dropping the table and creating another the backup knows nothing of, and
// restores the backup by loading it at a follower, which forwards it to the
// leader the way an operator's load would go. The restore must bring back
// what was dropped and remove what was added since, on every node's own copy.
// It is the job that job-level artifacts exist for: the backup arrives in the
// job's own process, not on any node, so no service could have collected it.
type backupJob struct {
	torx.JobBase
	db *rqlite.Service
}

// Declare registers a three-node cluster, so the load can be sent to a
// follower and the restored state checked on nodes that did not take it.
func (j *backupJob) Declare(jc *torx.JobContext) {
	j.db = rqlite.New(serviceName, backupNodes)
	jc.Register(j.db)
}

// Run backs the database up, damages it, restores it, and checks every node.
func (j *backupJob) Run(ctx context.Context, jc *torx.JobContext) error {
	leader, err := j.db.AwaitLeader(ctx)
	if err != nil {
		return err
	}
	stmts := []rqlite.Statement{rqlite.Stmt("CREATE TABLE events (id INTEGER PRIMARY KEY, note TEXT NOT NULL)")}
	for i := range backupRows {
		stmts = append(stmts, rqlite.Stmt("INSERT INTO events(note) VALUES(?)", fmt.Sprintf("row %d", i)))
	}
	if _, err := j.db.Client(leader).Execute(ctx, stmts...); err != nil {
		return err
	}

	// Back up at the leader. The SQLite file is checked for its header and
	// the dump for the schema before either is kept, so a body that is an
	// error page or an empty database fails here rather than at the restore.
	started := time.Now()
	backup, err := j.db.Client(leader).Backup(ctx)
	if err != nil {
		return err
	}
	backupTook := time.Since(started)
	if !bytes.HasPrefix(backup, []byte(sqliteHeader)) {
		return fmt.Errorf("backup from %s is not a SQLite database file (%d bytes starting %q)", leader.Name(), len(backup), head(backup))
	}
	dump, err := j.db.Client(leader).Dump(ctx)
	if err != nil {
		return err
	}
	if !bytes.Contains(dump, []byte("CREATE TABLE events")) {
		return fmt.Errorf("dump from %s does not define the events table (%d bytes starting %q)", leader.Name(), len(dump), head(dump))
	}
	jc.WriteArtifact(backupFile, backup)
	jc.WriteArtifact(dumpFile, dump)
	jc.Log("info", fmt.Sprintf("backed up %d rows from %s in %s: %d bytes as SQLite, %d as SQL",
		backupRows, leader.Name(), backupTook.Round(time.Millisecond), len(backup), len(dump)))

	// Damage the database: drop the table and add one the backup does not
	// hold, so the restore has to both bring back and take away.
	if _, err := j.db.Client(leader).Execute(ctx,
		rqlite.Stmt("DROP TABLE events"),
		rqlite.Stmt("CREATE TABLE junk (id INTEGER PRIMARY KEY)"),
		rqlite.Stmt("INSERT INTO junk(id) VALUES(1)"),
	); err != nil {
		return err
	}

	// Restore through a follower, which forwards the load to the leader.
	restorer := followerOf(j.db, leader)
	jc.Log("info", fmt.Sprintf("restoring the backup through %s", restorer.Name()))
	started = time.Now()
	if err := j.db.Client(restorer).Load(ctx, backup); err != nil {
		return err
	}
	restoreTook := time.Since(started)

	// The leader serves the restored state: every row back, the junk gone. A
	// loaded backup replaces the database; a load that merged would keep the
	// junk table.
	count := rqlite.Stmt("SELECT COUNT(*) FROM events")
	got, err := j.db.Client(leader).QueryInt(ctx, rqlite.LevelLinearizable, count)
	if err != nil {
		return fmt.Errorf("after the restore: %w", err)
	}
	if got != backupRows {
		return fmt.Errorf("after the restore the leader sees %d rows, want %d", got, backupRows)
	}
	_, err = j.db.Client(leader).QueryInt(ctx, rqlite.LevelLinearizable, rqlite.Stmt("SELECT COUNT(*) FROM junk"))
	switch {
	case err == nil:
		return errors.New("after the restore the junk table is still there; a loaded backup must replace the database, not add to it")
	case !strings.Contains(err.Error(), "no such table"):
		return fmt.Errorf("after the restore, checking the junk table is gone: %w", err)
	}

	// Every node's own copy converges on the restored state, the restorer's
	// and the leader's included.
	for _, n := range j.db.Nodes() {
		if err := awaitRows(ctx, j.db.Client(n), rqlite.LevelNone, count, backupRows); err != nil {
			return fmt.Errorf("%s after the restore: %w", n.Name(), err)
		}
	}

	if err := jc.Record(map[string]any{
		"rows": backupRows, "backup_bytes": len(backup), "dump_bytes": len(dump),
		"backup_ms": backupTook.Milliseconds(), "restore_ms": restoreTook.Milliseconds(),
		"restored_through": restorer.Name(),
	}); err != nil {
		return err
	}
	jc.SetSummary(fmt.Sprintf("%d rows backed up from %s (%d bytes) and restored through %s in %s; every node holds them again",
		backupRows, leader.Name(), len(backup), restorer.Name(), restoreTook.Round(time.Millisecond)))
	return nil
}

// head returns the start of data for an error message.
func head(data []byte) string {
	const n = 32
	if len(data) <= n {
		return string(data)
	}
	return string(data[:n]) + "..."
}
