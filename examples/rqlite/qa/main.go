//go:build unix

// Command qa is the torx suite for rqlite: a real distributed database, tested
// through the framework the way a project's own QA suite would test it.
//
// The suite is one binary. Run bare it is the driver, which discovers the jobs
// below, allocates local nodes to each, and runs every job in a worker
// subprocess; re-executed with "worker" it is that worker. It resolves rqlited
// from each node's PATH and stages nothing itself: the harness beside it
// (../harness) is the launcher that checks the binary is there, prepares the
// run directory, and invokes this suite with -run-dir. README.md in the parent
// directory walks through both.
package main

import "github.com/dotnwat/torx"

func main() { torx.Main() }

// serviceName is the rqlite service's name in every job, and so the directory
// its collected output lands under in the results tree.
const serviceName = "rqlite"

func init() {
	torx.Register("rqlite.smoke", func() torx.Job { return &smokeJob{} })
	torx.Register("rqlite.cluster", func() torx.Job { return &clusterJob{} })
	torx.Register("rqlite.failover", func() torx.Job { return &failoverJob{} })
	torx.Register("rqlite.rolling", func() torx.Job { return &rollingJob{} })
}
