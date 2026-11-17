//go:build unix

// Command qa is the torx suite for Litestream, which replicates a SQLite
// database to object storage by shipping its WAL.
//
// The suite is one binary. Run bare it is the driver, which discovers the
// jobs below, allocates local nodes to each, and runs every job in a worker
// subprocess; re-executed with "worker" it is that worker. Each job serves
// the replica's bucket from the worker, with torx's objstore, and runs
// litestream and lsapp, the application whose database it replicates, on
// its nodes, resolving both from each node's PATH. README.md in the parent
// directory says how to run it.
package main

import "github.com/dotnwat/torx"

func main() { torx.Main() }

// serviceName is the service's name in every job, and so the directory its
// collected output lands under in the results tree.
const serviceName = "litestream"

func init() {
	torx.Register("litestream.chaos", func() torx.Job { return &chaosJob{} })
	torx.Register("litestream.restart-gap", func() torx.Job { return &restartGapJob{} })
	torx.Register("litestream.torn-commit", func() torx.Job { return &tornCommitJob{} })
	torx.Register("litestream.follow-regress", func() torx.Job { return &followRegressJob{} })
}
