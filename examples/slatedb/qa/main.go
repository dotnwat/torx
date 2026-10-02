//go:build unix

// Command qa is the torx suite for SlateDB, an embedded LSM-tree key-value
// store that keeps its data in object storage.
//
// The suite is one binary. Run bare it is the driver, which discovers the jobs
// below, allocates local nodes to each, and runs every job in a worker
// subprocess; re-executed with "worker" it is that worker. Each job serves
// the database's bucket from the worker, with torx's objstore, and runs
// SlateDB's processes on its nodes through slatedb-node, which it resolves
// from each node's PATH. README.md in the parent directory says how to run
// it.
package main

import "github.com/dotnwat/torx"

func main() { torx.Main() }

// serviceName is the SlateDB service's name in every job, and so the
// directory its collected output lands under in the results tree.
const serviceName = "slatedb"

func init() {
	torx.Register("slatedb.chaos", func() torx.Job { return &chaosJob{} })
}
