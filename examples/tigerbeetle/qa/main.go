//go:build unix

// Command qa is the torx suite for TigerBeetle, a distributed database for
// financial transactions: replicas that agree through Viewstamped
// Replication, reached through TigerBeetle's own Go client.
//
// The suite is one binary. Run bare it is the driver, which discovers the jobs
// below, allocates local nodes to each, and runs every job in a worker
// subprocess; re-executed with "worker" it is that worker. It resolves the
// tigerbeetle binary from each node's PATH and stages nothing itself.
// README.md in the parent directory says how to run it.
package main

import "github.com/dotnwat/torx"

func main() { torx.Main() }

// serviceName is the TigerBeetle service's name in every job, and so the
// directory its collected output lands under in the results tree.
const serviceName = "tigerbeetle"

func init() {
	torx.Register("tigerbeetle.smoke", func() torx.Job { return &smokeJob{} })
	torx.Register("tigerbeetle.chaos", func() torx.Job { return &chaosJob{} })
}
