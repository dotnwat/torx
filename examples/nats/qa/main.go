//go:build unix

// Command qa is the torx suite for NATS, a messaging system whose
// JetStream layer persists messages in streams: logs replicated with Raft
// across a cluster's servers, read back by sequence and subject and
// delivered through consumers.
//
// The suite is one binary. Run bare it is the driver, which discovers the
// jobs below, allocates local nodes to each, and runs every job in a worker
// subprocess; re-executed with "worker" it is that worker. Each job runs a
// NATS cluster on its nodes and reaches it with the nats.go client from the
// worker. README.md in the parent directory says how to run it.
package main

import "github.com/dotnwat/torx"

func main() { torx.Main() }

// Service names in every job, and so the directories their collected
// output lands under in the results tree.
const clusterName = "nats"

func init() {
	torx.Register("nats.smoke", func() torx.Job { return &smokeJob{} })
	torx.Register("nats.chaos", func() torx.Job { return &chaosJob{} })
	torx.Register("nats.deposed", func() torx.Job { return &deposedJob{} })
}
