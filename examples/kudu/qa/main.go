//go:build unix

// Command qa is the torx suite for Apache Kudu, a distributed columnar
// storage engine: tablets replicated with Raft across tablet servers, a
// catalog kept by masters, and multi-version concurrency control that
// orders every write by a hybrid timestamp.
//
// The suite is one binary. Run bare it is the driver, which discovers the
// jobs below, allocates local nodes to each, and runs every job in a worker
// subprocess; re-executed with "worker" it is that worker. Each job runs a
// Kudu cluster on its nodes, and reaches it through kudu-node, which serves
// Kudu's Java client over HTTP. README.md in the parent directory says how
// to run it.
package main

import "github.com/dotnwat/torx"

func main() { torx.Main() }

// Service names in every job, and so the directories their collected
// output lands under in the results tree.
const (
	clusterName = "kudu"
	clientName  = "client"
)

func init() {
	torx.Register("kudu.smoke", func() torx.Job { return &smokeJob{} })
	torx.Register("kudu.chaos", func() torx.Job { return &chaosJob{} })
}
