//go:build unix

// Command qa is the torx suite for RustFS, an S3-compatible object store
// that spreads each object over the drives of a cluster with erasure
// coding, reached through its S3 API.
//
// The suite is one binary. Run bare it is the driver, which discovers the jobs
// below, allocates local nodes to each, and runs every job in a worker
// subprocess; re-executed with "worker" it is that worker. It resolves the
// rustfs binary from each node's PATH and stages nothing itself.
// README.md in the parent directory says how to run it.
package main

import "github.com/dotnwat/torx"

func main() { torx.Main() }

// serviceName is the RustFS service's name in every job, and so the
// directory its collected output lands under in the results tree.
const serviceName = "rustfs"

func init() {
	torx.Register("rustfs.smoke", func() torx.Job { return &smokeJob{} })
}
