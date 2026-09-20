//go:build unix

// Command 02-service is the second step of the torx tutorial: the first
// suite with a service. It deploys kvd, the tutorial's small key-value
// server in ../kvd, onto one node, and its one job writes a key through
// kvd's HTTP API and reads it back. README.md walks through it.
package main

import "github.com/dotnwat/torx"

func main() { torx.Main() }

// serviceName is the kvd service's name in every job, and so the directory its
// collected output lands under in the results tree.
const serviceName = "kvd"

func init() {
	torx.Register("kv.smoke", func() torx.Job { return &smokeJob{} })
}
