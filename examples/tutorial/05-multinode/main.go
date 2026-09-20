//go:build unix

// Command 05-multinode is the fifth step of the torx tutorial: more than one
// node. The benchmark's load generator becomes a service of its own, with as
// many nodes as a parameter asks for, and the job declares it beside the
// server: the framework sums the two services' demand, allocates that many
// nodes, and hands each service its share. README.md walks through it.
package main

import "github.com/dotnwat/torx"

func main() { torx.Main() }

// serviceName is the kvd service's name in every job, and so the directory its
// collected output lands under in the results tree; loadName is the load
// generator's.
const (
	serviceName = "kvd"
	loadName    = "load"
)

func init() {
	torx.Register("kv.smoke", func() torx.Job { return &smokeJob{} })
	torx.Register("kv.bench", func() torx.Job { return &benchJob{} })
	torx.Register("kv.durability", func() torx.Job { return &durabilityJob{} })
	torx.Register("kv.graceful", func() torx.Job { return &gracefulJob{} })
}
