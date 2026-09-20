//go:build unix

// Command 03-params is the third step of the torx tutorial: parameters,
// benchmarks, and artifacts. Its one job is a benchmark that runs once per
// point of a parameter matrix, drives kvd's load generator on the node,
// records what it measured, and keeps the raw output beside its result; and
// the service registers kvd's data log for collection when a job fails.
// README.md walks through it.
package main

import "github.com/dotnwat/torx"

func main() { torx.Main() }

// serviceName is the kvd service's name in every job, and so the directory its
// collected output lands under in the results tree.
const serviceName = "kvd"

func init() {
	torx.Register("kv.bench", func() torx.Job { return &benchJob{} })
}
