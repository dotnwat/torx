//go:build unix

// Command 04-faults is the fourth step of the torx tutorial: fault injection
// on one node. The service gains Crash, Shutdown, and Restart, and two jobs
// use them: one checks that writes survive a kill and a restart, the other
// that a graceful stop is graceful. The results tree keeps what each
// incarnation of the server logged. README.md walks through it.
package main

import "github.com/dotnwat/torx"

func main() { torx.Main() }

// serviceName is the kvd service's name in every job, and so the directory its
// collected output lands under in the results tree.
const serviceName = "kvd"

func init() {
	torx.Register("kv.durability", func() torx.Job { return &durabilityJob{} })
	torx.Register("kv.graceful", func() torx.Job { return &gracefulJob{} })
}
