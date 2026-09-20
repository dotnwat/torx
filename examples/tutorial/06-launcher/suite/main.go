//go:build unix

// Command suite is the torx suite of the tutorial's last step: every job
// from steps 2 to 5 in one binary, with one line added, the blank import of
// the ssh backend, so that nodes of the "ssh" kind in a -pool manifest are
// constructible. It sits beside the launcher (../launcher) that builds it,
// prepares its nodes, and runs it. README.md walks through it.
package main

import (
	"github.com/dotnwat/torx"

	// NEW in step 6: the one line the suite changes. The ssh backend
	// registers itself under the "ssh" kind; without this import a manifest
	// naming ssh nodes is refused at load, with an error that says so.
	_ "github.com/dotnwat/torx/ssh"
)

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
