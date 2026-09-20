//go:build unix

// Command suite is the torx suite of the tutorial's last step: the suite of
// step 5, unchanged but for one line -- the blank import of the ssh backend,
// so that nodes of the "ssh" kind in a -pool manifest are constructible --
// and moved beside the launcher (../launcher) that builds it, prepares its
// nodes, and runs it. README.md walks through it.
package main

import (
	"github.com/dotnwat/torx"

	// The ssh backend registers itself under the "ssh" kind. Without this
	// import a manifest naming ssh nodes is refused at load, with an error
	// that says so.
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
