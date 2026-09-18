package main

import (
	"os"

	"github.com/dotnwat/torx"
)

func main() {
	// A suite binary can wrap its own subcommands around torx.Main: here
	// "echo-server" is the service process EchoService launches on a node. Every
	// other invocation -- the bare driver and the "worker" subcommand -- is
	// handled by torx.Main.
	if len(os.Args) > 1 && os.Args[1] == "echo-server" {
		os.Exit(echoServerMain(os.Args[2:]))
	}
	torx.Main()
}
