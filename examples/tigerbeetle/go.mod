module github.com/dotnwat/torx/examples/tigerbeetle

go 1.26.0

require (
	github.com/dotnwat/torx v0.0.0
	github.com/tigerbeetle/tigerbeetle-go v0.17.9
)

// The example is built against the torx in this repository, not a release.
replace github.com/dotnwat/torx => ../..
