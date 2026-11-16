module github.com/dotnwat/torx/examples/nats

go 1.26.0

require (
	github.com/dotnwat/torx v0.0.0
	github.com/nats-io/nats.go v1.55.0
)

require (
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

// The example is built against the torx in this repository, not a release.
replace github.com/dotnwat/torx => ../..
