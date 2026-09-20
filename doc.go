//go:build unix

// Package torx is a distributed testing and benchmarking framework.
//
// It takes the architecture, data model, and programming model of the
// ducktape framework as its basis: tests and benchmarks ("jobs") declare
// the services they need, the framework allocates nodes from a finite pool,
// and each job runs in an isolated worker process that drives those nodes
// through a pluggable backend.
//
// A suite is a Go binary that links this package and registers its jobs; the
// same binary is the driver and, re-executed, the worker that runs one job.
// See [Main], [Register], [Job], and [Service] for the programming model, and
// the README for a walkthrough. The [LocalBackend] runs nodes as local
// subprocesses; the ssh subpackage adds remote nodes behind the same [Backend]
// interface. A step-by-step tutorial lives under examples/tutorial, and a
// fuller example -- a real distributed database with a multi-node service,
// fault injection, and a launcher harness -- under examples/rqlite.
//
// torx drives Unix processes (process groups, POSIX signals, sh); Windows is
// not supported. It is pre-1.0: the API may change between minor versions.
package torx
