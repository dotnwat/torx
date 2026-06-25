// Package torx is a distributed testing and benchmarking framework.
//
// It takes the architecture, data model, and programming model of the
// ducktape framework as its basis: tests and benchmarks ("jobs") declare
// the services they need, the framework allocates nodes from a finite pool,
// and each job runs in an isolated worker process that drives those nodes
// through a pluggable backend. This package is the root of the framework
// library; subpackages add the backend, pool, service, and job machinery.
package torx

// Name is the framework's identifier, used in reports and command output.
const Name = "torx"
