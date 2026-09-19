# torx

[![CI](https://github.com/dotnwat/torx/actions/workflows/ci.yml/badge.svg)](https://github.com/dotnwat/torx/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/dotnwat/torx.svg)](https://pkg.go.dev/github.com/dotnwat/torx)

torx is a distributed testing and benchmarking framework. A **suite** is a Go
binary that links the torx library and its own jobs; the same binary is both the
driver and, re-executed, the worker that runs one job. You write two kinds of
thing:

- a **Service** — how to run a process (a server, a client, a load generator) on
  the nodes allocated to it, and
- a **Job** — one test or benchmark, which declares the services it needs and
  drives them.

The framework does the rest: it sizes each job from the services it declares,
allocates a disjoint set of nodes for it, starts the services, waits for
readiness, runs the job body, and tears everything down — collecting logs and
artifacts along the way.

A few concepts you will meet:

- **Node** — one execution target. A node runs commands and moves files through
  its **Backend** (`LocalBackend` for local runs, the `ssh` backend for remote
  nodes); a service never touches the transport, it calls `node.Exec`,
  `node.Stream`, `node.WriteFile`, and so on. A node also carries a scratch
  directory, a port allocator, and its reachable address (`node.Addr()`).
- **Pool** — the finite set of nodes a run owns. The driver allocates a sub-pool
  per job and frees it on completion, so two jobs never share a node.
- **Result** — a job passes when `Run` returns `nil`. A benchmark additionally
  records an opaque `Data` payload plus a one-line `Summary`; torx stores these
  verbatim and never interprets them.

A worked example lives in [`examples/echo/`](examples/echo/): a tiny echo
service and job, the whole vertical slice in one file. A fuller one,
[`examples/rqlite/`](examples/rqlite/), tests a real distributed database:
a multi-node service, parametrized and fault-injection jobs, and the launcher
harness a project builds around torx to run its suite.

## Authoring a Service

A service embeds `*torx.ServiceBase` and implements the four per-node lifecycle
hooks. `ServiceBase` turns those hooks into the coarse `Start`/`Stop`/`Clean`/
`Wait` lifecycle the framework drives, stopping and cleaning each node before
starting it so a service always begins from a known state.

```go
package myservice

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/dotnwat/torx"
)

// Service runs one myserver per node.
type Service struct {
	*torx.ServiceBase

	mu      sync.Mutex
	servers map[string]io.ReadCloser // node name -> running server handle
	addrs   map[string]string        // node name -> host:port
}

// New builds a service named name that needs one node.
func New(name string) *Service {
	s := &Service{servers: map[string]io.ReadCloser{}, addrs: map[string]string{}}
	// Homogeneous(count, spec) is the node demand. A spec can require CPUs,
	// memory, or labels (torx.NodeSpec{Required: torx.Resources{...}}); an empty
	// spec matches any node.
	s.ServiceBase = torx.NewServiceBase(name, torx.Homogeneous(1, torx.NodeSpec{}), s)
	return s
}

// StartNode launches the server on n. It leases a free port, binds every
// interface so a client off the node can reach it, and advertises the node's
// reachable address. StartCaptured runs the process with its output redirected
// to a node-local file and registers that file for collection.
func (s *Service) StartNode(ctx context.Context, n *torx.Node) error {
	port, err := n.AllocatePort()
	if err != nil {
		return err
	}
	dir := n.ServiceScratch(s.Name()).Root // a disjoint scratch dir for this service
	cmd := torx.Command("myserver",
		"--listen", "0.0.0.0",
		"--port", strconv.Itoa(port),
		"--dir", dir,
	)
	handle, err := s.StartCaptured(ctx, n, cmd)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.servers[n.Name()] = handle
	s.addrs[n.Name()] = net.JoinHostPort(n.Addr(), strconv.Itoa(port))
	s.mu.Unlock()
	return nil
}

// WaitNode blocks until the server is ready. Readiness is a real check, never a
// bare sleep: poll a port, an HTTP endpoint, or a protocol ping.
func (s *Service) WaitNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	addr := s.addrs[n.Name()]
	s.mu.Unlock()
	return torx.WaitForPort(ctx, addr) // or torx.WaitUntil(ctx, poll, backoff)
}

// StopNode terminates the server. Closing the StartCaptured handle kills the
// remote process group.
func (s *Service) StopNode(ctx context.Context, n *torx.Node) error {
	s.mu.Lock()
	h := s.servers[n.Name()]
	delete(s.servers, n.Name())
	delete(s.addrs, n.Name())
	s.mu.Unlock()
	if h == nil {
		return nil
	}
	return h.Close()
}

// CleanNode removes the server's persistent state.
func (s *Service) CleanNode(ctx context.Context, n *torx.Node) error {
	return n.Rm(ctx, n.ServiceScratch(s.Name()).Root)
}

// Addr exposes the server's advertised address to jobs and other services.
func (s *Service) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, n := range s.Nodes() {
		if a, ok := s.addrs[n.Name()]; ok {
			return a
		}
	}
	return ""
}
```

Guidelines that keep a service portable across the local and ssh backends:

- **Reach the node only through its methods** — `n.Exec`, `n.Stream`,
  `n.ReadFile`, `n.WriteFile`, `n.Mkdir`, `n.Rm`, `n.Signal`. They run on the
  node whether that is a local subprocess or a container over SSH.
- **Bind broadly, advertise `n.Addr()`.** Bind the server to all interfaces so a
  client on another node can reach it, and record `n.Addr()` (not a hardcoded
  `127.0.0.1`) as the address others dial.
- **Lease ports with `n.AllocatePort()`** rather than hardcoding one, so services
  co-located on a host do not collide.
- **Capture output with `StartCaptured`.** It redirects the process's stdout and
  stderr to a node-local file and collects it into the results tree. For any
  other output (a `--log-file`, a data dump) call `s.AddArtifact(n,
  torx.Artifact{Name: ..., Path: ..., CollectOnPass: true})`. A service that
  launches more than one process on a node over a job -- a crash-and-restart
  test -- calls `s.SetCapturePolicy(torx.CaptureRotate)` at construction so each
  launch moves the previous process's log aside as `stdout.<k>.log` and
  collects it too, instead of discarding it (the default, `CaptureTruncate`).
- **One-shot commands** (a load generator that runs and exits) use
  `n.Exec(ctx, cmd)` instead of `StartCaptured`; it runs to completion and
  returns the captured `ExecResult` (exit code, stdout, stderr).

A service that is not a per-node server — a rolling restart, a one-shot client, a
single cloud-API call — can override the coarse lifecycle methods (`Start`,
`Stop`, `Clean`, `Wait`) directly instead of implementing the per-node hooks.

## Authoring a Job (test or benchmark)

A job embeds `torx.JobBase` and implements `Declare` and `Run`.

```go
package main

import (
	"context"

	"example.com/myservice"
	"github.com/dotnwat/torx"
)

func main() { torx.Main() }

// Register the job by a stable id in an init function so the driver can discover
// it and the worker can reconstruct it.
func init() {
	torx.Register("my.smoke", func() torx.Job { return &smokeJob{} })
}

type smokeJob struct {
	torx.JobBase
	server *myservice.Service
}

// Declare registers and configures the services the job needs. It must be pure:
// construct and register services, but do not allocate or start anything. The
// framework calls it to size the job and the worker calls it to rebuild the job
// identically.
func (j *smokeJob) Declare(jc *torx.JobContext) {
	j.server = myservice.New("myserver")
	jc.Register(j.server)
}

// Run is the test body. It runs after the framework has started every declared
// service and waited for readiness. Return nil to pass, an error to fail.
func (j *smokeJob) Run(ctx context.Context, jc *torx.JobContext) error {
	addr := j.server.Addr()
	// ... connect to addr, exercise the server, assert behavior ...
	jc.SetSummary("myserver came up and answered")
	return nil
}
```

What `JobBase` gives you, and how to take control:

- **`Setup`** starts every declared service and waits for each to be ready, in
  registration order. Override `Setup` to control start order or start lazily.
- **`Teardown`** stops the services, collects their artifacts, cleans them, and
  runs finalizers — in reverse order, aggregating every error. Override it only
  if you need to, and call `jc.CollectArtifacts(ctx)` between stopping and
  cleaning so logs survive.
- **`jc.Defer(fn)`** registers a cleanup callback run during teardown.
- A **benchmark** records what it measured: `jc.Record(anyValue)` stores an
  opaque JSON payload and `jc.SetSummary("...")` a one-line human summary. torx
  never interprets `Data`; large outputs belong in artifacts.

Multi-service jobs just declare more services; the framework sums their demand
into the pool it allocates and hands each service its nodes (`Bind`) in
registration order. A job accesses one service from another through the service's
own methods (e.g. `server.Addr()`), exactly as in `Run` above.

**Parametrization.** A job may expand into several variants by implementing
`Matrix() []torx.Params`; the `torx.Matrix(map[string][]any)` helper builds the
cross product. Each variant gets a stable id and is selected, scheduled, and
reported independently. Read `jc.Params` (with the typed `Int`/`String`/`Bool`
getters) inside `Declare`/`Run`.

A parametrized job — especially one meant to take externally supplied
configurations (`-params`, below) — should also implement

```go
ResolveParams(p torx.Params) (torx.Params, error)
```

Discovery calls it once per variant, before selection, duplicate detection,
and id construction. The job returns the complete canonical map — defaults
filled in, values type- and range-checked, unknown keys rejected — and that
map is what the variant runs with, what its id is computed from, and what its
results record. Without the hook, `{a:1}` and `{a:1, b:<default>}` are two
different ids for the same configuration, and a mistyped key silently runs
the default value. An error from the resolver fails the variant loudly before
any node is allocated. One consequence worth knowing: adding a dimension
later changes every canonical id (its default joins every map), so join runs
on the recorded params in results, not on id strings.

## Wiring it into a suite binary

A suite is a Go binary whose `main` calls `torx.Main()` and whose jobs are
registered via `init`. A job is discoverable only if its package is linked into
that binary — so the file with the `torx.Register(...)` call must be imported
(the `main` package here contains it directly).

## Running a suite

The binary is the driver by default. Positional arguments select jobs by id
(regular expressions); with none, every registered job runs.

```bash
# Local pool sized to the largest job (or fix it with -nodes N):
go run ./path/to/suite my.smoke
go run ./path/to/suite -nodes 3 'my\..*'

# Results land under ./results/<timestamp>/ with a `latest` symlink:
#   results/<ts>/<jobVariant>/{events.ndjson, test_log, result.json,
#                              <service>/<node>/stdout.log[, stdout.<k>.log]}
```

Useful flags: `-nodes N` (local pool size), `-parallel N` (concurrent jobs),
`-results <file>` (newline-delimited JSON results), `-results-dir <dir>` (the
per-run tree; empty to disable), `-run-dir <dir>` (below), `-params <file>`
(below), and `-pool <manifest.json>` (below).

**External parametrization.** `-params FILE` replaces the named jobs'
compiled-in variants with externally supplied ones, so a specific
configuration or sweep runs without editing the suite. The file is JSON keyed
by job id; each entry gives `matrix` (dimension name → list of values,
expanded to the cross product), `configs` (explicit parameter objects, for
curated points a cross product cannot express), or both (the expanded matrix
plus the configs):

```json
{
  "my.bench": {
    "matrix":  { "clients": [1, 8, 32], "trial": [1, 2, 3] },
    "configs": [ { "clients": 64, "pipeline": 8 } ]
  }
}
```

Entries replace a job's compiled-in variants entirely; nothing is merged. The
envelope is strict — unknown fields, empty forms, empty dimensions, and null
values are rejected, an entry expanding to more than `torx.MaxVariants`
(65536) variants is refused with the count named, and naming a job that is
unknown or whose variants end up entirely unselected is an error — because
externally supplied configuration must never degrade silently. Each supplied parameter set still passes through
the job's `ResolveParams` (above), so ids stay canonical and two entries that
resolve to the same configuration are rejected as duplicates.

**Launchers and `-run-dir`.** A tool that wraps a suite — building it, writing
an invocation record, archiving the run's inputs — needs to know the exact run
directory before the run starts ([`examples/rqlite/harness`](examples/rqlite/harness/)
is one such tool): resolving the `latest` symlink afterwards
races concurrent runs, and an interrupted run would leave the metadata with no
home at all. Such a launcher creates the run directory itself, writes its
metadata into it, and then invokes the suite with `-run-dir DIR`. torx uses
the directory exactly as given — no timestamped subdirectory is minted and no
`latest` symlink is maintained (those conveniences belong to `-results-dir`
mode) — and fills in the per-variant subdirectories and the final `run.json`.
The directory must already exist, and `-run-dir` is mutually exclusive with
`-results-dir`.

**Remote pools.** To run against nodes a provisioner prepared, pass a node
manifest with `-pool` and blank-import the ssh backend in your suite's `main` so
`ssh`-kind nodes are constructible:

```go
import _ "github.com/dotnwat/torx/ssh"
```

```json
{
  "nodes": [
    { "name": "n0", "address": "10.0.0.5", "scratch": "/var/tmp/torx",
      "ports": { "min": 30000, "max": 31000 },
      "backend": { "kind": "ssh", "host": "10.0.0.5",
        "config": { "user": "torx", "identity_file": "/etc/torx/id_ed25519",
                    "known_hosts": "/etc/torx/known_hosts" } } }
  ]
}
```

A node needs a POSIX `sh`. To kill a service's whole process group on
teardown, torx runs each streamed command as the leader of its own group.
Under OpenSSH (macOS Remote Login included) with a `bash` or `zsh` login shell
that is already so, and nothing else is needed; otherwise the wrapper creates
the group with `setsid` (util-linux or busybox) or `perl`, whichever the node
has -- which also covers a login shell that forks (`dash`), `ForceCommand`
wrappers, and sshds that do not isolate commands at all (Dropbear). A node
with none of those refuses to stream, and the error says what to install,
rather than risk signalling the sshd itself.

Because a suite is one static binary, production and multi-node runs invoke it
directly; `go run`/`go test` is one way to invoke the same binary, not a second
code path.

## Testing your service and job

Cross-language wire compatibility and full service lifecycles need a live server,
so a suite is usually exercised by running it end to end and asserting the
result. The idiom (see [`examples/echo/echo_test.go`](examples/echo/echo_test.go)) is a Go test
whose `TestMain` lets the test binary double as the torx worker, then runs the
suite through the real driver/worker split on a small local pool:

```go
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "worker" {
		torx.Main() // dispatch to worker mode and exit
	}
	os.Exit(m.Run())
}

func TestSmoke(t *testing.T) {
	reqs, err := torx.Discover("my.smoke")
	// ... build a local pool, torx.Run(ctx, pool, torx.SelfExecLauncher{}, reqs, opts),
	//     assert res.Ok() and inspect the collected results tree ...
}
```

Everything else — the pure functions a service and job are built from (readiness
predicates, address handling, result parsing) — is ordinary Go unit-testable, and
should be: design services and jobs so their logic is reachable without a running
server wherever possible.

## Status and license

torx is pre-1.0. The API may change between minor versions; pin a tag. It
drives Unix processes (process groups, POSIX signals, `sh`) and is developed
on Linux and macOS; Windows is not supported.

Licensed under the [Apache License, Version 2.0](LICENSE).
