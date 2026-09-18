# examples/rqlite — a QA suite for a real distributed system

[`examples/echo`](../echo/) shows torx's programming model in one file. This
example shows what a project actually builds around torx: a suite that tests a
real distributed database, and the launcher that a team runs it through.

The system under test is [rqlite](https://rqlite.io), a distributed SQLite
replicated with Raft. It suits a test suite well: one static binary, a plain
HTTP and JSON API, two ports per node, a readiness endpoint, a membership
endpoint that names the leader, and leader elections that finish in a couple
of seconds -- so every job here runs in seconds, and the whole suite in under
half a minute.

Two things live here:

- **`qa/`** is the suite: a torx binary with three jobs, and the service
  package (`qa/rqlite`) that deploys an rqlite cluster onto torx nodes and
  speaks to it.
- **`harness/`** is the launcher: the front end a person or CI runs. It owns
  what a suite cannot own itself -- building the binary, checking the system
  under test is installed, preparing a run directory that records how the run
  was invoked -- and then hands that directory to the suite.

torx stops at running jobs against nodes it is handed. It does not build
suites, install servers, provision machines, or decide where results go,
because every project answers those differently. The split above is the shape
of one project's answer, cut down to what a reader needs to see.

## Requirements

- Go, as for torx itself.
- `rqlited` from rqlite **v10** on `PATH`. On macOS: `brew install rqlite`. On
  Linux: a release tarball from
  [github.com/rqlite/rqlite/releases](https://github.com/rqlite/rqlite/releases)
  (the `linux-amd64` or `linux-arm64` one), or a distribution package.

The suite resolves the binary from each node's `PATH` by name and stages
nothing itself; that is torx's convention for the system under test, and the
harness's job is to check the binary is there and record which one it found.
The major version is pinned because rqlite's cluster-join semantics have
changed across majors; the harness refuses any other.

## Running

From anywhere inside the repository:

```sh
go run ./examples/rqlite/harness                    # every job
go run ./examples/rqlite/harness rqlite.smoke       # jobs are selected by id, as regular expressions
go run ./examples/rqlite/harness 'rqlite\.cluster'  # every variant of one job
```

The harness prints one line, `run directory: <path>`, and then the suite's own
output; its exit status is the suite's. The run directory lands under
`results/rqlite/` in the repository (`-results-dir` moves it) with `latest`
pointing at the newest run:

```
results/rqlite/2026-09-18T17-38-54Z-3312641955/
  invocation.json                 how the run was launched (below)
  bin/qa                          the suite binary that ran
  params.json                     verbatim copy of -params, when given
  rqlite.smoke/
    result.json                   status, summary, recorded data, error if any
    test_log                      the job's log lines
    events.ndjson                 every event the worker emitted
  rqlite.failover/
    rqlite/node-2/stdout.log      each node's captured rqlited output ...
    rqlite/node-2/stdout.1.log    ... and, for a node that was crashed and
                                  restarted, the output of the first process
  run.json                        the whole run's results
```

`invocation.json` is written before the suite starts, so even an interrupted
run says what produced it: the harness's own argv, the git commit and whether
the tree was dirty, the `rqlited` path and version the nodes resolved, the
suite binary and exactly the arguments it was exec'd with, and the archived
params file if there was one.

**Parametrization.** `rqlite.cluster` takes two parameters, `nodes` and
`level`. Its compiled-in matrix runs three nodes at every read level; a
`-params` file replaces that, for example to try a five-node cluster:

```json
{ "rqlite.cluster": { "matrix": { "nodes": [5], "level": ["weak", "linearizable"] } } }
```

```sh
go run ./examples/rqlite/harness -params five.json 'rqlite\.cluster'
```

**Without the harness.** The suite is an ordinary torx binary, so it also runs
bare, with results under `./results/`:

```sh
go run ./examples/rqlite/qa rqlite.smoke
```

That is what the harness execs, plus `-run-dir`. Nothing in the suite knows
the harness exists.

## The jobs

**`rqlite.smoke`** starts one node, creates a table, inserts two rows in one
transaction, and reads the count back. It is the smallest job that proves the
binary runs, the service brings it to readiness, and the API answers.

**`rqlite.cluster[level=…,nodes=…]`** starts a cluster, checks its membership
(every node a reachable voter, exactly one leader), writes a hundred rows at
the leader, and reads the count from a follower at the given read consistency
level. One variant per level, so the suite covers each level's guarantee:

- `weak`, `linearizable`, and `strong` reads must see every row immediately.
- `none` reads are served from the follower's own copy with no cluster check,
  so they may trail a write the leader has acknowledged. The first run of this
  example showed how far: the follower had not applied the `CREATE TABLE`
  yet, and the read failed with "no such table". The variant therefore asserts
  what the level actually guarantees -- that the copy converges -- by polling
  with a deadline.

The `level` parameter is validated by the job's `ResolveParams` because rqlite
itself accepts an unknown level and silently serves the request at its
default; without the check, a typo in a params file would test the wrong
guarantee and pass.

**`rqlite.failover`** starts three nodes, writes at the leader, crashes the
leader outright (SIGKILL, so no graceful stepdown), waits for the survivors to
elect a successor, writes a thousand rows through the successor, and checks
every survivor sees all the rows at `linearizable`. Then it restarts the
crashed node, waits until it reports it has received the log it missed
(`/readyz?sync`), polls its own local copy at `none` until every row is
there -- receiving the log and applying it to SQLite are separate steps, so
the count converges rather than being current the moment the node is synced
-- and checks the membership is whole again. The election time is recorded in
the result's data.

## How the service is built

`qa/rqlite/service.go` is the part worth reading closely; the choices below
are the ones that matter for any multi-node service on torx.

**A cluster is one service, not N.** `rqlite.New(name, nodes)` asks for
`nodes` nodes and runs one `rqlited` on each. The first node bootstraps a
one-node cluster and every later node joins through the nodes started before
it. `ServiceBase`'s default lifecycle starts nodes one at a time in order,
but it launches every node before the framework waits on any of them, so a
predecessor that has been launched is not one that is ready -- and a joiner
that cannot reach a ready seed gives up after a few attempts and exits for
good. The service therefore waits for a node's predecessors to be ready
inside `StartNode`, before launching it, which turns the start order into a
real prerequisite. (rqlite's order-independent alternative,
`-bootstrap-expect`, needs every node's Raft address before the first start,
so a service using it would allocate all ports up front in an overridden
`Start`.)

**Ports are identity.** A node's two ports are leased when it first starts and
kept until the framework stops it, not released with its process. Its peers
know it by those addresses, so `Crash` kills the process and keeps the ports,
and `Restart` launches a new process behind the same addresses on the same
data directory: the node comes back as the member that went away, and its
Raft log on disk lets it catch up. `StopNode`, which the framework calls at
teardown and before every start, is the one that ends a membership and
releases the ports. A job injects faults only through these methods, never by
reaching past the service to the process.

**Bind broadly, advertise the node's address.** Each `rqlited` binds `0.0.0.0`
and advertises `node.Addr()` for both its HTTP and Raft endpoints. On the
local pool that address is the loopback and every node shares it, told apart
by port; on remote nodes it is the address peers dial. The service does not
change between the two.

**Readiness is a real check.** `WaitNode` polls `/readyz` with
`torx.WaitForHTTP`, which treats the 503 rqlite serves until it knows a leader
as "not yet". `WaitSynced` polls `/readyz?sync`, which additionally waits for
the node to receive everything the leader had committed; applying those
entries to the node's SQLite copy comes after, so a job reading that copy
polls for what it expects (`awaitRows`) instead of asserting it. Every wait
is bounded by a deadline inside the service, so a node that never comes up
fails the job rather than hanging it, and torx bounds each probe on its own,
so a probe that stalls costs one attempt rather than the node's whole
readiness window.

**Output is an artifact.** `StartCaptured` sends each `rqlited`'s output to
`stdout.log` on its node and collects it after the job. It truncates that file
on every start, so before a `Restart` the service moves the crashed process's
log aside as `stdout.<n>.log` and registers it with `AddArtifact`; the results
tree then holds what a crashed leader logged up to its crash.

## Testing the example

`go test ./examples/rqlite/...` runs unit tests for the pure parts -- the
client's request and response handling against rqlite's real response
bodies, the command line the service builds, parameter resolution, membership
checks, the harness's version parsing and run-directory minting -- and one
end-to-end test that runs the whole suite through the real driver/worker
split on a local pool.

The end-to-end test needs `rqlited`. Without it the test skips, saying so;
with `TORX_RQLITE_REQUIRED=1` in the environment a missing binary fails it
instead. CI runs the main test matrix without rqlite and one Ubuntu job with
it, which installs a pinned release, runs the tests in required mode, and
then runs every job through the harness.
