# The torx tutorial

Six suites, each built on the one before, that take torx from a job with no
service to a launcher that provisions docker containers and runs the suite
against them over ssh. Read them in order. Each step's README says what the
step adds, how to run it, and what to look at in the results; each step's
code is the previous step's plus the change, so a diff between two steps is
the lesson:

```sh
diff -r examples/tutorial/02-service examples/tutorial/03-params
```

| Step | What it adds | What you meet |
|---|---|---|
| [`01-hello`](01-hello/) | Two jobs and no service. One passes, one fails. | A suite binary, `Register`, `Declare` and `Run`, selecting jobs, the results tree, what a failure looks like |
| [`02-service`](02-service/) | A service that runs kvd on one node, and a job that writes a key and reads it back. | `ServiceBase` and its four hooks, ports, readiness, captured output, a graceful stop, cleanup, the end-to-end test idiom |
| [`03-params`](03-params/) | A benchmark that runs once per point of a parameter matrix and records what it measured. | `Matrix`, `ResolveParams`, `-params`, `n.Exec`, `Record`, job and service artifacts, `-nodes` and `-parallel` |
| [`04-faults`](04-faults/) | Crashing, stopping, and restarting the server, with every incarnation's log kept. | The process handle, `Shutdown` as an assertion, `CaptureRotate` |
| [`05-multinode`](05-multinode/) | The load generator becomes a service with nodes of its own. | Two services in one job, demand, node binding, why a service advertises the node's address |
| [`06-launcher`](06-launcher/) | A launcher that builds, records, and runs the suite, and then provisions docker nodes and runs it over ssh. | `MakeRunDir` and `-run-dir`, the node manifest and `-pool`, the ssh backend |

## The system under test

Every step tests [`kvd`](kvd/), a small HTTP key-value server written for
the tutorial. `kvd serve` keeps an append-only log and replays it before it
opens its port, answers a readiness endpoint, and exits cleanly on SIGTERM;
`kvd load` is its load generator. It exists so that the tutorial has a server
whose every behavior is known and readable in one file, and so that nothing
needs installing before step 2 beyond building it. It is not a distributed
system: the multi-node steps distribute the load against it, and
[`examples/rqlite`](../rqlite/) is the example that tests a real distributed
database.

## Prerequisites

- Go, as for torx.
- For steps 2 to 5, `kvd` on your `PATH`. torx resolves the system under
  test from each node's `PATH` by name and stages nothing itself, and on the
  local pool the nodes are subprocesses of the suite, so its `PATH` is
  theirs:

  ```sh
  go build -o /tmp/torx-bin/kvd ./examples/tutorial/kvd
  export PATH=/tmp/torx-bin:$PATH
  ```

  or, if `$(go env GOPATH)/bin` is on your `PATH` already:

  ```sh
  go install ./examples/tutorial/kvd
  ```

  A suite whose node has no kvd fails at once and says so. Step 6's launcher
  takes this chore over, which is part of what a launcher is for.
- For step 6's docker part, docker with the compose plugin: Docker Desktop,
  or a docker engine with `docker compose`.

## Running and testing

Every command in the READMEs is run from the repository root, and every
suite runs the same way:

```sh
go run ./examples/tutorial/01-hello              # every job in the suite
go run ./examples/tutorial/01-hello hello.pass   # jobs selected by id, as regular expressions
```

Results land under `./results/<timestamp>/` in the current directory, with
`results/latest` pointing at the newest run; `-results-dir` moves them.

`go test ./examples/tutorial/...` runs each step's own end-to-end test,
which builds kvd into a temporary directory and runs the suite through the
real driver/worker split. Step 6's docker test skips when docker is not
usable.

## Where next

[`examples/rqlite`](../rqlite/) tests a real distributed database with the
same building blocks: a cluster service whose nodes join in order, a crashed
leader and a rolling restart, and a backup taken and restored. Its README
explains each choice, and the torx [README](../../README.md) is the reference
for everything the tutorial touches.
