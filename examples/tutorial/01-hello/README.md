# Step 1: a suite with no service

A torx **suite** is one Go binary. It links torx, registers its jobs, and
calls `torx.Main`. When you run it, it is the **driver**: it discovers the jobs,
allocates nodes to each, runs each one in a **worker** subprocess, and prints the results.
A worker is the same binary, re-executed with the `worker` argument. The suite in this
step of the tutorial has two jobs and needs no server at all, so it shows that machinery
with nothing else in the way.

## The code

[`main.go`](main.go) is the whole suite. It is very short; read it and its
comments before going on.

`torx.Register` takes a stable id and a factory. The driver discovers jobs
from the registry, and a worker uses the factory to rebuild the one it is
handed. Registration happens in an `init` function, so a job is discoverable
as soon as its file is linked into the binary.

A job embeds `torx.JobBase` and implements two methods. `Declare` is where it
registers the services it needs; these jobs need none. `Run` is the test
body: it returns `nil` to pass and an error to fail, and the error's text is
what the results record. `jc.Log` writes a line to the job's log, and
`jc.SetSummary` sets the one line the report prints under the job's status.

A real suite has the same shape at a larger size: many files of jobs, the
services they deploy, helpers for the system under test, all compiled into
the one binary. That is what makes a suite simple to version and deploy: it
is one artifact, built from one commit, copied to wherever it runs. Step 6
does exactly that, and builds it for the platform of the nodes it runs on.

## Running it

```sh
go run ./examples/tutorial/01-hello
```

```
FAIL   hello.fail  (1ms)
        this job always fails
PASS   hello.pass  (1ms)
        nothing to test, and it passed

2 jobs: 1 passed, 1 failed, 0 flaky, 0 ignored
```

One job fails on purpose, so the exit status is 1. Positional arguments
select jobs by id, as regular expressions:

```sh
go run ./examples/tutorial/01-hello hello.pass   # exit status 0
```

## The results tree

Each run writes a directory under `./results/` and points `results/latest` at
it. The `-results-dir` flag chooses the root, and an empty value turns the tree
off.

```
results/2026-09-19T23-16-28Z-1748229304/
  hello.fail/
    result.json      status, summary, the error, timing, and any recorded data
    test_log         the job's log lines, with the framework's around them
    events.ndjson    every event the worker emitted, one JSON object per line
  hello.pass/
    result.json
    test_log
    events.ndjson
  run.json           every job's result, as the driver collected them
```

The failing job's `result.json` carries its error:

```json
{
  "id": "hello.fail",
  "status": "FAIL",
  "start": "2026-09-19T16:34:00.094802-07:00",
  "stop": "2026-09-19T16:34:00.095261-07:00",
  "error": {
    "message": "this job always fails"
  }
}
```

and the passing job's `test_log` shows what the worker did around `Run`:

```
16:34:00.099 [worker]  worker.go:140        RUNNING  hello.pass
16:34:00.099 [worker]  worker.go:141        INFO     bound 0 node(s):
16:34:00.099           main.go:38           INFO     hello from a torx job
16:34:00.099 [worker]  worker.go:192        FINISHED PASS
```

A job with no service is bound to no nodes at all: the number of nodes a job
needs is the sum of what its services need. The driver still built a **pool**,
the set of nodes a run owns and hands out to its jobs. Here it is a pool of local
nodes, sized to the largest job and never smaller than one, and the driver gives each
job a disjoint share of it while it runs. Step 2 declares a service, and
gets a node.

## Flags to know

`-results-dir DIR` chooses where the results tree is written, `-results FILE` also writes every
result as newline-delimited JSON to one file, `-parallel N` runs up to N
jobs at once, and `-nodes N` fixes the local pool's size. The later steps use
each of them.

## The test

[`hello_test.go`](hello_test.go) runs the suite from `go test`, through the
real driver and worker: `TestMain` lets the test binary stand in for the
suite binary's worker role, and the test calls `torx.Run` with a local pool
and `torx.SelfExecLauncher`, which re-executes the test binary as the worker
for each job. Step 2 explains the idiom in full; here it only checks that
the two jobs end the way they should and that their files are in the tree.

A suite is a program, so it has a test of its own, as any Go program does;
this is it. torx does not need it and nothing in the framework knows it
exists, but every step of the tutorial has one, because a suite that is
wrong says nothing about the system it tests. Nor is `go test` another way
to run the suite: it runs the suite on a local pool in a temporary
directory, in a second, to check the suite itself. Running the suite against
the system you mean to test, on the nodes you mean to test it on, is what
the driver is for, and what step 6 sets up.
