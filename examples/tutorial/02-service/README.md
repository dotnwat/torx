# Step 2: a service

This step deploys a server. `kvd` -- the tutorial's key-value server, in
[`../kvd`](../kvd/) -- runs on one node, and the job writes a key through its
HTTP API and reads it back. Most of torx's programming model appears here,
so this is the longest step.

Before running it, build kvd onto your `PATH` (the tutorial
[README](../README.md) shows how). torx stages nothing on a node: a service
names the binary it runs, and whoever prepared the node -- you, for now --
guarantees it is there.

## What a service is

A **service** is how to run a process on the nodes allocated to it.
[`kvd.go`](kvd.go) defines one: a struct that embeds `*torx.ServiceBase`,
built by `New` with a name and a node demand -- `torx.Homogeneous(1,
torx.NodeSpec{})`, one node of any kind -- and that implements four per-node
hooks. `ServiceBase` turns those hooks into the lifecycle the framework
drives around a job:

1. Before the job, for each node: `StopNode`, then `CleanNode` -- so the
   service starts from a known state whatever an earlier run left -- then
   `StartNode`; and then `WaitNode` for every node.
2. The job's `Run`.
3. After the job, in reverse: `StopNode`, then the collection of the
   service's artifacts into the results tree, then `CleanNode`.

The hooks, in that order:

**`StartNode`** first checks that kvd is on the node's PATH by running its
version command through `n.Exec`, which runs a command on the node to
completion and returns what it printed and how it exited. Without the
check, an unprepared node would fail at the end of the readiness wait with
a timeout; with it, the job fails at once and the error says what to do.
Then it leases a port with `n.AllocatePort()` -- the allocator is shared by
every node on a host, so two services never lease the same port -- and
launches kvd with `s.StartCaptured`: the process runs with its output
redirected to `stdout.log` in the service's scratch directory on the node,
that file is registered for collection into the results tree, and the call
returns a `torx.Process` to signal, wait for, and stop.

Two choices in the command line matter beyond this step. kvd binds
`0.0.0.0`, every interface, and the service advertises `n.Addr()`, the
node's reachable address, as the address clients dial. On the local pool
that address is the loopback and nothing seems to depend on the choice; on
a pool of real machines, a client on another node reaches the server only
because of it. Step 6 is where it bites. The data directory is
`n.ServiceScratch(name).Sub("data")`: a directory the node gives this
service and no other.

**`WaitNode`** polls kvd's readiness endpoint with `torx.WaitForHTTP` until
it answers, bounded by a deadline. Readiness is a real check, never a sleep,
and a server that never comes up fails the job at the deadline instead of
hanging it. (`torx.WaitForPort` and `torx.WaitUntil` cover servers with no
HTTP endpoint and readiness predicates of any other kind.)

**`StopNode`** stops kvd the way an operator would, with `torx.Shutdown`:
SIGTERM, up to five seconds for the process to exit on its own, and then a
SIGKILL of its whole process group. A process that had to be killed, or
that exited with a non-zero status, is logged as a warning rather than
returned as an error. The node is clean either way, and an error from a
teardown hook marks the node dirty and quarantines it. Step 4 shows the stop
that *is* an assertion. Then the port goes back to the allocator.

**`CleanNode`** removes the service's scratch directory: the data and the
captured log.

## The job

[`smoke.go`](smoke.go). `Declare` constructs the service and registers it
with `jc.Register`. It must be pure -- construct and register, start nothing
-- because the framework calls it once to size the job and again, in the
worker, to rebuild it identically. `Run` gets a client from the service and
does the test. The job runs in the worker process, not on a node, so it
reaches kvd over the network at the address the service advertises;
[`client.go`](client.go) is a small `net/http` client for kvd's API.

## Running it

```sh
go run ./examples/tutorial/02-service
```

```
PASS   kv.smoke  (213ms)
        kvd at 127.0.0.1:60587 answered; wrote and read back "hello, torx"

1 jobs: 1 passed, 0 failed, 0 flaky, 0 ignored
```

The results tree gains the service's directory, with the server's captured
output under the node it ran on:

```
results/latest/
  kv.smoke/
    kvd/node-0/stdout.log
    result.json
    test_log
    events.ndjson
  run.json
```

`stdout.log` is what kvd printed, from its start to its stop:

```
kvd: 2026/09/19 16:17:56.137729 replayed 0 entries from /var/folders/.../torx/node-0/kvd/data/kv.log
kvd: 2026/09/19 16:17:56.138937 listening on [::]:60587
kvd: 2026/09/19 16:17:56.183058 shutting down
kvd: 2026/09/19 16:17:56.183221 stopped
```

and `test_log` shows the lifecycle around the job, each line tagged with the
component that emitted it:

```
16:17:55.974 [worker]  worker.go:140        RUNNING  kv.smoke
16:17:55.974 [worker]  worker.go:141        INFO     bound 1 node(s): node-0
16:17:55.974 [kvd]     service.go:134       INFO     starting
16:17:55.975 [kvd]     capture.go:95        INFO     exec kvd serve --listen 0.0.0.0 --port 60587 --dir /var/folders/.../torx/node-0/kvd/data on node-0
16:17:55.976 [kvd]     service.go:190       INFO     waiting for readiness
16:17:56.180 [kvd]     service.go:198       INFO     ready
16:17:56.182           smoke.go:46          INFO     kvd at 127.0.0.1:60587 holds 1 key(s) after 1 put(s) and 1 get(s)
16:17:56.182 [kvd]     service.go:164       INFO     stopping
16:17:56.185 [kvd]     job.go:187           INFO     collecting from node-0: stdout.log
16:17:56.186 [kvd]     service.go:177       INFO     cleaning
16:17:56.187 [worker]  worker.go:192        FINISHED PASS
```

Without kvd on the PATH, the preflight fails the job before anything
starts:

```
FAIL   kv.smoke  (1ms)
        service: start kvd: torx: service lifecycle operation failed: kvd is not on the PATH of node-0 (see README.md): backend: exec kvd: torx: backend operation failed: exec: "kvd": executable file not found in $PATH
```

## The test

[`tutorial_test.go`](tutorial_test.go) is the idiom every torx suite uses to
test itself, because a service lifecycle needs a live server and the real
driver/worker split:

- `TestMain` checks whether the test binary was started with `worker` and,
  if so, hands the process to `torx.Main`. The driver in the test process
  spawns the test binary again for each job, so the worker is real, and the
  worker launches kvd as a third process.
- `installKVD` builds kvd into a temporary directory and puts it on the
  test's `PATH`, which the workers and their nodes inherit -- the test's
  version of the chore a launcher does for a real run.
- The test builds a local pool the way the driver does, runs the suite with
  `torx.Run(ctx, pool, torx.SelfExecLauncher{}, reqs, opts)`, and asserts
  on the result and on the files in the results tree.

Everything a service and job are made of that does not need a live server
-- address handling, response parsing, readiness predicates -- is ordinary
Go, and should be unit-tested as such; `client.go` is written to make that
possible.
