# Step 4: faults

Two new jobs inject faults on the one node. `kv.durability` writes a
hundred keys, kills kvd without warning, restarts it, and reads every key
back; `kv.graceful` does the same with a graceful stop, and holds the stop
itself to a standard. The diff from step 3 is [`faults.go`](faults.go) and
the service's new methods in [`kvd.go`](kvd.go): `Crash`, `Shutdown`, and
`Restart`.

A job injects faults only through the service's methods, never by reaching
past it to the process. The service owns the process handle and the
invariants around it; the job says what should happen and asserts on the
result.

## The process handle

`StartCaptured` returned a `torx.Process` back in step 2, and this step uses
all of it. `Signal` sends a signal to the program itself, not a shell around
it; `Wait` reports how it exited; `Close` kills its whole process group and
reaps it. `torx.Shutdown(ctx, p, sig, grace)` composes them into the stop an
operator would do: the signal, a bounded wait for the exit, and the kill if
the grace period runs out, reported as `torx.ErrShutdownTimeout`.

The service uses them three ways, and the difference between the last two
is the point of this step:

- **`Crash`** calls `Close`: SIGKILL, so none of kvd's shutdown code runs.
- **`Shutdown`** calls `torx.Shutdown` with SIGTERM and *asserts* on the
  outcome: a process that did not exit within the grace period, or exited
  with a non-zero status, fails the call and so the job. The stop is under
  test.
- **`StopNode`** -- the teardown's stop, unchanged since step 2 -- calls the
  same `torx.Shutdown` but only *logs* a process that had to be killed or
  exited unclean. The node is clean either way, and an error from a
  teardown hook marks the node dirty and quarantines it; a job that passed
  should not lose its node over a server that was slow to stop.

## Ports are identity

The port kvd was leased is its address as far as a job is concerned, so
`Crash` and `Shutdown` keep it, and `Restart` launches a new process behind
it on the same data directory. Only `StopNode`, when the framework stops the
service, releases it. `Addr()` is therefore stable across a fault, and a job
holds one address for the whole run.

## A kill that could not be carried out

`Crash` and `Shutdown` clear the process only once it is established to be
gone. If the kill could not be delivered -- or, for `Shutdown`, the wait was
cut short or the transport lost track of the process -- the handle is kept:
`Restart` refuses to launch beside a process that may still be running, and
the teardown's `StopNode` tries the stop again and fails the job if it
cannot, so a node that may still be running a stray kvd is quarantined
rather than reused. (`torx.Shutdown` reports a timeout only once its kill
went through, so a timeout means the process is gone.)

## Every incarnation's log

`New` sets one policy:

```go
s.SetCapturePolicy(torx.CaptureRotate)
```

By default, `StartCaptured` truncates `stdout.log` when it launches a second
process on a node, so the results would only ever show the last one. With
`CaptureRotate`, the previous process's output is moved aside as
`stdout.1.log` and collected too. After `kv.durability`, the crashed
server's log ends where the kill caught it:

```
kvd: 2026/09/19 16:23:14.796797 replayed 0 entries from /var/folders/.../node-0/kvd/data/kv.log
kvd: 2026/09/19 16:23:14.797499 listening on [::]:61317
```

and its replacement's shows the replay of what the crashed one had
recorded, then the teardown's graceful stop:

```
kvd: 2026/09/19 16:23:14.916581 replayed 100 entries from /var/folders/.../node-0/kvd/data/kv.log
kvd: 2026/09/19 16:23:14.916839 listening on [::]:61317
kvd: 2026/09/19 16:23:15.017992 shutting down
kvd: 2026/09/19 16:23:15.018086 stopped
```

## The jobs

Both follow the same shape: write, fault, restart, wait, verify. After
`Restart`, the job calls `j.db.Wait(ctx)` -- the same readiness wait the
framework did before `Run`, which returns once the new process answers,
having replayed its log -- and takes a fresh client, because the old one's
connections died with the process. Each records what it timed:
`kv.durability` the time from the restart to readiness, `kv.graceful` the
time SIGTERM took.

```sh
go run ./examples/tutorial/04-faults 'kv\.durability' 'kv\.graceful'
```

```
PASS   kv.durability  (387ms)
        100 keys survived a crash; kvd was back in 109ms
PASS   kv.graceful  (247ms)
        kvd stopped cleanly in 2ms and came back with all 100 keys

2 jobs: 2 passed, 0 failed, 0 flaky, 0 ignored
```

```
results/latest/
  kv.durability/
    kvd/node-0/stdout.1.log   the crashed process
    kvd/node-0/stdout.log     its replacement
    result.json
    test_log
    events.ndjson
  kv.graceful/
    kvd/node-0/stdout.1.log
    kvd/node-0/stdout.log
    ...
```

`test_log` for `kv.graceful` shows the rotation happening between the two
launches, and the collection gathering both logs at the end.

## Next

Step 5 adds nodes.
