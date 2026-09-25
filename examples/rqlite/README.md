# examples/rqlite — a QA suite for a real distributed system

[`examples/tutorial`](../tutorial/) teaches torx's programming model step by
step. This example shows what a project actually builds around torx: a suite
that tests a real distributed database, and the launcher that a team runs it
through.

The system under test is [rqlite](https://rqlite.io), a distributed SQLite
replicated with Raft. It suits a test suite well: one static binary, a plain
HTTP and JSON API, two ports per node, a readiness endpoint, a membership
endpoint that names the leader, and leader elections that finish in a couple
of seconds -- so every job here runs in seconds, apart from the chaos job,
which runs for as long as it is told to, and the whole suite in under a
minute.

Two things live here:

- **`qa/`** is the suite: a torx binary with six jobs, and the service
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
pointing at the newest run. The harness mints it through `torx.MakeRunDir`, so
apart from what the harness adds the tree is the one a bare `-results-dir` run
of the suite would leave:

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
    rqlite/node-2/stdout.1.log    ... and, for a node that was stopped and
                                  restarted, the output of the first process
  rqlite.backup/
    backup.db                     the backup the job took, as rqlite restores it
    backup.sql                    the same backup as a SQL dump, for reading
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

**`rqlite.rolling`** is the failover job's graceful twin: a rolling restart
that stops every node in turn with SIGTERM, the way an operator would. rqlite
has a leader step down before it exits on that signal, so the cluster hands
leadership over instead of waiting out an election. For each node the job
stops it, waits until the survivors report a leader, writes a hundred rows
through that leader, restarts the node, and waits until it has received what
it missed before moving on to the next. The stop itself is under test: a
node that ignores the signal, or exits with a non-zero status, fails the
job. At the end every node's own copy is checked for every row and the
membership for wholeness. The time from each leader's stop until its
successor is reported is recorded beside the failover job's election time,
so the two numbers -- a handoff of about a hundred milliseconds against an
election of a couple of seconds -- sit side by side in the results.

**`rqlite.backup`** checks that a backup restores the cluster. It writes a
thousand rows at the leader, takes a backup there in both forms rqlite offers
-- the SQLite file a restore loads, and the SQL dump a person can read -- and
attaches both to its results with `jc.WriteArtifact`, so they land beside the
job's `result.json` as `backup.db` and `backup.sql`. It then damages the
database, dropping the table and creating another the backup knows nothing
of, and restores by loading the backup at a follower, which forwards it to
the leader the way an operator's load would go. The leader must serve every
row again with the junk table gone -- a loaded backup replaces the database,
where a loaded dump would only run its statements on top -- and every node's
own copy must converge on the restored rows. The backup and restore times and
sizes are recorded in the result's data. The job is what job-level artifacts
exist for: the backup arrives in the job's own process, not on any node, so
no service could have collected it.

**`rqlite.chaos`** is a randomized test of rqlite's guarantees under faults,
in the manner of a Jepsen test. Clients add unique values to a table, through
plain writes and through rqlite's write queue, and read them back at every
consistency level, from whichever node they pick, while a nemesis injects one
fault at a time: crashing a node (SIGKILL), crashing a majority, stopping one
gracefully (SIGTERM), pausing one (SIGSTOP, then SIGCONT), each aimed at the
leader more often than not, and having a node snapshot, reap its snapshot
store, or step down as leader. Run with `-netns`, which gives each node a
network of its own, it also partitions the network (isolating the leader,
splitting the nodes in two, or bridging two halves through one node), has
the leader stop hearing its peers while they still hear it, slows or drops
one node's packets, and black-holes the large packets from the leader to a
follower while letting the small ones through, as a link with a broken MTU
would -- faults injected with torx's `netfault` package. rqlite runs a configuration drawn at random:
snapshots every few entries instead of every few thousand, snapshot checks
several times a second, a WAL threshold that snapshots on nearly every
write, VACUUMs alongside, fast or slow elections. At the end every fault is
healed, every node's write queue is flushed, and the job reads the table at
the strong level. Then it checks:

- the history of every operation (`checkSet` in `checkset.go`): no
  acknowledged write lost; no read at `linearizable` or `strong` missing a
  value known to be committed before it began, where a value is known
  committed once its write was acknowledged or any read of any level saw it;
  no value applied twice, none nobody wrote, none whose write definitely
  failed. A write that failed with no answer as to whether it took effect (a
  timeout, leadership lost while committing) may go either way. A `none`
  read bounded by `freshness=1s` and `freshness_strict` must not trail by
  more than that second. A stale `weak` read is a warning, since rqlite
  documents that a just-deposed leader may serve one.
- that every node's own copy converges on the final read;
- that no `rqlited` exited on its own, and that a graceful stop exited
  within its grace period, not killed. A stop that hangs is sent SIGQUIT
  before the kill, so its node's log ends in the stacks of every goroutine.

The operation history is attached as `history.ndjson`, and what the checks
found as `anomalies.json`. Every choice the job makes, from the configuration
to each fault and each client operation, is drawn from the variant's seed
(`jc.Rand`), so a failing variant is rerun with the run's `-seed` and the
same selection.

The compiled-in variant runs for twenty seconds and tolerates the issues
below, reporting them as warnings, so a run of the whole suite passes on a
release that has them. A search for bugs is a `-params` run, strict by
default, with longer runs, many trials, and a pool large enough to run them
side by side:

```json
{ "rqlite.chaos": { "matrix": { "duration": [60], "trial": [1, 2, 3, 4, 5, 6, 7, 8] } } }
```

```sh
go run ./examples/rqlite/harness -netns -params hunt.json -parallel 8 -nodes 24 'rqlite\.chaos'
```

Without `-netns` the network faults are left out; a variant that names one
fails, saying why. The harness passes `-seed` on to the suite, so a failure
is rerun with the seed its run printed.

Its parameters are `nodes`, `duration` (seconds), `clients`, `faults` (a
comma-separated list from `nemesis.go`, or `all`), `queued` (whether clients
also write through the queue), `tolerate` (anomaly kinds reported as
warnings), and `trial`.

### What it found in rqlite v10.3.6

- **Queued writes applied twice** (`duplicate`). rqlite's write queue
  retries a batch whose execution failed, including when the failure says
  nothing about whether the batch committed: "leadership lost while
  committing log", or a connection to the leader lost mid-request. When the
  first attempt did commit, the retry applies the batch a second time, and a
  client that waited for the write (`queue&wait`) is told it succeeded once.
  Most runs of the job that crash or pause the leader show it. The request
  forwarding a follower does for any write has the same shape: it retries
  once even when asked for no retries.
- **A graceful stop that hangs** (`shutdown-hang`), for two reasons the
  goroutine dumps the job takes tell apart.
  - *For a minute, on a follower whose leader went silent.* A follower
    forwards writes and some reads to the leader, and the forwarding ignores
    the request being cancelled: against a leader that has gone silent, each
    forwarded request holds its handler for its full timeout, thirty
    seconds, and then retries for thirty more, whether or not its client is
    still there. rqlited's SIGTERM path waits, without a deadline, for every
    in-flight request to finish. The dump shows the main goroutine in
    `http.(*Server).Shutdown` and a handler in `cluster.(*Client).Execute`,
    reading from the silent leader.
  - *Forever, on a leader, in hashicorp/raft.* The dump shows the main
    goroutine in `store.(*Store).Close`, waiting for Raft to shut down, and
    a replication goroutine per follower blocked in
    `netPipeline.AppendEntries`. That is a deadlock in hashicorp/raft
    v1.8.0's pipelined replication: when the goroutine decoding a
    follower's responses gives up on one (the follower rejected the append,
    or answered with a newer term, as every follower does once a stopping
    leader has handed over), nothing reads the pipeline's responses any
    more, and with rqlite's settings the pipeline's channels are unbuffered,
    so the next two sends wedge the replication goroutine for good. Raft's
    shutdown waits for it forever. Short of a shutdown, the same wedge
    stops replication to that follower while heartbeats keep it from
    calling an election -- the symptom of hashicorp/raft#612. It reproduces
    with hashicorp/raft alone, without rqlite, and having the decoding
    goroutine close the pipeline when it gives up fixes it.

- **`freshness_strict` serving data seconds out of date**
  (`stale-fresh-read`). rqlite documents that a `none` read with `freshness`
  and `freshness_strict` checks that "the data it last received is not
  out-of-date by (at most) the freshness interval". A follower decides it is
  caught up when it has applied every command entry it has *received*, and
  only looks at how old its data is when it is behind that. A follower that
  hears the leader's heartbeats but receives no new entries -- behind a link
  that loses large packets, or in the moments after one heals, while TCP's
  backoff holds back the stalled replication stream; or behind the Raft
  replication wedge above, for good -- has received nothing it has not
  applied, so it serves its old data as fresh. Under the MTU black hole the
  job finds reads five and six seconds stale with a freshness of one
  second, and a follower held that way serves the same stale count for as
  long as the fault lasts.

Two smaller things show up as warnings: a stop takes five seconds when a
client holds a connection that has not yet sent a request (Go's HTTP server
waits that long for such a connection during shutdown), and a node stopped
while it is still joining the cluster exits with status 1.

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
`Shutdown` stops it gracefully and keeps them too, and `Restart` launches a
new process behind the same addresses on the same data directory: the node
comes back as the member that went away, and its Raft log on disk lets it
catch up. `StopNode`, which the framework calls at teardown and before every
start, is the one that ends a membership and releases the ports. A job
injects faults only through these methods, never by reaching past the
service to the process.

**Stops are graceful, and one of them is under test.** `StartCaptured` returns
a `torx.Process`, and the service stops one with `torx.Shutdown`: SIGTERM,
a grace period for rqlite's own shutdown -- the leader stepping down, the
store snapshotting and closing -- and a SIGKILL of the process group only if
that runs out. `StopNode` uses it so every job's teardown goes through the
real shutdown path, but it only logs a process that had to be killed or
exited unclean: the node is clean either way, and a teardown error would
mark it dirty and quarantine it. `Shutdown`, the method the rolling job
calls, is where the same stop is an assertion -- a timeout or a non-zero
exit fails the call, and so the job. `Crash` stays SIGKILL, so the failover
job's crash is still a crash. The one stop that does fail the teardown is a
`Crash` or `Shutdown` that could not establish its process was gone -- the
kill could not be carried out, or the transport lost track of it. The member
is left unstopped: `Restart` refuses it, and `StopNode` fails with that error,
so a node that may still be running the old `rqlited` is quarantined rather
than reused.

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

**Faults beyond the crash.** `Pause` and `Resume` stop a node's process in
its tracks and continue it, the fault that makes a node go silent rather than
die; a paused process is resumed before any stop, so SIGTERM can act.
`SetFlags` adds rqlited flags to every launch, which is how the chaos job
runs its drawn configuration, and `SetStopGrace` changes the grace period.
Every process is watched: one that exits without the service having stopped
it is recorded, and `Exits` returns them, so a job can tell a crash it caused
from one it did not.

**A stop that hangs leaves its stacks.** The service stops a process with
`torx.Stop` and a `Dump` of SIGQUIT, so rqlited, when it has not exited by the
end of the grace period, prints every goroutine's stack into its log before
it is killed.

**Output is an artifact.** `StartCaptured` sends each `rqlited`'s output to
`stdout.log` on its node and collects it after the job. The service sets the
`CaptureRotate` policy, so a `Restart` moves the stopped process's log aside
as `stdout.<n>.log` instead of discarding it; the results tree then holds what
a crashed leader logged up to its crash, or a stopped one up to its stepdown,
beside what its replacement logged.

## Testing the example

`go test ./examples/rqlite/...` runs unit tests for the pure parts -- the
client's request and response handling against rqlite's real response
bodies, the command line the service builds, parameter resolution, membership
checks, the chaos job's checker and error classification, the harness's
version parsing -- and one end-to-end test that runs the suite through the
real driver/worker split on a local pool. The end-to-end test leaves out
`rqlite.chaos`, which draws its faults at random and runs for its duration;
the harness run in CI covers it.

The end-to-end test needs `rqlited`. Without it the test skips, saying so;
with `TORX_RQLITE_REQUIRED=1` in the environment a missing binary fails it
instead. CI runs the main test matrix without rqlite and one Ubuntu job with
it, which installs a pinned release, runs the tests in required mode, and
then runs every job through the harness.
