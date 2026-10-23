# examples/nats — hunting for bugs in NATS JetStream

[NATS](https://nats.io) is a messaging system; its JetStream layer keeps
messages in streams, each a log replicated with Raft across a cluster's
servers. A stream gives every message a sequence of its own, files it under
its subject, and answers reads of a sequence or of a subject's last message;
a publish may be conditional on the stream's last sequence or the subject's
(the compare-and-set a key-value bucket's `Update` is), deduplicated by a
message id, an increment of a counter (`Nats-Incr`), or one of an atomic
batch. Consumers deliver a stream's messages and track their acks; mirrors
and sourcing streams copy other streams.

This suite holds JetStream to its guarantees while a nemesis crashes,
pauses, partitions, and freezes its servers, fills their disks, and moves
streams' leaders and replicas.

- **`qa/`** is the suite: a torx binary with three jobs -- `nats.smoke`,
  `nats.chaos`, and `nats.deposed` -- the service package (`qa/nats`) that
  runs a `nats-server` on each torx node and speaks the JetStream API, and
  the checkers. The clients are the nats.go client in the worker; the suite
  sends JetStream requests itself, so that nothing retries one behind its
  back and every answer is classified the same way.
- **`harness/`** builds the suite, records the `nats-server` under test in
  the run directory, and execs the suite.

The example is a Go module of its own (`go.mod` here), so torx's own module
does not depend on nats.go.

## Requirements

- Go.
- A `nats-server`. Build it from source, a release and `main`, each also
  with the race detector:

  ```sh
  git clone https://github.com/nats-io/nats-server ~/src/nats-server
  cd ~/src/nats-server && git worktree add ../nats-server-2.15.1 v2.15.1
  B=~/.cache/torx-nats/bin
  (cd ../nats-server-2.15.1 && go build -o $B/2.15.1/nats-server . && go build -race -o $B/2.15.1-race/nats-server .)
  go build -o $B/main/nats-server . && go build -race -o $B/main-race/nats-server .
  ```

  nats-server's `go.mod` names a newer toolchain than a Go of the same
  minor version may have; `GOTOOLCHAIN=local` builds with the one at hand
  rather than downloading another.
- Network faults need `-netns`, disk-full faults a node that can mount
  (`-netns` nodes can), and freezes `-cgroups`; a run without them leaves
  those faults out. `nats.deposed` needs `-netns`.

## Running

From `examples/nats`:

```sh
go run ./harness nats.smoke
go run ./harness -netns -cgroups nats.chaos
go run ./harness -netns 'nats\.deposed'
go run ./harness -netns -cgroups -params hunt.json -parallel 8 -nodes 40 'nats\.chaos'
go run ./harness -nats ~/.cache/torx-nats/bin/main-race/nats-server -netns -cgroups 'nats\.chaos'
```

with, for a hunt, a `-params` file such as

```json
{ "nats.chaos": { "matrix": { "duration": [180], "trial": [1, 2, 3, 4, 5, 6, 7, 8] } } }
```

`-nats PATH` names the `nats-server` under test; the default is the
2.15.1 build above. Each job takes five nodes. Send the harness's output to
a file rather than through `tail`: a worker that hangs is sent SIGQUIT, and
the stacks it prints are also kept in its `worker.log`.

## The job

**`nats.chaos`** runs a server on each of `nodes` nodes (5), routed into
one cluster, with a system account and an application account. Its streams
are file streams (memory streams a quarter of the time), each of
`replicas` (3) replicas:

| stream | what clients do with it |
|---|---|
| `reg` | registers: each subject `reg.<k>` is one, its value the subject's last message. Clients `put` unconditionally, `cas` on the subject's last sequence, and read the last message from the stream's leader (`get`) and from any replica (`dget`, a direct get) |
| `log` | `append` on condition of the stream's last sequence |
| `ctr` | a counter stream: `incr` adds to `ctr.<k>`, `cget` reads it |
| `batch` | atomic batches of one to six messages over several subjects, half of them conditional on the first subject's last sequence |
| `work` | `enqueue`d messages, which `consumers` (2) clients fetch from a durable pull consumer and ack (waiting for the server to confirm), nak, or leave to be redelivered |
| `sched` | `schedule`d messages: each on a subject of its own, due a few seconds on, for the stream to publish to a target subject and purge |
| `regm` | a mirror of `reg` |
| `agg` | sources `reg`, `batch`, and `work` |
| `ctragg` | a counter stream that sources `ctr` |

The copies source through ephemeral consumers, or, half the time, through
durable push consumers on their origins acked by flow control (2.15). Each
client (`clients`, 8) issues `rate` (100) operations a second; a write
whose answer was lost is sent again, under its message id, half the time.
The streams' storage, compression, sync interval, direct gets, and
duplicate window are drawn from the seed (`config=random`), or pinned
(`storage`, `sync`).

The faults (`faults=all`, a list, `all,-name`, or `all,+name` for one off
by default):

| fault | what it does |
|---|---|
| `crash`, `crash-minority` | kills one server, or as many as a majority survives, and restarts them after the hold |
| `restart`, `lame-duck` | stops a server with SIGTERM, or with SIGUSR2 (lame-duck mode, which moves its leaders off and its clients away), and starts it again |
| `pause`, `freeze` | SIGSTOPs a server, or freezes its node's cgroup |
| `partition-one`, `partition-halves`, `partition-ring` | cuts a server off, splits the servers in two, or lets each reach only its neighbors in a ring |
| `stepdown`, `stepdown-meta`, `stepdown-consumer` | has a stream's leader step down (in favor of a peer of the nemesis's choosing, or not), the meta leader, or the work consumer's |
| `scale`, `scale-cancel` | changes a stream's replica count and back, or cancels the change |
| `peer-remove`, `evacuate` | removes one of a stream's replicas, or moves it |
| `consumer-peer-remove`, `consumer-evacuate` | the same for the work consumer |
| `server-evacuate` | moves every stream's replica off a server |
| `update-config` | toggles a stream's direct gets or compression, or changes its duplicate window |
| `disk-full` | fills a server's store; a server that shuts its JetStream down for want of space (by design) is restarted, as an operator would |
| `wipe` (off) | crashes a server and loses its store |

A supervisor restarts what exited on its own, and a server left with its
JetStream shut down or a stream stuck on a write error.

Once every fault is healed and the cluster is healthy, the consumers drain
the work stream, the copies catch up, and each stream is read back from
every replica in turn, made the stream's leader for the read. Then:

- every stream's replicas must hold the same messages;
- [`logcheck`](../../logcheck/) holds each stream's writes to its final
  contents: every acked write there once, where its ack said, at most once
  for one with a message id; no refused write; nothing no client wrote; a
  conditional write after the sequence it named; and the stream's order of
  writes the order real time put them in. It checks reads of a subject's
  last message from the leader for linearizability, with a refusal that
  names the subject's last sequence ("wrong last sequence: N") read as a
  read of it; direct gets may be stale, and a stale one is its own kind;
- a batch is in its stream whole or not at all, its messages in order, one
  after another;
- each counter's value is the one before it plus its increment, and acks
  and reads report the values the stream holds;
- each schedule the stream took fired once, no sooner than its time, and
  was purged; none it refused fired;
- the consumer delivered every message, first in the stream's order, what
  the stream holds at each sequence, never a consumer sequence twice, and
  never a message again after confirming its ack;
- the mirror holds what `reg` does, sequence for sequence; `agg` holds each
  origin message once, in each origin's order; `ctragg`'s counters end
  where `ctr`'s do;
- no server exited on its own, and no log holds a panic or, in a `-race`
  build, a data race.

A run writes `history.ndjson` (every client operation), `deliveries.ndjson`,
`nemesis.ndjson`, each stream's `final-<stream>.ndjson`, and `check.json`:
the anomalies, the settings drawn, the servers restarted and why, and
each stream's state on each replica before the final reads moved any
leader.

**`nats.deposed`** reproduces one finding, below: a stream's leader cut off
from its peers but not from the server a client publishes through refuses
the client's conditional writes from its stale state while the leader its
peers elected stores them.

## Known issues

`allow` lists issues a run reports as warnings rather than failures, so
that a run that keeps finding one can find others; by default, all of them.
An issue the checker cannot tell apart from others is sidestepped instead.

| issue | what it is |
|---|---|
| `stale-direct-read` | a direct get is served by any replica, which may be behind |
| `stale-read` | a leader read that breaks linearizability while a fault silenced a server, or soon after: a deposed leader answering, since JetStream has no leader leases |
| `deposed-refusal` | a write refused, and stored, while a fault silenced a server or soon after: a deposed leader refused a conditional write the new leader stored |
| `flush-error` | a conditional write that broke its condition, a leader read that broke linearizability, or a refused write stored, while a disk was full or soon after: a leader whose flush failed checked writes against a store that had dropped messages (finding 2) |
| `compress-truncate` | sidestepped: a run never draws S2 compression with sync always (finding 1) |
| `copy-stall` | a copy behind its origins once every fault healed, which a new leader brings up to date (finding 3) |
| `snapshot-sources` | sidestepped: the servers run with the feature flag `js_snapshot_sources` off (finding 5) |

## Findings

Against nats-server 2.15.1 and `main` (2026-10-20); each has an issue with
a reproducer, tracked in
[#67](https://github.com/dotnwat/torx/issues/67):

1. [#62](https://github.com/dotnwat/torx/issues/62) A file stream compressed with S2 corrupts its last block when its store
   is truncated within that block -- as a replica that syncs always does on
   every restart after a crash -- and every message in the block becomes
   unreadable on that replica; once one led a stream, the stream's leader
   answered every read of its history "no message found". A
   `fileStore.Truncate` that needs no tombstone leaves the block it cut the
   write block, but the new block it made for tombstones had the old last
   block compressed in the background. 2.14.8 too; not 2.12.15.
2. [#63](https://github.com/dotnwat/torx/issues/63) A write error while a replica flushes its buffered messages -- its disk
   filled for a moment -- drops messages its store had applied. On the
   stream's leader, until the stream notices, conditional writes are
   checked against what is left: one whose condition is already false
   commits, a lost update. On any replica, the stream is out of service
   there until the server restarts, though the disk works again.
3. [#64](https://github.com/dotnwat/torx/issues/64) A mirror, or a stream that sources others, stops copying, and reports
   no lag, until its leader changes, after it is scaled to one replica and
   back while its origins take writes, or after its origin loses a replica.
   2.12.15 and 2.14.8 too.
4. [#65](https://github.com/dotnwat/torx/issues/65) A stream's leader cut off from its peers keeps answering publishes and
   refuses conditional ones from its stale state ("wrong last sequence")
   while the leader its peers elected stores them, for as long as it takes
   it to notice -- about fifteen seconds. A client told its write failed
   retries a write that took effect.
5. [#66](https://github.com/dotnwat/torx/issues/66) With the feature flag `js_snapshot_sources` (on by default from 2.16, on
   `main`), a stream that sources others, led by a replica that crashed
   after its last snapshot, sources again everything it had sourced since.
