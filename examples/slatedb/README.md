# examples/slatedb — hunting for bugs in SlateDB

[SlateDB](https://github.com/slatedb/slatedb) is an embedded key-value store,
an LSM tree kept in object storage. One writer appends to a write-ahead log
of objects in a bucket and flushes its memtables to SSTs there; a
compactor, its compaction workers, and a garbage collector work over the
same bucket, in the writer's process or processes of their own; readers
follow along. Nothing is shared but the bucket, so SlateDB stakes its
guarantees on the object store's: a write is atomic, a read sees the latest
write, and a write with `If-None-Match` or `If-Match` takes effect only if
its condition holds. On those it builds writer fencing (a new writer fences
the old one, whose writes then fail), durability (a write is durable once
its WAL object is), and snapshot and serializable-snapshot transactions.

This suite holds SlateDB to its guarantees while a nemesis crashes, pauses,
and fails over its processes, and faults the object store under each of
them: requests that fail before they take effect or after, that are slow,
that land after their client gave up, or that hang.

Three things live here:

- **`node/`** is `slatedb-node`, a small Rust program that runs one
  SlateDB process -- a writer, a reader, a standalone compactor, a
  compaction worker, or a garbage collector -- over a database in an S3
  bucket, and serves its API (puts, deletes, batches, reads, scans,
  transactions, snapshots, flushes) as JSON over HTTP. SlateDB is a library;
  this is the process the suite starts, kills, and pauses. Its clock is
  SlateDB's, offset by an amount the suite sets, at start (`--clock-offset-ms`)
  or at any time (`POST /clock`).
- **`qa/`** is the suite: a torx binary with one job, `slatedb.chaos`, the
  service package (`qa/slatedb`) that runs SlateDB's processes on torx
  nodes, and the checker. The bucket is served from the job's worker by
  torx's `objstore`, an in-memory S3 server whose faults the job injects
  and whose every request it records.
- **`harness/`** builds the suite and `slatedb-node` -- against a SlateDB
  checkout, or the release `node/Cargo.toml` names -- puts the node first on
  the suite's PATH, records the invocation in the run directory, and execs
  the suite.

## Requirements

- Go, and Rust (`cargo`; https://rustup.rs). The first build of the node
  compiles SlateDB and its dependencies, which takes a few minutes; later
  ones reuse `$XDG_CACHE_HOME/torx-slatedb/target`.
- Nothing else: the object store is in the suite. Each SlateDB process takes
  a few hundred MiB of memory.

## Running

From anywhere in the repository:

```sh
go run ./examples/slatedb/harness slatedb.chaos                      # the release
go run ./examples/slatedb/harness -slatedb ~/src/slatedb slatedb.chaos  # a checkout
go run ./examples/slatedb/harness -slatedb ~/src/slatedb -nodes 24 -parallel 8 -params hunt.json
```

with, for a hunt, a `-params` file such as

```json
{ "slatedb.chaos": { "matrix": {
    "trial": [1, 2, 3, 4], "duration": [120],
    "standalone": [false, true], "workers": [0, 2] } } }
```

Each job takes three nodes, so `-parallel 8` needs `-nodes 24`: without
`-nodes` the pool is sized to one job, and jobs run one at a time.

## The job

**`slatedb.chaos`** runs a writer, and with `reader` a `DbReader`; with
`standalone`, a compactor and `collectors` garbage collectors of their own
(and with `embedded-gc` the writer's collector as well); with `workers`,
compaction workers that run the compactor's jobs. Clients then work for
`duration` seconds while a nemesis injects one fault at a time.

Each client runs, one at a time against the current writer:

- puts and deletes of a few keys (`keys=16`), most awaiting durability,
  some not; batches of puts and deletes;
- reads and scans of durable data only (`DurabilityLevel::Remote`), some
  from the reader;
- list-append transactions over keys of their own (`txn-keys=8`) at the
  `isolation` level, `ssi` or `si`;
- with `merges` (on by default), blind appends through an append merge
  operator to keys of their own, and reads of them;
- snapshots, scanned two to four times over a few seconds.

With `segments=3`, every key is in a segment of its own first three bytes
(RFC 0024's segmented compaction), each its own LSM tree.

Every value written is unique, and its bytes derive from its name, so every
read is checked byte for byte.

The faults (`faults=all`, or a list, or `all,-name`):

| fault | what it does |
|---|---|
| `crash-writer` | kills the writer, restarts it |
| `restart-writer` | closes the writer with SIGTERM, restarts it |
| `failover` | pauses the writer, opens the other, resumes the first -- now a zombie -- sends it writes, which must fail, and stops it |
| `failover-live` | opens the other writer while the first runs |
| `crash-failover` | kills the writer, opens the other |
| `store-errors` | object-store requests fail, before or after they take effect, or lose their connection |
| `store-slow` | requests are delayed, or land late, after their client gave up |
| `store-partition` | a process's requests hang |
| `crash-aux`, `pause-aux` | kill or pause the compactor, a worker, a collector, or the reader |
| `clock-skew` | restarts a process with its clock off by up to `max-skew` ms (off by default) |
| `clock-jump` | steps a running process's clock (off by default) |

Store faults pick one process or all, all requests or one kind (writes,
reads, lists, deletes), and all keys or one prefix (`manifest/`, `wal/`,
`compacted/`, `compactions/`). Unless `config=default`, each run draws
SlateDB's settings from its seed: small memtables and SSTs, frequent
flushes, compactions, and collections with short `min_age`s, small bounds
on unflushed data, and readers whose checkpoints outlive a pause or not.

## What is checked

Once the run's time is up and every fault is healed, the job brings the
writer back, reads every key, reads every transaction key in one
transaction, and opens a fresh reader, which must read what the writer did.
Then the history is checked:

- **Linearizability** of each key: the writer's reads of durable data, and
  its durable writes, must be explained by some order of them. A write
  that did not await durability may be lost -- unless a later write of the
  same writer was acknowledged durable, for SlateDB makes writes durable in
  sequence order, so the earlier one must survive too.
- **Snapshots**: every scan, and every scan of a snapshot, must read the
  state after some prefix of the writes in sequence order (a snapshot, at
  its own sequence number), and every scan of one snapshot the same.
- **Readers** never go back: once a reader has read a key at some
  sequence number, it reads it there or later.
- **Transactions** must show none of the anomalies their isolation level
  forbids, as torx's `listappend` finds them; nor may the **merges**, each
  a transaction of one append, and each read of a merged key one of one
  read, in realtime order: a read must see every append acknowledged
  before it began. SlateDB's transactions read
  what the writer holds in memory, durable or not (slatedb#1147), so a
  transaction that only reads may see a write that is then lost; its reads
  are not checked.
- No value is read that was never written, written to another key, or
  told it failed; no process exits, and no compactor, worker, or collector
  loop ends.

The artifacts of each job: `history.jsonl` and `txns.jsonl` (every
operation as the clients saw it), `faults.json`, `store-history.jsonl` (every
object-store request as the store saw it, with the faults injected),
`settings.json`, each process's log under `slatedb/`, and on a failure
`anomalies.json` and `bucket.tar`, the bucket's objects.

## Known issues

`allow` lists issues of SlateDB to report as warnings rather than fail on,
so that a run that keeps finding one can find others:

- `worker-store-error` (allowed by default; #49): a standalone compaction
  worker does not retry object-store errors as every other process does,
  so one that outlasts the object-store client's own retries ends its
  loop for good.

The clock faults are off by default for the same reason: a compaction
worker whose clock is a few milliseconds behind the compactor's dies
(#48), and a writer whose clock is behind the database's last tick fails
for good (#47).

## Findings

Against SlateDB 0.17.0 and main, tracked in #51, each with a standalone
reproducer:

- #47: a writer whose clock is behind the database's last tick by more than
  about 10 seconds -- one that fails over to a host whose clock is behind
  the previous writer's -- fails for good on its first write.
- #48: a compaction worker whose clock is 20 ms or more behind the
  compactor's fails its compactions, and its loop ends.
- #49: a standalone compaction worker does not retry object-store errors
  as every other process does, and a short outage ends its loop
  (`allow=worker-store-error`).
- #50: `SystemClockTicker` panics when its clock moves backwards.
