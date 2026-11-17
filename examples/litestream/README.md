# examples/litestream — hunting for bugs in Litestream

[Litestream](https://github.com/benbjohnson/litestream) replicates a SQLite
database to object storage. It runs beside the application that writes the
database, holds a read transaction open so that SQLite cannot checkpoint
past it, reads the WAL as the application writes it, and ships what each
sync finds as an LTX file -- one per TXID -- to the replica, where it
compacts them into coarser levels and snapshots, and deletes them as
retention says. `litestream restore` rebuilds the database from the
replica, as it was latest, at a TXID, or at a time, and `restore -f`
follows the replica as it grows. `litestream sync -wait` returns once the
replica holds everything the database did when it was called.

This suite holds Litestream to that while a nemesis crashes, stops, and
pauses it, crashes the application mid-transaction, has the application
checkpoint and vacuum behind its back, faults the object store, and fails
the primary over to a node that starts from the replica.

- **`app/`** is `lsapp`, the application: it keeps the database in WAL mode
  and commits the transactions the suite sends it over HTTP, one request
  each, logging each in a `txlog` table with a unique id. `lsapp verify`
  reads a copy of the database -- a restore, a follower's -- and prints its
  `integrity_check`, its txlog, and a digest of its rows; `lsapp tear`
  leaves a transaction as a writer killed mid-commit leaves it.
- **`model/`** is the database's schema, the values transactions write --
  each derived from the transaction, table, key, and size, which it begins
  with, so a value read back is checked byte for byte -- and a model that
  replays a txlog to what the copy holding it must hold.
- **`qa/`** is the suite: the service package (`qa/litestream`) that runs
  lsapp, `litestream replicate`, and `litestream restore -f` on torx nodes
  through `torx.Procs`, the chaos job, the jobs that reproduce its
  findings, and the checker, which holds the copies to the history with
  torx's `prefixcheck`.
- **`harness/`** builds the suite, lsapp, and Litestream, records the
  Litestream under test in the run directory, and execs the suite.

The example is a Go module of its own (`go.mod` here), so torx's own module
does not depend on a SQLite driver.

## Requirements

- Go.
- A Litestream checkout. The harness builds it, by default from
  `~/.cache/torx-litestream/src/litestream`:

  ```sh
  git clone https://github.com/benbjohnson/litestream ~/.cache/torx-litestream/src/litestream
  ```

  `-litestream PATH` names another checkout, or a `litestream` binary
  already built; `-race` builds it with the race detector.

The object store is torx's `objstore`, served from the worker: nothing to
install. The jobs run on local nodes, with or without `-netns`.

## Running

From `examples/litestream`:

```sh
go run ./harness 'litestream\.chaos'
go run ./harness -race -params hunt.json -parallel 8 -nodes 16 'litestream\.chaos'
go run ./harness 'litestream\.(restart-gap|torn-commit|follow-regress)'
```

with, for a hunt, a `-params` file such as

```json
{ "litestream.chaos": { "matrix": { "duration": [120], "trial": [1, 2, 3, 4, 5, 6, 7, 8] } } }
```

Each job takes two nodes. Two harnesses running at once on the host's
network may lease the same ports; give each `-netns`.

## The job

**`litestream.chaos`** runs lsapp and `litestream replicate` on one node,
the primary, replicating to an S3 bucket the worker serves; the other node
is the standby. Throughout the run:

- **clients** (`clients`, 4) send lsapp transactions: a few upserts and
  deletes; large ones, larger than a small page cache, which SQLite spills
  to the WAL before they commit, a fifth of which roll back; savepoints
  rolled back to; range deletes; and ephemeral tables created and dropped.
  Every value is up to `max-value` bytes, a rare one five times that, so
  some take overflow pages.
- **a syncer** runs `litestream sync -wait` every so often: an ack that the
  replica holds every transaction committed before it was called.
- **a restorer** on the standby restores the replica -- its latest state,
  a TXID the bucket holds or any below its last, or a time since the run
  began -- and reads the copy with `lsapp verify`.
- **a follower** on the standby, `litestream restore -f`, applies the
  replica as it grows; its copy is read every few seconds.

Litestream's settings are drawn from the seed (`config`, `random`): short
monitor, sync, and checkpoint intervals; small checkpoint, truncate, and
sync-size thresholds; one to three compaction levels of seconds; snapshots
every few seconds to a minute, kept a short while; L0 retention of
seconds. So are SQLite's: page sizes from 512 to 65536, auto-vacuum,
cache size, `wal_autocheckpoint`, `synchronous`, and `journal_size_limit`.

The faults (`faults`, `all`):

| fault | what it does |
|---|---|
| `crash-litestream` | kill Litestream; the app writes on; restart it |
| `stop-litestream` | stop Litestream with SIGTERM, which syncs one last time; restart it |
| `pause-litestream` | SIGSTOP Litestream, SIGCONT it |
| `outage` | Litestream down while the app checkpoints, closes its connections, crashes, or vacuums |
| `crash-app` | kill the app mid-transaction; restart it |
| `restart-app` | stop the app with SIGTERM; restart it |
| `app-checkpoint` | the app checkpoints the WAL, in any mode, under Litestream |
| `vacuum` | the app vacuums, in full or incrementally |
| `store-errors` | Litestream's store requests fail or are reset, before or after they take effect |
| `store-slow` | Litestream's store requests are delayed, or stall and land late |
| `store-down` | every store request of Litestream's waits seconds |
| `store-partition` | Litestream is cut off from the store until the partition heals, when its requests go through |
| `restore-errors` | the restores' and the follower's store requests fail or are delayed |
| `failover` | lose the primary; restore the replica on the standby, and run the app and Litestream there |
| `disk-full` | the primary's disk -- the database's, its WAL's, and Litestream's LTX files' -- fills up; with `-netns`. Off by default: name it (`"faults": "all,disk-full"`) |

Once every fault is healed, the job brings everything back, has the replica
catch up with a last `sync -wait`, and reads the primary's database, a
final restore, and the follower's copy.

### What is checked

Every copy -- each restore, each read of the follower, the restore a
failover starts from, the primary's database at the end -- must be
readable, pass `integrity_check`, and hold values that are whole. Its
txlog says which transactions it holds, in order, and:

- it must be a prefix of a sequence some primary committed (`prefixcheck`),
  holding no transaction that did not commit;
- it must hold exactly what those transactions wrote: its digest, row for
  row, must be the model's after replaying them;
- a restore of the latest state must hold every transaction a `sync -wait`
  that returned before it began acknowledged, and never go back;
- a follower must never go back in one life, and must catch up;
- a TXID must restore the same way every time, and a later TXID to a later
  state;
- a restore to a time must hold no transaction sent after it. (A restore to
  a time is not held to the acks before it: it can reach only the end of a
  file stamped before the time, and compaction and retention replace fine
  files with coarse ones, so how close it gets changes as the run goes on.
  A TXID that compaction no longer covers fails to restore, which is fine.)
- once the replica has caught up, it must restore to the primary's database
  exactly, and the follower must hold it too;
- no process may exit on its own, and none may log a panic or a data race.

The record is kept with the results: `txns.jsonl`, `acks.jsonl`,
`copies.jsonl` (what each copy held), `faults.jsonl`, `store-history.jsonl`
(every request the bucket served), `settings.json`, `anomalies.json`, and
`known.json`. A copy found broken is kept as `broken-<id>-<kind>.db` with
the service's files.

### Known issues

`allow` (by default all three) reports these as warnings, so a run that
keeps hitting one can find others:

- **`torn-commit`**: a copy holding a transaction that was in flight when
  the job killed its app, and that the app's database does not hold.
  The checker leaves such copies out of its comparisons, and reports what
  is wrong with them as this.
- **`restart-gap`**: sidestepped by configuration -- the app does not
  checkpoint on its own, and while Litestream is down it checkpoints only to
  truncate the WAL.
- **`follow-regress`**: what is wrong with a follower's copy read after one
  of its polls failed in that life.

The `disk-full` fault is off by default for a fourth finding, below: a full
disk stops Litestream's syncs but not its snapshots, and one then can be
corrupt, which corrupts every restore after it.

## The findings

Each of the first three has a job that reproduces it deterministically, on
one primary with Litestream's own checkpoints held off; each of the four
has a test, in Litestream's own style, in the issue that tracks it.

- **`litestream.restart-gap`**: Litestream is killed after syncing part of
  the WAL; the app commits more, checkpoints, and writes a few frames into
  the WAL from its start. Litestream, restarted, takes the new generation of
  the WAL for a continuation of the one it left, and never ships what the
  app committed while it was down: `sync -wait` succeeds, and the replica
  restores to a database without those transactions.
- **`litestream.torn-commit`**: Litestream ships WAL frames SQLite's WAL
  index does not count -- those of a transaction whose writer was killed
  between writing its frames and publishing them -- though SQLite discards
  them and writes the next transaction over them. The replica holds a
  transaction the primary never did; once Litestream's own checkpoint
  restarts the WAL, its pages stay in the replica under every page the
  primary writes afterwards, and the replica restores to a corrupt
  database (`2nd reference to page`).
- **`litestream.follow-regress`**: a follower whose poll fails partway
  keeps the TXID it began the poll at, though it applied the files before
  the failure; its next poll applies them again over the newer pages, and
  its copy goes back -- to a state the primary never had, if that poll
  fails too.
- **A stale snapshot** (`disk-full`): Litestream's read transaction keeps
  the app from checkpointing past what Litestream has synced only once it
  has synced to where the transaction began. Litestream restarted after the
  app wrote while it was down opens the transaction at the WAL's end, past
  frames it has yet to sync, and until its first sync succeeds -- which,
  while its disk is full, it cannot, as it stages each LTX file there --
  the app may checkpoint those frames into the database file. A snapshot
  then, at Litestream's position, reads those pages from the file beside
  the WAL's older ones: the replica restores to a database SQLite cannot
  read, at that TXID and at every later one, until the next snapshot.
