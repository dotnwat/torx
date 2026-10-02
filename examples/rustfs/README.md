# examples/rustfs — hunting for bugs in RustFS

[RustFS](https://github.com/rustfs/rustfs) is an S3-compatible object store
written in Rust: each object is erasure-coded across the drives of a set,
spread over the servers of a cluster, and written and read at a quorum of
those drives. S3 promises strong read-after-write consistency -- a read, or a
listing, sees every write that finished before it began -- and conditional
writes (`If-Match`, `If-None-Match`) that hold at the instant they take
effect. This suite holds RustFS to those promises while a nemesis crashes,
pauses, partitions, freezes, and starves its servers, and fills, wipes, and
corrupts their drives.

Two things live here:

- **`qa/`** is the suite: a torx binary with two jobs, the service package
  (`qa/rustfs`) that runs a cluster on torx nodes, and a small S3 client
  (`qa/s3`) that signs requests itself and sends each exactly once, so a
  history records every request as it was sent and every answer as it came
  back.
- **`harness/`** is the launcher: it builds the suite, puts the rustfs build
  under test first on the suite's PATH, gives the nodes scratch space of
  their own in the run directory, records the invocation, and execs the
  suite.

## Requirements

- Go.
- A `rustfs` binary: a release from
  [github.com/rustfs/rustfs/releases](https://github.com/rustfs/rustfs/releases)
  (the `rustfs-linux-x86_64-gnu-*.zip`), or a build of `main`. A build with
  debug assertions catches internal invariants a release skips:
  `CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
  CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true CARGO_PROFILE_RELEASE_LTO=off
  cargo build --release --bin rustfs` (it needs `protoc`).
- For the network and disk-full faults, `-netns` (Linux; no root): each
  server gets a network of its own, and each drive a tmpfs of its own, so
  that a drive is a device of its own, as RustFS expects, and can be
  filled. For the resource faults, `-cgroups` (Linux with a systemd user
  manager; no root).

A server takes about a gigabyte of memory; the drives under `-netns` are
memory too, up to 1GiB each, though a run of the default job holds far less.

## Running

From anywhere in the repository:

```sh
go run ./examples/rustfs/harness rustfs.smoke
go run ./examples/rustfs/harness -netns -cgroups rustfs.chaos
go run ./examples/rustfs/harness -rustfs ~/rustfs-main/rustfs -netns -cgroups -parallel 4 -params hunt.json
```

with, for a hunt, a `-params` file such as

```json
{ "rustfs.chaos": { "matrix": {
    "drives": [1, 2], "versioning": [false, true], "trial": [1, 2, 3],
    "duration": [120] } } }
```

`-parallel 4` runs four jobs at once, each on four nodes.

## The jobs

**`rustfs.smoke`** starts four servers with two drives each, writes an
object through one and reads it back through every other, checks
`If-None-Match` and `If-Match`, lists, and deletes.

**`rustfs.chaos`** runs clients against one bucket while a nemesis injects
one fault at a time, and checks everything they saw.

Each client issues one operation at a time to a server picked at random, on
a key picked from a few (`keys=8`): `PUT`, `PUT If-None-Match: *`, `PUT
If-Match` (on the ETag of the value the client last saw, or now and then of
an older one), multipart uploads, `GET`, `HEAD`, `DELETE`, `DELETE
If-Match`, and `ListObjectsV2`. Every value written is unique, and its bytes
derive from its name, so every read is checked byte for byte against the
write that made it, and an ETag names the value it belongs to. Most objects
are small enough to be stored inline in their metadata; some span one or
more erasure blocks.

The faults:

| fault | what it does | needs |
| --- | --- | --- |
| `crash`, `crash-two`, `crash-all` | SIGKILL one server, two, or all at once; restart after the hold | |
| `terminate` | SIGTERM a server, as an operator stops it; restart after the hold | |
| `pause`, `pause-two` | SIGSTOP one or two servers, then SIGCONT; one hold in ten outlasts a lock's 30-second lease | |
| `reconfigure` | restart a server with settings drawn anew | |
| `wipe-drive`, `replace-drive` | empty a drive under its running server, or while it is down | |
| `corrupt` | overwrite part of a few of a drive's object files -- metadata or shards -- with garbage (torx's `diskfault.Corrupt`) | |
| `disk-full` | fill one drive of a server, or all of them (torx's `diskfault`) | `-netns` |
| `partition-one`, `partition-half`, `partition-bridge`, `deafen`, `slow`, `lossy` | cut, split, or degrade the network between servers (torx's `netfault`) | `-netns` |
| `freeze`, `cpu-starve`, `mem-pressure` | freeze a server's node, cap it at 1 to 10% of a CPU, or reclaim its memory to a fraction of its use (torx's `resfault`) | `-cgroups` |

The drive faults hit only the *fragile* drives, chosen at the start: never
more than the erasure code's parity can lose, so a read that comes back
wrong, or an acknowledged write that is lost, is RustFS's doing and not the
nemesis's. A supervisor restarts servers that exit on their own.

At the end every fault is healed, the clients' operations in flight must
complete, and every server reads every key and lists the bucket (and, with
versioning, lists every version). The checks:

- **Bytes.** Every `GET` must return exactly the bytes of the value it
  names, under that value's ETag (`corrupt-read`).
- **Linearizability.** Each key's history must be linearizable
  (`nonlinearizable`), checked with torx's `linearize` against a model of
  one key (`model.go`): a register that holds a value or no object, whose
  conditional writes take effect exactly when their precondition holds. A
  listing is a read of every key. An operation whose answer was lost -- a
  timeout, a 5xx -- may have taken effect at any time after its call, or
  never. A violation is reported by its core (`linearize.Minimize`): the
  few operations that, with every other weakened, still admit no order.
- **Versions.** With versioning, every version an acknowledged write made
  must be listed at the end (`lost-version`), every version listed must be
  one a write made (`phantom-version`), and versions must be in an order
  that keeps a write acknowledged before another began older than it
  (`version-order`).
- **Drives.** With `audit=` naming RustFS's `dump_versions` (built from
  the tree under test: `cargo build --release -p rustfs-filemeta --example
  dump_versions`), the job decodes every drive's `xl.meta` of every key each
  second, and a copy of an acknowledged version that a drive held and then
  does not is an anomaly (`copy-lost`), with when it vanished, to find in
  the servers' logs. A drive the nemesis wipes is exempt.
- **Servers.** A server that exits on its own is an anomaly.

`check.json` in each variant's results holds the anomalies, statistics, the
servers' exits, their settings, the fragile drives, what each drive held at
the end, and the history's wall-clock start, to find an operation in the
servers' logs; `history.ndjson` is every operation and fault.

Known issues get in the way of finding new ones. `allow` lists the ones the
model accepts (by default all of them, below); `allow=""` holds RustFS to
S3's semantics in full.

## Findings

Against RustFS 1.0.0, 1.0.1-preview.16, and main at 370b517b4. Each is an
issue here, with a standalone reproducer, its output, and a root-cause trail
pinned to the release, tracked in #36.

1. **A failed delete on full drives erases versions** (#38). A delete backs
   up an object's metadata before it changes it, with a write that a nearly
   full drive leaves truncated; when the delete fails below quorum, its
   rollback restores that truncated backup over the metadata, and the next
   write replaces metadata that does not parse with its own version alone.
   Acknowledged versions are lost from every drive that was full. The trash
   (below) fills drives by itself. Found by `lost-version`, and pinned down
   with `audit=`, an auditor of what each drive holds.
2. **A conditional write takes effect on a delete marker** (#39). In a
   bucket with versioning, `PUT` and `CompleteMultipartUpload` with
   `If-Match` succeed when the object's current version is a delete marker,
   whatever ETag they name; S3 answers 404 or 412, since the object does not
   exist. An optimistic-concurrency client that read a version, and lost a
   race to a delete, recreates the object. Allowed as
   `ifmatch-delete-marker`.
3. **A conditional delete of no object succeeds, and writes** (#40). In a
   bucket with versioning, `DELETE` with `If-Match` (an ETag, or `*`) of a
   key that holds no object answers 204 and writes a delete marker; S3
   answers 404. Allowed as `delete-ifmatch-missing`.
4. **Listings read uncommitted state** (#41). Under faults, `ListObjectsV2`
   lists a write that then fails and is rolled back, and omits an object
   that a delete, then failing, has removed from some drives: reads after it
   see the object again. A listing takes no lock, and with drives
   unreachable accepts what a read quorum of them holds.
5. **Listings of a versioned bucket go stale** (#42). With some drives
   unreachable, a listing in a bucket with versioning drops each key's
   newest version if too few of the drives it reaches hold it, and lists the
   version before it as the latest; `GET` refuses in the same state.
6. **The trash fills the drives** (#43). What an overwrite or delete
   replaces is kept in a trash emptied every five minutes, whatever the
   space left, so a workload that overwrites fills its drives with trash
   while it holds little: writes then fail with `500 InternalError`.
