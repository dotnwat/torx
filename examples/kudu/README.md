# examples/kudu — hunting for bugs in Apache Kudu

[Apache Kudu](https://kudu.apache.org) is a distributed columnar storage
engine. A table is split into tablets, each replicated with Raft across
tablet servers; masters keep the catalog. Every write gets a hybrid
timestamp, and Kudu keeps each row's history for `tablet_history_max_age_sec`
(a week by default) as multi-version base data and delta files, which
flushes and compactions rewrite in the background. Kudu stakes its
guarantees on those timestamps: a scan at snapshot t holds exactly the
writes with timestamps before t, whatever replica serves it and however long
ago t was; a diff scan between two timestamps -- what an incremental backup
takes -- returns exactly the rows that changed between them; and a
transaction's rows become visible together.

This suite holds Kudu to those guarantees while a nemesis crashes, pauses,
partitions, and freezes its servers, fails and fills their disks, and moves
leaders and replicas.

Three things live here:

- **`node/`** is `KuduNode.java`, a small program that serves Kudu's Java
  client over HTTP: writes (every row operation), scans in every read mode
  and as diff scans, transactions, and table administration. Kudu has no Go
  client. It runs from source with no build step (`java KuduNode.java`,
  JEP 330), so it needs only a JRE with the `jdk.compiler` module, and the
  Java client's jars. Its answers carry the timestamps Kudu assigned: each
  write's, and each scan's snapshot, and which tablet server served each
  row.
- **`qa/`** is the suite: a torx binary with two jobs, `kudu.smoke` and
  `kudu.chaos`; the service package (`qa/kudu`) that runs masters and
  tablet servers on torx nodes and the client node; and the checkers.
- **`harness/`** builds the suite, records the Kudu build and client jars
  in the run directory, and execs the suite.

## Requirements

- Go, and Java 22 or later (the system JRE will do: it runs `KuduNode.java`
  from source).
- A Kudu build: a directory with `bin/kudu-master`, `bin/kudu-tserver`,
  `bin/kudu`, and the libraries in `lib/` they load. The release binaries
  are on Maven Central, in the `kudu-binary` test jar:

  ```sh
  M=https://repo1.maven.org/maven2
  mkdir -p ~/.cache/torx-kudu/bin ~/.cache/torx-kudu/jars && cd ~/.cache/torx-kudu
  curl -fLO $M/org/apache/kudu/kudu-binary/1.18.1/kudu-binary-1.18.1-linux-x86_64.jar
  unzip -q kudu-binary-1.18.1-linux-x86_64.jar 'kudu-binary-1.18.1-linux-x86_64/*'
  mv kudu-binary-1.18.1-linux-x86_64 bin/1.18.1 && chmod +x bin/1.18.1/bin/*
  for j in org/apache/kudu/kudu-client/1.18.1/kudu-client-1.18.1.jar \
           com/stumbleupon/async/1.4.1/async-1.4.1.jar \
           org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar \
           org/slf4j/slf4j-simple/1.7.36/slf4j-simple-1.7.36.jar; do
    curl -fL -o jars/$(basename $j) $M/$j
  done
  ```

  Check each download against the `.sha1` beside it. The bundled
  `libsasl2` asks for `libcrypt.so.1` but uses nothing from it; on a
  system without it (Fedora 44), an empty stub does:
  `gcc -shared -fPIC -Wl,-soname,libcrypt.so.1 -o bin/1.18.1/lib/libcrypt.so.1 -x c /dev/null`.
- Network faults need `-netns`, disk-full faults a node that can mount
  (`-netns` nodes can), and freezes `-cgroups`; a run without them leaves
  those faults out. Each job takes about 2 GiB of memory, plus the
  size-limited directories the disk-full faults fill.

## Running

From anywhere in the repository:

```sh
go run ./examples/kudu/harness kudu.smoke
go run ./examples/kudu/harness -netns -cgroups kudu.chaos
go run ./examples/kudu/harness -netns -cgroups -params hunt.json -parallel 4 -nodes 24 kudu.chaos
```

with, for a hunt, a `-params` file such as

```json
{ "kudu.chaos": { "matrix": { "duration": [300], "trial": [1, 2, 3, 4], "nemeses": [1, 2] } } }
```

`-kudu DIR` names another Kudu build, `-jars DIR` other client jars. Each
job takes six nodes -- five for servers, one for the clients -- so
`-parallel 4` needs `-nodes 24`.

## The job

**`kudu.chaos`** runs a tablet server on each of `nodes` nodes (5), a
master on each of the first `masters` (3), and `jvms` (2) client JVMs on a
node of its own: the Java client picks a tablet's "closest" replica by a
number it draws once per JVM, so reads spread over replicas only across
JVMs. Its table has three range partitions, each hashed into `buckets`
tablets: keys the clients contend on (`keys`, 24), keys each client inserts
once, and keys transactions insert. With `ai` (on), a second table has a
non-unique primary key, so Kudu adds an auto-incrementing id to each row.

Each client (`clients`, 8) runs, until `duration` seconds pass:

- every row operation -- insert, upsert, update, delete, and their
  `_ignore` forms -- on contended keys, one at a time or batched; and
  inserts of fresh keys;
- snapshot scans at a timestamp the server picks, and at timestamps in the
  past, reaching back toward and past the history kept; read-your-writes
  and read-latest reads; leader and closest replica; fault tolerant or not;
- diff scans of the contended keys between two timestamps in the past;
- transactions that insert a few keys (committed, or rolled back);
- inserts into the table of a non-unique key, scans of it, and deletes of
  rows a scan saw, by key and id;
- now and then, a new Kudu client, as a restarted process would have.

Every value is unique, and a payload whose bytes derive from it is checked
byte for byte.

The faults (`faults=all`, a list, or `all,-name`):

| fault | what it does |
|---|---|
| `crash-tserver`, `crash-tservers` | kills one tablet server, or two, and restarts them after the hold |
| `restart-tserver` | stops a tablet server with SIGTERM and starts it again |
| `pause-tserver`, `pause-master` | SIGSTOPs a server |
| `crash-master` | kills a master |
| `partition-one`, `partition-halves`, `partition-client` | cuts a node off, splits the nodes, or cuts the clients off from some servers |
| `freeze` | freezes every process of a node (cgroup freezer) |
| `step-down` | has a tablet's leader step down, abruptly or not |
| `move-replica` | moves a replica to another tablet server: a tablet copy under load |
| `eio` | fails some I/O to one of a tablet server's data directories, then restarts it |
| `disk-full-wal`, `disk-full-data` | fills a tablet server's WAL directory, or one data directory |

`nemeses` nemeses inject faults at once. Unless `config=default`, each run
draws server flags from its seed: small flush thresholds and log segments,
more maintenance threads, small compaction targets and block sizes, short
history, auto-rebalancing, 3-4-3 or 3-2-3 replica replacement; and the
columns' encodings and compression. `tserver-flags`, `master-flags`, and
`columns` pin any of them, to reproduce a configuration.

## What is checked

Once the time is up and every fault is healed, the job waits for `kudu
cluster ksck` to find the cluster healthy, reads everything at one
snapshot through a new client and then through every JVM at the same
snapshot -- other replicas -- and runs `ksck --checksum_scan`, which
compares every replica of every tablet. Then:

- **Each key's history, in Kudu's timestamp order** (`linearize.Timeline`).
  Every write Kudu answered sits at its commit timestamp, every snapshot
  read at its snapshot, and a write whose outcome is unknown anywhere past
  the timestamp its client had seen -- so the history must be a legal
  sequence of row operations, and every read, now or an hour in the past,
  must hold exactly the writes before it. A conditional write Kudu refused
  (`AlreadyPresent`, `NotFound`) says what the row held at its timestamp.
- **Causality**: a write's timestamp, and a snapshot the server picks, are
  past the timestamp the client propagated.
- **Diff scans** return every key whose state changed between their two
  timestamps, with its state at the end, and no key that held one value
  throughout.
- **Transactions**: every snapshot holds all of a transaction's rows or
  none; never an aborted one's; a committed one's past its commit; and once
  a snapshot holds it, every later one does.
- **The table of a non-unique key**: a row's id never changes; ids in a
  tablet follow the order inserts committed in, and no two rows share one;
  and each row's history (inserted, deleted by the id a scan saw, seen or
  not) is legal in timestamp order.
- **Replicas agree** (`ksck --checksum_scan`), every read's rows are sound
  (in range, once each, payloads intact), and no server exits on its own,
  except as a fault documents (a full WAL disk).

`check.json` in the job's results holds what was found, with the
operations of `history.ndjson` that show it, and the faults in
`nemesis.ndjson`. A failed job keeps every server's directories
(`<server>-data.tar.gz`), to read with `kudu local_replica`.

## Known issues

`allow` lists known issues a run reports as warnings, so a run that keeps
hitting one can find others; by default all of these:

| issue | what it is |
|---|---|
| `deleted-rowset-gc` | with more than one maintenance thread, the deleted-rowset GC deletes a fully deleted rowset whose deletes are not ancient when it races a DeltaMemStore flush; snapshot and diff scans in the history window lose rows. Allowed, the tablet servers run with `--enable_deleted_rowset_gc=false`. |
| `auto-id-restart` | bootstrap replays committed inserts into a table of a non-unique key from a counter taken from the live rows on disk, not the one the leader logged; after the highest row was deleted, a restart renumbers rows and replicas disagree. |
| `stale-read` | a leader deposed by a pause, freeze, or partition serves snapshot reads at "now" from what it has: Kudu has no leader leases (KUDU-430, KUDU-1188). The checker tells these apart: a key whose history only reads at server-chosen snapshots break, or a transaction read a cut-off server served. |
| `exit-under-disk-full-data` | a tablet server crashes when a data directory fills between its cached free-space checks, though another has room (KUDU-2404). |

## Findings

Against Kudu 1.18.1 (both also on `master`):

1. **Deleted-rowset GC drops recent history** (#55; `deleted-rowset-gc`
   above): snapshot scans in the history window lose rows deleted after
   their snapshot, and diff scans -- incremental backups -- lose the
   deletes.
2. **Bootstrap renumbers auto-incrementing ids** (#56; `auto-id-restart`):
   after a restart, committed rows have new ids; a delete by an id read
   before it deletes another row; replicas diverge for good.
3. **A tablet server SIGSEGV in the transaction coordinator** during aborts
   (#57), seen once.

#58 tracks them, with what held up.
