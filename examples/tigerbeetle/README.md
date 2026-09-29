# examples/tigerbeetle — hunting for bugs in TigerBeetle

[TigerBeetle](https://tigerbeetle.com) is a database for financial
transactions: accounts, and transfers between them that TigerBeetle checks
against the accounts' limits, replicated with Viewstamped Replication across
up to six replicas. It is tested harder than most systems -- a deterministic
simulator (the VOPR) runs around the clock, Vortex runs real replicas under
crashes, pauses, network faults, and upgrades, and Jepsen analyzed 0.16.11 in
2025 -- so this suite looks where those do not: faults Vortex does not inject
(partitions, full disks, slow disks, starved CPUs, frozen machines, lost
data files, session storms), replicas that each run a configuration of their
own, and a checker that holds every answer the cluster gives to an exact
model of its state machine.

Two things live here:

- **`qa/`** is the suite: a torx binary with two jobs, and the service
  package (`qa/tigerbeetle`) that formats and runs a cluster on torx nodes.
- **`harness/`** is the launcher: it builds the suite, puts the tigerbeetle
  build under test first on the suite's PATH, gives the nodes scratch space
  of their own, records the invocation, and execs the suite.

The example is a Go module of its own, because TigerBeetle's Go client links
its native library through cgo; torx itself stays free of cgo.

## Requirements

- Go, and a C compiler for cgo.
- A `tigerbeetle` binary: a release from
  [github.com/tigerbeetle/tigerbeetle/releases](https://github.com/tigerbeetle/tigerbeetle/releases)
  (the `-debug` zip has extra assertions and stack traces), or a build of
  `main` (`./zig/download.sh && ./zig/zig build -Drelease -Dconfig_verify=true
  -Dconfig-release=0.17.10 -Dconfig-release-client-min=0.16.4`; the release
  flags let the 0.17.9 client talk to it). The client is v0.17.9.
- For the network and disk-full faults, `-netns` (Linux; no root). For the
  resource faults, `-cgroups` (Linux with a systemd user manager; no root).

A replica allocates about 2GiB and its data file starts at 1.06GiB, its
write-ahead log written out in full, so a five-replica job wants about 20GiB of
memory under `-netns`, where the data files live on a tmpfs. Without
`-netns` they live on disk under the nodes' scratch: the harness puts that in
the run directory, since a small tmpfs `/tmp` fills with three of them.

## Running

From `examples/tigerbeetle`:

```sh
go run ./harness tigerbeetle.smoke
go run ./harness -netns -cgroups tigerbeetle.chaos
go run ./harness -tigerbeetle ~/src/tigerbeetle/tigerbeetle -netns -cgroups -params hunt.json
```

with, for a hunt, a `-params` file such as

```json
{ "tigerbeetle.chaos": { "matrix": {
    "trial": [1, 2, 3, 4], "replicas": [3, 5, 6], "duration": [120],
    "faults": ["all,-batch-limit"] } } }
```

The suite also runs bare, `TMPDIR=<a disk> go run ./qa -netns -cgroups
tigerbeetle.chaos`, with the tigerbeetle on PATH.

## The jobs

**`tigerbeetle.smoke`** starts three replicas, creates two accounts and a
transfer between them, and reads the balances back.

**`tigerbeetle.chaos`** runs clients against the cluster while a nemesis
injects one fault at a time, and checks everything they saw.

Each client is a session of its own. It sends batches of up to 32 transfers
between a dozen accounts, some limited to their credits or their debits:
plain transfers, pending ones (a third of them timing out after one to
three seconds, a few closing an account until they are voided or expire),
posts and voids of pending ones for all, part, or more than their amount,
balancing transfers, linked chains that stand or fall together (now and then
left open), resubmissions of transfers already sent, and events that must
fail. It reads every account at once,
looks transfers up, and runs the four scans: `get_account_transfers`,
`get_account_balances`, `query_transfers`, and `get_change_events`, with
limits, reversed order, and timestamp ranges drawn from what it has seen.

The faults, each aimed at the primary more often than not:

| fault | what it does | needs |
| --- | --- | --- |
| `crash`, `crash-primary`, `crash-majority`, `crash-all` | SIGKILL one replica, a majority, or all at once; restart after the hold | |
| `pause`, `pause-primary` | SIGSTOP, then SIGCONT | |
| `recover` | a backup loses its data file and gets a new one from `tigerbeetle recover` | |
| `corrupt` | overwrite sectors or whole slots of one replica's data file -- WAL prepares, client replies, grid blocks, a sector of one superblock copy, and, while it runs, WAL headers -- live or while it is down (torx's `diskfault.Corrupt`) | |
| `corrupt-headers` | corrupt a WAL-header sector of a replica while it is down (finding 3 below) | |
| `reconfigure` | restart a replica with a configuration drawn anew | |
| `batch-limit` | restart a replica with another `--limit-request` (finding 1 below) | |
| `session-storm` | 64 to 95 more sessions, busy until the heal, than the cluster keeps | |
| `standbys=N` | (a parameter) standbys beside the replicas, which follow the log without voting | |
| `partition-primary`, `partition-half`, `partition-bridge`, `deafen-primary`, `slow`, `lossy`, `mtu-blackhole` | cut, split, or degrade the network (torx's `netfault`) | `-netns` |
| `disk-full` | fill a replica's data directory (torx's `diskfault`); the replica exits, as designed, and restarts after the hold | `-netns` |
| `freeze`, `freeze-primary`, `cpu-starve`, `cpu-starve-primary`, `mem-pressure` | freeze a replica's node, cap it at 1 to 10% of a CPU, or reclaim its memory to a fraction of its use (torx's `resfault`) | `-cgroups` |
| `slow-disk`, `slow-disk-primary` | throttle the disk under a replica's data file to as little as 64KiB/s and 5 IOPS | `-cgroups`, `storage=disk` |

Each replica starts with a configuration of its own (`config=random`): the
grid cache or `--memory`, the object caches, `--limit-pipeline-requests`,
prepare and repair timeouts, injected commit stalls, and now and then StatsD
metrics or debug logging. `reconfigure` redraws one replica's.

At the end every fault is healed, the clients' operations in flight must
complete, a fresh session reads every account and every transfer submitted,
and the checker (`check.go`, `check_query.go`) replays the history:

- **The serial order.** TigerBeetle stamps every event with a timestamp, and
  the events of a batch take consecutive ones ending at the batch's own, so
  the answers place every batch in the cluster's serial order. Batches whose
  answer was lost are placed by the transfers they created. The expiries of
  pending transfers, which the primary runs on its own, are placed by their
  change events.
- **Every result.** `model.go` is a Go transcription of
  `state_machine.zig`'s `create_transfer` and its helpers, in the order their
  checks run. Each batch is replayed through it in serial order, and every
  event's status and timestamp must match what the cluster returned:
  `created`, `exists`, `id_already_failed` after a transient failure burned
  the id, `linked_event_failed` for a chain rolled back,
  `pending_transfer_expired`, and each of the failures. An expiry must come
  no earlier than its transfer's timeout, and every pending transfer whose
  timeout passed must have expired.
- **Real time.** A batch answered before another was sent comes first.
- **Reads.** Every account read must show the balances after some prefix of
  the serial order within its window (after every batch answered before it
  began, before every batch sent after it ended); every lookup must find
  exactly the transfers of some such prefix.
- **Scans.** Every scan must return the first (or, reversed, last) matches
  of some such prefix, field for field, balances after each transfer for
  `get_account_balances`, and each change event's transfer, type, and both
  accounts' balances after it.
- **The final state.** The model's transfers and balances must be the
  cluster's.
- **Replicas.** A replica that exits on its own is an anomaly, unless it
  stopped on a full disk the nemesis filled.

`check.json` in each variant's results holds the anomalies, the statistics
(every status the model agreed with, and how many reads and scans it
checked), the replicas' exits with what each said last, and the
configurations; `history.ndjson` is every operation and fault.

## Findings

Against TigerBeetle 0.17.9 (release, debug build) and main at 6f8e6b5;
reproducers are in `results/tigerbeetle-findings/` (not committed).

1. **Raising the batch size by a rolling restart crashes the backups.** A
   replica started with `--development` takes requests of at most 32KiB
   (`--limit-request`); without, 1MiB. The CLI's help says a cluster can
   always raise its batch size by restarting without `--development`. Done
   one replica at a time, as an operator keeps a cluster available, once a
   replica restarted without it becomes primary, the next client to
   register makes every backup still on `--development` abort:
   `execute_op_register` asserts that the batch size the primary stamped on
   the registration fits the backup's own limit (`replica.zig:5580` in
   0.17.9), and it does not. The cluster loses its quorum. PR #1981, which
   introduced `--limit-request`, meant a replica that cannot take a large
   prepare to panic loudly as an operator error; this prepare is small, and
   the failure is an assertion. `batch-limit` provokes it, which is why the
   compiled-in variant leaves it out.
2. **Two experimental flags reach assertions instead of the CLI's errors.**
   `--commit-stall-lag-min` greater than `--commit-stall-lag-max`, or
   `--commit-stall-multiple-max=0`, abort the replica at startup with
   "reached unreachable code"; `cli.zig` says every argument is validated
   there.

3. **A power loss and one bad sector on each of two disks leaves the
   cluster down for good.** A replica restarted with a corrupt sector in its
   WAL's headers cannot be sure of its log's head, and waits in
   `recovering_head` for a primary to tell it. When two of three replicas
   are there at once -- the whole cluster lost power, or the primary and a
   backup crashed, and each has one bad sector, different ones -- the
   healthy replica's `exit_view` votes are ignored by both
   (`ignore_exit_view_message`), no view change starts, and no primary ever
   tells them. `tigerbeetle recover` cannot help, since it needs a working
   cluster. Every operation still has intact copies on two replicas, and
   `docs/concepts/safety.md` says TigerBeetle stays available "unless the
   data gets corrupted on every single replica". This is tigerbeetle#1376,
   open since 2023 ("Allow recovering head replicas to contribute to DVC");
   the reproducer shows it is still reached this simply on 0.17.9 and main.
   `corrupt-headers` provokes it; `corrupt` stays clear of it.
4. **A data file left by an interrupted `tigerbeetle recover` panics on
   start** ("superblock not found"). Minor: the message says what is wrong.

Nothing else, in the runs so far: the model agreed with the cluster on every
result, read, scan, and expiry under every other fault above, on 3, 5, and 6
replicas, with and without standbys.
