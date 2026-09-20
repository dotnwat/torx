# Step 6: a launcher, and nodes in docker

This step has two parts. The first wraps the suite in a **launcher**, the
front end a person or CI runs. The second gives the launcher a second
backend: it provisions three docker containers, and runs the suite against
them over ssh. The suite is step 5's, moved to [`suite/`](suite/) and
changed in one line.

Three things live here:

- [`suite/`](suite/) -- the suite. Its `main.go` gains a blank import of the
  ssh backend, `_ "github.com/dotnwat/torx/ssh"`, so that nodes of the
  `ssh` kind are constructible; without it, a manifest naming them is
  refused at load with an error that says so.
- [`launcher/`](launcher/) -- the launcher, a Go program of a few hundred
  lines.
- [`docker/`](docker/) -- what the docker backend provisions with: a
  `Dockerfile` for the node image, its `sshd_config`, a `compose.yaml`, and
  the node `manifest.json`.

## Part A: the launcher

torx deliberately stops at running jobs against nodes it is handed. It does
not build the suite, put the system under test on the nodes, provision the
nodes, or decide where results go, because every project answers those
differently. The launcher is one answer, kept to what a real one needs.
[`launcher/main.go`](launcher/main.go) does, in order:

1. Build kvd and the suite from the current tree, into a temporary
   directory, so a failed build leaves nothing behind.
2. Mint the run directory with `torx.MakeRunDir`, the same call the driver
   makes for `-results-dir`, so it is named and linked the way a bare run's
   would be: a UTC timestamp plus a random suffix, and `latest` repointed
   at it.
3. Move the binaries into it, archive any `-params` file into it, and write
   `invocation.json`: the launcher's argv, the git commit and whether the
   tree was dirty, the platform built for, and exactly how the suite is
   about to be run. All of this lands before the suite starts, so even an
   interrupted run says what produced it.
4. Run the suite with `-run-dir` naming that directory. torx uses the
   directory exactly as given -- no timestamped subdirectory, no `latest`
   symlink; those belong to `-results-dir` mode -- and fills in the
   per-job subdirectories and `run.json`. For the local backend the suite
   is exec'd in place, with `bin/` prepended to its PATH so every node --
   a subprocess of the suite -- finds kvd, and its exit status is the
   launcher's.

```sh
go run ./examples/tutorial/06-launcher/launcher                 # every job, local pool
go run ./examples/tutorial/06-launcher/launcher -parallel 3 'kv\.bench'
```

The launcher prints one line, `run directory: <path>`, and then the suite's
own output. The run directory lands under `results/tutorial/` in the
repository (`-results-dir` moves it):

```
results/tutorial/2026-09-19T23-29-52Z-1673092302/
  invocation.json         how the run was launched
  bin/kvd                 the server the nodes ran
  bin/suite               the suite binary that ran
  params.json             verbatim copy of -params, when given
  kv.smoke/               the jobs, as a bare run would leave them
  kv.graceful/
  run.json
```

Nothing in the suite knows the launcher exists: it also runs bare, as every
earlier step did, with results under `./results/`.

## Part B: nodes in docker

```sh
go run ./examples/tutorial/06-launcher/launcher -backend docker -parallel 3
```

```
launcher: building for linux/arm64
run directory: /Users/.../torx/results/tutorial/2026-09-19T23-30-19Z-2455559147
launcher: starting 3 nodes (compose project torx-tutorial-2026-09-19t23-30-19z-2455559147)
 ...
PASS   kv.bench[clients=4,nodes=2,seconds=2]  (2.215s)
        2 node(s) x 4 clients for 2s: 70814 ops/s, worst p99 0.32 ms
PASS   kv.bench[clients=16,nodes=2,seconds=2]  (2.213s)
        2 node(s) x 16 clients for 2s: 172633 ops/s, worst p99 1.00 ms
PASS   kv.durability  (300ms)
        100 keys survived a crash; kvd was back in 104ms
PASS   kv.graceful  (302ms)
        kvd stopped cleanly in 2ms and came back with all 100 keys
PASS   kv.smoke  (175ms)
        kvd at n2:30000 answered; wrote and read back "hello, torx"
PASS   kv.bench[clients=4,nodes=1,seconds=2]  (2.144s)
        1 node(s) x 4 clients for 2s: 44984 ops/s, worst p99 0.23 ms
PASS   kv.bench[clients=16,nodes=1,seconds=2]  (2.154s)
        1 node(s) x 16 clients for 2s: 99948 ops/s, worst p99 0.67 ms

7 jobs: 7 passed, 0 failed, 0 flaky, 0 ignored
launcher: stopping the nodes
```

The first run builds the node image, which takes a minute; after that the
whole suite runs in about fifteen seconds. Every job that ran on the local
pool runs here unchanged, and one line in the output says what changed
underneath: `kvd at n2:30000`. The server was reached at a node's name on
the docker network, by the job in the driver container and by the load
generators in the other containers -- because the service binds every
interface and advertises `n.Addr()`. A service that had hardcoded the
loopback would have passed every earlier step and failed here.

### The topology, and why

Three containers are the nodes, and a fourth, the **driver**, runs the
suite. The suite could not simply run on your machine: on Docker Desktop,
containers on a bridge network are not reachable from the host, so a suite
there could dial no node. Running it in a container on the same network is
also what a real deployment looks like -- a CI runner or a bastion host on
the nodes' network -- so the tutorial's topology is the general one, not a
workaround. A suite is one static binary, so "deploying" it into that
container is cross-compiling it for Linux on the engine's architecture,
which is not the host's platform on a Mac; the launcher asks docker which.

### The pieces

[`docker/Dockerfile`](docker/Dockerfile) builds the node image from alpine:
an sshd with its sftp subsystem (the ssh backend runs commands through exec
sessions and moves files over sftp), a POSIX `sh` with the `setsid` torx
uses to give each streamed command a process group of its own, a `torx`
user with a scratch root, and kvd on PATH. One line is worth a comment in
the file: alpine's `adduser` locks a new account, and sshd refuses a locked
account even for key authentication.

[`docker/compose.yaml`](docker/compose.yaml) declares the three nodes and
the driver on one network. Each node's healthcheck is its sshd accepting
connections, and the launcher's `compose up --wait` returns only once all
three are healthy, so the suite never dials a node that is not there. The
driver mounts the run directory at `/torx/run`, which is where the suite
finds its binary, the manifest, and the keys, and where it writes its
results -- straight into the run directory on your machine.

[`launcher/keys.go`](launcher/keys.go) generates the keys, fresh for every
run: a client key pair and a host key pair, from which it derives the
`authorized_keys` the nodes accept the client from and the `known_hosts`
the client verifies every node against. Generating both sides before any
container exists means there is no trust-on-first-use anywhere; the torx
ssh backend has no such fallback, and a real deployment should not either.
The image bakes the host key and the authorized key in.

[`docker/manifest.json`](docker/manifest.json) is the pool: the boundary
between provisioning and torx. Each node has a name, the address others
dial it at (its compose service name, resolved by docker's DNS), a scratch
root, a port range, and a backend descriptor -- `ssh`, with the host to
dial and the user, identity file, and known_hosts to dial with, spelled as
paths inside the driver container. The port range must be explicit for a
remote node: the default allocator finds free ports by listening on the
driver's own host, which says nothing about a container's. The nodes share
one range because each container has a port space of its own. The manifest
is static, checked in, and copied into every run directory.

[`launcher/docker.go`](launcher/docker.go) ties it together: copy `docker/`
into the run directory, write the keys, `docker compose up --build --wait`
the nodes, `docker compose run` the suite in the driver with `-pool` and
`-run-dir`, and `docker compose down` afterwards, whatever happened. Each
run is its own compose project, named after the run directory, so two runs
do not share containers or a network; a run interrupted hard enough to skip
the teardown shows up in `docker compose ls` and is removed with `docker
compose -p <name> down`.

The run directory records all of it:

```
results/tutorial/2026-09-19T23-30-19Z-2455559147/
  invocation.json
  bin/kvd                 the binaries, built for linux/arm64
  bin/suite
  docker/                 the Dockerfile, compose file, sshd_config, and manifest that ran
  keys/                   this run's keys
  kv.smoke/
    kvd/n2/stdout.log     collected over sftp
    ...
  kv.bench[clients=4,nodes=2,seconds=2]/
    kvd/n0/stdout.log
    load-n1.json
    load-n2.json
    ...
  run.json
```

### Beyond the tutorial

A manifest can also carry each node's CPUs, memory, and labels, and a
service's `NodeSpec` can require them, so a job asks for the nodes it needs
and the scheduler matches. Nothing here needs that, but the mechanism is
the same one that places a job on real machines. A launcher for cloud
instances would provision them, write a manifest with their addresses and a
host key it obtained out of band, and run the suite exactly as this one
does; the suite would not change.

## Testing

[`launcher/launcher_test.go`](launcher/launcher_test.go) unit-tests the
pure parts -- the project name, the PATH handling, the keys fitting together
-- and runs the launcher end to end on both backends as a person would, from
the repository root, reading the run directory it announces; once with a
`-results-dir` given relative to that root, which the launcher has to
resolve before it puts the run directory on a PATH or in compose's flags.
The docker test skips when `docker info` fails; with `TORX_DOCKER_REQUIRED=1`
in the environment it fails instead, which is how CI runs it. The suite's
own test is the one from step 5, unchanged.

## Where next

[`examples/rqlite`](../../rqlite/) tests a real distributed database with
the same building blocks. Its service starts a cluster whose nodes join in
order, its jobs crash a leader and roll through a restart, and its harness
is this launcher's older sibling.
