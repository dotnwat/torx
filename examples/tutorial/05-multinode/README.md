# Step 5: more than one node

Until now every job used one node. This step gives the benchmark's load
generator nodes of its own: [`load.go`](load.go) makes it a service, and
[`bench.go`](bench.go) declares that service beside the server and gains a
`nodes` parameter. Nothing else changes.

## A service with nothing to start

`Load` embeds `*torx.ServiceBase` like the kvd service, asks for `nodes`
nodes, and implements the same four hooks -- but `StartNode` only runs the
preflight, and the other three do nothing. There is no long-running process:
the work happens in `Run`, when the job asks for it, which runs `kvd load`
on every node at once through `n.Exec` and returns each node's report.

A service that is not a server per node -- a one-shot client, a rolling
operation, a single API call -- is still a service. It is how a job gets
nodes, and how the framework knows the job's demand. (Such a service can
also override `Start`, `Wait`, `Stop`, and `Clean` directly instead of the
per-node hooks; the hooks are the default lifecycle, not the only one.)

## Two services in one job

`Declare` now registers two services. The framework sums their demand -- one
node for the server, `nodes` for the load -- allocates that many disjoint
nodes for the job, and hands each service its share in registration order:
the server gets the first node, the load generator the rest. The node count
comes from `jc.Params`, which is available in `Declare` as well as `Run`,
so the job's demand depends on the variant.

The pool sizes itself to the largest job by default, three nodes here, so
`-nodes` is no longer needed; with `-parallel 3` the three-node variants run
alone and the one-node jobs run alongside each other.

## The address, again

`kvd load` on a load node dials the server at `j.db.Addr()`: the node's
reachable address and the leased port. On the local pool every node's
address is `127.0.0.1` -- they are all subprocesses of one host, told apart
by port -- so a load node dialing the server's address is dialing its own
host, and the choice made in step 2 (bind every interface, advertise
`n.Addr()`) still changes nothing visible. In step 6 the load nodes are
containers, the server's address is `n0`, and the same code reaches across
the network. The service did not change between the two.

## What the first run found

The first two-node run of this step failed:

```
node-1: kvd load exited 1: kvd load: client 1: read back "37fe0057350007f4" for c1-k358, wrote "443ec822c36721b8"
```

Every load client checks that it reads back what it wrote, and the two
generators named their keys the same way, so client 1 on `node-1` and
client 1 on `node-2` overwrote each other. The fix is one argument in
`runOn`: each generator gets its node's name as a key prefix. It is the kind
of thing a multi-node benchmark exists to find, even when the bug turns out
to be in the benchmark.

## Aggregating

`Summary`, the recorded data, holds every node's report and the totals a
reader wants first. Throughput adds up across nodes; percentiles do not, so
the worst node's p99 stands for the whole rather than an average that would
mean nothing. Each node's raw report is kept as its own artifact,
`load-<node>.json`.

## Running it

```sh
go run ./examples/tutorial/05-multinode -parallel 3 'kv\.bench'
```

```
PASS   kv.bench[clients=4,nodes=2,seconds=2]  (2.215s)
        2 node(s) x 4 clients for 2s: 70814 ops/s, worst p99 0.32 ms
PASS   kv.bench[clients=16,nodes=2,seconds=2]  (2.213s)
        2 node(s) x 16 clients for 2s: 172633 ops/s, worst p99 1.00 ms
PASS   kv.bench[clients=4,nodes=1,seconds=2]  (2.144s)
        1 node(s) x 4 clients for 2s: 44984 ops/s, worst p99 0.23 ms
PASS   kv.bench[clients=16,nodes=1,seconds=2]  (2.154s)
        1 node(s) x 16 clients for 2s: 99948 ops/s, worst p99 0.67 ms

4 jobs: 4 passed, 0 failed, 0 flaky, 0 ignored
```

The driver schedules the largest jobs first, so the two-node variants run
before the one-node ones.

```
results/latest/
  kv.bench[clients=4,nodes=2,seconds=2]/
    kvd/node-0/stdout.log
    load-node-1.json
    load-node-2.json
    result.json
    test_log
    events.ndjson
  ...
```

## Next

Step 6 runs this suite, unchanged, on containers.
