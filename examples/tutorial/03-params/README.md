# Step 3: parameters, benchmarks, and artifacts

This step adds `kv.bench`, a benchmark of kvd under a number of clients. It
runs once per point of a parameter matrix, drives kvd's load generator on
the node, records what it measured, and keeps the raw output beside its
result; and the service registers kvd's data log for collection when a job
fails. The diff from step 2 is [`bench.go`](bench.go), a few lines in
[`kvd.go`](kvd.go), and a registration in [`main.go`](main.go). Step 2's
`kv.smoke` is not repeated here: a step carries only the jobs it teaches,
and step 6 gathers them all.

## Variants

A job that implements `Matrix() []torx.Params` expands into one **variant**
per parameter set. `torx.Matrix` builds the cross product of the dimensions
it is given; `kv.bench` has one, `clients`, so it becomes three variants.
Each variant is a job of its own, selected, scheduled, and reported on its
own, with an id that spells out its parameters:

```
kv.bench[clients=1,seconds=2]
kv.bench[clients=4,seconds=2]
kv.bench[clients=16,seconds=2]
```

The job reads its parameters from `jc.Params` with the typed getters
(`Int`, `String`, `Bool`), in `Run` or in `Declare`.

## ResolveParams

`seconds` is not in the matrix, yet it is in every id. That is
`ResolveParams` at work: discovery calls it once per variant, before ids are
built, and the job returns the complete, checked parameter set the variant
runs with: defaults filled in, values type- and range-checked, unknown
keys rejected. Without it, `{clients: 4}` and `{clients: 4, seconds: 2}`
would be two variants of one configuration, and a mistyped key would
silently run the default. One consequence to know: a dimension added later
joins every id, so join runs on the parameters recorded in `result.json`,
not on id strings. Parameters arrive as JSON, so a number may be a float64;
`intParam` accepts one when it is whole and rejects anything else rather
than defaulting.

## Running it

```sh
go run ./examples/tutorial/03-params -nodes 3 -parallel 3
```

```
PASS   kv.bench[clients=1,seconds=2]  (2.276s)
        1 clients for 2s: 11655 ops/s, p50 0.08 ms, p99 0.20 ms
PASS   kv.bench[clients=4,seconds=2]  (2.278s)
        4 clients for 2s: 36294 ops/s, p50 0.09 ms, p99 0.39 ms
PASS   kv.bench[clients=16,seconds=2]  (2.282s)
        16 clients for 2s: 67926 ops/s, p50 0.18 ms, p99 1.01 ms

3 jobs: 3 passed, 0 failed, 0 flaky, 0 ignored
```

The local pool is sized to the largest job by default, one node here, and
the driver runs one job at a time. `-nodes 3` gives the three variants a
node each and `-parallel 3` runs them at once; two jobs never share a node.

## What the job does

`Run` asks the service's node to run `kvd load` with `n.Exec`, the same
call the preflight used, and gets back an `ExecResult`: the exit code,
stdout, and stderr. A non-zero exit is not an error from `Exec`; the
command ran, and the job decides what its exit means. The load generator
runs on the server's node here, next to the server; step 5 moves it onto
nodes of its own.

Then three calls record the outcome:

- `jc.WriteArtifact("load.json", stdout)` keeps the load generator's raw
  output at the top of the job's results directory, beside `result.json`,
  whatever happens next, so a failed variant still shows what the generator
  saw. A job's own files sit at the top of its directory; the services'
  collected files sit in directories below.
- `jc.Record(rep)` stores what was measured as the result's `data`. torx
  records it verbatim and never interprets it: benchmarks differ too much
  for a framework to model their metrics, so the schema is the job's own.
- `jc.SetSummary(...)` is the one line a person reads.

```json
{
  "id": "kv.bench[clients=4,seconds=2]",
  "params": { "clients": 4, "seconds": 2 },
  "status": "PASS",
  "summary": "4 clients for 2s: 36294 ops/s, p50 0.09 ms, p99 0.39 ms",
  "data": {
    "clients": 4, "seconds": 2, "ops": 72588, "ops_per_sec": 36294,
    "p50_ms": 0.092417, "p99_ms": 0.392625, "errors": 0
  }
}
```

## Collecting the server's files

`StartNode` gained one line:

```go
s.AddArtifact(n, torx.Artifact{Name: "kv.log", Path: s.dataDir(n) + "/kv.log"})
```

`AddArtifact` registers a file on the node for collection after the job,
into the service's directory in the results tree beside the captured
output. Without `CollectOnPass`, it is gathered only when the job fails:
kvd's data log says what the server had actually recorded, which is worth
having when something went wrong and noise when nothing did. (Captured
output is collected either way.)

## Parameters from outside

A `-params` file replaces the named jobs' compiled-in variants with
externally supplied ones, so a sweep or a specific configuration runs
without editing the suite. [`params.json`](params.json) gives `kv.bench` a
matrix and one explicit configuration a cross product cannot express:

```json
{
  "kv.bench": {
    "matrix": { "clients": [8, 32], "seconds": [1] },
    "configs": [ { "clients": 64, "seconds": 3 } ]
  }
}
```

```sh
go run ./examples/tutorial/03-params -params examples/tutorial/03-params/params.json -parallel 3
```

```
PASS   kv.bench[clients=8,seconds=1]  (1.127s)
        8 clients for 1s: 87808 ops/s, p50 0.08 ms, p99 0.28 ms
PASS   kv.bench[clients=32,seconds=1]  (1.122s)
        32 clients for 1s: 116020 ops/s, p50 0.21 ms, p99 0.96 ms
PASS   kv.bench[clients=64,seconds=3]  (3.136s)
        64 clients for 3s: 141864 ops/s, p50 0.33 ms, p99 1.68 ms
```

Each supplied set still passes through `ResolveParams`, so a typo in the
file fails loudly instead of running a default:

```sh
echo '{"kv.bench": {"matrix": {"client": [8]}}}' > /tmp/typo.json
go run ./examples/tutorial/03-params -params /tmp/typo.json
```

```
FAIL   kv.bench[client=8]  (0s)
        discover: job "kv.bench": resolve params: unknown parameter "client" (want clients, seconds)
```

The variant fails before any node is allocated, and the run's exit status
says so. A file that names a job the suite does not have is refused before
anything runs:

```
torx: discover: params override names unknown job "kv.bnech"
```

## Next

Step 4 crashes the server.
