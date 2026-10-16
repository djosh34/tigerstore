# Jepsen and etcd robustness prework for #148

Run `.github/workflows/jepsen.yml` on a GitHub runner. These scripts refuse to
run outside Actions. No product code changes are included.

```sh
gh workflow run jepsen.yml --repo djosh34/tigerstore --ref prep/148-jepsen \
  -f suite=all -f product_sha=<main-commit>
```

The workflow builds the specified product commit, not the workflow branch's
server code. The default is main at the start of this prework,
`40e7c0a009a08d6ba80f369dd2e66c9735512dbb`. Every job has its own fresh three-node
cluster and uploads logs and histories even when it fails. Choose `jepsen` or
`robustness` to run only that suite, or `tools` for the parser regressions.
To repeat one Jepsen case, pass `-f workloads='["register"]'` and
`-f nemeses='["lazyfs"]'`. Set `-f stores='["tigerstore"]'` to omit controls.
Omitted arrays use the full matrix. A partial run is not evidence for omitted cases.

## Jepsen

The seven active upstream workloads each run with kill, pause, partition, and
kill with LazyFS. Each case also runs against stock etcd v3.7.2. The `none`
workload is a no-op and upstream disables the old `lock` workload. `lock-set`
is included even though upstream excludes it from its expected-to-pass list.
It protects an in-memory set with expiring leases and has no fencing. Its
failure under faults is not by itself a tigerstore defect.

Each run uses six clients, 30 operations per second, 90 seconds of traffic,
a five-second nemesis interval and 50 operations per independent key. The
register checker uses WGL, one key at a time, in a 10 GiB JVM. Its versioned
register model is unchanged. Timeouts and unknown verdicts fail the job.

The runner and DB adapter come from plan 1's `exp/jepsen2` at `a0e3500d`.
They retain these pins:

- jepsen-io/etcd `b47cc401f27bfb7c28afd9ac088b5f0dca4501b8`, Jepsen 0.3.11,
  jetcd 0.8.5.
- nurturenature/jepsen-docker `4b5707813a0a24966082ae4fd2dd223da447e499`.
  The three image digests are in `run.sh`.
- Jepsen 0.3.11 installs LazyFS 0.3.0.

The adapter changes server installation, startup flags, primary lookup,
readiness and checker scheduling. It preserves upstream clients, models,
generators and fault implementations. Each artifact includes `upstream.patch`.
Client endpoints use plain gRPC. Peers use mutual TLS.

Fault checks require completed injection and healing records, plus observed
replacement PIDs, stopped processes, or DROP counters with packet hits.
LazyFS additionally requires a cache-clear completion in the filesystem log.
The checker summary must report `valid: true` and no coverage errors.

Docker nodes share the host clock. Clock faults are rejected, as #114 requires.
SIGKILL alone does not simulate power loss. Only the LazyFS cases discard
unsynced writes.

Plan 1's original register histories exhausted Knossos under the fast
leaderless errors later fixed in #105 by #106. Its offline certificates do not
count as results for this new main commit. The new run needs its own verdicts.

## Etcd robustness traffic

`robustness.sh` checks out etcd v3.7.2 at
`68c065e562994b89e333e77b039ad066f933c586` and copies `robustness_test.go.txt`
into its tests module. The text extension keeps its dependencies out of the
tigerstore Go module. Actions formats and compiles the copied test.

The five upstream presets run unchanged against both products:
`EtcdPutDeleteLease`, `EtcdPut`, `EtcdDelete`, `Kubernetes`, and
`KubernetesCreateDelete`. Each has member-pinned and cluster clients, watch
traffic, 40 seconds of load, a 50-QPS shared limit, a pause/resume and a
kill/restart. A recorded revision-1 read proves fresh storage. A terminal put
must succeed after recovery. At least 200 successful KV operations and some
watch events are required.

The upstream recording client and nondeterministic KV validator preserve
uncertain writes. Histories are saved before validation. Assumptions and KV
linearization must explicitly succeed, including the checker deadline.

This is not the complete upstream robustness suite. Its process manager and
WAL decoder only understand etcd. No tigerstore WAL adapter exists here.
Watch traffic runs, but the replay-dependent watch and historical-read
validators do not run. `summary.json` names those omissions rather than
calling their zero-value statuses successful. This limitation must remain
visible when the task worker decides how to complete #148's gate.

Plan 1 audited this integration in `experiments/jepsen/robustness.md`; it did
not execute upstream robustness traffic. This prework uses its endpoint-based
entrypoints rather than pretending tigerstore's files are etcd WALs.
