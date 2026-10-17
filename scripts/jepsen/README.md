# Jepsen and etcd robustness for #148

Run `.github/workflows/jepsen.yml` on a GitHub runner. These scripts refuse to
run outside Actions. No product code changes are included.

```sh
gh workflow run jepsen.yml --repo djosh34/tigerstore --ref main -f suite=all
```

The workflow builds its own commit unless `product_sha` explicitly selects
another commit. The nightly run tests current main. The original prework ran
on `e6c654e8253674f972d4227aea157fa907102363`; those results do not certify a
later commit. Every job has its own fresh three-node cluster and uploads logs
and histories even when it fails. Choose `jepsen` or
`robustness` to run only that suite, or `tools` for the parser regressions.
To repeat one Jepsen case, pass `-f workloads='["register"]'` and
`-f nemeses='["lazyfs"]'`. Set `-f stores='["etcd","tigerstore"]'` to add
controls. Omitted arrays use the required tigerstore matrix. A partial run is
not evidence for omitted cases.

## Jepsen

The six expected-to-pass upstream workloads each run with kill, pause,
partition, and kill with LazyFS. This is the 24-case scope in the
[manager's decision](https://github.com/djosh34/tigerstore/issues/148#issuecomment-6221843602).
The `none` workload is a no-op and upstream disables the old `lock` workload.
The unfenced `lock-set` workload remains available through `workloads`, but
upstream excludes it from its expected-to-pass list. It protects an in-memory
set with expiring leases and has no fencing. The four prework failures remain
failures, not waived passes. An explicit `lock-set` run still fails on an
invalid or unknown verdict. Stock etcd controls remain available too.

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
- The pinned node image contains LazyFS 0.3.0 at
  `9c5685e828da8b5d113d2f9134f6671e481d7162`. The runner verifies that revision
  and records each executable's SHA-256 before mounting it. It does not rebuild
  LazyFS during Jepsen setup. The first run timed out inside upstream
  `install!`, before any traffic; the blocked subcommand was not identified.

The adapter changes server installation, startup flags, primary lookup,
readiness and checker scheduling. It preserves upstream clients, models,
generators and fault implementations, except that pause/resume signals the
server PID file rather than matching command lines. Broad matching once stopped
the nemesis's own `xargs` process and prevented resume, recorded in
[#217](https://github.com/djosh34/tigerstore/issues/217). The adapter regression
checks the exact signal target and propagation of signal errors before traffic.
Each artifact includes `upstream.patch`.
Client endpoints use plain gRPC. Peers use mutual TLS. Unlike plan 1's
adapter, this one sets `--initial-advertise-peer-urls` for tigerstore too.
Current tigerstore rejects the default HTTP peer URL. Readiness has a
60-second bound so failed startup retains server logs rather than hanging.

Fault checks require completed injection and healing records, plus observed
replacement PIDs, stopped processes, or DROP counters with packet hits.
LazyFS additionally requires a cache-clear completion in the filesystem log.
The checker summary must report `valid: true` and no coverage errors.

Docker nodes share the host clock, so `run.sh` takes only `kill`, `pause`,
`partition` and `lazyfs` and rejects anything else, upstream's `all` included,
because it contains the clock nemesis (#114).
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

The five upstream presets run unchanged against tigerstore, and against etcd
with `stores`:
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
calling their zero-value statuses successful. The
[manager decided](https://github.com/djosh34/tigerstore/issues/148#issuecomment-6221843602)
not to add them for #148.

The test uses upstream's endpoint-based entrypoints rather than pretending
tigerstore's files are etcd WALs.
