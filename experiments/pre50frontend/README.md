# #74 frontend measurement experiment

Throwaway runner scripts, not a server change. No measurements run locally.

## Sources and variants

All three tigerstore builds share main `eb8a20eda7b1436e3319b924753469f2d8c0ee99`:

- a is that main commit, unchanged except observation.
- b is native gRPC, `2d4052e96939b12fd68f0252dfd1154127cdf7c6`.
- c is native gRPC with tuning, `78db0c56fe2b051b651bbfa3f1aed03ca3724b9c`.
- etcd is the stock v3.7.2 linux-arm64 release. The runner verifies the etcd binary hash borrowed from bench-68 and records both binary hashes and the archive hash.

`refs.json` commits immutable refs and the mode. Workflow inputs can override b/c refs and mode. The runner rejects floating refs, refs without the pinned main ancestor, and changes to `internal/core`, `internal/node`, `internal/wal`, `internal/osfs` or `internal/peer`. It saves complete variant diffs for review. Ancestor checking alone does not prove a ref has no unrelated frontend changes. Review the diffs.

`observability.patch` adds one command hook to each source archive. Identical `pre50frontend`-tagged observation files count file writes/syncs and expose runtime snapshots. The checkout's server files stay untouched. No core, node, WAL or peer code changes. File Sync and Write call the original os.File methods, return the original errors, and count calls afterward. Directory syncs stay on their original path and are not counted. There are no durability flags or timers to tune.

Borrowed code:

- `origin/exp/pre-50-profile` at `1d90dcebf4ff9c910f7053dc0634d251767fd92b`: `load/main.go` and the `attribute.py` raw pprof parser.
- `origin/exp/bench-68` at `32ceb80fb36fd4229c2e2e7594b40da197fe4eaf`: process setup, applied-revision fences, etcd post-WAL.Save fences, and CPU/latency reporting.

The generator is a direct etcd protobuf `pb.KVClient` over grpc-go, not a custom lean HTTP/2 framer. Its request loop, key/value corpus, connection warmup, process CPU sampling and percentile calculation are unchanged. Additions are controller barriers and explicit client metadata. Each request is a Put of a 1 KiB value in a 4,096-key corpus. There are 16 connections at every concurrency.

Stock etcd uses its shared TLS client port, with gRPC via ServeHTTP. There is no separate HTTP port, insecure cmux path, or etcd transport tuning. The native variants do not select their frontend through environment variables.

## Primary mode

`.github/workflows/pre50frontend.yml` runs free `ubuntu-24.04-arm` jobs with clients 16, 64 and 256. Each job has a 15-minute limit, an 800-second script limit and a 600-second measurement limit. A job that exceeds these limits fails, rather than reporting a partial cohort. All command launches check the absolute deadline `2026-10-13T22:00:00Z`. No dispatch, polling, push or issue-comment automation is included.

Each job builds a/b/c with the same instrumentation, then runs stock etcd back to back with them. It runs five repetitions, with fresh three-process clusters and data directories each time. The order rotates and reverses on alternate repetitions. `schedule.json` saves the order. Five repetitions cannot perfectly balance all four positions; keep paired per-repetition rows when comparing.

The leader runs on CPU0, followers on CPU1/2, generator and controller on CPU3. Every server starts with `GOMAXPROCS=2`, including etcd. The generator uses `GOMAXPROCS=1`. The runner verifies tigerstore's actual runtime value and stock etcd's process environment. It reassigns all server threads after election and verifies two thread walks before and after the measurement. A thread created with a wrong inherited mask rejects the run. Generator threads also get before/after checks. This checks observed layouts, not every instant during a window.

The runner requires ext4 with barriers enabled and rejects `nobarrier`/`barrier=0`. It uses normal file syncs, TLS and encrypted peers. It never changes fsync behavior or the disk guarantee.

Before the primary schedule, each client-count job runs a and b in separate fresh smoke clusters. Stock etcdctl checks TLS health, Put, Get and Watch. The first runner attempt stopped at endpoint health. Pinned main leaves Alarm unimplemented, but etcdctl v3.7.2 also calls AlarmList during its health check. The revised script records that failure and verifies the unimplemented Alarm response on both variants. It does not call this compatibility check a pass or modify the servers to hide it. Any other health failure still stops the run. The Watch must match the written key, value and exact mod revision. Curl checks `/version` and `/health` with both `--http1.1` and `--http2`, and saves `%{http_version}`. A requested HTTP/2 connection that falls back to HTTP/1.1 fails. `mode=smoke` runs only this check.

For each measured cluster, seed 4,096 Puts and warm up with 20,000 Puts. A new generator warms all 16 TLS connections and parks its workers. The controller fences all replicas at the warmup revision, takes before snapshots, and releases the workers for 60,000 Puts. Workers drain and park again. The controller fences all replicas at the final revision and takes after snapshots before releasing the generator's final validation probe. Etcd also fences each node against its post-WAL.Save committed gauge before a new metrics scrape. Revisions, response terms, leader/term stability, process identity, affinities, RPC errors and status errors must all pass.

## Counters and reports

Each client-count artifact contains raw JSON, logs, etcd Prometheus scrapes, source/build hashes, variant diffs, environment/mount data, commands, affinity snapshots and schedule. `runs.csv` has each repetition and `summary.csv` has medians of five repetitions per store. It reports:

- Throughput and p50, p90, p99, p999 latency.
- Process CPU microseconds/op for each node and generator, plus summed server CPU/op.
- Per-node allocation objects/op and bytes/op from cumulative `runtime.MemStats.Mallocs` and `TotalAlloc` deltas. Stock etcd uses its stock `go_memstats_mallocs_total` and `go_memstats_alloc_bytes_total` metrics. Missing allocation counters fail instead of producing empty cells.
- Per-node Puts/file-sync, with etcd WAL fsyncs and separate backend commit counts. File counts are not syscall wall times. They include segment-header file syncs if a rotation occurs; directory syncs are excluded.

Allocation and sync windows are replica-fenced, slightly broader than the generator's CPU/latency window. They include post-load fence RPCs, debug/scrape overhead and background raft/runtime work. The generator's new connection handshakes and final validation probe are outside these counter windows. Runtime.ReadMemStats briefly stops the world outside the load window; it does not force GC. Snapshot fields and stock Prometheus collectors are not an atomic cross-node sample. At 60,000 operations, fixed fence overhead is amortized, not subtracted. `*_fenced_cpu_us_op` reports process CPU across the controller's broader before/after interval as an extra control. `*_cpu_us_op` uses the original generator window. CPU clocks have `/proc` tick resolution, recorded in the raw JSON.

## Profile mode after selecting a winner

After primary results, commit `mode: "profile"` and `selected_variant: "a"`, `"b"` or `"c"` in `refs.json`. Selection is manual, not an automatic claim of a winner. Profile mode again uses matrix 16/64/256 and five fresh repetitions. The selected variant and stock etcd run back to back in each job, with alternating order, the same warmup and the same 12-second closed-loop Put arrival window. Drain time counts in elapsed time.

Only the selected tigerstore leader gets a 15-second CPU profile. No simultaneous block, mutex, heap or allocation profiles, and no etcd profile. The controller starts the profile request, waits 0.5 seconds, then releases the generator. There is an estimated idle lead of 0.5 seconds and tail of up to about 2.5 seconds, less drain time. HTTP request bounds do not exactly mark runtime sampler start/stop. The tail includes replica fences, observation RPCs and the final validation Range probe. `profile-window.json` records request/release/drain/response timestamps, estimates, elapsed time and operation count. A load that drains after the profile response ends fails attribution.

`cpu-raw.txt`, `cpu-top-flat.txt` and `cpu-top-cumulative.txt` accompany each leader profile. `attribution.json` reports each repetition's sample CPU/op, process leader/server CPU/op and inclusive categories. It aggregates five profiles by sample nanoseconds and operation counts, not by averaging percentages. Categories overlap and must not be added. Profile controls and their unprofiled stock-etcd cohort live under `results/profile`, separate from `results/primary`. Do not use profiled throughput as headline performance.

## Commands

Parent review and execution only. The push trigger registers the workflow on branch `exp/pre-50-frontend`; paths are restricted to this directory and the workflow. Evidence-only commits elsewhere do not start it. `workflow_dispatch` is optional and may be unavailable until the workflow exists on the default branch. Parent can copy downloaded artifacts to `evidence/pre50frontend/`, outside the push filter. Do not poll CI faster than every two minutes.

On the Actions runner the workflow executes:

```sh
timeout --kill-after=5s 800s bash experiments/pre50frontend/run.sh
```

For offline artifact checks, no measurements:

```sh
python3 experiments/pre50frontend/summarize.py experiments/pre50frontend/results/primary/c16
GOFLAGS=-p=2 GOMEMLIMIT=2GiB python3 experiments/pre50frontend/attribute.py experiments/pre50frontend/results/profile/c16
```

Small local validation, no servers or load:

```sh
GOFLAGS=-p=2 GOMEMLIMIT=2GiB go test ./experiments/pre50frontend/load -parallel 2 -count=1
python3 -m unittest discover -s experiments/pre50frontend -p 'test_*.py' -v
bash -n experiments/pre50frontend/run.sh
```

No local measurements, no full test campaign, and no benchmark results are part of this commit. Hosted-runner time and disk speed are unknown until the parent runs it. Both local builds and workflow Go calls use `GOFLAGS=-p=2` and `GOMEMLIMIT=2GiB`; tests use `-parallel 2`.
