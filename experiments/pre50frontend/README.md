# Main frontend baseline for #74

Question: what does main's current ServeHTTP path cost per Put, and what remains outside that frontend?

The architect narrowed this experiment before any native-variant measurement completed. Current jobs compare only main `eb8a20eda7b1436e3319b924753469f2d8c0ee99` with stock etcd v3.7.2. Native variants b and c remain on throwaway branches but are not measured or recommended. No code here is for merging.

## Sources

The runner archives the exact main SHA, not the experiment branch's server files. Main changes only by the identical observation files and the command hook in `observability.patch`. These count file Write/Sync calls after calling the original os.File methods and return the original errors. Directory syncs remain unchanged and are not counted. No core, node, WAL, peer, apply or acknowledgement behavior changes.

The stock etcd v3.7.2 ARM64 executable has SHA-256 `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d`. The runner checks that hash and records binary, archive and source hashes.

Borrowed code:

- `exp/pre-50-profile` at `1d90dcebf4ff9c910f7053dc0634d251767fd92b` supplies the load generator and pprof parser.
- `exp/bench-68` at `32ceb80fb36fd4229c2e2e7594b40da197fe4eaf` supplies process setup, revision and post-WAL.Save fences, CPU and latency reporting.

The generator calls the generated etcd protobuf `pb.KVClient` through grpc-go. It is not the custom lean HTTP/2 framer. Its request loop, key/value corpus, connection warmup, CPU sampling and percentile calculation remain unchanged. New barriers let the controller sample counters while workers are parked. Each Put overwrites one of 4,096 keys with a 1,024-byte value. All cells use 16 connections.

Both main and stock etcd use their shared TLS client ports and ServeHTTP. Etcd's native cmux routing applies to its insecure shared port, not this TLS configuration.

## Runner method

Free `ubuntu-24.04-arm`, three jobs for 16, 64 and 256 clients. Each job runs five fresh three-process clusters per store, back to back, reversing store order every second repetition. Leader CPU0, followers CPU1/2, generator and controller CPU3. Every server has `GOMAXPROCS=2`, including etcd. The generator has `GOMAXPROCS=1`. All Go runs use `GOFLAGS=-p=2 GOMEMLIMIT=2GiB`; tests use `-parallel 2`.

The runner remounts its disposable ext4 filesystem with barriers enabled and rejects `nobarrier` or `barrier=0`. Client and peer TLS remain enabled. Three data directories share the VM's disk, not separate failure domains. Guest barriers cannot prove physical power-loss protection.

Every job has a 15-minute limit, an 800-second script limit and a 600-second measurement limit. A partial cohort fails. Commands check deadline `2026-10-13T22:00:00Z`. No measurements run locally.

Servers start pinned, then all their threads move to the required role's CPU after election. The runner checks all server and generator thread masks before and after the window. It verifies main's runtime processor count and etcd's process environment. These checks establish the observed layout, not every instant between samples.

For each cluster, seed 4,096 Puts and warm up with 20,000. A new generator establishes 16 TLS connections and parks its workers. The controller fences every replica at the warmup revision and snapshots counters, then releases 60,000 measured Puts. Workers drain and park again. Every replica must apply the final revision before the after snapshots. Etcd additionally fences its post-WAL.Save committed gauge before a new metrics scrape. Revisions, response terms, leader/term stability, process identity, affinities, status errors and RPC errors must pass.

## Compatibility observations

The first attempted run stopped at `etcdctl endpoint health`. That command also calls AlarmList, which pinned main leaves unimplemented. The second attempt recorded that exact failure on both main and b. It then found that curl's HTTP/2 `/version` failed on b while main passed, despite b's Go HTTP/2 test passing. Neither attempt reached measured Put windows. These are recorded failures, not passing compatibility checks.

The final baseline checks main's TLS Put, Get, Watch, HTTP/1.1 and HTTP/2 `/health` and `/version` in a separate fresh smoke cluster. Watch must match the written key, value and exact mod revision. Curl saves its negotiated HTTP version and must not fall back. The known endpoint-health failure remains recorded with `compatible: false`, and a separate `alarm list` must fail with Unimplemented. No server patch conceals that gap. Any other failure stops the job.

## Reported counters

Artifacts retain raw JSON, etcd scrapes, commands, environment/mount data, affinity snapshots and source/build hashes. `runs.csv` records each repetition; `summary.csv` records medians of five per-run statistics, not pooled latency percentiles.

- Throughput, mean and p50/p90/p99/p99.9 latency.
- Process user plus system CPU microseconds per Put for each node and generator.
- Per-node allocated objects and bytes per Put. Main uses cumulative runtime.MemStats.Mallocs and TotalAlloc. Etcd uses its stock go_memstats_mallocs_total and go_memstats_alloc_bytes_total metrics. Missing counters fail the summary.
- Puts per file sync per node. Main counts file fsync calls, including segment headers. Etcd reports its WAL sync histogram, predominantly fdatasync. Its separate backend commit count remains in the raw rows. Neither denominator includes directory syncs.

Allocation and sync windows include replica catch-up, fence requests, snapshots and background work. Connection establishment and the generator's final validation probe are outside them. Runtime.ReadMemStats runs outside the load window and does not force GC. Snapshot fields and stock Prometheus collectors are not an atomic cross-node sample. Fixed observation overhead is amortized over 60,000 Puts, not subtracted.

CPU/latency uses the generator's narrower window. CPU excludes later follower catch-up and work outside the measured processes. Extra fenced CPU columns expose the broader counter window as a control. `/proc` CPU tick resolution is recorded.

## Leader profiles

Profile mode again runs main and etcd back to back, five fresh repetitions at 16, 64 and 256 clients. It uses 12 seconds of closed-loop arrivals plus drain. Only main's leader gets a 15-second CPU profile. No simultaneous block, mutex, heap or allocation profiling runs. Profile results stay separate from unprofiled headline rows.

The controller starts the profile request, waits 0.5 seconds, then releases the generator. The capture includes about 0.5 seconds of idle lead and up to 2.5 seconds of idle tail, less drain time. The tail includes fences and observation RPCs. `profile-window.json` records request/release/drain/response times and operation counts. HTTP request bounds do not exactly mark sampler start/stop. A drain beyond capture end fails attribution.

Each profile has raw, flat-top and cumulative-top text. `attribution.json` records each repetition and sums sampled nanoseconds and operation counts across five profiles. Categories include owner loop, core, entry encoding/decoding, protobuf, allocations/GC, Raft, WAL and peers. Inclusive categories overlap and must not be added. CPU in a syscall stack is not the blocked wall time of an fsync.

## Commands

Only on the disposable ARM runner, before the deadline:

```sh
CLIENTS=16 MODE=primary timeout --kill-after=5s 800s bash experiments/pre50frontend/run.sh
CLIENTS=16 MODE=profile SELECTED_VARIANT=a timeout --kill-after=5s 800s bash experiments/pre50frontend/run.sh
# Separate identical jobs use CLIENTS=64 and CLIENTS=256.
```

The workflow reads committed `refs.json`. Change its mode to `profile` and selected variant to `a` after the baseline. Push triggers only changes under `experiments/pre50frontend/` or its workflow. Evidence archives belong under `evidence/pre50frontend/` so they do not launch another measurement.

Offline summaries need no server or workload:

```sh
python3 experiments/pre50frontend/summarize.py evidence/pre50frontend/primary/c16
GOFLAGS=-p=2 GOMEMLIMIT=2GiB python3 experiments/pre50frontend/attribute.py evidence/pre50frontend/profile/c16
python3 -m unittest discover -s experiments/pre50frontend -p 'test_*.py'
```
