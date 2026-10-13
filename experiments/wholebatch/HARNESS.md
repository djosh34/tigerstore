# Runner scripts and metric contract

The parent owns `README.md` and the final #74 comment. These scripts make no performance claim until the integrated candidate runs on GitHub Actions.

## Integration

Cherry-pick the harness commit onto `exp/pre-50-wholebatch` after the frontend, batch command and node changes. A push to that branch starts `.github/workflows/wholebatch.yml`. A push to `exp/pre-50-wholebatch-harness` does not start it. Do not push the integrated branch until its code is ready.

The workflow has one correctness job and one matched benchmark job per 16, 64 and 256 clients. Correctness runs only `internal/batchcmd`, `internal/batchfront`, `internal/batchmem`, `internal/core`, `internal/node` and `internal/lint` with `-race -count=1 -parallel 2 -timeout 5m`. It does not run simulator seed campaigns. The correctness job has a 10-minute limit. Benchmark jobs have a 14-minute limit and a 780-second script limit.

Each benchmark job archives and builds these sources, then runs five repetitions of every target on that same runner:

- Exact main baseline `eb8a20eda7b1436e3319b924753469f2d8c0ee99`.
- Stock etcd v3.7.2, with its ARM64 binary SHA-256 checked.
- One candidate binary with `TIGERSTORE_FRONTEND=wholebatch` and `TIGERSTORE_BATCH_MODE=rpc`, `admit` and `full`.
- The full candidate with `TIGERSTORE_BATCH_MEMORY=ring` and `pool`, under the same transient byte budgets.
- An optional additional source selected by `EXTRA_BASELINE_REF`, labelled `extra`, with `EXTRA_BASELINE_FRONTEND=native` by default. Set the frontend explicitly if this additional experiment uses another selector. This does not replace the seven default targets.

All seven default targets run five times in each client-count job. An earlier run included two native prototypes. Experiment 1 dropped them without declaring a winner; their old data are not the final comparison.

For the on-push path, set repository variables `WHOLEBATCH_EXTRA_BASELINE_REF` and `WHOLEBATCH_EXTRA_BASELINE_FRONTEND` before the parent's push if a best native candidate has appeared in #74. For a registered workflow, dispatch inputs `candidate_ref`, `extra_baseline_ref` and `extra_baseline_frontend` provide the same options. Refs resolve to commits in `variants.json`. Empty candidate ref means workflow checkout HEAD. All experiment code and the generator come from the workflow checkout, not the optional extra ref.

Every source gets the same observation overlay in its disposable archive. `instrument.py` inserts one command hook and creates `cmd/tigerstore/pre50.go` and `internal/osfs/pre50.go`. It changes no checkout files and refuses pre-existing hooks rather than silently combining instruments. An insertion failure requires coordination with the parent. No core, node or server source changes are needed for observation.

## Matched configuration

The script requires GitHub Actions on ARM64. It remounts the disposable runner root with ext4 barriers enabled, checks the data filesystem and saves the mount options. It cannot start a local measurement.

All stores use the same generated CA, client certificates and per-node certificates. Client traffic negotiates TLS 1.3 with mTLS. Peer traffic uses mTLS. etcd also pins its peer TLS range to TLS 1.3. Nodes run on loopback with no injected regional delay. Each new cluster gets fresh data directories and a unique token.

After election, the runner pins every existing thread of the leader to CPU 0, followers to CPUs 1 and 2, and the generator and orchestration to CPU 3. It verifies node thread affinities at the end. Every server receives `GOMAXPROCS=2`. Tigerstore's debug endpoint verifies the actual value. etcd's value is recorded from its environment. The generator receives `GOMAXPROCS=1`.

The unchanged pre50 direct-protobuf generator uses 16 connections at every concurrency. It records `conns=16` in every result. Each cluster receives 4,096 seed Puts, 8,000 warmup Puts and 30,000 measured Puts. Values are independent deterministic 1 KiB byte sequences over the same 4,096 keys. All requests target the elected leader. Variant order rotates across repetitions; stores never overlap.

## Metrics

- Throughput is successful Puts divided by generator elapsed time from worker release to completion, including the final drain. This is closed-loop capacity, not an open-loop offered rate.
- Latency is elapsed time around each successful unary RPC, including request context setup and cancellation. Percentiles use the nearest-rank method. Raw JSON and CSV contain p50, p90, p95, p99 and p999, plus mean and maximum in raw JSON.
- Per-node process CPU/op is the delta of `/proc/PID/stat` user plus system ticks, divided by `CLK_TCK` and by 30,000 successful Puts. It excludes connection setup, the leading and trailing revision probes, and replica catch-up fences. PID start-time checks reject restarts. CPU rows use leader, follower1, follower2 and generator order. Total server CPU/op sums the three server processes. This is not owner-goroutine CPU attribution.
- Allocation counts use Tigerstore `runtime.MemStats.Mallocs` and `TotalAlloc`. etcd uses `go_memstats_mallocs_total` and `go_memstats_alloc_bytes_total` from its stock metrics endpoint. The generator scrapes every node sequentially after connection setup and the leading revision probe, before the first CPU sample. It scrapes again after the last CPU sample and before the trailing revision probe. Deltas divide by 30,000 successful Puts. This slightly wider window includes small debug scrape and process-sampling overhead. It does not include connection creation. It counts total heap allocations and allocated bytes, not live or retained bytes. It includes all goroutines in each process. It does not wait for follower catch-up, just as the CPU window does not.
- Puts/file-fsync uses a separate, wider, fully applied cohort window. Before and after the measured load, every node must report the exact expected revision through a serializable Range. The term must remain unchanged. Tigerstore counts completed file `Sync` calls, excluding directory syncs but including WAL segment header syncs. etcd counts WAL fsync histogram observations. Its post-`WAL.Save` committed gauge must reach the endpoint status target before a fresh metrics scrape, as in the #68 experiment. This matters because etcd can apply before `WAL.Save` finishes. Puts/fsync is 30,000 divided by the per-node fsync delta. It is not operations per Raft entry. etcd backend commit counts remain separate CSV fields and are not added to its WAL fsync denominator.

Raw measurements must have zero errors, 30,000 successes, one Raft term and a revision delta of exactly 30,000. Changed leader, changed runtime policy, missing counters, incomplete replication, a PID restart or wrong affinity rejects the run. The summary script also requires exactly five repetitions of every manifest target and checks 16 connections, 1 KiB values and 4,096 keys.

These are counting-only runs. Atomic file-call counters execute in the WAL path; debug allocation and metrics scrapes happen outside the CPU window. No fsync wall-clock timing, mutex or block sampling, heap profile capture, runtime trace, or CPU profile capture runs in a headline job. Go's default runtime heap sampling remains unchanged in both binaries. The observation endpoints expose standard pprof handlers for a later, separate diagnostic job. Do not mix a diagnostic result into `put.json` or `summary.csv`.

## Memory and endpoint diagnostics

`/debug/alloc` also records Go runtime GC CPU accounting, heap gauges and the custom frontend's admission, response and control-send counters. The GC counter is `/cpu/classes/gc/total:cpu-seconds`, an estimate from runtime accounting rather than measured process CPU. Stock etcd does not expose the same counter through this setup. Deltas use the allocation-counter boundaries. Leading and trailing revision probes are outside those boundaries.

After the fully applied fence and CPU/latency windows, each cluster waits eight seconds for seven-second pending metadata timeouts. Tigerstore then runs one forced GC and records retained heap, runtime-reserved bytes and allocator bank materialized/pinned budgets. This GC does not contribute to headline GC CPU/op. etcd's retained metrics scrape is not forced-GC-equivalent.

Before load, every target must pass client-port TLS curl GET `/health` and `/version` over HTTP/1.1 and HTTP/2. The final workload is shorter than the initial 60,000-Put attempt because that attempt's 16-client job timed out on its last native variant.

Channel figures count successful sends on named handoffs, not every runtime channel operation. grpc-go's internal queues on main are not counted. The custom frontend's per-stream control sends remain visible separately from batched admission and completion.

## Artifacts and summary

Artifacts are `wholebatch-c16-RUN_ID`, `wholebatch-c64-RUN_ID`, `wholebatch-c256-RUN_ID` and `wholebatch-correctness-RUN_ID`. Benchmark artifacts contain source refs, binary hashes and build metadata, environment and mount details, the complete observation templates and source insertion diffs, command history, placement and runtime metadata, affinity checks, seed/warmup/Put JSON, all-node replication fences, disk counter snapshots and logs. etcd additionally retains raw metrics and WAL-save fence evidence. Private keys, certificates and server data directories are not uploaded.

The correctness artifact contains its tested commit and test output. An artifact existing does not mean the job passed. The parent must check every job result before reporting success.

After downloading benchmark artifacts into their `c16`, `c64` and `c256` directories under one root, run:

```sh
python3 experiments/wholebatch/summarize.py /absolute/path/to/wholebatch
```

The same command accepts one `cCLIENTS` directory. It writes `runs.csv`, `summary.csv`, `paired.csv` and `summary.md`. Medians are independent per metric over five repetitions. `paired.csv` divides each candidate metric by its main, etcd, native, native-tuned or optional extra metric from the same repetition. It also contains the stage comparisons `admit/rpc` and `full/admit`. The report gives the median of the five per-repetition stage ratios for throughput, total server CPU/op, allocations/op, bytes/op, p95 and p99. No ratio of independent medians is presented as a paired result. Job summaries include the median throughput, p50/p95/p99, per-node CPU/op, allocations/op, bytes/op and Puts/fsync.
