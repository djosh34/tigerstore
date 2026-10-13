# Earlier PR #68 acfe8fa measurements

Historical run. The user requested a rerun at `b4fb26b` before this draft was posted. No PR comment was posted from this report. All servers in this earlier run used one Go processor; the new candidate forces at least two. These measurements remain separate from the new-head comparison.

At 64 and 256 clients, this run does not show the intended growth in leader batching. PR #68 completes **0.981 and 1.010 Puts per leader file fsync**, versus etcd's 3.367 and 6.223 Puts per WAL sync. Its median throughput is 3.8% above main at 64 clients and effectively unchanged at 256. At 16 clients, its median rate is 44% above main and 32% above etcd.

Nothing merged. Only `exp/bench-68` was pushed. No PR or subagent was created. All builds, tests and benchmarks ran on GitHub Actions, not the shared host.

- [Experiment branch](https://github.com/djosh34/tigerstore/tree/exp/bench-68), adapted from the [pre-work comment on #47](https://github.com/djosh34/tigerstore/issues/47#issuecomment-6136038385).
- Measured experiment commit `03ce0cd2a0a4a3ed79e33ec4bbe01255827b8fe5`, [successful run 38338340948](https://github.com/djosh34/tigerstore/actions/runs/38338340948).
- Jobs and raw artifacts for [16 clients](https://github.com/djosh34/tigerstore/actions/runs/38338340948/job/115103449056), [64 clients](https://github.com/djosh34/tigerstore/actions/runs/38338340948/job/115103448908) and [256 clients](https://github.com/djosh34/tigerstore/actions/runs/38338340948/job/115103449041).
- Main comparison source `72bd5d7680dbf456e7811b02f300c26e2b3b805a`, the merge base of main and the requested candidate. PR #68 source `acfe8fa43993c447b1a322e6dfd59a0492222297`.
- Both sources come from `git archive` and receive the identical pre-work observability patch from baseline `81053bcc54ac315b6ac9302dbe09b5eb591c18db`. It adds debug endpoints and file Write/Sync counters. No core, driver, WAL, persistence-order or transport behavior changes. No old prototype batching patch is applied. The counters add measurement overhead to both builds.
- Stock etcd v3.7.2, upstream `68c065e`, built with Go 1.26.8. The script verified ARM64 binary SHA-256 `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d`. Tigerstore and the unchanged pre50 load client use Go 1.27.2. Executable hashes, module versions, source commits, command logs and raw counters are archived.

PR #68 targets `work/47-lease`, whose merge base with the candidate is `4f65ce5c14f72277b42bf0030de994656c03eefa`. This requested comparison is against main, not that stacked PR base. It includes the candidate's lease and compaction ancestry, so it does not isolate the batching patch.

## Runner and workload

Three free `ubuntu-24.04-arm` jobs, one per concurrency. Each job runs main, #68 and etcd back to back, five fresh clusters per store. Odd repetitions use that order; even repetitions reverse it. Different concurrency cells use different VMs.

All runners had four Neoverse-N2 cores, about 16.72 GB RAM, image `ubuntu24-arm64 20261004.142.1`, Linux `6.17.0-1022-azure`, and a 150 GiB virtual disk. Leader CPU 0, followers CPUs 1 and 2, generator CPU 3, all `GOMAXPROCS=1`. The script checked every server thread's affinity. Both stores use the root ext4 mount after `sudo mount -o remount,barrier=1 /`; all post-remount checks reject `nobarrier` and `barrier=0`. The replicas share one disk and failure domain. Guest barriers do not prove physical power-loss protection.

The client is unchanged from pre-work. It uses 16 established connections, leader-only closed-loop Puts, 4,096 registry-shaped keys and 1,024-byte SHA-256-derived values. Values differ by key and repeat on overwrite. Each cluster receives 4,096 seed Puts, 20,000 warmup Puts, a one-second pause and 60,000 measured Puts. Client and peer traffic use certificate-authenticated TLS 1.3. No injected delay, profiling, compression, watches or compaction workload. Etcd keeps safe persistence defaults and an 8 GiB quota.

All 45 measured windows passed, with no errors, 60,000 successes, exact revision deltas, unchanged leader and term, all-replica completion fences and correct affinities. The runner also passed short core and WAL tests for both sources. The node and load packages have no test files.

## Throughput and latency

Medians of five per-run statistics, not pooled percentiles.

| Clients | Store | Put/s | Mean ms | p50 ms | p99 ms | p99.9 ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 16 | Main | 2,560.8 | 6.247 | 6.519 | 11.195 | 17.119 |
| 16 | #68 | 3,686.0 | 4.340 | 3.041 | 9.240 | 15.505 |
| 16 | Etcd | 2,798.8 | 5.716 | 5.516 | 11.700 | 15.578 |
| 64 | Main | 2,046.9 | 31.257 | 31.159 | 52.899 | 62.429 |
| 64 | #68 | 2,123.9 | 30.128 | 30.791 | 51.964 | 58.631 |
| 64 | Etcd | 3,901.0 | 16.399 | 15.885 | 25.933 | 30.468 |
| 256 | Main | 2,362.5 | 108.295 | 103.326 | 218.391 | 266.674 |
| 256 | #68 | 2,361.3 | 108.300 | 104.590 | 213.312 | 255.455 |
| 256 | Etcd | 4,526.4 | 56.478 | 55.862 | 95.057 | 115.104 |

## CPU microseconds per Put

Process user plus system ticks from `/proc/PID/stat`, with start-time checks. The window includes client drain but excludes later follower catch-up. I/O wait and work charged outside these processes are excluded. One tick is 0.1667 CPU us per measured Put. Independent column medians need not sum.

| Clients | Store | Leader | Follower 1 | Follower 2 | All servers | Generator |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 16 | Main | 207.500 | 67.000 | 57.833 | 332.333 | 79.167 |
| 16 | #68 | 172.333 | 53.167 | 45.833 | 271.333 | 74.167 |
| 16 | Etcd | 262.333 | 92.667 | 85.333 | 438.667 | 86.000 |
| 64 | Main | 207.667 | 39.667 | 38.667 | 283.667 | 55.500 |
| 64 | #68 | 204.500 | 39.167 | 36.167 | 279.333 | 55.000 |
| 64 | Etcd | 184.833 | 45.000 | 44.167 | 275.833 | 57.167 |
| 256 | Main | 188.833 | 21.833 | 19.500 | 230.167 | 61.000 |
| 256 | #68 | 184.667 | 21.833 | 19.500 | 226.167 | 60.167 |
| 256 | Etcd | 173.833 | 30.500 | 29.500 | 233.833 | 54.167 |

## Puts per fsync per node

| Clients | Store | Leader | Follower 1 | Follower 2 |
| ---: | --- | ---: | ---: | ---: |
| 16 | Main | 0.948 | 2.844 | 2.805 |
| 16 | #68 | 1.613 | 3.240 | 3.237 |
| 16 | Etcd WAL | 1.873 | 3.481 | 3.430 |
| 64 | Main | 0.998 | 5.980 | 5.950 |
| 64 | #68 | 0.981 | 6.080 | 6.091 |
| 64 | Etcd WAL | 3.367 | 11.617 | 11.490 |
| 256 | Main | 0.998 | 17.689 | 17.580 |
| 256 | #68 | 1.010 | 17.916 | 17.762 |
| 256 | Etcd WAL | 6.223 | 46.189 | 46.368 |

These are gross sync densities, not logical Raft-entry batch sizes. Tigerstore counts file fsyncs, including hard states and segment headers, excluding directories. A density below one means more file fsyncs than successful Puts, not a fractional logical batch. Etcd uses its stock WAL sync histogram, mostly fdatasync, not all process fsyncs. Backend commit counts are archived separately.

Disk counter windows surround the client subprocess and extend through all-replica persistence fences. Tigerstore's applied-revision fence relies on async Raft not applying unstable local entries. Etcd can apply before WAL.Save completes, so its fence also waits for the post-Save committed-index gauge and then starts a fresh metrics scrape. CPU uses the narrower client interval, as in pre-work.

## What is proven, and what is not

The stated workload completed with all measurement checks passing. In this run, #68 improves the 16-client median rate and latency while using 18.4% less server CPU per Put than main. Its five rates span 3,124.6 to 5,695.1 Put/s, and leader sync densities span 1.347 to 4.405. This cell has substantial variation.

At 64 clients, #68's five rates span 2,053.7 to 2,484.2 Put/s, overlapping main's 1,763.3 to 2,166.0. At 256 clients, #68 spans 2,248.5 to 2,513.7, overlapping main's 2,310.9 to 2,412.9. These results do not establish a high-concurrency throughput win. At 64 and 256 clients, #68 reaches only 54.4% and 52.2% of etcd's median rate, with about twice its median p99 latency.

The candidate's leader sync density does not grow with client count. Its follower densities grow, but remain close to main. This run therefore does not demonstrate that #68 solves the high-load leader batching problem. It does not identify the cause. No ablation separates batching from the candidate's other changes, and no statistical significance claim is made.

This is one runner per concurrency and one complete experiment run. Comparisons with the old pre-work results would also change source commits and VMs. No maximum-capacity, open-loop, delayed-network, follower-client or production Kubernetes result is established. No crash, disk-failure, gray-node, power-loss or batching-liveness campaign ran here. Passing workload fences and short tests are not a durability proof.

## Reproduction

From measured experiment commit `03ce0cd2a0a4a3ed79e33ec4bbe01255827b8fe5`, on disposable ARM runners before the deadline:

```sh
CLIENTS=16 timeout --kill-after=5s 820s bash experiments/pre47groupcommit/run.sh
# Separate jobs use CLIENTS=64 and CLIENTS=256.
python3 experiments/pre47groupcommit/summarize.py
```

The workflow, `run.sh`, `run.py` and `summarize.py` retain the pre-work workload, alternating order, CPU pinning and fences. The adaptation changes source selection, identical counter installation, evidence paths and deadline checks. Raw files use `baseline` for main and `tigerstore` for #68. Each `c*/commands.json` contains the full argv. The client saves percentile aggregates, not individual latency samples. From the updated evidence checkout, reproduce this earlier run's CSV files with `python3 experiments/pre47groupcommit/summarize.py evidence/bench68-first-acfe8fa`. The default path now holds the new-head measurements.
