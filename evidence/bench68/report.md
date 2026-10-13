# PR #68 self-clocked batching measurements

Measured the requested new head **`b4fb26b`**, not the superseded `acfe8fa`. Its median rate is 2.31, 3.09 and 4.90 times main at 16, 64 and 256 clients. Leader sync densities are 4.422, 16.138 and 11.570 Puts per file fsync. All 45 measured windows passed.

The physical CPU allocation is unchanged, but the Go scheduler configuration is not equal. The new #68 binary forces two Go processors per server on its one pinned CPU. Main and etcd retain one. These numbers compare the requested configurations, not batching alone or a tuned two-processor etcd.

Nothing merged. Only `exp/bench-68` was pushed. No PR or subagent was created. All builds, tests and benchmarks ran on GitHub Actions, not the shared host.

## Commits and runs

- [Experiment branch](https://github.com/djosh34/tigerstore/tree/exp/bench-68), adapted from the [append-clocked pre-work](https://github.com/djosh34/tigerstore/issues/47#issuecomment-6136038385). The processor layout follows the [batch256 finding](https://github.com/djosh34/tigerstore/issues/47#issuecomment-6137116710).
- Measured experiment commit `b51d0403a3223adfd1e124e381c4820abfb71b04`, [successful run 38339879453](https://github.com/djosh34/tigerstore/actions/runs/38339879453).
- Jobs for [16 clients](https://github.com/djosh34/tigerstore/actions/runs/38339879453/job/115108242240), [64 clients](https://github.com/djosh34/tigerstore/actions/runs/38339879453/job/115108242195) and [256 clients](https://github.com/djosh34/tigerstore/actions/runs/38339879453/job/115108242042). Raw artifacts are attached to the run and archived with this report.
- Main source `72bd5d7680dbf456e7811b02f300c26e2b3b805a`, the merge base of main and the candidate. PR #68 source `b4fb26bc596d2f89acfefe73d36cf5f86f7a2ad5`.
- Both sources come from `git archive` and receive the identical pre-work observability patch from baseline `81053bcc54ac315b6ac9302dbe09b5eb591c18db`, plus read-only runtime processor metadata. It adds debug endpoints and file Write/Sync counters. No core, driver, WAL, persistence-order or transport behavior changes. No old prototype batching patch is applied. The counters add measurement overhead to both builds.
- Stock etcd v3.7.2, upstream `68c065e562994b89e333e77b039ad066f933c586`, built with Go 1.26.8. The script verified ARM64 binary SHA-256 `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d`. Tigerstore and the unchanged pre50 client use Go 1.27.2. Executable hashes, module versions, source commits, command logs and raw counters are archived.

The stacked PR base has common ancestor `4f65ce5c14f72277b42bf0030de994656c03eefa` with the candidate. The requested comparison is against main, not that `work/47-lease` base. It includes lease and compaction ancestry and later fixes. It cannot isolate the batching patch.

## Runner, processor settings and workload

Three free `ubuntu-24.04-arm` jobs, one per concurrency. Each job runs main, #68 and etcd back to back, five fresh clusters per store. Odd repetitions use that order; even repetitions reverse it. Different concurrency cells use different VMs.

All runners had four Neoverse-N2 cores, about 16.72 GB RAM, image `ubuntu24-arm64 20261004.142.1`, Linux `6.17.0-1022-azure`, and a 150 GiB virtual disk.

| Setting | Server environment | Go processors per server | Leader CPU | Follower CPUs | Generator |
| --- | --- | ---: | ---: | --- | --- |
| Main, both runs | `GOMAXPROCS=1` | 1 | 0 | 1, 2 | CPU 3, one Go processor |
| #68 `acfe8fa`, earlier run | `GOMAXPROCS=1` | 1 | 0 | 1, 2 | CPU 3, one Go processor |
| #68 `b4fb26b`, final run | `GOMAXPROCS=1`, binary raises it | 2 | 0 | 1, 2 | CPU 3, one Go processor |
| Etcd v3.7.2, both runs | `GOMAXPROCS=1` | 1 | 0 | 1, 2 | CPU 3, one Go processor |

Two Go processors do not add a physical CPU. Every server thread remains pinned to its assigned single CPU. Affinity checks passed in both runs. In the final run, the debug endpoint observed one Go processor in every main server and two in every #68 server before and after load. Each cluster's `layout.json` records this. Etcd's count comes from its configured environment, not the Tigerstore debug endpoint.

Both stores use the root ext4 mount after `sudo mount -o remount,barrier=1 /`; all post-remount checks reject `nobarrier` and `barrier=0`. The replicas share one disk and failure domain. Guest barriers do not prove physical power-loss protection.

The unchanged client uses 16 established connections, leader-only closed-loop Puts, 4,096 registry-shaped keys and 1,024-byte SHA-256-derived values. Values differ by key and repeat on overwrite. Each fresh cluster receives 4,096 seed Puts, 20,000 warmup Puts, a one-second pause and 60,000 measured Puts. Client and peer traffic use certificate-authenticated TLS 1.3. No injected delay, profiling, compression, watches or compaction workload. Etcd keeps safe persistence defaults and an 8 GiB quota.

All 45 final windows passed, with no errors, 60,000 successes, exact revision deltas, unchanged leader and term, all-replica completion fences and correct affinities. Short core and WAL tests passed for both sources. The node and load packages have no test files.

## Throughput and latency at b4fb26b

Medians of five per-run statistics, not pooled percentiles.

| Clients | Store | Put/s | Mean ms | p50 ms | p99 ms | p99.9 ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 16 | Main | 1,879.0 | 8.514 | 9.736 | 17.256 | 24.911 |
| 16 | #68 | 4,338.4 | 3.687 | 3.540 | 7.266 | 15.379 |
| 16 | Etcd | 2,218.3 | 7.212 | 7.140 | 13.461 | 16.284 |
| 64 | Main | 2,885.7 | 22.174 | 21.843 | 38.327 | 49.070 |
| 64 | #68 | 8,906.0 | 7.182 | 6.773 | 15.934 | 23.843 |
| 64 | Etcd | 4,080.8 | 15.679 | 15.154 | 25.957 | 50.462 |
| 256 | Main | 1,622.0 | 157.649 | 151.817 | 313.773 | 377.604 |
| 256 | #68 | 7,950.1 | 32.165 | 30.158 | 69.129 | 92.416 |
| 256 | Etcd | 4,052.8 | 63.079 | 62.030 | 99.522 | 113.875 |

## CPU microseconds per Put

Process user plus system ticks from `/proc/PID/stat`, with start-time checks. The window includes client drain but excludes later follower catch-up. I/O wait and work charged outside these processes are excluded. One tick is 0.1667 CPU us per measured Put. Independent column medians need not sum.

| Clients | Store | Leader | Follower 1 | Follower 2 | All servers | Generator |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 16 | Main | 265.500 | 81.500 | 84.333 | 441.000 | 89.333 |
| 16 | #68 | 202.833 | 61.500 | 63.167 | 327.500 | 85.500 |
| 16 | Etcd | 315.000 | 112.333 | 115.333 | 541.833 | 101.167 |
| 64 | Main | 171.167 | 36.333 | 30.333 | 236.833 | 55.500 |
| 64 | #68 | 109.833 | 16.333 | 14.000 | 139.833 | 50.333 |
| 64 | Etcd | 191.500 | 46.333 | 40.500 | 277.833 | 60.000 |
| 256 | Main | 258.833 | 25.000 | 24.667 | 308.500 | 66.333 |
| 256 | #68 | 125.167 | 11.167 | 11.333 | 147.500 | 51.667 |
| 256 | Etcd | 192.333 | 32.333 | 32.833 | 257.333 | 57.333 |

## Puts per fsync per node

| Clients | Store | Leader | Follower 1 | Follower 2 |
| ---: | --- | ---: | ---: | ---: |
| 16 | Main | 1.252 | 3.316 | 3.259 |
| 16 | #68 | 4.422 | 4.003 | 4.046 |
| 16 | Etcd WAL | 1.888 | 3.434 | 3.450 |
| 64 | Main | 1.044 | 6.109 | 5.992 |
| 64 | #68 | 16.138 | 14.670 | 14.691 |
| 64 | Etcd WAL | 3.352 | 11.492 | 11.409 |
| 256 | Main | 0.999 | 17.678 | 17.539 |
| 256 | #68 | 11.570 | 41.783 | 41.638 |
| 256 | Etcd WAL | 6.225 | 46.656 | 46.584 |

These are gross sync densities, not logical Raft-entry batch sizes. Tigerstore counts file fsyncs, including hard states and segment headers, excluding directories. A density below one means more file fsyncs than successful Puts. Etcd uses its stock WAL sync histogram, mostly fdatasync, not all process fsyncs. Backend commit counts are archived separately.

Disk counter windows surround the client subprocess and extend through all-replica persistence fences. Tigerstore's applied-revision fence relies on async Raft not applying unstable local entries. Etcd can apply before WAL.Save completes, so its fence also waits for the post-Save committed-index gauge and then starts a fresh metrics scrape. CPU uses the narrower client interval, as in pre-work.

## Earlier acfe8fa run

The earlier [run 38338340948](https://github.com/djosh34/tigerstore/actions/runs/38338340948), experiment `03ce0cd2a0a4a3ed79e33ec4bbe01255827b8fe5`, finished before the requested head update. All 45 windows passed. Candidate source was `acfe8fa43993c447b1a322e6dfd59a0492222297`; main and etcd sources were the same as above. All servers used one Go processor. Its full latency, CPU and per-node sync tables and raw counters remain in `evidence/bench68-first-acfe8fa`.

| Clients | Main Put/s | acfe8fa Put/s | Etcd Put/s | acfe8fa leader Puts/fsync |
| ---: | ---: | ---: | ---: | ---: |
| 16 | 2,560.8 | 3,686.0 | 2,798.8 | 1.613 |
| 64 | 2,046.9 | 2,123.9 | 3,901.0 | 0.981 |
| 256 | 2,362.5 | 2,361.3 | 4,526.4 | 1.010 |

These are separate runs on new VMs, with both candidate source and processor settings changed. They are not a same-binary processor ablation or proof of how much each update contributed. No earlier results were discarded or pooled with the final run.

## What is proven, and what is not

In the final run, the requested #68 configuration has higher median rate and lower mean, p50, p99 and p99.9 latency than both requested references in every client-count cell. Its median rate is 1.96, 2.18 and 1.96 times the one-processor etcd reference. Server CPU per Put is 25.7%, 41.0% and 52.2% below main. These ratios compare the separately reported medians.

The candidate's five rates span 3,981.6 to 4,374.2 Put/s at 16 clients, 8,327.5 to 9,055.2 at 64, and 7,906.5 to 8,140.6 at 256. Its leader density grows from 16 to 64 clients, then falls at 256. The 64-client densities span 7.906 to 16.287; at 256 they span 10.655 to 16.060. Larger leader groups than main are demonstrated in this workload, but monotonic growth with client count is not.

This is one runner per concurrency and one final experiment run. No ablation separates the own-batch append gate, minimum Go-processor policy and other source changes. The reference main and etcd keep the requested one-processor setting; this is not an equal-Go-processor comparison. It does not establish a general throughput win, maximum capacity, open-loop latency, delayed-network behavior, follower-client performance or production Kubernetes performance. No statistical significance claim is made.

No crash, disk-failure, gray-node, power-loss or batching-liveness campaign ran here. Passing workload fences and short tests are not a durability proof. The old driver-gated pre-work is a different implementation; its measurements do not certify this core change.

## Reproduction

From measured experiment commit `b51d0403a3223adfd1e124e381c4820abfb71b04`, on disposable ARM runners before the deadline:

```sh
CLIENTS=16 timeout --kill-after=5s 820s bash experiments/pre47groupcommit/run.sh
# Separate jobs use CLIENTS=64 and CLIENTS=256.
```

From the evidence checkout, regenerate both summaries without running benchmarks:

```sh
python3 experiments/pre47groupcommit/summarize.py
python3 experiments/pre47groupcommit/summarize.py evidence/bench68-first-acfe8fa
```

The adaptation keeps the pre-work client, workload, alternating order, CPU pinning and fences. It changes source selection, identical counter installation, layout metadata, evidence paths and deadline checks. Raw files use `baseline` for main and `tigerstore` for #68. Each `c*/commands.json` contains the full argv. The client saves percentile aggregates, not individual latency samples.
