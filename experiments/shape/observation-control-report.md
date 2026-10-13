# Observation-control report

## Scope and validity

Runs [38424937611](https://github.com/djosh34/tigerstore/actions/runs/38424937611) and [38424941260](https://github.com/djosh34/tigerstore/actions/runs/38424941260) passed. The downloaded artifacts contain six closed-loop cells, five repetitions and four targets per cell. All 120 load results and audits are valid, with complete cohorts, no request or sampling errors, stable server PID/starttime identities and expected revision deltas. The script also checks endpoint balancing, process CPU accounting, placement and native-status fences.

Targets are pristine `e5c3327e3800edf64f86ed8c1971ba7ebc1e8342`, observation-capable Tigerstore with observation off, the same executable with observation on, and etcd 3.7.2. Build manifests record SHA-256 `f42998c8e42725fd453bd9f6d672d49fa4bf20aa8a43feb3f85a80d3a8b6505a` for pristine and `10ce6b1e8361828b651f12531e18909da364dc1353cff586f335a6a156e40f0b` for both off and observed. The control build script makes `tigerstore-off` a symlink to `tigerstore`. These are manifest identities, not a local rehash of downloaded executables; this artifact set does not include executables. Runner source is `a0d88e2e69593332ee64ac10f3d553dccdcdec4e`, built with Go 1.27.2 on linux/arm64.

Put and point Range have three-second arrival windows; LIST has six. All have one-second time-based warmup, 18 connections and 30-second allowed drain. Put/Range use 4096 keys; LIST returns 1000 seeded 1 KiB values. These are free four-CPU Neoverse-N2 runners, with leader CPU0, followers CPU1/2, generator and external sampler CPU3. Server GOMAXPROCS is 2 and generator GOMAXPROCS is 1. TLS remains enabled and there is no injected regional delay.

All three Tiger variants use native applied/committed/revision status agreement, a 0.3-second cooldown and a second unchanged status, before and after each cohort. This does not prove all persistence jobs have completed. All 90 Tiger audits explicitly set `physical_counter_fence=false`. The campaign summarizer deliberately skips physical counters for observation-control windows, including its etcd rows. **No file-write, fsync, physical-byte, disk or owner-queue conclusions are made here.** Etcd uses its own post-save fence rather than the Tiger cooldown, so it is context, not a fourth identical-fence treatment.

## CPU and time accounting

The main CPU measure is `shape.cpu_through_drain.server_us_per_op_by_node`, summed across all three replicas and divided by `shape.success`. It covers generator CPU samples before admission through the end of request drain. It excludes the later native-status/cooldown fence. The legacy top-level `server_cpu_us_per_op` instead divides completion-window CPU by `load.ops`, the operations completed within the arrival window. Neither is a six-second pprof estimate from the earlier diagnostic captures.

The reproduction script recovers integer user-plus-system CPU deltas from both views, verifies Linux 10 ms process-accounting ticks, checks the window node sums against the cluster field, and reconstructs window elapsed time from CPU deltas and reported cores. Both views fit within the independently recorded, wider external process snapshots. All process identities match the generator's leader-first PID list.

The timed completion-window sampler runs slightly late. Across Tiger rows it overruns the configured window by 0.078 to 12.352 ms. The cohort CPU clock exceeds the dispatch through-drain clock by 0.060 to 0.107 ms, consistent with sampling boundaries. Do not substitute the exact configured three/six seconds for the recorded CPU clock, or mix a completion-window numerator with the whole-cohort denominator. Individual server CPU precision is one 10 ms tick, not the printed decimal precision. Generator CPU measures only loadgen, not the Python scraper/sampler on CPU3.

TP below is `shape.success / shape.through_drain_s`; latency quantiles include successful cohort responses through drain. Values in change tables are percentages, shown as the median followed by the minimum and maximum of all five repetition-number pairs. They are not confidence intervals, pooled quantiles or ratios of separate medians. Raw individual results and all five changes are archived.

### Absolute median TP and cohort CPU

Each entry is `operations/s / cluster CPU us per success`. All target ranges, p50, p99 and p99.9 are in the generated tables.

| Cell | Pristine | Off | Observed | Etcd |
| --- | ---: | ---: | ---: | ---: |
| Put 1 KiB, leader, 256 | 40212 / 34.16 | 41766 / 32.83 | 39656 / 34.82 | 5124 / 270.97 |
| Put 1 KiB, leader, 4096 | 71247 / 14.06 | 73292 / 13.84 | 73101 / 14.00 | 4778 / 263.95 |
| Put 10 KiB, leader, 256 | 8264 / 89.96 | 8359 / 88.43 | 7663 / 86.77 | 2527 / 526.57 |
| Point Range, leader, 1024 | 125663 / 8.93 | 123438 / 9.09 | 121398 / 9.31 | 6713 / 160.06 |
| LIST, leader, 64 | 793 / 1280.18 | 789 / 1285.00 | 774 / 1322.97 | 309 / 3451.51 |
| LIST, balanced, 64 | 1121 / 1494.29 | 1117 / 1507.40 | 1100 / 1523.21 | 871 / 3085.23 |

### Observed versus off, combined enabled pipeline

| Cell | TP % | Cohort CPU % | Window CPU % | p50 % | p99 % |
| --- | ---: | ---: | ---: | ---: | ---: |
| Put 1 KiB, leader, 256 | -5.05 [-10.62, 3.33] | 4.93 [-0.28, 10.56] | 4.96 [-0.29, 10.58] | 1.72 [-0.58, 4.78] | 13.85 [-25.05, 50.67] |
| Put 1 KiB, leader, 4096 | -1.21 [-2.68, 7.20] | 2.27 [-3.73, 4.17] | 1.82 [-3.75, 3.79] | 0.96 [-7.26, 3.34] | 2.68 [-7.67, 10.63] |
| Put 10 KiB, leader, 256 | -0.15 [-12.86, 0.93] | 0.73 [-1.88, 2.60] | 1.27 [-1.81, 4.15] | 2.00 [-3.15, 7.19] | 8.52 [0.00, 915.49] |
| Point Range, leader, 1024 | -1.63 [-1.85, 1.09] | 2.28 [-1.59, 3.10] | 2.16 [-1.88, 2.81] | 0.87 [-0.85, 1.71] | -0.68 [-8.51, 8.75] |
| LIST, leader, 64 | -1.47 [-3.31, 2.87] | 2.29 [-2.67, 3.91] | 2.32 [-2.71, 3.62] | 1.40 [-3.34, 7.27] | 1.57 [-9.38, 5.28] |
| LIST, balanced, 64 | -2.05 [-2.68, 1.42] | 1.28 [-1.87, 2.59] | 1.10 [-1.80, 2.45] | -3.92 [-7.63, 8.90] | 1.21 [-15.97, 17.82] |

This contrast includes enabled file/owner/peer hooks and sampling, serving `/debug/shape`, and the added HTTP/JSON scraping work on CPU3. The base 100 ms external sampler runs for off and pristine too, but only observed Tiger fetches the three debug snapshots. The server CPU delta includes serving those requests. TP and latency can also change because the scraper shares CPU3 with the generator. No independent scraper CPU counter or isolated in-server-only treatment is available, so this is not pure hook overhead.

The median paired cohort CPU increase is 0.73 to 4.93%, with mixed-sign individual pairs in every cell. That is not an overhead bound. Put-1K-256 has a paired p99.9 median increase of 117.20%, ranging from -38.39 to 205.33%. Put-10K-256 contains an observed p99 of 395.313 ms and a maximum paired p99 increase of 915.49%. Attribution of those tails to hooks alone is unsupported. Its TP median-pair change is -0.15%, whereas the ratio of separate TP medians is about -8.33%; the two statistics answer different questions.

### Off versus pristine, compiled hook and layout contrast

| Cell | TP % | Cohort CPU % | Window CPU % | p50 % | p99 % |
| --- | ---: | ---: | ---: | ---: | ---: |
| Put 1 KiB, leader, 256 | 1.11 [-4.21, 4.39] | -3.79 [-5.00, 1.91] | -3.11 [-4.78, 1.67] | -1.18 [-1.71, 2.28] | 7.09 [-18.46, 15.48] |
| Put 1 KiB, leader, 4096 | 4.42 [-2.10, 17.60] | 0.15 [-2.79, 2.94] | 0.22 [-3.15, 3.62] | -3.70 [-8.70, 2.95] | -13.65 [-56.41, 7.51] |
| Put 10 KiB, leader, 256 | 2.72 [-12.39, 18.56] | -1.70 [-4.33, 2.94] | -1.81 [-4.81, 2.46] | -1.69 [-10.75, 11.63] | -7.52 [-80.58, 63.54] |
| Point Range, leader, 1024 | -1.79 [-3.76, 0.57] | 2.50 [-0.77, 3.93] | 2.80 [-1.07, 4.56] | 2.18 [0.00, 3.89] | -0.66 [-3.30, 14.63] |
| LIST, leader, 64 | -0.76 [-0.94, 1.71] | 0.55 [-1.37, 1.02] | 0.39 [-1.36, 0.86] | 0.00 [-2.83, 2.14] | -0.49 [-4.53, 4.38] |
| LIST, balanced, 64 | -1.06 [-2.70, 1.79] | 0.88 [-2.20, 2.96] | 0.97 [-1.82, 3.07] | -1.06 [-8.31, 9.17] | 6.88 [-5.02, 49.03] |

Off disables observation runtime activity but retains compiled hook branches, added fields and changed code/data layout. It is not pristine main with a runtime flag. The off/pristine contrast includes those compiled differences and run variability; negative changes do not prove the added code improves performance. The cohort CPU median changes range from -3.79 to 2.50%, and every cell has both positive and negative individual pairs.

### Observed versus pristine, full deployment contrast

| Cell | TP % | Cohort CPU % | Window CPU % | p50 % | p99 % |
| --- | ---: | ---: | ---: | ---: | ---: |
| Put 1 KiB, leader, 256 | -4.44 [-10.71, 7.53] | 4.82 [-4.06, 5.12] | 4.83 [-3.39, 5.42] | 2.28 [-1.73, 3.99] | 15.88 [-36.38, 73.99] |
| Put 1 KiB, leader, 4096 | 1.62 [-2.85, 26.07] | 2.32 [-3.59, 7.23] | 2.04 [-3.32, 7.54] | -2.26 [-15.32, 5.90] | -12.46 [-59.75, 15.70] |
| Put 10 KiB, leader, 256 | -2.27 [-12.52, 7.43] | -0.49 [-3.63, 5.62] | -0.09 [-3.60, 6.71] | 1.27 [-9.64, 13.87] | -1.88 [-9.39, 497.23] |
| Point Range, leader, 1024 | -2.37 [-4.52, -1.07] | 2.86 [1.49, 5.01] | 2.87 [1.50, 5.02] | 2.65 [0.87, 4.75] | 2.64 [-5.98, 5.21] |
| LIST, leader, 64 | -2.06 [-3.75, 4.63] | 2.74 [-4.00, 4.70] | 2.27 [-4.04, 4.42] | 2.85 [-4.62, 4.27] | 0.50 [-8.48, 6.03] |
| LIST, balanced, 64 | -1.32 [-3.14, -0.94] | 0.61 [0.33, 4.47] | 0.58 [0.34, 4.61] | -5.31 [-10.91, 4.89] | 8.17 [2.87, 50.14] |

## Limits and reproduction

Targets alternate order. Repetitions 1, 3 and 5 run pristine, off, observed, etcd; repetitions 2 and 4 reverse that order. Pairing by repetition controls the VM/cell, not time drift, order effects or identical prior work. One-second warmup is time-based, not operation-count matched. For Put-1K-256, the pre-cohort revision is 31,566 to 48,569 for pristine, 42,102 to 46,429 for off and 43,644 to 46,934 for observed. Those revisions include seed/probe/warmup state, not exact warmup counts. Range/LIST revisions match but their warmup read counts are not recorded. Different GC, allocation and WAL states can therefore survive the nominal warmup. Five repetitions do not isolate code-layout effects or establish a reliable tail-latency bound.

The newly found fixed-arrival 100 ms fallthrough bug does not affect these `offered_rate=0` closed controls or the prior closed CPU/trace profiles. This report contains no open-arrival claims. Prior open-arrival claims are retired pending corrected-generator reruns.

[Script and instructions](observation-control-analysis/README.md), [generated tables](observation-control-analysis/archive/computed-tables.md), [raw load results and all individual calculations](observation-control-analysis/archive/computed.json.gz), and [input hashes](observation-control-analysis/archive/input-manifest.json) are committed. The generated tables include all five ranges for p99.9, leader and generator CPU as well as the reported metrics. Analysis was offline only; no measurement, server, benchmark, test or runner was started locally. Nothing was pushed or publicly posted.
