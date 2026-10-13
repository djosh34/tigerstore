# Client dispatch changes the measured server cost

The static connection control improves leader-only throughput at concurrency 256 and lowers leader CPU per Put in every paired window at all three concurrencies. The throughput advantage shrinks at 1024 and is inconsistent at 4096. Neither generator saturates its assigned core. That does not make their arrival patterns equivalent.

This is an independent analysis of completed [run 38415480337](https://github.com/djosh34/tigerstore/actions/runs/38415480337), not a new measurement. No code changed for this report. The cap comparison in run 38418280627 is excluded.

## Sources and measurement

Raw files are under `/home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/results/client-controls`. I recomputed the results from each window's `load.json`, `before.json`, `after.json`, `audit.json` and command log, rather than taking the aggregate table as evidence. The supplied `windows.csv` provides the separate device-wide disk statistics.

The measured experiment source is `99cd39439b62ccfb80792f36f51d4a5e98c23797`. Generator dispatch controls originate in `ec1d5ac9a64e4bd2b699424face780aa266541b7`. Both tigerstore labels execute the same instrumented server binary. `tigerstore-static` is a symlink, not a server variant. The following SHA-256 hashes match across all three jobs:

```text
server:    eec2f3e8fda3b27dc1ded4d468c6bf141dda7a6f6b2944b7d635451196089dcf
generator: 0a9733c8758dca37ca465d7490b63111183d1b2adc8e46698bb3c81f2b11ea7b
etcd:      17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d
```

Each concurrency has its own free `ubuntu-24.04-arm` runner, with four Neoverse-N2 cores and 16.72 GB RAM. The leader uses CPU 0, followers CPUs 1 and 2, and generator plus external sampler CPU 3. Servers have `GOMAXPROCS=2`; the generator has one. Go processes inherit `GOMEMLIMIT=2GiB`. The jobs use Go 1.27.2 for tigerstore and loadgen, authenticated client TLS, encrypted peers, and ext4 with guest write barriers enabled. Etcd is the stock v3.7.2 release, built with Go 1.26.8, with an 8 GiB quota and safe persistence defaults. Profiling is off.

Every window uses a fresh three-node cluster, one leader endpoint, 16 warmed connections, 4096 Kubernetes-shaped keys and 1024-byte values. Seed and per-connection probes precede one second of warmup, which fully drains. Replica/persistence fences bracket the three-second arrival window and its drain. There are five repetitions per target. Actual command order is global, static, etcd on odd repetitions and etcd, static, global on even repetitions. The two tigerstore modes are adjacent, back to back on the same VM in each pair. Comparisons between concurrency jobs are not paired on one VM.

Global means `shape.dispatch=global-ticket-round-robin`. Static means `-dispatch connections`, reported as `static-connections`. Its per-connection limits are 16, 64 and 256 for global ceilings 256, 1024 and 4096. The server's default proposal cap remains 4 MiB in both modes.

## Independent validity checks

All 45 planned windows are present and valid, including 30 tigerstore windows. I checked each raw load and audit for zero errors, zero failed or unsent cohort requests, successful exit, and equal cohort, expected, success, latency-count and revision-delta values. All three replicas have the exact expected revision delta, with unchanged leader and term. Every window reports the requested observed peak concurrency and 16 connections.

Before/after PID and process-start identities match in all windows. All recorded server-thread affinities match their leader/follower placement. For tigerstore, issued persistence jobs equal completed jobs at both fenced boundaries. Every fenced file-operation histogram sums to its call count, and no new file write or fsync error appears. No repetition or tail outlier was dropped.

These checks validate workload accounting and the stated measurement boundaries. They do not replace a general linearizability or crash-durability campaign.

## Paired changes

All changes below are `100 * (static / global - 1)` within the same repetition. Cells give the median change and the minimum to maximum across all five pairs. These ranges are not confidence intervals. Negative CPU changes mean less CPU; negative latency changes mean lower latency. A ratio of independent medians is not the paired median.

| Concurrency | Throughput change % | Leader CPU/Put change % | Generator CPU/Put change % | All-server CPU/Put change % | p99 change % | p99.9 change % |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | +21.46 [18.02, 23.08] | -22.08 [-22.71, -20.38] | -54.60 [-55.23, -52.69] | -13.73 [-15.72, -12.76] | -10.36 [-17.25, -1.22] | -14.48 [-30.20, +214.09] |
| 1024 | +6.77 [-0.20, 10.30] | -16.46 [-19.08, -15.80] | -44.88 [-45.09, -43.77] | -11.79 [-14.59, -11.25] | -12.53 [-19.22, -2.46] | -10.20 [-38.27, +175.88] |
| 4096 | +2.03 [-5.02, 7.57] | -6.25 [-7.99, -4.24] | -25.40 [-26.15, -24.78] | -5.81 [-7.04, -3.84] | +6.04 [-6.14, 18.87] | +9.82 [-8.75, 33.21] |

Static is faster in all five 256-client pairs, four of five 1024-client pairs, and three of five 4096-client pairs. Leader CPU/Put is lower in all 15 pairs. Median paired absolute leader CPU savings are 3.89, 1.64 and 0.53 us/Put respectively. Follower CPU changes are smaller. Their paired medians for F1/F2 are -3.75/-3.80%, -6.10/-3.47%, and -5.46/-4.74%.

The lower CPU/Put persists under cohort accounting. Recomputing from externally fenced process ticks divided by the successful cohort gives paired median leader changes of -22.05%, -16.23% and -6.65%. Internal CPU through drain gives -22.05%, -16.23% and -6.20%. Cohort-through-drain throughput changes are +21.42%, +6.83% and +2.09%.

Static is not a general tail-latency winner. Its valid 256-client p99.9 values include 59.90 ms in repetition 3 and 33.95 ms in repetition 4. At 1024 clients, static repetition 3 reaches 66.72 ms versus global 24.18 ms. At 4096, both median paired p99 and p99.9 worsen.

## Rates and CPU levels

Each cell is a median of five per-window statistics. Brackets retain the full five-window minimum to maximum. Throughput counts completions in the three-second arrival window. CPU counts process user plus system time, not just the committer. All-server CPU is summed within each window before taking its median.

| Concurrency | Dispatch | Put/s | Leader us/Put | All-server us/Put | Generator us/Put | Leader cores | Generator cores |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | Global | 51,908 [51,161, 54,110] | 17.72 [17.13, 18.04] | 29.82 [28.65, 30.64] | 8.29 [8.09, 8.52] | 0.927 [0.906, 0.940] | 0.431 [0.423, 0.444] |
| 256 | Static | 63,023 [61,498, 65,725] | 13.81 [13.24, 14.36] | 25.63 [24.14, 26.45] | 3.82 [3.63, 3.96] | 0.870 [0.856, 0.896] | 0.239 [0.233, 0.248] |
| 1024 | Global | 87,041 [84,784, 87,724] | 9.99 [9.92, 10.42] | 16.30 [16.19, 16.97] | 4.55 [4.42, 4.63] | 0.870 [0.850, 0.907] | 0.390 [0.380, 0.403] |
| 1024 | Static | 92,575 [87,552, 96,719] | 8.34 [8.31, 8.43] | 14.37 [14.22, 14.49] | 2.51 [2.47, 2.54] | 0.770 [0.737, 0.807] | 0.229 [0.220, 0.241] |
| 4096 | Global | 80,174 [77,048, 83,040] | 8.41 [8.31, 8.49] | 13.52 [13.35, 13.57] | 3.17 [3.13, 3.19] | 0.667 [0.650, 0.690] | 0.251 [0.246, 0.262] |
| 4096 | Static | 82,088 [78,699, 83,756] | 7.88 [7.76, 7.96] | 12.74 [12.59, 12.83] | 2.36 [2.35, 2.37] | 0.643 [0.623, 0.660] | 0.193 [0.185, 0.198] |

The same-job etcd baseline uses global dispatch, not static. Its median Put/s and five-window ranges are 4929 [4689, 5044], 5353 [5133, 5405], and 4302 [4105, 4354]. Median leader CPU is 201.70, 186.38 and 230.92 us/Put. These are workload-specific baselines, not maximum-capacity claims or a static/global etcd comparison.

## Fsync groups and wall time

These values come from before/after file counters, after replica and persistence fencing. `Puts/fsync` is successful cohort operations divided by file-fsync calls. It is not requests per Raft entry. Segment-header file syncs are included; directory syncs are excluded. Mean fsync time is elapsed wall time, not CPU. The fenced interval for these tigerstore rows is about 3.18 to 3.21 seconds, wider than the generator's arrival/drain interval.

The final column is calculated per window as `(write wall sum + fsync wall sum) / fenced elapsed`. It describes time inside the leader's measured file operations. It is not device-wide utilization, a fraction of request latency, or a CPU fraction. It must not be added to CPU occupancy. Columns are independent medians, so summing the displayed median write and fsync values does not reproduce every derived median.

| Concurrency | Dispatch | Leader Puts/fsync | Fsync calls | Mean fsync ms | Fsync wall s | Write wall s | Write+fsync wall fraction |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | Global | 73.1 [68.3, 74.7] | 2134 [2059, 2294] | 0.928 [0.871, 1.041] | 2.055 [1.979, 2.143] | 0.465 [0.441, 0.554] | 0.795 [0.763, 0.805] |
| 256 | Static | 76.1 [75.4, 77.7] | 2466 [2447, 2594] | 0.850 [0.811, 0.890] | 2.104 [2.042, 2.194] | 0.438 [0.427, 0.479] | 0.808 [0.785, 0.827] |
| 1024 | Global | 289.1 [264.4, 294.5] | 914 [879, 966] | 1.978 [1.907, 2.207] | 1.842 [1.700, 1.940] | 0.421 [0.408, 0.500] | 0.702 [0.687, 0.740] |
| 1024 | Static | 305.3 [301.1, 318.8] | 908 [827, 967] | 2.181 [1.951, 2.567] | 1.981 [1.886, 2.122] | 0.339 [0.319, 0.444] | 0.744 [0.723, 0.767] |
| 4096 | Global | 1205.0 [1168.8, 1206.4] | 204 [195, 214] | 10.580 [9.853, 10.798] | 2.145 [2.106, 2.222] | 0.276 [0.237, 0.301] | 0.756 [0.743, 0.775] |
| 4096 | Static | 1226.9 [1180.9, 1257.5] | 205 [191, 212] | 10.711 [10.136, 11.404] | 2.163 [2.117, 2.207] | 0.266 [0.246, 0.325] | 0.761 [0.757, 0.773] |

| Concurrency | Paired Puts/fsync change % | Paired fsync-call change % | Paired mean-fsync-wall change % | Paired write+fsync-wall-fraction change % |
| ---: | ---: | ---: | ---: | ---: |
| 256 | +6.03 [0.96, 10.44] | +14.67 [6.84, 20.06] | -10.61 [-16.21, -4.30] | +2.50 [-1.27, 3.12] |
| 1024 | +5.47 [3.77, 15.49] | -0.46 [-9.52, 4.54] | +13.60 [-1.40, 27.25] | +4.28 [0.54, 8.55] |
| 4096 | +1.70 [0.31, 6.47] | -4.21 [-5.91, 5.64] | +2.74 [-3.61, 5.65] | +0.03 [-2.32, 3.98] |

At 256 clients, static does more Puts, more fsync calls and modestly more Puts per fsync, with shorter mean fsync wall time. At 1024 it has similar fsync count, somewhat larger groups and longer mean fsync wall time. At 4096 the leader spends similar total wall time in file I/O in both modes. Lower measured CPU cost does not produce a consistent throughput advantage there.

Follower CPU and fsync statistics show the smaller CPU change and different physical grouping. These cells also give medians and full five-window ranges. Group direction is not uniform across replicas at the highest concurrency.

| Concurrency | Dispatch | F1 us/Put | F2 us/Put | F1 Puts/fsync | F2 Puts/fsync | F1 fsync wall s | F2 fsync wall s |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | Global | 6.23 [6.04, 6.59] | 5.84 [5.48, 6.01] | 66.7 [65.1, 68.1] | 66.9 [65.6, 68.7] | .976 [.956, 1.264] | 1.001 [.898, 1.202] |
| 256 | Static | 6.13 [5.63, 6.34] | 5.70 [5.27, 5.79] | 70.2 [69.4, 70.3] | 70.3 [69.0, 70.9] | 1.263 [1.211, 1.396] | 1.260 [1.048, 1.404] |
| 1024 | Global | 3.23 [3.19, 3.33] | 3.12 [3.07, 3.22] | 262.8 [260.6, 267.1] | 276.1 [263.5, 276.5] | 1.219 [1.011, 1.299] | 1.288 [1.166, 1.345] |
| 1024 | Static | 3.05 [3.00, 3.05] | 3.01 [2.88, 3.03] | 272.2 [270.9, 275.5] | 283.9 [278.6, 286.1] | 1.355 [1.295, 1.556] | 1.458 [1.362, 1.627] |
| 4096 | Global | 2.55 [2.53, 2.56] | 2.53 [2.49, 2.56] | 1058.9 [1023.3, 1098.1] | 1103.0 [1064.4, 1223.3] | 1.987 [1.951, 2.113] | 1.890 [1.795, 1.975] |
| 4096 | Static | 2.41 [2.39, 2.45] | 2.41 [2.40, 2.47] | 1038.1 [1009.2, 1057.5] | 1088.5 [1046.5, 1170.1] | 2.054 [1.990, 2.128] | 1.869 [1.829, 1.979] |

As a separate logical-batch check, cohort Puts divided by the fenced Raft-index increment give median Puts/entry of 116.0 to 119.1, 423.4 to 447.4, and 1854.6 to 1876.5 for global to static. Paired median changes are +2.74%, +5.36% and +1.05%. These are not the physical fsync groups above and do not explain the full CPU reduction by themselves.

Device-wide `sda` write throughput medians are 191.7 to 234.5 MiB/s, 272.7 to 290.7 MiB/s, and 242.8 to 251.1 MiB/s. Its busy fractions are 0.351 to 0.414, 0.403 to 0.458, and 0.584 to 0.584. Those counters include all replicas and other runner activity on the shared virtual disk. Neither those values nor the file-operation fractions prove a hardware throughput ceiling or physical power-loss protection.

## All five repetitions

Each paired cell is global / static. This retains the individual observations behind the ranges. The wall fraction uses the wider fenced interval defined above.

| Concurrency | Rep | Put/s | Leader us/Put | Generator cores | Leader Puts/fsync | Leader fsync wall s | Leader write wall s | Leader file-I/O wall fraction | p99.9 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 1 | 54,110 / 65,725 | 17.13 / 13.24 | .438 / .239 | 71.8 / 76.1 | 2.055 / 2.104 | .467 / .479 | .788 / .808 | 16.74 / 14.32 |
| 256 | 2 | 51,204 / 63,023 | 17.71 / 13.70 | .423 / .233 | 73.7 / 76.8 | 2.123 / 2.194 | .465 / .438 | .805 / .827 | 17.76 / 14.58 |
| 256 | 3 | 51,161 / 62,033 | 17.72 / 13.81 | .431 / .237 | 74.7 / 75.4 | 2.143 / 2.155 | .441 / .427 | .805 / .810 | 19.07 / 59.90 |
| 256 | 4 | 52,106 / 61,498 | 18.04 / 14.36 | .444 / .244 | 68.3 / 75.4 | 1.997 / 2.042 | .448 / .479 | .763 / .787 | 16.97 / 33.95 |
| 256 | 5 | 51,908 / 63,315 | 17.98 / 14.16 | .430 / .248 | 73.1 / 77.7 | 1.979 / 2.081 | .554 / .434 | .795 / .785 | 19.20 / 13.40 |
| 1024 | 1 | 85,764 / 88,624 | 9.95 / 8.31 | .390 / .222 | 293.9 / 305.0 | 1.940 / 2.055 | .421 / .319 | .740 / .744 | 25.76 / 23.92 |
| 1024 | 2 | 84,784 / 92,575 | 10.03 / 8.32 | .380 / .229 | 264.4 / 305.3 | 1.842 / 1.978 | .408 / .339 | .702 / .723 | 33.23 / 25.62 |
| 1024 | 3 | 87,724 / 87,552 | 9.99 / 8.41 | .399 / .220 | 289.1 / 318.8 | 1.843 / 2.122 | .442 / .336 | .713 / .767 | 24.18 / 66.72 |
| 1024 | 4 | 87,041 / 92,934 | 10.42 / 8.43 | .403 / .236 | 294.5 / 308.2 | 1.700 / 1.981 | .500 / .409 | .687 / .745 | 98.30 / 60.69 |
| 1024 | 5 | 87,687 / 96,719 | 9.92 / 8.34 | .388 / .241 | 285.5 / 301.1 | 1.830 / 1.886 | .408 / .444 | .697 / .727 | 29.56 / 26.54 |
| 4096 | 1 | 77,048 / 82,879 | 8.44 / 7.76 | .246 / .195 | 1206.4 / 1226.9 | 2.106 / 2.207 | .276 / .266 | .743 / .773 | 82.58 / 79.43 |
| 4096 | 2 | 80,174 / 78,699 | 8.32 / 7.96 | .251 / .185 | 1205.0 / 1257.5 | 2.191 / 2.178 | .291 / .246 | .775 / .757 | 74.71 / 82.05 |
| 4096 | 3 | 82,092 / 83,756 | 8.41 / 7.88 | .260 / .198 | 1170.0 / 1245.7 | 2.109 / 2.117 | .301 / .325 | .752 / .761 | 72.61 / 96.73 |
| 4096 | 4 | 83,040 / 78,870 | 8.31 / 7.90 | .262 / .186 | 1205.8 / 1209.6 | 2.222 / 2.163 | .237 / .277 | .771 / .762 | 73.66 / 84.15 |
| 4096 | 5 | 78,113 / 82,088 | 8.49 / 7.84 | .249 / .193 | 1168.8 / 1180.9 | 2.145 / 2.149 | .271 / .266 | .756 / .757 | 89.92 / 82.05 |

## Interpretation and the earlier 63k result

The global dispatcher was introduced to enforce an exact shared concurrency ceiling and strict request-level round robin across multiple endpoints. Equal connection counts alone did not keep actual requests balanced when endpoints completed at different speeds. It selects endpoint by global request ticket, rotates available connections within that endpoint, accumulates frames per connection and flushes them from one writer. It admits at most 256 requests in a dispatch turn. Round robin applies to admission tickets. Batched socket flushes do not alternate individual wire sends across endpoints.

Static workers instead have fixed per-connection quotas and independent response-driven refill/flush loops. With `GOMAXPROCS=1` they still interleave on one CPU, but their socket batches and feedback timing are not the global dispatcher's batches. Both use the lean wire transport and prebuilt requests. At one leader endpoint, cross-endpoint round robin is trivial, yet the new admission and connection scheduling choices still change the input pattern. Equal ceilings and observed peaks do not establish equal per-connection outstanding counts or equal time-averaged occupancy.

The controlled comparison establishes that this client configuration changes measured throughput and server cost with byte-identical servers. It does not isolate whether batch size, socket writes, intake timing, GC, fsync interaction or another mechanism accounts for the difference. The modest physical-group changes are evidence of a different workload execution, not a complete explanation of a 22% leader-CPU reduction. There is no request-arrival trace or matched CPU profile in these windows.

Generator occupancy stays below 0.45 core globally and below 0.25 core statically. A generator need not saturate its CPU to change server cost. Refilling and flushing at different times can change how the server amortizes transport, Raft and disk work. CPU utilization alone cannot certify identical arrivals, sufficient batching, or absence of generator scheduling effects.

Warmup is time-based, not a fixed operation count. It keeps the same live-key count but leaves different revision/history depths. For example, 1024-client repetition 1 begins at revision 23,761 globally and 38,474 statically, while later repetitions begin near 93k to 98k. The one-second warmup is not proof of an identical steady state. Throughput-dependent log/history growth within the measured three seconds also differs. The paired comparison is of the complete client configurations, not an isolated burst-timing intervention with identical retained histories.

The earlier [#74 lean-generator result](https://github.com/djosh34/tigerstore/issues/74) reported 63,361 Put/s and 12.167 leader us/Put at 256 clients. It used a different lean generator and a finite 60,000-Put cohort, with different warmup, keys, request-value generation and observation boundaries. Its production server source `aa9d9b6eea79c73f12fe0153aefa809841967e88` has no production server diff against the `e5c3327e` base used here; the experiment instrumentation and workload protocol still differ. This run's static control again reaches about 63k/s. The new global result near 52k/s therefore does not contradict the older observation or prove a server regression. The older 12.167 us and this control's 13.81 us are not a paired CPU comparison.

Keep the global dispatcher for strict balanced-ingress measurements and label the static leader-only control separately. Treat connection count, dispatcher, concurrency, warmup, measurement boundaries and sampler as part of the workload specification. Do not use these leader-only controls as evidence for balanced follower ingress, regional delay, an open-loop latency SLO, or a general server performance gain.

## Reproduction of the arithmetic

For each concurrency and repetition, pair `tigerstore-N` with `tigerstore-static-N`. Recompute these quantities directly from the raw JSON:

```text
window throughput = load.ops / load.duration_s
leader CPU/Put = load.server_cpu_us_per_op_by_node[0]
all-server CPU/Put = sum(load.server_cpu_us_per_op_by_node)
fenced interval = after.monotonic - before.monotonic
fsync calls = after.nodes[0].files.fsync.calls - before.nodes[0].files.fsync.calls
Puts/fsync = load.cohort / fsync calls
fsync wall seconds = delta(files.fsync.wall_ns_sum) / 1e9
write wall seconds = delta(files.write.wall_ns_sum) / 1e9
mean fsync milliseconds = fsync wall seconds * 1000 / fsync calls
file-I/O wall fraction = (fsync wall seconds + write wall seconds) / fenced interval
fenced CPU us/Put = delta(proc[0].utime + proc[0].stime) * 10000 / load.cohort
paired change percent = 100 * (static value / global value - 1)
```

Take the median and full minimum to maximum across the five per-window values or five paired changes, as labelled. CPU tick arithmetic uses Linux process accounting's `USER_HZ=100`, matching `cmd/loadgen/cpu.go`. No Go tests, build, local benchmark or runner launch was needed for this analysis.
