# Cap confirmation, with the old open-loop results quarantined

Replacement update, 2026-10-13: the corrected campaign is complete at source `223f8f6aad7923ff186ac77a2c9fc29b9f140c86`. See [fixed-arrival-validation.md](fixed-arrival-validation.md) for the new all-five cap comparisons and runner regression checks. The old open numbers below remain quarantined. Static results are unchanged. The new 95k Cap comparison contains a valid severe tail/drain outlier and does not justify a uniform gain or SLO claim.

The static 4096-client control supports a smaller, workload-specific throughput gain from the 1 MiB proposal cap. The paired full-cohort median gain is **5.47%, with all-five range 5.36% to 13.45%**. The inactive-cap 256-client control is slower, with paired median -3.71%. These results do not establish a universal 20% gain or a general tail-latency improvement.

The old fixed-rate results below are historical observations, not valid arrival-shape confirmation. A generator defect can defer an already-due arrival to the 100 ms observation timer after flushing. Successful cohort accounting does not detect that error. The parent reran run 38421656211 and the other affected open-loop experiments with the fix while retaining the old data. Do not use the old apparent 92.5k versus 98k plateau as an established capacity improvement.

The fix is generator-only commit `afc56a18ff02944951c6853cb1fdecd4f14ebb35`, based on `a0d88e2e`. It retains the admission stop reason across flushing and does not busy-spin on blocked connection credit or capacity. This particular defect does not affect closed-loop dispatch, including the static controls.

## Evidence and scope

I independently recomputed the statistics from the raw plans, command logs, `load.json`, `audit.json`, `before.json` and `after.json` under:

- `/home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/results/static-caps`, run 38421648705.
- `/home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/results/cap-open`, run 38421656211.

All three jobs record source `4ee913fbb7f799070c36af37de7e9aafbd696d49`. Main has the default 4 MiB cap; Cap is the 1 MiB overlay. The recorded production-source diff changes only `DefaultConfig.MaxBatchBytes`, from `4 << 20` to `1 << 20`. I independently reproduced and checked the modified `internal/core/node.go` SHA-256 against each job's variant manifest. The builder archives that same source, adds test-only witnesses, changes the constant and builds the candidate.

The binary hashes match across the three jobs, but Main and Cap are different executables:

```text
Main:      e35171d23afbd21032dc5f14d73e9faaba69df1c7712b04301c470b41a653130
Cap:       3c79e251d6b6c33304adcda040fbf146e581a98bd1176a725d490dc2fc555448
Generator: d8b496f2e4da538787930d6ffb143b9fe0aa473568c95a67441071d7784f724e
```

Static aliases are symlinks to those binaries. The archived candidate and repository-built Main also have different build paths and VCS metadata. This is a narrow source overlay, not a byte-identical binary placebo experiment. Cap256k was built and tested but not measured in these plans.

The runner logs show Go 1.27.2, four Neoverse-N2 CPUs, about 16.72 GB RAM, and an ext4 virtual disk with guest barriers enabled. The leader uses CPU 0, followers CPUs 1 and 2, and generator plus sampler CPU 3. Servers have `GOMAXPROCS=2`, generator one, and Go processes inherit `GOMEMLIMIT=2GiB`. Client TLS and peer encryption remain enabled. Profiling is off. These are separate Actions jobs, so cross-job comparisons are not matched VM trials.

Every window uses a fresh three-node cluster, leader-only ingress, 4096 keys, 1024-byte Puts, one second of warmup, three seconds of measured arrivals and a 30-second drain limit. Static uses 16 connections with independent fixed worker quotas, 16 streams each at C256 and 256 each at C4096. Open uses the global dispatcher, 18 connections, concurrency ceiling 4096 and fixed offered rates 95k, 110k and 125k/s. Same-number repetitions use the same seed. Main and Cap are adjacent on one job VM; their order reverses on even repetitions. Open rates always run in ascending order within each repetition, so rate and elapsed job time are not randomized separately.

## Accounting audit

All 75 planned windows are present: 50 Main/Cap and 25 etcd. I checked successful exits, load/audit validity flags, zero request/sampling/invariant errors, zero failed or finally unsent requests, and exact agreement among cohort, success, expected, latency counts and replica revision deltas. Leader and term do not change. Fixed-rate cohorts have exactly three seconds of scheduled operations, 285,000, 330,000 or 375,000 for Main/Cap. Queue plus inflight at arrival stop equals cohort minus arrival-window completions. Final queue and inflight are zero.

Before/after process IDs and start identities match. All recorded server-thread affinities match the intended placement. In all 50 tigerstore windows, persistence jobs are completed at both fences, every file-operation histogram sums to its call count, and there are no new write or fsync errors. All repetitions and outliers remain in the tables.

These accounting checks pass even for the buggy open-loop generator. They validate completion and fenced counter arithmetic, not correct arrival timing.

## Paired rates

Cells give medians and the full minimum to maximum across five windows. Rates are thousands of Puts/s. Cohort rate is successful scheduled operations divided by arrival plus drain time. Window rate counts completions in the three-second arrival window. Paired changes are calculated per repetition as `100 * (Cap / Main - 1)`, then summarized, not calculated from aggregate medians. Ranges are not confidence intervals.

| Input shape | Main cohort k/s | Cap cohort k/s | Paired cohort change % | Paired window change % |
| --- | ---: | ---: | ---: | ---: |
| Static C256 | 62.905 [59.056, 66.402] | 59.175 [56.954, 63.449] | -3.71 [-5.93, -0.02] | -3.58 [-5.91, -0.01] |
| Static C4096 | 96.255 [88.837, 98.213] | 101.622 [100.782, 103.488] | +5.47 [5.36, 13.45] | +5.39 [5.24, 13.10] |
| Old open 95k | 92.735 [87.518, 93.488] | 94.345 [93.754, 94.593] | +2.00 [0.93, 7.13] | +1.91 [1.25, 7.83] |
| Old open 110k | 92.861 [88.833, 96.169] | 97.969 [96.627, 99.879] | +5.84 [3.86, 9.73] | +8.01 [3.25, 10.50] |
| Old open 125k | 92.531 [89.509, 95.479] | 98.332 [96.782, 99.724] | +7.71 [2.68, 10.78] | +9.03 [1.18, 11.61] |

At C4096, all five static pairs favor Cap. The first Main window is notably slower and produces the largest gain. The other four full-cohort gains are 5.36% to 5.58%; the first remains in the reported range. At C256, all five pairs favor Main, although repetition 2 is nearly equal.

## Tails, backlog and drain

Open latency measures intended arrival to completion, including generator backlog. The separately named service latency starts at admission, not at server receipt. It is not a server-only measurement. Static has no scheduled-arrival queue. `Qstop` is scheduled but not admitted at three seconds; it excludes admitted inflight requests. `Qmax` is the maximum observed backlog, not a continuously sampled exact maximum.

| Shape | Binary | p99 ms | p99.9 ms | Qstop requests | Qmax requests | Drain s |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| C256 | Main | 8.62 [7.85, 9.40] | 12.88 [12.68, 58.33] | 0 | 0 | .002 [.002, .003] |
| C256 | Cap | 8.49 [7.75, 8.88] | 15.04 [13.07, 126.09] | 0 | 0 | .004 [.002, .007] |
| C4096 | Main | 64.88 [62.52, 70.52] | 76.81 [68.94, 87.82] | 0 | 0 | .031 [.021, .032] |
| C4096 | Cap | 69.47 [60.95, 74.71] | 81.00 [72.61, 98.30] | 0 | 0 | .025 [.017, .027] |
| Old 95k | Main | 107.74 [85.20, 282.07] | 117.70 [96.21, 292.55] | 3581 [1785, 21353] | 6785 [4738, 22644] | .073 [.049, .256] |
| Old 95k | Cap | 67.37 [62.00, 96.73] | 85.20 [74.19, 108.79] | 548 [217, 1034] | 3222 [2178, 5671] | .021 [.013, .040] |
| Old 110k | Main | 564.13 [443.55, 731.91] | 576.72 [466.62, 740.29] | 52731 [39056, 61881] | 53578 [40196, 62630] | .554 [.431, .715] |
| Old 110k | Cap | 382.73 [319.82, 426.77] | 391.12 [332.40, 435.16] | 30964 [29722, 38485] | 31914 [29722, 40060] | .368 [.304, .415] |
| Old 125k | Main | 1054.87 [933.23, 1195.38] | 1067.45 [954.20, 1212.15] | 98646 [85549, 106415] | 98981 [86499, 107453] | 1.053 [.928, 1.190] |
| Old 125k | Cap | 819.99 [769.65, 878.71] | 832.57 [773.85, 887.10] | 76841 [74063, 82183] | 77047 [74063, 82183] | .814 [.760, .875] |

| Shape | Paired p99 change % | Paired p99.9 change % | Paired Qstop change % |
| --- | ---: | ---: | ---: |
| C256 | -3.02 [-8.69, +13.15] | +9.16 [-74.21, +884.14] | Both zero |
| C4096 | -0.74 [-12.92, +19.50] | +5.37 [-9.46, +38.38] | Both zero |
| Old 95k | -42.46 [-76.26, -17.85] | -36.97 [-72.31, -11.44] | -90.07 [-95.79, -84.37] |
| Old 110k | -34.39 [-45.41, -24.35] | -33.15 [-44.90, -24.55] | -41.51 [-45.60, -19.62] |
| Old 125k | -26.49 [-34.70, -11.24] | -26.72 [-34.81, -12.31] | -24.31 [-28.91, -3.93] |

Static C4096 does not show a general tail win. Cap p99.9 is worse in three of five pairs. C256 has large valid tail outliers in both binaries. Old open Cap has lower measured backlog and tails in every pair, but the defective scheduler is part of that comparison. Those improvements cannot establish a latency SLO or the gain under corrected arrivals. Even the old data show substantial growth and drain at 110k and 125k in both variants. Small nonzero backlog at 95k alone does not establish instability; it also does not establish long-run headroom.

## CPU

CPU below is internal through-drain user plus system CPU divided by the complete successful cohort. It is not an instruction count. Generator cores use the arrival-window CPU sampler. Each bracket is the full five-window range.

| Shape | Binary | Leader us/Put | F1 us/Put | F2 us/Put | Generator cores |
| --- | --- | ---: | ---: | ---: | ---: |
| C256 | Main | 12.219 [12.074, 12.486] | 5.569 [5.508, 5.638] | 4.925 [4.899, 5.046] | .214 [.202, .224] |
| C256 | Cap | 12.381 [12.146, 12.574] | 5.639 [5.489, 5.740] | 4.977 [4.905, 5.075] | .201 [.192, .214] |
| C4096 | Main | 7.590 [7.340, 7.758] | 2.351 [2.322, 2.404] | 2.335 [2.253, 2.413] | .226 [.212, .230] |
| C4096 | Cap | 7.704 [7.601, 7.859] | 2.448 [2.427, 2.505] | 2.431 [2.331, 2.532] | .239 [.235, .241] |
| Old 95k | Main | 8.667 [8.456, 9.088] | 2.596 [2.526, 2.772] | 2.526 [2.491, 2.632] | .337 [.298, .359] |
| Old 95k | Cap | 9.333 [9.228, 9.474] | 2.877 [2.772, 2.947] | 2.772 [2.737, 2.807] | .382 [.365, .384] |
| Old 110k | Main | 8.455 [8.364, 8.485] | 2.545 [2.485, 2.576] | 2.485 [2.455, 2.515] | .314 [.300, .325] |
| Old 110k | Cap | 8.818 [8.606, 9.000] | 2.667 [2.576, 2.727] | 2.606 [2.515, 2.606] | .347 [.338, .356] |
| Old 125k | Main | 8.427 [8.373, 8.533] | 2.533 [2.480, 2.560] | 2.453 [2.400, 2.507] | .311 [.301, .323] |
| Old 125k | Cap | 8.667 [8.640, 8.693] | 2.587 [2.533, 2.587] | 2.533 [2.507, 2.533] | .344 [.344, .351] |

Paired leader through-drain CPU changes are +0.75% [-0.59, 2.54] at C256 and +1.43% [1.02, 4.66] at C4096. For old 95k/110k/125k they are +6.91% [3.09, 12.03], +4.30% [2.90, 6.07], and +2.53% [1.57, 3.50]. Externally fenced process-tick/cohort accounting corroborates the direction at C4096 and every open pair, with paired medians +1.24%, +6.91%, +4.29% and +2.53%. This is not a CPU/Put reduction.

The C4096 arrival-window leader occupancy rises from median .733 to .790 core. Generator occupancy never reaches .39 core. Non-saturation does not certify correct arrivals or equal socket batching, as the discovered scheduling defect demonstrates.

## Logical entries, physical fsync groups and wall time

File counters cover the wider replica/persistence-fenced interval. `Puts/fsync` is cohort divided by completed file-fsync calls, not requests per Raft entry. Header file syncs count; directory syncs do not. Mean fsync is elapsed wall time per call. The I/O fraction is `(write wall + fsync wall) / fenced elapsed`, calculated for each window. It is neither device utilization nor CPU occupancy. It cannot be added to CPU occupancy.

| Shape | Binary | Leader Puts/fsync | Mean fsync ms | Fsync wall s | Write wall s | Leader I/O fraction |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| C256 | Main | 77.4 [76.9, 78.3] | .914 [.812, 1.011] | 2.267 [2.107, 2.288] | .414 [.367, .494] | .826 [.813, .838] |
| C256 | Cap | 77.3 [75.7, 78.1] | .983 [.884, 1.063] | 2.288 [2.174, 2.331] | .407 [.377, .464] | .841 [.826, .851] |
| C4096 | Main | 1167.5 [1093.3, 1235.9] | 7.360 [7.183, 8.904] | 1.911 [1.860, 1.977] | .336 [.281, .340] | .704 [.684, .721] |
| C4096 | Cap | 1397.2 [1343.9, 1458.8] | 11.337 [10.560, 11.745] | 2.466 [2.396, 2.494] | .280 [.230, .345] | .858 [.851, .874] |
| Old 95k | Main | 756.0 [539.8, 1079.5] | 5.048 [3.316, 7.263] | 1.874 [1.751, 1.917] | .357 [.303, .390] | .667 [.651, .670] |
| Old 95k | Cap | 547.0 [479.8, 705.4] | 3.630 [3.123, 5.194] | 1.885 [1.855, 2.098] | .431 [.361, .474] | .727 [.714, .768] |
| Old 110k | Main | 1051.0 [1028.0, 1082.0] | 6.609 [6.095, 7.374] | 2.118 [1.908, 2.360] | .381 [.358, .491] | .659 [.638, .703] |
| Old 110k | Cap | 1114.9 [1111.1, 1162.0] | 8.475 [7.783, 8.786] | 2.448 [2.312, 2.522] | .372 [.359, .448] | .794 [.769, .811] |
| Old 125k | Main | 1116.1 [1077.6, 1129.5] | 6.656 [6.252, 7.673] | 2.316 [2.151, 2.548] | .525 [.442, .569] | .678 [.651, .686] |
| Old 125k | Cap | 1221.5 [1198.1, 1293.1] | 9.143 [8.823, 9.866] | 2.762 [2.718, 2.861] | .497 [.419, .509] | .812 [.800, .829] |

| Shape | Paired Puts/fsync change % | Paired mean-fsync-wall change % | Paired I/O-fraction change % |
| --- | ---: | ---: | ---: |
| C256 | -0.09 [-2.81, +0.95] | +7.67 [-2.73, +8.88] | +1.51 [0.25, 1.79] |
| C4096 | +17.75 [14.96, 28.75] | +44.81 [28.47, 59.57] | +21.62 [19.09, 27.68] |
| Old 95k | -27.23 [-55.56, -10.51] | -18.54 [-57.00, -3.67] | +9.19 [8.01, 17.80] |
| Old 110k | +7.39 [5.39, 11.50] | +28.23 [15.40, 34.68] | +21.59 [13.48, 24.21] |
| Old 125k | +13.36 [6.39, 16.61] | +34.70 [22.27, 49.40] | +20.77 [19.17, 24.73] |

At static C4096, median physical fsync calls fall from 255 [222, 266] to 218 [210, 233]. Median cohort Puts per fenced Raft-index increment falls from 1691.6 [1606.7, 1819.3] to 905.1 [889.6, 920.9]. Smaller logical entries therefore coexist with larger physical fsync groups. Multiple entries can share a persistence job. Do not equate a proposal entry with a Sync call or claim the cap makes each fsync faster. Mean fsync and total fsync wall are higher in every C4096 pair.

Follower physical grouping also increases at C4096. F1 median/range changes from 1009.2 [973.9, 1056.4] to 1202.0 [1142.6, 1265.9]; F2 from 1052.0 [1021.7, 1113.2] to 1220.5 [1181.6, 1275.4]. Their I/O fractions rise from .532 [.504, .547] and .563 [.518, .582] to .788 [.781, .812] and .796 [.775, .807]. These observations do not prove a disk hardware ceiling or isolate which pipeline overlap causes the gain.

## Every paired repetition

Rates here are full-cohort k/s; every slash is Main / Cap. These rows retain all 25 pairs, including the quarantined open observations.

| Shape | Rep | Cohort k/s | Gain % | p99 ms | p99.9 ms | Qstop |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| C256 | 1 | 59.149 / 56.954 | -3.71 | 8.62 / 8.49 | 12.81 / 126.09 | 0 / 0 |
| C256 | 2 | 59.056 / 59.045 | -0.02 | 9.40 / 8.75 | 58.33 / 15.04 | 0 / 0 |
| C256 | 3 | 62.905 / 59.175 | -5.93 | 8.68 / 8.42 | 12.88 / 14.06 | 0 / 0 |
| C256 | 4 | 64.021 / 62.883 | -1.78 | 7.85 / 8.88 | 12.68 / 13.07 | 0 / 0 |
| C256 | 5 | 66.402 / 63.449 | -4.45 | 8.49 / 7.75 | 13.01 / 59.11 | 0 / 0 |
| C4096 | 1 | 88.837 / 100.782 | +13.45 | 69.99 / 60.95 | 83.10 / 75.24 | 0 / 0 |
| C4096 | 2 | 96.255 / 101.622 | +5.58 | 64.88 / 69.47 | 68.94 / 81.00 | 0 / 0 |
| C4096 | 3 | 98.213 / 103.488 | +5.37 | 63.57 / 62.26 | 76.81 / 72.61 | 0 / 0 |
| C4096 | 4 | 96.060 / 101.208 | +5.36 | 70.52 / 69.99 | 87.82 / 92.54 | 0 / 0 |
| C4096 | 5 | 96.849 / 102.149 | +5.47 | 62.52 / 74.71 | 71.04 / 98.30 | 0 / 0 |
| Old 95k | 1 | 87.518 / 93.754 | +7.13 | 282.07 / 66.98 | 292.55 / 81.00 | 21353 / 1034 |
| Old 95k | 2 | 92.735 / 94.593 | +2.00 | 107.74 / 62.00 | 117.70 / 74.19 | 3581 / 548 |
| Old 95k | 3 | 89.596 / 94.342 | +5.30 | 210.24 / 96.73 | 221.77 / 108.79 | 13796 / 581 |
| Old 95k | 4 | 93.321 / 94.345 | +1.10 | 88.87 / 67.37 | 97.26 / 85.72 | 1785 / 279 |
| Old 95k | 5 | 93.488 / 94.357 | +0.93 | 85.20 / 69.99 | 96.21 / 85.20 | 2185 / 217 |
| Old 110k | 1 | 88.833 / 97.476 | +9.73 | 731.91 / 399.51 | 740.29 / 407.90 | 61881 / 34149 |
| Old 110k | 2 | 91.434 / 97.969 | +7.15 | 618.66 / 382.73 | 627.05 / 391.12 | 56915 / 30964 |
| Old 110k | 3 | 96.169 / 99.879 | +3.86 | 443.55 / 319.82 | 466.62 / 332.40 | 39056 / 29722 |
| Old 110k | 4 | 92.861 / 98.287 | +5.84 | 564.13 / 370.15 | 572.52 / 382.73 | 52731 / 30842 |
| Old 110k | 5 | 92.943 / 96.627 | +3.96 | 564.13 / 426.77 | 576.72 / 435.16 | 47878 / 38485 |
| Old 125k | 1 | 89.509 / 96.782 | +8.13 | 1195.38 / 878.71 | 1212.15 / 887.10 | 106415 / 80541 |
| Old 125k | 2 | 90.020 / 99.724 | +10.78 | 1178.60 / 769.65 | 1186.99 / 773.85 | 106193 / 75496 |
| Old 125k | 3 | 95.479 / 98.037 | +2.68 | 933.23 / 828.38 | 954.20 / 836.76 | 85549 / 82183 |
| Old 125k | 4 | 92.531 / 99.665 | +7.71 | 1054.87 / 769.65 | 1067.45 / 782.24 | 98646 / 74063 |
| Old 125k | 5 | 95.256 / 98.332 | +3.23 | 941.62 / 819.99 | 958.40 / 832.57 | 88864 / 76841 |

## Qualifications and safety evidence

At C256, even all 256 outstanding fixed-size requests occupy well below 1 MiB of proposal packing budget. The candidate cap should not trigger. The negative throughput difference cannot be explained as a benefit from splitting those batches. Both C256 series rise across repetitions. Alternating adjacent pairs reduces drift but does not remove VM timing, shared-disk effects, different binary layout/build metadata or warmup-state differences. The report does not identify which of these explains the inactive-control difference.

Static warmup is time-based closed loop. It drains but leaves different operation/history counts. Before-revision medians and ranges are Main 67,168 [63,186, 68,832] versus Cap 64,384 [61,808, 66,432] at C256; Main 101,969 [94,147, 106,253] versus Cap 108,285 [102,924, 113,561] at C4096. Equal live-key count does not imply identical retained history or runtime state.

Open warmup is different: it preserves the configured fixed rate for one second and drains the complete scheduled warmup cohort. Both binaries start at exactly the same revision in every repetition at each rate, 99,116, 114,116 and 129,116. Do not claim different open warmup write counts. Logical entry packing, elapsed warmup/drain time, runtime state and actual arrival timing still differ. The scheduler defect also affects this warmup path.

The original roughly 20% global closed-loop result, this static result and the old open result use different input shapes and job VMs. Static quotas and global ticket admission produce different refill/flush timing. The source comparison from `99cd3943` to `4ee913fb` changes experiment files, not production server or generator code, yet the Main baseline also changes across jobs. Those observations prevent attributing cross-run baseline shifts or all observed gains to the cap.

Candidate test logs in all three jobs report success for `internal/core`, `internal/node`, `internal/server`, `internal/lint` and `internal/sim`. The build script runs `go test -short -parallel 2 -timeout 3m` on those packages. The included witnesses test typed and wire-form requests at the accepted 1.5 MiB envelope boundary, intact encoding beyond the soft batch cap, no success before Sync/Persisted, cold durable replay, and proposing another capped batch before the first append completes. The separate request-admission limit is unchanged. The cap is a packing target, not a new per-request rejection limit.

This is short-suite simulation and boundary evidence, not a full simulation campaign, a real-disk three-replica crash proof, or evidence for balanced ingress, larger values, LIST, regional delay or a production SLO. Retain the static result as qualified evidence, quarantine the old open measurements, and publish no universal 20% claim. The corrected parent reruns are documented separately in [fixed-arrival-validation.md](fixed-arrival-validation.md). They do not retroactively validate these old fixed-rate observations.

## Arithmetic

For each shape/rate and repetition, pair Main with Cap. Calculate cohort rate as `load.cohort / shape.through_drain_s`, window rate as `load.ops / load.duration_s`, CPU from `shape.cpu_through_drain`, and file statistics from fenced counter differences. Linux process-tick verification uses `USER_HZ=100`. Divide cohort by fsync calls for physical grouping and by fenced Raft-index increment for logical grouping. Calculate every percentage within a pair before taking the median/range. No local benchmark, new runner or public post was needed for this audit.
