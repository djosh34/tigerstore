# Fixed-arrival runner validation and replacement cap results

The replacement low-rate Range windows no longer show the old 100 ms admission-timer fallthrough. All five leader Range 5k/s p99 values are 1.503 to 1.544 ms, with maximum observed backlog 7 to 11 requests rather than 417 to 501. Balanced Range and LIST checks agree with that change.

The corrected cap comparison supports a qualified roughly 5% full-cohort gain at offered 110k and 125k/s. All five pairs favor Cap at those rates. It does not establish a universal 20% gain, sustained offered-rate capacity or a latency SLO. At 95k, Cap repetition 4 is valid but has p99 7.365 seconds and 7.528 seconds of drain. This report retains it.

These are existing runner results, not new measurements. Old open results remain quarantined in [cap-confirmation.md](cap-confirmation.md). Old versus replacement jobs use fresh VM allocations and different generator/server build artifacts. This is regression validation, not a causal estimate of the performance improvement from the fix.

## Exact source and data

All 28 replacement runs record `223f8f6aad7923ff186ac77a2c9fc29b9f140c86`. Its `cmd/loadgen/dispatch.go` and `dispatch_test.go` match generator-only fix `afc56a18ff02944951c6853cb1fdecd4f14ebb35`. Admission records that it stopped because the next scheduled arrival was not yet due. After flushing, it retains that arrival's timestamp even if overdue, so admission immediately retries. Credit/capacity stalls still wait for a notification or normal poll timer. Fixed schedule, strict endpoint-ticket selection and finite cohort drain remain unchanged.

The campaign mapping is `/home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/repo/experiments/shape/campaign.json`. It records exact run IDs, inputs and status. Replacement data roots are:

- `/home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/results/open-fixed`
- `/home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/results/matched-fixed`
- `/home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/results/extended-fixed`
- `/home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/results/cap-open-fixed`

Except for the single cap run, each root has a run-ID directory. A raw window is `RUN/shape-CASE/CASE/TARGET-REP-rRATE/`, containing `load.json`, `before.json`, `after.json` and `audit.json`. Plans, command logs and runner metadata are in `RUN/shape-CASE/`. For cap run 38427624163, `cap-open-fixed/shape-put-1k-leader-open/` is the job root, without another run-ID directory.

| Purpose | Replacement run | Case | Selected rate/s |
| --- | ---: | --- | ---: |
| Leader Range regression | 38427598354 | range-1k-leader-open | 5000 |
| Balanced Range regression | 38427601551 | range-1k-balanced-open | 5000 |
| Balanced LIST regression | 38427610056 | list-1k-balanced-open | 600 |
| Cap comparison | 38427624163 | put-1k-leader-open | 95000, 110000, 125000 |
| Failed primary high balanced 1 KiB Put | 38427533507 | put-1k-balanced-open | etcd 12000 |
| Failed primary high balanced 10 KiB Put | 38427549648 | put-10k-balanced-open | etcd 4000, 5000 |

I recomputed the numbers from raw window JSON and checked all 875 commands against campaign plans, including rate, seed, workload, connection count, sampled PIDs and alternating target order. Every runner records the same fixed-generator SHA-256 listed below. Summaries were cross-checks, not the authority for completeness or validity.

## Campaign accounting, including failures

All 875 planned windows from 28 runs are present. Raw accounting passes for 868 windows; seven etcd windows fail. All 445 tigerstore/Cap windows pass the checked accounting invariants. By folder, there are 473 valid plus seven invalid primary-open windows, 240 valid matched windows, 110 valid extended windows and 45 valid cap-comparison windows.

For every valid window I checked zero request/sampling/invariant errors, successful exit, zero failed or finally unsent requests, complete scheduled/latency cohorts, zero final backlog/inflight, the concurrency ceiling, exact revision deltas on all replicas and unchanged leader/term. Endpoint request counts obey ticket round robin, differing by at most one. PID/start identities and recorded server-thread placement match before and after. For all 445 tigerstore/Cap windows, persistence jobs are complete at the two fences, file histograms sum to call counts and no new write/fsync error occurs.

Only primary runs 38427533507 and 38427549648 have workflow failure status. The failing raw windows are:

| Run | Store | Rep | Offered/s | Cohort | Failed requests | Finally unsent | Replica revision delta |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 38427533507 | etcd, 1 KiB balanced | 5 | 12000 | 36000 | 1615 | 0 | 36000 |
| 38427549648 | etcd, 10 KiB balanced | 1 | 5000 | 15000 | 1479 | 0 | 14999 |
| 38427549648 | etcd, 10 KiB balanced | 3 | 4000 | 12000 | 1720 | 0 | 12000 |
| 38427549648 | etcd, 10 KiB balanced | 3 | 5000 | 15000 | 1463 | 0 | 14996 |
| 38427549648 | etcd, 10 KiB balanced | 4 | 5000 | 15000 | 1536 | 0 | 15000 |
| 38427549648 | etcd, 10 KiB balanced | 5 | 4000 | 12000 | 1321 | 0 | 12000 |
| 38427549648 | etcd, 10 KiB balanced | 5 | 5000 | 15000 | 11 | 0 | 15000 |

All seven audits report `arrival cohort incomplete` and exit code 1. Load errors are gRPC Unavailable request timeouts, some with the connection-lost qualification. A full revision delta does not mean every client observed success. These windows cannot supply complete-cohort latency/SLO claims. The supplied tables mark all groups in each failed artifact `Valid=False`, including groups whose individual windows pass accounting. Keep that job-level failure visible rather than calling the whole paired artifact successful. Empty `summary-errors.json` means no summarizer error, not that every workload window succeeded.

## Runner regression checks

Cells are medians with the minimum to maximum across all five repetitions. Service p99 is admission-to-completion, not server-only latency. Intended-arrival p99 includes generator waiting. These two percentiles must not be subtracted to claim a queue-delay percentile. `Qmax` is maximum observed scheduled-but-not-admitted backlog, not a continuously sampled exact maximum.

| Input | Data | Arrival p99 ms | Service p99 ms | Arrival p99.9 ms | Qmax requests |
| --- | --- | ---: | ---: | ---: | ---: |
| Range leader 5k/s | Old 38416431454 | 73.662 [69.992, 95.683] | 4.440 [4.088, 5.358] | 88.867 [83.624, 102.498] | 441 [417, 501] |
| Range leader 5k/s | Fixed 38427598354 | 1.511 [1.503, 1.544] | .571 [.526, .662] | 2.695 [2.044, 4.342] | 8 [7, 11] |
| Range balanced 5k/s | Old 38416435674 | 84.673 [74.711, 88.343] | 3.269 [2.548, 3.940] | 95.158 [87.818, 100.401] | 481 [441, 501] |
| Range balanced 5k/s | Fixed 38427601551 | 1.397 [1.372, 1.430] | .452 [.445, .555] | 1.749 [1.495, 2.793] | 6 [6, 10] |
| LIST balanced 600/s | Old 38416443586 | 103.023 [42.336, 148.374] | 78.905 [22.872, 110.887] | 171.442 [72.090, 221.774] | 49 [31, 57] |
| LIST balanced 600/s | Fixed 38427610056 | 6.013 [5.620, 6.242] | 5.554 [5.456, 5.882] | 9.535 [8.176, 9.994] | 2 [1, 2] |

The old files are under `results/matched/RUN/shape-CASE/CASE/`; replacement files are under `results/matched-fixed/RUN/shape-CASE/CASE/`, using the absolute results root above. Old matched source is `99cd39439b62ccfb80792f36f51d4a5e98c23797`.

All-five fixed observations below retain the individual values, in repetition order 1 through 5:

| Input | Arrival p99 ms, all five | Service p99 ms, all five | Qmax, all five |
| --- | --- | --- | --- |
| Range leader 5k/s | 1.511, 1.544, 1.503, 1.528, 1.511 | .526, .662, .535, .600, .571 | 8, 11, 8, 11, 7 |
| Range balanced 5k/s | 1.430, 1.397, 1.421, 1.389, 1.372 | .555, .462, .445, .452, .445 | 10, 6, 7, 6, 6 |
| LIST balanced 600/s | 6.111, 6.013, 6.242, 5.882, 5.620 | 5.554, 5.652, 5.882, 5.456, 5.489 | 1, 2, 2, 2, 1 |

Every selected Range window completes all 15,000 scheduled requests; balanced endpoint counts are exactly 5000 each. Every selected LIST window completes all 1800 scheduled LISTs, exactly 600 per endpoint. Replica revision deltas stay zero and final backlog/inflight is zero. LIST validation checks the seeded 1000-key result and value bytes, not merely a successful RPC. This confirms the correction on these runner workloads. It does not prove every conceivable credit/timer interleaving or turn the old/new difference into an isolated server optimization estimate.

## Corrected cap comparison

Run 38427624163 uses one leader endpoint, 18 connections, global fixed arrivals, ceiling 4096, 4096 keys and 1024-byte Puts. Each window starts a fresh three-node cluster, seeds/probes, warms for one second at its fixed offered rate, drains, then measures three seconds of scheduled arrivals and drains the complete cohort. Same-number Main/Cap repetitions use the same seed and adjacent windows on the same job VM. Order reverses on even repetitions; rates always ascend. All 45 windows, including 15 etcd baseline windows, pass the independent checks. The etcd baseline uses different offered rates and is not a Main/Cap equal-rate comparison.

Main keeps the 4 MiB packing cap. Candidate production source changes only `DefaultConfig.MaxBatchBytes` to `1 << 20`. I reproduced its source-file SHA-256, `2f0003581a137053a10ed0bec9fade55247c9fc83974a604b604209b11ce5095`, against `variants/cap1m-sha256.txt`. Its short tests include core, node, server, lint and simulation, plus the large-request/durability/pipelining witnesses. The default cap remains a soft packing target, not a new request-admission limit.

```text
Main binary:      27e98ccdb7c1a2a54d9e52cac5b22638b2365c3c29938afcfda099bb4559abd0
Cap binary:       3c79e251d6b6c33304adcda040fbf146e581a98bd1176a725d490dc2fc555448
Fixed generator:  7c9ac7d18b9e46952f6d310a2dc6e4a038c610acd555302fe89c6b787fcd45f6
```

The runner has Go 1.27.2, four Neoverse-N2 CPUs, about 16.72 GB RAM and an ext4 virtual disk with guest barriers enabled. The leader uses CPU 0, followers CPUs 1 and 2, and generator plus sampler CPU 3. Server `GOMAXPROCS=2`, generator one, `GOMEMLIMIT=2GiB`, client TLS and encrypted peers are unchanged. Profiling is off. Main and Cap remain different executables with different build paths/metadata. This is not a byte-identical placebo comparison.

### Paired throughput and tails

Cohort rate is successful scheduled operations divided by arrival plus drain time. Window rate counts completions in the three-second arrival window. Rates below are thousands of Puts/s. Each paired percentage is computed within repetition as `100 * (Cap / Main - 1)`, then summarized across five pairs. No range is a confidence interval.

| Offered k/s | Main cohort k/s | Cap cohort k/s | Paired cohort change % | Paired window change % |
| ---: | ---: | ---: | ---: | ---: |
| 95 | 91.323 [88.422, 94.149] | 93.501 [27.070, 94.746] | +2.38 [-71.25, +5.68] | +2.79 [-34.09, +5.62] |
| 110 | 94.140 [87.985, 95.327] | 99.169 [93.031, 100.494] | +5.47 [4.03, 6.37] | +4.23 [3.68, 8.65] |
| 125 | 93.534 [89.782, 97.741] | 99.288 [97.459, 101.056] | +5.33 [2.11, 8.55] | +6.63 [3.90, 8.50] |

| Offered k/s | Binary | p99 ms | p99.9 ms | Qstop requests | Qmax requests | Drain s |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 95 | Main | 155.714 [77.332, 244.842] | 174.588 [84.148, 258.474] | 9810 [58, 17394] | 11992 [3467, 18640] | .121 [.027, .223] |
| 95 | Cap | 76.808 [61.997, 7365.198] | 88.867 [73.138, 7532.970] | 2259 [41, 95434] | 3800 [2442, 95434] | .048 [.008, 7.528] |
| 110 | Main | 519.045 [468.713, 765.460] | 531.628 [483.394, 778.043] | 43317 [39266, 66083] | 44064 [39975, 66083] | .505 [.462, .751] |
| 110 | Cap | 342.884 [298.844, 551.551] | 351.273 [309.330, 568.328] | 30503 [27154, 47762] | 33187 [27337, 48304] | .328 [.284, .547] |
| 125 | Main | 1017.119 [840.958, 1178.599] | 1029.702 [849.347, 1186.988] | 93562 [81227, 104410] | 94140 [82027, 104948] | 1.009 [.837, 1.177] |
| 125 | Cap | 782.238 [719.323, 849.347] | 794.821 [727.712, 861.929] | 71971 [69942, 81751] | 72302 [70728, 81751] | .777 [.711, .848] |

| Offered k/s | Paired p99 change % | Paired p99.9 change % | Paired Qstop change % |
| ---: | ---: | ---: | ---: |
| 95 | -50.67 [-62.70, +9424.07] | -49.10 [-59.94, +8852.03] | -85.07 [-99.64, +164441.38] |
| 110 | -29.90 [-39.29, -27.82] | -29.98 [-38.80, -26.95] | -28.13 [-46.65, -23.92] |
| 125 | -20.13 [-29.28, -9.48] | -19.87 [-29.33, -8.89] | -20.53 [-24.40, -13.89] |

Cap improves full-cohort rate, p99/p99.9 and stop backlog in all five 110k and 125k pairs. Both variants still accumulate substantial backlog and drain at those rates, so neither demonstrates sustained service at the offered rate. Cap favors four of five 95k pairs, but its severe observation in repetition 4 prevents a uniform benefit claim. The huge 95k percentage ranges also reflect small Main stop backlog in that pair, not a typical effect size.

### CPU and file sync

CPU is through-drain process user plus system CPU divided by the complete successful cohort, in us/Put. Generator cores use arrival-window CPU sampling. File counters cover the wider replica/persistence-fenced interval. Puts/fsync is cohort divided by file Sync calls, not requests per Raft entry. Mean fsync is wall time, not CPU. File I/O fraction is `(write wall + fsync wall) / fenced elapsed`; it is not device utilization and cannot be added to CPU occupancy.

| Offered k/s | Binary | Leader us/Put | F1 us/Put | F2 us/Put | Generator us/Put | Generator cores |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 95 | Main | 8.772 [8.526, 9.088] | 2.632 [2.456, 2.737] | 2.526 [2.316, 2.667] | 3.580 [3.446, 3.986] | .323 [.314, .372] |
| 95 | Cap | 9.614 [9.579, 10.035] | 2.842 [2.702, 2.947] | 2.702 [2.491, 2.772] | 4.297 [3.995, 4.491] | .408 [.276, .422] |
| 110 | Main | 8.303 [8.061, 8.848] | 2.545 [2.364, 2.697] | 2.424 [2.242, 2.606] | 3.354 [3.206, 3.501] | .313 [.308, .325] |
| 110 | Cap | 8.303 [8.242, 9.212] | 2.485 [2.455, 2.848] | 2.364 [2.303, 2.697] | 3.325 [3.288, 3.670] | .333 [.330, .350] |
| 125 | Main | 8.080 [7.893, 8.693] | 2.427 [2.347, 2.613] | 2.320 [2.240, 2.507] | 3.255 [3.177, 3.440] | .307 [.297, .323] |
| 125 | Cap | 8.267 [8.213, 8.800] | 2.453 [2.400, 2.693] | 2.347 [2.293, 2.587] | 3.310 [3.297, 3.587] | .337 [.334, .356] |

| Offered k/s | Binary | Leader Puts/fsync | Fsync calls | Mean fsync ms | Fsync wall s | Write wall s | File I/O fraction |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 95 | Main | 993.0 [798.3, 1096.2] | 287 [260, 357] | 6.408 [4.888, 6.745] | 1.836 [1.666, 1.909] | .343 [.323, .434] | .652 [.633, .693] |
| 95 | Cap | 680.2 [639.0, 1131.0] | 419 [252, 446] | 6.083 [4.221, 21.670] | 2.008 [1.883, 9.340] | .425 [.302, .483] | .759 [.708, .917] |
| 110 | Main | 1068.0 [1061.1, 1089.1] | 309 [303, 311] | 6.803 [6.574, 7.289] | 2.109 [2.012, 2.267] | .408 [.398, .457] | .677 [.668, .699] |
| 110 | Cap | 1304.3 [1195.7, 1346.9] | 253 [245, 276] | 9.771 [9.150, 10.876] | 2.542 [2.394, 2.708] | .356 [.231, .404] | .825 [.795, .839] |
| 125 | Main | 1112.8 [961.5, 1150.3] | 337 [326, 390] | 7.069 [5.903, 7.721] | 2.443 [2.228, 2.517] | .447 [.402, .531] | .676 [.657, .705] |
| 125 | Cap | 1353.8 [1284.2, 1378.7] | 277 [272, 292] | 10.451 [9.845, 11.036] | 2.919 [2.875, 3.002] | .407 [.318, .422] | .828 [.806, .862] |

| Offered k/s | Paired leader CPU change % | Paired Puts/fsync change % | Paired mean-fsync-wall change % | Paired I/O-fraction change % |
| ---: | ---: | ---: | ---: | ---: |
| 95 | +11.84 [6.64, 14.40] | -22.27 [-41.70, +13.89] | +22.24 [-34.12, +295.34] | +16.56 [6.74, 35.88] |
| 110 | +2.26 [-0.73, +4.11] | +21.69 [11.96, 24.90] | +48.64 [26.35, 52.70] | +20.10 [16.39, 23.71] |
| 125 | +2.31 [1.23, 4.05] | +19.86 [12.67, 42.86] | +45.40 [35.36, 83.76] | +25.53 [17.54, 27.15] |

Externally fenced process ticks per cohort corroborate paired leader CPU medians of +11.38%, +2.62% and +1.97%. This is not a general CPU/Put reduction. The identical displayed CPU medians at 110k do not imply zero paired change. Generator occupancy stays below .43 core, but non-saturation alone does not certify arrivals or equal socket bursts.

At 110k, median Puts per fenced Raft-index increment falls from 1534.9 [1513.8, 1609.8] to 873.0 [844.0, 899.2]; at 125k it falls from 1616.4 [1306.6, 1712.3] to 890.7 [882.4, 901.4]. Physical groups grow while logical entries shrink. Multiple entries can share a persistence job. Mean fsync wall rises in every 110k and 125k pair, so the gain cannot be described as faster individual Sync calls.

The 95k Cap repetition 4 has a 10.708-second fenced interval. File-sync wall sums are 9.340, 9.358 and 9.365 seconds on leader/F1/F2. All requests eventually succeed, with no new file error or leader/term change. Its Sync histograms contain many long calls, not one recorded seven-second Sync. These counters document a severe persistence/time anomaly but do not identify its cause. Do not discard it as irrelevant VM noise or call it a correctness failure without further evidence.

### All fifteen cap pairs

Every slash is Main / Cap. Rates are complete-cohort k/s. No repetition is excluded.

| Offered k/s | Rep | Cohort k/s | Paired gain % | p99 ms | p99.9 ms | Qstop |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 95 | 1 | 88.422 / 93.447 | +5.68 | 244.842 / 93.061 | 258.474 / 103.547 | 17394 / 2596 |
| 95 | 2 | 93.206 / 94.459 | +1.34 | 95.158 / 73.138 | 103.023 / 84.148 | 3815 / 57 |
| 95 | 3 | 91.323 / 93.501 | +2.38 | 155.714 / 76.808 | 174.588 / 88.867 | 9810 / 2259 |
| 95 | 4 | 94.149 / 27.070 | -71.25 | 77.332 / 7365.198 | 84.148 / 7532.970 | 58 / 95434 |
| 95 | 5 | 90.800 / 94.746 | +4.35 | 166.199 / 61.997 | 174.588 / 73.138 | 11337 / 41 |
| 110 | 1 | 87.985 / 93.031 | +5.73 | 765.460 / 551.551 | 778.043 / 568.328 | 66083 / 47762 |
| 110 | 2 | 94.140 / 98.396 | +4.52 | 519.045 / 363.856 | 531.628 / 372.244 | 43317 / 32955 |
| 110 | 3 | 93.737 / 99.710 | +6.37 | 533.725 / 324.010 | 543.162 / 332.399 | 50985 / 27201 |
| 110 | 4 | 95.327 / 99.169 | +4.03 | 475.005 / 342.884 | 483.394 / 351.273 | 42440 / 30503 |
| 110 | 5 | 95.285 / 100.494 | +5.47 | 468.713 / 298.844 | 483.394 / 309.330 | 39266 / 27154 |
| 125 | 1 | 89.782 / 97.459 | +8.55 | 1178.599 / 849.347 | 1186.988 / 861.929 | 104410 / 81751 |
| 125 | 2 | 93.331 / 98.242 | +5.26 | 1025.507 / 819.986 | 1038.090 / 832.569 | 93562 / 76895 |
| 125 | 3 | 97.741 / 99.801 | +2.11 | 840.958 / 761.266 | 849.347 / 773.849 | 81227 / 69942 |
| 125 | 4 | 94.259 / 99.288 | +5.33 | 979.370 / 782.238 | 991.953 / 794.821 | 90561 / 71971 |
| 125 | 5 | 93.534 / 101.056 | +8.04 | 1017.119 / 719.323 | 1029.702 / 727.712 | 93783 / 70904 |

## Interpretation limits and reproducible arithmetic

Open warmup uses the fixed rate for one second and drains. At each cap rate, Main and Cap have identical before revisions across all five repetitions, 99,116, 114,116 and 129,116. Do not attribute these open comparisons to unequal warmup write counts. Equal history counts still do not imply identical entry packing, elapsed warmup, heap/cache state or machine state. Static closed-loop warmup in the older control is time-based and leaves different history counts, as the retained report explains.

The corrected high-rate cap result agrees in scale with the older qualified static C4096 gain, but the trials use different input shape, binaries and VM jobs. Their magnitudes cannot be pooled into a universal gain. The old open results at source `4ee913fb` remain excluded from arrival-shape and SLO claims. The corrected low-rate comparison demonstrates removal of the known timer artifact on these workloads, without isolating how much of the old/new server cost or tail change came from smoother arrivals versus a fresh machine.

Use `load.cohort / shape.through_drain_s` for complete-cohort throughput, `load.ops / load.duration_s` for window throughput, and `shape.cpu_through_drain` for full-cohort CPU. Recompute external CPU from process user/system tick deltas times 10,000 us divided by cohort, using Linux `USER_HZ=100`. Derive file counts and wall sums from fenced before/after differences. Take every paired percentage before taking its five-pair median/range. The audit required JSON analysis only. No Go build/test, local benchmark, runner launch, push or public post was performed for this report.
