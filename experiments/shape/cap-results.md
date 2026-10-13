# Byte-cap A/B results, Actions run 38418280627

## Result

Lowering only the core proposal-batch cap from 4 MiB to 1 MiB improved the leader-only 1 KiB C4096 **through-drain cohort throughput** in all five pairs: median **+20.224498%**, range **+15.284310% to +22.361771%**. The 10 KiB C256 result also improved in every pair: **+9.605841%**, range **+8.278519% to +17.112521%**.

The measured queue changes support the active-byte-cap/pipelining explanation. Work moved from queued proposals into outstanding persistence jobs; the disk worker synced more jobs and operations per fsync, while fsync counts barely changed. Leader CPU consumption, file-call wall occupancy and shared-device activity increased. This is not a measured reduction in fsync cost or CPU per operation.

There is no clear additional throughput benefit from 256 KiB. Balanced C4096 was already pipelined and showed mixed, small throughput changes. At 1 KiB C256 the 1 MiB cap cannot be reached by the client cohort; its five negative pairs are a measured difference, not evidence that an active cap caused a regression. Tail latencies do not uniformly improve.

## Evidence and independent validation

Raw root:

```text
/home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/results/caps/38418280627
```

For each full case name `CASE`, exact paths are:

```text
shape-CASE/runner.txt
shape-CASE/plan.json
shape-CASE/commands.json
shape-CASE/variants/{cap1m.diff,cap256k.diff,cap1m-test.txt,cap256k-test.txt,cap1m-sha256.txt,cap256k-sha256.txt}
shape-CASE/CASE/{tigerstore,cap1m,cap256k,etcd}-{1,2,3,4,5}/{load.json,audit.json,before.json,after.json,samples.jsonl,placement.json}
```

For example: `shape-put-1k-leader-c4096/put-1k-leader-c4096/cap1m-1/load.json`. Case abbreviations used below:

| Label | Full case |
| --- | --- |
| L256 | put-1k-leader-c256 |
| L1024 | put-1k-leader-c1024 |
| L4096 | put-1k-leader-c4096 |
| B4096 | put-1k-balanced-c4096 |
| 10K256 | put-10k-leader-c256 |

All 100 planned load records were present. I recomputed counts, revision deltas, same-process identities, leadership/term stability, recorded affinities, balanced endpoint allocation, nonnegative file-counter/histogram deltas and the two fresh owner persistence-fence proofs. All **75/75 Tigerstore windows** passed these checks. Their peak in-flight counts equal the requested global concurrency; every cohort was fully successful, with no unsent requests. No recorded sampling errors occurred.

The five case plans use three-second arrival windows, one-second warmups, five repetitions and no profiles. All targets use 18 connections and **global-ticket-round-robin dispatch**, verified in both load records and commands. The static-dispatch binary is present but unused in this run. Within each case, target order reverses on alternate repetitions. These are five short synthetic closed-loop pairs per case, not identical request traces, confidence intervals or a production clientv3/kube-apiserver result.

Every runner record identifies harness `b2994a4b68efde0f4f20d9e4bfe9796d42e1db1a`, Go 1.27.2 for Tigerstore, four ARM Neoverse-N2 CPUs, ext4 on `/dev/sda1`, one CPU per server with GOMAXPROCS=2, and generator/sampler sharing CPU3. All five artifacts have the same binary hashes:

| Binary | SHA-256 |
| --- | --- |
| main | 09e14b91ea7739fbd3e67fbdd87ddd534e1ca5e6e1befef592b855139908c676 |
| cap1m | 3c79e251d6b6c33304adcda040fbf146e581a98bd1176a725d490dc2fc555448 |
| cap256k | d5888682092d09f3d0a3506df84f25f85896ca7dac1ff7aef176bd8c6506afb9 |
| loadgen | df4bb1a4de5f9aa6b371f119e8a3ff1d182adc843d4f04523ce08b39fdd1e15b |

The recorded variant diffs change only `DefaultConfig.MaxBatchBytes` to `1 << 20` or `256 << 10`. The build harness also installs experiment tests; its recorded core/node/server/lint tests passed. I did not execute Go, servers or measurements locally.

### Invalid etcd window remains visible

Etcd has **24/25 valid windows overall and 4/5 in B4096**, not five valid balanced repetitions. `shape-put-1k-balanced-c4096/failures.json` identifies rep 2. Its raw load has 32,200 arrivals, 11 plain request timeouts, 1,686 connection-lost timeouts and 501 too-many-requests responses. Revision delta is 31,531, not 32,200; drain is 6.574747 s. It is invalid.

The balanced Actions measurement step consequently failed. Tigerstore's 15 balanced windows individually passed; that does not make the entire four-target job pass. No passing-only etcd median, speedup or tail comparison is used here.

## Definitions

- **Cohort throughput** = successful arrivals / elapsed time through their drain, from `shape.cohort_throughput`.
- **Completion-window throughput** = successful completions inside the three-second window / 3, from `load.throughput`. It is a different denominator and excludes drain completions.
- **Paired change** for rep i = `100 * (variant_i / main_i - 1)`. Reported medians are medians of those five ratios, not ratios of independent medians. Values below are rounded to six decimal places.
- Latency quantiles cover successful arrival cohorts through drain. Tables contain medians of per-repetition quantiles, not pooled latency quantiles. Positive latency changes mean worse latency.
- CPU/op uses `shape.cpu_through_drain`; CPU cores use the completion-window samples. Generator cores exclude the Python sampler and are not total CPU3 utilization.
- File metrics subtract fenced before/after counters. **File fraction** = leader `(Write wall + Sync wall) / fenced elapsed`. This includes descheduling and syscall waiting; it is not device utilization or a critical-path decomposition.
- Ops/fsync = cohort operations / leader file fsync calls. Jobs/fsync = owner-issued persistence jobs / those calls. Jobs include hard-state-only work; neither ratio is operations per Raft entry. A smaller entry cap does not imply one fsync per entry.
- WAL bandwidth sums file-write bytes across all three nodes / fenced elapsed. Device bytes are `/proc/diskstats` sda sector deltas times 512. Whole-device values include filesystem overhead, sampling output and other host activity.
- Queue means are sampled gauge means within the cohort start/done interval. The main cases have about 30 samples per repetition, with delayed owner publications. Means/maxima are not continuous-time averages/true peaks. `queued_writes` overlaps `pending_proposals`; do not add them. Outstanding jobs = issued minus completed, including active disk groups.

## Paired throughput changes

Each cell is median [minimum, maximum] across five pairs.

| Case | Cap | Cohort throughput change % | Completion-window change % | Positive cohort pairs |
| --- | --- | ---: | ---: | ---: |
| L256 | 1 MiB | -1.303502 [-4.139214, -0.634585] | -1.303831 [-4.118363, -0.640768] | 0/5 |
| L256 | 256 KiB | +0.031250 [-7.684007, +3.610080] | +0.074331 [-7.665310, +3.650873] | 3/5 |
| L1024 | 1 MiB | +3.877777 [-3.353049, +7.576623] | +3.690437 [-3.394340, +7.285182] | 4/5 |
| L1024 | 256 KiB | +2.941909 [-3.715159, +8.821522] | +2.748548 [-3.732801, +8.514737] | 3/5 |
| L4096 | 1 MiB | +20.224498 [+15.284310, +22.361771] | +21.268155 [+15.537879, +22.257511] | 5/5 |
| L4096 | 256 KiB | +20.734273 [+15.423620, +23.103562] | +20.668667 [+17.020496, +22.883629] | 5/5 |
| B4096 | 1 MiB | +0.091140 [-1.945091, +3.346370] | +0.067188 [-2.093751, +3.310386] | 3/5 |
| B4096 | 256 KiB | +0.764216 [-0.032491, +2.534587] | +0.695062 [-0.163898, +2.576507] | 4/5 |
| 10K256 | 1 MiB | +9.605841 [+8.278519, +17.112521] | +11.018818 [+8.757490, +17.304414] | 5/5 |
| 10K256 | 256 KiB | +11.347625 [+6.158797, +18.214635] | +11.268109 [+6.520141, +18.414276] | 5/5 |

The exact five cohort-throughput changes, ordered rep 1 through 5, are:

| Case | 1 MiB vs main % | 256 KiB vs main % |
| --- | --- | --- |
| L256 | -4.139214, -1.303502, -0.634585, -2.732856, -1.067382 | -7.684007, -1.385870, +0.031250, +3.610080, +0.557632 |
| L1024 | +5.750085, +1.478233, +3.877777, -3.353049, +7.576623 | +6.158486, -3.715159, +2.941909, -1.738968, +8.821522 |
| L4096 | +20.224498, +21.624865, +15.284310, +22.361771, +15.836611 | +15.423620, +20.734273, +22.775007, +23.103562, +16.947294 |
| B4096 | +0.372677, +0.091140, -1.945091, -0.069684, +3.346370 | +2.144727, -0.032491, +0.304974, +2.534587, +0.764216 |
| 10K256 | +9.605841, +11.707096, +17.112521, +8.278519, +9.213290 | +6.158797, +11.347625, +18.214635, +10.061433, +11.349886 |

Comparing 256 KiB directly with 1 MiB gives mixed signs in every case:

| Case | Paired cohort change %, median [range] |
| --- | ---: |
| L256 | +0.670088 [-3.697855, +6.521150] |
| L1024 | +0.386194 [-5.117740, +1.670080] |
| L4096 | +0.606227 [-3.993261, +6.497585] |
| B4096 | +1.765470 [-2.498543, +2.606088] |
| 10K256 | +0.941072 [-3.144945, +1.956352] |

## CPU and throughput levels

Every row below is a median across five valid windows. CPU/op is aggregate server CPU through drain, not the sum of separately reported node medians.

| Case | Cap | Cohort ops/s | Server CPU us/op | Leader window cores | All server window cores | Generator window cores |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| L256 | main | 60,828.807 | 25.438 | 0.930 | 1.540 | 0.457 |
| L256 | 1 MiB | 60,089.400 | 25.608 | 0.923 | 1.540 | 0.459 |
| L256 | 256 KiB | 61,157.493 | 25.707 | 0.937 | 1.560 | 0.469 |
| L1024 | main | 85,151.784 | 17.223 | 0.890 | 1.430 | 0.428 |
| L1024 | 1 MiB | 85,944.825 | 16.838 | 0.910 | 1.457 | 0.436 |
| L1024 | 256 KiB | 85,967.379 | 18.064 | 0.960 | 1.557 | 0.452 |
| L4096 | main | 78,940.099 | 13.868 | 0.693 | 1.097 | 0.263 |
| L4096 | 1 MiB | 93,996.252 | 14.638 | 0.870 | 1.383 | 0.333 |
| L4096 | 256 KiB | 94,566.083 | 14.819 | 0.883 | 1.410 | 0.333 |
| B4096 | main | 96,241.472 | 18.538 | 0.697 | 1.810 | 0.344 |
| B4096 | 1 MiB | 97,385.834 | 18.602 | 0.703 | 1.813 | 0.349 |
| B4096 | 256 KiB | 98,450.422 | 18.768 | 0.710 | 1.823 | 0.361 |
| 10K256 | main | 9,334.212 | 80.857 | 0.420 | 0.760 | 0.144 |
| 10K256 | 1 MiB | 10,556.124 | 82.274 | 0.477 | 0.863 | 0.155 |
| 10K256 | 256 KiB | 10,729.941 | 86.094 | 0.497 | 0.910 | 0.151 |

In L4096, leader cohort CPU rises from 8.761 to 9.175 to 9.299 us/op. In 10K256 it rises from 44.663 to 45.302 to 47.511 us/op. The larger throughput gains are accompanied by more work per operation, not an observed CPU optimization.

## Leader file sync/grouping and shared device

Medians of per-window measurements; fsync counts include every observed WAL file Sync, including metadata/rotation. Fenced elapsed medians are 3.18-3.21 seconds, so file bandwidth is not divided by exactly three seconds.

| Case | Cap | Fsync calls | Ops/fsync | Jobs/fsync | Mean fsync ms | Write+Sync wall fraction | All-node WAL MiB/s | sda MiB/s | sda busy fraction |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| L256 | main | 2,487 | 73.620 | 1.138 | 0.833 | 0.802 | 178.609 | 224.751 | 0.340 |
| L256 | 1 MiB | 2,481 | 72.631 | 1.138 | 0.854 | 0.800 | 176.207 | 223.656 | 0.331 |
| L256 | 256 KiB | 2,445 | 74.583 | 1.155 | 0.831 | 0.806 | 179.125 | 226.581 | 0.348 |
| L1024 | main | 888 | 288.815 | 1.152 | 2.012 | 0.697 | 248.935 | 270.852 | 0.359 |
| L1024 | 1 MiB | 893 | 285.064 | 1.146 | 1.893 | 0.694 | 251.915 | 269.220 | 0.357 |
| L1024 | 256 KiB | 772 | 334.648 | 2.124 | 2.214 | 0.717 | 251.833 | 271.915 | 0.369 |
| L4096 | main | 197 | 1,193.365 | 1.099 | 10.038 | 0.724 | 231.859 | 235.881 | 0.552 |
| L4096 | 1 MiB | 195 | 1,460.108 | 2.314 | 12.308 | 0.848 | 277.227 | 282.512 | 0.724 |
| L4096 | 256 KiB | 192 | 1,473.109 | 4.510 | 12.911 | 0.852 | 279.825 | 285.528 | 0.718 |
| B4096 | main | 226 | 1,288.080 | 14.848 | 11.505 | 0.878 | 284.391 | 294.324 | 0.758 |
| B4096 | 1 MiB | 218 | 1,334.050 | 13.139 | 12.332 | 0.898 | 289.402 | 294.691 | 0.771 |
| B4096 | 256 KiB | 189 | 1,567.429 | 14.497 | 14.288 | 0.888 | 290.430 | 295.756 | 0.761 |
| 10K256 | main | 373 | 77.081 | 1.184 | 6.759 | 0.854 | 259.655 | 269.833 | 0.669 |
| 10K256 | 1 MiB | 324 | 98.187 | 1.949 | 7.950 | 0.893 | 294.016 | 302.072 | 0.800 |
| 10K256 | 256 KiB | 274 | 119.230 | 5.292 | 9.773 | 0.892 | 300.076 | 306.949 | 0.806 |

In L4096, leader fsync wall sums increase from 2.014 to 2.445 to 2.472 seconds; write wall medians are 0.308, 0.246, 0.287 seconds. Owner-issued job medians rise from 223 to 447 to 866 while fsync counts stay around 195. In 10K256, issued jobs rise from 436 to 632 to 1,437, even as fsync calls fall. This directly supports greater disk-group coalescing rather than one sync per smaller proposal.

All-node file bytes per operation stay approximately constant: L4096 3,273.096/3,273.164/3,273.442 bytes; B4096 3,277.661/3,276.636/3,276.153; 10K256 30,922.273/30,919.721/30,922.116. There is no meaningful reduction in WAL byte amplification explaining the gain. Physical sda bytes/op are similarly stable in L4096: 3,335.212/3,339.497/3,337.102.

The fsync histogram supplies upper bounds, not exact p99 values. Its median per-run p99 upper bound is 65.536 ms for all L4096 and B4096 variants; 10K256 gives 65.536/16.384/65.536 ms. Do not interpret this coarse bucket movement as a universal tail improvement.

## Queue and inflight evidence

Means below are medians of five sampled means. `Max queued` is the largest sampled queued-write count across all five repetitions. The inflight count column is the median of each repetition's largest observed member count, not a sum over followers.

| Case | Cap | Mean queued writes | Max queued | Mean outstanding persist jobs | Mean persist-channel batches | Largest-member inflight count |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| L256 | main | 73.828 | 255 | 1.655 | 0.552 | 2 |
| L256 | 1 MiB | 56.483 | 255 | 1.700 | 0.500 | 2 |
| L256 | 256 KiB | 72.233 | 235 | 1.700 | 0.567 | 3 |
| L1024 | main | 247.267 | 1,023 | 1.733 | 0.276 | 4 |
| L1024 | 1 MiB | 238.414 | 940 | 1.655 | 0.367 | 9 |
| L1024 | 256 KiB | 94.862 | 235 | 3.586 | 1.241 | 7 |
| L4096 | main | 993.800 | 3,599 | 1.433 | 0.267 | 2 |
| L4096 | 1 MiB | 413.300 | 940 | 4.167 | 1.333 | 6 |
| L4096 | 256 KiB | 105.300 | 235 | 7.633 | 2.600 | 23 |
| B4096 | main | 523.267 | 1,672 | 27.833 | 9.233 | 256 |
| B4096 | 1 MiB | 405.433 | 940 | 24.300 | 8.867 | 256 |
| B4096 | 256 KiB | 110.133 | 231 | 26.033 | 9.067 | 256 |
| 10K256 | main | 72.800 | 255 | 1.600 | 0.300 | 2 |
| 10K256 | 1 MiB | 51.267 | 101 | 3.567 | 1.600 | 3 |
| 10K256 | 256 KiB | 6.767 | 25 | 9.967 | 4.233 | 15 |

Every balanced repetition, for every Tigerstore variant, samples an inflight-full flag. None of the leader-only or 10 KiB repetitions does. Balanced owner backlogs remain small, but the persistence channel and active/completion pipeline already contain much more work than leader-only main. Count-window fullness is observed; this alone does not prove it is the sole throughput limiter.

For 45-byte benchmark keys, the conservative queued-proposal charge is 1,115 bytes for a 1 KiB Put and 10,331 for a 10 KiB Put. A 1 MiB cap fits 940 or 101 such requests; 256 KiB fits 235 or 25. The sampled queue boundaries match these values. L4096 main's sampled maximum, 3,599, remains below its 4 MiB threshold of 3,761 requests. Sampling cannot prove the byte trigger never fired between samples.

L256's entire 256-request 1 KiB cohort is below 1 MiB, so the 1 MiB byte trigger is inactive there. L1024 can cross it only near a full queue; the small, mixed gain and unchanged job depth do not show the sustained pipeline transition found in L4096 and 10K256.

## Latencies and tails

Absolute values are medians of the five per-window quantiles, milliseconds. Brackets give the full repetition range for each tail.

| Case | Cap | p50 ms | p99 ms [range] | p99.9 ms [range] |
| --- | --- | ---: | ---: | ---: |
| L256 | main | 4.071 | 8.946 [8.618, 9.273] | 15.106 [11.633, 17.760] |
| L256 | 1 MiB | 4.121 | 9.142 [8.749, 9.470] | 17.629 [13.074, 79.430] |
| L256 | 256 KiB | 4.039 | 9.535 [9.011, 10.322] | 13.730 [13.533, 71.041] |
| L1024 | main | 11.239 | 25.494 [22.610, 45.744] | 38.928 [28.115, 98.304] |
| L1024 | 1 MiB | 10.977 | 24.445 [22.872, 26.673] | 28.639 [28.115, 90.440] |
| L1024 | 256 KiB | 11.174 | 24.838 [23.790, 25.887] | 33.686 [29.950, 42.074] |
| L4096 | main | 51.511 | 76.808 [68.944, 79.954] | 85.197 [74.187, 87.818] |
| L4096 | 1 MiB | 40.763 | 73.662 [69.468, 79.954] | 83.624 [78.905, 92.013] |
| L4096 | 256 KiB | 41.288 | 74.187 [66.454, 76.808] | 85.197 [81.002, 94.634] |
| B4096 | main | 39.977 | 75.235 [69.468, 97.255] | 85.721 [78.905, 123.470] |
| B4096 | 1 MiB | 41.288 | 68.944 [66.454, 73.138] | 82.575 [75.760, 99.353] |
| B4096 | 256 KiB | 40.763 | 68.420 [62.521, 83.100] | 84.148 [71.041, 101.450] |
| 10K256 | main | 27.329 | 37.618 [34.734, 44.433] | 42.074 [39.453, 97.255] |
| 10K256 | 1 MiB | 24.838 | 37.093 [35.521, 50.725] | 40.763 [38.142, 151.519] |
| 10K256 | 256 KiB | 23.396 | 37.093 [35.258, 68.944] | 42.861 [39.977, 144.179] |

Paired tail changes vs main are median [minimum, maximum], percent:

| Case | Cap | p99 change % | p99.9 change % |
| --- | --- | ---: | ---: |
| L256 | 1 MiB | +2.197802 [-2.197802, +9.056605] | +17.211330 [-26.383765, +582.816952] |
| L256 | 256 KiB | +6.593407 [+2.120142, +16.730040] | -9.110630 [-23.800739, +510.704269] |
| L1024 | 1 MiB | -10.282777 [-44.842408, +17.971015] | -14.028057 [-48.345155, +9.324010] |
| L1024 | 256 KiB | -6.683805 [-43.409743, +9.855073] | -23.063974 [-65.733334, +28.657316] |
| L4096 | 1 MiB | 0.000000 [-9.556314, +7.117438] | +0.597015 [-5.538462, +17.391305] |
| L4096 | 256 KiB | -3.558719 [-16.336634, +11.406844] | +3.344482 [-6.079027, +27.561838] |
| B4096 | 1 MiB | -3.690037 [-29.110512, -0.754717] | -5.678234 [-33.121019, +15.902141] |
| B4096 | 256 KiB | -8.867925 [-29.649596, +9.688581] | -8.637874 [-31.847134, +18.348624] |
| 10K256 | 1 MiB | +6.792453 [-20.058998, +34.843207] | +0.664452 [-60.781672, +260.124617] |
| 10K256 | 256 KiB | +6.037736 [-20.648968, +74.750832] | +8.637874 [-58.894879, +242.679133] |

The 1 MiB L4096 p50 improves in every pair, paired median -21.052632%, but p99 is not uniformly better and p99.9 sometimes worsens. The 10 KiB p50 also improves in every 1 MiB pair, median -7.888631%, while two repetitions have substantially worse tails. Balanced 1 MiB p50 worsens in every pair, median +4.560261%, even though p99 improves in every pair. These distribution changes should remain visible alongside throughput.

## Mechanistic interpretation and limits

The source path is unchanged apart from the cap: `internal/core/node.go:514-526` calls `propose()` when adding a write would cross the nonempty batch budget, bypassing the ordinary leader local-append hold in `Process` at lines 654-675. Followers already forward proposals without this hold; incoming forwarded proposals go through Raft Step. `internal/node/node.go` still serially appends ordered jobs, syncs a drained group once, then reports ordered durable callbacks. See `audit.md` and the recorded variant diffs for the source audit.

Measured changes in L4096 and 10K256 are consistent with that mechanism: fewer writes waiting to be proposed, more issued persistence work, more jobs per fsync, higher engine byte rate and higher device busy accounting. Balanced ingress provides an independent consistency check: its main already has deep persistence work and count-window fullness, and reducing the cap adds little throughput. These observations support a pipeline-fill explanation rather than reduced byte amplification or cheaper fsync.

What is not established: exact byte-trigger counts, active-worker idle duration, per-entry grouping, a critical-path decomposition, a hardware bandwidth ceiling, or a general optimum of 1 MiB. The observation counters cover pending jobs but do not directly measure worker idle time. A smaller cap also increases entry/proposal work and can move the limit toward CPU, count windows or storage. The extra depth at 256 KiB supplies no consistent added gain and can worsen tails. No claim is made for LIST, linearizable reads, mixed production workloads or all design guarantees solely from these Put measurements.

No rerun, local Go execution, public post or push was performed for this audit.
