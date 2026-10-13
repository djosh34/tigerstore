# Inflight-message count control, run 38421653015

## Result

Balanced 1 KiB C4096 reaches the main's 256-message follower credit limit in sampled observations. Raising only `MaxInflightMsgs` to 1024 removes those sampled full/paused states and permits counts above 256, but **does not produce a consistent throughput gain**. Paired cohort-throughput change is median **-0.166118%**, range **-1.346423% to +1.673499%**, with two positive and three negative pairs. Count-credit stalls are real; their occurrence does not establish a binding aggregate throughput limit.

## Provenance and completeness

Raw root: `/home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/results/inflight`.

C4096 paths are `shape-put-1k-balanced-c4096/plan.json`, `runner.txt`, `commands.json`, `failures.json`, `variants/inflight1024.diff`, and `put-1k-balanced-c4096/{tigerstore,inflight1024,etcd}-{1..5}/{load.json,audit.json,before.json,after.json,samples.jsonl}`. The runner identifies harness `4ee913fbb7f799070c36af37de7e9aafbd696d49`, Go 1.27.2, four ARM Neoverse-N2 CPUs, three seconds measured and one second warmup. Main/candidate use identical global-ticket dispatch, 18 connections, global C4096, balanced sends within one, server CPUs0/1/2 and generator CPU3.

The recorded diff changes only `internal/core/node.go:233`, `MaxInflightMsgs: 256` to `1024`. The 32 MiB inflight-byte budget, 4 MiB message budget and core batch cap remain unchanged. Binary SHA-256 values:

- Main: `e35171d23afbd21032dc5f14d73e9faaba69df1c7712b04301c470b41a653130`
- Candidate: `935934e25c5c0b8784340d3dc0eb0946715e2151c5bb496b90827b368b7cb76a`
- Generator: `d8b496f2e4da538787930d6ffb143b9fe0aa473568c95a67441071d7784f724e`

All 15 C4096 planned load records exist. I independently checked all **10/10 Tigerstore windows**: complete successful cohorts, matching revision deltas, stable processes/leadership, affinities, balanced allocation, file-counter integrity and two distinct fresh owner persistence-fence proofs. Each passes. Etcd is **2/5 valid**, not five; its failures make the overall C4096 job fail.

**C1024 has no measurement or plan.** Its only artifacts are `shape-put-1k-balanced-c1024/variants/inflight1024.diff` and `inflight1024-test.txt`. The build-and-test step failed before the runner: `TestCanceledCalls` and `TestRawFrames` logged the leader-disk-stall watchdog, with `etcdserver: server stopped` also reported in the former. Core/node/lint/sim passed; server tests failed. This does not identify whether the parameter, disk service, scheduling or another condition caused the watchdog. The 15 intended C1024 windows must remain **missing**, tracked by the external dispatch manifest since there is no local plan. There is no C1024 throughput or queue result to infer or silently omit. No rerun was used to replace this failure.

## Count-full occurrence

Observations below use only leader-node samples whose outer timestamps fall within the arrival cohort through drain. Each repetition has 30 distinct, fresh owner publications, with two follower progress records per publication. Self-progress is excluded. Counts are nonempty MsgApp messages, not writes.

| Rep | Main largest sampled count | Main publications with a count-full follower / 30 | Candidate largest sampled count | Candidate full/paused publications / 30 |
| --- | ---: | ---: | ---: | ---: |
| 1 | 235 | 0 | 398 | 0 |
| 2 | 256 | 6 | 429 | 0 |
| 3 | 256 | 8 | 302 | 0 |
| 4 | 256 | 10 | 391 | 0 |
| 5 | 256 | 6 | 300 | 0 |

Main has a follower at count 256 with both `inflight_full` and `msg_app_flow_paused` in **30/150 publications (20%)**, covering four repetitions. At follower-record granularity it is **46/300 records**. Candidate has **0/150** full/paused publications and **0/300** full follower records. Its counts exceed the former 256 limit in all five repetitions, reaching 429, but never sample 1024.

Exact examples: main rep2 `samples.jsonl:5` has both followers at 256/full/paused; rep3 line5 has follower3 at 256/full/paused; rep4 line4 has both followers at 256/full/paused; rep5 line3 has follower2 at 256/full/paused. Rep1 does not sample fullness, which is not proof that fullness never occurred between publications.

Raft `tracker/inflights.go:131-134` defines `Full()` as count-at-limit **or** bytes-at-limit. The observer does not expose byte occupancy. Main's simultaneous count==256 proves its count credit was exhausted in these samples, regardless of bytes. Candidate's lack of sampled Full flags does not prove continuous absence of either limit. Sampling is approximately 100 ms and cannot measure continuous stall fractions.

## Queues, CPU and file grouping

Values are medians of five per-repetition measurements; queue means are medians of sampled means, not continuous-time averages. Pending proposals overlap queued writes. CPU/op covers successful cohorts through drain; cores cover the three-second completion window.

| Quantity | Main 256 | Candidate 1024 |
| --- | ---: | ---: |
| Cohort throughput, ops/s | 96,634.593 | 96,535.678 |
| Completion-window throughput, ops/s | 96,325.000 | 96,249.000 |
| Leader CPU us/op | 7.243 | 7.185 |
| All-server CPU us/op | 19.148 | 18.916 |
| Generator CPU us/op | 3.634 | 3.584 |
| Leader / all-server / generator cores | 0.703 / 1.859 / 0.353 | 0.710 / 1.863 / 0.353 |
| Leader pending-proposal mean | 1,534.367 | 1,545.333 |
| Leader queued-write mean | 597.767 | 535.967 |
| Leader persist-channel batches, mean | 10.433 | 9.400 |
| Leader owner persist-backlog batches, mean | 0.200 | 0.200 |
| Leader outstanding persist jobs, mean | 26.967 | 24.900 |
| Leader inbox events, mean | 12.600 | 20.967 |
| Peer queued DATA bytes, mean | 205,399.333 | 229,705.400 |
| Sum of follower inflight counts, mean | 341.633 | 331.367 |
| Leader fsync calls | 231 | 223 |
| Cohort ops / leader fsync | 1,284.197 | 1,341.807 |
| Issued persist jobs / leader fsync | 14.405 | 14.565 |
| Leader Write+Sync wall / fenced elapsed | 0.880 | 0.878 |

The larger count window removes sampled send-credit pauses without a sustained increase in pipeline depth or a material change in file-call wall occupancy. It changes the permitted peaks, but does not demonstrate that count credit was setting the measured steady throughput. File wall includes syscall wait/descheduling and is not device utilization or an exact critical-path decomposition.

## Paired changes and tails

For each rep, change = `100 * (candidate/main - 1)`. Medians below are medians of paired ratios, not ratios of the two independent medians above. Positive latency changes mean worse latency.

| Metric | Five changes %, rep1 through rep5 | Median % [minimum, maximum] |
| --- | --- | ---: |
| Cohort throughput | +1.480864, -0.618493, +1.673499, -0.166118, -1.346423 | -0.166118 [-1.346423, +1.673499] |
| Completion-window throughput | +1.545977, -1.014505, +1.358595, -0.555973, -1.613888 | -0.555973 [-1.613888, +1.545977] |
| Leader CPU/op | -1.504902, +3.370422, +0.119800, -0.852413, -1.265450 | -0.852413 [-1.504902, +3.370422] |
| All-server CPU/op | -2.395454, +4.425308, -0.931190, -1.214289, -1.290866 | -1.214289 [-2.395454, +4.425308] |
| Generator CPU/op | -0.774674, +3.014721, -1.384310, -3.709720, -1.932597 | -1.384310 [-3.709720, +3.014721] |
| p50 latency | -1.311475, +1.967213, -3.236246, -1.993356, -1.929260 | -1.929260 [-3.236246, +1.967213] |
| p99 latency | -10.561056, -2.768166, -1.433692, +6.083650, +20.938628 | -1.433692 [-10.561056, +20.938628] |
| p99.9 latency | -10.434783, -11.142061, +3.934426, +6.825939, +47.524753 | +3.934426 [-11.142061, +47.524753] |

Median per-run p50/p99/p99.9 values are main **39.977/73.138/79.954 ms**, candidate **39.453/73.138/83.100 ms**. Candidate rep5 p99.9 is 117.178 ms versus main 79.430 ms. Tail improvement is therefore not uniform; there is no demonstrated gain worth presenting without these ranges. Five short synthetic closed-loop repetitions are not a production-client result or a general proof that count credit cannot limit other workloads.

## Invalid etcd repetitions

| Rep | Arrivals | Successful replies | Revision delta | Failed replies | Drain s |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 34,908 | 33,519 | 34,908 | 1,389 connection-lost timeouts | 6.739124 |
| 2 | 32,037 | 31,152 | 31,810 | 883 connection-lost + 2 plain timeouts | 6.482835 |
| 5 | 32,444 | 30,599 | 32,444 | 1,845 connection-lost timeouts | 6.425944 |

Only reps3/4 pass. Reps1/5 have revision deltas matching arrivals despite failed replies; matching revision counts do not make the reply cohorts valid. Rep2 also has missing revision increments. Aggregate differences cannot attribute outcomes to individual requests. No passing-only etcd median or speedup is used here.

This report uses existing artifacts and offline Python arithmetic only. No local Go, server execution, new measurement, push or public post.
