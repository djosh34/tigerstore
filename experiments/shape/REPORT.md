# Before #50: throughput limits and Kubernetes-shaped requests

Nothing in this experiment was merged. The branch is [`exp/pre-50-shape`](https://github.com/djosh34/tigerstore/tree/exp/pre-50-shape).

The next write bottleneck is not uniformly the core owner. Large write batches spend much of their time in the durable append pipeline and the shared runner disk. Point reads remain CPU-bound on the leader. A 1000-key LIST spends most of its leader CPU in the response writer. Balanced LIST reaches the one-core generator limit before establishing a server ceiling.

A smaller write-packing target is worth further work. A 1 MiB target improved large-backlog throughput by about 5% to 20%, depending on input shape, and improved the tested 10 KiB case by about 10%. It is not a general default recommendation. Some low-concurrency controls regressed, tails did not consistently improve, and one valid fixed-rate candidate window had a 7.365-second p99.

## Sources and conditions

- Pinned main: [`e5c3327e3800edf64f86ed8c1971ba7ebc1e8342`](https://github.com/djosh34/tigerstore/commit/e5c3327e3800edf64f86ed8c1971ba7ebc1e8342), PR #86.
- Etcd v3.7.2: [`68c065e562994b89e333e77b039ad066f933c586`](https://github.com/etcd-io/etcd/commit/68c065e562994b89e333e77b039ad066f933c586). The official ARM binary SHA-256 is `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d`.
- Main and loadgen used Go 1.27.2. The official etcd binary used Go 1.26.8. This is a released-binary comparison, not a same-compiler rebuild of both stores.
- Every measurement used free `ubuntu-24.04-arm` Actions runners with four Neoverse-N2 CPUs. Stores ran back to back on each VM. The order reversed between repetitions.
- Leader CPU0, followers CPU1 and CPU2, generator and sampler CPU3. Server `GOMAXPROCS=2`, generator `GOMAXPROCS=1`, `GOMEMLIMIT=2GiB`. Recorded thread affinities and process start times validate this placement.
- Three replicas shared one virtual ext4 device and loopback transport. Client and peer TLS stayed enabled. The runner remounted with `barrier=1`; no run disabled fsync. The recorded mount also had `data=writeback,journal_async_commit,commit=30`.
- Each window used a fresh cluster, seeded data and a drained warmup. The ordinary physical-counter measurements required all replicas at the target revision/index and fresh persistence proofs before and after the cohort.

These are direct etcd-API workload proxies. They do not measure kube-apiserver, its watch cache, watches mixed with writes, transactions, leases, production object distributions, regional latency or long-lived history. The 1 KiB and 10 KiB values contain independent data blocks, rather than one compressible block repeated to fill a value. Most numbers use the instrumented main-derived executable; the observation control below measures its effect.

The [campaign manifest](campaign.json) records all 67 substantive dispatches with full commits, inputs, result folders and dispositions. It also identifies two excluded pilots. The important measured source pins are:

| Purpose | Source commit | Actions run |
| --- | --- | --- |
| Closed Put/Range and LIST matrix | `7043fcd6a83a76b3a22753d54c8aca2739286e50` | [closed](https://github.com/djosh34/tigerstore/actions/runs/38412800347), [LIST](https://github.com/djosh34/tigerstore/actions/runs/38412803420) |
| Port-only setup retry, client control, profiles | `99cd39439b62ccfb80792f36f51d4a5e98c23797` | [retry](https://github.com/djosh34/tigerstore/actions/runs/38415476997), [client](https://github.com/djosh34/tigerstore/actions/runs/38415480337), [CPU](https://github.com/djosh34/tigerstore/actions/runs/38415945699), [trace](https://github.com/djosh34/tigerstore/actions/runs/38415950650) |
| Initial byte-target controls | `b2994a4b68efde0f4f20d9e4bfe9796d42e1db1a` | [38418280627](https://github.com/djosh34/tigerstore/actions/runs/38418280627) |
| Static byte-target and inflight controls | `4ee913fbb7f799070c36af37de7e9aafbd696d49` | [static](https://github.com/djosh34/tigerstore/actions/runs/38421648705), [inflight](https://github.com/djosh34/tigerstore/actions/runs/38421653015) |
| Pristine/disabled/enabled observation control | `a0d88e2e69593332ee64ac10f3d553dccdcdec4e` | [Put/Range](https://github.com/djosh34/tigerstore/actions/runs/38424937611), [LIST](https://github.com/djosh34/tigerstore/actions/runs/38424941260) |
| All accepted fixed-arrival results | `223f8f6aad7923ff186ac77a2c9fc29b9f140c86` | 28 replacement runs in the manifest |

The last pin includes the generator timing fix. Its instrumented Tigerstore SHA-256 is `27e98ccdb7c1a2a54d9e52cac5b22638b2365c3c29938afcfda099bb4559abd0`; loadgen is `7c9ac7d18b9e46952f6d310a2dc6e4a038c610acd555302fe89c6b787fcd45f6`. Each artifact records its binaries and build metadata. `vcs.modified=true` records untracked `bin/` and `results/`, not an undisclosed tracked-source patch. Candidate builds have explicit one-constant overlays and their own hashes.

## What the numbers mean

Unless stated otherwise, throughput is successful cohort replies divided by time from cohort start through final drain. CPU uses that successful cohort and its separately recorded CPU clock. Closed-loop arrival windows lasted three seconds; LIST windows lasted six seconds. Every headline cell has five repetitions. Latencies are medians of the five per-window quantiles, not pooled percentiles. The loadgen histogram has buckets at most 0.8% wide.

The complete [closed tables](report-data/closed.md), [fixed-arrival curves](report-data/open.md), [same-rate comparisons](report-data/matched.md) and [per-window CSV](report-data/all-current-windows.csv) include p50, p99, p99.9, CPU for every node, generator CPU and queue measurements. JSON tables retain full five-window ranges. Invalid cells have no numerical aggregate; the CSV retains their raw rows.

Do not mix this throughput with completion-window ops/s. For example, etcd's leader-only 10 KiB C4096 case completed 607/s inside the three-second window but 947/s over the full cohort and drain. Its cohort leader CPU was 1047 us/op, not the larger window-only estimate. Neither is a long-duration steady-state result.

Physical counters cover the externally fenced interval. Puts/fsync means cohort Puts per physical WAL sync call, including WAL metadata/header calls and excluding directory sync. It does not mean Puts per Raft entry. Etcd backend commits remain separate. Fsync histogram quantiles are bucket upper bounds. File-call duty and device bandwidth use the fenced clock, which includes administrative fence time. Owner/peer gauges and saved client-queue samples have a nominal 100 ms cadence. The generator updates its peak queue at every dispatcher turn. Neither method gives a continuous maximum.

### Closed-loop results

These are cohort ops/s. `L` sends every request to the leader; `B` assigns request tickets round robin; endpoint totals differ by at most one. Approximately two thirds of balanced requests enter through followers.

| Workload | Ingress | Tigerstore C256 / C1024 / C4096 | Etcd C256 / C1024 / C4096 |
| --- | --- | --- | --- |
| Put, 1 KiB | L | 56,051 / 74,315 / 79,379 | 5,135 / 5,302 / 5,071 |
| Put, 1 KiB | B | 41,878 / 75,536 / 95,001 | 8,574 / 10,674 / invalid, 3/5 valid |
| Put, 10 KiB | L | 10,238 / 9,267 / 10,546 | 2,991 / 1,852 / 947 |
| Put, 10 KiB | B | 10,629 / 10,919 / 8,972 | 2,924 / 2,071 / 1,238 |
| Point Range, 1 KiB | L | 72,737 / 123,771 / 147,280 | 6,320 / 6,631 / 6,005 |
| Point Range, 1 KiB | B | 106,740 / 214,609 / 299,098 | 16,977 / 22,806 / 17,656 |

Different concurrency cells ran on different VMs. Do not interpret every cross-cell difference as a causal effect of concurrency. Full ranges matter: the valid leader 1 KiB C256 Tigerstore windows span 33,347 to 57,715/s, and balanced 10 KiB C1024 spans 5,447 to 11,008/s. No outlier was removed.

Representative Tigerstore costs and tails:

| Case | p50 / p99 / p99.9 ms | CPU us/op L/F1/F2 | Leader cores | Puts/fsync on leader | Mean leader fsync ms |
| --- | --- | --- | ---: | ---: | ---: |
| 1 KiB Put, L, C256 | 4.375 / 10.191 / 14.647 | 16.49 / 5.94 / 5.35 | .93 | 73.3 | .877 |
| 1 KiB Put, L, C1024 | 13.337 / 23.790 / 32.571 | 10.73 / 3.10 / 3.05 | .80 | 295.6 | 2.828 |
| 1 KiB Put, L, C4096 | 50.725 / 74.187 / 79.430 | 8.13 / 2.51 / 2.50 | .65 | 1242.9 | 10.655 |
| 1 KiB Put, B, C4096 | 40.501 / 80.478 / 104.071 | 7.94 / 6.29 / 6.25 | .74 | 1207.3 | 11.317 |
| 10 KiB Put, L, C256 | 24.838 / 38.404 / 44.171 | 44.74 / 17.82 / 17.82 | .46 | 76.9 | 6.042 |
| 10 KiB Put, B, C4096 | 437.256 / 731.906 / 773.849 | 34.15 / 23.55 / 23.45 | .31 | 1225.9 | 123.727 |

High concurrency buys grouping, not cheap latency. At 10 KiB, large physical groups can make individual fsyncs much slower without improving useful throughput.

LIST returns exactly 1000 keys, each with a 1024-byte value. The client checks every key/value, count and `more` flag. We tested C1, C16, C64 and C256, not C4096, which could retain 3.91 GiB of value payload alone, before reply framing, buffers and runtime overhead. The 2 GiB Go memory limit is soft, not a hard process or aggregate-reply cap.

| LIST ingress | Tigerstore C1 / C16 / C64 / C256 | Etcd C1 / C16 / C64 / C256 |
| --- | --- | --- |
| Leader | 514 / 760 / 753 / 854 | 243 / 312 / 294 / 324 |
| Balanced | 559 / 1061 / 1101 / 1084 | 237 / 642 / 833 / 961 |

At leader C64, Tigerstore p50/p99/p99.9 was 77.857/207.094/264.765 ms and leader CPU was 1327 us/LIST, one full core. Balanced C64 used .97 generator cores; its server CPU was .59/.56/.57 cores. All primary point-Range and LIST windows had zero measured WAL fsyncs in both stores. Tigerstore also had zero measured WAL file writes. The etcd metrics do not expose an equivalent WAL Write-call counter.

### Fixed arrivals and queue growth

The generator schedules arrivals independently of replies, retains every scheduled ticket and measures latency from the intended arrival. Its concurrency ceiling is 4096 for Put/point Range and 256 for LIST. A full ceiling delays admission rather than dropping tickets. Scheduled latency includes that queue; the metric called `service_latency_us` begins at admission before socket flush, so it is not server execution time.

The [independent open audit](open-results.md) defines a reproducible screen. A repetition tracks if throughput reaches 98% of offered rate and stop-admission lag is at most 20 ms. It is clearly backlogged below 95% throughput or above 50 ms lag. The following are sparse-grid bookends, not precise capacity inequalities or SLOs.

| Workload / ingress | Tigerstore: highest 5/5 tracking; first 5/5 backlogged | Etcd: corresponding evidence |
| --- | --- | --- |
| 1 KiB Put, leader | 80k; 100k. At 95k only 1/5 tracks. | 4k; 6k. At 5k, 4/5 track. |
| 1 KiB Put, balanced | 70k; 110k. At 90k, 3/5 track. | 6k tracks all five. 10k reaches 97.23% to 99.09% with no admission queue; 12k has a failed cohort. No all-valid upper bound. |
| 10 KiB Put, leader | 10k; 12k | 2k; 3k |
| 10 KiB Put, balanced | 10k; 12k | 3k tracks all five. 4k and 5k have failures; no all-valid upper bound. |
| Point Range, leader | 125k; 150k | 5k; 8k |
| Point Range, balanced | 250k; 350k. At 300k, 3/5 track. | 16k; 25k |
| LIST, leader | 700; 900 | 200; 400 |
| LIST, balanced | 900; 1500. At 1200, 4/5 backlog. | 600; 1000 |

Overloaded throughput is not monotonic across these short runs. In the extension job, Tigerstore at offered 100k leader Puts delivered only 69,541/s through drain, with p99 between .996 and 1.757 seconds. Do not replace that result with the roughly 95k seen on a different runner. Three-second cohorts and shared virtual storage do not establish one sustainable maximum.

Balanced point Range reaches both the generator and leader CPU budgets. At offered 350k, median cohort generator usage was .970 cores and server usage .950/.900/.897. It is not a clean server-only ceiling. Balanced LIST gives stronger generator evidence: at offered 1500, cohort generator usage was .949 cores while servers used .475/.439/.446, delivering about 1086 LIST/s through drain.

### Same-rate latency examples

These use only the corrected generator. Every cell below has five valid cohorts. Numbers are median p99 followed by the full five-window range, in milliseconds.

| Request / ingress | Offered/s | Tigerstore p99 | Etcd p99 |
| --- | ---: | --- | --- |
| 1 KiB Put, leader | 5000 | 3.465 [2.515, 21.037] | 42.074 [40.239, 77.857] |
| 1 KiB Put, balanced | 5000 | 3.400 [3.236, 32.965] | 33.948 [19.988, 95.683] |
| 10 KiB Put, leader | 2000 | 2.613 [2.171, 2.875] | 42.074 [29.688, 44.433] |
| 10 KiB Put, balanced | 2000 | 2.449 [2.367, 14.909] | 29.295 [16.089, 43.385] |
| Point Range, leader | 5000 | 1.511 [1.503, 1.544] | 26.018 [23.790, 27.984] |
| Point Range, balanced | 5000 | 1.397 [1.372, 1.430] | 7.586 [5.980, 9.339] |
| LIST, leader | 200 | 4.186 [3.793, 4.407] | 15.106 [14.647, 21.037] |
| LIST, balanced | 600 | 6.013 [5.620, 6.242] | 23.003 [17.367, 30.736] |

Median latency is not a promise. Only three of five balanced 1 KiB 5k Put windows had p99 at or below 10 ms. At balanced 2k Puts, one valid Tigerstore window had p99 235.405 ms; etcd reached 462.422 ms. Those outliers remain in the tables. The Tigerstore outlier also had post-admission p99 235.405 ms, so the old pre-admission timer bug does not explain it. Its location within client flush, transport or server execution is not isolated.

## Which resource limits each workload?

| Workload | Evidence | Most useful next experiment and gain estimate |
| --- | --- | --- |
| 1 KiB Put, modest concurrency | Leader CPU near one core; frontend reader/writer account for 8.36 of 15.17 sampled CPU us/op at C256. Input shaping changes the cost. | Measure frontend/socket batching changes with the same generator discipline. The client-only control already changed throughput by 21%, so do not label that a server optimization. |
| 1 KiB Put, large leader backlog | Leader CPU falls to .65 cores; the median per-window sampled leader persist-channel peak is one batch. The lower byte target increases persistence depth and physical grouping. | A 1 MiB packing target gave 20.22% paired cohort gain in the global C4096 control, 5.47% with static dispatch and about 5% at the overloaded fixed rates. These are measured, scoped gains, not a universal 20%. |
| 1 KiB Put, balanced backlog | The write pipeline is already deeper. Raft count credit becomes full, but removing sampled count stalls does not improve throughput. | Work on the durable byte path and shared-I/O cost rather than increasing the message count. The count control's paired median gain was -0.17%. No measured software change lifted this case materially. |
| 10 KiB Put | Leader CPU is only .3 to .5 cores in closed cells. WAL call duty is high; individual file writes and fsyncs can be long. Throughput is roughly a tenth of the 1 KiB high-backlog rate. | The 1 MiB target gave 9.61% in the tested leader C256 case. More independent disk bandwidth is the hardware direction. Compression requires real compressible data and a separate CPU/format experiment; no compression gain was measured here. |
| Point Range, leader | One leader core; sampled frontend reader/writer 5.56 of 8.49 us/op at C1024, owner 1.57, inclusive Raft only .068. | A specialized point-response path is a plausible target without weakening ReadIndex. Removing 1 to 2 us would imply a 13% to 31% CPU-only throughput gain at that cost. This is arithmetic, not a measured optimization. |
| Point Range, balanced | Leader and generator both near one core. | First supply more generator capacity on a separately authorized runner setup. A faster server alone may produce little gain under the current CPU3 limit. No isolated engine gain estimate is justified. |
| LIST, leader | Response writer accounts for 1018 of 1285 sampled us/LIST; inclusive TLS 674 and memmove 214 overlap. | Reduce response construction/copies, not consistency. Saving 100 to 250 us implies about 8% to 24% CPU-only rate gain. The saving is a hypothesis and must be measured; the whole TLS stack is not removable encryption work. |
| LIST, balanced | Generator near one core while each server uses roughly half a core at overload. | More generator capacity is required before estimating the engine ceiling. Do not call the roughly 1100 LIST/s plateau a server limit. |

The bandwidth conclusion is about this shared virtual disk and these byte volumes, not a proven provisioned device limit. Recorded per-device and per-node counters remain in the CSV. Measurements span different VMs and have large valid outliers.

## Controlled changes and diagnostics

### Client discipline matters

The global writer supplies the exact concurrency ceiling, fixed arrivals and request-ticket round robin. It does not reproduce the original per-connection worker's bursts. With 16 connections and otherwise matched controls, the static/global paired cohort-throughput changes were +21.42% [17.90, 23.06] at C256, +6.83% [-.14, 10.35] at C1024 and +2.09% [-5.88, 7.44] at C4096. Brackets show the full five-pair percentage range. The C1024/C4096 ranges include negative pairs. Neither generator saturated its core. A non-saturated generator can still change server work and grouping.

Static C256 landed near 63k/s, but it is not an exact reproduction of the earlier #74 setup. Do not compare the new global C256 result directly with the old 63k figure or call the difference an engine regression. See the [client-control audit](client-control-report.md).

### The byte target changes pipeline occupancy

The candidate changes only `DefaultConfig.MaxBatchBytes` from 4 MiB to 1 MiB. `queueWrite` already proposes a nonempty batch when the next request would cross the target, even while a local append is outstanding. The change adds no timer. It does not move apply/ack before sync, split an oversized request, lower the independent 1.5 MiB `InternalRaftRequest` admission limit or change WAL/peer formats.

The focused witnesses cover oversized typed/wire Puts, no early acknowledgement, ordered persistence and durable replay. Runner controls also ran short core/node/server/lint tests; confirmations added short simulation tests. This is not a full new three-node crash campaign or approval to merge.

In the initial leader C4096 control, medians of the per-window sampled means changed from 994 to 413 queued writes and from 1.43 to 4.17 outstanding persistence jobs. Median jobs per physical fsync rose from 1.10 to 2.31. Median physical fsync calls stayed near 197 versus 195, while Puts/fsync rose from 1193 to 1460. Fenced leader file-call duty rose from .724 to .848 and device busy fraction from .552 to .724. Mean fsync cost and CPU/op increased. This supports better pipeline occupancy, not cheaper operations.

Paired 1 MiB/main cohort-throughput changes:

| Control | Median gain % | Full five-pair range % |
| --- | ---: | --- |
| Global leader 1 KiB C4096 | 20.224 | 15.284 to 22.362 |
| Static leader 1 KiB C4096 | 5.473 | 5.359 to 13.446 |
| Global leader 10 KiB C256 | 9.606 | 8.279 to 17.113 |
| Fixed arrivals, 95k/s | 2.385 | -71.247 to 5.683 |
| Fixed arrivals, 110k/s | 5.467 | 4.030 to 6.372 |
| Fixed arrivals, 125k/s | 5.335 | 2.108 to 8.551 |

The 256 KiB target added depth without a clear further throughput benefit. Balanced traffic already had a deep pipeline and gained essentially nothing. Low-concurrency 1 KiB controls sometimes regressed even where the packing trigger was inactive; that regression cannot be attributed to the byte-trigger mechanism.

The corrected fixed-rate control is not a latency guarantee. At offered 95k, candidate repetition 4 was valid but delivered only 27,070/s versus the paired main's 94,149/s, with p99 7.365 seconds, p99.9 7.533 seconds, 95,434 queued tickets at arrival stop and 7.528 seconds of drain. Each replica accumulated roughly 9.34 seconds of fsync wall time: 9.340, 9.358 and 9.365 seconds. No error or revision failure justifies excluding it. Its cause is not isolated. At 110k and 125k, all five throughput pairs favored the candidate by medians 5.47% and 5.33%, while arrival queues remained large.

See [initial controls](cap-results.md), [static confirmation](cap-confirmation.md) and [corrected fixed-rate validation](fixed-arrival-validation.md).

### No measured throughput gain from more Raft count credit

The count-only control changed `MaxInflightMsgs` from 256 to 1024 and kept the byte bound unchanged. Main sampled full count credit in 30 of 150 owner publications at balanced C4096. The candidate sampled none; its observed counts reached 300 to 429, above the old limit. Yet its paired cohort throughput change was -0.166%, with a range of -1.346% to +1.673%. p99.9 sometimes worsened.

The intended C1024 job never measured anything: server tests stopped leaders after disk-stall watchdog events. All 15 intended windows remain missing in the external dispatch plan. The failure is not attributed to the count change, and the job was not rerun to obtain a passing result. See [the inflight audit](inflight-results.md).

### Profiles support the distinction between CPU and waiting

The CPU diagnostic has nine valid Tigerstore cohorts. One paired etcd cell failed, so it is not a complete passing comparison. Six-second CPU captures surround five-second cohorts; sampled CPU/success includes the capture margins. The three traces cover only two seconds in the middle of four-second cohorts.

Across 1 KiB C256, 1 KiB C4096 and 10 KiB C256 leader traces, the WAL goroutine spent about 87%, 68% and 94% of the capture in the syscall state. The owner spent 1.262, .904 and 1.763 seconds waiting in its select. These are goroutine wall states, not additive request latency or pure device latency. One 10 KiB file-write span lasted 255 ms; fsync is not the only long WAL syscall.

The trace scheduler profile attributes wakeups to wakers. The report instead reconstructs the subject goroutine's runnable intervals. Directory syncs are separate from observed WAL-file sync spans. Full methods and integer calculations are in the [profile report](profile-report.md) and [analysis directory](profile-analysis/).

### Observation is not free

The observation control compared pristine main, the instrumented binary with observation disabled, and exactly the same binary with it enabled. All 120 windows were valid. These diagnostics used a common native applied/revision fence and .3-second cooldown for the Tigerstore variants, not the stronger all-disk-job proof. Their summaries deliberately suppress physical-counter claims.

Median paired enabled/disabled throughput changes were -5.05% for 1 KiB C256, -1.21% for 1 KiB C4096, -.15% for 10 KiB C256, -1.63% for point Range, -1.47% for leader LIST and -2.05% for balanced LIST. CPU and tail pairs had mixed signs and large outliers. There is no general tail-overhead bound. This measures the combined observer, HTTP scraping and CPU3 sampler effect, not just in-server counters. Disabled versus pristine also includes compiled-hook/layout differences. Do not apply one correction factor to all headline numbers. See [the control report](observation-control-report.md).

## Failures and corrections retained

1. The original Range leader C4096 job failed all ten startups because port 40030 was already in use. We moved listeners below the ephemeral range and repeated that setup-only cell. The exact original port owner is unproved. Both attempts remain archived.
2. The first fixed-arrival implementation could sleep until a 100 ms fallback after a flush crossed the next arrival deadline. All 28 old open/matched/extended/cap-open dispatches are superseded, not extra repetitions. The fix preserves the pre-flush admission stop reason and retains an overdue arrival deadline. It does not busy-spin through genuine flow-credit or concurrency pressure. All accepted fixed-rate results use commit `223f8f6a`.
3. The replacement fixed campaign has 875 planned records, 868 valid cohorts and seven etcd failures. All 445 Tigerstore/candidate cohorts are valid. Three etcd rate cells are invalid: balanced 1 KiB at 12k, balanced 10 KiB at 4k and at 5k. No survivor-only aggregate appears in the headline tables.
4. Six of those seven failures coincide with snapshot receive/save deltas. The last has only eleven plain timeouts and no snapshot delta. Some failed replies correspond to committed writes. Neither eventual recovery nor matching aggregate revision counts makes a timed-out cohort valid. Initial follower-lag causes and external-network causes are not established. The earlier closed-loop snapshot/admission analysis is in [the etcd audit](etcd-overload.md).
5. Valid outliers remain, including the candidate's 7.365-second p99 and the low-rate Put tails above. Five valid repetitions are not five SLO passes.
6. Across all 39 non-retired runs, the CSV contains 1494 planned rows: 1455 valid, 14 invalid actual records and 25 missing records. The missing records are the ten original startup failures and fifteen build-gated control windows. The 28 retired runs remain separately archived.

## Reproduce and inspect

[README.md](README.md) gives extraction and offline recalculation commands. The [manifest](campaign.json) supplies exact historical workflow inputs and commits. Measured artifacts contain `commands.json` with the actual cluster/loadgen command lines, plus placement, build identity, load output, audits, samples and logs. A job that failed before measurement retains only the available build/test evidence and its offline expected plan. Explicit offline `expected-plan.json` files also retain jobs that failed before the runner could create its own plan.

No measurement ran locally. Local Go use was limited to correctness tests, vet/lint and offline profile decoding under the requested resource limits. The experiment retains failed and superseded evidence, rather than rerunning a failure until it disappears. No server optimization from these controls is merged.
