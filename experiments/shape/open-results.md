# Replacement fixed-arrival audit

## Scope and verdict

Only replacement data under `../results/{open-fixed,extended-fixed,matched-fixed}` is used. These folders contain **27 dispatches, 830 planned windows and 166 five-repetition rate cells**, all mapped by `../repo/experiments/shape/campaign.json`. The source pin is `223f8f6aad7923ff186ac77a2c9fc29b9f140c86`, which includes the reviewed arrival-deadline fix. Older open/matched/extended/cap-open results remain superseded, not additional repetitions.

All 830 planned load records are present. **Tigerstore: 415/415 individually valid. Etcd: 408/415 valid.** Primary plus extended data have 295/295 Tigerstore and 288/295 etcd valid windows; matched data have 120/120 valid for each store. Seven etcd repetitions invalidate three primary rate cells. Their aggregate throughput/tails are withheld, not calculated from passing survivors. The two affected mixed-store Actions jobs still have failed conclusions, even though their Tigerstore windows pass individually.

The eight workload/ingress curves show distinct finite-cohort transitions. They do not establish steady-state capacity or an operational SLO. The corrected 5k point-read examples no longer contain the former 70-96 ms scheduled-latency artifact. Some new low-rate Put repetitions still have large **post-admission** tail outliers; these must not disappear behind median quantiles.

## Reproduction and validation

From this audit worktree:

```sh
python3 -B open-calculations.py \
  --results ../results \
  --campaign ../repo/experiments/shape/campaign.json \
  --output /tmp/tigerstore-open-audit-223f8f6a
```

This uses only Python's standard library and existing local files. It generates `calculations.json`, `windows.csv` and `cells.md`, preserving every external-manifest-planned repetition. JSON includes per-node cohort CPU, full five-run ranges, raw failures, fence checks and failed-window snapshot metric deltas. Invalid rate cells have no aggregate metrics. Paths are recorded relative to the supplied results path. No Go, network fetch, server, runner or new measurement is invoked.

I cross-checked manifest inputs against every plan and runner pin; target/rate/count/mode/size/connections; successful-reply counts and revision deltas; stable PIDs/start times and leadership; thread/generator affinities; balanced ticket allocation; empty response buffers at done; file-counter integrity; and recorded before/after persistence proofs. All 830 recorded native-status and persistence proofs pass. Validity fails only on the seven etcd reply cohorts, including two revision-count mismatches.

Every dispatch uses three-second fixed-arrival cohorts, one-second warmup, five repetitions, 18 connections and global-ticket round-robin dispatch. Put and point Range use global C4096; LIST uses C256, exactly 1000 keys with 1 KiB values and a 2 MiB response limit. Each window starts a fresh cluster. Leader/followers are pinned to CPUs0/1/2, generator and Python sampler to CPU3. These are synthetic ingress, not clientv3/Kubernetes observations.

All 27 dispatches have matching binary SHA-256 values:

| Binary | SHA-256 |
| --- | --- |
| Instrumented Tigerstore | 27e98ccdb7c1a2a54d9e52cac5b22638b2365c3c29938afcfda099bb4559abd0 |
| Fixed loadgen | 7c9ac7d18b9e46952f6d310a2dc6e4a038c610acd555302fe89c6b787fcd45f6 |
| Etcd 3.7.2 | 17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d |

The Tigerstore engine lineage is pinned main `e5c3327e...`, with experimental observation enabled; this is not a pristine-binary performance result. Runner/build pin and engine lineage must remain distinct.

## Denominators and knee convention

**Through-drain TP** = successful arrival-cohort replies / elapsed time from scheduled cohort start through final drain. The calculation independently verifies `shape.cohort_throughput`; completion-window ops/s is not substituted. Scheduled latency begins at each ticket's intended arrival and therefore includes admission queueing. The client metric named `service_latency_us` starts at admission, before socket flush; it includes client writer/transport/server/reply delay and is not server execution time.

The following conservative, explicit screen makes sparse-grid bookends reproducible:

- A repetition **tracks** if valid, through-drain TP is at least 98% of offered rate, and unsent-at-stop tickets divided by offered rate are at most 20 ms.
- A valid repetition is **clearly backlogged** if TP is below 95% of offered rate or that stop-admission lag exceeds 50 ms. This can detect drain/reply backlog even when all tickets were admitted before the stop.
- The lower bookend is the highest sampled primary/extended rate with **5/5 tracking**. The upper is the first higher rate with **5/5 clearly backlogged**. Mixed intermediate rates are shown separately. Failed cells cannot supply an all-valid upper capacity bound.

These thresholds are diagnostic conventions, not fitted physical limits or p99 SLOs. A 2% TP deficit corresponds to about 61 ms of drain in a fully successful three-second cohort. Tail-heavy boundary effects can fail that screen without accumulating an admission queue. Rates are sparse, hosts differ between dispatches, and results are not perfectly monotonic. The script exposes all rates and supports alternate tracking thresholds; do not present the brackets as precise capacity estimates or guaranteed inequalities.

Rates below are requests/s, not thousands unless explicitly written with commas. All numeric bookend cells have 5/5 valid repetitions.

| Workload / ingress | Store | Highest 5/5 tracking | Intermediate evidence | First 5/5 backlogged | Tentative transition bracket |
| --- | --- | ---: | --- | ---: | --- |
| 1 KiB Put, leader | Tigerstore | 80,000 | 95,000: 1/5 tracking, 4/5 backlogged | 100,000 | 80,000-100,000 |
| 1 KiB Put, leader | Etcd | 4,000 | 5,000: 4/5 tracking, 1/5 backlogged | 6,000 | 4,000-6,000 |
| 1 KiB Put, balanced | Tigerstore | 70,000 | 90,000: 3/5 tracking, 2/5 backlogged | 110,000 | 70,000-110,000 |
| 1 KiB Put, balanced | Etcd | 6,000 | 8,000/10,000: 4/5 tracking each; 12,000 invalid, 4/5 valid | unavailable | Failure-limited, not a measured all-valid bracket |
| 10 KiB Put, leader | Tigerstore | 10,000 | None | 12,000 | 10,000-12,000 |
| 10 KiB Put, leader | Etcd | 2,000 | None | 3,000 | 2,000-3,000 |
| 10 KiB Put, balanced | Tigerstore | 10,000 | None | 12,000 | 10,000-12,000 |
| 10 KiB Put, balanced | Etcd | 3,000 | 4,000 invalid, 3/5 valid; 5,000 invalid, 1/5 valid | unavailable | Failure-limited above 3,000 |
| Point Range, leader | Tigerstore | 125,000 | None | 150,000 | 125,000-150,000 |
| Point Range, leader | Etcd | 5,000 | 6,500: 4/5 tracking, 1/5 backlogged | 8,000 | 5,000-8,000 |
| Point Range, balanced | Tigerstore | 250,000 | 300,000: 3/5 tracking, 2/5 backlogged | 350,000 | 250,000-350,000, generator/host constrained |
| Point Range, balanced | Etcd | 16,000 | 20,000: 2/5 tracking, 2/5 clearly backlogged | 25,000 | 16,000-25,000 |
| 1000-key LIST, leader | Tigerstore | 700 | None | 900 | 700-900 |
| 1000-key LIST, leader | Etcd | 200 | 300: 2/5 tracking; tails already 81-358 ms | 400 | 200-400 |
| 1000-key LIST, balanced | Tigerstore | 900 | 1,200: 1/5 tracking, 4/5 backlogged | 1,500 | 900-1,500, generator constrained |
| 1000-key LIST, balanced | Etcd | 600 | 800: 4/5 tracking; tails 108-318 ms | 1,000 | 600-1,000 |

Etcd balanced 1 KiB Put needs special care: at 10k, through-drain TP/rate is **97.228-99.085%**, with zero stop-admission queue in all five repetitions. The conservative 98% all-five screen gives 6k as its lower marker, but does **not** establish a 6k engine ceiling. Its 12k cell has a failed reply cohort; neither a four-survivor median nor a failed-run success-only tail is a valid capacity/SLO headline.

### Through-drain levels at the bookends

Medians of the five complete cohorts, not steady-state capacity medians:

| Workload / ingress | Store | Lower rate: TP | Upper rate: TP | Upper scheduled p99 range, ms |
| --- | --- | --- | --- | --- |
| 1 KiB Put, leader | Tigerstore | 80k: 79,874.481 | 100k: 69,541.425 | 996.147-1,757.413 |
| 1 KiB Put, leader | Etcd | 4k: 3,982.410 | 6k: 5,101.317 | 987.759-1,581.253 |
| 1 KiB Put, balanced | Tigerstore | 70k: 69,700.850 | 110k: 92,255.529 | 296.747-891.290 |
| 1 KiB Put, balanced | Etcd | 6k: 5,986.942 | 12k: withheld, invalid | withheld |
| 10 KiB Put, leader | Tigerstore | 10k: 9,843.400 | 12k: 10,652.870 | 489.685-668.991 |
| 10 KiB Put, leader | Etcd | 2k: 1,996.464 | 3k: 1,443.822 | 481.296-4,068.475 |
| 10 KiB Put, balanced | Tigerstore | 10k: 9,976.635 | 12k: 10,944.937 | 443.548-564.134 |
| 10 KiB Put, balanced | Etcd | 3k: 2,984.408 | 4k: withheld, invalid | withheld |
| Point Range, leader | Tigerstore | 125k: 124,551.991 | 150k: 141,945.742 | 97.255-290.456 |
| Point Range, leader | Etcd | 5k: 4,997.536 | 8k: 5,671.749 | 1,514.144-1,874.854 |
| Point Range, balanced | Tigerstore | 250k: 249,139.586 | 350k: 304,627.415 | 389.022-576.717 |
| Point Range, balanced | Etcd | 16k: 15,967.073 | 25k: 17,864.013 | 1,119.879-1,379.926 |
| LIST, leader | Tigerstore | 700: 699.314 | 900: 792.368 | 673.186-740.295 |
| LIST, leader | Etcd | 200: 199.967 | 400: 299.699 | 1,430.258-2,172.649 |
| LIST, balanced | Tigerstore | 900: 899.138 | 1500: 1,086.464 | 1,203.765-1,354.760 |
| LIST, balanced | Etcd | 600: 598.799 | 1000: 838.809 | 543.162-870.318 |

The falling overloaded Put rates are retained, not replaced by the maximum from a more favorable rate/host. Three-second burst/cohort averages cannot identify a stable long-duration throughput plateau.

## Cohort CPU and the client ceiling

At each lower bookend, values are medians of per-run **through-drain CPU us per successful operation**, leader/follower1/follower2 order. They are not completion-window CPU divided by a different cohort count.

| Workload / ingress | Store | Offered rate | Server us/op L/F1/F2 | Generator us/op |
| --- | --- | ---: | --- | ---: |
| 1 KiB Put, leader | Tigerstore | 80,000 | 11.583 / 3.667 / 3.583 | 5.202 |
| 1 KiB Put, leader | Etcd | 4,000 | 243.333 / 70.833 / 65.833 | 44.830 |
| 1 KiB Put, balanced | Tigerstore | 70,000 | 13.810 / 11.571 / 11.381 | 7.010 |
| 1 KiB Put, balanced | Etcd | 6,000 | 147.778 / 130.556 / 121.667 | 44.963 |
| 10 KiB Put, leader | Tigerstore | 10,000 | 45.333 / 16.667 / 16.333 | 16.462 |
| 10 KiB Put, leader | Etcd | 2,000 | 455.000 / 168.333 / 150.000 | 79.822 |
| 10 KiB Put, balanced | Tigerstore | 10,000 | 64.000 / 51.333 / 47.000 | 19.384 |
| 10 KiB Put, balanced | Etcd | 3,000 | 260.000 / 210.000 / 191.111 | 65.544 |
| Point Range, leader | Tigerstore | 125,000 | 8.027 / 0.160 / 0.373 | 4.739 |
| Point Range, leader | Etcd | 5,000 | 200.000 / 22.667 / 22.667 | 42.826 |
| Point Range, balanced | Tigerstore | 250,000 | 3.787 / 3.480 / 3.453 | 3.902 |
| Point Range, balanced | Etcd | 16,000 | 59.583 / 55.208 / 54.583 | 28.902 |
| LIST, leader | Tigerstore | 700 | 1,419.048 / 38.095 / 47.619 | 929.555 |
| LIST, leader | Etcd | 200 | 3,983.333 / 266.667 / 266.667 | 1,344.395 |
| LIST, balanced | Tigerstore | 900 | 644.444 / 633.333 / 648.148 | 858.444 |
| LIST, balanced | Etcd | 600 | 1,427.778 / 1,400.000 / 1,400.000 | 1,033.454 |

CPU/op varies strongly with rate and batching, so low-rate costs cannot be extrapolated linearly into saturated capacity.

Balanced Tigerstore point reads consume **0.962-0.978 generator cores** across the 200k-350k samples. At 350k, median generator usage is 0.970 cores and leader/F1/F2 usage is **0.953/0.903/0.897**. At 250k the corresponding medians are **0.972 and 0.947/0.870/0.863**. CPU3 also runs the scraper, whose CPU is not included in generator process accounting. Thus the observed plateau is generator/host confounded, while the leader is also close to its own CPU budget. This does not isolate a sole generator bottleneck or reveal an unconstrained engine ceiling.

The balanced LIST case supplies stronger client-side evidence: at offered 1500, median generator usage is **0.953 cores** versus **0.490/0.453/0.453** on the servers, while stop-admission lag is 665-800 ms. Through-drain generator CPU is 877.683 us/op. An engine-only LIST-capacity claim from this plateau would be unjustified. Leader-only point Range and LIST instead show roughly one full leader core near their transitions.

## Latest same-rate latency examples

These use only `matched-fixed`, not the earlier defective generator. All listed cells are **5/5 valid**, with both stores tracking and no failed/unsent replies. TP is through drain. p99 is scheduled end-to-end latency; each cell is median [minimum, maximum] of five per-run p99 values, milliseconds, not a pooled percentile.

| Workload / ingress | Rate | Tigerstore TP | Etcd TP | Tigerstore scheduled p99 ms | Etcd scheduled p99 ms | Reported p99 <=10 ms, Tigerstore / Etcd |
| --- | ---: | ---: | ---: | --- | --- | --- |
| 1 KiB Put, leader | 5,000 | 4,998.542 | 4,991.378 | 3.465 [2.515, 21.037] | 42.074 [40.239, 77.857] | 4/5 / 0/5 |
| 1 KiB Put, balanced | 2,000 | 1,999.664 | 1,999.492 | 1.847 [1.716, 235.405] | 13.271 [9.339, 462.422] | 4/5 / 1/5 |
| 1 KiB Put, balanced | 5,000 | 4,997.991 | 4,995.354 | 3.400 [3.236, 32.965] | 33.948 [19.988, 95.683] | 3/5 / 0/5 |
| 10 KiB Put, leader | 2,000 | 1,999.306 | 1,993.529 | 2.613 [2.171, 2.875] | 42.074 [29.688, 44.433] | 5/5 / 0/5 |
| 10 KiB Put, balanced | 2,000 | 1,999.324 | 1,998.269 | 2.449 [2.367, 14.909] | 29.295 [16.089, 43.385] | 4/5 / 0/5 |
| Point Range, leader | 5,000 | 4,998.663 | 4,997.395 | 1.511 [1.503, 1.544] | 26.018 [23.790, 27.984] | 5/5 / 0/5 |
| Point Range, balanced | 5,000 | 4,999.674 | 4,999.003 | 1.397 [1.372, 1.430] | 7.586 [5.980, 9.339] | 5/5 / 5/5 |
| LIST, leader | 200 | 199.953 | 199.938 | 4.186 [3.793, 4.407] | 15.106 [14.647, 21.037] | 5/5 / 0/5 |
| LIST, balanced | 600 | 599.822 | 598.985 | 6.013 [5.620, 6.242] | 23.003 [17.367, 30.736] | 5/5 / 0/5 |

The 10 ms column is an **illustrative finite-cohort p99 screen**, not a promised SLO or an all-request deadline. LIST leader runs contain only 600 measured calls each; p99 and especially p99.9 have limited tail resolution. Even five passing histograms do not prove a stable long-duration SLO.

Median post-admission client p99 is **0.571 ms** for Tigerstore's leader 5k Range and **0.452 ms** for balanced 5k Range. Their mean scheduled-minus-admission latency is 0.369 and 0.155 ms, respectively. The remaining Put outlier at balanced 2k has scheduled and post-admission p99 both 235.405 ms, so these metrics do not support attributing that outlier to the old pre-admission timer bug. They do not locate its cause within client flush, transport or server execution either. Quantiles must not be subtracted to infer queueing; the reproducible calculation subtracts the corresponding means only.

All matched p50/p99.9, post-admission p99, CPU/node and queue ranges are retained in the generated JSON/CSV. No large outlier is removed.

## Failed etcd cells and fences

The only errors are gRPC Unavailable request timeouts, with plain and connection-lost classifications. There are **no reported too-many-requests rejects** in these seven repetitions. Exact raw records are under:

```text
open-fixed/38427533507/shape-put-1k-balanced-open/put-1k-balanced-open/etcd-5-r12000/
open-fixed/38427549648/shape-put-10k-balanced-open/put-10k-balanced-open/etcd-{rep}-r{rate}/
```

| Workload | Rate | Rep | Arrivals | Successful replies | Revision delta | Failed replies | Drain s |
| --- | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| 1 KiB balanced Put | 12,000 | 5 | 36,000 | 34,385 | 36,000 | 1,615 connection-lost timeouts | 7.117101 |
| 10 KiB balanced Put | 4,000 | 3 | 12,000 | 10,280 | 12,000 | 1,720 connection-lost timeouts | 6.462170 |
| 10 KiB balanced Put | 4,000 | 5 | 12,000 | 10,679 | 12,000 | 1,321 connection-lost timeouts | 7.198507 |
| 10 KiB balanced Put | 5,000 | 1 | 15,000 | 13,521 | 14,999 | 1,479 connection-lost timeouts | 11.999785 |
| 10 KiB balanced Put | 5,000 | 3 | 15,000 | 13,537 | 14,996 | 1,463 connection-lost timeouts | 10.593894 |
| 10 KiB balanced Put | 5,000 | 4 | 15,000 | 13,464 | 15,000 | 656 plain + 880 connection-lost timeouts | 8.912884 |
| 10 KiB balanced Put | 5,000 | 5 | 15,000 | 14,989 | 15,000 | 11 plain timeouts | 13.062240 |

The native applied/commit/revision equality and recorded post-WAL.Save gauge fences pass before and after each failed repetition. Processes and leadership/terms stay stable; recorded sampling errors are empty. Therefore these are not missing or unfenced windows, but **failed reply cohorts**, sometimes with committed writes whose replies failed. Aggregate revision accounting does not identify individual ambiguous requests.

Six failures have a follower snapshot-receive success delta of one and snapshot-DB-save count delta of one, alongside the corresponding leader snapshot-send success. The final 10 KiB 5k rep5 has no such snapshot delta and only eleven plain timeouts. Proposal-failure counter deltas match failed replies. Leader-change counter deltas are zero. These metric deltas are reproduced in the offline JSON from before/after Prometheus text.

Snapshot catch-up and timeout classifications are evidenced; an external network outage or the initial follower-lag cause is not. The pinned-source mechanism and its uncertainty limits remain as documented in `audit-etcd-overload.md`. Neither matching revision deltas nor eventual recovery turns a timed-out reply cohort into a valid SLO run.

No passing-only capacity median, Go execution, new measurement, push or public post was performed for this audit.
