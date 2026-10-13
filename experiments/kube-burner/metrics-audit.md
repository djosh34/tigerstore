# Pilot metrics audit

Evidence: `/home/joshazimullah.linux/work_mounts/scratch/kube-burner/runs/38420832209`.

Read all four whole-window `metrics.json` files and all 16 phase `diff.json` files. Independently parsed the raw Prometheus responses and endpoint status JSON, and recalculated counter deltas, inferred baselines, status totals, histogram buckets and quantiles, monotonic rate intervals, and CPU from raw `/proc/PID/stat`. All 20 windows matched. No calculation bug found; no collector edits made.

## Whole-window checks

API values exclude WATCH and CONNECT. RPC rates are client-observed apiserver storage RPCs, not server counters. CPU is the `external-etcd-1` main PID only.

| Store / runner | API count | API/s | API p50 ms | API p99 ms | Storage RPC/s | Main-PID CPU seconds | Revision delta per replica |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| etcd / X64 | 12,928 | 54.464 | 4.422 | 194.362 | 77.609 | 20.31 | 11,758 |
| Tigerstore / X64 | 12,856 | 56.659 | 3.913 | 192.906 | 81.018 | 10.24 | 11,689 |
| etcd / ARM64 | 12,853 | 55.955 | 4.162 | 182.145 | 79.743 | 20.26 | 11,710 |
| Tigerstore / ARM64 | 12,790 | 56.980 | 3.556 | 146.859 | 81.402 | 14.14 | 11,677 |

For etcd/X64, raw CPU ticks rose from `146 + 89` to `1205 + 1061`. At 100 ticks/second this is 20.31 CPU seconds over 237.4384966245 seconds, or 0.085537940514 average cores. PID, container ID, start-time ticks, and container start time matched. CPU identities matched in all audited windows.

The etcd/X64 overall API p99 falls between 0.1 and 0.2 seconds. Their cumulative deltas are 12,543 and 12,814, with 12,928 total observations. Interpolation gives `0.1 + 0.1 * (0.99 * 12928 - 12543) / 271 = 0.194361623616` seconds. Storage count delta 18,422 divided by the API scrape interval 237.3685401195 seconds gives 77.609273709 RPC/s. Endpoint revisions rose from 639 to 12,397 on each replica; do not sum those replica deltas.

HTTP 4xx/5xx totals matched raw status counters: etcd/X64 226, Tigerstore/X64 216, etcd/ARM64 206, Tigerstore/ARM64 193. These include background traffic, not just workload errors. API process start times matched. The whole-window request summaries infer 98 new series for each etcd run and 100 for each Tigerstore run, with explicit inference flags. Stock-etcd exporter start times also matched.

## Interpretation limits

- Resource plus verb groups include subresources. For etcd/X64, POST pods has 354 observations: 186 pod-create requests and 168 binding requests. Its grouped p50/p99 are 20.385/386.500 ms. Do not label that group as pure pod-create latency or individual object latency.
- Tigerstore returned HTTP 404 for all server metric scrapes. Server counters remain null and unsupported; API RPC and revision measurements remain valid. The alternate `apiserver_etcd_request_duration_seconds` family is absent in all four runs; the actual `etcd_request_duration_seconds` family is present.
- This audit validates telemetry arithmetic, not workload equivalence. The parent is correcting overlapping delete selections before final repetitions. Pilot rates include background traffic and snapshot collection overhead, so they are not final benchmark conclusions.

Only local evidence reads and arithmetic ran. No clusters, builds, or issue posts.

## Final 12-run audit

Evidence: `/home/joshazimullah.linux/work_mounts/scratch/kube-burner/runs/38422256112`. Audited only the 12 whole-window `metrics.json` files and their before/after snapshots. Also checked the parent's `evidence/kube-burner/final-38422256112/summary` whole-window rows and spreads.

Independent raw-counter, histogram p50/p99, rate, revision, and CPU calculations matched every reported value. All host boot IDs, API exporter start times, stock-etcd exporter start times, and sampled process identities remained unchanged. No counter regressions, collection failures, or unexpected nulls were found. Expected nulls are zero-observation histogram groups, the absent alternate storage metric family, and Tigerstore's HTTP-404 server telemetry. Summary rows omit these nulls rather than replacing them with zero.

The CPU columns below cover the three external container main PIDs, not the control-plane PID or whole cgroups. Summed cores add each node's CPU seconds divided by its own monotonic sample interval.

| Store / runner / rep | API count | API/s | API p99 ms | Storage RPC/s | CPU seconds, nodes 1 / 2 / 3 | Sum CPU seconds | Sum cores |
| --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
| etcd / ARM64 / 1 | 12,736 | 56.700 | 188.117 | 81.141 | 14.54 / 18.50 / 15.71 | 48.75 | 0.2170 |
| etcd / ARM64 / 2 | 12,730 | 55.867 | 259.811 | 80.022 | 15.41 / 19.57 / 15.99 | 50.97 | 0.2236 |
| etcd / ARM64 / 3 | 12,796 | 56.189 | 193.294 | 80.313 | 16.25 / 20.65 / 17.90 | 54.80 | 0.2406 |
| Tigerstore / ARM64 / 1 | 12,763 | 57.010 | 160.761 | 81.595 | 9.32 / 13.53 / 10.32 | 33.17 | 0.1481 |
| Tigerstore / ARM64 / 2 | 12,791 | 56.422 | 174.174 | 80.709 | 15.38 / 10.55 / 11.43 | 37.36 | 0.1648 |
| Tigerstore / ARM64 / 3 | 12,795 | 57.537 | 143.428 | 82.328 | 9.52 / 13.92 / 9.88 | 33.32 | 0.1498 |
| etcd / X64 / 1 | 12,796 | 55.621 | 248.219 | 79.533 | 15.82 / 15.37 / 18.93 | 50.12 | 0.2178 |
| etcd / X64 / 2 | 12,864 | 53.832 | 313.888 | 76.773 | 17.16 / 21.53 / 17.92 | 56.61 | 0.2368 |
| etcd / X64 / 3 | 12,860 | 55.794 | 567.191 | 79.639 | 15.47 / 13.37 / 12.84 | 41.68 | 0.1808 |
| Tigerstore / X64 / 1 | 12,838 | 56.068 | 198.267 | 80.089 | 10.40 / 7.71 / 8.33 | 26.44 | 0.1154 |
| Tigerstore / X64 / 2 | 12,801 | 54.352 | 247.059 | 77.633 | 15.30 / 10.99 / 11.55 | 37.84 | 0.1606 |
| Tigerstore / X64 / 3 | 12,799 | 56.014 | 200.019 | 80.072 | 8.48 / 11.93 / 8.05 | 28.46 | 0.1245 |

Flags for interpretation, not calculation failures:

- API count is the request-counter delta. Raw histogram observations differ by one in two runs. etcd/X64/1 has 12,797 histogram observations versus 12,796 requests, in GET `openapi/v3`. Tigerstore/X64/2 has 12,800 versus 12,801, in GET `/readyz`. Both summaries match their raw families; these differences are consistent with non-atomic collection during background requests.
- HTTP 422 spreads have `n=1` for etcd/ARM64 and Tigerstore/X64. Only one repetition in each group exposes that status series, with an observed zero delta. The other repetitions remain absent, not inferred zeros. Other reported whole-window spreads have three repetitions; their min, median, and max match the per-run rows.
- HTTP 4xx totals range from 171 to 234 per run. The exposed HTTP 500 counter has zero delta in all 12 runs; no other 5xx status series appears. This is not proof that every possible missing error series is zero.
- etcd/ARM64/1 revision deltas are 11,655 / 11,654 / 11,654. The one-revision replica difference is in the raw endpoint status, not a calculation error. Revision deltas remain per replica, never summed.

No arithmetic errors require correction. No source edits or new cluster/build runs were made.
