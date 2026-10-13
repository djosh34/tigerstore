# Mixed Kubernetes benchmark, final generator

[Run 38432864221](https://github.com/djosh34/tigerstore/actions/runs/38432864221) passed five repetitions per target on the free `ubuntu-24.04-arm` runner. The benchmark job took 27m45s. All 15 windows are valid, with no failed cohort requests or unsent arrivals, and passed the leader/term check. Cohort counts match across targets in every repetition: 5,026, 5,024, 5,014, 5,019 and 5,048. Final live-key counts range from 1,000,355 to 1,000,413.

- Generator, scripts and report: `05e551026800d9efb21df13078c5107b0891c1cc`.
- Baseline and candidate: main `5d964228952d6983ba273bc2e6813770aad796c6`. These are independent five-run series, not ten pooled repetitions.
- Stock etcd v3.7.2, release archive checked by SHA-256 in the workflow.
- No injected delay, disk write barriers on, client and peer TLS with client certificates.
- Generator CPU 3, leader CPU 0, follower CPUs 1 and 2. Same layout for all targets, alternating target order.
- Fresh three-node clusters per repetition. One million preloaded keys, up to 128 puts and 60 KiB per Txn, 300 Txn arrivals/s over six connections. Setup is outside measurement.
- 500 open-loop Poisson arrivals/s, three-second warmup, ten-second window, six connections with 16 slots each, all three endpoints. Latency uses scheduled arrivals through drain.
- GET/LIST/update/create/delete shares: 25/10/40/15/10. Ten percent of GETs use known absent keys when any exist. LIST asks for 500 keys, count and more. CAS mutations and full KV fields are checked against worker-owned state.
- All 27 resource-prefix watches start after preload. Events and previous values match acknowledged mutations. Matching is checked again after the readers stop.
- Random value-pool bytes, with count-weighted resource/bin choices and bin means taken from the Kubernetes 1.37 inventory on #50.

Every statistic in `summary.md` is min / median / max over five repetitions. Percentiles are not pooled. Throughput is completion rate at fixed offered load, not maximum capacity. Server CPU sums all three node processes and includes work for both requests and watches; CPU per event is not an isolated watch-cost measurement.

The directory contains raw JSONs, role placements and post-run resource samples. Node logs remain in the workflow artifact. There are no certificates or object values here. Reproduce the report without rerunning the benchmark:

```sh
GOFLAGS=-p=2 GOMEMLIMIT=2GiB go run ./cmd/benchreport evidence/mixed50/run-38432864221
```

Maximum sampled post-run node RSS sum was 6.11 GiB for etcd and 7.00 GiB for main baseline. Maximum cluster-directory size was 7.24 GiB and 4.30 GiB. The baseline samples had at least 7.14 GiB available. These are post-run samples, not peak measurements. Targets ran sequentially with fresh data directories.

This run supersedes the PR's initial comparison in `run-38423820862`. Both the generator and main changed between those runs, so they are not a controlled test of the effect of the generator changes. The older evidence is retained, not pooled with this run. Earlier uncapped-preload attempts timed out in etcd; their cause was not established and no partial windows are included in either report.

Dispatch the comparison from the branch with:

```sh
gh workflow run bench.yml --ref work/50-mixed \
  -f candidate=main -f baseline=main -f benchmarks=mixed-k8s -f modes=rate
```

Mixed is rate-only in the bench scripts to avoid a second set of million-key preloads exceeding the 45-minute job limit. Other benchmarks retain max and rate modes. All 41 checks on the measured source passed, including lint, race tests, simulator, planted-bug, differential and benchmark jobs.
