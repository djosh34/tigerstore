# Mixed Kubernetes benchmark

[Successful run 38423820862](https://github.com/djosh34/tigerstore/actions/runs/38423820862), five repetitions of each target on one free `ubuntu-24.04-arm` runner. The benchmark job took 27m40s. All 15 windows are valid, have zero failed cohort requests and zero unsent arrivals, and passed the leader/term check. Every repetition has identical arrival-cohort counts across its targets. Live keys at the end range from 1,000,348 to 1,000,414.

- Generator and scripts: `69c899f9562c3c50c720b4b88644f5b1586359dd`.
- Both baseline and candidate are main `90870876b6459334c7eddd67c0f9d184267f2eba`. They are separate five-run series, not ten pooled repetitions.
- Stock etcd v3.7.2, installed from the release archive with the workflow's SHA-256 check.
- No injected node delay. Client and peer TLS with client certificates. Disk write barriers on.
- Generator CPU 3, leader CPU 0, followers CPUs 1 and 2. Same placement for every target.
- One million preloaded keys, up to 128 puts and 60 KiB per preload Txn, 300 Txn arrivals/s over six connections. Preload and watch setup are outside measurement.
- Measured load is 500 open-loop Poisson arrivals/s, three-second warmup and ten-second window, six connections with 16 slots each, round robin over all three endpoints. Scheduled arrival times determine latency through drain.
- Get/LIST/update/create/delete percentages are 25/10/40/15/10, with 27 resource-prefix watches alongside them. Each event matches an acknowledged mutation, including its previous value.
- Value sizes use the Kubernetes 1.37 inventory from #50. The table has 6,261 objects and 9,217,907 value bytes. Values are random pool bytes, not dump contents.

`summary.md` was regenerated from the raw JSONs with the PR's report formatting. Formatting changed after this run; the statistics and measured generator did not. Reproduce the report with:

```sh
go run ./cmd/benchreport evidence/mixed50/run-38423820862
```

The `mixed-k8s` directory preserves every JSON, role placement and post-run resource sample. Node logs remain in the workflow artifact. There are no certificates or object values here. Post-run samples are not peak-memory measurements. Maximum sampled sum of node RSS was 6.36 GiB for etcd and 6.95 GiB for main baseline. Maximum cluster-directory size was 7.25 GiB and 4.31 GiB respectively. At least 7.12 GiB was available in the baseline post-run samples. Targets had fresh data directories and ran sequentially.

These are fixed-rate results, not maximum throughput. The two identical main series have different latency tails, and the baseline series' worst p99.9 is higher than etcd's. No cause for that variation was isolated. `summary.md` reports every spread rather than choosing a single window.

Two earlier uncapped-preload attempts failed with etcd request timeouts: [38420164244](https://github.com/djosh34/tigerstore/actions/runs/38420164244) and [38421410348](https://github.com/djosh34/tigerstore/actions/runs/38421410348). Their partial windows are not included. The cause of those timeouts was not established. After capping preload at 300 Txns/s, [38422881230](https://github.com/djosh34/tigerstore/actions/runs/38422881230) and the reported run both passed all five repetitions. The reported run resets the measurement arrival RNG after preload, so preload completion time cannot change the measured schedule.

Dispatch the same comparison from this branch with:

```sh
gh workflow run bench.yml --ref work/50-mixed \
  -f candidate=main -f baseline=main -f benchmarks=mixed-k8s -f modes=rate
```

`mixed-k8s` is rate-only in the bench scripts, so the default matrix does not spend another half hour on a second set of preloads. Other benchmarks still support max and rate modes.
