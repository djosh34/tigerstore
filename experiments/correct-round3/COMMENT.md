## Correctness testing round 3

Tested main `499a4c6759d0c1276aa4257af462295216f8ab21`, newest at launch, with metrics, RangeStream and the client-certificate size fix. Product code is unchanged. All workloads, faults and expensive checkers ran on free runners.

### Jepsen

Register, set and append ran twice each on tigerstore and stock etcd v3.7.2, with `kill,partition,pause`, concurrency `2n`, rate 30, 90 seconds and `--ops-per-key 50`.

- All six tigerstore histories have valid workload verdicts, covering 6,844 calls. All six etcd controls are valid, covering 6,894 calls.
- Both tigerstore register jobs hit their online checking time limit. The round 2 certificate method gave both full histories valid verdicts through separate Python and upstream-model Clojure verification. Those original online failures remain retained.
- All 12 histories have observed kills, pauses, partition packet hits and healing.
- Upstream set and append ignore `ops-per-key`; only register actually uses the 50-operation key bound. Their generators were not changed.

[Jepsen report, commands, histories and certificates](https://github.com/djosh34/tigerstore/blob/058594029a280b46fd3c09117f53d7eef1ad1fa9/experiments/jepsen3/REPORT.md).

### Combined faults with RangeStream and watches

Repeated round 2's partition-then-kill, leaderless-election pause, one-way cuts and link flaps. Each matrix ran twice on both architectures and both products.

| Matrix | Passed | Failed |
| --- | ---: | ---: |
| Initial snapshot/watch checks | 22 | 10 |
| LIST freshness in whole-history checks | 27 | 5 |
| Lower load, same freshness checks | 31 | 1 |
| Total | 80 | 16 |

Tigerstore passed all 16 final lower-load cases. The remaining control failure was an etcd ARM linearizability timeout. Across all matrices, tigerstore passed 43 of 48 cases and etcd passed 37 of 48.

All 288 in-fault LIST/watch probes passed. The 32-key LISTs exercised multiple chunks and checked full KV metadata against historical Range. Watches checked previous values and completeness through an independently obtained revision. The last two matrices also recorded 54,672 linearizable LISTs in the ordinary history, with invocation/EOF bounds and a stale-LIST negative regression. The initial matrix did not check LIST freshness.

The 96 cases contain 117,536 recorded operations and 384 original/cold-recovery history variants. All four repository checkers ran on every variant: 1,472 verdicts passed and 64 linearizability checks timed out. All 16 failed cases remain unverified; reduced-load passes do not validate them. Watch, durability and revision checks passed throughout.

[Combined-fault report, scripts and permanent evidence](https://github.com/djosh34/tigerstore/blob/93c4b85b2bd14e495aebf154356641093f76c45a/experiments/correct-round3/REPORT.md). [Parent report and independent archive audits](https://github.com/djosh34/tigerstore/tree/exp/correct-round3/experiments/correct-round3).

No product failure was confirmed. Bugs filed: zero. This is not a clean pass and does not close #49. This round did not repeat ENOSPC, clock, lease or compaction tests. SIGKILL is not power loss.

The last job finished at 21:26:27Z. Experiment workflows are disabled and both helpers are deleted. Main later advanced to `76952348`, changing only `cmd/loadgen/conn_test.go`, not the server source. Testing has stopped.
