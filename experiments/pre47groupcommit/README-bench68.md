# PR 68 comparison

Adapted from `exp/pre-47-groupcommit` and issue #47's "Append-clocked group commit pre-work" comment. This branch contains only experiment code; it does not change production source.

The runner builds archives of main's common ancestor with PR #68, `72bd5d7680dbf456e7811b02f300c26e2b3b805a`, and the requested PR commit, `acfe8fa43993c447b1a322e6dfd59a0492222297`. Both receive the identical observability-only patch from pre-work baseline `81053bcc54ac315b6ac9302dbe09b5eb591c18db`. The patch adds debug endpoints and file Write/Sync timing counters, not core, driver, WAL or transport changes. It forwards each original file call and returns its original result. No group-commit prototype patch is applied. The pre50 load client is copied unchanged from `exp/pre-47-groupcommit`.

PR #68 actually targets `work/47-lease` at `4f65ce5c14f72277b42bf0030de994656c03eefa`. The comparison requested here is against main, not against that stacked PR base. It therefore cannot isolate batching from the lease and compaction changes in the candidate's ancestry.

The workflow runs three free `ubuntu-24.04-arm` jobs at 16, 64 and 256 clients. Each job runs five repetitions each of main, PR #68 and stock etcd v3.7.2, with alternating store order. Leader, followers and generator occupy CPUs 0, 1, 2 and 3 with `GOMAXPROCS=1`. Each fresh cluster receives 4,096 seed Puts, 20,000 warmup Puts and 60,000 measured Puts. Barriers, TLS, correctness checks, affinity checks, CPU sampling and all-replica disk fences are unchanged from pre-work. Tests and builds run only on Actions. The script rejects starts at or after 2026-10-13T22:00:00Z.

```sh
CLIENTS=16 timeout --kill-after=5s 820s bash experiments/pre47groupcommit/run.sh
# Separate jobs use CLIENTS=64 and CLIENTS=256.
python3 experiments/pre47groupcommit/summarize.py
```

Raw evidence and aggregate CSVs use `evidence/bench68`. `baseline` means main and `tigerstore` means PR #68 in those files. Tables report medians of five per-run statistics, not pooled latency percentiles. CPU includes client drain but excludes later follower catch-up; disk counters include the later persistence fences. Etcd's WAL histogram measures WAL sync calls, mostly fdatasync, not every process file fsync. The replicas share one VM and disk. Guest ext4 barriers do not prove physical power-loss protection.
