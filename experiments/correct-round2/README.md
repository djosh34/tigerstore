# Correctness testing round 2

Reliability tests for issue #49. No product code changes.

The campaign pins main at `5d964228952d6983ba273bc2e6813770aad796c6`, fetched on 2026-10-13 at 16:16 UTC. Round 1 tested `90870876b6459334c7eddd67c0f9d184267f2eba`. The intervening commit adds the large-value stress test and diagnostic logging. The frontend, batching and wire-form paths named in this round's request are already present in both commits.

The scripts start from `exp/correct-round1` at `07ed9fb7edea41262910dbdc7b304d5d76a1998c`. Test-tool changes and evidence remain on throwaway branches, not main.

| Branch | Assigned cases |
| --- | --- |
| `exp/correct-round2-faults` | Partition plus new-leader kill, election pause, leader ENOSPC, one-way and flapping partitions, 256 clients across the cluster |
| `exp/correct-round2-semantics` | Lease grant, keepalive and expiry across partitions and leader changes; compaction and watch resume; many watchers requesting previous values |
| `exp/correct-round2-clocks` | A node limited to 5 to 10 percent of one CPU, single-node clock jumps, request-limit values under faults |

Each part uses both `ubuntu-24.04-arm` and `ubuntu-24.04`, with stock etcd v3.7.2 comparisons. Tests must prove requested faults took effect. Every recorded history goes through the repository's linearizability, watch, durability and revision checkers. A missing fault, missing checker or timeout fails the run. A failed attempt remains in the report after its test tool is corrected.

The deadline is 2026-10-13T22:00:00Z. No work starts after it. All fault runs execute on GitHub-hosted runners. Local Go commands, if needed, use `GOFLAGS=-p=2 GOMEMLIMIT=2GiB`, with tests limited to `-parallel 2`.

Testing has stopped. [REPORT.md](REPORT.md) records the results and remaining validation gaps. [summary.json](summary.json) pins every source commit and workflow run. The `audit-*.json` files contain the parent's independent archive and checker-completeness audits. Later main `b5f2e28` is not covered by these runs.
