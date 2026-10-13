# Correctness round 1

Reliability tests only. Product code is unchanged. The throwaway branch is `exp/correct-round1`.

The first campaigns built main commit `90870876b6459334c7eddd67c0f9d184267f2eba`. The runner built that exact commit in a detached checkout, separately from the test tooling. Every server and fault ran on a free `ubuntu-24.04-arm` GitHub runner. No server or fault ran on the development host.

## Workload and checks

Each case starts three real server processes with client and peer TLS. Three stock etcd clientv3 v3.7.2 clients each use one pinned member. They issue Put, MOD/VERSION CAS Txn, single-key and multi-key DeleteRange, and linearizable Range over six shared keys. Three prefix watches request previous values and resume across member restarts. Values identify their originating client operation. Unary mutations have one client attempt, and errors remain ambiguous when commitment is unknown.

The nominal rate is five operations per second per client, with one outstanding call per client and a two-second request timeout. `history.History` records requests, responses, errors, monotonic call/return times, and watch responses. A sidecar preserves raw watch transport responses. After traffic stops, the client waits eight seconds, issues three recorded no-revision transactions, reads every member, and demands watch progress through the terminal revision. The no-revision transactions were added after the first etcd run exposed a persisted-commit lower-bound limitation.

After SIGKILL of all members, the controller preserves the original disks. It then makes three independent copies, omits one member in each, cold-starts only the other two, and compares every surviving member's revision, value bytes and MVCC metadata with the terminal receipt. It stops each pair before offline inspection.

Each original or cold-pair disk set supplies a committed-prefix log. Every available enriched history goes through `check.Linearizable`, `check.Watches`, `check.Durable` and `check.Revisions`. The linearizability timeout is 30 seconds. Missing fault evidence, missing recovery sets, missing coverage, missing watch progress and checker timeouts fail the run. A failed controller does not suppress the offline checks.

The client decodes command batches separately from the product's command decoder and uses the repository's independent checker model. Tigerstore's physical WAL recovery still uses `internal/wal.Open` on private byte copies, so this is not an independent audit of the physical WAL decoder. Original files remain unchanged. SIGKILL does not discard the kernel page cache. These runs test process-crash recovery, not power-loss durability.

## Fault evidence

- SIGKILL targets both the current leader and a follower. `waitpid` must report signal 9. Restarts must become ready.
- SIGSTOP targets both roles. `waitpid`, every task's stopped state and unchanged CPU counters prove the pause. SIGCONT must produce a continued wait status and resumed tasks.
- Packet loss, delay and reordering use `tc netem` in a private network namespace. Peer listeners use `127.77.0.N:2380`; clients use `127.78.0.N:2379`. Selectors account for initiators that do not bind their source address. Effective parameters, exact filters, TCP peer counters, positive UDP canaries and excluded negative canaries must all agree. Delay is per selected packet direction, not an RTT claim.
- Disk tests use a bounded ext4 loop filesystem with no reserved blocks. The controller fills it to numeric ENOSPC, requires the actual server to report ENOSPC and exit, frees space, verifies a real write and fsync, and restarts the node. A filler error alone does not count as an exercised server fault.

Every fault records requested, installed, exercised and healed phases. A seed fixes client choices and controller choices, not kernel scheduling or the kernel's random packet-loss decisions.

## Failed test-tool runs

[Run 38421292600](https://github.com/djosh34/tigerstore/actions/runs/38421292600) used tooling `91e8a5f`. Tigerstore's signal case passed. Etcd's original-disk oracle failed, while its three cold-pair histories passed all four checkers. All three original etcd WALs had `HardState.Commit=729` and the same durable entry 730. Entry 730 contained the exact acknowledged Put missing from the selected prefix. Its SHA-256 was `7e1a6c2f3a56cb8bb6b346f5b786357c888ea1a59a8b94ead1f8bbe0468e8e5d`. The selected prefix ended at revision 487, while clients saw 488. This was an incomplete persisted-commit oracle, not observed loss of the value. Three recorded no-revision transactions now advance the persisted commit lower bound past the final changing operation. The original failure remains a failed run.

[Run 38422574256](https://github.com/djosh34/tigerstore/actions/runs/38422574256) used tooling `3acbf5f`. Both products passed signals and netem. Four tigerstore disk/mixed cases stopped because the one-MiB filler allocation failed while smaller allocations still fit. The strict four-KiB canary correctly rejected those fault installations. Etcd reached server ENOSPC, but its post-unlink write still saw ENOSPC before ext4 reclaimed the blocks. The controller now exhausts four-KiB allocations as well, and synchronizes and verifies reclamation before restarting. All five cases remain failed runs. No product bug follows from these controller errors.

The next etcd disk case used a 128-MiB filesystem. Etcd's two 64-million-byte WAL allocations plus filesystem and database space exhausted it during initial traffic, before the scheduled filler fault. The controller reported that premature exit and failed the case. The matching follow-up disk comparison uses 256 MiB for both products with one-KiB values.

## Evidence

GitHub Actions artifacts retain full server disks and logs. The `results/` directory preserves compressed histories, fault records, checker outputs and logs, with SHA-256 manifests. Product and tooling commits are in each artifact's `commits.txt`. Run summaries distinguish client histories from the four disk-set variants of each history.

This round does not cover the rest of issue #49, including the byte-stream proxy, CPU throttling, injected fsync errors, power loss, or snapshot-install faults. It does not close that outcome.
