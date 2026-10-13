# Semantics campaign

This branch contains reliability experiments only. It does not change product code. The server and repository checkers come from main `5d964228952d6983ba273bc2e6813770aad796c6`. The module, client conversion, physical WAL reader, receipt checks and cold-recovery controller reuse `experiments/correct-round1`.

The workflow runs both stock etcd v3.7.2 and tigerstore on free `ubuntu-24.04-arm` and `ubuntu-24.04` runners. It allows at most four concurrent jobs. Each controller starts three TLS server processes in a private network and mount namespace. Every case requests a full peer TCP blackhole, a paused leader with a verified new leader, and a killed/restarted leader. Client ports remain outside the blackhole. Actual peer packet counters and a failed quorum read prove the partition hit. Wait statuses, task states and CPU counters prove process signals hit.

The lease case records an explicit grant and an ambiguous synthetic LeaseRevoke from a separate client. That operation starts with the grant, as in `internal/sim/history.go`. Keepalive calls and TTL reads stay in raw diagnostics because the repository format has no keepalive operation. Timing assertions and watched previous values check early expiry, deletion of all attached keys at one revision, and exactly one deletion per key per watch.

The physical WAL decoder independently reads all batch kinds 1 through 8. Its stamp counter advances on successful grants and renewals. A matching expiry becomes a LeaseRevoke; a stale stamped expiry becomes a recorded no-op. Raw protobuf requests and every normalization decision remain in `history-*.json.normalization.json`. The adapter does not use core.State or infer mutations from watch events. Stock etcd checkpoints record remaining TTL and do not mutate keys. Unknown fields, unknown commands and bytes that cannot fit JSON fail conversion.

The compaction case requires an acknowledged compaction while the leader remains paused. It records three watches from C-1 and three new logical watches from C+1. Old-revision cancellation and historical Range errors stay in the checked history. The repository watch checker permits a previous-value watch to cancel at C itself. No boundary event loses its previous value silently.

The watcher case opens 192 concurrent previous-value watches across the three pinned clients before any writes. It requires creation responses and progress through an independently captured terminal revision. Both CAS branches, updates and multi-key deletes are part of every workload.

Every preserved client history runs Linearizable, Watches, Durable and Revisions against each available original or cold-recovery WAL set. Successful cases require all four sets and 16 passing verdicts. A missing or failed log conversion does not skip checkers: they run against the raw history with its missing log and fail explicitly. If no history was recorded, the offline report says so and makes no checker claim. A timeout, absent fault hit, missing oracle or partial coverage is FAILED.

All Actions attempts remain in the archive, including build/setup failures. A manifest names each compressed evidence file with its original size and SHA-256. Attempts that reached the controller are counted separately. A green rerun does not erase an earlier failure.

Run the workflow by pushing a script change to `exp/correct-round2-semantics`, or dispatch its workflow. Product comparisons use immutable scripts, seeds, repetitions and runner labels from `campaign.json`. Never start a new run at or after `2026-10-13T22:00:00Z`.
