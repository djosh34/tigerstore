[Actions run 38387706444](https://github.com/djosh34/tigerstore/actions/runs/38387706444), [raw evidence and profiles](@EVIDENCE@), [per-repetition CSV](@BLOB@/runs.csv), [median CSV](@BLOB@/summary.csv).

## Commits and method

The resolver pinned these heads at 2026-10-13T11:04:41Z before the three jobs started. PR 77 merged at 10:56:39Z and its branch was deleted. Its retained PR-head ref remains available. Main and PR 77 have identical complete Git trees, so their rows are same-code controls.

| Store | Exact commit |
|---|---|
| main | `8efd83a173394dd94a3981c72b4c51a1f3d061f1` |
| PR 77, retained `refs/pull/77/head` | `936bc5b69ae8a0c59585521734b41222006a0a2c` |
| PR 79, `work/50-hpack` | `129e49a842b4802e9a36b74c830b3538525f846f` |
| PR 80, `work/50-txn` | `6179f12c2bbaaffc9960782533ad83c292605962` |
| etcd v3.7.2 | `68c065e562994b89e333e77b039ad066f933c586` |

Five repetitions per store, back to back within each job. Odd repetitions run main, 77, 79, 80, etcd; even repetitions reverse that order. Every repetition starts three fresh server processes. Each cluster gets 4,096 seed Puts, 20,000 warmup operations, a pause, then 60,000 measured operations. The tables give medians of five per-repetition statistics, not pooled latency percentiles.

The generator is `experiments/bench75/load` at `7035df682bf10b2aaaa9e224911c4a187da41b31`, direct etcd protobuf through grpc-go v1.84.0. It uses 16 established TLS 1.3 connections with client certificates, 4,096 Kubernetes-shaped keys and independent SHA-256-derived 1,024-byte values reused on overwrite. Workers each have one outstanding RPC and target only the leader. Disk barriers remain enabled on the shared ext4 root disk. Three replicas and the generator share one VM. This checks the configured durability path, not physical power-loss protection.

All benchmarks ran on free `ubuntu-24.04-arm` Actions runners. Put 64 used `GitHub Actions 1000009418`; Put 256 used `GitHub Actions 1000009417`; CAS 256 used `GitHub Actions 1000009419`. All used image `20261004.142.1`. Within each VM, the leader uses CPU 0, followers CPUs 1 and 2, and the generator CPU 3. The environment sets `GOMAXPROCS=1`; tigerstore forces two Go processors per server despite the one-CPU affinity. Etcd and the generator retain one. Tigerstore and the generator use Go 1.27.2; stock etcd uses Go 1.26.8. Its executable SHA256 is `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d`.

Each tigerstore build has the same observation-only loopback runtime-metrics hook in a private source extract. Core, frontend, Raft, WAL and disk behavior do not change. No profiles or forced GC run in the comparison windows. CPU is the user-plus-system `/proc/PID/stat` delta divided by completed operations. Total server CPU sums the three processes, not their threads. Leader allocations use cumulative object and byte deltas. Etcd uses its stock Prometheus memstats counters. GC share is Go's estimated GC CPU divided by estimated non-idle runtime CPU, not pause time or a ratio to `/proc` CPU; stock etcd does not expose that counter.

The syscall columns are leader `/proc/PID/io` `syscr` and `syscw` deltas over the CPU measurement window, divided by completed operations. They cover the whole leader process. They are Linux read/write I/O-accounting counters, not client-socket-only counts and not all Syscall6 calls. Fsync and epoll are not read/write counts. CPU and syscall samples bracket arrivals through drain; allocation snapshots bracket a slightly wider interval. Later replica fences stay outside these intervals, so trailing follower work can fall outside the CPU sum.

## Put results

Latency columns are milliseconds. CPU columns are microseconds per Put, leader then all three servers. Allocations and GC share are leader process totals. `n/a` means the stock endpoint does not expose GC CPU.

@PUT_TABLES@

The 256-client tigerstore generator occupied median 0.96 to 0.98 CPU cores. These closed-loop results do not establish maximum leader capacity.

@CAS_SECTION@

## Source counts, separate from runtime measurements

[Full tigerstore audit](@BLOB@/source-audit/audit.md) and [exact verified sites](@BLOB@/source-audit/verified-sites.md). A send and a selected receive are separate channel operations. Closes and creation are separate. The counts below cover named request/replication sites, not unlisted transport, runtime or context lifecycle channels.

All four tigerstore Put paths have zero fixed request-private sends, receives and closes, and no fresh private reply channel. CAS on main and PR 80 has the same topology. This is not zero total channel work. For N requests in a consumed cohort, sends per operation are `(A+Q+F+O+I+U+Sw)/N`; receives are `(A+Q+F+O+I+U+Ticks+Rw)/N`. A counts admission batches, Q persist arrays, F disk completion groups, O/I outbound/inbound peer messages, U consumed Unreachable reports, Sw/Rw successful/selected coalesced writer wakes, and Ticks selected background ticks. These populations differ and were not instrumented. No batch size or numerical total is invented.

Every counted tigerstore channel site follows. All four commits share these line numbers unless stated otherwise.

| Counted handoff | Send site | Receive site |
|---|---|---|
| Admission A | `internal/node/node.go:136` | `internal/node/node.go:335` or `internal/node/node.go:340` |
| Persist Q | `internal/node/node.go:353` | `internal/node/node.go:486` or `internal/node/node.go:493` |
| Disk completion F | `internal/node/node.go:507` | `internal/node/node.go:346` |
| Outbound peer O | `internal/peer/peer.go:297` | `internal/peer/peer.go:308`, `internal/peer/peer.go:314` or `internal/peer/peer.go:317` |
| Inbound peer I | `internal/node/node.go:266` | `internal/node/node.go:335` or `internal/node/node.go:340` |
| Unreachable U, conditional | `internal/node/node.go:274` | `internal/node/node.go:335` or `internal/node/node.go:340` |
| Writer wake Sw/Rw, coalesced | `internal/server/h2.go:195` on main/77, `internal/server/h2.go:201` on 79/80 | `internal/server/write.go:52` |
| Background tick | none | `internal/node/node.go:357` |

Receive alternatives count once per consumed message. Grouped completion is a direct call and atomic list, not another channel. Normal repository closes are zero. Queued work can cross a timed boundary, so the formula is a cohort identity, not presumed matching in-window sends and receives.

The known leader full-value materialization subtotal per ordinary Put is **4 for main, 77, 79 and 80**. Every counted stage is:

1. Owned request accumulation at `internal/server/stream.go:127`.
2. Entry encoding at `internal/core/codec.go:68`.
3. Retained Raft Entry.Data clone, reached at `internal/core/node.go:571` on main/77, `internal/core/node.go:585` on 79, `internal/core/node.go:591` on 80. The actual clone is raft v3.7.0 `raft.go:817`, with the proto2 bytes copy at protobuf v1.36.11 `internal/impl/merge.go:188`.
4. WAL staging at `internal/wal/wal.go:351`.

Replication adds one leader peer encoding at `internal/peer/peer.go:419` for each transmitted occurrence. A newly received, persisted and applied follower occurrence adds two stages, peer decode at `internal/peer/peer.go:241` and WAL staging at `internal/wal/wal.go:351`. Fanout, retransmission and asynchronous trailing work are conditional. No fixed peer-message count per Put is assumed.

For the CAS request's new 1 KiB value, main has **6** known leader stages and PR 80 has **4**, for either compare outcome. The four common stages are accumulation, entry encoding at `internal/core/codec.go:73`, Raft clone and WAL staging. Main also owns decoded value bytes at frontend `internal/server/rpc.go:63` and apply `internal/core/node.go:851`, both through protobuf's bytes decoder at `internal/impl/codec_gen.go:5497`. PR 80 aliases those bytes instead. It still decodes fields and rebuilds Txn request structs. A failed comparison still logs the entire Txn, including the unexecuted Put value. Its else-Range adds one separate returned-stored-value reply encoding at `internal/server/stream.go:207`. With f failures among N completed Txns, the known leader averages are `6 + f/N` and `4 + f/N`, across the two distinct payloads. The measured f was zero.

These are named full-value stages, not physical memcpy totals or allocations. Pointer, struct and slice-header assignments copy zero value bytes. TLS, transport buffering, kernel/page-cache copies and unenumerated buffer growth remain excluded, not zero. Payload copies do not amortize by batch size. Neither PR 79 nor PR 80 removes another ordinary Put value stage beyond PR 77.

@ETCD_SOURCE_COUNTS@

@PROFILE_SECTION@

## Validation and setup history

All 65 comparison windows passed, with 3,900,000 completed operations and no RPC errors. CPU identities and monotonic counters, leader/term stability, thread affinities, revision deltas and all three replicas' application fences passed. [Independent counter and affinity checks](@BLOB@/validation.txt) recompute CPU, I/O and allocation deltas from the raw JSON.

The first dispatch, [38387063293](https://github.com/djosh34/tigerstore/actions/runs/38387063293), failed setup for both Put jobs because the deleted PR 77 branch resolved to an empty candidate. No Put benchmark started. Its CAS job completed, but none of that run enters these medians. [Its logs and CAS evidence](@FIRST_EVIDENCE@) are retained. The resolver now rejects failed lookups and accepts retained PR-head refs. The fresh full matrix above passed. Nothing was merged.
