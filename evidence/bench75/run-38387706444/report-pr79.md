PR 79 reduced leader object allocations versus PR 77 by 33.1% at 64 clients and 36.2% at 256, and allocated bytes by 14.1% and 14.6%. Put throughput medians changed by -0.1% and +1.2%. The nearly full generator CPU at 256 clients limits any capacity claim.

[Actions run 38387706444](https://github.com/djosh34/tigerstore/actions/runs/38387706444), [raw evidence and profiles](https://github.com/djosh34/tigerstore/tree/ece9ff4dd212e3cd9130eb85aed78ab9955a440b/evidence/bench75/run-38387706444), [per-repetition CSV](https://github.com/djosh34/tigerstore/blob/ece9ff4dd212e3cd9130eb85aed78ab9955a440b/evidence/bench75/run-38387706444/runs.csv), [median CSV](https://github.com/djosh34/tigerstore/blob/ece9ff4dd212e3cd9130eb85aed78ab9955a440b/evidence/bench75/run-38387706444/summary.csv).

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

### Put, 64 clients

| Store | Put/s | Mean / p50 / p99 / p99.9 ms | Leader / total CPU µs/Put | Generator µs/Put / cores | Objects / bytes per Put | GC CPU share | syscr / syscw per Put |
|---|---:|---:|---:|---:|---:|---:|---:|
| main | 25,087 | 2.549 / 2.398 / 5.716 / 9.185 | 34.33 / 67.17 | 30.00 / 0.75 | 30.79 / 6,601 | 1.42% | 1.2022 / 0.8584 |
| PR 77 | 25,292 | 2.527 / 2.371 / 5.541 / 8.379 | 34.17 / 65.83 | 29.83 / 0.77 | 30.59 / 6,608 | 1.39% | 1.1883 / 0.8543 |
| PR 79 | 25,269 | 2.531 / 2.381 / 5.664 / 8.298 | 33.83 / 66.33 | 30.00 / 0.76 | 20.47 / 5,674 | 1.17% | 1.2088 / 0.8622 |
| PR 80 | 25,607 | 2.497 / 2.347 / 5.634 / 8.899 | 33.33 / 65.00 | 30.00 / 0.76 | 20.36 / 5,711 | 1.09% | 1.2160 / 0.8582 |
| etcd 3.7.2 | 3,805 | 16.816 / 16.270 / 28.259 / 34.729 | 203.50 / 301.33 | 65.17 / 0.25 | 278.01 / 32,830 | n/a | 1.1428 / 5.1431 |

### Put, 256 clients

| Store | Put/s | Mean / p50 / p99 / p99.9 ms | Leader / total CPU µs/Put | Generator µs/Put / cores | Objects / bytes per Put | GC CPU share | syscr / syscw per Put |
|---|---:|---:|---:|---:|---:|---:|---:|
| main | 41,447 | 6.155 / 5.654 / 13.113 / 16.684 | 21.67 / 39.67 | 23.33 / 0.96 | 26.69 / 6,478 | 2.29% | 0.7882 / 0.3721 |
| PR 77 | 41,357 | 6.167 / 5.708 / 12.913 / 16.487 | 21.17 / 39.17 | 23.17 / 0.97 | 26.64 / 6,469 | 2.11% | 0.7770 / 0.3710 |
| PR 79 | 41,859 | 6.095 / 5.683 / 12.784 / 15.409 | 21.33 / 39.83 | 23.33 / 0.98 | 16.99 / 5,527 | 1.82% | 0.7920 / 0.3776 |
| PR 80 | 41,453 | 6.151 / 5.654 / 12.833 / 16.902 | 20.67 / 39.00 | 23.33 / 0.97 | 16.88 / 5,559 | 1.76% | 0.8172 / 0.3748 |
| etcd 3.7.2 | 4,525 | 56.518 / 55.334 / 93.223 / 113.975 | 180.83 / 243.50 | 54.00 / 0.25 | 264.87 / 33,499 | n/a | 0.3871 / 4.0789 |

The 256-client tigerstore generator occupied median 0.96 to 0.98 CPU cores. These closed-loop results do not establish maximum leader capacity.

## CAS Txn, 256 clients

The CAS job alternates main, PR 80, etcd and the reverse order. Before each warmup and measured subprocess, an untimed keys-only prefix Range seeds positive existing-key mod revisions. Each worker compares its cached ModRevision with EQUAL, then puts 1 KiB on success, else ranges that same key and validates its value/revision. The shared cache only advances. There are no separate timed pre-reads or client retries. This compares the PR 80 stack with main, not the alias change alone.

Each store completed 300,000 measured transactions, all with compare-pass and none with compare-fail. The failure arm was present but not exercised. These are successful-CAS results, not conflict or failed-CAS latency measurements. Revision growth equalled compare passes. The following costs divide by completed Txns, not by separate Puts inside them.

### Txn, 256 clients

| Store | Txn/s | Mean / p50 / p99 / p99.9 ms | Leader / total CPU µs/Txn | Generator µs/Txn / cores | Objects / bytes per Txn | GC CPU share | syscr / syscw per Txn |
|---|---:|---:|---:|---:|---:|---:|---:|
| main | 27,397 | 9.309 / 8.275 / 23.372 / 31.592 | 33.00 / 57.33 | 29.67 / 0.78 | 72.50 / 11,650 | 2.94% | 0.6854 / 0.3572 |
| PR 80 | 26,035 | 9.798 / 8.569 / 23.066 / 28.519 | 33.83 / 60.67 | 29.33 / 0.80 | 77.20 / 9,040 | 2.04% | 0.6207 / 0.3513 |
| etcd 3.7.2 | 3,450 | 74.137 / 73.328 / 120.803 / 160.854 | 222.83 / 308.17 | 64.50 / 0.22 | 333.38 / 40,921 | n/a | 0.3881 / 4.0868 |

## Source counts, separate from runtime measurements

[Full tigerstore audit](https://github.com/djosh34/tigerstore/blob/ece9ff4dd212e3cd9130eb85aed78ab9955a440b/evidence/bench75/run-38387706444/source-audit/audit.md) and [exact verified sites](https://github.com/djosh34/tigerstore/blob/ece9ff4dd212e3cd9130eb85aed78ab9955a440b/evidence/bench75/run-38387706444/source-audit/verified-sites.md). A send and a selected receive are separate channel operations. Closes and creation are separate. The counts below cover named request/replication sites, not unlisted transport, runtime or context lifecycle channels.

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

### Stock etcd source counts

[Etcd release audit](https://github.com/djosh34/tigerstore/blob/ece9ff4dd212e3cd9130eb85aed78ab9955a440b/evidence/bench75/run-38387706444/etcd-source-audit/audit.md) and [verified sites](https://github.com/djosh34/tigerstore/blob/ece9ff4dd212e3cd9130eb85aed78ab9955a440b/evidence/bench75/run-38387706444/etcd-source-audit/verified-sites.md) bind the exact release and its stock dependency buildinfo. The fixed core/request/reply count is **3 sends, 3 receives, 2 closes and 2 fresh reply channels per Put or Txn**, plus batched/conditional work. The Raft proposal acceptance result is separate from the applied response.

| Fixed handoff | Send | Receive | Close and creation |
|---|---|---|---|
| Proposal | raft v3.7.0 `node.go:531` | raft `node.go:386` | none |
| Proposal acceptance | raft `node.go:391` | raft `node.go:541` | close `node.go:392`; create `node.go:528` |
| Applied response | etcd `pkg/wait/wait.go:83` | `server/etcdserver/v3_server.go:1123` | close `pkg/wait/wait.go:84`; create `pkg/wait/wait.go:65` |

For N requests and R Ready cohorts, the enumerated stable-stream sends are `3N+4R+Ws+O+2I` and receives `3N+4R+Wr+O+2I`. Ws/Wr count coalesced scheduler wakes, O outbound and I inbound peer messages. Divide by N, but do not assume R or a batch size. Every additional counted handoff is:

| Batched/conditional handoff | Send | Receive |
|---|---|---|
| Ready | raft `node.go:435` | `server/etcdserver/raft.go:185` |
| Apply cohort | `server/etcdserver/raft.go:232` | `server/etcdserver/server.go:843` |
| WAL completion | leader `server/etcdserver/raft.go:327`, follower alternative `server/etcdserver/raft.go:302` | `server/etcdserver/server.go:983` |
| Advance | raft `node.go:557` | raft `node.go:443` |
| Scheduler wake | `pkg/schedule/schedule.go:116`, conditional | `pkg/schedule/schedule.go:174` |
| Outbound peer stream | `server/etcdserver/api/rafthttp/peer.go:247` | `server/etcdserver/api/rafthttp/stream.go:207` |
| Inbound peer stream | `server/etcdserver/api/rafthttp/stream.go:530` | `server/etcdserver/api/rafthttp/peer.go:177` |
| Peer processor to Raft | raft `node.go:517` | raft `node.go:395` |

Background ticks remain separate, ticker receive at `server/etcdserver/raft.go:183`, successful tick send at raft `node.go:460`, and tick receive at raft `node.go:433`. A tick send can fail. No-snapshot normal Ready work does not use the raftAdvanced channel. Stock gRPC also closes one deadline synchronization channel per timed request at grpc v1.83.2 `internal/transport/http2_server.go:615`; that library close is outside the fixed core table. Its timeout callback receive does not fire here. Other context/gRPC lifecycle channels remain unenumerated, not zero.

Etcd has **8 known immediate leader new-value stages per Put**, excluding peers, optional codec coalescing and deferred backend pages. Every counted stage is:

1. gRPC's owning bytes decode, grpc v1.83.2 `encoding/proto/proto.go:96` and protobuf v1.36.11 `internal/impl/codec_gen.go:5497`.
2. InternalRaftRequest encoding at `server/etcdserver/v3_server.go:1093`.
3. Retained Raft clone at raft v3.7.0 `raft.go:817`, using protobuf `internal/impl/merge.go:188`.
4. WAL Entry encoding at `server/storage/wal/wal.go:976`.
5. WAL Record encoding at `server/storage/wal/encoder.go:84` or `server/storage/wal/encoder.go:89`.
6. WAL PageWriter staging at `pkg/ioutil/pagewriter.go:59` and `pkg/ioutil/pagewriter.go:72` as applicable. These short records cannot take the direct-page bypass. Prefix/tail chunks total one value equivalent.
7. Owning apply decode at `server/etcdserver/apply/apply.go:29`, using the same protobuf bytes decoder.
8. Backend KeyValue encoding at `server/storage/mvcc/kvstore_txn.go:250`.

The protobuf bytes encoding instruction is `encoding/protowire/wire.go:455`. The call and its implementing instruction count once. Backend buffer and bbolt Put retain value slices. Deferred bbolt serialization adds one stage per actual KV occurrence in a leaf page at bbolt v1.5.0 `internal/common/inode.go:101`, reached through `node.go:199` and `node.go:334`. Older residents can be serialized again. Remapping can copy resident values at `node.go:479`. These populations are not a fixed request-time count.

A successful etcd CAS has **8 new-input stages plus 1 stored-value stage**. Even a MOD compare executes a backend Range at `server/etcdserver/txn/txn.go:274` and owns the old KeyValue.Value at `server/storage/mvcc/kvstore_txn.go:142`. A failed compare has **7 new-input stages plus 3 stored-value stages**. It omits the new backend KV encoding, repeats the stored-value decode for its else-Range, then encodes that old value in the reply through grpc `server.go:1189` and `encoding/proto/proto.go:67` or `encoding/proto/proto.go:75`. Measured failures were zero. The summed stages cover distinct payload identities, not nine copies of the proposed value.

Optional grpc codec coalescing adds a new-input copy when multiple buffers need one destination at grpc `mem/buffer_slice.go:115`, implemented at `mem/buffer_slice.go:82`. A single buffer aliases instead. Each transmitted peer occurrence adds an encoding, MsgAppV2 alternatives at `server/etcdserver/api/rafthttp/msgappv2_codec.go:114`, `server/etcdserver/api/rafthttp/msgappv2_codec.go:122` or `server/etcdserver/api/rafthttp/msgappv2_codec.go:143`. Each received occurrence adds a decode at `server/etcdserver/api/rafthttp/msgappv2_codec.go:227` or `server/etcdserver/api/rafthttp/msgappv2_codec.go:243`. General-stream alternatives are `server/etcdserver/api/rafthttp/msg_codec.go:38` and `server/etcdserver/api/rafthttp/msg_codec.go:69`. A newly persisted/applied follower adds its WAL, apply and backend stages. The audit separates these conditional populations. Different frontend boundaries and excluded transport work mean **tigerstore 4 versus etcd 8 is not an end-to-end physical-copy ratio**.




## Validation and setup history

All 65 comparison windows passed, with 3,900,000 completed operations and no RPC errors. CPU identities and monotonic counters, leader/term stability, thread affinities, revision deltas and all three replicas' application fences passed. [Independent counter and affinity checks](https://github.com/djosh34/tigerstore/blob/ece9ff4dd212e3cd9130eb85aed78ab9955a440b/evidence/bench75/run-38387706444/validation.txt) recompute CPU, I/O and allocation deltas from the raw JSON.

The first dispatch, [38387063293](https://github.com/djosh34/tigerstore/actions/runs/38387063293), failed setup for both Put jobs because the deleted PR 77 branch resolved to an empty candidate. No Put benchmark started. Its CAS job completed, but none of that run enters these medians. [Its logs and CAS evidence](https://github.com/djosh34/tigerstore/tree/ece9ff4dd212e3cd9130eb85aed78ab9955a440b/evidence/bench75/run-38387063293) are retained. The resolver now rejects failed lookups and accepts retained PR-head refs. The fresh full matrix above passed. Nothing was merged.
