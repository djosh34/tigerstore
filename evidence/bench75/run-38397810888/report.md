PR 80's restored frontend decoder used the same median leader CPU as main: 11.67 µs/Put and 19.33 µs/CAS Txn. Put throughput was 73.2k versus 72.3k/s; CAS was 44.5k versus 45.3k/s. CAS allocated 6.8% fewer bytes but 12.2% more objects, with 6.7% more total server CPU per Txn.

[Actions run 38397810888](https://github.com/djosh34/tigerstore/actions/runs/38397810888), [raw evidence](https://github.com/djosh34/tigerstore/tree/ba54bee9de85f61162c52869950d02a153dacf6c/evidence/bench75/run-38397810888), [five repetitions](https://github.com/djosh34/tigerstore/blob/ba54bee9de85f61162c52869950d02a153dacf6c/evidence/bench75/run-38397810888/runs.csv), [medians](https://github.com/djosh34/tigerstore/blob/ba54bee9de85f61162c52869950d02a153dacf6c/evidence/bench75/run-38397810888/summary.csv), [counter/affinity validation](https://github.com/djosh34/tigerstore/blob/ba54bee9de85f61162c52869950d02a153dacf6c/evidence/bench75/run-38397810888/validation.txt).

## Heads and placement

The resolver pinned main `aa9d9b6eea79c73f12fe0153aefa809841967e88` and PR 80 `67ff792a72f78e83cb6905563dcb6bc0e9bb2764` at 2026-10-13T12:30:50Z. Main includes PR 79. Stock etcd v3.7.2 is `68c065e562994b89e333e77b039ad066f933c586`, executable SHA256 `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d`.

Both jobs used free `ubuntu-24.04-arm` runners, image `20261004.142.1`. Put used `GitHub Actions 1000009786`; CAS used `GitHub Actions 1000009785`. Leader CPU 0, followers CPUs 1 and 2, generator CPU 3. Three replicas and the generator share one VM/disk. TLS 1.3 with client certificates and ext4 disk barriers remain enabled. The environment sets `GOMAXPROCS=1`; tigerstore forces two Go processors on its one pinned CPU, while etcd and the generator retain one. Tigerstore/generator use Go 1.27.2; stock etcd uses Go 1.26.8. The identical private loopback observation hook changes no frontend/core/storage behavior. No profiles or forced GC ran in these comparison windows.

## Lean generator and method

Every store, including etcd, used `experiments/bench75/load/newlean.go` at `e3bc29f5de3b9184b8aae5f64ed290681d6e1d6f`. It adapts `exp/pre-47-grpc` commit `d2408301966b99471bfed06d4c2dc1cd1e5af462`, `experiments/grpc/lean.go`. It has prebuilt Put requests and CAS comparison/branch templates, warm HPACK, coalesced writes, and two goroutines per active connection. CAS encodes only its changing positive mod revision and length as minimal varints into reusable scratch. There are 16 active TLS connections, 16 outstanding slots each, plus one idle grpc-go control connection for untimed probes/cache loading. All 4,096 Kubernetes-shaped keys and independent 1,024-byte SHA-derived values are unchanged, including the global key permutation. There are no client retries or timed pre-reads.

Each subprocess completes one valid untimed warm RPC on each raw connection before its revision and accounting snapshots. Those 16 warm writes are outside its requested count. Each fresh cluster then runs 4,096 seed Puts, 20,000 warmup operations, a pause and 60,000 measured operations. Five repetitions alternate main, PR 80, etcd and the reverse order. Tables give medians of five per-run statistics, not pooled percentiles.

Raw headers omit grpc-timeout. The client retains a local 10-second stall guard. Connection receive credit coalesces at 64 KiB; small unary replies send no per-stream credit update. The cached write deadline renews only in its final five seconds. These framing/header changes mean the throughput jump from the previous grpc-go run cannot be credited to the PR 80 frontend reset. In that previous 256-client Put run, the generator occupied about **0.96 cores**, so Put throughput may have been generator-bound and leader CPU per Put is the better comparison. This adaptation measured **5.33 µs/Put and 0.39 cores on PR 80**, rather than assuming the earlier pre47 figure of about 2.8 µs. The table includes the actual generator cost for every store. These remain fixed-concurrency results, not capacity limits.

## Results

Latency is milliseconds. CPU is process user-plus-system `/proc/PID/stat` deltas divided by completed operations. Total server CPU sums three processes. Leader allocations use cumulative object/byte deltas; stock etcd uses Prometheus memstats. GC share is the runtime's estimated GC CPU divided by estimated non-idle runtime CPU, not pause time or a ratio to `/proc`; etcd lacks that exposed counter.

Leader read/write syscall columns are `/proc/PID/io` `syscr`/`syscw` deltas over the CPU window, divided by completed operations. They cover the entire leader process, not only sockets or all Syscall6 calls. Allocation snapshots bracket a slightly wider interval. Later replica fences stay outside the CPU window, so trailing follower work can fall outside the sum.

### Put, 256 clients

| Store | Put/s | Mean / p50 / p99 / p99.9 ms | Leader / total CPU µs/Put | Generator µs/Put / cores | Leader objects / bytes per Put | GC CPU share | Leader syscr / syscw per Put |
|---|---:|---:|---:|---:|---:|---:|---:|
| main | 72,348 | 3.525 / 3.311 / 7.491 / 10.748 | 11.67 / 21.83 | 5.50 / 0.39 | 10.81 / 5,362 | 2.45% | 0.1863 / 0.2344 |
| PR 80 | 73,180 | 3.483 / 3.286 / 7.258 / 9.030 | 11.67 / 22.33 | 5.33 / 0.39 | 10.79 / 5,454 | 3.02% | 0.1876 / 0.2340 |
| etcd 3.7.2 | 5,851 | 43.728 / 42.808 / 72.606 / 84.681 | 148.50 / 201.50 | 33.50 / 0.20 | 260.33 / 33,140 | n/a | 0.2520 / 4.1464 |

### Txn, 256 clients

| Store | Txn/s | Mean / p50 / p99 / p99.9 ms | Leader / total CPU µs/Txn | Generator µs/Txn / cores | Leader objects / bytes per Txn | GC CPU share | Leader syscr / syscw per Txn |
|---|---:|---:|---:|---:|---:|---:|---:|
| main | 45,346 | 5.621 / 5.334 / 13.287 / 17.407 | 19.33 / 34.83 | 6.67 / 0.29 | 57.35 / 10,611 | 3.77% | 0.2391 / 0.2415 |
| PR 80 | 44,463 | 5.736 / 5.534 / 11.533 / 14.568 | 19.33 / 37.17 | 6.67 / 0.30 | 64.35 / 9,892 | 4.28% | 0.2329 / 0.2425 |
| etcd 3.7.2 | 4,978 | 51.391 / 50.463 / 82.343 / 95.547 | 174.00 / 240.50 | 34.67 / 0.17 | 328.91 / 40,552 | n/a | 0.2574 / 4.1719 |

All three stores completed 300,000 measured CAS Txns each, with 300,000 compare passes and zero compare failures. The existing-key MOD_EQUAL comparison uses a positive cached mod revision, then Put 1 KiB, else Range of that key. Cache revisions only advance. Revision growth equalled compare passes. The failure arm was present but not exercised, so there is no failed-CAS latency result. CAS costs divide by completed transactions.

All 30 windows passed with 1,800,000 completed operations and no RPC errors. PID identities, counters, leadership/term checks, pinned thread affinities and all-replica application fences passed. Etcd also passed the post-WAL-save committed-gauge fences. No local benchmarks or heavy tests ran; nothing was merged by this benchmark work.

## Source counts, not runtime metrics

[Final source audit](https://github.com/djosh34/tigerstore/blob/ba54bee9de85f61162c52869950d02a153dacf6c/evidence/bench75/run-38397810888/source-audit/audit.md) and [every exact verified site](https://github.com/djosh34/tigerstore/blob/ba54bee9de85f61162c52869950d02a153dacf6c/evidence/bench75/run-38397810888/source-audit/verified-sites.md). Known leader full-value stages are **Put 4 on both heads; CAS 6 on main, 5 on PR 80**. PR 80 now owns the frontend-decoded bytes again and removes only the owning apply decode. It still rebuilds request structs at apply.

Every counted input stage is request accumulation `internal/server/stream.go:127`; entry encoding `internal/core/codec.go:68` for Put and `internal/core/codec.go:73` for CAS; retained Raft clone via `internal/core/node.go:585` on main and `internal/core/node.go:591` on PR 80, implemented by raft v3.7.0 `raft.go:817` and protobuf v1.36.11 `internal/impl/merge.go:188`; WAL staging `internal/wal/wal.go:351`. CAS adds frontend owning decode at `internal/server/rpc.go:63` on both. Main also owns apply bytes at `internal/core/node.go:865`. Both owning decodes use protobuf `internal/impl/codec_gen.go:5497`.

A compare failure still logs the full Txn, including its unexecuted Put value. Its old stored-value reply adds one separate encoding at `internal/server/stream.go:207`. The averages would be `6 + f/N` and `5 + f/N` across distinct payloads; measured f was zero. Each transmitted peer occurrence adds an encode at `internal/peer/peer.go:426`; a newly received/persisted/applied follower has peer decode `internal/peer/peer.go:248`, WAL staging, and main's owning CAS apply decode where applicable. Follower subtotals are Put 2 on both; CAS 3 on main and 2 on PR 80. No fixed peer fanout or batch size is assumed.

Both tigerstore heads have **0 fixed request-private sends, 0 receives, 0 closes and 0 fresh reply channels**. Additional sends per consumed cohort are `A+Q+F+O+I+U+Sw`; receives are `A+Q+F+O+I+U+Rw+Ticks`. Divide by N, but do not invent the actual populations. A is admission batches; Q persist arrays; F sync groups; O/I peer messages; U consumed Unreachable reports; Sw/Rw coalesced wakes; Ticks background ticks. Every counted site is:

| Handoff | Send | Receive |
|---|---|---|
| A | `internal/node/node.go:136` | `internal/node/node.go:335` or `internal/node/node.go:340` |
| Q | `internal/node/node.go:353` | `internal/node/node.go:486` or `internal/node/node.go:493` |
| F | `internal/node/node.go:507` | `internal/node/node.go:346` |
| O | `internal/peer/peer.go:304` | `internal/peer/peer.go:315`, `internal/peer/peer.go:321` or `internal/peer/peer.go:324` |
| I | `internal/node/node.go:266` | the same inbox receive alternatives |
| U | `internal/node/node.go:274` | the same inbox receive alternatives |
| Sw/Rw | `internal/server/h2.go:201` | `internal/server/write.go:52` |
| Ticks | none | `internal/node/node.go:357` |

Grouped completion is a direct call plus an atomic list, not another channel. Send/receive alternatives count once per consumed message; queued work can cross a timed boundary.

[The exact stock etcd audit](https://github.com/djosh34/tigerstore/blob/ece9ff4dd212e3cd9130eb85aed78ab9955a440b/evidence/bench75/run-38387706444/etcd-source-audit/audit.md) and its linked verified sites enumerate **3 fixed core sends, 3 receives, 2 closes and 2 fresh reply channels**, plus actual Ready/scheduler/peer populations. Its known immediate stages remain Put **8 new-input**, successful CAS **8 new-input plus 1 stored-value decode**, and conditional failed CAS **7 new-input plus 3 stored-value stages**. The counted WAL PageWriter stage remains included. Optional codec coalescing, peers and deferred bbolt pages remain separate populations. [The lean scope addendum](https://github.com/djosh34/tigerstore/blob/ba54bee9de85f61162c52869950d02a153dacf6c/evidence/bench75/run-38397810888/source-audit/lean-etcd-scope.md) removes the old extra grpc timeout-synchronization close because raw headers have no grpc-timeout. Codec coalescing stays conditional because flow control can fragment DATA.

These are named materialization stages, not physical memcpy totals or allocations. Slice/struct assignments, TLS, transport buffers, kernel copies and unenumerated growth/lifecycle work remain excluded, not zero. Tigerstore 4 versus etcd 8 is not a physical-copy ratio.
