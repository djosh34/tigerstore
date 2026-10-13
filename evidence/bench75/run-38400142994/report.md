PR 84 did not reduce median leader CPU per Put. Main and PR 84 both used **23.50 µs at 64 clients and 13.50 µs at 256**. At 256, PR 84's leader read/write counts were **0.1978 / 0.23865 per Put**, versus main's **0.18878 / 0.23495**. That is 4.8% more reads and 1.6% more writes, not a read/write syscall win in this run.

[Actions run 38400142994](https://github.com/djosh34/tigerstore/actions/runs/38400142994), [raw evidence](https://github.com/djosh34/tigerstore/tree/60bb67e1e7b393b0e76f0c4c43214c9ad31e5129/evidence/bench75/run-38400142994), [per-repetition CSV](https://github.com/djosh34/tigerstore/blob/60bb67e1e7b393b0e76f0c4c43214c9ad31e5129/evidence/bench75/run-38400142994/runs.csv), [medians](https://github.com/djosh34/tigerstore/blob/60bb67e1e7b393b0e76f0c4c43214c9ad31e5129/evidence/bench75/run-38400142994/summary.csv), [independent counter and affinity checks](https://github.com/djosh34/tigerstore/blob/60bb67e1e7b393b0e76f0c4c43214c9ad31e5129/evidence/bench75/run-38400142994/validation.txt).

## Leader CPU and I/O headline

| Clients | Store | Leader CPU µs/Put | Leader syscr/Put | Leader syscw/Put |
|---:|---|---:|---:|---:|
| 64 | main | 23.50 | 0.71762 | 0.52547 |
| 64 | PR 84 | 23.50 | 0.71838 | 0.52392 |
| 64 | etcd 3.7.2 | 174.50 | 0.88208 | 4.66678 |
| 256 | main | 13.50 | 0.18878 | 0.23495 |
| 256 | PR 84 | 13.50 | 0.19780 | 0.23865 |
| 256 | etcd 3.7.2 | 186.33 | 0.26082 | 4.10628 |

At 64 clients, reads changed by +0.1% and writes by -0.3%. Throughput medians were 0.7% higher at 64 and 2.8% higher at 256, but leader CPU medians were unchanged and there was no 256-client read/write reduction. These are five-repetition point comparisons, not significance or capacity claims. Read/write I/O counters do not measure all timer, futex, epoll or clock syscalls.

## Method and heads

The resolver pinned main `aa9d9b6eea79c73f12fe0153aefa809841967e88` and PR 84 `d5681651cd554b8804ed3a77b9ff019ade6815bf` at 2026-10-13T12:49:30Z. The candidate came from retained `refs/pull/84/head`, corresponding to `work/50-readclock`. Main includes PR 79. Etcd is stock v3.7.2, commit `68c065e562994b89e333e77b039ad066f933c586`, executable SHA256 `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d`. Later heads do not replace these measurements.

Both jobs used free `ubuntu-24.04-arm` runners, image `20261004.142.1`. Put 64 used `GitHub Actions 1000009831`; Put 256 used `GitHub Actions 1000009832`. Within each VM, leader CPU 0, follower CPUs 1 and 2, generator CPU 3. Three replicas and the generator share one VM/disk. TLS 1.3 with client certificates and ext4 disk barriers remain enabled. The environment sets `GOMAXPROCS=1`; tigerstore forces two Go processors on its one pinned CPU, while etcd and the generator retain one. Tigerstore/generator use Go 1.27.2, stock etcd Go 1.26.8. Identical private observation hooks change no frontend/core/storage behavior. No profiles or forced GC ran in comparison windows.

Every store, including etcd, used the same lean engine at harness commit `6148beab08e91a02c243ccd2d2b788ecdacd45fd`. `experiments/bench75/load/newlean.go` is byte-identical to the preceding PR 80 lean run. It adapts `exp/pre-47-grpc` commit `d2408301966b99471bfed06d4c2dc1cd1e5af462`, `experiments/grpc/lean.go`. It has prebuilt requests, warm HPACK, coalesced writes and two goroutines per active connection. There are 16 active TLS connections, four or sixteen closed-loop slots each, plus one idle grpc-go control connection for untimed probes. Raw headers omit grpc-timeout; a local 10-second stall guard remains. Receive credit coalesces at 64 KiB. Actual generator CPU appears next to server CPU below, not an assumed 2.8 µs/Put.

All 4,096 Kubernetes-shaped keys, independent SHA-derived 1,024-byte values reused on overwrite, and global key permutation are unchanged. Every fresh cluster runs 4,096 seed Puts, 20,000 warmup Puts, a pause and 60,000 measured Puts. Each subprocess also completes 16 untimed warm Puts before revision/accounting snapshots to establish every raw connection's HPACK and settings state. Those writes are outside the requested counts. Five repetitions alternate main, PR 84, etcd and the reverse order, back to back within each job. There are no retries or timed pre-reads. Medians are medians of five per-run statistics, not pooled latency percentiles.

CPU is process user-plus-system `/proc/PID/stat` deltas divided by completed Puts. Total server CPU sums the three processes, not their threads. Leader `/proc/PID/io` `syscr` and `syscw` deltas use the CPU window and the same denominator. They cover the entire leader process, not sockets alone or all Syscall6 calls. Leader allocation deltas use runtime object/byte counters, or stock etcd's Prometheus memstats. GC share is estimated GC CPU divided by estimated non-idle runtime CPU, not pause time or `/proc` CPU; etcd lacks that exposed counter. Allocation snapshots bracket a slightly wider interval. Replica fences remain outside the CPU window, so trailing follower work can fall outside its sum.

## Throughput, latency and process costs

Latency is milliseconds. CPU is microseconds per Put. Allocations and GC share are leader process totals.

### Put, 64 clients

| Store | Put/s | Mean / p50 / p99 / p99.9 ms | Leader / total CPU µs/Put | Generator µs/Put / cores | Leader objects / bytes per Put | GC CPU share |
|---|---:|---:|---:|---:|---:|---:|
| main | 34,911 | 1.830 / 1.771 / 3.343 / 6.938 | 23.50 / 48.33 | 9.17 / 0.32 | 15.21 / 5,491 | 1.32% |
| PR 84 | 35,169 | 1.816 / 1.777 / 3.743 / 6.448 | 23.50 / 48.33 | 9.17 / 0.33 | 15.27 / 5,485 | 1.41% |
| etcd 3.7.2 | 4,734 | 13.517 / 12.997 / 22.196 / 26.426 | 174.50 / 261.00 | 39.33 / 0.18 | 272.53 / 33,375 | n/a |

### Put, 256 clients

| Store | Put/s | Mean / p50 / p99 / p99.9 ms | Leader / total CPU µs/Put | Generator µs/Put / cores | Leader objects / bytes per Put | GC CPU share |
|---|---:|---:|---:|---:|---:|---:|
| main | 65,342 | 3.903 / 3.760 / 8.635 / 11.090 | 13.50 / 26.00 | 6.33 / 0.41 | 10.79 / 5,367 | 2.74% |
| PR 84 | 67,159 | 3.795 / 3.616 / 8.814 / 11.289 | 13.50 / 26.00 | 6.17 / 0.42 | 10.80 / 5,373 | 2.54% |
| etcd 3.7.2 | 4,424 | 57.811 / 55.619 / 101.648 / 125.932 | 186.33 / 250.50 | 38.17 / 0.17 | 260.30 / 33,383 | n/a |

The tigerstore generator occupied roughly 0.32 cores at 64 clients and 0.41 at 256. The earlier grpc-go 256-client Put generator used about 0.96 cores, so that earlier throughput may have been generator-bound and leader CPU per Put is the better comparison there. Do not attribute cross-run throughput changes to PR 84. The earlier grpc-go run used different client framing/headers and different server heads. These remain fixed-concurrency results, not capacity limits.

All 30 windows passed with 1,800,000 completed Puts and no RPC errors. PID identities, monotonic counters, revision growth, leadership/term stability and every server thread's affinity passed. All three replicas applied each measured revision. Etcd also passed its post-WAL-save committed-gauge fences. Benchmarks ran only on Actions; nothing was merged by this work.

## Source counts, separate from runtime results

[Final audit](https://github.com/djosh34/tigerstore/blob/60bb67e1e7b393b0e76f0c4c43214c9ad31e5129/evidence/bench75/run-38400142994/source-audit/audit.md) and [every counted site with exact source text](https://github.com/djosh34/tigerstore/blob/60bb67e1e7b393b0e76f0c4c43214c9ad31e5129/evidence/bench75/run-38400142994/source-audit/verified-sites.md). PR 84 introduces no new request-value buffer. Known leader full-value stages per Put remain **4 on both heads**: accumulation `internal/server/stream.go:127`, entry encoding `internal/core/codec.go:68`, retained Raft clone through `internal/core/node.go:585`, and WAL staging `internal/wal/wal.go:351`. The clone uses raft v3.7.0 `raft.go:817` and protobuf v1.36.11 `internal/impl/merge.go:188`. Put frontend/apply parsing and index assignment alias bytes, not another value materialization. Replication adds a sender encoding at `internal/peer/peer.go:426` per transmitted occurrence; a newly received/persisted/applied follower has peer decode `internal/peer/peer.go:248` and WAL staging, two stages. No fixed fanout or batch size is assumed.

Both heads retain **0 fixed request-private sends, 0 receives, 0 closes and 0 fresh reply channels**. Additional admission, disk, peer, writer-wake and tick work is batch-amortized or conditional. Main cohort sends are `A+Q+F+O+I+U+Sw`; receives `A+Q+F+O+I+U+Rw+Ticks`, divided by N Puts. A counts actual admission batches, Q persist arrays, F sync groups, O/I peer messages, U consumed Unreachable reports, Sw/Rw coalesced writer wakes, and Ticks background ticks. The audit names every site at both commits, including the shifted line numbers. These populations were not instrumented, so no numerical total is invented.

PR 84 adds **K_s successful Taken sends and K_r selected Taken receives per connection cohort**, on top of that formula. It creates one capacity-one channel per connection at `internal/server/h2.go:125`, not one per Put. The send is `internal/server/batch.go:215`; receive `internal/server/batch.go:231`. The node's direct Taken callback is `internal/node/node.go:453`. It follows admission into SubmitAll, not application or disk completion. The reader avoids the receive when a complete next frame is already buffered at `internal/server/batch.go:227`. Sends can coalesce and tokens have no batch IDs. This is not a fixed exchange per Put, batch, frame or syscall. Normal Taken closes are zero; lifecycle closes remain outside scope.

Stock etcd's [unchanged release census](https://github.com/djosh34/tigerstore/blob/ece9ff4dd212e3cd9130eb85aed78ab9955a440b/evidence/bench75/run-38387706444/etcd-source-audit/audit.md) has **3 fixed core sends, 3 receives, 2 closes and 2 fresh reply channels**, plus actual Ready/scheduler/peer work. Its known immediate leader value subtotal is **8 plus conditional codec coalescing**, including WAL PageWriter. Deferred bbolt page serialization and peer occurrences are separate. Timed raw headers omit grpc-timeout, so the old extra grpc deadline-synchronization close does not execute. The release audit retains all implementing sites. The [lean scope correction](https://github.com/djosh34/tigerstore/blob/ba54bee9de85f61162c52869950d02a153dacf6c/evidence/bench75/run-38397810888/source-audit/lean-etcd-scope.md) records the changed deadline and conditional coalescing scope.

These are named full-value stages, not physical memcpy totals or allocations. Struct/slice assignments, TLS, transport buffers, kernel copies and unenumerated growth/lifecycle work remain excluded, not zero. Tigerstore 4 versus etcd 8 is not a physical-copy ratio. Source does not establish actual syscall counts or a CPU win; those headline numbers come from the measurement windows.
