## Paired Put benchmark and leader profiles

[Run 38372738426](https://github.com/djosh34/tigerstore/actions/runs/38372738426) passed. Separate free `ubuntu-24.04-arm` jobs used 64 and 256 clients. Each ran main, PR 76, PR 77 and stock etcd v3.7.2 back to back for five repetitions, reversing the order on even repetitions. All 40 measured windows passed the success-count, error, revision, term, leadership and affinity checks. The PR 77 profiles came from another fresh cluster after the comparison windows.

The newest branch heads resolved at 08:56:32Z were:

| Store | Measured commit |
| --- | --- |
| Main | `e1fd889c345bc3c06d8100eb6d635c88ad6967cd` |
| PR 76, `work/50-batch` | `29c3f8f95e3c9bcecf5bb39aa1f3bca49fe99c1f` |
| PR 77, `work/50-wire` | `f2edb7b2b1e81f65d7ca7592638654f09217b933` |
| Etcd v3.7.2 release | `68c065e562994b89e333e77b039ad066f933c586` |

Both runners had four Neoverse-N2 cores, 16.72 GB RAM, image `ubuntu24-arm64 20261004.142.1` and Linux `6.17.0-1022-azure`. Runner names were `GitHub Actions 1000008442` for 64 clients and `GitHub Actions 1000008443` for 256. Leader CPU 0, followers CPUs 1 and 2, generator CPU 3, with every server thread's affinity checked. Tigerstore used its existing minimum of two Go processors on each pinned CPU; etcd and the generator used one. Tigerstore and the generator used Go 1.27.2; stock etcd used Go 1.26.8.

The same direct protobuf/grpc-go v1.84.0 closed-loop generator drove all stores through 16 established TLS 1.3 connections with client certificates, leader endpoint only. Each fresh cluster received 4,096 seed Puts, 20,000 warmup Puts and 60,000 measured Puts. Values were 1 KiB, across 4,096 Kubernetes-shaped keys, with independent deterministic values reused on overwrite. Peer links also used certificate-authenticated TLS. No watches, leases, compaction or injected delay. Etcd kept safe persistence defaults and an 8 GiB quota. Both runners remounted their ext4 data filesystem with `barrier=1` and rejected `nobarrier` or `barrier=0` afterward.

### Medians of five repetitions

CPU is process user plus system ticks from `/proc/PID/stat`, with process-start identity checks and `CLK_TCK=100`. Total server CPU sums the three processes, not their threads. Latencies are medians of per-run percentiles, not pooled percentiles.

| Clients | Store | Put/s | p50 ms | p99 ms | Leader CPU us/Put | Total server CPU us/Put |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 64 | Main | 9,907.2 | 6.144 | 14.001 | 98.50 | 127.83 |
| 64 | PR 76 | 26,092.3 | 2.280 | 5.950 | 33.67 | 62.67 |
| 64 | PR 77 | 29,295.7 | 2.043 | 5.072 | 29.50 | 56.67 |
| 64 | Etcd | 4,535.6 | 13.664 | 24.040 | 170.33 | 251.67 |
| 256 | Main | 8,144.0 | 29.573 | 66.382 | 122.00 | 147.67 |
| 256 | PR 76 | 33,877.0 | 6.850 | 16.668 | 27.33 | 48.33 |
| 256 | PR 77 | 38,783.0 | 6.002 | 14.117 | 24.00 | 43.00 |
| 256 | Etcd | 3,879.9 | 65.147 | 105.393 | 203.83 | 274.33 |

PR 77's median rate was 12.3% above PR 76 at 64 clients and 14.5% above it at 256. It was 2.96x and 4.76x main, 6.46x and 10.00x etcd. These ratios compare stores inside the same job. PR 76's head has changed since the earlier PR 75/76 report; these are not controlled cross-run comparisons with that report.

### Leader allocations and GC

These are whole-process cumulative allocation deltas divided by 60,000 successful Puts. Tigerstore uses `runtime/metrics`; etcd uses `go_memstats_mallocs_total` and `go_memstats_alloc_bytes_total`.

| Clients | Store | Objects/Put | Bytes/Put | GC CPU share | Heap object MiB after window |
| ---: | --- | ---: | ---: | ---: | ---: |
| 64 | Main | 139.28 | 21,964 | 1.32% | 254.8 |
| 64 | PR 76 | 41.12 | 13,865 | 2.14% | 255.5 |
| 64 | PR 77 | 30.75 | 6,593 | 1.32% | 156.6 |
| 64 | Etcd | 277.80 | 32,691 | Unavailable | 35.8 |
| 256 | Main | 138.80 | 21,807 | 1.55% | 223.4 |
| 256 | PR 76 | 36.28 | 14,001 | 3.76% | 271.0 |
| 256 | PR 77 | 26.16 | 6,423 | 2.03% | 149.1 |
| 256 | Etcd | 265.06 | 33,556 | Unavailable | 36.0 |

Against PR 76, PR 77 reduced allocated bytes per Put by 52.4% and 54.1%, and objects by 25.2% and 27.9%. GC share is the runtime's estimated GC CPU delta divided by its estimated non-idle CPU delta, not pause time or a fraction of `/proc` CPU. Etcd's stock endpoint has no cumulative GC CPU metric. The allocation snapshot interval is slightly wider than the CPU window. Heap bytes after the window are observed object bytes without forced GC, not a minimum live set.

### Channels and copies, counted from code

The [source audit](https://github.com/djosh34/tigerstore/blob/40613c0f6eafa2ff82debc7c79f267dcb8c8a73f/evidence/bench75/run-38372738426/source-audit/audit.md) verifies the exact measured commits and every counted site. These are source counts, not runtime measurements. A send and a receive count separately; failed nonblocking attempts do not count. A value copy moves all 1 KiB into another backing store, not a slice header.

| Leader cost per Put | Main | PR 76 | PR 77 |
| --- | --- | --- | --- |
| Fixed request/reply channel operations | 4, two sends and two receives | 0 on batch Put path | 0 on batch Put path |
| Fresh explicit reply channels | 1 | 0 on batch Put path | 0 on batch Put path |
| Admission channel operations | Included above | 2 per actual admission batch, divided by Puts | Same |
| Completion handoff | Included above | Direct grouped call and atomic list, no channel operation | Same |
| Known leader full-value materialization stages before peer sends | 5 | 6 | 4 |
| Known follower stages for one newly received, persisted and applied occurrence | 3 | 3 | 2 |

Channel counts remain batch-dependent. For N fully accounted Puts, A admission batches, Q disk-queue handoffs, F disk-completion handoffs, O outbound and I inbound peer messages, T ticker receives, and W actual writer-wake sends plus receives, the repository-level counts per Put are `4 + (2Q+2F+2O+2I+T)/N` on main and `(2A+2Q+2F+2O+2I+T+W)/N` on both candidates. Those populations were not instrumented, so no exact total or assumed batch size is reported. At these heads the old `room` channel is gone. DATA-frame credit and completion groups can both signal the coalesced writer wake. No explicit application channel closes occur per normal Put; library/context lifecycle channels are outside this census.

Every ordinary-path channel site is named below. Alternative receive sites count once, not all alternatives for a single handoff.

| Work | Main sites | PR 76 sites | PR 77 sites |
| --- | --- | --- | --- |
| Admission send and inbox receive | `internal/node/node.go:122`, `:264` or `:269` | `internal/node/node.go:136`, `:333` or `:338` | `internal/node/node.go:136`, `:335` or `:340` |
| Reply send and receive | `internal/node/node.go:301`, `:217` | Not executed by batch Put | Not executed by batch Put |
| Disk queue send and receive | `internal/node/node.go:282`, `:369` or `:376` | `internal/node/node.go:351`, `:483` or `:490` | `internal/node/node.go:353`, `:486` or `:493` |
| Synced-group send and receive | `internal/node/node.go:390`, `:275` | `internal/node/node.go:504`, `:344` | `internal/node/node.go:507`, `:346` |
| Outbound peer queue send and receive | `internal/peer/peer.go:294`, `:305` or `:311` or `:314` | Same | `internal/peer/peer.go:297`, `:308` or `:314` or `:317` |
| Inbound peer inbox send | `internal/node/node.go:197`, with inbox receives above | `internal/node/node.go:264`, with inbox receives above | `internal/node/node.go:266`, with inbox receives above |
| Writer wake send and receive | HTTP/gRPC library internals excluded | `internal/server/h2.go:190`, `internal/server/write.go:54` | Same |
| Background ticker receive | `internal/node/node.go:286` | `internal/node/node.go:355` | `internal/node/node.go:357` |

The named leader value-copy stages are:

| Stage | Main | PR 76 | PR 77 |
| --- | --- | --- | --- |
| Custom frontend message accumulation | HTTP/gRPC buffering excluded | 1, `internal/server/stream.go:127` | 1, same site |
| Frontend owning protobuf decode | 1, grpc-go `encoding/proto/proto.go:96` | 1, `internal/server/rpc.go:62` | 0, `internal/server/batch.go:59` parses in place |
| Encode into entry | 1, `internal/core/codec.go:64` | 1, same site | 1, `internal/core/codec.go:68` appends raw wire |
| Raft entry deep clone | 1, `internal/core/node.go:522` | 1, same site | 1, `internal/core/node.go:571` |
| WAL staging append | 1, `internal/wal/wal.go:351`, reached at `:368` | 1, same sites | 1, same sites |
| Apply owning protobuf decode | 1, `internal/core/codec.go:114` | 1, same site | 0, `internal/core/node.go:839`, `:844`, `:880` walks and parses in place |
| Peer sender serialization | 1 per transmitted occurrence per recipient, `internal/peer/peer.go:416` | Same | Same count, `internal/peer/peer.go:419` |

The owning Put decode copies bytes at protobuf v1.36.11 `internal/impl/codec_gen.go:5497`. Raft v3.7.0 still clones at `raft.go:817`; its proto2 Entry.Data copy is protobuf `internal/impl/merge.go:188`. PR 77 does not remove that clone. Follower entry decode remains at `internal/peer/peer.go:238` on main and PR 76, `:241` on PR 77; WAL staging remains too. Only its apply owning value decode disappears. PR 77's index assignment at `internal/mvcc/ops.go:259` retains an entry-backed value slice and adds no payload copy. Ordinary Put replies contain no input value.

Thus 5/6/4 are known leader materialization subtotals, plus peer-send multiplicity and conditional growth or transport copies. They are not physical memcpy totals. TLS, bufio, sockets/kernel and main's HTTP/gRPC buffering are excluded, not zero. PR 77 removes two known leader value materializations and one on each follower. It also pre-sizes entry encoding, removing that buffer-growth mechanism. Put applies in place and its index shares in-memory Raft-entry bytes; this is not an on-disk segment-offset index or an all-command no-decode path.

### Separate 256-client profiles

The matching PR 77 head used a fresh cluster with the same seed and warmup, followed by 25 seconds of closed-loop arrivals through drain. The driver waited for the generator's arrival-start marker, then requested a 20-second leader CPU profile and a concurrent 20-second allocation-delta profile one second later. The captures were 09:02:01Z to 09:02:21Z. They did not overlap any comparison window.

The longer profiled workload completed 950,277 Puts in 25.005 seconds, 38,003.4 Put/s, with zero reported errors and valid revision/term/leader/affinity checks. This is a diagnostic run, not a sixth repetition. The CPU profile has 17.23 sampled CPU seconds. The allocation profile has 3,569.44 MB and 15,962,340 estimated allocated objects over its 20.01-second delta. Go's default heap sampling remains enabled; delayed heap-profile accounting can include earlier warmup records. These profile estimates are not the exact allocation counters in the comparison table, and are not divided by the longer run's Put count.

The largest flat allocation-byte sites are Raft's `mergeBytes`, frontend `receive` and `encodeBatch`. They agree with the retained materialization stages above. Cumulative rows include descendants and overlap, so do not add them. `Syscall6` includes different syscall callers; its flat share is not an fsync-only cost.

The six listings below show the top 25 functions by flat and cumulative cost for CPU, `alloc_space` and `alloc_objects`, generated on the runner with `go tool pprof -top -nodecount=25 -nodefraction=0 -edgefraction=0`, the matching binary and the stated sample index.

#### CPU, top 25 flat

```text
File: second
Build ID: 20d62c56c5ef7b16a45c9629dd891614f9a54be8
Type: cpu
Time: 2026-10-13 09:02:01 UTC
Duration: 20s, Total samples = 17.23s (86.15%)
Showing nodes accounting for 11.58s, 67.21% of 17.23s total
Showing top 25 nodes out of 662
      flat  flat%   sum%        cum   cum%
     7.14s 41.44% 41.44%      7.14s 41.44%  internal/runtime/syscall/linux.Syscall6
     0.83s  4.82% 46.26%      0.83s  4.82%  runtime.memmove
     0.42s  2.44% 48.69%      0.42s  2.44%  cmpbody
     0.37s  2.15% 50.84%      0.37s  2.15%  crypto/internal/fips140/aes/gcm.gcmAesEnc
     0.28s  1.63% 52.47%      0.28s  1.63%  runtime.memclrNoHeapPointers
     0.23s  1.33% 53.80%      0.23s  1.33%  crypto/internal/fips140/aes/gcm.gcmAesDec
     0.20s  1.16% 54.96%      0.55s  3.19%  runtime.selectgo
     0.19s  1.10% 56.07%      0.19s  1.10%  runtime.futex
     0.17s  0.99% 57.05%      0.17s  0.99%  internal/runtime/maps.ctrlGroupMatchH2 (inline)
     0.17s  0.99% 58.04%      0.17s  0.99%  runtime.nextFreeFast (inline)
     0.16s  0.93% 58.97%      0.20s  1.16%  runtime.unlock2
     0.15s  0.87% 59.84%      0.17s  0.99%  runtime.lock2
     0.14s  0.81% 60.65%      0.16s  0.93%  runtime.findObject
     0.12s   0.7% 61.35%      0.84s  4.88%  github.com/djosh34/tigerstore/internal/mvcc.(*writer).latest
     0.12s   0.7% 62.04%      0.12s   0.7%  golang.org/x/net/http/httpguts.isCTL (inline)
     0.11s  0.64% 62.68%      0.53s  3.08%  github.com/djosh34/tigerstore/internal/mvcc.New.func1
     0.10s  0.58% 63.26%      1.06s  6.15%  golang.org/x/net/http2.(*Framer).ReadFrameForHeader
     0.10s  0.58% 63.84%      0.28s  1.63%  runtime.scanObject
     0.09s  0.52% 64.36%      0.09s  0.52%  internal/sync.(*Mutex).Unlock (inline)
     0.09s  0.52% 64.89%      0.09s  0.52%  runtime.(*mspan).moveInlineMarks
     0.08s  0.46% 65.35%      0.56s  3.25%  github.com/djosh34/tigerstore/internal/server.(*conn).newStream
     0.08s  0.46% 65.82%      0.60s  3.48%  github.com/google/btree.items[go.shape.*uint8].find.func1 (inline)
     0.08s  0.46% 66.28%      0.08s  0.46%  internal/sync.(*Mutex).Lock
     0.08s  0.46% 66.74%      0.11s  0.64%  runtime.(*mspan).writeHeapBitsSmall
     0.08s  0.46% 67.21%      0.90s  5.22%  runtime.growslice
```

#### CPU, top 25 cumulative

```text
File: second
Build ID: 20d62c56c5ef7b16a45c9629dd891614f9a54be8
Type: cpu
Time: 2026-10-13 09:02:01 UTC
Duration: 20s, Total samples = 17.23s (86.15%)
Showing nodes accounting for 7.35s, 42.66% of 17.23s total
Showing top 25 nodes out of 662
      flat  flat%   sum%        cum   cum%
         0     0%     0%      7.33s 42.54%  sync.(*WaitGroup).Go.func1
     7.14s 41.44% 41.44%      7.14s 41.44%  internal/runtime/syscall/linux.Syscall6
     0.01s 0.058% 41.50%      6.85s 39.76%  syscall.Syscall
     0.01s 0.058% 41.56%      6.75s 39.18%  syscall.RawSyscall6
         0     0% 41.56%      5.61s 32.56%  github.com/djosh34/tigerstore/internal/server.(*Server).Serve.func1
         0     0% 41.56%      5.61s 32.56%  github.com/djosh34/tigerstore/internal/server.(*Server).serveConn
         0     0% 41.56%      5.61s 32.56%  github.com/djosh34/tigerstore/internal/server.(*conn).readLoop
         0     0% 41.56%      5.61s 32.56%  github.com/djosh34/tigerstore/internal/server.(*conn).serve
         0     0% 41.56%      5.61s 32.56%  internal/poll.ignoringEINTRIO (inline)
     0.03s  0.17% 41.73%      3.82s 22.17%  golang.org/x/net/http2.(*Framer).ReadFrame
         0     0% 41.73%      3.51s 20.37%  bufio.(*Writer).Flush
         0     0% 41.73%      3.51s 20.37%  crypto/tls.(*Conn).Write
     0.04s  0.23% 41.96%      3.48s 20.20%  crypto/tls.(*Conn).writeRecordLocked
         0     0% 41.96%      3.33s 19.33%  internal/poll.(*FD).Write
         0     0% 41.96%      3.33s 19.33%  syscall.Write (inline)
     0.01s 0.058% 42.02%      3.33s 19.33%  syscall.write
     0.01s 0.058% 42.08%      3.19s 18.51%  bufio.(*Reader).Read
         0     0% 42.08%      3.19s 18.51%  io.ReadAtLeast
         0     0% 42.08%      3.19s 18.51%  io.ReadFull (inline)
         0     0% 42.08%      3.14s 18.22%  crypto/tls.(*Conn).Read
         0     0% 42.08%      3.05s 17.70%  crypto/tls.(*Conn).readRecord (inline)
     0.07s  0.41% 42.48%      3.05s 17.70%  crypto/tls.(*Conn).readRecordOrCCS
     0.02s  0.12% 42.60%      2.89s 16.77%  crypto/tls.(*Conn).write
         0     0% 42.60%      2.87s 16.66%  net.(*conn).Write
     0.01s 0.058% 42.66%      2.87s 16.66%  net.(*netFD).Write
```

#### Allocated bytes, top 25 flat

```text
File: second
Build ID: 20d62c56c5ef7b16a45c9629dd891614f9a54be8
Type: alloc_space
Time: 2026-10-13 09:02:21 UTC
Duration: 20.01s, Total samples = 3569.44MB 
Showing nodes accounting for 3540.74MB, 99.20% of 3569.44MB total
Showing top 25 nodes out of 134
      flat  flat%   sum%        cum   cum%
  651.55MB 18.25% 18.25%   651.55MB 18.25%  google.golang.org/protobuf/internal/impl.mergeBytes
  639.20MB 17.91% 36.16%   639.20MB 17.91%  github.com/djosh34/tigerstore/internal/server.(*stream).receive
  618.25MB 17.32% 53.48%   618.25MB 17.32%  github.com/djosh34/tigerstore/internal/core.encodeBatch
  316.57MB  8.87% 62.35%   316.57MB  8.87%  golang.org/x/net/http2.(*Framer).readMetaFrame.func1
  200.06MB  5.60% 67.96%   272.57MB  7.64%  github.com/djosh34/tigerstore/internal/server.(*conn).newStream
  143.02MB  4.01% 71.96%   143.02MB  4.01%  github.com/djosh34/tigerstore/internal/server.(*stream).pushLocked
  140.86MB  3.95% 75.91%   140.86MB  3.95%  github.com/djosh34/tigerstore/internal/mvcc.(*writer).add (inline)
   98.46MB  2.76% 78.67%   111.47MB  3.12%  github.com/djosh34/tigerstore/internal/core.(*Node).Submit
   98.11MB  2.75% 81.42%   305.97MB  8.57%  github.com/djosh34/tigerstore/internal/core.(*Node).applyPut
   97.59MB  2.73% 84.15%    98.59MB  2.76%  github.com/djosh34/tigerstore/internal/node.completeAll
   83.51MB  2.34% 86.49%    83.51MB  2.34%  github.com/djosh34/tigerstore/internal/server.(*conn).admit
      79MB  2.21% 88.70%   395.57MB 11.08%  golang.org/x/net/http2.(*Framer).readMetaFrame
   72.51MB  2.03% 90.73%    72.51MB  2.03%  time.newTimer
   65.04MB  1.82% 92.56%   240.06MB  6.73%  github.com/djosh34/tigerstore/internal/server.(*conn).complete
      46MB  1.29% 93.84%       46MB  1.29%  github.com/djosh34/tigerstore/internal/node.(*Node).SubmitBatch
   34.50MB  0.97% 94.81%    34.50MB  0.97%  github.com/djosh34/tigerstore/internal/mvcc.(*writer).latest
   32.50MB  0.91% 95.72%    32.50MB  0.91%  github.com/djosh34/tigerstore/internal/core.(*State).track
      32MB   0.9% 96.62%       32MB   0.9%  github.com/djosh34/tigerstore/internal/server.(*conn).putResponse
   31.50MB  0.88% 97.50%    31.50MB  0.88%  golang.org/x/net/http2.(*frameCache).getDataFrame
      27MB  0.76% 98.26%       27MB  0.76%  golang.org/x/net/http2.parseHeadersFrame
   13.01MB  0.36% 98.62%    13.01MB  0.36%  github.com/djosh34/tigerstore/internal/core.(*Node).queueWrite
       9MB  0.25% 98.87%       11MB  0.31%  go.etcd.io/raft/v3.(*raft).maybeSendAppend
    4.50MB  0.13% 99.00%     4.50MB  0.13%  go.etcd.io/raft/v3.newStorageAppendRespMsg
       4MB  0.11% 99.11%        5MB  0.14%  github.com/djosh34/tigerstore/internal/peer.(*TCP).receive
       3MB 0.084% 99.20%     7.50MB  0.21%  go.etcd.io/raft/v3.newStorageAppendMsg
```

#### Allocated bytes, top 25 cumulative

```text
File: second
Build ID: 20d62c56c5ef7b16a45c9629dd891614f9a54be8
Type: alloc_space
Time: 2026-10-13 09:02:21 UTC
Duration: 20.01s, Total samples = 3569.44MB 
Showing nodes accounting for 1913.50MB, 53.61% of 3569.44MB total
Showing top 25 nodes out of 134
      flat  flat%   sum%        cum   cum%
         0     0%     0%  1819.32MB 50.97%  github.com/djosh34/tigerstore/internal/node.(*Node).Run
    0.50MB 0.014% 0.014%  1819.32MB 50.97%  github.com/djosh34/tigerstore/internal/node.(*Node).commit
         0     0% 0.014%  1819.32MB 50.97%  main.main
         0     0% 0.014%  1819.32MB 50.97%  main.run
         0     0% 0.014%  1819.32MB 50.97%  runtime.main
         0     0% 0.014%  1602.77MB 44.90%  github.com/djosh34/tigerstore/internal/core.(*Node).Process
         0     0% 0.014%  1496.86MB 41.94%  github.com/djosh34/tigerstore/internal/server.(*Server).Serve.func1
         0     0% 0.014%  1496.86MB 41.94%  github.com/djosh34/tigerstore/internal/server.(*Server).serveConn
         0     0% 0.014%  1496.86MB 41.94%  github.com/djosh34/tigerstore/internal/server.(*conn).readLoop
         0     0% 0.014%  1496.86MB 41.94%  github.com/djosh34/tigerstore/internal/server.(*conn).serve
         0     0% 0.014%  1280.80MB 35.88%  github.com/djosh34/tigerstore/internal/core.(*Node).propose
         0     0% 0.014%   995.28MB 27.88%  github.com/djosh34/tigerstore/internal/server.(*conn).onFrame
         0     0% 0.014%   722.71MB 20.25%  github.com/djosh34/tigerstore/internal/server.(*conn).onData
         0     0% 0.014%   666.05MB 18.66%  go.etcd.io/raft/v3.(*raft).Step
         0     0% 0.014%   666.05MB 18.66%  go.etcd.io/raft/v3.stepLeader
       2MB 0.056%  0.07%   662.55MB 18.56%  go.etcd.io/raft/v3.(*RawNode).Propose
       2MB 0.056%  0.13%   655.05MB 18.35%  go.etcd.io/raft/v3.(*raft).appendEntry
         0     0%  0.13%   653.05MB 18.30%  google.golang.org/protobuf/proto.Clone
         0     0%  0.13%   651.55MB 18.25%  google.golang.org/protobuf/internal/impl.(*MessageInfo).merge
         0     0%  0.13%   651.55MB 18.25%  google.golang.org/protobuf/internal/impl.(*MessageInfo).mergePointer
  651.55MB 18.25% 18.38%   651.55MB 18.25%  google.golang.org/protobuf/internal/impl.mergeBytes
         0     0% 18.38%   651.55MB 18.25%  google.golang.org/protobuf/proto.mergeOptions.mergeMessage
  639.20MB 17.91% 36.29%   639.20MB 17.91%  github.com/djosh34/tigerstore/internal/server.(*stream).receive
  618.25MB 17.32% 53.61%   618.25MB 17.32%  github.com/djosh34/tigerstore/internal/core.encodeBatch
         0     0% 53.61%   455.58MB 12.76%  golang.org/x/net/http2.(*Framer).ReadFrame
```

#### Allocated objects, top 25 flat

```text
File: second
Build ID: 20d62c56c5ef7b16a45c9629dd891614f9a54be8
Type: alloc_objects
Time: 2026-10-13 09:02:21 UTC
Duration: 20.01s, Total samples = 15962340 
Showing nodes accounting for 15595482, 97.70% of 15962340 total
Showing top 25 nodes out of 134
      flat  flat%   sum%        cum   cum%
   2378487 14.90% 14.90%    4454655 27.91%  golang.org/x/net/http2.(*Framer).readMetaFrame
   2076168 13.01% 27.91%    2076168 13.01%  golang.org/x/net/http2.(*Framer).readMetaFrame.func1
   1616697 10.13% 38.04%    1616697 10.13%  github.com/djosh34/tigerstore/internal/server.(*stream).pushLocked
   1346667  8.44% 46.47%    2025506 12.69%  github.com/djosh34/tigerstore/internal/server.(*conn).newStream
   1247111  7.81% 54.28%    1247111  7.81%  github.com/djosh34/tigerstore/internal/server.(*conn).admit
    753699  4.72% 59.01%     753699  4.72%  github.com/djosh34/tigerstore/internal/mvcc.(*writer).latest
    710005  4.45% 63.45%     710005  4.45%  github.com/djosh34/tigerstore/internal/core.(*State).track
    688159  4.31% 67.77%     688159  4.31%  golang.org/x/net/http2.(*frameCache).getDataFrame
    678839  4.25% 72.02%     678839  4.25%  time.newTimer
    602977  3.78% 75.80%     602977  3.78%  github.com/djosh34/tigerstore/internal/node.(*Node).SubmitBatch
    597945  3.75% 79.54%     658827  4.13%  github.com/djosh34/tigerstore/internal/core.(*Node).Submit
    589850  3.70% 83.24%     589850  3.70%  golang.org/x/net/http2.parseHeadersFrame
    581815  3.64% 86.88%     581815  3.64%  github.com/djosh34/tigerstore/internal/server.(*stream).receive
    524320  3.28% 90.17%     524320  3.28%  github.com/djosh34/tigerstore/internal/server.(*conn).putResponse
    216957  1.36% 91.53%    2357974 14.77%  github.com/djosh34/tigerstore/internal/server.(*conn).complete
    201471  1.26% 92.79%     234240  1.47%  github.com/djosh34/tigerstore/internal/node.completeAll
    131075  0.82% 93.61%     131075  0.82%  github.com/djosh34/tigerstore/internal/server.(*conn).takeCompletions
    108388  0.68% 94.29%     120993  0.76%  go.etcd.io/raft/v3.newStorageApplyMsg
    105872  0.66% 94.95%     269712  1.69%  go.etcd.io/raft/v3.(*raft).maybeSendAppend
     98304  0.62% 95.57%      98304  0.62%  go.etcd.io/raft/v3/raftpb.MessageType.Enum (inline)
     83184  0.52% 96.09%      83184  0.52%  go.etcd.io/raft/v3.newStorageAppendRespMsg
     65537  0.41% 96.50%      65537  0.41%  golang.org/x/net/http2.parseWindowUpdateFrame
     65537  0.41% 96.91%      65537  0.41%  google.golang.org/protobuf/internal/impl.consumeUint64Ptr
     65536  0.41% 97.32%      65536  0.41%  go.etcd.io/raft/v3.(*raft).send
     60882  0.38% 97.70%      60882  0.38%  github.com/djosh34/tigerstore/internal/core.(*Node).queueWrite
```

#### Allocated objects, top 25 cumulative

```text
File: second
Build ID: 20d62c56c5ef7b16a45c9629dd891614f9a54be8
Type: alloc_objects
Time: 2026-10-13 09:02:21 UTC
Duration: 20.01s, Total samples = 15962340 
Showing nodes accounting for 6040124, 37.84% of 15962340 total
Showing top 25 nodes out of 134
      flat  flat%   sum%        cum   cum%
         0     0%     0%   10277455 64.39%  github.com/djosh34/tigerstore/internal/server.(*Server).Serve.func1
         0     0%     0%   10277455 64.39%  github.com/djosh34/tigerstore/internal/server.(*Server).serveConn
         0     0%     0%   10277455 64.39%  github.com/djosh34/tigerstore/internal/server.(*conn).readLoop
         0     0%     0%   10277455 64.39%  github.com/djosh34/tigerstore/internal/server.(*conn).serve
         0     0%     0%    5820046 36.46%  golang.org/x/net/http2.(*Framer).ReadFrame
         0     0%     0%    5820046 36.46%  golang.org/x/net/http2.(*Framer).ReadFrameForHeader
   2378487 14.90% 14.90%    4454655 27.91%  golang.org/x/net/http2.(*Framer).readMetaFrame
         0     0% 14.90%    3854432 24.15%  github.com/djosh34/tigerstore/internal/server.(*conn).onFrame
         0     0% 14.90%    3110019 19.48%  github.com/djosh34/tigerstore/internal/node.(*Node).Run
     21845  0.14% 15.04%    3110019 19.48%  github.com/djosh34/tigerstore/internal/node.(*Node).commit
         0     0% 15.04%    3110019 19.48%  main.main
         0     0% 15.04%    3110019 19.48%  main.run
         0     0% 15.04%    3110019 19.48%  runtime.main
         0     0% 15.04%    2574757 16.13%  sync.(*WaitGroup).Go.func1
         0     0% 15.04%    2489049 15.59%  github.com/djosh34/tigerstore/internal/server.(*conn).serve.func1
         0     0% 15.04%    2489049 15.59%  github.com/djosh34/tigerstore/internal/server.(*conn).writeLoop
    216957  1.36% 16.40%    2357974 14.77%  github.com/djosh34/tigerstore/internal/server.(*conn).complete
   2076168 13.01% 29.40%    2076168 13.01%  golang.org/x/net/http2.(*Framer).readMetaFrame.func1
         0     0% 29.40%    2076168 13.01%  golang.org/x/net/http2/hpack.(*Decoder).Write
         0     0% 29.40%    2076168 13.01%  golang.org/x/net/http2/hpack.(*Decoder).callEmit
         0     0% 29.40%    2076168 13.01%  golang.org/x/net/http2/hpack.(*Decoder).parseFieldIndexed
         0     0% 29.40%    2076168 13.01%  golang.org/x/net/http2/hpack.(*Decoder).parseHeaderFieldRepr
         0     0% 29.40%    2068443 12.96%  github.com/djosh34/tigerstore/internal/core.(*Node).Process
   1346667  8.44% 37.84%    2025506 12.69%  github.com/djosh34/tigerstore/internal/server.(*conn).newStream
         0     0% 37.84%    2025506 12.69%  github.com/djosh34/tigerstore/internal/server.(*conn).onHeaders
```

### Failed first attempt and limits

[Earlier run 38371820704](https://github.com/djosh34/tigerstore/actions/runs/38371820704) used PR 76 `751f69c` and PR 77 `fcd54b1`. Its 256-client comparison and profiles passed, but its 64-client job failed during repetition 4 on PR 76. The leader logged `core: the leader's disk stalled; stopping so that another node can lead`; the generator then got connection refused from its runtime-metrics endpoint. That attempt has no valid five-repetition 64-client comparison and contributes nothing to the tables above. The cause of the stall is not established as physical runner storage versus software. The fresh successful run on newer heads does not prove the earlier failure was harmless. Its [evidence is retained separately](https://github.com/djosh34/tigerstore/tree/40613c0f6eafa2ff82debc7c79f267dcb8c8a73f/evidence/bench75/run-38371820704).

Both replicas and the leader share one VM and disk. Guest barriers do not prove physical power-loss protection. CPU/allocation windows include drain but exclude later follower persistence fences. At 256 clients PR 77's generator occupied a median 0.934 of its core; its measured windows lasted a median 1.55 seconds. These are fixed-concurrency results, not maximum or sustained capacity, open-loop latency, Kubernetes behavior or crash-durability evidence.

[Raw JSONs, CSVs, profiles, matching symbol binary and source audit](https://github.com/djosh34/tigerstore/tree/40613c0f6eafa2ff82debc7c79f267dcb8c8a73f/evidence/bench75/run-38372738426) preserve the results. The reusable code is on `exp/bench-75`, [experiment commit `15aa787`](https://github.com/djosh34/tigerstore/tree/15aa787c0e7d868d9995b049c9e3d26b71bd6dd8/experiments/bench75). Its branch-only manual workflow takes `candidate_ref`, optional `second_candidate_ref`, `baseline_ref` and `capture_profiles`. Each run gets a fresh output directory. Tigerstore archives get the same observation-only loopback endpoint; only the separate profile cluster exposes pprof. Core, frontend, Raft, storage and peers remain the named commits' code. No local benchmarks or heavy tests, and nothing merged.
