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

{{PERFORMANCE_TABLE}}

PR 77's median rate was 12.3% above PR 76 at 64 clients and 14.5% above it at 256. It was 2.96x and 4.76x main, 6.46x and 10.00x etcd. These ratios compare stores inside the same job. PR 76's head has changed since the earlier PR 75/76 report; these are not controlled cross-run comparisons with that report.

### Leader allocations and GC

These are whole-process cumulative allocation deltas divided by 60,000 successful Puts. Tigerstore uses `runtime/metrics`; etcd uses `go_memstats_mallocs_total` and `go_memstats_alloc_bytes_total`.

{{ALLOCATION_TABLE}}

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

{{PROFILE_LISTINGS}}

### Failed first attempt and limits

[Earlier run 38371820704](https://github.com/djosh34/tigerstore/actions/runs/38371820704) used PR 76 `751f69c` and PR 77 `fcd54b1`. Its 256-client comparison and profiles passed, but its 64-client job failed during repetition 4 on PR 76. The leader logged `core: the leader's disk stalled; stopping so that another node can lead`; the generator then got connection refused from its runtime-metrics endpoint. That attempt has no valid five-repetition 64-client comparison and contributes nothing to the tables above. The cause of the stall is not established as physical runner storage versus software. The fresh successful run on newer heads does not prove the earlier failure was harmless. Its [evidence is retained separately](https://github.com/djosh34/tigerstore/tree/40613c0f6eafa2ff82debc7c79f267dcb8c8a73f/evidence/bench75/run-38371820704).

Both replicas and the leader share one VM and disk. Guest barriers do not prove physical power-loss protection. CPU/allocation windows include drain but exclude later follower persistence fences. At 256 clients PR 77's generator occupied a median 0.934 of its core; its measured windows lasted a median 1.55 seconds. These are fixed-concurrency results, not maximum or sustained capacity, open-loop latency, Kubernetes behavior or crash-durability evidence.

[Raw JSONs, CSVs, profiles, matching symbol binary and source audit](https://github.com/djosh34/tigerstore/tree/40613c0f6eafa2ff82debc7c79f267dcb8c8a73f/evidence/bench75/run-38372738426) preserve the results. The reusable code is on `exp/bench-75`, [experiment commit `15aa787`](https://github.com/djosh34/tigerstore/tree/15aa787c0e7d868d9995b049c9e3d26b71bd6dd8/experiments/bench75). Its branch-only manual workflow takes `candidate_ref`, optional `second_candidate_ref`, `baseline_ref` and `capture_profiles`. Each run gets a fresh output directory. Tigerstore archives get the same observation-only loopback endpoint; only the separate profile cluster exposes pprof. Core, frontend, Raft, storage and peers remain the named commits' code. No local benchmarks or heavy tests, and nothing merged.
