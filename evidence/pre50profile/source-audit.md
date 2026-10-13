# Put and linearizable point Range audit

Inspected read-only at main `08d620707e25182219d8bd23830b11fed14f6c6a`. Source paths below are relative to `../repo`. This is a source audit, not a measured profile. No builds, code changes or benchmark runs. Concurrent profiling additions appeared in the checkout after inspection and are excluded from this pinned-revision analysis.

Read issue #50 through `gh issue view`, especially [benchmark pre-work comment 6133988913](https://github.com/djosh34/tigerstore/issues/50#issuecomment-6133988913). Its measurements concern etcd and separate prototypes, not this tigerstore revision.

## Request paths and expected costs

### Put

- Client TLS, HTTP/2 and protobuf processing happen in grpc-go. `internal/server/server.go:106`, `(*kvServer).Put`, calls `do`, then `internal/node/node.go:189`, `(*Node).Do`. Each request allocates a request and buffered reply channel, increments an atomic ID, enters the shared inbox and waits. The committer's `commit` and `handle` manage waiter maps and drain up to 4096 additional inputs per turn.
- `internal/core/node.go:269`, `(*Node).Submit`, validates through `check` and `mvcc.CheckPut`, then `queueWrite` tracks the pending request and its protobuf size. `propose` calls `internal/core/codec.go:29`, `encodeBatch`. The original client request is encoded again with origin, boot and request IDs. Buffer growth, protobuf work and value copies are plausible CPU and allocation drivers.
- `RawNode.Propose`, `Process`, `handleReady` and `Persisted` run consensus. A follower-origin Put forwards its packed proposal to the leader and replies only after that origin applies the committed entry. `internal/peer/peer.go`, `(*sender).write` and `(*TCP).receive`, marshal and unmarshal `raftpb.Message` frames over buffered TLS/TCP. Fanout, forwarding, socket work and scheduling add cost beyond the entry codec.
- `internal/node/node.go:336`, `(*Node).persist`, drains queued persist jobs. `internal/wal/wal.go`, `(*WAL).Append` and `record`, copy payloads into the WAL buffer and compute chained CRC-32C. `Sync` calls `Flush`, the append write, then the segment's `Sync`. `internal/osfs/osfs.go` embeds `*os.File`, so this is `os.File.Sync`, not an explicit fdatasync implementation. Rotation at 64 MiB adds segment creation, a header write and fsync, plus directory sync before the driver reports completion.
- After durability and commit, `internal/core/node.go:562`, `(*Node).apply`, calls `DecodeBatch`, which uses fresh request messages and `proto.Unmarshal` on every replica. `applyOp` calls `internal/mvcc/ops.go:178`, `(*Store).Put`. `writer.latest` searches the B-tree, `writer.put` allocates the response and inserts a key if needed, and `writer.add` appends a version. The index retains decoded value slices. `matchWatches` and `flushWatches` in `internal/core/watch.go` add watcher-dependent work. Every replica computes the response, but only the matching origin boot replies. grpc-go subsequently encodes the client response.

Likely Put drivers are transport and scheduling at small values, repeated serialization and copying as values grow, and allocation/GC as retained history grows. Fsync and quorum waits can dominate elapsed latency without dominating CPU. `PrevKv` enlarges responses. Lease Puts fail today and must not enter a successful-Put denominator.

### Linearizable point Range

For `Serializable=false`, an empty `RangeEnd`, and a present key:

- `(*kvServer).Range` at `internal/server/server.go:102` uses the same `Node.Do` inbox/reply path. `core.Node.Submit` validates and queues the read. `requestReads` at `internal/core/node.go:381` sends one `RawNode.ReadIndex` for that turn's reads. Followers obtain the leader's safe read index and serve locally.
- `readIndexKnown` records the barrier. `serveReads` waits for `b.index <= n.applied`, then `serveRead` calls `Store.Range`. Missing barriers retry every three 100 ms ticks and when a new known leader appears. Concurrent writes or recovery can delay local apply. The read itself does not append a WAL record or require a per-read fsync.
- `internal/mvcc/ops.go:51`, `(*writer).rangeKeys`, validates the revision and calls `internal/mvcc/store.go:107`, `(*Store).ascend`. Empty `RangeEnd` takes `BTreeG.Get`, not an ordered range scan. `keyIndex.live` uses `keyIndex.at` to binary-search the version list, even at the current revision. `keyIndex.kv` allocates metadata but aliases the key and value slices. Ordinary point reads do not need sorting or a disk value fetch. grpc-go response serialization and TLS still copy/process the returned value.

Point lookup cost grows with key count and per-key version depth. ReadIndex batching, peer control traffic, scheduling and response encoding may exceed lookup CPU. Keep missing-key, historical, `CountOnly`, `KeysOnly` and serializable reads separate from ordinary present-key linearizable reads.

## Main differences from `docs/design.md`

The document distinguishes its target design from "What is built". These are mostly acknowledged unfinished work, not new correctness findings.

- Responses and watch events are computed after commit, not prepared and encoded during replication. Apply-time decode, index mutation and response preparation therefore remain on the post-commit path.
- Values are not retained in ready-to-send wire format. `DecodeBatch` copies them out of the entry during protobuf decode. `core/storage.go` retains the durable entries in stock `raft.MemoryStorage`, while the MVCC index retains decoded values. Point reads use these in-memory values, not a disk-log slice plus a short header encode.
- `mvcc.New` uses Google's degree-32 B-tree and per-key version slices, not an arena B+tree with live subtree counts. The missing counts hurt broad Range workloads, not this single-key lookup. There is no compaction, cleaning or checkpoint path, so overwrite history, entry bytes, memory pressure and restart replay grow with run length.
- Batching is eager per `Process`, with additional proposals when `queueWrite` crosses the cap. It is not strictly gated on completion of the previous local append. The disk worker groups whatever jobs are queued, but the design's `batch size ≈ arrival rate × append time` needs measurement here. `MaxBatchBytes` counts request protobuf bytes, excluding per-request batch metadata. Record operations/entry, bytes/entry, jobs/sync, bytes/sync and queue depths rather than assuming a 64 KiB group.
- Peers use protobuf Raft frames, an explicitly accepted interim choice. grpc-go remains the client path. The Raft byte window is 32 MiB with a simultaneous 256-message limit, so small-entry workloads can hit an entry-count constraint despite the target's byte-window model.

## Suitable pprof attribution

Use stack ancestry to assign each CPU sample once. These categories separate work ownership even when shared protobuf, TLS or runtime functions are the sampled leaf.

| Category | Source anchors |
| --- | --- |
| Client RPC transport and codecs | grpc-go, HTTP/2/HPACK, client-facing TLS; `server.(*kvServer).Put`, `.Range` |
| Driver and request bookkeeping | `node.(*Node).Do`, `put`, `commit`, `handle`; channels, maps, atomic IDs |
| Entry codec | `core.encodeBatch`, `core.DecodeBatch`; protobuf descendants |
| Consensus and read barriers | `core.(*Node).Process`, `handleReady`, `Persisted`, `requestReads`, `serveReads`; `go.etcd.io/raft/v3`, `core/storage.go` |
| Peer transport | `peer.(*sender).write`, `.send`, `.next`, `peer.(*TCP).receive`; peer protobuf, TLS, buffered/socket I/O |
| WAL encoding and persistence CPU | `node.(*Node).persist`, `wal.(*WAL).Append`, `.record`, `.Flush`, `.Sync`, `.newSegment`; CRC and file syscalls |
| MVCC and watch apply | `core.(*Node).applyOp`, `mvcc.(*Store).Put`, `.Range`, `writer.put`, `.rangeKeys`, `keyIndex.at`, Google B-tree; `matchWatches`, `flushWatches` |
| Runtime and unassigned | GC workers, scheduler and samples whose ownership cannot be recovered |

Charge allocation/GC-assist descendants to their calling category, or report all GC separately with a stated rule. Do not do both. Background GC usually cannot be assigned to one RPC. Flat samples partition CPU; cumulative totals overlap. Shared leaves such as protobuf, memcpy and TLS are not additive subsystem totals.

## CPU versus elapsed waits

- CPU pprof samples execution, not time asleep in fsync, network I/O or `Node.Do`. A small `WAL.Sync` CPU share does not establish cheap durability. Report timed write, fsync and rotation distributions separately, including mean wall time for throughput models. The pre-work's 64 KiB fsync measured 174.442 µs mean wall versus 16.337 µs CPU per batch. Neither is a per-Put cost without the actual group size. Cyclic overwrites excluded file growth and rotation, which this WAL performs.
- Block profiles measure sampled synchronization waits, not general disk or network waits. `Node.Do` reply waits can contain quorum and persistence time, while `commit`, `persist` and peer writer receives also accumulate harmless idle time. A buffered inbox's queue residence is not its channel-send blocking time. Use execution traces or explicit stage timing to distinguish queueing, runnable delay, apply and I/O.
- Mutex profiles attribute contention to the releasing stack, not CPU spent by the waiter. Core/MVCC ownership needs no state locks. Examine grpc-go, TLS, runtime and stock MemoryStorage separately; the explicit `peer.TCP.mu` protects connection tracking, not every frame. Summed blocked or contention time across goroutines can exceed the measurement's wall duration. Never add it to CPU time or RPC latency.
- Asynchronous disk, committer and peer goroutines lose ordinary request-stack ancestry. Handler-only cumulative views miss that work, and handler labels do not automatically follow inbox messages. Use per-process profiles and worker stacks. This revision has no pprof endpoint, profiling flags or explicit block/mutex sampling setup in `cmd/tigerstore/main.go` or `internal/server/server.go`; collection needs a separate approved experiment.
- Normalize absolute sampled CPU and matching process user+system CPU by successful operations in the same window. Report leader, each follower, all servers and generator separately. Percentages alone hide differing CPU/op, throughput and idle fractions. Sampling is approximate and is not a complete kernel/IRQ accounting source.

For comparison, retain the pre-work's five repetitions, safe barriers, TLS, verified affinity and `GOMAXPROCS`, binary identity, leader placement and explicit measurement/drain boundaries. Its etcd workload was leader-only, closed-loop, 256 clients and random binary keys, not Kubernetes-shaped open-loop load or follower-balanced traffic. Reject invalid CPU samples and RPC failures; preserve arrival-cohort latency through drain. Do not add overlapping primitive rows or infer an integrated ceiling from them. The pre-work used grpc-go 1.83.2, while this `go.mod` uses 1.84.0. The design's 90k Put/s and earlier 27k etcd figures remain hypotheses or results from different conditions, not baselines for this revision.
