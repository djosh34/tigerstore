# tigerstore design

tigerstore is a drop-in etcd replacement for Kubernetes. It serves the etcd v3 API that kube-apiserver uses, reaches consensus with go.etcd.io/raft, and stores data in its own storage engine, designed from what one CPU core can do.

This file is the current design. It starts from the merged starting design ([#32](https://github.com/djosh34/tigerstore/issues/32)), the merged starting test design ([#33](https://github.com/djosh34/tigerstore/issues/33)) and the owner design directions ([#53](https://github.com/djosh34/tigerstore/issues/53)). The architect changes it with evidence ([#37](https://github.com/djosh34/tigerstore/issues/37)), through a pull request, and records each large change in a "Design change" comment on the outcome issue. All numbers are hypotheses until measured.

## Status

Built so far: the Go module, a `tigerstore` command that exits with an error, CI with lint, build and test, and the storage pieces of one node: the MVCC index (`internal/mvcc`), the Raft log on disk (`internal/wal`), the real disk adapter (`internal/osfs`) and the simulated disk (`internal/simfs`). The section "What is built" describes them. Everything else is the design that the next pull requests build.

## Fixed by the owner

These never change:

- The etcd v3 API as kube-apiserver uses it, with client TLS and client certificates.
- Consensus with go.etcd.io/raft.
- Go, one executable per node, 3 nodes in one region.
- Encrypted node-to-node traffic.
- Linearizable reads and writes.
- An acked write and its value bytes survive the loss of one node.
- Tests and benchmarks run on free GitHub runners. The red team does chaos only, no security work.

Goals, in order: the most objects, the highest write throughput, then latency equal to or lower than etcd.

## Core

- One committer goroutine per node owns all state: the Raft `RawNode`, revisions, the index, leases and watch matching. Nothing else touches that state, so the core takes no locks.
- Helper goroutines do the work around it: client TLS, HTTP/2 framing, protobuf decode and encode, peer encryption and framing, and disk I/O. They hand the committer prepared batches. The committer only batches, applies and releases, and never blocks on I/O.
- Raft is go.etcd.io/raft v3.7.0 `RawNode` with `AsyncStorageWrites`, `CheckQuorum`, `PreVote` and `ReadOnlySafe`. With async storage the leader still cannot apply and ack through a stall of its own disk, because `RawNode` does not apply entries that are not stable locally.
- Many RPCs are packed into one Raft entry. Each RPC keeps its own revision.
- Every node terminates client TLS and gRPC. The etcd client balances round robin, and kube-apiserver opens one client per resource, so two thirds of requests land on followers. Followers forward packed proposals to the leader and serve reads with one batched ReadIndex per core turn.
- Lease expiry is a replicated mutation.

### Responses prepared before commit

While a batch is being replicated and synced, its responses and watch events are prepared. The committer fixes the results and revisions, and helper goroutines encode the bytes. When the batch commits, the committer releases them at once. Nothing is released before commit. If the entries are lost in a leader change, the prepared bytes are dropped. Replicas only report the log index they have durably accepted, as Raft followers already do.

### Batching

Batching is self-clocked: the committer proposes whatever arrived while the previous local append was in flight, capped by bytes, with no timer. The analysis below is the starting point. The benchmarks in outcome 4 replace its symbols with measured values.

Symbols:

- `λ` is the arrival rate of writes at the leader, in operations per second.
- `t_a` is the time for one local append, including the fsync.
- `RTT` is the round trip between nodes. Single region means 1 to 10 ms ([#29](https://github.com/djosh34/tigerstore/issues/29)).
- `c_b` is the fixed core time per batch (Raft bookkeeping, syscalls, one fsync submission). `c_op` is the core time per operation.

How batch size follows load:

- One batch holds what arrived during the previous append, so the batch size is about `λ · t_a`.
- At low load this is one operation. No operation waits for a timer, so one client sees no added latency.
- As load rises, batches grow. The fixed cost per batch is shared, so the core time per operation falls to `c_op + c_b / (λ · t_a)`.
- When the core saturates, queues grow, batches grow, and the per-operation cost falls further. The system settles near `1 / c_op` operations per second instead of collapsing.
- The byte cap bounds the disk time of one batch, and so bounds the latency that one large batch adds to the next.

How batch size follows node-to-node latency:

- The leader does not wait for a batch to commit before it proposes the next one. Replication is pipelined, so `RTT` does not change the batch size.
- `RTT` sets how many batches are in flight: about `RTT / t_a`. The bytes in flight are `λ · RTT · value size`. At 50k writes/s, 10 ms and 10 KiB that is about 5 MB per follower.
- The peer window is bounded by bytes (16 MiB or more), not by entry count, so 10 ms between nodes does not throttle throughput.
- Followers forward what arrived in one core turn and do not wait for the previous forward to be acked.

Write latency for one client is the client round trip, plus at most one batch of queueing, plus the commit path:

- A request that lands on the leader commits after about `max(t_a, RTT + t_a on a follower)`, so about `RTT + t_a`.
- A request that lands on a follower is forwarded to the leader, replicated, and the follower learns the commit afterwards. That is about `2 · RTT + t_a`. Two thirds of requests take this path.

At 1 to 10 ms between nodes, one-client write latency is therefore about 2.3 to 20 ms, dominated by `RTT`.

The batch byte cap and the peer window size are open for measurement.

## Node-to-node transport

Decided in the [design change on #47](https://github.com/djosh34/tigerstore/issues/47): node-to-node traffic is framed TLS 1.3 over TCP. There is one connection per directed peer pair. Length-prefixed frames carry packed Raft messages, with no gRPC or protobuf framing between nodes. The transport sits behind a small interface, so the tournament in #50 and #52 can try others.

Evidence: a loopback experiment where TLS won every batched case and raw AES-GCM over UDP led only at 1 KiB with batch 1, by 8%, within noise; an Astra second opinion agreed. Both measured loopback with one outstanding batch. They pick a default, not batch sizes or in-flight windows.

Rules that come with it:

- Per peer, frame bytes and queued bytes are capped. Control messages (heartbeats, votes, ReadIndex) go between data frames. The peer writers are not the committer.
- A stalled connection hits a write deadline, is dropped and is reported to Raft as unreachable. It never blocks the committer.
- Snapshots stream in bounded chunks to temporary storage and install only when complete and validated, through the durable install path. A separate snapshot connection is allowed if control traffic still starves.
- The codec keeps every Raft field, including the ReadIndex context and snapshot membership. A socket write is never a durable acknowledgement.

## Storage and recovery

- The Raft log is the value store. The full command and value go in the log, written once per node. There is no bbolt and no second copy.
- Values are stored in wire format, so reads and watch events are a short header encode plus a memcpy from the log.
- Truncation of a replaced Raft suffix is persisted before any later ack. The index never points at replaced bytes.
- Recovery starts from consistent checkpoints plus ordered replay of the log. Fuzzy checkpoints are not proven correct for CAS replay and stay an experiment.
- Segment cleaning runs only below the compaction revision. It copies live data, syncs it, installs and syncs a new recovery root, and only then deletes the old segment. It keeps everything that outstanding roots, replay and readers need.
- The index is one arena B+tree with live subtree counts. A key-to-slot hash next to it is an experiment, judged by total bytes per key and core time.
- Compression is an experiment, judged by the ratio on a real Kubernetes dump, the CPU cost and the read latency.

### What is built

**MVCC index.** `internal/mvcc` is the key index of one node. A Google B-tree maps each key to all its versions, oldest first. A version holds the mod revision (main and sub), create revision, version, lease and value. A tombstone is a version with version 0. Range, Put, DeleteRange and Txn follow etcd v3.7.2 rule by rule, including the quirks: a request that changes nothing keeps the revision, Count ignores the revision filters, the limit goes into the scan only with the default order, compares and checks read the state before the transaction, reads inside a transaction see its earlier writes, and nested transaction responses carry an empty header. The validation that etcd does before a request enters the log (empty key, sort options, too many operations, duplicate keys across nested transactions) is `mvcc.Check*`, and errors are etcd's own `rpctypes` values. After each write, `Store.Changes` lists the changed keys, and `Change.Event` builds the watch event with or without the previous value. `Store.Hash` hashes the whole state so that the simulator can compare replicas.

Values are slices that the index keeps without copying. Today the apply step copies them out of the protobuf decode of the Raft entry, so a value sits in memory twice, once in the log entry and once in the index. Pointing the index straight into the entry bytes is the next step for memory (see Seams).

The B-tree stands in for the arena B+tree with live subtree counts. Count walks every key in the range, which took 61 ms at 1M keys in the [10M-key experiment](https://github.com/djosh34/tigerstore/issues/47); the B+tree with subtree counts replaces it.

**Raft log on disk.** `internal/wal` is the only durable copy of the data. The log is a sequence of segment files of 64 MiB. Each segment starts with a header of a magic string, its sequence number and the crc that its first record chains from. A record is `u32 length | u32 crc | u8 type | payload`, with two types: an entry (term, index, entry type, data) and a hard state (term, vote, commit). The crc is CRC-32C over the type and payload, seeded with the crc of the record before it, so a stale or misplaced record does not verify.

An entry record replaces every entry at or after its index. A truncated Raft suffix and its replacement therefore reach the disk in the same write and the same fsync, before any later acknowledgement.

`WAL.Append` only encodes into a buffer. `WAL.Sync` writes the buffer with one write and one fsync, so whoever calls it decides the group commit: the node driver drains every queued append, then syncs once. A failed fsync is never retried; the node stops.

A new segment gets its header written and synced before its directory entry is synced and before any record goes in. So after a crash, a segment either has its full header or holds nothing else.

Recovery (`wal.Open`) reads the segments in order. In the newest segment, a record that does not verify starts a torn tail. If nothing after it verifies, the write was never synced and never acknowledged, so recovery truncates the tail and syncs. A newest segment that holds no more than a damaged header is removed. Everything else is `wal.ErrCorrupt`, and the node refuses to start without changing any file:

- a damaged record in an older segment;
- a damaged record followed by a record that verifies, chained from the damaged record's stored crc;
- a damaged header with records after it, a file name that does not match its header, or a gap in the segment sequence or crc chain;
- an entry that leaves a gap or does not start the log at index 1;
- a commit index past the last entry.

The second rule can also fence a node after a crash in which the disk wrote a later sector of the last write but not an earlier one. Refusing to start is the safe answer there: cutting the tail could roll back a synced vote. A disk that lies about fsync can still lose a synced tail without detection; that is outside the guarantee.

The WAL does no I/O of its own. It goes through `wal.FS`, a directory with List, Create, Open, Remove and SyncDir, and files with ReadAll, Write (append), Truncate and Sync. `internal/osfs` implements it on the local file system. `internal/simfs` implements it in memory for the simulator.

**Simulated disk.** `simfs.Disk` keeps volatile and durable state per file and per directory. Write and Truncate change the volatile content; Sync makes it durable. Create and Remove change the volatile directory; SyncDir makes it durable. `Crash` keeps the durable state plus a random prefix of each file's unsynced appends, and one time in four turns the last kept bytes into garbage, like a torn sector. `Erase` empties the disk for the erase-one-disk checks. A `Fault` hook runs before every operation and can fail it, which is where ENOSPC, EIO and stalled fsyncs come in.

The WAL crash test runs 500 seeds of random appends, suffix replacements and crashes, including crashes between the write and the fsync of one sync and repeated crashes in the middle of recovery. After each crash, recovery must return the synced log plus a prefix of the unsynced records. The test fails if recovery skips the torn-tail truncation or the crc check. Separate tests damage a record in the middle of the newest segment, the header of the newest segment, and remove a middle segment; each must give `ErrCorrupt` and leave the files alone.

`osfs.New` creates the data directory and its missing parents and syncs the parent of each directory it created.

## Testing

### Deterministic simulation

- The core is an event and effect machine. Inputs are requests, cancels, peer bytes, ticks, and I/O and send completions. Outputs are disk operations, sends, replies and timers.
- Four adapters sit at the edge: disk, network, clock and randomness. Core packages may not call `time.Now`, use global randomness, make syscalls or start goroutines. A lint in CI enforces this.
- The simulator runs the real core of all 3 nodes in one process from one seed. Stock raft v3.7.0 becomes deterministic through `testing/cryptotest.SetGlobalRandom`, with one seed per process and a pinned Go version. Every PR replays seeds twice and compares trace hashes.
- Buffers are poisoned after release at every asynchronous boundary.

### Simulated faults

- A byte-level disk with volatile and stable state and directory operations: torn, lost and misdirected writes, slow, stalled and failing fsync, and ENOSPC. Disks that lie about fsync are outside the guarantee, but detectable damage must fence the node.
- TCP as ordered streams: head-of-line blocking, stalls and half-open connections. All 64 directed-link reachability masks run on every PR, plus switching between them. Short, long, switching and asymmetric delays, and one link much faster than another.
- Crashes at every persistence step, and repeated crashes during WAL recovery and snapshot install. At checkpoints, each node's disk is erased in turn and the other two cold-restart.
- Gray nodes and barely alive clients, each with a numeric liveness bound. For example, writes commit within 10 election timeouts while two nodes are healthy. A gray leader is fenced, not only asked to transfer leadership.
- Process pauses, clock skew and clock jumps.

### Checkers

- Porcupine with the ported etcd model, on isolated small histories, as the independent oracle.
- A revision-witness checker for large histories, added only after the differential tests settle historical Range and failed-branch revision semantics.
- A strict watch checker. The previous value is required on update and delete events of watches that ask for it, and not on creates. Kubernetes rejects a non-create event without it. Each watch must be complete up to a target revision, in order and without duplicates.
- Durability of acked and returned data, revision rules, always-on invariants and a coverage floor.
- Sequential differential tests against etcd v3.7.2 for the boundaries: compaction at R-1, R and R+1, error codes, and the maximum request size.
- About 18 planted bugs behind a test-only build tag. CI fails if one is not caught within its fixed seed budget.

### Real processes

The fault proxy on every link, tc netem, SIGKILL, SIGSTOP, CPU throttling and a fault-injecting file layer. Each job checks that every requested fault was actually installed. Bugs in TLS, HTTP/2 or the Go scheduler get their regression test at the layer that reproduces them.

### Budgets and regressions

The first measurement is the full harness cost per seed on `ubuntu-24.04-arm`, capped at 256 MiB per simulator worker. PR, nightly and release campaigns are sized from that. A failing seed becomes a shrunk schedule and a named scenario test that fails on the bad commit.

## Seams for later pieces

Later pieces plug in here without reshaping what exists.

- **Values point into the log.** The index stores value slices without copying. A decoder that leaves the value bytes inside the Raft entry (protowire instead of `proto.Unmarshal`) removes the second copy without touching the index.
- **Compaction and segment cleaning.** `Store.CompactRev` already gates Range and Txn reads with `ErrCompacted`. Compaction drops versions below the revision from each key's list. Segment cleaning removes old segments with `FS.Remove` and `FS.SyncDir`. Recovery accepts a first segment with any sequence number, because its header carries the crc it chains from, but it still requires the log to start at index 1. Cleaning therefore needs the recovery root first: a synced checkpoint that names the first index the log must start at.
- **Leases.** `Store.LeaseExists` decides whether a Put with a lease is accepted. Until the lessor exists it is nil, and every lease is unknown.
- **Disk faults.** `simfs.Disk.Fault` can fail any operation on any file. Slow and stalled fsync are a delay in the simulator's scheduling of the completion, not a disk property.

## Open for the architect

- How the committer prepares responses before commit: which node executes a batch, and when, so that the prepared revisions and results match the committed order.
- The scope of a custom client TLS, gRPC and HTTP/2 path. grpc-go stays the compatibility reference, and the custom path stops growing if it does not clearly beat grpc-go.
- The hash index, fuzzy checkpoints, compression, and the batch byte cap.

## First experiments

1. A lean load generator, the same for etcd and tigerstore, plus a custom TLS gRPC/HTTP2 transport measured against grpc-go on pinned cores.
2. A core loop prototype with real fsync at 0 and 10 ms between nodes, against etcd.
3. Index memory and speed at 10M keys.
4. Compression ratio on a real Kubernetes dump.

## Expected numbers

Hypotheses from [#32](https://github.com/djosh34/tigerstore/issues/32), for a GitHub runner with one pinned core per replica and one for the load generator:

- At 1 KiB, about 90k writes/s with the lean generator (the shared disk caps near 98k/s), and about 30k/s with the stock etcd client. etcd measured 27k/s ([#26](https://github.com/djosh34/tigerstore/issues/26)).
- At 10 KiB, about 11k writes/s, or about 20k/s if compression reaches 2x.
- Besides throughput, the benchmarks report server CPU microseconds per operation per node.
