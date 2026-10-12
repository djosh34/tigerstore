# tigerstore design

tigerstore is a drop-in etcd replacement for Kubernetes. It serves the etcd v3 API that kube-apiserver uses, reaches consensus with go.etcd.io/raft, and stores data in its own storage engine, designed from what one CPU core can do.

This file is the current design. It starts from the merged starting design ([#32](https://github.com/djosh34/tigerstore/issues/32)), the merged starting test design ([#33](https://github.com/djosh34/tigerstore/issues/33)) and the owner design directions ([#53](https://github.com/djosh34/tigerstore/issues/53)). The architect changes it with evidence ([#37](https://github.com/djosh34/tigerstore/issues/37)), through a pull request, and records each large change in a "Design change" comment on the outcome issue. All numbers are hypotheses until measured.

## Status

Built so far: the Go module, a `tigerstore` command that exits with an error, and CI with lint, build and test. Everything below is the design that the next pull requests build.

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

While a batch is being replicated and synced, the committer prepares the response bytes and watch events for it. When the batch commits, the committer releases them at once. Nothing is released before commit. If the entries are lost in a leader change, the prepared bytes are dropped. Replicas only report the log index they have durably accepted, as Raft followers already do.

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

Write latency for one client is about the client round trip plus `max(t_a, RTT + t_a on the follower)` plus at most one batch of queueing. At 1 to 10 ms between nodes that is about 2.3 to 20 ms, dominated by `RTT`.

The batch byte cap and the peer window size are open for measurement.

## Node-to-node transport

Between tigerstore nodes no gRPC or protobuf is required. Only the client-facing API stays etcd gRPC. Any transport is allowed if it is fast and encrypted, for example AES-GCM messages over UDP, QUIC, or framed TCP with TLS. The architect picks by measurement.

## Storage and recovery

- The Raft log is the value store. The full command and value go in the log, written once per node. There is no bbolt and no second copy.
- Values are stored in wire format, so reads and watch events are a short header encode plus a memcpy from the log.
- Truncation of a replaced Raft suffix is persisted before any later ack. The index never points at replaced bytes.
- Recovery starts from consistent checkpoints plus ordered replay of the log. Fuzzy checkpoints are not proven correct for CAS replay and stay an experiment.
- Segment cleaning runs only below the compaction revision. It copies live data, syncs it, installs and syncs a new recovery root, and only then deletes the old segment. It keeps everything that outstanding roots, replay and readers need.
- The index is one arena B+tree with live subtree counts. A key-to-slot hash next to it is an experiment, judged by total bytes per key and core time.
- Compression is an experiment, judged by the ratio on a real Kubernetes dump, the CPU cost and the read latency.

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

## Open for the architect

- How the committer prepares responses before commit: which node executes a batch, and when, so that the prepared revisions and results match the committed order.
- The node-to-node transport.
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
