# tigerstore design

tigerstore is a drop-in etcd replacement for Kubernetes. It serves the etcd v3 API that kube-apiserver uses, reaches consensus with go.etcd.io/raft, and stores data in its own storage engine, designed from what one CPU core can do.

This file is the current design. It starts from the merged starting design ([#32](https://github.com/djosh34/tigerstore/issues/32)), the merged starting test design ([#33](https://github.com/djosh34/tigerstore/issues/33)) and the owner design directions ([#53](https://github.com/djosh34/tigerstore/issues/53)). The architect changes it with evidence ([#37](https://github.com/djosh34/tigerstore/issues/37)), through a pull request, and records each large change in a "Design change" comment on the outcome issue. All numbers are hypotheses until measured.

## Status

Built so far: the Go module, CI with lint, build and test, and the storage pieces of one node: the MVCC index (`internal/mvcc`), the Raft log on disk (`internal/wal`), the real disk adapter (`internal/osfs`), the simulated disk (`internal/simfs`), the committer (`internal/core`), the deterministic simulator (`internal/sim`), the determinism lint (`internal/lint`), and the real node: its driver (`internal/node`), the peer transport (`internal/peer`), the etcd gRPC server (`internal/server`) and the `tigerstore` command. The section "What is built" describes them. Everything else is the design that the next pull requests build.

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

Recovery (`wal.Open`) first checks that the segment names are consecutive, then reads the segments in order. In the newest segment, a record that does not verify starts a torn tail. If the next record does not verify either, chained from the bad record's stored crc or from its computed one, the write was never synced and never acknowledged, so recovery truncates the tail and syncs. A newest segment that holds no more than a damaged header is removed. Everything else is `wal.ErrCorrupt`, and the node refuses to start without changing any file:

- a damaged record in an older segment;
- a damaged record (body, type or stored crc) followed by a record that verifies;
- a damaged header with records after it, a file name that does not match its header, or a gap in the segment sequence or crc chain;
- an entry that leaves a gap or does not start the log at index 1;
- a commit index past the last entry.

The second rule can also fence a node after a crash in which the disk wrote a later sector of the last write but not an earlier one. Refusing to start is the safe answer there: cutting the tail could roll back a synced vote.

Known limits: damage to a record's length field misframes everything after it and looks like a torn tail, and so does damage to the last synced record. A disk that lies about fsync can lose a synced tail the same way. Recovery cuts the log silently in these cases; they are outside the guarantee until records carry redundant headers.

The WAL does no I/O of its own. It goes through `wal.FS`, a directory with List, Create, Open, Remove and SyncDir, and files with ReadAll, Write (append), Truncate and Sync. `internal/osfs` implements it on the local file system. `internal/simfs` implements it in memory for the simulator.

**Simulated disk.** `simfs.Disk` keeps volatile and durable state per file and per directory. Write and Truncate change the volatile content; Sync makes it durable. Create and Remove change the volatile directory; SyncDir makes it durable. `Crash` keeps the durable state plus a random prefix of each file's unsynced appends, and one time in four turns the last kept bytes into garbage, like a torn sector. `Erase` empties the disk for the erase-one-disk checks. A `Fault` hook runs before every operation and can fail it, which is where ENOSPC, EIO and stalled fsyncs come in.

The WAL crash test runs 500 seeds of random appends, suffix replacements and crashes, including crashes between the write and the fsync of one sync, and repeated crashes at the first writing operations of recovery (truncate and sync of a torn tail). A separate test crashes at each of the six writing steps of replacing a damaged header-only newest segment (remove, directory sync, and the create, write and syncs of the new segment). After each crash, recovery must return the synced log plus a prefix of the unsynced records. The test fails if recovery skips the torn-tail truncation or the crc check. Separate tests damage the body, type and stored crc of a record in the middle of the newest segment and the header of the newest segment, and remove a middle segment, also right before an empty newest one; each must give `ErrCorrupt` and leave the files alone.

`osfs.New` creates the data directory and its missing parents, and on every start syncs the directory and every ancestor up to the root, so a setup that crashed halfway is finished by the next start.

**Committer.** `internal/core` is one node's committer as an event and effect machine. Its inputs are method calls: `Submit` (a client request), `Step` (a Raft message from a peer), `Tick` (one clock tick) and `Persisted` (a disk write finished). `Process` runs Raft until it has nothing left to do and returns `Effects`: persist jobs, messages to send and replies. The driver calls these from one goroutine and carries out the effects. In the real node that driver is the committer goroutine; in the simulator it is the event loop.

- **Writes.** `Submit` validates the request with `mvcc.Check*` and the 1.5 MiB size limit, then queues it. `Process` packs every queued write into one Raft entry, so the batch holds whatever arrived since the last call. The byte cap is `MaxBatchBytes` (4 MiB), and there is no timer. Each write in the entry carries its origin node, the origin's boot ID and the request ID (`core/codec.go`). Raft is `RawNode` with `AsyncStorageWrites`, so the local append comes out as a `PersistJob`. The driver appends every queued job to the WAL, syncs once and reports each job with `Persisted`. The core then appends the entries to its in-memory Raft log and steps the responses. When an entry commits, the core applies it to the index on the spot and answers the requests whose origin and boot match its own. A boot ID is random per process start, so an entry proposed before a restart never answers a request made after it. Within one boot, the driver never reuses a request ID, because a write that timed out can still commit later. A write that the index rejects at apply time (an unknown lease, a missing key for `ignore_value`, a range at a compacted or future revision) is answered with etcd's error on every replica and changes nothing.
- **Reads.** Linearizable reads and read-only transactions queue up, and each `Process` turns the queue into one `ReadIndex` request. The reads run against the index once the applied index reaches the read index. Raft drops a `ReadIndex` without a word when a follower knows no leader, when a message is lost, and on every term change. So a batch without an index asks again every 3 ticks and at once when a new leader appears, and batches are served in any order. Serializable reads run at once against local state. The read context carries the boot ID too.
- **Timeouts.** Every request has a deadline in ticks (`RequestTicks`, 7 s). After it, the request fails with etcd's "request timed out", which is ambiguous: it may still commit. A proposal that Raft drops because there is no leader fails at once with "no leader".
- **Membership is fixed.** The Raft storage wrapper reports the 3 voters as its configuration, and there are no configuration entries. On start, the core loads the recovered log into memory and Raft applies it again from index 1, which rebuilds the index. That is the whole of recovery until checkpoints exist.
- **Fatal errors stop the node.** A failed disk write, an entry that does not decode, or a snapshot (none is ever made yet) stops the core. `Process` then returns the error, and every waiting request fails with "server stopped". Raft's own logger is silenced but still panics on broken invariants.
- **Responses are not prepared before commit yet.** Every node computes the results when it applies an entry, and the origin node replies right away. The apply step is a function of the index and the entry alone, so the leader can later run it ahead on a copy and hold the encoded replies until commit (see Seams).

**Watches.** Watchers live in the core (`core/watch.go`) and follow etcd v3.7.2's watch server. A driver hands the core the requests of a watch stream (`Node.Watch`) and closes the stream with `CloseWatchStream`; the responses come back as `Effects.Watch`. On create, watch ID 0 picks the next free ID of the stream, starting at 0. A taken ID and a negative start revision get etcd's created-and-canceled response with watch ID -1. A start revision below the compaction revision gets a response with `CompactRevision` that cancels the watch. A watcher that starts in the past catches up from `Store.History` at once, inside the call, in responses of at most 1,000 revisions that never split a revision, as etcd does. Then it is synced: after each applied write, the core matches the write's changes against every watcher and queues the events, and at the end of `Process` it sends each watcher one response with all of them and the current revision as header. `prev_kv` comes from the key's version list, so a create has no previous value and every update and delete has one. The NOPUT and NODELETE filters work. Because every watcher is always synced, a progress request is answered at once with watch ID -1, and watchers that ask for progress notifications get one every `ProgressTicks` (etcd's 10 minutes).

The node driver keeps a queue of 4096 responses per stream. A stream whose client falls further behind is closed with `ResourceExhausted`, and the client resumes from its last revision; Kubernetes does that on any broken watch. The gRPC handler honours etcd's `hasleader` metadata: it refuses to open without a leader and closes the stream when the leader is gone for a 500 ms check.

Matching walks every watcher for every change. An interval index over the watchers' ranges replaces that when Kubernetes' hundreds of watchers show up in profiles. Responses are never split into fragments.

**Deterministic simulator.** `internal/sim` runs the real `core.Node`, `wal.WAL` and `mvcc.Store` of all 3 nodes in one process. Everything is an event in one queue ordered by virtual time and sequence number:

- Ticks every 100 ms per node, with a random phase.
- A disk write writes at once and finishes its fsync 50 µs to 1 ms later, or 20 to 100 ms later for 2% of writes. A crash in between leaves a torn tail.
- Directed links are ordered streams: a message never overtakes an earlier one on the same link. Messages are marshalled at the sender and unmarshalled at the receiver, so nothing is shared between nodes. Delays are 20 to 220 µs, and 10% of messages take up to 5 ms. A link that goes down, and every link of a node that crashes, drops what it carries.
- Faults in the first 20 virtual seconds: a node crash about every 3 s with a restart up to 2 s later, and a change of the set of up links about every 2 s. Half of those changes heal every link; the other half pick a random one of the 64 directed-link masks.
- Six clients, each with one call in flight, on 8 keys: puts and deletes with and without previous values, range deletes of up to 3 keys, compare-and-swap transactions with a failure-branch range, transactions that put two keys, linearizable and serializable reads, prefix reads with limits, and reads at past revisions. A client gives up after 3 s.
- Three watch clients: a prefix and a single key with `prev_kv`, and an open-ended range without. Each starts from revision 1 or from now. When its node crashes, and on purpose about every 3 s, it closes its stream and opens a new one on a random node up to 2 s later, from the revision after its last event, as Kubernetes does.
- Then 10 healthy seconds with the clients still running, and 3 quiet seconds.

The seed drives a PCG generator for every choice. Raft draws its election timeouts from `crypto/rand`, which the test makes deterministic with `cryptotest.SetGlobalRandom` seeded from the same seed. Each event, each message, each reply and each restart goes into an FNV-64a trace hash. `TestSeeds` runs every seed twice and fails if the hashes differ.

After a run, the simulator checks:

- every node is up, has applied the same entries, and has the same index hash;
- every successful read returns exactly what the final index holds at the revision the read saw, including reads inside transactions;
- every acknowledged put is in the final index at its revision, its previous value is the final index's value at the revision before, and an acknowledged delete removed a key that existed and left none behind;
- every compare-and-swap held at the revision before its write, or failed at the revision it saw;
- no call sees a revision older than one that another call returned before it started, every write gets a newer revision than that, and no two writes share a revision;
- every write is in the committed log at most once, every acknowledged write is in it, and no write that failed for certain is (each written value belongs to one call);
- every watch delivered exactly the final store's history of its range from its start revision to the final revision, in order, without duplicates, with previous values where asked, and no event above its response's header revision;
- every call that starts 2 s or more after healing succeeds, and at least 50 do. Two seconds is two election timeouts, which a node restarted at healing may need.

These checks catch a stale read (linearizable reads served without `ReadIndex`), a WAL that skips fsync, a transaction compare that is off by one, a proposal applied twice, watch events without previous values, a watcher that skips history, a watcher that gets only the first change of a write, and a watcher that starts one revision late; each was planted by hand and failed seed 1. The Porcupine, watch, durability and revision checkers of [#56](https://github.com/djosh34/tigerstore/pull/56) are the independent oracle; `Result.History` and `Result.Log` are what the simulator hands them.

Cost on this host (aarch64, 8 cores): about 80 ms per run of one seed, so 64 seeds replayed twice take 10 s, with 230 MB resident. Trace hashes match between processes with `GOMAXPROCS` 1 and 8. CI runs 4 shards of 400 seeds, two on fixed seeds and two on seeds from the run ID; each shard replays its seeds in a second process with `GOMAXPROCS=1` and compares the hashes.

**Determinism lint.** `internal/lint` is a test that parses the non-test files of `internal/core`, `internal/mvcc`, `internal/wal`, `internal/simfs` and `internal/sim`. It fails on a `go` statement, a dot import, a call to a global `math/rand/v2` function, a `log` function that writes to standard error, an import of a module package outside the core list, and imports of `time`, `os`, `syscall`, `net`, `path/filepath`, `crypto/rand`, `math/rand`, `sync`, `runtime` and `unsafe`. It does not see a range over a map, which Go randomizes; the cross-process trace diff in CI catches that. A second test feeds it a file with each violation and requires each to be reported. CI runs it as its own job.

**Real node.** `internal/node` is the driver of `core.Node` outside the simulator. One committer goroutine owns the core. It waits for an input (a client request, a peer message, an unreachable report), a finished disk write or a 100 ms tick, takes up to 4096 more inputs that are already queued, calls `Process` once, and carries out the effects. One disk goroutine takes every queued persist job, appends them to the WAL, syncs once and hands them back on its own channel. That is the group commit: while one fsync runs, the next batch builds up. The committer never blocks on the disk: persist batches wait in a list that it hands to the disk goroutine as space frees up. With 64 batches waiting, it stops taking new inputs but keeps ticking and taking finished writes, so a slow disk slows down clients and peers instead of stopping the node. gRPC handlers call `Node.Do(ctx, op)`, which queues the request with a reply channel and waits for the reply or the context. A request whose client gave up still runs; its reply is dropped. A failed write or sync stops the node.

**Peer transport.** `internal/peer` implements the decided transport. `peer.Transport` has two methods, `Send` and `Close`, so other transports can replace it. The TCP implementation keeps one outbound TLS 1.3 connection per peer, with the peer CA verified on both ends. The dialer first sends its Raft ID as 8 bytes, then frames of a 4-byte length and a `raftpb.Message` in its protobuf encoding, so every Raft field survives. The decision said no gRPC or protobuf between nodes; the architect accepted protobuf frames for now ([clarification on #47](https://github.com/djosh34/tigerstore/issues/47)), since raft v3.7's message types are protobuf and the writer goroutines, not the committer, do the encoding. A hand-written codec is an outcome-4 experiment. Each peer has a writer goroutine with two queues: appends and snapshots, empty appends included, go on the data queue so they stay in order; everything else (heartbeats, votes, ReadIndex and their responses) goes on the control queue, which the writer empties first. Each queue holds at most 64 MiB of entries, contexts and snapshot data. A message over the 64 MiB frame limit is dropped before it is sent, received frames are capped at 64 MiB, and each write has a 5 s deadline. `Send` never blocks: a full queue or a broken connection drops the message and reports the peer unreachable to Raft, which falls back to probing. The writer marshals messages, not the committer. Snapshots are not sent yet.

**gRPC server.** `internal/server` registers etcd's KV service (Range, Put, DeleteRange, Txn) and Cluster.MemberList. Watch is served as described above. Every other service and method, including Compact, Lease, Maintenance and RangeStream, returns Unimplemented through the generated `Unimplemented*Server` types. The receive limit is etcd's 1.5 MiB request limit plus 512 KiB. Client TLS takes a certificate, and with `--client-cert-auth` it requires client certificates from `--trusted-ca-file`.

**Command.** `tigerstore` runs one node with etcd's flag names: `--name`, `--data-dir`, `--initial-cluster`, `--initial-cluster-token`, `--listen-peer-urls`, `--listen-client-urls`, `--advertise-client-urls`, `--cert-file`, `--key-file`, `--trusted-ca-file`, `--client-cert-auth`, and the `--peer-*` versions. A node's Raft ID is its position in `--initial-cluster`, and the cluster ID is a hash of the token and that list. Peer URLs must be https. `scripts/dev-certs.sh` and `scripts/local-cluster.sh` start a local cluster. CI's `smoke` job runs etcdctl v3.7.2 put, get, del and watch against it over TLS and checks that a client without a certificate is refused. `internal/server` has a Go test that runs 3 nodes in one process over real TLS, restarts one from its disk and reads its data back.

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
- **Disk faults.** `simfs.Disk.Fault` can fail any operation on any file. Slow and stalled fsync are a delay in the simulator's scheduling of the completion, not a disk property. Today any disk error fails the run; with injected faults, the simulator must instead stop the node and check that it fenced itself.
- **More simulated faults.** `sim.Faults` holds the fault rates. `Sim.setLinks(mask)` sets all six directed links from one 6-bit mask, so a test can walk all 64 masks and the switches between them. Clock skew and jumps are a per-node tick interval and a burst of ticks. A process pause is a node whose events are held back and then delivered at once. A gray node is a node with slow disk writes and delayed links. Each fault needs a numeric liveness bound in `Sim.check`, like the 4 s after healing used now.
- **Responses prepared before commit.** `core.Node.apply` runs one entry against the index and answers its requests. To prepare responses before commit, the leader runs the same function on its proposals while they replicate, keeps the encoded replies, and releases them when the entry commits at the expected index and term. A leader change drops them. The followers keep applying at commit.
- **Leases and compaction.** New kinds in the entry codec (`core/codec.go`). The lessor lives in the core and sets `Store.LeaseExists`. The leader checks expiry on `Tick` and proposes a revoke. Raft log compaction needs snapshots, which the core refuses today.
- **Planted bugs.** A build tag such as `plantedbugs` can switch a code path in `core`, `mvcc` or `wal`, as the three hand-planted bugs above did. A test lists each bug with its seed budget and requires `TestSeeds` to fail within it.
- **Other transports.** The core only produces and consumes `raftpb.Message` values, and `node.Node` only needs a `peer.Transport` for sending and `Node.Deliver` for receiving. A tournament transport implements those two.
- **Health, version and the rest of the API.** `GET /health` and `/version` share the client port with gRPC: serve the port with an `http.Server` whose handler sends `application/grpc` requests to `grpc.Server.ServeHTTP` and the rest to the HTTP handlers, or split the listener by the first bytes. Maintenance.Status, Lease and Compact replace their Unimplemented stubs in `server.New`.
- **Backpressure.** With 64 persist batches waiting, the committer takes no new inputs. A peer connection then blocks in `Node.Deliver` once the inbox of 8192 inputs is full, and client requests wait in `Node.Do`.
- **Membership details.** MemberList reports client URLs only for the node that answers, because nodes do not tell each other their client URLs yet; `etcdctl --cluster` and clientv3's `Sync` see one endpoint. `--listen-client-urls`, `--advertise-client-urls` and `--listen-peer-urls` take one URL each, where etcd takes lists; a list is refused with an error.

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
