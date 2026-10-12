# Design: deterministic simulation and chaos testing
https://github.com/djosh34/tigerstore/issues/33

Part of [Map: tigerstore, a faster etcd for Kubernetes](https://github.com/djosh34/tigerstore/issues/1).

## Question
How do we test tigerstore so hard that bugs have nowhere to hide, in TigerBeetle's style?

## Context
Fixed by the owner: etcd v3 API as kube-apiserver uses it plus TLS ([#3](https://github.com/djosh34/tigerstore/issues/3), [#7](https://github.com/djosh34/tigerstore/issues/7)), consensus with go.etcd.io/raft, fully new custom storage, Go, one executable per node, 3 nodes in one region, linearizable reads and writes, acked writes survive loss of one node ([#8](https://github.com/djosh34/tigerstore/issues/8)). No TigerBeetle. Goals in order: max number of objects, max throughput, latency equal or lower than etcd. Measured facts: [#26](https://github.com/djosh34/tigerstore/issues/26) (etcd profile: CPU-bound, gRPC plus protobuf about half the leader CPU), [#28](https://github.com/djosh34/tigerstore/issues/28) (primitive costs on a 4-CPU ARM runner), [#27](https://github.com/djosh34/tigerstore/issues/27) (k8s mass deletes), [#15](https://github.com/djosh34/tigerstore/issues/15) (value durability and group commit ideas, written for TigerBeetle but the byte path ideas carry over).

Decided in [Test layers and the release gate](https://github.com/djosh34/tigerstore/issues/20): our own seeded Go fault proxy on every link plus tc netem, apiserver storage tests through an adapter, the etcd linearizability checker (Porcupine) ported, full Kubernetes conformance, chaos with the checker running, all gating the release. TigerBeetle tests itself with deterministic simulation (VOPR): the whole cluster in one process with simulated network, disk, clock and crashes, driven by one seed, so every failure replays exactly. go.etcd.io/raft is a deterministic library without I/O, which may make the same possible here.

## Options
Two independent designs (GPT-6 Astra and Opus 5.5), then each answers the other.

## Recommendation
A design comment with: how the code must be structured so the whole cluster runs deterministically in one process (I/O, time and randomness behind small interfaces, single-threaded core); the simulator (network faults, disk faults such as torn writes, lost fsyncs, corruption, misdirected writes, crashes and restarts, clock skew); the checkers (linearizability, durability of acked writes, watch order and completeness, revision rules, invariants inside the state machine); how real-process chaos and Kubernetes tests complement it; what runs on every PR, nightly and before release on GitHub Actions; and how a failing seed becomes a regression test. No more machinery than needed.


## djosh34 2026-10-12T20:41:43Z
https://github.com/djosh34/tigerstore/issues/33#issuecomment-6132256744

## Design by astra

1. Run the real three-node core, storage format and recovery code in one deterministic Go process.
2. Put disk, network, clock, randomness and client delivery behind explicit events, not hidden goroutines.
3. Fix one determinism hole first: Raft v3.7.0 uses private, cryptographic election randomness.
4. Crash at persistence boundaries, then crash again during recovery and snapshot installation.
5. Attack asymmetric, switching and barely-alive links, not just clean outages.
6. Model bytes, partial persistence and directory operations; a fake durable key-value map tests nothing useful.
7. Check client histories, durable recovery, revisions and complete watch delivery independently.
8. Separate failures within the durability contract from disks that lie about successful syncs.
9. Gate every PR on simulation, storage compatibility and real-process chaos; gate releases on all layers.
10. Keep every failure reproducible, and prove the tests work by making deliberately broken implementations fail.

### Scope and the trap to avoid

Assume the implementation contains bugs. The job is to produce a small execution that exposes one, not a dashboard full of successful requests.

I read #3, #7, #8, #15, #20, #26, #27 and #28, including their comments. I did not read the other independent design. This proposal follows #33's current constraints: Go, `go.etcd.io/raft`, custom storage, three nodes, one executable per node, Kubernetes-compatible etcd APIs and TLS. The TigerBeetle architecture in older issues is superseded.

Keep #15's obligation, not its old machinery: visible references must never outrun durable value bytes. Start with complete mutation bytes in the replicated WAL. That removes the need for a second blob publication protocol. If a later storage design separates values from the WAL, its simulator must add the missing-byte, delayed-publication and reclamation races. Do not assume a durable hash is a durable value.

Use two complementary oracles:

* A small, independent specification over sorted keys, immutable versions and explicit revisions. No production index, recovery routine or watch matcher in it.
* The real etcd API at pinned **v3.7.2** for sequential differential tests and boundary cases. Do not compare two concurrent faulted clusters response-for-response: different valid execution orders can produce different answers.

The required contract includes durability of data returned by reads, not just writes whose callers received success. A timed-out mutation can have committed. Watches have ordering/completeness guarantees, but are not linearizable RPCs. These distinctions come from the [pinned etcd guarantee specification][guarantees].

### Structure the executable for attack

One `Core` owns each node's `RawNode`, request sequencing, apply state, MVCC metadata, lease decisions, watch cursors and persistence protocol. All mutations of that state happen on its event loop. Production runs three such cores in three processes. Simulation calls the same cores sequentially in one process.

Use a small event/effect boundary, not an abstraction for every function:

```go
// Sketch, not an existing API.
func (n *Core) Step(in Event) []Effect
// Inputs: request, cancel, peer bytes, tick, I/O completion, send completion.
// Outputs: write/read/sync/rename/remove, send bytes, reply, arm timer.
```

Four adapters are sufficient:

1. **Disk:** actual byte reads/writes and file/directory operations, with completion IDs and explicit sync completion. Includes WAL, snapshots, manifests and segment cleaning.
2. **Network:** owned encoded buffers and completion/backpressure events. Both client and peer boundaries are controllable.
3. **Clock:** explicit monotonic ticks and separately supplied wall time. Deadlines, group-commit timers, lease timers and watch-progress timers use this boundary.
4. **Randomness:** separately seeded streams for workload, scheduler, each link, each disk and each node. Cryptographic keys remain production cryptographic keys, not simulation PRNG output.

Production workers perform syscalls and enqueue completions. They cannot change core state. A core must not block on a slow disk or socket: it records outstanding work and processes permitted subsequent events. Start with one ordered writer per node and bounded byte queues. Do not add parallel apply, a custom goroutine scheduler or a simulation-specific storage engine.

The simulator must execute the production record encoder, checksum checks, parser, Raft integration, MVCC apply, snapshot installation and recovery logic. It replaces the environment, not the algorithm. A real-process suite separately exercises gRPC, TLS, Go scheduling and the actual filesystem. grpc-go has blocking send/receive and flow-control machinery outside this core; for example, `SendMsg` does not establish server receipt. A deterministic core test cannot claim to test those mechanics. [grpc-go v1.83.2 stream contract][grpc-stream], [transport queues][grpc-queues]

Own or copy buffers at every asynchronous boundary. Poison released buffers in tests and delay their consumers. This catches reuse after enqueue, not just races that happen under `-race`. Sort map-derived work before it affects output or scheduling. Use integer virtual time and canonical encodings. Ban `time.Now`, package-global randomness, direct syscalls and background goroutines from core packages with a small source/dependency check. Explicitly audit dependencies too.

### Raft is almost deterministic, not automatically deterministic

Use **Raft v3.7.0 `RawNode`**, not `Node`'s goroutine/channel wrapper. `RawNode` is explicitly thread-unsafe, and `Tick` advances a logical clock. That fits a single owner. [Pinned RawNode implementation][rawnode], [library integration contract][raft-readme]

There is an important obstacle: `raft.go` imports `crypto/rand`, its private `globalRand.Intn` reads `rand.Reader`, and `resetRandomizedElectionTimeout` calls it. `Config` does not expose a seed or random source. Merely seeding our scheduler or calling `math/rand.Seed` will not make this version replay. [Exact source, including Config and election timeout reset][raft-core]

Add one narrowly scoped dependency seam: an optional per-node election `Intn(n)` callback, defaulting to the existing implementation. Simulation supplies a seeded source with the same `[0,n)` range. Keep this as a minimal, documented patch against the pinned library and propose it upstream. Production uses the original default. Test both paths and keep upstream Raft tests green. Do not rewrite elections, force campaigns instead of testing normal ticks, use `unsafe` to reach private state, or globally replace cryptographic randomness. Dependency upgrades must audit this seam and every newly introduced entropy source. Exact replay is blocked until this is done.

Start with `AsyncStorageWrites=false` and the conservative integration:

1. Accept one `Ready`, preserve its bytes and assign a persistence batch ID.
2. Persist required entries, HardState and snapshot state, including the actual referenced snapshot bytes and namespace durability.
3. Release dependent messages, apply committed entries in order, satisfy read barriers only after local apply has caught up, then advance.
4. Publish client success only after the operation is committed and applied; never on proposal acceptance or buffered append completion.

These are deliberately stricter ordering choices than every optimization the library permits. The library's Ready contract, `MustSync`, delayed append responses and async-storage ordering are in the pinned [README][raft-readme], [RawNode][rawnode] and [Config][raft-core]. Do not add asynchronous-storage pipelining until a profile justifies it; enabling it changes the test state space and persistence obligations.

Use `ReadOnlySafe`, not lease-based read authority. The source explicitly warns that the latter can be unsafe with unbounded clock drift. A delayed ReadIndex response and a delayed local apply are separate attack points. Matching request context, leadership changes and local applied index all need tests. [Read-only options][raft-core]

### The simulator

Build one discrete-event queue ordered by `(virtual time, insertion sequence)`. A seed chooses delays and explicit tie-breaking perturbations before insertion. Dispatch one event at a time. Jump over idle virtual time instead of sleeping. Check safety after transitions and run a bounded healthy suffix for liveness.

A case has four phases: establish interesting state, attack it, stop injecting faults and heal, then inspect and recover. Long-running cases repeat these phases. A workload that never restores a functioning quorum cannot distinguish expected unavailability from a deadlock.

Each node has an incarnation. Restart destroys its volatile core and caches, then invokes production startup on surviving disk bytes. Old callbacks cannot complete into a new incarnation. Already-issued disk I/O may still take effect, and already-sent network bytes may still arrive: a process crash does not retroactively cancel them. Process death, machine power loss and permanent disk loss are different events.

Use tiny configurable limits to force rare paths early: WAL segments of a few records, frequent checkpoints, small watch queues, small admission queues and near-full disks. Also run ordinary limits. Tiny-limit success alone misses size thresholds, large-record fragmentation and integer overflow bugs. Cross the maximum accepted request size at minus one, exactly, and plus one byte; include large keys, empty values and binary key boundaries.

This adopts the useful part of TigerBeetle's approach, not its implementation: a seeded cluster attack followed by a liveness phase. Its pinned [0.17.9 VOPR entry point](https://github.com/tigerbeetle/tigerbeetle/blob/0.17.9/src/vopr.zig) provides a concrete primary-source example.

#### Network attacks

There are six directed peer links in a three-node full mesh: `3 * (3-1) = 6`. Fault all six independently, plus every client connection. Do not hide a healthy bypass endpoint from the fault controller.

| Attack | Required variations |
| --- | --- |
| Short and long delay | Around batch timers and heartbeat intervals; just below/above election and client-deadline boundaries; tens or hundreds of election intervals; rare extreme tails among otherwise fast deliveries. |
| Reordering | Reorder whole peer messages where the transport can allow it, late messages from old terms, replies after cancellation, and reconnect traffic overtaking an old connection. Preserve TCP byte order in the normal stream model. |
| Switching delay | Make A→B fast and A→C slow, then reverse them while appends, ReadIndex responses and snapshots are outstanding. Alternate faster than queues drain. |
| Unequal connections | One connection effectively immediate while another takes 100x or 1,000x longer; different streams sharing one slow connection; a fast control path with a stalled value/snapshot path. These are test settings, not measured LAN ratios. |
| Asymmetry | Requests delivered but responses blackholed, reverse direction healthy, different upstream/downstream rates and independent stalls. |
| Partitions | Every node isolated from the other two, every one-way cut and both partition/rejoin orders. For directed peer-link reachability alone there are `2^6 = 64` masks: enumerate them, then attack transitions between masks. |
| Barely-alive peers | Deliver just enough traffic to avoid an obvious timeout while withholding useful append progress, sync completion or snapshot completion. Change which node is gray during repair. |
| Barely-alive clients | Read a byte or an event occasionally, keep connections alive, abandon requests after admission, reconnect repeatedly, and never drain a watch. Other clients must not inherit their queue or memory leak. |
| TCP head-of-line blocking | Hold an early byte while later data queues, stall the socket writer, exhaust flow-control capacity, and carry snapshots alongside heartbeats or watches alongside ordinary requests. Run this against real sockets as well. |
| Damage and duplication | Duplicate/delay complete peer messages; truncate, reset, corrupt or duplicate stream bytes at the proxy. Malformed input must be rejected, not transformed into a valid mutation. |

Message scheduling and TCP faulting are not interchangeable. Reordering bytes inside a TCP stream is application-stream corruption, not ordinary packet reordering. `tc netem` supplies actual packet delay, jitter, loss and reordering in disposable CI network namespaces. A byte proxy exercises stalls, resets and corrupted streams. On TLS links, altered ciphertext should fail authentication; it is not a way to silently modify a decoded Raft message. Exercise decoded peer-frame parsers separately with malformed input. [TLS 1.3 record protection, RFC 8446 §5.2][tls]

For real-process chaos, use #20's seeded Go proxies on client and peer paths. Record connection IDs, direction, byte offsets, action and monotonic timestamps. Define fault decisions over fixed-size stream chunks/offsets rather than arbitrary `Read` call boundaries. A seed replays the fault policy, not the kernel schedule or gRPC timing. Do not claim real-process bit-for-bit replay.

#### Disk attacks

The simulated disk has separate volatile and stable **byte** state plus file/directory namespace state. Writes are not atomic records. Model torn sectors at configurable 512-byte and 4-KiB boundaries, partial writes at other offsets, background persistence before sync, and legal reordering between independent writes. Do not assume that a crash leaves a clean prefix of whole WAL records.

A successful honest sync persists the writes it covers. A sync call that never completes supplies no such promise. Directory sync, rename, truncation and unlink are separate operations. New-file data durability does not automatically imply durable directory naming. This distinction follows the [pinned Linux fsync contract][fsync].

Attack the following:

* Tear the length, checksum, payload and final bytes of a record. Truncate exactly at, one byte before and one byte after every format boundary.
* Lose unsynced writes, persist an unexpected subset of them, or complete a write but lose the reply. Lose an incomplete fsync, not just an append.
* Delay fsync briefly, stall it forever, return an error, or alternate fast and very slow completions. Keep reads and network traffic working while persistence is stuck.
* Return `ENOSPC`, short writes and I/O errors on data files, WAL rotation, snapshots, manifests and directories. Run out of space after copying live records but before installing the new manifest.
* Flip bits in durable data; replay an old checksummed record; write a valid record to the wrong offset or segment. Integrity checks must bind identity, generation, length and logical position, not only checksum the payload. Also damage length fields and assert bounded allocation before trusting them.
* Corrupt snapshot chunks or a complete snapshot footer, mismatch its term/index/MVCC revision, omit one chunk, or mix two valid snapshots. A partially installed snapshot must not become authority.
* Erase one disk and restart its node with the wrong or empty incarnation. It must not claim to be caught up just because it can answer health checks.

Keep two explicit fault classes:

**Contract faults:** arbitrary network behavior and process crashes; loss of unsynced data on machine crashes; at most one permanently lost or integrity-failed node's durable storage before repair; successful syncs on the remaining healthy storage are truthful. A three-node majority is `floor(3/2)+1 = 2`. Two durable copies minus one failed copy leave at least one survivor, and every surviving two-node set intersects every committing two-node set. Tests must exercise the recovery and election protocol that makes that set argument useful.

**Broken-device experiments:** a disk acknowledges sync and later loses those covered bytes, or misdirects/corrupts previously stable data. Charge that node against the one-storage-failure budget. Inject the same on two nodes too, but do not assert that Raft can reconstruct bytes nobody retained. Demand detectable corruption be rejected, invalid nodes stop serving/voting as healthy, and no fabricated success. A whole, internally consistent rollback can evade local checksums; without another trustworthy copy or external generation witness, detection is not guaranteed. Label that result rather than inventing a guarantee.

Do not let a test silently turn “one disk loss, repaired, another disk loss” into two unrepaired losses. Track repair completion by recoverable state coverage, not process liveness.

#### Crashes at every step means recovery too

Give every persistence protocol a numbered transition list and a failpoint before and after each I/O submission, partial completion, sync completion and dependent effect. For short scenarios, enumerate every cut point, not just a random sample. Include:

* proposal admission, local append, follower append, durable response, commit observation, apply, reply enqueue and client receipt;
* segment creation, metadata replacement, compaction, live-record copying, manifest installation and old-file deletion;
* recovery scanning, torn-tail repair, index rebuild, persisted recovery progress if any, and reopening service;
* snapshot receive, verification, temporary-file sync, rename, directory sync, Raft metadata update, apply and old-WAL retirement.

Crash again while recovering from the first crash. Crash repeatedly at the same installation step. Then let it finish with healthy I/O. Recovery must be restartable, bounded by retained data, and must neither lose an acknowledged operation nor apply it twice. A complete corrupted record is not automatically a disposable torn tail. If it might be needed by committed state, obtain a verified peer copy or refuse service.

Use both one-process SIGKILL semantics and power-loss semantics. SIGKILL alone does not test loss of the host page cache or power-fail ordering.

#### Clock attacks

Advance, stop and resume each node's monotonic tick stream independently. Jump wall time forward and backward, including across persisted lease deadlines; restart after a long pause; jump the client's deadline clock separately. Keep tick delivery, wall time and message delivery distinct.

KV safety and read authority must not depend on wall-clock agreement. Lease expiration must become an ordered replicated mutation, not a follower independently deleting its attached keys. Check grant, attach, reattach, overwrite, delete/recreate, leader change and expiry racing a successful CAS. The etcd API supplies TTL leases and attached-key deletion, while Kubernetes's inspected lease manager uses grants for object TTLs. [API schema][api], [Kubernetes v1.37.1 lease manager][k8s-lease]

Specify the supported clock envelope for TTL timing tests. Under normal monotonic time, assert no premature expiry and eventual expiry after a healthy interval. Under arbitrary clock jumps, assert state-machine consistency, no resurrection and no unauthorized read; do not claim a real-time TTL bound without a clock assumption. The oracle must not decide expiry by reading the same local clock branch as the implementation. Differential lease tests use controlled healthy timing; faulted expiry gets an explicit nondeterministic transition in the model.

### Checkers that can actually convict it

#### 1. External histories

Record invocation and return at the client boundary, using one simulator counter or one real load-generator monotonic clock. Include request identity, exact bytes, responses, errors and deadline/cancellation events. A server's “response queued” is not client receipt.

Port the **v3.7.2 etcd robustness models and Porcupine integration**, not etcd's process launcher or its WAL reader. The upstream tests pin Porcupine **v1.1.0**. Preserve the distinction between known semantic rejection and an ambiguous transport failure. [Deterministic model][model], [nondeterministic model][nondet], [checker][operations], [dependency pin][testmod]

A timeout is not an abort. Keep its mutation potentially effective, including after the timeout, until later evidence or the end of the checked execution resolves it. Use a pending/unknown completion interval that permits that late effect. Do not silently discard it, mark it failed, or make a retry exactly-once without a specified protocol. Test a committed mutation whose original reply is lost, then read it, crash the cluster and read it again.

Do not blindly copy upstream history patching: it uses persisted-request and watch evidence, which depends on etcd-specific recovery. Our external checker must remain useful even if our own WAL decoder or watcher is wrong. Internal trace evidence can narrow a separate white-box check, not replace the unmodified client history. [Upstream history patching][patch]

Extend the port where needed. Upstream's deterministic model explicitly does not retain complete historical state and can return partial knowledge for historical reads; it is not a complete oracle for our MVCC engine. Add independent retained versions, create revision, version reset after recreation, pagination, range counts and compaction boundaries. [Model limitations and state][model]

Do not partition the full check by key: global revisions, prefix ranges, multi-key operations and leases couple keys. Keep checked histories deliberately small, typically eight clients and 1,000 completed operations. For long chaos, use many separately validated epochs with clients stopped, ambiguous operations resolved or retained as uncertainty, a linearizable full scan and consistent watch watermark between epochs. Carry model state forward. Never cut a history in the middle of outstanding operations or restart every window's model at an invented empty state.

A checker timeout, panic, resource kill or `Unknown` is failure of the gate, not proof of correctness. Run the checker in a subprocess with a hard watchdog. Upstream itself notes that its nondeterministic `Step` can outlast Porcupine's nominal timeout. Preserve the entire case for debugging; a rerun that passes does not erase the failure. [Pinned deadline handling][nondet], [result handling][operations]

#### 2. Durability, independently of a successful run

Keep a harness-owned receipt ledger outside simulated disks. Include successful mutations and mutations whose effects were exposed by a completed read. Account for legal later overwrites, deletes, lease expiry and compaction: “every acknowledged value remains the current value forever” is the wrong invariant.

At selected checkpoints, freeze client submissions and fork the simulated disk images. For each of the three node choices, erase that node's disk, cold-restart the other two without cached state, let them elect and recover, and compare a full linearizable scan, retained history and lease/revision state with the reference. With unresolved requests, the result must match an allowed reference state consistent with every observed success, not an arbitrarily chosen outcome. Also test cold restart of all three with truthful stable media. The fork is a test optimization, not a production feature.

Use write-once sentinel keys without leases to make lost acknowledgments immediately visible. Separately run overwrite, recreation, TTL and compaction histories to verify that the checker understands legitimate disappearance. A canceled or timed-out client does not license reclaiming bytes a later committed state can still reference.

Before returning success, cheap white-box assertions check the required persistence dependencies. Periodic cold recovery validates that those dependencies really recover bytes. A watermark asserted by production code is not itself proof that the disk format is recoverable.

#### 3. Watches and revisions

Derive expected events from the independent reference history, not from another watcher sharing the same fan-out code. Check:

* inclusive start revision, key/prefix filtering and ordered events;
* no duplicate event within one logical watch, with identity at least revision/key/type, not just revision;
* complete matching revisions, including multi-key deletes and lease expiry;
* exact new and previous values, create/mod revision and per-key version;
* resume after the last fully received revision;
* progress at R only after every matching event through R has been delivered;
* correct explicit compaction cancellation rather than silently dropping history;
* slow watchers, reconnects, stream cancellation and bounded queue memory.

A progress notification alone must not provide its own proof of completeness. After a healthy suffix, obtain an independent target revision, stop mutations, and require each noncompacted test watch to cover its expected events through that target. Otherwise a watcher that emits nothing can pass an order-only checker. Test both requested and periodic progress, and put an update immediately before each progress boundary.

Do not demand watch events arrive within a fixed wall-time bound while the network is partitioned: the etcd guarantee explicitly allows unbounded watch delay. During the healthy suffix, impose our declared liveness bound. If watch fragmentation is enabled, reassemble a logical complete revision before checking atomicity; the pinned schema explicitly allows fragmented responses. [Guarantees][guarantees], [watch schema and progress headers][api], [upstream watch validator][watchcheck]

Do not carry upstream's relaxed `PrevKV` check unnoticed. Its validator permits a missing previous value around compaction; Kubernetes v1.37.1 rejects a non-create event with no `PrevKv`. Our compatibility scenarios must preserve the needed predecessor or return the appropriate compaction failure, not produce an unusable event and call it compatible. [Upstream caveat][watchcheck], [Kubernetes parser][k8s-event]

Assert one global revision increment per actual mutating transaction, even if several keys change. Missing-key deletes and read-only selected branches do not create artificial revisions. Failed compares can still select a mutating failure branch if that operation is supported. Verify absence comparisons, version/create-revision reset after delete/recreate, no ABA victory by an old CAS, and unchanged revision across restart. Compare Compact at `R-1`, `R`, `R+1` and future revisions against stock etcd rather than guessing off-by-one behavior. [MVCC implementation][mvcc], [API][api]

#### 4. Internal invariants and liveness

At every relevant transition:

* applied log index never exceeds commit; the same applied log prefix produces the same semantic state across replicas;
* no stale term response or stale incarnation completion authorizes a new reply;
* every live/history/snapshot/read-pinned value reference resolves to valid retained bytes;
* lease-to-key and key-to-lease mappings agree;
* ordered index, live counts, version chains and reference model agree;
* snapshot applied index, term, configuration, revision and compaction frontier describe one state;
* compaction/cleaning never removes the only recoverable copy or a pinned predecessor;
* queue bytes, watchers, file handles and unresolved requests remain within configured limits.

Do not hash the entire database after every event. Compare changed keys every apply, and perform canonical full-state comparisons at checkpoint/recovery boundaries and periodic samples. Compare actual bytes when diagnosing a hash mismatch; a hash is an efficient equality check, not a proof against adversarial collisions.

Safety runs continuously. Liveness starts only after the harness supplies a healthy quorum, bounded I/O/network delay, fair scheduling and enough free disk. Under that environment, require election, a successful read and mutation, drained required watches and completed repair within a declared event/time bound. Keep a gray node present while the other two are healthy: it must not capture a global lock or indefinitely stall their progress. A lone healthy node need not accept writes.

There is a gray-leader trap: a process can keep sending leadership traffic while useful durable work never completes. Network liveness alone is not enough. Add a bounded persistence-progress watchdog at the integration boundary: if required durable work remains stuck beyond the configured healthy-storage bound, fence that node's Raft participation and readiness, not merely its client request. Stop its outgoing leadership traffic and require recovery before rejoining. Measure progress by satisfied durability dependencies, not bytes trickling through a socket. Test the watchdog with virtual time and real stalled I/O. Choose its threshold above the healthy suffix's declared bounds. No test can demand a finite throughput bound from arbitrarily slow storage while simultaneously forbidding the system from treating it as failed.

### Real systems must still suffer

Deterministic simulation will not discover every lock race, TLS error, syscall misuse or transport deadlock. Run the actual executable with production durability enabled and three separate data directories, plus the seeded proxy and checker. Use real TCP connections, not only an in-memory transport.

Every PR includes short SIGKILL/restart, one-way partition, slow follower, stalled watcher, cancellation and TLS-authentication failures. Nightly and release chaos additionally rotates leaders, snapshots, recovery crashes, disk-full conditions in a size-limited disposable filesystem, compaction and repair under sustained churn. Check RSS, heap, goroutines, open descriptors, retained disk bytes and queue occupancy before and after repeated reconnect/repair cycles. Failure to return to a bounded steady state is a bug even if data remains correct.

Run `tc netem` only in disposable GitHub Actions test namespaces with explicit capabilities. Verify that requested faults were actually installed and affected the intended links. A missing privilege must fail the job, not skip chaos. No host sysctls need changing. I changed no sysctls, mounts or other system settings for this answer.

Physical power-loss survival is not demonstrated by three processes on one VM or a successful `fsync` benchmark. Simulation validates our behavior under a specified persistence model; real-process tests validate integration with the guest OS. A production durability claim also requires truthful storage flush semantics and independent failure domains. Record that assumption, and run abrupt VM/power-loss tests on suitable infrastructure when making a hardware-specific claim.

#### Kubernetes compatibility, not just a booting cluster

Port the apiserver storage tests through an endpoint/test-server adapter, preserving their assertions. Do not replace the storage implementation with a fake or skip hard tests. Kubernetes v1.37.1's suite calls shared storage tests but also depends on etcd-specific setup and helpers, so speaking the wire protocol alone does not make its existing test launcher point at tigerstore. [Pinned storage tests][k8s-tests]

Exercise the actual source-derived shapes: MOD comparisons for objects, VERSION comparisons for compaction coordination, failed-CAS reads, historical/paginated Range, count/keys-only, leased puts, watch `PrevKV` and progress. Include `RangeStream` returning `Unimplemented` and the client's fallback path, status/version fields, health, basic etcdctl and TLS client certificates. [Kubernetes storage client][k8s-client], [compactor][k8s-compact], [watcher][k8s-watch], [store][k8s-store], [factory][k8s-factory]

Run full conformance against a real external three-node tigerstore cluster. Also run explicit storage stress that conformance is not designed to provide: 10k/50k ConfigMap namespace deletion, worker settings 1 and 16, delete/recreate with conflicting CAS, paginated lists concurrent with deletes, slow watches and expiration bursts. Add foreground/background GC and a deliberately held finalizer, then release it and require completion. Do not mistake an intentionally held finalizer for storage failure.

#27 measured that the ordinary mass-delete path is generally a Range plus a conditional Txn per object, not one prefix delete. Test those exact shapes; measure both low-concurrency latency and high-concurrency throughput. The relevant source paths are the [v1.37.1 registry][k8s-registry], [storage implementation][k8s-store] and [etcd client][k8s-client].

#26's CPU profile and #28's shared-disk budgets mean correctness testing must include overload, not just quiet clusters. Run small values to stress RPC/CPU, large values to stress byte budgets, and historical churn to force reclamation. Never disable durability, remove history or omit TLS in a performance comparison while claiming an equivalent product. These earlier measurements are context, not predictions of simulation throughput.

### Hardware arithmetic and measurements on this host

I ran small Go benchmarks under `/tmp/astra-33/bench`, not in the repository. Observed host: Linux `5.14.0-611.26.1.el9_7.aarch64`, Apple-reported ARM CPU, **8 logical CPUs**, Go **1.26.3 linux/arm64**, approximately **12.18 GB RAM**, XFS `/dev/vda3`. It is a shared VM with other activity and swap in use, not the four-core Actions runner from #28. `/tmp` initially had about 4.6 GB free, so I did not allocate large disk images.

Method: Go `testing.B`, `GOMAXPROCS=1`, three 500-ms repetitions, dependencies compiled before measurement, ordinary GC, no sleeps or real disk/network. Porcupine was pinned to v1.1.0. Inputs were prebuilt; output globals or checker results prevented dead-code elimination. Numbers below are all three observations, not confidence intervals.

| Primitive | ns/op, three runs | Allocations |
| --- | --- | --- |
| Pop one event and push its seeded successor in a 256-entry `container/heap` | 112.4 / 120.0 / 115.7 | 32 B, 2 allocations/op |
| Copy 4 KiB and CRC32C the copied bytes, reused buffers | 555.1 / 555.4 / 556.7 | zero |
| SHA-256 over 10,000 canonical 1,056-byte records | 4,532,408 / 4,525,354 / 4,497,209 | zero |
| Porcupine over 1,000 single-register operations, eight-operation overlap groups | 3,219,099 / 3,332,970 / 3,317,012 | about 3.41 MB and 16,884 allocations/check |

These are primitives, **not a working simulator benchmark**. The checker case is an easy valid register history, not the full etcd model or an adversarial timeout-heavy history. Never size a worst-case checker budget from it.

Useful arithmetic:

* Median event-queue cost gives `100,000 * 115.7 ns = 11.57 ms` of scheduler work per 100k events. That excludes Raft, model execution and actual storage algorithms. It argues against prematurely building a custom event scheduler.
* A full 10k-key digest costs about **4.525 ms**. Doing it on every one of 100k events costs **452.5 seconds**. Doing it every 1,000 events costs **0.453 seconds**. Incremental invariants plus periodic full checks are necessary.
* Ten thousand such records are `10,000 * 1,056 = 10,560,000 B`. Three nodes with naive separate stable and volatile copies need `3 * 2 * 10,560,000 = 63,360,000 B`, about **60.4 MiB**, before retained history, index and model allocations. Therefore use small-state simulation for schedule coverage, with separate large-state capacity tests. Cap each initial simulation worker at a measured 256-MiB working-set target and limit worker count; do not assume eight workers fit because the host has eight cores.
* One 100k-event queue run allocates about `100,000 * 32 = 3.2 MB` through this simple heap implementation. Optimize only if the integrated profile says this matters. The checksum/copy primitive does about `4,096 / 555.4 ns = 7.38 GB/s` on hot reused buffers, not random-access simulated-disk throughput.
* The easy 1,000-operation check costs about **3.317 ms**, but ambiguous failures can branch the state space. Bound history size and ambiguity, and use a hard checker deadline. Do not advertise a fixed seeds/second figure before the real harness exists.

<details>
<summary>Reproduce the small benchmarks</summary>

The files and raw output remain under `/tmp/astra-33`. The important benchmark definitions are below; use `github.com/anishathalye/porcupine v1.1.0` and Go 1.26.3.

```sh
GOMAXPROCS=1 go test -run '^$' -bench . -benchtime=500ms -count=3 -benchmem
```

```go
package astra33bench

import (
    "container/heap"
    "crypto/sha256"
    "encoding/binary"
    "hash/crc32"
    "math/rand/v2"
    "testing"
    "time"
    "github.com/anishathalye/porcupine"
)
type event struct { at, seq uint64 }
type queue []event
func (q queue) Len() int { return len(q) }
func (q queue) Less(i,j int) bool {
    return q[i].at < q[j].at || q[i].at == q[j].at && q[i].seq < q[j].seq
}
func (q queue) Swap(i,j int) { q[i],q[j] = q[j],q[i] }
func (q *queue) Push(v any) { *q = append(*q,v.(event)) }
func (q *queue) Pop() any {
    a := *q; v := a[len(a)-1]; *q = a[:len(a)-1]; return v
}
var sink uint32
var digest [32]byte
func BenchmarkEvent256(b *testing.B) {
    r := rand.New(rand.NewPCG(1,2)); q := make(queue,256,257)
    for i := range q { q[i] = event{r.Uint64N(10000),uint64(i)} }
    heap.Init(&q); b.ReportAllocs(); b.ResetTimer()
    for i:=0; i<b.N; i++ {
        e := heap.Pop(&q).(event)
        heap.Push(&q,event{e.at+1+r.Uint64N(10000),uint64(i+256)})
    }
}
func BenchmarkDiskCopyCRC4K(b *testing.B) {
    src,dst := make([]byte,4096),make([]byte,4096)
    table := crc32.MakeTable(crc32.Castagnoli)
    for i:=range src { src[i]=byte(i) }
    b.SetBytes(4096); b.ReportAllocs(); b.ResetTimer()
    for i:=0; i<b.N; i++ {
        src[0]=byte(i); copy(dst,src); sink=crc32.Checksum(dst,table)
    }
}
func BenchmarkStateDigest10K(b *testing.B) {
    data:=make([]byte,10000*1056)
    for i:=0; i<10000; i++ { binary.BigEndian.PutUint64(data[i*1056:],uint64(i)) }
    b.SetBytes(int64(len(data))); b.ReportAllocs(); b.ResetTimer()
    for i:=0; i<b.N; i++ { digest=sha256.Sum256(data) }
}
type req struct { write bool; value int }
func BenchmarkChecker1000(b *testing.B) {
    m:=porcupine.Model{Init:func()any{return 0},
        Step:func(s,in,out any)(bool,any){
            r:=in.(req); if r.write{return true,r.value}; return s.(int)==out.(int),s
        }}
    ops:=make([]porcupine.Operation,1000); state:=0
    for i:=range ops {
        w:=i%2==0; v:=state; if w {state=i+1;v=state}; g:=int64(i/8)*100
        ops[i]=porcupine.Operation{ClientId:i%8,Input:req{w,v},Call:g,Output:v,Return:g+99}
    }
    b.ReportAllocs(); b.ResetTimer()
    for i:=0;i<b.N;i++ {
        r,_:=porcupine.CheckOperationsVerbose(m,ops,time.Second)
        if r!=porcupine.Ok {b.Fatal(r)}
    }
}
```

</details>

### GitHub Actions gates

The following are proposed initial budgets, not measured runtimes. Establish their runtime and memory on Actions before making them required checks. If a suite overruns, make it cheaper or buy more shards; do not silently stop checking histories or skip faults.

Start with exact Kubernetes test tags **v1.35.0, v1.36.0 and v1.37.1**, etcd baseline **v3.7.2**, Raft **v3.7.0 plus the recorded entropy patch**, grpc-go **v1.83.2**, and Porcupine **v1.1.0**. Pin tool versions, images by digest and Actions by commit in a machine-readable manifest. Updating a dependency is a reason to rerun the whole compatibility matrix, not to erase seeds.

| Gate | Every PR | Nightly | Before release |
| --- | --- | --- | --- |
| Unit, specification, codec/recovery fuzz corpus | All fixed regressions, parser fuzz smoke, targeted `-race` runs | Longer fuzzing, corruption corpus and leak checks | All on amd64 and arm64 |
| Deterministic simulation | All regressions, all 64 directed-link masks, enumerated crash cuts for short protocols, 256 fresh seeds | 8,192 fresh seeds plus deeper repeated-crash/recovery scenarios | 65,536 fresh seeds plus corpus, both architectures |
| Determinism self-test | Replay 16 fixed cases twice in separate processes, compare complete canonical trace digests | Perturb process `GOMAXPROCS`, allocation pressure and seed order | Matching fixed-case digests across architectures |
| Apiserver storage adapter | All three pinned minors, including watch/TTL/compaction cases | Same plus longer concurrent scenarios | Same against the exact candidate binaries |
| Real-process checker and chaos | Ten-minute scenario suite with faults forced, not left to chance | At least one hour, including sustained gray failures and disk maintenance | At least two hours per architecture, full fault-family checklist |
| Full Kubernetes conformance | Not required per PR | All supported minors, separate jobs | All supported minors against exact candidate artifacts; record upstream expected test inventory |
| Capacity/performance | Bounded low/high-concurrency smoke with no unsafe settings | Object-count growth, retained-history churn, 10k/50k deletes and repair under load | Matched three-node etcd comparison, same durability, TLS, history and resources |

A seed's initial target is 1,000 completed client operations, 256 keys, eight clients and several watches, with a deterministic event cap such as 100k. Large values, expiry and compaction get dedicated cases. Event-cap exhaustion before the required workload and healthy suffix complete is a failed/inconclusive case, not a pass. Seed counts above are campaign targets; coverage comes from the mandatory scenario matrix as well as randomness.

Independent seeds parallelize across processes, not goroutines inside one simulated cluster. Start with two simulation workers on a four-core Actions job, leaving room for checkers and the runtime. Do not run heavy conformance, disk benchmarks and simulation on the same runner at once. Report CPU, memory, free disk and actual versions per job, not the runner label alone.

The release is blocked by any missing required job, checker timeout, unexpected skipped supported test, unevaluated history, nondeterministic replay, unexplained state mismatch or unresolved data-loss case. Publish the conformance inventory so legitimately inapplicable upstream cases are distinguished from our own disabled assertions. The nightly result gates release only if it ran the same candidate commit/artifacts; otherwise rerun it. Do not average failures away across seeds or rely on GitHub's green status after a retry.

### Prove that the harness is hostile

Before trusting the gate, introduce deliberate test-only mutants, one at a time. Each needs a fixed scenario and a checker that fails:

1. Reply after buffered append but before sync.
2. Send follower append acknowledgment before persistence.
3. Serve ReadIndex before applying its index, or serve from the isolated former leader.
4. Treat canceled requests as aborted when they may still commit.
5. Increment revision once per changed key, or reuse the old version after delete/recreate.
6. Drop one watch event, duplicate another, omit `PrevKV`, or publish progress too early.
7. Install snapshot metadata before its bytes and directory are durable.
8. Delete an old segment before replacement data/manifest durability.
9. Treat a complete corrupted WAL record as an ordinary truncated tail.
10. Keep a global lock across a stalled send/fsync, or leak one watcher per reconnect.

All ten must be killed. If a mutant survives, improve the test before adding more random seeds. Also feed known legal and illegal histories directly to the checker, including a late-committing timeout and a legitimate overwrite after an acknowledged write. A checker that convicts valid behavior is broken too.

### A failing seed becomes a permanent case

On the first failure, capture the git commit, dependency/patch hashes, Go version, architecture, simulator format version, seed, full configuration, last event number, client history, disk images, reference state and canonical trace digest. Keep a bounded in-memory trace suffix during ordinary execution; deterministically rerun the exact failed case to emit its complete trace. Failure to reproduce is itself a determinism bug, not permission to dismiss the original failure.

Reduce the explicit operation/fault schedule while preserving causal dependencies and the same failing assertion. Try fewer operations, fewer keys, shorter delays and fewer crashes. Keep the original seed and binary/source identity as evidence. A seed alone is insufficient across changed code or PRNG consumption, so check in the minimized explicit schedule and expected property alongside a named regression such as `snapshot-install-crash-after-directory-sync`.

The fixed version must pass the original case, its reduced form and nearby delay/cut-point variants. Run the reduced case against the known mutant/bad commit to prove that the assertion still detects the defect. Promote both production bugs and simulator/checker bugs into the corpus. Store small schedules and tiny damaged-file fixtures in the repository; attach large forensic artifacts to the failure report with checksums and retention information.

Build in this order: event boundary and entropy seam; three-node replay self-test; byte-accurate WAL/recovery attacks; independent history/watch checkers; then snapshots, GC, gray failures and CI campaigns. No general simulator framework, no second consensus implementation, no pretending that a seed replaces an oracle. The result should be a small machine that repeatedly finds a place where our ordering is wrong.

Primary-source links above are pinned to the versions or commits inspected. Proposed thresholds, scenarios and CI budgets are design choices, not upstream guarantees.

[guarantees]: https://github.com/etcd-io/website/blob/a5140ec4fee595e84b7916cbcf2e0a53b17e3cbc/content/en/docs/v3.7/learning/api_guarantees.md
[raft-readme]: https://github.com/etcd-io/raft/blob/v3.7.0/README.md
[rawnode]: https://github.com/etcd-io/raft/blob/v3.7.0/rawnode.go
[raft-core]: https://github.com/etcd-io/raft/blob/v3.7.0/raft.go
[api]: https://github.com/etcd-io/etcd/blob/v3.7.2/api/etcdserverpb/rpc.proto
[mvcc]: https://github.com/etcd-io/etcd/blob/v3.7.2/server/storage/mvcc/kvstore_txn.go
[model]: https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/model/deterministic.go
[nondet]: https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/model/non_deterministic.go
[operations]: https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/validate/operations.go
[patch]: https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/validate/patch_history.go
[watchcheck]: https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/validate/watch.go
[testmod]: https://github.com/etcd-io/etcd/blob/v3.7.2/tests/go.mod
[grpc-stream]: https://github.com/grpc/grpc-go/blob/v1.83.2/stream.go
[grpc-queues]: https://github.com/grpc/grpc-go/blob/v1.83.2/internal/transport/controlbuf.go
[k8s-tests]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store_test.go
[k8s-client]: https://github.com/etcd-io/etcd/blob/v3.7.0/client/v3/kubernetes/client.go
[k8s-compact]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/compact.go
[k8s-watch]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/watcher.go
[k8s-store]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go
[k8s-event]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/event.go
[k8s-lease]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/lease_manager.go
[k8s-factory]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go
[k8s-registry]: https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/registry/generic/registry/store.go
[fsync]: https://git.kernel.org/pub/scm/docs/man-pages/man-pages.git/tree/man/man2/fsync.2?h=man-pages-6.16
[tls]: https://www.rfc-editor.org/rfc/rfc8446.html#section-5.2


## djosh34 2026-10-12T20:44:21Z
https://github.com/djosh34/tigerstore/issues/33#issuecomment-6132294098

## Design by opus

**Summary**

1. Assume tigerstore has bugs. The tests must find them, and a test that never fails is treated as a broken test.
2. Structure: a single-threaded deterministic core (raft `RawNode`, state machine, storage logic) that does no I/O. A real shell and a simulator shell drive it through a small set of inputs and outputs.
3. go.etcd.io/raft v3.7.0 is deterministic except for one `crypto/rand` call in the election timeout. Go 1.26 `testing/cryptotest.SetGlobalRandom` pins it. Measured: 20 of 20 seeds replayed bit-exact. Without pinning, 20 of 20 diverged.
4. A raft-only 3-node simulator with crashes, partitions, loss and duplication ran 65x to 480x faster than real time on one core. That leaves room for thousands of seeds per PR and hundreds of thousands per night.
5. The simulator injects every fault the owner listed: short, long and switching delays, asymmetric links, one-way and full partitions, TCP head-of-line blocking and stalled connections, crashes at every disk step including during WAL recovery and snapshot install, torn, lost and misdirected writes, slow, stalled and failing fsync, ENOSPC, gray nodes and gray clients, process pauses, clock skew and jumps.
6. Checkers: a revision-witness linearizability checker (1M ops checked in 0.4 s), Porcupine with the ported etcd model as an independent oracle on small histories, watch rules from etcd's robustness suite, durability of acked writes, revision rules, liveness bounds, and invariants that stay enabled in production.
7. Measured: Porcupine is fast on correct histories with 16 clients or fewer. It times out at 64 clients, and on a buggy history it ran out of 3 GiB of memory. That is why the witness checker runs first on everything.
8. Planted bugs: a fixed list of known etcd bug classes is compiled into the core behind a test-only tag. Each must be caught within a seed budget, otherwise the simulator is too weak and CI fails.
9. Real processes still run under the fault proxy, tc netem, SIGKILL, SIGSTOP, CPU throttling, a fault-injecting file layer and device-mapper fault targets. They cover what the simulator cannot: gRPC, HTTP/2, TLS, the kernel, real fsync and races.
10. A failing seed is shrunk, stored as a seed and rewritten as a named scenario test. Every bug found outside the simulator also counts as a simulator gap and must be reproduced there first.

## Stance

The guarantees are fixed by [#8](https://github.com/djosh34/tigerstore/issues/8): linearizable reads and writes, and acked writes survive loss of one node. The tests do not try to show that tigerstore works. They try to break those guarantees and the liveness bounds below, and they assume a bug exists until a large and varied search fails to find one. Three rules follow:

- A checker timeout, a skipped check or an unexercised fault path counts as a failure ([#20](https://github.com/djosh34/tigerstore/issues/20) already says this for timeouts).
- The tests themselves get tested. Planted bugs must be found, and every listed crash point must be hit.
- Faults are as hostile as the fault model allows, and some runs go beyond it on purpose to check that damage is detected rather than served.

## Host and method for the numbers in this comment

Host: 8-core aarch64 VM (lscpu vendor Apple), Linux 5.14.0-611.26.1.el9_7, Go 1.26.3, 11 GB RAM. Each measurement ran in one process with `GOMAXPROCS=1`. No sysctls or system settings were changed. Code lived under `/tmp` and is not published. Versions: go.etcd.io/raft/v3 v3.7.0 (commit [b867cf1](https://github.com/etcd-io/raft/tree/b867cf13f6bc0dae21204302df97bc2355c3af55)), the version etcd v3.7.2 [pins](https://github.com/etcd-io/etcd/blob/v3.7.2/go.mod); Porcupine v1.1.0, the version etcd v3.7.2's robustness tests [pin](https://github.com/etcd-io/etcd/blob/v3.7.2/tests/go.mod#L19).

The simulator prototype: three `RawNode`s with `MemoryStorage`, one goroutine, an event heap ordered by (simulated time, sequence number), and one seeded PCG source. Every peer message is `proto.Marshal`ed on send and unmarshaled on delivery, so no pointers are shared between nodes. Ticks are 100 ms with ±5% per-node rate skew, `ElectionTick=10`, `HeartbeatTick=1`, `CheckQuorum` and `PreVote` on. Faults: 1% message loss, 0.5% duplicates delivered up to 5 ms late (so reordered), one node isolated for 0.3 to 2.3 s at random times, and one node crashed and restarted every 1 to 4 s (it keeps storage and loses all in-memory raft state). The trace hash is FNV-1a over every delivered message (time, from, to, type, term, index) and every applied entry.

The fault model is coarse: no disk model, and storage is persisted synchronously before `Advance`. It measures determinism and the cost of raft plus the message codec, not of a full tigerstore core.

## Determinism of go.etcd.io/raft, checked

- The raft README says the library is a deterministic state machine: the same state and the same input give the same output ([README](https://github.com/etcd-io/raft/blob/v3.7.0/README.md#L10-L14)). Progress iteration is sorted to keep it so ([tracker.go](https://github.com/etcd-io/raft/blob/v3.7.0/tracker/tracker.go#L184-L199)).
- The one exception: `resetRandomizedElectionTimeout` draws from `globalRand`, which reads `crypto/rand.Reader` and cannot be injected through `Config` ([raft.go L90-L104](https://github.com/etcd-io/raft/blob/v3.7.0/raft.go#L90-L104), [L2053-L2055](https://github.com/etcd-io/raft/blob/v3.7.0/raft.go#L2053-L2055)). raft's own tests override it through a hook that exists only in a `_test.go` file ([raft_test.go L4204](https://github.com/etcd-io/raft/blob/v3.7.0/raft_test.go#L4204-L4206)), so other packages cannot use it.
- Go 1.26 `testing/cryptotest.SetGlobalRandom(t, seed)` makes `crypto/rand` deterministic for the duration of a test. Because it is process-wide, it "cannot be used in parallel tests" ([doc](https://pkg.go.dev/testing/cryptotest#SetGlobalRandom)).

Measured with crashes, partitions, loss and duplication active:

| Check | Result |
| --- | --- |
| 20 seeds x 30 simulated s, run twice each with `SetGlobalRandom` | 20 of 20 traces identical |
| Same 20 seeds, run twice each without it | 20 of 20 traces differ |
| Seed 7, 60 simulated s, three separate processes, `GOMAXPROCS` 1, 8, 1 | same hash `978c40d4e4e5e7d` each time (60,440 applies, 102,157 messages) |

Consequence: raft needs no fork. One seed runs per process at a time, and parallelism comes from running many processes.

## How the code must be structured

**One deterministic core.** Package `core` holds the raft `RawNode`, the state machine (index, revisions, MVCC, watch fan-out bookkeeping, leases, compaction) and the storage engine's logic: record layout, recovery and snapshot install. It runs on one goroutine. Its only entry point is a step function:

```go
// Everything that takes time enters as an input and leaves as an output.
type Input interface{ isInput() }   // Tick, ClientBatch, PeerBytes, ConnUp, ConnDown, DiskDone{op, err}, ...
type Output interface{ isOutput() } // Reply, WatchEvents, SendPeer{to, bytes}, DiskWrite{file, off, bytes}, DiskSync{file}, Rename, ...

func (c *Core) Step(now int64, in Input) []Output // now is monotonic ns from the shell
```

Reads during recovery may be synchronous calls on an injected `Files` interface, because in the simulator they are deterministic too. The core never sees wall time, so wall-clock jumps can only affect logs and TLS certificate checks in the shell.

**Two shells.**

- `shell/real`: gRPC and TLS server, peer TCP with mTLS, `os.File`, the monotonic clock. It runs the core loop on one goroutine and does I/O on others. The design of the hot path is #32's business. The only constraint from testing is that I/O completions reach the core as inputs.
- `shell/sim`: event queue, simulated network, simulated disks, simulated clocks, simulated clients and the checkers, all on one goroutine and driven by one seed.

**Rules for `core`, enforced in CI by a small `go/analysis` pass (about 150 lines):**

- No imports of `time` (except `time.Duration`), `math/rand`, `crypto/rand`, `os`, `net`, `sync`, `runtime` or `unsafe`.
- No `go` statements and no `select`.
- No `range` over a map unless the line is annotated as sorted.

The rules are also checked at runtime: the first 50 seeds of every PR run twice and their trace hashes must match. The only exception to the rules is raft's own `crypto/rand` use, which the simulator pins as shown above.

**What stays outside the simulator**: the gRPC and HTTP/2 server (custom or grpc-go), protobuf decoding of client requests, TLS, sockets and the real file system. Simulated clients hand decoded requests to the core. Simulated peers exchange marshaled bytes, so codec and aliasing bugs between nodes stay inside the simulation. The outside parts are covered by fuzzing, the apiserver storage tests and real-process chaos.

## The simulator

**Knobs come from the seed.** Every seed draws its own configuration from wide ranges. Batch size 1 to 4,096. Snapshot every 10 to 100,000 entries. Max message size 1 KiB to 1 MiB. Watch buffers of 1 event to 64k. Compaction every 1 to 1,000 revisions. Key space of 3 to 1,000,000 keys. Election timeout 5 to 20 ticks. Small values push the rare paths (snapshot install, compaction, watch cancel for slow consumers) thousands of times per seed. The production defaults are one point in the range, not the range.

**Workload**: the kube-apiserver shapes from [#3](https://github.com/djosh34/tigerstore/issues/3). Create (`MOD==0`), update and delete (`MOD==n`, `GetOnFailure`), paginated lists pinned to a revision, count and keys-only ranges, watches from `rev+1` with `prev_kv`, periodic and requested progress, the compactor's `VERSION` txn, `LeaseGrant` plus Put with lease, and Compact. Mass delete bursts follow [#27](https://github.com/djosh34/tigerstore/issues/27). Most seeds use 3 to 100 hot keys to force CAS contention. Simulated clients time out and retry the way clientv3 does.

### Network faults

Each directed link has its own "weather". The weather is redrawn every 0.5 to 5 simulated s and is independent per direction.

| Fault | How the simulator does it |
| --- | --- |
| Short delays | 10 µs to 1 ms one way |
| Long delays | 1.1 s to 30 s, longer than the 1 s election timeout, so messages from old terms keep arriving after elections |
| Switching delays | the weather flips between short and long mid-stream. Delay is drawn per message, so packets in flight overtake each other wherever the transport allows it |
| One link much faster | per-link bandwidth from 1 MB/s to 1.25 GB/s, and latency ratios up to 1:10,000 between links |
| Asymmetric links | leader to follower fast, follower to leader slow or dead. Followers keep hearing heartbeats and do not campaign while their acks never arrive. This is where CheckQuorum and liveness bugs hide |
| One-way partition | drop everything in one direction, keep the other |
| Full partition | isolate one node, or split the client from the leader only. Flapping partitions use periods just under and just over the election timeout |
| Loss, duplication | per message, 0 to 30% |
| Corruption | flipped bytes on a link. The receiver's frame checksum must reject them. In the real system TLS turns this into a connection reset, so the simulator models that reset too |

**TCP is modelled as streams, not as datagrams.** That is the nasty part. Each peer connection is a FIFO byte stream with a send buffer (64 KiB to 4 MiB) and a bandwidth:

- **Head-of-line blocking.** A slow or large message delays everything behind it on the same connection. Example: a 100 MiB snapshot on a 10 MB/s stream holds heartbeats queued behind it for about 10.5 s, more than ten election timeouts. etcd avoids this by sending snapshots on a separate connection ([peer.go L250-L269](https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/api/rafthttp/peer.go#L250-L269)). Whatever tigerstore does instead, the simulator has to be able to show it failing.
- **Stalled connections.** A stream stops making progress without an error. Writes fill the send buffer, then the sender blocks or must drop. Linux does not kill such a connection for at least 924.6 s with the default `tcp_retries2=15` ([ip-sysctl.rst](https://github.com/torvalds/linux/blob/v6.12/Documentation/networking/ip-sysctl.rst#L697-L711)). etcd's server-side gRPC keepalive defaults to a 2 h interval ([config.go L69-L71](https://github.com/etcd-io/etcd/blob/v3.7.2/server/embed/config.go#L69-L71)), while kube-apiserver's etcd client uses 30 s keepalive and a 10 s timeout ([etcd3.go L61-L69](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go#L61-L69)). tigerstore therefore needs its own peer progress timeouts, and the simulator checks them.
- **Half-open connections.** One side has crashed and restarted while the other still holds the old connection and keeps writing into it.
- **Resets.** Everything in flight is lost, the stream reconnects, and the new stream races the old one's last bytes.

Reordering only happens where the real transport allows it: across connections, and across a reconnect. A simulator that reorders within one TCP stream finds bugs that cannot happen. A simulator that never reorders across reconnects misses bugs that can.

### Process faults

- **Crash at every step.** Random crashes happen after any event. In addition, every disk operation boundary is a named crash point: after write and before fsync, after fsync and before rename, after rename and before the directory fsync, between each step of snapshot install, and between each step of compaction.
- **Systematic enumeration (nightly).** For short seeds, the simulator records every disk operation and replays the run once per crash point k. For each k it tries every legal state of unsynced data (kept, dropped, torn). A 10 simulated s seed with about 2,000 disk operations per node gives 2,000 crash points per node, at an estimated 50 ms each: about 100 s per node per seed.
- **Crash during WAL recovery.** Restart, then crash again at every recovery step (scan, torn-tail truncation, index rebuild, first fsync), up to 5 times in a row before recovery is allowed to finish.
- **Crash during snapshot install**, on the receiver and on the sender, at each named step. This includes a crash right after the new state is swapped in and before the old log is truncated.
- **Pause.** The process freezes for 0.1 to 60 s while others continue, then resumes with a stale view, for example a leader that still believes it leads. This is how etcd got stale reads in [#20418](https://github.com/etcd-io/etcd/issues/20418) ([robustness track record](https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/README.md)).
- **Gray node.** Barely alive and never declared dead: CPU slowed 10x to 1,000x (each core step costs simulated time), fsync 1 to 10 s, or reachable by only one peer. Heartbeats get through, appends do not.
- **Gray client.** Reads watch streams at 1 event per second and never closes them, holds thousands of watches, reconnects in a tight loop, or sends half a request and stalls. The last one is only real at the protocol level, so it runs in the real-process layer.

### Disk faults

Each node has a simulated disk with 4 KiB sectors and a volatile cache:

- Writes stay volatile until an fsync of that file completes. Directory entries stay volatile until the directory is fsynced.
- On crash, each volatile sector is independently persisted, lost or torn. Unsynced writes may land out of order.
- fsync latency is drawn per call: 50 µs, 10 ms, 1 to 10 s, or never (a stall).
- fsync can return EIO. The required behaviour is fail-stop. Retrying is a bug, because the kernel may already have dropped the dirty pages.
- Reads can return EIO or flipped bits (latent sector errors).
- Misdirected writes land at the wrong offset.
- Lost writes: fsync returns success, but the data never reaches the media.
- ENOSPC can hit any write, including during recovery, compaction and snapshot install.

**Fault model.** Not every fault can be survived with raft, so the simulator splits disk faults into two classes:

- **In model; the checkers must pass.** Any number of network faults. Crashes of any or all nodes at any point. Torn, lost or reordered unsynced data. Slow and stalled fsync. fsync EIO. ENOSPC. Clock faults. Damage that checksums can detect (read errors, bit flips, misdirected writes) on at most one node at a time. Detected damage must make that node stop voting and serving. It is then replaced: removed and re-added as a fresh member that catches up by snapshot. It must never serve the damaged data and never vote with it.
- **Out of model; for detection only.** Lost synced writes and undetectable corruption. Raft assumes stable storage. Here is a concrete failure. Nodes A and B ack entry e. A loses e through a lying fsync and restarts. B crashes. A and C now form a majority without e, and an acked write is gone. If the lost write was A's HardState, A can vote twice in one term. TigerBeetle's protocol-aware recovery handles this case. Stock raft does not, and this design does not claim it.

  These runs check two things. Detection must work wherever it is possible: checksums cover file, offset, length and content, so misdirected and torn records are caught. And the checkers must flag the violations that follow. Out-of-model failures are reported and do not gate the release.

Requirements this puts on the storage design (#32):

- Checksums bind file, offset and content.
- fsync errors are fatal.
- Detected log or HardState damage causes fail-stop and replacement, never silent repair.
- ENOSPC rejects writes cleanly while reads keep working, and recovery never accepts a partial record.
- A reserved space headroom lets recovery and compaction run on a full disk.

### Clock faults

- Per-node monotonic rate skew of up to ±5%.
- Forward monotonic jumps of 0.1 to 60 s (VM suspend, the same as a pause).
- Wall-clock steps of ±hours in either direction.

Rules the checkers enforce:

- Timeouts use only monotonic time.
- Linearizable reads use raft `ReadOnlySafe` (ReadIndex through a quorum), not lease reads, which raft documents as unsafe under clock drift ([raft.go L58-L70](https://github.com/etcd-io/raft/blob/v3.7.0/raft.go#L58-L70)).
- Lease expiry is decided by the leader and goes through raft as a revoke entry. A new leader refreshes all expiries, as etcd's lessor does on promotion ([lessor.go L480-L492](https://github.com/etcd-io/etcd/blob/v3.7.2/server/lease/lessor.go#L480-L492)).
- A key with a lease must never disappear before TTL x (1 - max skew) of true simulated time has passed since the last grant.

## The checkers

All checkers run on every seed and on every real-process run. Each one either passes or fails. There is no "inconclusive".

**1. Revision-witness linearizability checker (ours, about 300 lines).** Every etcd response carries a header revision, and every committed write has a position in the committed log. The checker uses these as the claimed linearization and verifies it in O(n log n):

- Replaying the log reproduces every acked write's response.
- The log contains only ops that a client actually issued, each at most once (no phantom or duplicate writes).
- Every acked write is in the log.
- Every read and every failed CAS matches the replayed state at its header revision.
- Real time is respected. If op a returned before op b was invoked, then pos(a) ≤ pos(b), with strict inequality when b is a write.

Ops with lost responses may appear in the log after their invoke, or never.

The checker does not trust the system. A log that agrees with every observed response is, by definition, a valid linearization of what clients saw. The logs of all three nodes must also agree on their common prefix.

In the simulator the log is read directly. In real-process runs each node's data directory is dumped offline after the run, the way etcd's robustness tests read the WAL.

**2. Porcupine with etcd's model, ported.** This is the independent oracle decided in #20. It comes from etcd's `tests/robustness/model` and `validate` at v3.7.2 ([validate.go](https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/validate/validate.go#L32-L66)), with the WAL reader replaced by our log dump. Measured with a toy global-revision KV model (10 keys; Put, Get and CAS on mod revision; responses carry revisions; lost responses as nondeterministic ops):

| History | Porcupine | Witness |
| --- | --- | --- |
| 10,000 ops, 8 clients, 1% lost, correct | Ok, 30 to 37 ms | 1.8 ms |
| 30,000 ops, 8 clients, 1% lost, correct | Ok, 171 ms | not run |
| 10,000 ops, 16 clients, 1% lost, correct | Ok, 178 ms | not run |
| 1,000 ops, 32 clients, 1% lost, correct | Ok, 172 ms | not run |
| 1,000 ops, 64 clients, no lost ops, correct | Unknown (timeout at 20 s) | 0.17 ms |
| 30,000 ops, 64 clients, 1% lost, correct | Unknown (timeout at 120 s) | 6.7 ms |
| 1,000,000 ops, 1,024 clients, 10% lost, correct | not run | Ok, 395 ms |
| 1,000 ops, 16 clients, one stale read | Illegal, 3.4 s | not run |
| 10,000 ops, 8 clients, one stale read | out of memory at 3 GiB | caught, 0.4 ms |
| 30,000 ops, 64 clients, one stale read with the correct value for 40 revisions earlier | not run | caught by the real-time rule |

Porcupine blows up exactly when concurrency is high or a bug exists. etcd's robustness traffic stays small for this reason: 50 to 200 QPS on 10 keys ([traffic.go](https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/traffic/traffic.go#L50-L75), [etcd.go](https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/traffic/etcd.go#L35)), with a 5 min validation timeout ([main_test.go L115](https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/main_test.go#L115)).

The order is therefore fixed:

- The witness runs first on the full history.
- Porcupine runs only on the history of a dedicated low-concurrency client group (at most 16 clients and 10k ops per check) and only when the witness passed.
- If the two disagree, that is a checker bug and fails the run.
- A Porcupine timeout is a failure, as decided in #20.

**3. Watch checker**: the same rules as etcd's validators ([watch.go L31-L39](https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/validate/watch.go#L31-L39)), checked against the log replay:

- Ordered, unique and atomic: a revision is never split across responses.
- Reliable: no gaps against the filtered log.
- Resumable from the last revision received.
- Bookmarkable: after a progress notification at R, every event at or below R has been delivered and every later event is above R.
- `prev_kv` and IsCreate match the replay, and events match the watch filter.

Additions:

- A watch must never deliver a revision beyond the committed log.
- A watch started below the compaction revision gets the compacted error with the right revision.
- A watch on a future revision delivers nothing early ([#20221](https://github.com/etcd-io/etcd/issues/20221)).

**4. Durability.** After healing, every acked write and every write any client ever observed must be present.

- Each node is checked in turn, through linearizable reads routed via that node.
- All three logs must agree up to the commit index.
- Every node's state hash at the same applied index must match.

**5. Revision rules.**

- A successful txn bumps the revision by exactly one, whatever the number of keys.
- A failed compare and a delete that matches nothing do not bump it.
- `mod_revision`, `create_revision` and `version` behave as in etcd, including the reset on delete and recreate. The compactor's `VERSION` txn works.
- Reads below the compaction revision fail as compacted. Reads above the current revision fail as future.
- The header revision never goes backwards for a client.

**6. Liveness.**

- While a majority of nodes with healthy disks is connected with bounded delay, writes commit with p99 under a set bound within 10 election timeouts of the last fault.
- After all faults heal, every pending op completes or fails, and all nodes reach the same applied index within 30 simulated s.

This forces design decisions instead of hiding them. A gray leader with a 10 s fsync, or one whose own appends never land, must hand over leadership (`TransferLeader`). A slow watch consumer must be cancelled rather than allowed to block apply.

**7. Invariants inside the core, always on in production** (cheap, checked per batch):

- Applied index ≤ commit index, and both are monotonic.
- No entry is applied twice.
- The revision is monotonic.
- Index key count equals live keys.
- Memory in every queue (per watcher, per connection, unapplied entries) stays within its configured limit.
- Every record read from disk passes its checksum.

Every N revisions the leader proposes a hash marker, and each node compares its state hash at that revision. A mismatch fail-stops that node. In the simulator these assertions abort the seed.

**8. Coverage floor.** Every named crash point, recovery branch, snapshot step, ENOSPC handler and slow-watcher cancel path keeps a hit counter. If any of them has zero hits across the PR seed set, CI fails. A fault that never reaches code tests nothing.

## Testing the tests: planted bugs

Behind a `plantedbugs` build tag, the core contains switchable known bugs. Each one must be caught within a fixed seed budget (for example 500 seeds). If it is not caught, CI fails, because a simulator that misses a planted bug is missing real ones too. The list starts with etcd's own history ([track record](https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/README.md)) and our riskiest paths:

1. Linearizable read served without ReadIndex (stale read after a pause or partition, the [#20418](https://github.com/etcd-io/etcd/issues/20418) class).
2. Leader acks a write before its local fsync completes.
3. Follower acks an append before its fsync.
4. WAL recovery accepts a torn tail record, or truncates past it into synced data.
5. Snapshot install swaps state before the snapshot file is fsynced.
6. Progress notification sent before all events at or below its revision were delivered ([#15220](https://github.com/etcd-io/etcd/issues/15220)).
7. Watch resumes from an older revision after a partition ([#15271](https://github.com/etcd-io/etcd/issues/15271)).
8. Compaction drops a delete event for a watch opened at the compaction revision ([#18089](https://github.com/etcd-io/etcd/issues/18089), [#19179](https://github.com/etcd-io/etcd/issues/19179)).
9. One revision per key instead of one per txn.
10. Lease expiry applied by local wall clock instead of through raft.
11. fsync error retried instead of fail-stop.
12. ENOSPC in the middle of an append leaves a partial record that recovery accepts.
13. A slow watcher blocks the apply loop (liveness).
14. Peer stream with no progress timeout: a stalled connection hangs replication.

## Real processes and Kubernetes

The simulator does not run gRPC, HTTP/2 flow control, TLS, the kernel's TCP and fsync, the Go scheduler, GC pauses, data races, or the real binary and its startup. Those run as real processes on the GitHub runner: 3 nodes on distinct loopback IPs, with the #20 fault proxy on every directed link. The same checkers consume the recorded client histories and the offline log dumps.

The real-process fault set mirrors the simulator's:

| Fault | Real-process mechanism |
| --- | --- |
| Short, long and switching delays, reordering, loss, duplication, corruption | fault proxy (stream level) plus tc netem on `lo` with per-IP filters (packet level, needs sudo, runner only) |
| One link much faster, asymmetric links | proxy per-direction bandwidth caps and delays |
| One-way and full partitions | proxy per direction |
| TCP head-of-line blocking, stalled and half-open connections | proxy stops forwarding one direction while keeping both sockets open (no RST), or reads at 1 byte/s so the sender's TCP window fills. This covers one HTTP/2 connection carrying watch and unary streams together |
| Crash at every step, during recovery and during snapshot install | SIGKILL at random times, plus the simulator's named crash points compiled in under a `failpoints` build tag (no-ops in release builds), the same idea as etcd's gofail points ([gofail.go](https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/failpoint/gofail.go#L34-L60)) |
| Pause, gray node | SIGSTOP and SIGCONT of the leader for 0.1 to 30 s; cgroup v2 CPU quota (for example 5%) through `systemd-run --scope`; one node's links slowed 100x |
| Gray client | proxy in slow-reader and slow-writer modes; a client holding thousands of watches it never reads |
| Torn and lost writes, slow, stalled and failing fsync, ENOSPC | `faultfs` build tag wrapping `os.File` (EIO, ENOSPC, fsync delay or stall, drop of unsynced data at kill); a small loop-mounted file system for real ENOSPC. Nightly, where the runner kernel has the modules: [dm-flakey](https://github.com/torvalds/linux/blob/v6.12/Documentation/admin-guide/device-mapper/dm-flakey.rst) (drop or error writes, corrupt bytes), [dm-delay](https://github.com/torvalds/linux/blob/v6.12/Documentation/admin-guide/device-mapper/delay.rst) (delay reads, writes and flushes), [dm-dust](https://github.com/torvalds/linux/blob/v6.12/Documentation/admin-guide/device-mapper/dm-dust.rst) (bad blocks), [dm-log-writes](https://github.com/torvalds/linux/blob/v6.12/Documentation/admin-guide/device-mapper/log-writes.rst) (replay the disk to every flush point and restart from each) |
| Clock jumps | `sudo date -s` steps on the runner for the wall clock. A `faultclock` build tag adds offset and rate faults to the monotonic clock, because Go reads time through the vDSO and LD_PRELOAD tools such as libfaketime do not reach it |
| Races | nightly chaos runs with a `-race` binary |

The Kubernetes layer is unchanged from #20. It runs the apiserver storage tests through the adapter ([store_tests.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/testing/store_tests.go)), full conformance on kind with external tigerstore, and one more run: a kube-burner style create, churn and mass-delete load while the proxy, kills and pauses run. Pass conditions:

- No apiserver errors other than retryable ones.
- The final object set equals the expected set.
- tigerstore's checkers pass on the recorded etcd-level history.

Fuzzing (`go test -fuzz`) covers what decodes untrusted bytes: WAL and snapshot record decoders, the peer frame decoder, the client request decoder if #32 builds a custom one, and key range math. Corpora are committed.

## What runs when (GitHub Actions)

Limits: GitHub-hosted jobs are capped at 6 h, the Free plan allows 20 concurrent jobs, and standard runners are free on public repositories ([limits.md](https://github.com/github/docs/blob/82b3580f2eb9476326ba9a5eb185c10a129c2c9c/content/actions/reference/limits.md#L45), [L64-L67](https://github.com/github/docs/blob/82b3580f2eb9476326ba9a5eb185c10a129c2c9c/content/actions/reference/limits.md#L64-L67)). The `ubuntu-24.04-arm` runner was observed with 4 CPUs in [#26](https://github.com/djosh34/tigerstore/issues/26).

**Cost of one simulator seed.** Measured raft-only speed on this host, one core:

| Load in the simulator | Simulated time per wall time | Committed entries applied per wall s (3 nodes) | Wall ns per event |
| --- | --- | --- | --- |
| idle, with faults | 73,313x | 0 | 346 |
| 1k proposals per simulated s, with faults | 481x | 247,439 | 1,059 |
| 10k proposals per simulated s, with faults | 65x | 292,175 | 1,129 |
| 10k proposals per simulated s, no faults | 8.4x | 248,664 | 2,466 |

The no-fault row gives 248,664 / 3 = 82,888 committed proposals per wall s, or about 12 µs each for raft plus the codec. Estimate for the full core:

- The real core (index, MVCC, watch, disk model, history recording) multiplies that by about 3, giving 36 µs.
- A runner core is assumed 2x slower than this host, giving about 72 µs per client op. This is unmeasured; it is the first experiment.
- A standard seed is 60 simulated s with 20,000 client ops: 20,000 x 72 µs = 1.44 s, rounded up to about 2 core-s for faults, idle time and checking. The witness checker is about 0.4 µs per op, which is negligible.

| When | What | Budget arithmetic |
| --- | --- | --- |
| Every PR | Determinism lint; unit tests with `-race`; replay of all regression seeds and scenarios; 50 seeds run twice with trace hashes compared | minutes |
| Every PR | Simulator: 4 jobs x 4 cores x 10 min = 9,600 core-s, about 4,800 seeds or about 96M client ops; coverage floor checked | 10 min wall |
| Every PR | Real processes: layer 1 (apiserver storage tests), layer 2 (robustness traffic plus all checkers), 3 short chaos runs of 5 min with mixed proxy faults, kills and pauses, as decided in #20 | about 20 min wall |
| PRs touching `core`, `shell/sim` or the checkers, and nightly | Planted bugs: 14 bugs x up to 500 seeds x 2 core-s = 14,000 core-s at most, about 15 min on 4 jobs | |
| Nightly | Simulator: 16 jobs x 4 cores x 5 h = 1,152,000 core-s, about 576,000 seeds. 10% of them are long seeds of 1 simulated hour with compaction, snapshots and lease churn. Plus crash-point enumeration on 200 short seeds (about 200 x 3 nodes x 100 s = 60,000 core-s) | 5 h wall |
| Nightly | Real processes: 1 h chaos per fault profile (network, process, disk with `faultfs`, clock), `-race` binary, dm-* disk targets if available, fuzzing 30 min per target, full conformance | 4 jobs |
| Before release | All nightly checks green on the release commit, plus one extra day of simulator on that commit: 20 jobs x 4 cores x 5.5 h = 1,584,000 core-s, about 790,000 seeds; a 6 h real-process soak under mixed chaos; conformance under chaos. Out-of-model runs must show that every injected detectable fault was detected | 1 to 2 days |

## From a failing seed to a regression test

1. A failing run prints the commit, the seed and a one-line command: `go test ./shell/sim -run TestSim -seed=0x... -ops=N`. All knobs derive from the seed, so the seed plus the commit is the complete reproduction. It also prints the first failing check, the minimal history slice around it, and the fault schedule.
2. The seed is shrunk automatically by bisecting the op count and simulated duration, then by disabling fault classes one at a time while the failure persists.
3. The shrunk seed goes into `shell/sim/testdata/seeds.txt` with a link to the issue, and every PR replays all of them.
4. A seed alone is not a lasting regression test. Any change to the core changes the trace, and the old seed silently stops reproducing the scenario. So every fix also adds a named scenario test written against the simulator's scripting API, for example: "partition n1 one way at t=2 s, crash n2 between snapshot fsync and rename, heal, assert". The scenario test is the real regression test. The seed is a cheap extra.
5. A bug found by real-process chaos, Kubernetes tests or a user is also a simulator gap. The first step of the fix is a simulator scenario that reproduces it, adding the missing fault model if needed. The fix is accepted only when that scenario fails before the fix and passes after it.

## Risks and first experiments

1. Measure seeds per core-s on `ubuntu-24.04-arm` with the first real core, and replace the 2x and 3x assumptions above.
2. Build the witness checker and the Porcupine port. Feed both the same 1,000 recorded etcd histories and require agreement, then run the planted stale-read bug.
3. Check whether dm-flakey, dm-delay, dm-dust and dm-log-writes load on the runner's `6.17.0-1022-azure` kernel ([runner image](https://github.com/actions/runner-images/blob/main/images/ubuntu/Ubuntu2404-Arm64-Readme.md)). If not, `faultfs` alone covers disk faults on real processes.
4. The simulator stops at the decoded request and does not see the protocol layer. If #32 builds a custom HTTP/2 server, that server needs its own fuzzing and slow-client tests, and they gate releases just as hard.
5. Lost synced writes on one node can break raft. That is a stated limit, not something the tests hide. If the owner wants that fault survived, it is a consensus design question, not a testing one.


## djosh34 2026-10-12T20:46:35Z
https://github.com/djosh34/tigerstore/issues/33#issuecomment-6132326276

## Reply by opus to astra

### Where we agree

The two designs agree on most of the structure:

- One `Core` per node with an event and effect boundary, built on raft `RawNode`, using `ReadOnlySafe` and lease expiry as a replicated mutation.
- A byte-level simulated disk with volatile and stable state.
- Two fault classes, with lying disks outside the contract.
- Crashes at every persistence step, including during recovery and snapshot install.
- TCP modelled as ordered streams, with reordering only across connections.
- A gray-leader trap and a liveness phase after healing.
- Planted bugs that must be caught.
- A failing seed is not enough on its own: it must be turned into an explicit scenario.

The disagreements are below.

### Where astra is wrong

**1. Patching raft for its election randomness is unnecessary.** Astra says exact replay "is blocked until" raft gets a random-source seam, and rejects "globally replace cryptographic randomness". Go 1.26's [`testing/cryptotest.SetGlobalRandom`](https://pkg.go.dev/testing/cryptotest#SetGlobalRandom) already makes `crypto/rand` deterministic inside a test. I measured it on raft v3.7.0:

- 20 of 20 seeds replayed bit-exact under crashes, partitions, loss and duplication.
- Without it, 20 of 20 diverged.
- The same hash came out across separate processes with `GOMAXPROCS` 1 and 8.

The simulator has no TLS keys, so making `crypto/rand` deterministic there costs nothing. A fork would cost an audit on every raft upgrade, forever. Proposing the seam upstream is fine, but it should not block anything. The only constraint is one seed per process at a time, which astra wants anyway.

**2. Porcupine alone, on 8 clients and 1,000 operations, is far too small.** I measured Porcupine v1.1.0 with a global-revision KV model:

- It times out at 64 clients even on correct histories of 1,000 operations.
- It ran out of 3 GiB of memory on a 10,000-operation history containing one stale read.

So Porcupine fails exactly when a bug is present. kube-apiserver runs hundreds of concurrent requests, and batching and group-commit bugs need that concurrency to appear.

The revision-witness checker covers this. It replays the committed log and checks every client response and the real-time order against it. It checked 1M operations from 1,024 clients in 0.4 s and caught a stale read that returned a correct value from 40 revisions earlier.

It does not depend on our WAL decoder being correct. A pass constructs a valid linearization from client observations alone. A wrong log can only cause a false failure, never a false pass. Astra's independence objection therefore does not apply. Keep Porcupine as the second oracle on small client groups, run only after the witness passes.

**3. The seed budgets are about 20x too small.** Astra proposes 256 fresh seeds per PR, 8,192 nightly and 65,536 per release, and declines to estimate cost before a harness exists. I built the raft part of the harness. Committing one proposal costs about 12 µs per core, including the message codec, and the simulator ran 65x to 480x faster than real time with faults on.

Even assuming the full core is 3x more expensive and a GitHub runner is 2x slower, one 4-core job gives about 1,200 seeds of 20,000 operations in 10 minutes. 256 seeds of 1,000 operations is about 256k operations per PR, against my estimate of about 96M. Their budget leaves most of the free runners idle while bugs go unfound.

**4. Astra describes gray-node attacks but sets no pass condition.** Astra says "just enough traffic to avoid an obvious timeout". Without a stated bound the test cannot fail. Each gray attack needs a numeric bound, for example: with two healthy nodes, writes commit within 10 election timeouts of the gray node appearing.

### What astra got right that I missed

1. **Erase each node's disk at checkpoints.** Fork the disk images, erase each of the three nodes in turn, cold-restart the other two and compare full state. This tests #8's guarantee ("survives loss of one node") directly. I only checked the healed cluster, which never actually loses a disk. Adopt.
2. **Enumerate all 64 directed-link reachability masks** (2^6) on every PR, then attack the transitions between them. It is cheap and systematic. Adopt.
3. **Upstream etcd's watch validator accepts a missing `PrevKV` around compaction.** Kubernetes v1.37.1 rejects that event ([event.go](https://github.com/kubernetes/kubernetes/blob/v1.37.1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/event.go#L63-L90)). Our port must be stricter than upstream. I missed this.
4. **A watcher that emits nothing passes order-only checks.** After the healthy suffix, require every watch to cover all expected events up to an independently obtained target revision. Adopt.
5. **Sequential differential tests against stock etcd v3.7.2** for boundary behaviour: Compact at R-1, R, R+1, error codes, and the maximum request size minus one, exact and plus one byte. Do not guess etcd's off-by-one rules; compare against it. Adopt.
6. **The gray-leader watchdog should fence the node**, stopping its raft participation and its outgoing leadership traffic, not just transfer leadership. That is more precise than my version. Adopt.
7. **Chaos jobs must check that the requested faults were actually installed.** A missing privilege for tc netem fails the job instead of silently skipping chaos. Adopt.
8. **Cap memory per simulator worker** (astra suggests 256 MiB) instead of assuming 4 workers fit on 4 cores. Adopt, and measure it.
9. **Planted bugs:** add theirs that I lacked. These are: cancelled requests treated as aborted, an old segment deleted before its replacement is durable, a global lock held across a stalled fsync, and one watcher leaked per reconnect.

### Revised recommendation

1. **Determinism:** `cryptotest.SetGlobalRandom` with stock raft v3.7.0, one seed per process, no fork. Determinism lint plus a twice-replayed trace-hash check on every PR.
2. **Core boundary:** astra's event and effect boundary with four adapters (disk, network, clock, randomness). Their byte-level disk model and their buffer-poisoning rule on every asynchronous boundary.
3. **Faults:** the union of both lists: TCP streams with head-of-line blocking, stalls and half-open connections; the 64 masks plus switching weather; enumerated crash points including repeated crashes during recovery and snapshot install; ENOSPC, fsync stall and fsync EIO; pauses; clock skew and jumps.
4. **Checkers, in order:**
   - The witness checker on the full history.
   - Porcupine on a group of 16 clients or fewer, only after the witness passes.
   - The watch checker with strict `PrevKV` and completeness up to a target revision.
   - Erase-one-disk cold-restart durability at checkpoints.
   - Revision rules, plus differential tests against etcd v3.7.2.
   - Liveness with numeric bounds.
   - Always-on invariants and the coverage floor.
5. **CI budget:** my numbers. About 4,800 seeds per PR (4 jobs x 10 min), about 576k nightly, about 790k on the release commit, plus all 64 masks and short crash enumeration on every PR. First experiment: measure the real per-seed cost and memory on `ubuntu-24.04-arm` and resize from there.
6. **Planted bugs:** the merged list of about 18. Each must be caught within a fixed seed budget, or CI fails.
7. **Regressions:** seed, then shrunk schedule, then a named scenario test that fails against the bad commit. Any bug found outside the simulator becomes a simulator scenario before its fix lands.


## djosh34 2026-10-12T20:47:18Z
https://github.com/djosh34/tigerstore/issues/33#issuecomment-6132336788

## Reply by astra to opus

### Agreement

Keep the single-owner core, real storage/recovery code, byte-stream faults, repeated recovery crashes and planted bugs. Neither design gets to substitute millions of seeds for a working oracle.

### What you caught that I missed

**My proposed Raft patch was unnecessary.** Go 1.26.3 already supplies [`cryptotest.SetGlobalRandom`](https://github.com/golang/go/blob/go1.26.3/src/testing/cryptotest/rand.go). I verified the exact `crypto/rand.Int(rand.Reader, ...)` path locally: 1,000 draws, reset to the same seed, three repetitions, all identical on this ARM host. Use isolated test processes, no parallel tests, and pin Go. Keep this process-global override away from production TLS. Your actual Raft prototype is stronger feasibility evidence than my event-heap benchmark.

Your lying-fsync counterexample also sharpens my fault boundary: **undetected durable rollback can destroy voting safety**, not merely lose a value. Exclude it from ordinary crash-recovery guarantees. Detectable damage must fence the node before it participates again.

### Disagreement

* **The witness checker is not correct as specified.** Historical Range can return values at requested revision R while its response revision is the current revision. Replaying at the header revision falsely convicts valid behavior. A successful read-only transaction does not increment revision; a failed comparison can select a mutating failure branch. Those blanket revision rules must go. [etcd v3.7.2 MVCC](https://github.com/etcd-io/etcd/blob/v3.7.2/server/storage/mvcc/kvstore_txn.go), [transaction model](https://github.com/etcd-io/etcd/blob/v3.7.2/tests/robustness/model/deterministic.go). A witness checker can be useful, but “300 lines” is not evidence that it handles the API.
* **Do not extract one client group's history from a concurrently mutated cluster.** Omitted writers still affect values, ranges and global revisions. Run dedicated, isolated small histories or include every interfering operation. Keep the external Porcupine check independent of our own WAL interpretation.
* **`TransferLeader` is not a guaranteed escape from a gray leader.** Raft may need to catch the transferee up first. If that path is stalled, transfer stalls too. Bound the attempt, then fence the stuck node's participation so the healthy pair can elect. [Raft v3.7.0 transfer path](https://github.com/etcd-io/raft/blob/v3.7.0/raft.go#L1636-L1667).
* **The campaign arithmetic overpromises.** At unchanged operation rate, 10% hour-long seeds cost `0.9 + 0.1*60 = 6.9` times the one-minute average. Your 576k estimate becomes roughly **83k**, before extra enumeration/checking. Measure the complete harness before buying that campaign.

### Revised starting design

Unmodified pinned Raft, test-only cryptographic seeding, conservative Ready persistence, one byte-accurate simulator and one independent small-history specification/Porcupine checker. Add a witness checker later, after differential tests establish its semantics. Start with mandatory crash/fault scenarios and modest measured seed budgets, not half a million guesses.

Keep real-process protocol/race tests as first-class regressions. Requiring every TLS, HTTP/2 or Go-scheduler bug to reproduce inside a simulator that explicitly excludes those mechanisms is nonsense. Add each regression at the layer that can actually reproduce it.


## djosh34 2026-10-12T20:48:31Z
https://github.com/djosh34/tigerstore/issues/33#issuecomment-6132354149

## Starting test design, merged from both designs and replies

Starting point for the plan; the architect may change it with evidence ([#37](https://github.com/djosh34/tigerstore/issues/37)).

### Structure
- The core is an event and effect machine: inputs (request, cancel, peer bytes, tick, I/O and send completion), outputs (disk ops, sends, replies, timers). Four adapters: disk, network, clock, randomness. Core packages may not call time.Now, global rand, syscalls or start goroutines (lint in CI).
- Determinism: stock raft v3.7.0 with Go's testing/cryptotest.SetGlobalRandom in tests, one seed per process, pinned Go. Every PR replays seeds twice and compares trace hashes. Buffers are poisoned after release at every asynchronous boundary.

### Simulator faults
- Byte-level disk with volatile and stable state and directory ops: torn, lost and misdirected writes, slow, stalled and failing fsync, ENOSPC. Disks that lie about fsync are outside the guarantee, but detectable damage must fence the node.
- TCP as ordered streams: head-of-line blocking, stalls, half-open connections; all 64 directed-link reachability masks on every PR plus switching between them; short, long, switching and asymmetric delays; one link much faster than another.
- Crashes at every persistence step, repeated crashes during WAL recovery and snapshot install. Erase each node's disk in turn at checkpoints and cold-restart the other two.
- Gray nodes and barely-alive clients, each with a numeric liveness bound (for example, writes commit within 10 election timeouts with two healthy nodes). A gray leader is fenced, not just asked to transfer leadership.
- Process pauses, clock skew and jumps.

### Checkers
- Porcupine with the ported etcd model on isolated small histories, as the independent oracle.
- A revision-witness checker for large histories (1M ops in 0.4 s in a prototype) only after differential tests settle historical Range and failed-branch revision semantics.
- Strict watch checker: PrevKV required (Kubernetes rejects events without it), completeness up to a target revision, order and uniqueness.
- Durability of acked and returned data, revision rules, always-on invariants, coverage floor.
- Sequential differential tests against etcd v3.7.2 for boundaries (compaction at R-1, R, R+1, error codes, max request size).
- About 18 planted bugs behind a test-only build tag; each must be caught within a fixed seed budget or CI fails.

### Real processes
Fault proxy on every link, tc netem, SIGKILL, SIGSTOP, CPU throttling, a fault-injecting file layer. Jobs verify every requested fault was actually installed. TLS, HTTP/2 and Go-scheduler bugs get regressions at the real-process layer.

### Budgets and regressions
Measure the full harness per seed on ubuntu-24.04-arm first (cap 256 MiB per simulator worker), then size PR, nightly and release campaigns. A failing seed becomes a shrunk schedule and a named scenario test that fails on the bad commit.

