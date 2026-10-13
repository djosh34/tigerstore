# Source audit of non-CPU throughput limits

This is the initial source-only audit. Later measurements and generator corrections are in [REPORT.md](REPORT.md). The LIST population arithmetic below uses 1024-byte values.

## Scope and evidence

Audited tigerstore main at `e5c3327e3800edf64f86ed8c1971ba7ebc1e8342`, on branch `exp/pre-50-shape-audit`. All tigerstore paths and line numbers below refer to that immutable commit. This is source reasoning, not a benchmark. No Go command, server, test, benchmark or Actions job ran for this audit. No source code changed.

Read `~/.agents/skills/unslop/SKILL.md`, the complete `docs/design.md`, issues [#46](https://github.com/djosh34/tigerstore/issues/46), [#1](https://github.com/djosh34/tigerstore/issues/1), [#50](https://github.com/djosh34/tigerstore/issues/50) and [#74](https://github.com/djosh34/tigerstore/issues/74), including their comments available at about 13:53 UTC on 2026-10-13. Also read both `bench` scripts, both workflows, and the relevant generator, server, driver, core, MVCC, WAL and pinned dependency source.

Issue #74's original audit is of `84c19cf5`, not this source. Its per-RPC ServeHTTP path and duplicate ordinary-Put MVCC value findings are obsolete here. Its later lean-generator results use `aa9d9b6e`, not a measured run of this pin. The engine/server paths inspected here have no diff against that earlier pin, but the generator and methodology need their own identities. Neither old measurements nor CPU sample percentages establish the limiting wait for the requested workloads.

The requested concurrency is interpreted as total outstanding operations, denoted `C`, not streams per connection. Report both numbers in any experiment. The pinned benchmark defaults to six connections times 256 streams, or 1,536 outstanding requests. It does not already implement the requested 256/1,024/4,096 matrix.

## Findings that matter first

- For leader-only 1 KiB Put, the local append clock and closed-loop concurrency can leave the CPU idle. The source does not establish a sustained throughput ceiling from either one. At these concurrency levels the configured client stream count and normal ingress byte windows are large relative to the request population, especially over several connections.
- Each follower has both a 256-message Raft append window and a soft 32 MiB payload window. The count limit matters when append messages are small. A byte-only bandwidth-delay calculation misses it.
- Follower ingress changes batching. A follower packs its own queued writes and forwards immediately. The leader accepts those proposal entries through `Step`, outside its local append-clocked accumulator. Peer socket buffering does not combine their Raft entries or remove each proposal's Raft broadcast.
- At 10 KiB, each 4 MiB entry holds about 405 conservatively charged ordinary Puts with the generator's keys. Network, shared runner disk bandwidth, durable group time and payload retention become plausible limits before a CPU ceiling. They are candidates to diagnose, not demonstrated bottlenecks.
- Linearizable point reads need a safe ReadIndex and applied-index barrier, but no data WAL append in a read-only steady state. A 1,000-key LIST adds a full owner scan and roughly 1 MiB of response data for 1 KiB values. Ordinary LIST responses are fully copied into an encoded buffer before transmission.
- The 64 MiB client-connection accounting is a receive-credit pressure signal, not a hard response-memory reservation. Tiny LIST requests can all be admitted before large replies appear. Neither that accounting nor the streaming 4 MiB queue bounds all unary output.
- The lean generator uses static endpoint assignment per connection and 1 GiB receive windows. Real clientv3 uses per-RPC round robin across ready endpoints. Kubernetes creates clients per storage instance and multiplexes long-lived watches. Synthetic round robin is not evidence of production request, byte or connection balance.

## Actual windows and queues

### Raft and replication

| Mechanism | Actual setting and semantics | Source |
| --- | --- | --- |
| One local command batch | `MaxBatchBytes = 4 << 20`. Admission counts request wire bytes plus 41 conservative framing bytes per operation. A full batch can bypass the append-clock hold. The outer batch-count varint is separate. | `internal/core/node.go:82-94,510-527,571-596`; `internal/core/codec.go:36-78` |
| Append message size | `MaxSizePerMsg = 4 << 20`, measured as protobuf-encoded entries. Raft always allows a first entry even if it exceeds this target. This is not a transport frame limit. | `internal/core/node.go:230-241`; Raft v3.7.0 `util.go:274-299` |
| Replication message count | 256 outstanding nonempty `MsgApp` messages per follower in replicate state. A message may contain multiple entries and an entry may contain many RPCs. Empty append messages do not consume an inflight slot. Probe state is less permissive. | Raft `tracker/progress.go:160-184`, `tracker/inflights.go:22-33,122-125` |
| Replication payload bytes | `MaxInflightBytes = 32 << 20` per follower. Raft counts `len(Entry.Data)`, not TLS/TCP bytes or full message protobuf size. It permits the last message that crosses the threshold. | `internal/core/node.go:234-236`; Raft `raft.go:618-661`, `util.go:300-318`, `tracker/inflights.go:44-73` |
| Outstanding apply bytes | Unset `MaxCommittedSizePerReady` defaults to `MaxSizePerMsg`, hence 4 MiB. With async storage it covers outstanding apply messages. This does not bound a driver's entire turn or Range execution. | Raft `raft.go:193-200,317-321`; `internal/core/node.go:654-684,750-759` |
| Leader proposal quota | `MaxUncommittedEntriesSize = 1 << 30`. Raft refuses additional nonempty proposals above this accounting threshold, except the first proposal into an empty tail. Async apply acknowledgements reduce the accounting. It is not a retained-log limit. | `internal/core/node.go:237`; Raft `raft.go:1208-1209,2090-2125` |
| Messages held for durable term/vote | At most 1,024 message references while a term/vote write is outstanding. This is a count bound, not a byte bound. | `internal/core/node.go:721-727,982-996` |

Do not treat the 256-message setting as 256 entries or 256 operations. Do not count the 32 MiB twice as one shared window across the two followers. A healthy leader needs itself and one follower for the usual durable commit path; a slow second follower does not impose its window rate on every leader-origin acknowledgement. A client on that slow follower still needs that origin's durable application before its reply.

### Peer transport

`internal/peer/peer.go:62-77,212-251,271-330,397-439` implements one outbound TLS/TCP connection per peer, plus the opposite directed connection. There is no peer HTTP/2 window.

Each outbound peer has separate data and control queues. Each queue allows 4,096 messages and at most 64 MiB of estimated retained bytes. The estimate is 64 bytes per message, 32 per entry, entry data, context and snapshot data. It is neither exact heap use nor exact encoded bytes. `MsgApp`, `MsgSnap` and forwarded `MsgProp` use the data queue. Heartbeats, votes, ReadIndex and replies use the control queue.

A dequeue releases the queue charge before marshal and socket write. Writer buffers, the current message, TLS records and kernel send buffers are outside that queue count. Frames have a hard 64 MiB serialized limit. Each reader and writer has a 256 KiB bufio buffer. The writer marshals each message separately, appends a four-byte frame length and payload, and flushes when its queues are empty. A larger message or full buffer also causes writes. This batches socket writes, not proposals or Raft entries.

Control is chosen before queued data, but cannot interrupt a frame already marshaled or written, or overtake bytes in TCP. `Send` drops on queue exhaustion and reports unreachable instead of blocking the owner. Inbound `Deliver` blocks at the shared node inbox. A blocked receive goroutine cannot read later control frames on that connection. Five-second write deadlines detect a blocked actual write, not every quiet half-open connection. There is no application-level receive heartbeat timeout here.

### Driver and disk

`internal/node/node.go:190-202,235-240,264-276,298-395,482-515` gives the following count bounds:

- The shared inbox contains 8,192 input events. A unary batch is one event, not one operation. It can contain 256 commands. Peer input and client input share this channel.
- A turn handles one selected input and up to 4,096 additional queued input events before `Process`. In the all-full-client-batch case that permits 1,048,832 submissions before Raft processing. This is a theoretical count allowance, not a typical batch size or measured turn duration.
- The owner's persistence backlog stops new input selection at 64 effect batches. The separate `persistQ` channel can also contain 64 batches, with a disk group in progress and one completed group on `persisted`. Thus 64 is not a total jobs, bytes or process-memory bound.
- Ticks and disk completions remain selectable during admission pressure. Incoming peer control messages do not have a separate bypass into the owner. `SubmitBatch` can block the connection reader on inbox admission, so that reader is not guaranteed to keep reading pings or credit updates under a full inbox.

The disk goroutine drains every currently queued effect batch, calls `WAL.Append` for each job in order, performs one `WAL.Sync`, then reports jobs in the original order. The ordinary file data write and fsync are one group. A group may contain several entries and hard-state records. The 4 MiB cap bounds an entry, not this write or group.

`internal/wal/wal.go:65-76,315-339,342-426` uses 64 MiB segment targets. Each entry adds 26 record bytes plus `Entry.Data`; a hard-state record adds 33 bytes. Rotation happens after the whole group is synced and may overshoot the segment target. New-segment header file sync and directory sync are additional durable operations. They can add wall-time stalls without high process CPU. Preserve their order.

### Client HTTP/2 and admission

| Mechanism | Setting and effect | Source |
| --- | --- | --- |
| Concurrent streams | 1,048,576 per connection. Not 256 or 1,024. The latter can be generator choices. | `internal/server/h2.go:25-42,518-555` |
| Request receive size | 2 MiB raw gRPC message. Core write validation separately limits the canonical InternalRaftRequest to 1.5 MiB. A literal 1.5 MiB value is not necessarily an accepted write. | `internal/server/h2.go:36-37`; `internal/server/stream.go:105-135`; `internal/core/node.go:445-493` |
| Stream receive window | 4 MiB. Streaming input is credited as `Recv` consumes messages, at 1 MiB thresholds. A unary request fits without stream-credit recycling. | `internal/server/h2.go:32,602-638`; `internal/server/stream.go:155-182` |
| Connection receive window | 16 MiB, with credit returned in steps of at least 4 MiB if the held-byte accounting permits it. | `internal/server/h2.go:35,387-394`; `internal/server/batch.go:174-179` |
| Reader handoff batch | At 256 requests, 4 MiB of bodies, or before the next frame read could block. No fill timer. The body-byte test happens after adding a request, so the handoff may overshoot 4 MiB by the last body. | `internal/server/batch.go:20-24,46-97`; `internal/server/h2.go:251-282` |
| Connection pressure threshold | 64 MiB of request charges, encoded reply charges and estimated waiting replies. Request charge is body size plus 512. This withholds receive credit; it does not refuse every over-budget request or reserve future response space. | `internal/server/batch.go:25-34,103-179,194-285` |
| Streaming output queue | 4 MiB per stream, checked by `Send`. One oversized message can enter an empty queue. This wait does not apply to ordinary unary replies. | `internal/server/h2.go:38-40`; `internal/server/stream.go:191-212,243-272` |
| Client writer | Initially 65,535 connection and stream send credits, then whatever the client advertises and grants. DATA is capped by connection credit, stream credit, peer max frame and a 1 MiB writer-take budget. Default frame payload is 16,384. | `internal/server/h2.go:123-128,315-357`; `internal/server/write.go:20,95-173` |
| Stalled write | 20-second deadline on actual socket writing. A reply waiting only for HTTP/2 credit can remain queued without an actual write to time out. | `internal/server/write.go:45-87`; `internal/server/h2.go:40-42` |

The 64 MiB threshold is not a hard memory cap. Partial request buffers, already granted receive credit, batch overshoot, decoded objects, inflight frame buffers and preexisting admitted calls sit outside a simple hard-bound interpretation. A LIST request is tiny but its result can be large. Thousands of such requests can enter while their future output reservation is zero.

The watch driver's separate limit is 4,096 responses and 64 MiB per stream. It closes a lagging watcher with resumable `Unavailable`. Those limits do not constrain unary Range output. See `internal/node/node.go:61-86,365-390`.

## Batching, owner work and copies

### Ordinary Put and follower forwarding

The client reader copies DATA out of the reusable framer into an exact-size gRPC message body. `ParsePut` borrows key and value slices. `SubmitAll` allocates one pending-record array per admitted batch. On the owner, validation, pending maps, timeout lists and conservative proposal sizing still run per command.

The leader packs local queued requests into one entry, copying their wire bytes in `encodeBatch`. While its own previous proposal append is not durable, it accumulates local writes. An entry at the byte cap goes out early. Pure forwarded proposal appends and hard-state-only writes do not extend this hold. `Persisted` advances the durable index and releases Raft's ordered storage responses. The owner computes results at committed application, not before commit.

A follower calls `propose` without that local-append wait. Its queued commands become one packed entry per proposal call. Raft forwards the `MsgProp`; the leader's `Step` appends the supplied entries and broadcasts immediately. The leader does not put forwarded commands into `n.proposals` or re-pack them with its local commands. Several entries can later share an append message or disk group, but that is a different batching layer. Sources are `internal/core/node.go:514-527,571-596,654-673,847-923`, Raft `raft.go:1294-1353,1718-1734` and `rawnode.go:90-97`.

One encode is not one payload copy. Pinned Raft v3.7.0 `raft.go:812-819` performs `proto.Clone` of each accepted proposal entry on the leader, including its byte data. Thus after an assembled ordinary Put body, the leader copies into the packed batch and Raft copies that batch into its accepted entry. A follower-origin proposal has peer decode ownership before the leader's proposal clone. This is transient duplication, not a second retained MVCC value store.

Peer writers copy entries into protobuf output and TLS/kernel buffers. Peer receivers unmarshal into owned entry bytes. The WAL copies entries into its record buffer. At apply, ordinary Put parsing is in place and MVCC retains slices of the accepted log entry without another value copy. MemoryStorage retains entry pointers. Transactions still decode protobuf at application and can retain additional decoded value storage. Do not project the ordinary-Put copy path onto Kubernetes CAS Txn.

The owner still executes B-tree lookup, version-slice append, revision assignment, lease bookkeeping and watch matching on every replica. Matching is proportional to watchers times changes. Higher follower ingress does not offload that replicated state work away from the leader. It moves some client TLS, parsing and response writing to follower processes and adds peer forwarding work.

### Reads and LIST

`internal/core/node.go:599-609,764-843` batches newly queued linearizable reads into one ReadIndex per core turn. It serves them only after the returned index is applied. Missing indices retry every three 100 ms ticks and on discovery of a new leader. There is no lease-based unsafe read shortcut, and no configured ReadIndex inflight count window equivalent to append's 256 slots. The same peer transport and owner scheduling still affect it. Followers request their own batched safe index and read local state.

A present-key point Range uses B-tree lookup and binary search through that key's versions. A prefix Range walks every matching key to compute `Count`. With default ordering and no revision filters, `Limit = 1000` caps collected results at 1001, but the scan still traverses the entire remaining matching range for the count. If there are exactly 1000 matching live keys it walks those 1000. It is not constant owner work just because the requested limit is 1000. Sorted or revision-filtered variants can collect more before filtering and limiting. See `internal/mvcc/ops.go:56-111` and `internal/mvcc/store.go:44-77,110-123`.

The owner allocates KeyValue objects containing borrowed immutable log slices, response headers and result slices. `Complete` performs `proto.Size` on each protobuf reply on the owner, so large Range results also require a sizing walk there. It atomically publishes a per-connection completion list without waiting for the writer. The writer sizes again, allocates the whole gRPC response and marshals all key/value bytes into owned output. This full copy happens before flow-control transmission. A completion that already lost its stream can still be encoded before the writer checks reset/done under its lock. Sources are `internal/server/batch.go:194-285` and `internal/server/stream.go:200-212`.

The writer slices the encoded buffer into DATA frames. It then copies frames through bufio and TLS. No `writev` or direct log-to-socket scatter/gather bypass exists. Slow clients retain encoded responses and, before encoding, the completion objects referring to log data. The log already retains that data independently.

Fairness is one frame per ready stream per pass, with a 1 MiB DATA take budget. The ready list is not rotated when that budget ends. With default 16 KiB frames, more than 64 large, ready replies can leave later replies waiting behind the first 64 across repeated takes. This is a source-level tail-risk mechanism, not a measured starvation result. See `internal/server/write.go:95-121`.

Reply accounting releases when `endStream` selects the terminal frame into the writer's frame list, before the actual socket flush. GC-owned buffers remain live through those frame references, so the current code does not recycle their bytes. A pooling experiment must not mistake release of that charge for permission to overwrite the bytes before flush or abort.

### Unbounded retained data

The log and WAL retain all entries. MVCC compaction removes old index versions but does not trim the in-memory Raft log or delete WAL segments. Overwrites therefore keep growing the log even with a fixed key population. Recovery replays from index 1. Live slices can pin a whole entry, and recovered slices can pin a whole segment read buffer. A queue limit or transient arena does not solve this. See `internal/core/storage.go`, `internal/wal/wal.go:108-206,275-309` and the design's cleaning/checkpoint section.

Compaction, watch catch-up sorting and lease revocation also execute synchronously on the owner. No wall-time owner-turn budget limits these operations. `Process` drains ready work and can serve an entire read batch before returning. These are owner CPU costs, but they also delay the control, admission and completion progress needed to keep I/O pipelines occupied. They should not be mislabeled as an independent off-CPU wait from a CPU profile.

## Conditional bounds for the requested workloads

All formulas below are capacity conditions under stated assumptions. They are not measured rates, promises of gains or an attribution of today's limit. Network RTT alone omits durable follower time, transport queues and scheduling.

### Request sizes and concurrency

The generator produces 45-byte pod keys. A canonical ordinary Put with no lease or previous value has `w = v + 50` protobuf body bytes for these value sizes. Its HTTP/2 DATA payload is `w + 5`; per-call request charge is `w + 512`. Core proposal accounting uses `w + 41` per command.

| Total outstanding requests | 1 KiB Put DATA population, MiB | 1 KiB request charges, MiB | 10 KiB Put DATA population, MiB | 10 KiB request charges, MiB |
| ---: | ---: | ---: | ---: | ---: |
| 256 | 0.263 | 0.387 | 2.513 | 2.637 |
| 1,024 | 1.054 | 1.549 | 10.054 | 10.549 |
| 4,096 | 4.215 | 6.195 | 40.215 | 42.195 |

These are arithmetic populations, assuming all requests are ordinary Put and simultaneously retained, with no replies included. They are not process heap measurements. Across several connections the pressure is split. Even 4,096 outstanding 1 KiB Puts all on one connection fit the nominal 16 MiB receive window and sit below 64 MiB request accounting. At 4,096 10 KiB Puts, one connection cannot send the whole population without receive-credit recycling. Multiple connections change that condition. The 4 MiB core entry accounting admits at most about 3,761 ordinary 1 KiB Puts or 405 ordinary 10 KiB Puts under this conservative charge.

A closed-loop population obeys `lambda = C / mean_latency` when all slots remain busy and there is no think time. It can only reach a proposed rate `lambda` if `C >= lambda * mean_latency`. For illustration, if the entire mean completion time were fixed at the values below, the population budgets would be:

| Mean completion time | C = 256 | C = 1,024 | C = 4,096 |
| ---: | ---: | ---: | ---: |
| 1 ms | 256,000/s | 1,024,000/s | 4,096,000/s |
| 5 ms | 51,200/s | 204,800/s | 819,200/s |
| 10 ms | 25,600/s | 102,400/s | 409,600/s |
| 20 ms | 12,800/s | 51,200/s | 204,800/s |

The table does not assert any of those latencies. Leader-origin writes overlap local append with peer replication; a useful path model is client overhead plus `max(local_durable_time, peer_RTT + follower_durable_time)`, followed by owner application and writer delivery. Local batching, segment rotation and scheduling add time. Follower-origin writes also need forwarding and delivery of commit knowledge to the origin. The design's approximate `2 * peer_RTT + durable_time` is a planning model, not a source-proven latency constant.

At 256 leader clients, modest completion waits alone can cap a closed-loop rate below a CPU-only forecast. Increasing to 1024 or 4096 tests that population limit, but also changes batches, memory and tails. A gain would not by itself prove the old wait was fsync. At 4096, exceeding one entry's capacity does not impose a 4096-operation ceiling; entries are pipelined and the byte cap can propose full local entries before the preceding append finishes.

### Local disk and replication windows

Let `g` be successful operations per durable disk group and `a` the mean serial disk-group service time including write, required syncs and rotation. A fully occupied disk worker has the conditional budget `lambda <= g / a`. Measure both terms on the actual workload. Entries per fsync and operations per entry are different quantities. The local append-clock batching approximation concerns admitted local traffic, `batch_ops approximately lambda_local * append_completion_time`, not all forwarded writes.

For a replicated follower, let `b` be operations per nonempty append message, `s` be packed payload bytes per operation, and `R` be the message lifetime until its durable acknowledgement frees inflight accounting. Ignoring the byte threshold's final overshoot, a planning condition is:

`lambda approximately <= min(256 * b / R, 32 MiB / (s * R))`.

For equal-size messages with fixed lifetime, the source's soft byte behavior gives at most `min(256, ceil(32 MiB / message_payload))` occupied nonempty message slots. This is the more precise uniform-message model. Variable size and acknowledgement batching require actual lifetime and occupancy counters, not that simple quotient.

At a hypothetical `R = 10 ms`, the count-only budget is 25,600 operations/s for one operation/message, 409,600/s for 16/message and 1,638,400/s for 64/message. Divide all of these by two if the durable acknowledgement lifetime is 20 ms. These are not benchmark results. Control messages and forwarded `MsgProp` do not consume this Raft append window.

For an illustrative boot varint of ten bytes and request-ID varint of three, batch framing contributes about 17 bytes/operation. Then `s` is about 1,091 for a 1 KiB Put or 10,307 for a 10 KiB Put, plus a shared count varint. The nominal 32 MiB byte budgets at 10 ms are about 3.08 million and 326 thousand operations/s per follower. The count/byte crossover is 128 KiB payload/message, about 120 small Puts or 13 large Puts. The final allowed append can overshoot the byte budget by roughly one append's payload. Actual ID widths, entry batching and durable lifetime change the arithmetic.

This explains why a 32 MiB byte window can coexist with a much smaller count-limited rate when follower ingress produces tiny proposals and immediate broadcasts. It does not establish that the window is full in any requested run. Inspect full-window intervals and actual operations per append before changing it.

The peer queue is a burst buffer, not an acknowledged throughput window. Sustained offered bytes beyond writer or receiver service fill it and cause drops and probing. Increasing its size alone cannot raise sustained link service capacity. Under queue pressure, a writer's current frame and buffered bytes can delay control even though `Send` itself does not block.

### Balanced follower ingress and 10 KiB

If equal operation fractions really reach each of three endpoints, the leader accepts approximately one-third locally and two-thirds through forwarding. Replication still sends each accepted write to both followers. At the same write rate, leader inbound forwarded payload is approximately `2/3 * lambda * s`, and each follower adds roughly `1/3 * lambda * s` outbound proposal traffic. Healthy replication traffic remains roughly `2 * lambda * s` outbound from the leader before protocol overhead. Retries and catches-up increase these counts.

There is no follower forward-ack count window. The origin waits for replicated durable application, not a separate successful forward RPC. Its pending requests, shared inbox, data queue and client output provide other pressure mechanisms. The leader's local append clock can no longer be treated as the clock for all arrivals.

Let `D` be sustainable shared disk write bytes/s on the runner and `d` be actual WAL bytes/operation including amortized entry and hard-state records. Three replicas on the same disk require approximately `3 * lambda * d <= D`. On independent disks use the corresponding per-node condition instead. A hypothetical 50,000 writes/s with `s` near 1,091 requires about 164 MB/s of payload writes across three nodes; at 10,307 it requires about 1.55 GB/s. These examples do not assert that the rate or bandwidth is available. Compression of these random values is not an assumed saving.

Growing files and segment header/directory durability make primitive cached-write or fsync timings insufficient to substitute for `a` or `D`. Long runs also require `elapsed * 3 * lambda * d` free shared disk bytes and enough memory for retained entries and versions. Short-window speed need not be sustainable capacity.

### Linearizable point reads and 1000-key LIST

For a read-only steady state, ordinary point Range has no per-read WAL sync to amortize. It still needs the safe quorum check and applied-index barrier. Arrival concurrency bounds the closed-loop rate through that wait. The owner and connection writer service rates are separate constraints.

For exactly 1000 returned keys with 1 KiB values, value bytes alone are 1,024,000 per response. KeyValue metadata, 45-byte keys, nested fields and the response envelope make about 1.04 MiB a useful small-revision example, not an exact response size. At 10 KiB the corresponding response is about 9.83 MiB. The writer must marshal the whole unary message before sending its fragments.

If every outstanding LIST reaches response preparation before clients drain it, value bytes alone are 250 MiB at C=256, 1000 MiB at C=1024 and 3.90625 GiB at C=4096 for 1 KiB values. At 10 KiB multiply those figures by ten. This excludes log retention, response descriptors, encoded copies, TLS and client buffers. Do not launch a high-concurrency LIST pilot without an aggregate byte and memory budget.

For a client connection, sustained response delivery needs both enough network service bandwidth `B` and enough effective connection credit `W` over credit-return lifetime `Rc`. A conditional budget for `q` DATA bytes per result is `lambda_conn <= min(B / q, W / (q * Rc))`. Stream credit can add another condition, but client implementations can enlarge it while reading a large message. Credit replenishment batching and TCP buffers also matter. These formulas require actual effective credit and lifetime, not just the initial advertised window.

For LIST, increasing admission or replication windows does not eliminate the full response copy, owner scan, response sizing, writer fairness or downstream receive limits. Prefix count work also depends on the entire matching range and version depth, not only returned bytes. CountOnly and KeysOnly are different workloads, not substitutes for a 1000-value LIST.

## Real clientv3 and kube-apiserver behavior

### Immutable upstream sources

Fetched read-only upstream source by immutable release commits, without building it:

- etcd v3.7.2 is [`68c065e562994b89e333e77b039ad066f933c586`](https://github.com/etcd-io/etcd/tree/68c065e562994b89e333e77b039ad066f933c586). Its client module requires grpc-go v1.83.2.
- Kubernetes v1.37.1 is [`f78e722310e50bcaca9276be22276d9e91d91308`](https://github.com/kubernetes/kubernetes/tree/f78e722310e50bcaca9276be22276d9e91d91308). `vendor/modules.txt:613,937` identifies clientv3 v3.7.0 and grpc-go v1.82.1. Its vendored resolver and inspected transport files match those upstream release files byte-for-byte.
- etcd v3.7.0 is `4d71f7c83a2cef992d57abd3284edfaf66c91a4a`. grpc-go v1.82.1 is `ebd8f06a09426fbece97157c95c3917abff28f4e`.

This establishes behavior of these pinned sources, not a Kubernetes deployment result or every supported 1.35-1.37 version. Tigerstore itself imports API v3.7.2 and grpc-go v1.84.0, but does not use clientv3 in its lean generator.

### Balancing

The clientv3 resolver supplies `loadBalancingPolicy: round_robin` and one resolver endpoint for every configured endpoint. Client construction creates one logical gRPC ClientConn using that resolver. gRPC round robin creates pick-first children per endpoint and chooses across ready children for each RPC pick, with a randomized initial picker position. It does not choose the leader by default. Failed or not-yet-ready endpoints change the eligible set. Explicit `Client.Dial(endpoint)` is endpoint-specific.

Sources are etcd `client/v3/internal/resolver/resolver.go:37-75`, `client/v3/client.go:277-328,438-459`, and Kubernetes's vendored `google.golang.org/grpc/balancer/endpointsharding/endpointsharding.go:244-318` plus `balancer/roundrobin/roundrobin.go:45-59`.

With three direct, ready endpoints and sustained homogeneous unary traffic, an approximately two-thirds follower operation fraction is a reasonable scenario. It is not guaranteed equal inflight population, byte rate or completion rate. Retries, endpoint readiness, different operations, multiple client instances, proxies and explicit dials change it. Each long-lived streaming RPC stays on its chosen connection rather than moving per watch event. clientv3 multiplexes logical watches by context-derived stream key and resumes them by opening another gRPC stream after failure. See `client/v3/watch.go:325-350,543-546,947-1008`.

Kubernetes's `staging/src/k8s.io/apiserver/pkg/storage/storagebackend/factory/etcd3.go:286-365,455-504` calls `kubernetes.New` with the configured `ServerList`, client TLS and keepalive options. Every new storage instance creates a client; subresources can reuse a storage instance. The compactor uses a separately shared client per transport configuration. This supports the design's many-resource-client premise, not a literal invariant of exactly one client for every resource name. The factory does not force all resource traffic to the leader or set a static 1 GiB receive window.

The lean generator does something different. `cmd/loadgen/main.go:215-232` dials `endpoints[i % len(endpoints)]` once for each worker connection. It does not use clientv3 or a per-RPC endpoint picker. With a connection count divisible by three it assigns equal connections and equal open-loop offered rates. In closed loop, the faster endpoint's fixed slots can finish more operations, so equal connections need not produce equal ingress operations. With sixteen connections, the assignment is 6/5/5. Failed endpoints invalidate the run instead of providing clientv3's retry and rebalance behavior.

`bench/run.sh:86-101` passes only the elected leader endpoint. Its current workflow therefore measures neither synthetic balanced ingress nor production balancing. The generator's watch endpoint placement is separate from its unary connections.

Tigerstore's MemberList contains ClientURLs only for the answering node, because `cmd/tigerstore/main.go:225-231` does not know the others' advertised client URLs. Explicit clientv3 `Sync`, or enabled AutoSyncInterval, can replace a three-endpoint configuration with that one endpoint. Kubernetes's inspected factory leaves AutoSyncInterval unset, so that behavior must not be assumed for its default clients. Sources are clientv3 `client.go:186-223` and tigerstore `internal/server/server.go:204-211`.

### Receive windows and production request shape

The lean client's `cmd/loadgen/conn.go:19-26,148-154,373-402` advertises a 1 GiB stream receive window and increases connection credit to 1 GiB, returning credit in 512 MiB steps. It also batches outbound work up to about 256 KiB before flush. These are generous synthetic response windows, not stock client defaults.

Stock grpc-go starts with 65,535 stream and connection receive bytes. With default dynamic flow control it uses BDP PING sampling and can grow its connection and stream limits to 16 MiB. Connection credit returns on received DATA independently of application consumption, in roughly quarter-window increments. Stream consumption returns stream credit; `maybeAdjust` can grant extra stream credit for an active read larger than the current limit. Therefore a large unary LIST does not necessarily require one RTT per 64 KiB of its stream. Nor is `65535 / RTT` a permanent clientv3 connection bandwidth ceiling.

See Kubernetes's vendored transport `http2_client.go:316-320,390-399,1147-1239`, `flowcontrol.go:80-108,134-201` and `bdp_estimator.go:27-43,103-139`. The same mechanisms exist in tigerstore's test client grpc-go v1.84.0. Actual warmup, RTT, competing streams and read behavior determine credit. A cold stock connection and a lean connection opened with 1 GiB credits are not matched flow-control cases.

clientv3 sets its default receive-message limit to `math.MaxInt32`, rather than bare grpc-go's 4 MiB default. Its send limit is 2 MiB. A roughly 10 MiB unary LIST is not inherently rejected by clientv3's default 4 MiB receive limit, because that is not its limit. See `client/v3/options.go:25-40`.

Kubernetes writes are optimistic transactions, not plain Put. Its pinned adapter compares ModRevision, puts on success, and can get on failure. Point reads and LISTs use the same client KV connection. LIST uses a range end for the prefix, a requested limit and revision, then continues at a pinned revision when more pages are needed. It can try RangeStream when enabled and supported, and falls back on Unimplemented. Tigerstore's method table returns exactly that code for RangeStream.

Sources are etcd v3.7.0 `client/v3/kubernetes/client.go:47-106`, Kubernetes `staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go:747-981` and tigerstore `internal/server/rpc.go:25-55`. The pinned generator's `range` workload is only a present-key linearizable point read, as `cmd/loadgen/wire.go:228-232` and `workload.go:286-343` show. There is no 1000-key prefix-LIST generator mode in this pin. API-server watch caches can also serve requests without an etcd RPC; an API LIST result must not automatically be described as this direct storage LIST workload.

## Safe, promising experiments, not measured gains

The source suggests the following order. No experiment was launched here.

1. Diagnose waits before changing queues. On one immutable binary collect local append submission-to-completion wall time, operations/entry, operations/fsync, file bytes/group, rotation duration, append inflight count/bytes/full intervals, peer queue bytes/drops and per-node CPU. Record client intended-send backlog and connection flow-credit stalls. Capture intrusive traces separately from primary paired windows. Higher C distinguishes a population limit only if service time and batches are also recorded.
2. Test append-window sizing only when its full intervals are material. Raise count at fixed bytes, or bytes at fixed count, as separate candidates. Keep messages bounded and verify control latency, slow-peer isolation, unreachable recovery and memory. Raising the count cannot beat disk or client service capacity, and enlarging peer queues alone is not the same experiment.
3. Test follower proposal aggregation without a fill timer. Compare leader-only, explicit synthetic balanced offered ingress and actual clientv3 ingress. Count origin commands per forwarded entry and operations per leader append. Aggregate only commands or entries that have not already been ordered by Raft. Preserve each origin, boot and request identity, ordered execution, each RPC's revision and CAS semantics. Never change already-proposed Entry.Data. A socket-write batching patch is not a forwarding-aggregation result.
4. Improve LIST response preparation and fairness as separate experiments. Rotate the ready-stream starting point after a DATA budget expires. Consider a bounded, pinned immutable response plan with helper encoding or incremental transmission. Keep one complete ordinary gRPC RangeResponse and unchanged Count, More, order, revision filters and historical semantics. Do not replace a valid unary response with a private streaming protocol. A subtree count index can avoid the full count scan, but needs correct version/tombstone accounting.
5. Add byte-aware admission with control/completion reserve if overload measurements justify it. Do not block the owner while reserving output. Keep large legal requests and resumable watches accepted under the existing API contract. Keep receive/reset/deadline processing able to release pressure. Account encoded responses through actual flush or abort before pool reuse. A tiny LIST request needs a future-output strategy, not only a 512-byte call charge. Treat all such bounds as transient memory bounds, not a database capacity claim.
6. For long 10 KiB and object-capacity runs, separate shared disk throughput and retained-log growth from steady-state CPU. Checkpointing and cleaning require the durable recovery-root protocol before deletion. Compression remains conditional on real data, fanout, CPU and read amplification. Independent random values provide no justified compression gain.

Every candidate keeps the owner-fixed guarantees: the etcd v3 API needed by kube-apiserver, client TLS and certificates, go.etcd.io/raft, Go and one executable per node, three regional replicas, encrypted peer traffic, linearizable reads and writes, and survival of every acknowledged write and its value bytes after one node is lost. Measurements stay on free GitHub runners under the requested layout. No security or exploit work is proposed.

The implementation-specific protections also stay intact. Preserve ordered async persistence responses, durable term/vote message dependencies, durable truncation and recovery validation, fatal handling of failed fsync, disk/commit watchdogs, control priority, boot/request identity, transaction atomicity, lease ordering and expiry, watch order/previous values/replay/progress/compaction cancellation, deadline/reset behavior, peer/client byte limits and healthy-client progress under a slow peer, disk or consumer. No response or watch observes uncommitted speculative state. No socket write becomes a durable acknowledgement. Cancellation may still be ambiguous about whether an already-proposed write commits.

A later runner report must name exact engine, generator, observation and workflow commits, store binaries, connection and outstanding counts, value/key sizes, endpoint allocation and actual ingress fractions, client window behavior, delay definition, duration and drain, layout, barriers, all five repetitions, errors and fences. Keep invalid or failed windows visible. Pair etcd v3.7.2 and tigerstore sequentially on the same VM and resource budget. Do not infer a measured gain from the arithmetic in this audit.
