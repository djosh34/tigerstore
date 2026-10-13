# #50 allocation-path audit — base `0d406cb`

Read-only source census, with the **final baseline** allocation-profile delta. Production-source references below are to `git show 0d406cb:<path>`, not the concurrently edited worktree. Only this report was written; no build, test, benchmark, or subagent was run for this audit. The only Go commands were analysis of the supplied profiles using `go tool pprof`.

## 1. Scope, evidence, and interpretation

Canonical path: an established TLS/HTTP2 connection sends one successful unary `/etcdserverpb.KV/Put`, with a 1 KiB value, to the leader; no lease, `PrevKv`, ignore flags, watches, compression, or client `grpc-timeout`. The serial test overwrites one key, so request batches and completion groups normally contain one Put. Larger batches amortize some sites listed below; a serial result does not establish their high-concurrency cost.

Final evidence is `evidence/alloc50/local-base/`:

- Profile executable name: `alloc50-base.test`; build ID: `fb2a61cb503dfb80b824a6c6c9514c4ab0645679`.
- Matching executable SHA256, recorded in `binary.sha256` (compressed executable retained as `server.test.gz`): `e5e4df7931ba27431aeabf167cb7bd5023e21e19c9e3d2a8c1bff93feda6f79f`.
- `before.pb.gz` SHA256: `ff5e3fb233c6844446cc0fc482e13d9872ef738d7aa92f7ac0e1d3e889f7d277`.
- `after.pb.gz` SHA256: `87a72df0f6a72a447af2a0188cd26f8f9ff66e87fd8bfc5104781d41275e2907`.
- Hashes were unchanged across this analysis. Earlier baseline runs are **not** used in any count below.

The updated `TestAllocationPath` checks exact +1 response revisions and all three replicas' values; replica-value checks are outside the counters. The profile/MemStats mechanism is global. **These are client + leader + two followers, not leader-only measurements.** A stack containing `internal/core` does not identify which replica allocated it. Readiness, connection establishment, and 32 warmup puts precede the measured 256 puts.

| Final MemStats metric | Value |
|---|---:|
| Successful measured puts / value bytes | 256 / 1,024 |
| `MemProfileRate` | 1 |
| Leader ID | 2 |
| Mallocs / Put | 326.05859375 |
| Allocated bytes / Put | 19,757.90625 |
| HeapAlloc after forced GC | 15,115,608 |

The last number is total live heap, including cluster startup, retained history, client, and profiling machinery; it is neither incremental retained bytes/Put nor a steady-state retention bound.

Reproduction (analysis only; append the matching executable path if requesting source/disassembly listings):

```sh
GOFLAGS=-p=2 GOMEMLIMIT=2GiB go tool pprof -top -lines \
  -nodefraction=0 -edgefraction=0 -sample_index=alloc_objects \
  -base=evidence/alloc50/local-base/before.pb.gz \
  -ignore=runtime/pprof '-focus=internal/(server|core|node)' \
  evidence/alloc50/local-base/after.pb.gz
# For flat-byte counts: -sample_index=alloc_space -unit=bytes.
# Remove -focus for the complete production-site inventory below.
```

`-ignore=runtime/pprof` excludes whole sample stacks containing pprof, including allocations made while writing the before profile. The raw delta is **90,209 profiled blocks**. Ignoring those stacks leaves **63,851**; additionally focusing server/core/node leaves **52,583**. Pprof's displayed percentages still use 90,209, not the filtered subtotal. The corresponding unfocused, profiler-excluded flat-byte total is **5,094,368 bytes** (raw delta: 10,862,600).

These are exact **profile-record deltas**, not an exact census of logical Go allocation expressions. In particular, Go's tiny allocator packs small pointer-free allocations into blocks: MemStats adds `tinyAllocCount` to Mallocs (`runtime/mstats.go:430–431`), while the memory profile counts allocated blocks. Thus scalar-pointer/header-capture lines can have fractional blocks/Put even when their source expression runs once/Put. The profile interval also includes its boundary bookkeeping, GC, and asynchronous background activity; it is wider than the MemStats loop. Do not force the filtered profile sum to equal Mallocs, or describe a fractional line count as a fractional source invocation. `make`/address-taking is only a heap-allocation candidate until confirmed by this binary; zero observed growth is not a universal zero-allocation guarantee.

Discussion context read: #50's correction in `/tmp/alloc-50-current.txt`, #74 in `/tmp/alloc-74*.txt`, and PR77 in `/tmp/alloc-pr77*.txt`. #50 does **not** prohibit channels: it targets per-request channel/request/reply churn and hot-path blocking. #74 is advisory, not a new acceptance gate. Whole-batch ownership, byte-bounded capacity, and progress/control reserves are useful design constraints. PR77's in-place Put and handwritten socket reply must survive any storage-reuse experiment.

## 2. Exact managed-site profile census

All nonzero **flat** production allocation lines in server/node/core/MVCC/peer/WAL are listed below. Paths are relative to `internal/`. `Objects/Put` is rounded to six decimals; the integer delta divided by 256 is the exact ratio. Bytes include allocator size-class rounding. Cumulative counts must not be added to these flat counts.

| Base source allocation line | Flat objects | Objects/Put | Flat bytes |
|---|---:|---:|---:|
| `core/codec.go:37` | 256 | 1.000000 | 294,912 |
| `core/node.go:376` | 256 | 1.000000 | 36,864 |
| `core/node.go:378` | 53 | 0.207031 | 4,072 |
| `core/node.go:511` | 256 | 1.000000 | 2,048 |
| `core/node.go:723` | 768 | 3.000000 | 18,432 |
| `core/node.go:726` | 1,575 | 6.152344 | 75,600 |
| `core/node.go:728` | 372 | 1.453125 | 5,952 |
| `core/node.go:730` | 53 | 0.207031 | 8,144 |
| `core/node.go:905` | 256 | 1.000000 | 16,384 |
| `core/node.go:981` | 2,322 | 9.070312 | 23,376 |
| `core/state.go:92` | 768 | 3.000000 | 12,288 |
| `core/storage.go:49` | 1,936 | 7.562500 | 67,840 |
| `mvcc/ops.go:234` | 768 | 3.000000 | 36,864 |
| `mvcc/ops.go:250` | 9 | 0.035156 | 105,216 |
| `node/node.go:186` | 256 | 1.000000 | 20,480 |
| `node/node.go:321` | 5 | 0.019531 | 512 |
| `node/node.go:351` | 1,575 | 6.152344 | 37,824 |
| `node/node.go:408` | 256 | 1.000000 | 20,480 |
| `node/node.go:464` | 4 | 0.015625 | 448 |
| `node/node.go:473` | 12 | 0.046875 | 1,248 |
| `peer/peer.go:231` | 6 | 0.023438 | 4,640 |
| `peer/peer.go:240` | 2,322 | 9.070312 | 482,976 |
| `server/batch.go:159` | 256 | 1.000000 | 6,144 |
| `server/batch.go:194` | 256 | 1.000000 | 12,288 |
| `server/batch.go:243` | 256 | 1.000000 | 16,384 |
| `server/batch.go:73` | 256 | 1.000000 | 32,768 |
| `server/batch.go:74` | 256 | 1.000000 | 4,096 |
| `server/h2.go:433` | 256 | 1.000000 | 81,920 |
| `server/stream.go:122` | 256 | 1.000000 | 294,912 |
| `server/stream.go:275` | 256 | 1.000000 | 12,288 |
| `server/stream.go:278` | 256 | 1.000000 | 20,480 |
| `server/stream.go:298` | 256 | 1.000000 | 40,960 |
| **Sum of these flat production lines** | **16,644** | **65.015625** | **1,798,840** |

There are no nonzero flat `internal/wal` sites in this warmed run. The `select` lines `node/node.go:321`, `node/node.go:464`, and `peer/peer.go:313` are compiler-attributed select workspace, not evidence that each channel send constructs a channel. The final run has no nonzero flat `peer.go:313` sample; other runs' counts must not be substituted.

**Stable explicit request/completion-workspace subset:** server's ten allocating lines give **2,560 objects = 10/Put**. Add node batch (`186`) and completion (`408`), and core pending (`376`), proposal-pointer array (`511`), encoded batch (`codec:37`), and reply array (`905`): **4,096 objects = 16/Put**, **913,408 bytes = 3,568 bytes/Put**. This subset is a useful optimization target, not the entire leader cost. Some of its sites are intrinsically per-request, others happen once/Put because the test is serial. The three replicas add **1,536 = 6/Put** at `State.track` and `writer.latest`, plus variable history growth. Neither subtotal includes HTTP2 objects, Raft, transport, or client costs.

Selected focused function totals explain why large cumulative stacks must not be confused with managed churn:

| Function | Flat objects | Cumulative objects |
|---|---:|---:|
| `core.(*Node).handleReady` | 2,768 | 5,812 |
| `core.(*Node).send` | 2,322 | 2,322 |
| `core.raftHardState` | 1,936 | 1,936 |
| `node.(*Node).commit` | 1,580 | 47,561 |
| Raft `(*raft).send` | 5,156 | 5,156 |
| Raft `newStorageAppendMsg` | 5,115 | 8,789 |
| Raft `(*raft).hardState` | 4,348 | 4,348 |
| Raft `(*raft).maybeSendAppend` | 3,236 | 5,935 |
| `raftpb.MessageType.Enum` | 3,003 | 3,003 |
| Raft `newStorageAppendRespMsg` | 2,879 | 2,890 |
| Raft `newStorageApplyMsg` | 2,679 | 5,377 |
| Raft `RawNode.readyWithoutAccept` | 2,087 | 20,601 |

## 3. Source census and ownership, in path order

Classification: **R** = per request/message; **B** = amortized per batch, completion group, or driver/writer turn; **C** = connection/startup; **G** = conditional capacity/map/tree growth; **V** = variable Raft/WAL/TLS/peer/background cost. An append can be G even when an ownership reset makes its first allocation unavoidable for every B.

### TLS, HTTP2, and request admission

| Site | Class | Source behavior / owner |
|---|---|---|
| `server/h2.go:108–125` | C, G | Connection/context/cancel, buffered reader/writer and byte buffers, one buffered `wake` channel, stream map, Framer, HPACK decoder, initial control array. Connection reader/writer goroutines and TLS handshake/ciphers/certificates are C/V, not per Put. |
| `h2.go:222` | C | HTTP2 preface buffer. Socket read/write deadlines do not create a request reply channel or per-Put context. |
| x/net `http2/frame.go:459` | G | Framer read payload buffer grows to largest frame; reused by next `ReadFrame`. `NewFramer` at 445 installs this cache. Server does not enable `SetReuseFrames`. |
| x/net `frame.go:1071,1711,1714,1717,1721,1755,439` | R, G | HEADERS object, MetaHeaders object, escaping metadata-parser state/emit closure, Fields append growth, DATA object. Metadata field slice starts fresh per header block; decoder/table storage is longer-lived. Final profile: 256 HEADERS objects, 256 DATA objects, 894 flat metadata blocks (256 at each of 1711/1717/1721 and 126 at 1714), and 1,024 Fields growth blocks = four growth steps/Put. Tiny-packed 1714 is not half a parser invocation. |
| x/net HPACK `hpack.go:510,517,255,272`, `tables.go:78`, `huffman.go:15` | R/G, C | Literal non-Huffman strings or Huffman decoding can allocate; indexed headers reuse table strings. Dynamic table and continuation `saveBuf` growth are G. Decoder table construction does **not** eagerly construct encoder lookup maps: `byName`/`byNameValue` are populated lazily for encoder searches. No nonzero flat HPACK-string site is observed in this warmed canonical run. |
| `h2.go:433,454` | R, G | `&stream` escapes into map/tag/ready lists; observed 320 allocated bytes/Put. Stream map inserts may grow at larger concurrency; no measured map growth here. |
| `h2.go:446–458` | conditional R | Node-run Put needs neither a per-stream context nor streaming `inSignal`/`space` channels. A supplied `grpc-timeout` adds an `AfterFunc` timer and bound callback; firing starts callback execution in a goroutine. Those are absent from the canonical test. |
| `stream.go:109,122–131` | R | gRPC's five-byte prefix is copied to inline `st.hdr`. `make([]byte,0,size)` allocates one owned message buffer; appends copy payload fragments without growth once size is known. Final 256 buffers consume 294,912 bytes (1,152-byte size class), not just 256 KiB of values. |
| `server/rpc.go:31`, `batch.go:54–74` | R/B/G | Wire Put dispatch bypasses `newOp[pb.PutRequest]` and generic Unmarshal. `core.ParsePut` returns a value whose wire/key/value slices alias the owned message. `reqs` and `tags` appends allocate fresh arrays: `flush:103–104` moves the arrays and zeroes `c.batch`. Tags are existing stream pointers in interfaces, not independently allocated tag objects. Serial final count: one array each/Put. |

`core/wire.go:26–42,50–99` preserves protobuf last-value-wins, unknown/mistyped-field accounting, and clipped key/value slice capacities. `checkPut`/`mvcc.CheckPutOp` validate decoded semantics; raw repeated/unknown fields still consume raw byte ownership. Do not substitute wire length for protobuf-normalized request-size acceptance. Parse twice (admission and apply) does not mean copy twice. Capacity clipping prevents an **append** overwriting adjacent entry fields; it does not permit mutating the bytes.

### Node handoff, pending requests, batching, and effects

| Site | Class | Source behavior / owner |
|---|---|---|
| `node/node.go:186` | B | `&batch` owns references to req/tag arrays and completer; one inbox send per connection batch. No `Do` request/reply channel is constructed for socket Put. |
| `node/node.go:306–310,425` | C/G | Waiter map, completer-index map, groups/backlog/watch maps are driver-owned workspace. Waiter values carry completer + stream tag; map storage grows only at occupancy thresholds. `groups` outer capacity is reused after `clear(groups)`; individual `cs` arrays are transferred, not reused. |
| `core/node.go:376,378` | R, G | One `&pending` (144 bytes observed) holds the copied Request descriptor and deadline; key/value still alias the message. Append to timeout list may grow. `waiting` map insertion at 509 also grows conditionally, not every request in this profile. |
| `core/node.go:511,560–566` | B/G | Proposal pointer array appends; `propose` sets `n.proposals=nil`, so next proposal group needs new pointer storage. Expired queued requests are removed with `slices.DeleteFunc`. `propBytes` includes raw Put length plus framing. |
| `core/codec.go:37,62–68` | B | One sized byte buffer per encoded batch, with count/framing appended. Each Put wire message is copied into it once. Serial count is 256 buffers/294,912 bytes. A correct size estimate prevents subsequent append growth. |
| `core/node.go:668–670` | B/G | Entire `Effects` is transferred out then zeroed. Therefore `out.Persist`, `out.Send`, `out.Replies`, etc. start fresh in the next emitting turn; they are not permanent reusable slices. Multiple outputs in one turn grow these arrays geometrically. |
| `core/node.go:723,726,728,730` | V/B/G | `&wal.HardState` for local append messages carrying Vote; PersistJob-array append; appending tick history; leader-only uncommitted-index history. Profile totals reflect three replicas and multiple Ready/storage turns, not one job per Put. |
| `core/storage.go:49` | V | Storage conversion creates `raftpb.HardState` and scalar pointers for term/vote/commit. Flat profile attribution includes tiny-packed scalars; do not read 1,936 as 1,936 complete state transitions. |
| `core/node.go:977,981` | V/B/G | Send-array appends, or held-message array growth while term/vote is unsynced. Already-built Raft message pointers are forwarded; appends do not deep-copy entry data. |
| `node/node.go:351,340–342,473` | V/B/G | Backlog outer slice append; prefix removed as work goes to persistence; persist worker merges job arrays via append. Prefix slicing loses reusable headroom and can cause recurrent growth. Jobs/entries must survive through Sync and persisted acknowledgement. |

Base verification of the PR77 lease-expiry review warning: **already accounted for here**. `core/lease.go:83–85` calls `queueWrite`, which adds `proto.Size(op)+batchFraming` to `propBytes` (`node.go:500–512`). It does not bypass size accounting. Lease expiry, sorted lease scratch, and expiry-operation allocations are conditional background work, absent with no leases.

### Raft, WAL, and peer transport

Pinned dependencies: Raft `v3.7.0`, protobuf `v1.36.11`, x/net `v0.58.0`, B-tree `v1.1.3`; standard library inspected at `/home/joshazimullah.linux/.local/opt/go1.27.2/src` (not `/usr/local/go`, which is a different version).

| Site | Class | Source behavior / owner |
|---|---|---|
| Raft `rawnode.go:91–94` | B/V | `Propose` creates proposal Message, scalar pointers, one-entry pointer slice, and Entry with `Data:data`. Initial Data aliases encodeBatch bytes. |
| Raft `raft.go:814–819` | B/V | Leader `appendEntry` creates cloned Entry-pointer slice, **deep clones** each Entry with `proto.Clone`, then creates term/index pointers. Protobuf `internal/impl/merge.go:188` uses `append(emptyBuf[:],src...)` for bytes. Final focused clone-byte count is 256 buffers (one additional full entry-data copy/batch), distinct from encodeBatch. |
| Raft `rawnode.go:168,172,224–392`, `raft.go:505–508,516–598,651–657` | V | Ready message slice/soft-state, storage append/apply messages/responses, hard-state messages and scalar pointers, ordinary Raft sends and replication slices/messages. Core enables `AsyncStorageWrites:true` (`core/node.go:231`), so these storage-protocol objects are allocated in the Ready loop. Frequency depends on Ready turns, replicas, heartbeats, persistence coalescing, retries, and elections. |
| Raft `log_unstable.go:205`, `shrinkEntriesArray`; `storage.go:320` | G/V | Unstable and MemoryStorage entry-pointer arrays grow/shrink; overwrite paths protect array aliasing. These array copies are **not** full payload copies. Profile observes 768 unstable-growth blocks and nine MemoryStorage growth blocks. |
| Raft `tracker.Visit`, quorum/status/inflight helpers | G/V | `Visit` uses a stack `[7]uint64` and allocates ID scratch only above seven peers; the three-node configuration does not imply this allocation/Put. Quorum scratch/status maps and inflight growth depend on use/size; final focused profile has one inflight growth. |
| `wal/wal.go:348–352,362–377,396` | G/V | Record framing and Entry.Data append into reusable `w.buf`; **one full value copy into WAL staging** per stored entry. Flush writes it then resets length, preserving capacity. Checksums scan bytes, not duplicate them. No warmed flat WAL allocation here, despite this copy. |
| WAL `newSegment:315–327`; `osfs` and OS calls | V | Segment rotation introduces name formatting/file/path/header and other OS allocations; opening/recovery allocates entry metadata and retains replay-buffer Data aliases (`wal.go:295`). Recovery/rotation are not fixed per-Put costs. |
| `peer/peer.go:256–260,296,306–322` | C/V | Per-peer control/data channels and byte budgets are persistent; enqueue references an existing message. Sender select workspace is compiler/scheduling dependent; queue overflow reports unreachable rather than blocking the committer. |
| `peer/peer.go:419` | G/V | Per-connection sender `MarshalAppend(buf[:0],m)` reuses capacity; marshaling copies each Entry.Data into wire scratch. Bufio/TLS introduce further transport copies. Not a newly allocated protobuf PutRequest per peer. |
| `peer/peer.go:231,240–247` | R-message/V/G | Receiver grows reusable wire scratch conditionally; **every peer frame creates `&raftpb.Message`**. Unmarshal creates entry/message/scalar/repeated-field storage and copies byte fields out of reusable scratch. Final flat receive object count 2,322, plus six scratch growths; cumulative Unmarshal dependencies lie outside the managed line. |
| Go TLS `conn.go:380,470,519,817,973,993,1402` | C/G/V | Read `rawInput.Grow` at 817 is conditional; AEAD Open at 380 decrypts in place, and `Conn.Read` at 1402 copies plaintext into caller's buffer. Write uses global `outBufPool` (pool miss at 973 allocates), `sliceForAppend` capacity miss at 470, plaintext append at 519 into scratch then in-place AEAD Seal. GC can discard pools. Record boundaries/cipher/flush size/peer connections prevent a fixed TLS allocation or copy count per Put. |

### MVCC apply and completion/response output

| Site | Class | Source behavior / owner |
|---|---|---|
| `core/node.go:879–889`; `mvcc/ops.go:206–213` | R | `ParsePut` aliases entry bytes, `State.ApplyPut` calls `PutKV`; no generic Put Unmarshal, PutRequest, or canonical-path PutResponse/Header object. `Store.start:27` returns `&writer`, but no flat allocation is observed there: do not count source address-taking as a measured heap object. |
| `mvcc/ops.go:234` | R | Temporary lookup `&keyIndex{key:key}` escapes through B-tree API in this binary: 768 blocks = one/Put/replica. It aliases the key; no value copy. |
| `mvcc/ops.go:277–278`; B-tree `btree_generic.go:105,172,243,250,268,271,686–699` | new-key/G | New persistent keyIndex and B-tree node/items/children storage when inserting new keys or splitting; freelist can reuse nodes. The measured key already exists after warmup, so no new-key allocation appears. A many-distinct-key test has a different census. |
| `mvcc/ops.go:250,252` | G | Version-history array grows (nine blocks across replicas); Change-array capacity is retained and reset per request (`start:26`). Version moves copy slice descriptors, not value bytes. History retains old values until compaction. |
| `core/state.go:92` | R | `string(c.Key())` allocates even for these lease-free changes in the measured binary: 768 blocks. Lease key maps may grow with leased keys; no such map-growth claim for canonical Put. |
| `mvcc/store.go:66–74`, `ops.go:261–262` | conditional R | Requested PrevKv constructs a KeyValue descriptor with aliased stored key/value. No previous-value object on canonical Put. IgnoreValue preserves the old value alias. Watches and generic Txn/DeleteRange/lease paths create additional objects and are outside this census. |
| `core/node.go:905` | B/G | Leader appends scalar wire Reply into a fresh Effects reply array. `p.req.Op!=nil` instead takes generic response branch at 899; `core.PutResponse` in `wire.go:104` constructs objects for other callers/simulators, **not** this socket dispatch. |
| `node/node.go:408,411–415` | B/G | Fresh Completion array per completer/turn transferred to `Complete`; outer grouping workspace cleared/reused. Waiters deleted on reply. One 80-byte completion slot per serial Put observed. |
| `server/batch.go:159,168–169` | B/G | Outer `[][]Completion` queue grows, then `takeCompletions` detaches it and sets queue nil. One 24-byte backing array/serial Put, not permanent connection-capacity reuse. |
| `server/batch.go:183–194` | B/G | Local `[]finished` grows per completion drain; resets on each call. One 48-byte slot/serial Put. Reply already encoded even if late/reset; not a guarantee of zero abandoned-call churn. |
| `server/batch.go:243–269` | R | One response byte buffer with gRPC prefix and max scalar-header capacity; stack `[44]byte` header scratch. No value in normal Put response and no generic response message construction. PrevKv marshaling can grow this buffer and adds old-value serialization. |
| `stream.go:275,278,298` | R/G | New stream output slice grows for HEADERS, DATA, trailers: observed three blocks/Put (48 + 80 + 160 bytes). Success headers/trailers are shared preencoded bytes, not encoded or allocated per Put. `takeStream:173–176` consumes/nils the queue; descriptor storage is not retained for another request. |
| `write.go:26,43,46,78,141–159` | C/G/B | Connection ready list and writer-local frame/control/header arrays grow conditionally and are reused. Frame descriptors alias queued payload bytes. No warmed flat growth on these lines. |
| `write.go:79–85` | B/V | Writes frame header/payload to persistent bufio, then Flush invokes TLS/socket. No generic gRPC transport scheduling or per-Put writer goroutine. Error status construction, reset/control frames (`endStream:195`), connection close, and timeout work are conditional extras. |

## 4. Channel-operation census (not an allocation count)

| Operation | Multiplicity / blocking | On canonical wire Put? |
|---|---|---|
| `conn.signal` sends to persistent `wake`; writer receives (`write.go:53–61`) | Coalesced nonblocking send, writer sleeps when no work; multiple producers can signal per Put | Yes; no channel creation per signal |
| `node.put` inbox send (`node.go:121–129`), commit receive/drain (`322–331`) | One send per connection batch; **may block** until inbox space, cancellation, or node stop | Yes; batching eliminates per-request handoff for larger batches, not serial blocking |
| `persistQ` send/receive (`340,465,472`) | One per effect-job slice; driver sends only through selectable case and can process other events | Yes |
| `persisted` send/receive (`486,333`) | One per synced/coalesced job group; worker may wait for acknowledgement capacity; separate channel prevents inbox competition | Yes |
| Tick channel (`344`); context/Done receive cases | Per tick/shutdown, not per request | Yes background |
| Peer `ctrl`/`data` send (`296–300`), sender receive (`308–320`), drop receive (`331–336`) | Per Raft message; nonblocking enqueue with overflow handling, blocking sender select while idle | Yes; persistent channels |
| `Deliver` inbox send (`253`) | Per inbound peer message; **may block** and backpressure peer reader | Yes, variable message count |
| `Unreachable` inbox send (`261–263`) | Nonblocking best effort | Conditional |
| Stream `space` send (`write.go:131–134`) | Nonblocking notification; node-run Put has nil `space`, so default wins | Not a real Put handoff |
| `Do` request/reply channel (`node.go:268,273,399`) | Allocates one reply channel/request and waits; buffered reply send on completion | **No**; direct/internal/generic callers only |
| Streaming input/watch queues and leader monitor | Per stream/event with own goroutines/contexts | **No** canonical Put |

Successful `Complete` is a direct interface call into the connection's short completion lock, **not a reply-channel send**. `takeCompletions` and the writer do the encoding. Mutex contention, inbox blocking, disk backpressure, and slow socket flush can still consume CPU/latency despite removal of per-request channels. Channels can also require runtime wait/select/goroutine workspace when actually blocking; that is distinct from allocating their buffered storage at startup.

## 5. Full-value copies versus allocations and aliases

PR77 removed generic Put decoding and a separate MVCC-owned value, but it did **not** make the entire TLS/HTTP2/Raft/WAL path a single memcpy. Count logical full-value copies separately from number of fragment appends/records and from allocating buffers.

| Stage | Copy or alias? | Fresh allocation necessarily? |
|---|---|---|
| TLS receives ciphertext, decrypts record | Ciphertext lands in record storage; AEAD decrypts in place | Conditional growth only |
| TLS `Conn.Read` into server bufio; Framer `ReadFrame` into its payload buffer | For the 1 KiB payload/64 KiB reader-buffer path, plaintext copies into bufio and then into Framer payload storage. Large reads may bypass bufio's own buffer | Cached buffers; transport/record partition dependent |
| `stream.receive:122–125` | **Copy #1 at application ingress:** Framer payload to owned request wire buffer | One/message |
| `ParsePut`; Request/batch/pending descriptors | **Alias** owned request wire, including key/value slices | No value copy; descriptor allocations listed separately |
| `encodeBatch:68` | **Copy #2:** request wire to batch entry bytes, once/batch encoding | One/batch |
| Leader Raft `appendEntry` → `proto.Clone` | **Copy #3:** batch bytes to owned cloned Entry.Data | One Data buffer/entry; confirmed clone-byte site |
| Unstable → PersistJob → MemoryStorage → local apply | **Alias** entry Data; some pointer-array/slice-descriptor copies | Metadata/array growth only |
| WAL `record:351` | **Copy #4 on leader local persistence branch:** entry Data to WAL staging | Reused WAL buffer, growth only |
| Each peer's protobuf marshal | Full entry Data copied into peer wire scratch | Reused sender scratch, growth only |
| Peer TLS/bufio, remote read scratch, protobuf Unmarshal | More transport copies; Unmarshal **copies** Data to follower-owned entry storage | Per-message protobuf/bytes allocations; transport buffers reused |
| Apply `walkBatch` → `ParsePut` → `version.value`/`keyIndex.key` | **Alias** the committed Entry.Data; **zero additional MVCC full-value copy** | Version/index metadata can grow |
| Normal response encode + frame/bufio/TLS flush | No 1 KiB value included, only revision/term/header; response payload still copied through transport | One small response buffer plus variable transport |

The numbered ingress/encode/clone/WAL copies are different branches/owners, not four separately retained logical MVCC values. After transient request/encode/WAL buffers become unreachable/reusable, the leader's current log entry and MVCC index share the cloned Data buffer. Followers share their own unmarshaled entry Data with their indexes. With historical versions, distinct **old** values are retained intentionally. `PrevKv`, watches, and range replies serialize value bytes again. A restarted WAL replay can retain a whole segment read buffer through Data subslices; that is a different retained-memory shape from this fresh-run ingress.

## 6. Reclamation hazards for fixed rings or pooled arenas

### A. Retained entry bytes are not request workspace

`applyPut:876–884`, `mvcc/ops.go:259,277`, and `mvcc/store.go:66–74` keep subslices of Entry.Data. Reclaiming an entry region merely because it was applied, durably persisted, or its client answered corrupts live MVCC state and historical revisions. Raft MemoryStorage/unstable, asynchronous PersistJobs, outgoing message queues, and snapshots/reads/watch/PrevKv aliases can also hold it. A log index advancing is not an ownership release for all those holders.

Treat transient message/encode buffers separately from **retained entry storage**. This Raft version deep clones proposals on leader append; replacing/avoiding that clone changes its ownership contract and requires proving no caller mutation. A region can be reused only after its last reader/reference ends; clipping slice capacity is not an immutability or recycling mechanism. One live key/value subslice can pin a whole batch backing allocation: byte retention must include this batch-granularity effect, history, and first-key ownership. Whole-batch allocation simplifies ownership but does not automatically solve lifetime or compaction.

### B. Pending completion and timeout ownership are different lifetimes

`settle:539–549` marks `p.done` and deletes `waiting`; it does **not** remove `p` from `timeouts`. `expire:518–528` later skips done descriptors and removes a prefix. Before proposal, `proposals` also references p; `propose` skips done entries only when it runs. Core timeout does not cancel a proposal already admitted to Raft, so a timed-out write may still commit; late apply sees nil/done waiter and suppresses duplicate response.

Returning p to a pool immediately on settle can make an old timeout/proposal slot refer to a newly reused request: resetting `done`, ID, or deadline permits wrong expiry, missing completion, or accidental proposal of the wrong operation. Stream tags/waiter IDs and any arena offsets have the same stale-reference risk. Recycle only after all list/map holders retire, or use explicit generations/indices with checked state transitions. Remove/clear stale pointer slots before resetting objects. Prefix slicing alone does not clear the backing array; the GC can retain stale pending/request bytes while that backing array remains reachable. `pending.req.Put` still references original wire bytes after encoding; lifetime/retained-byte work must account for or sever those aliases, not just delete the waiting map entry.

### C. Stream ending is not socket-consumption completion

`takeStream` transfers `outItem.b` into `wframe.b` slices. `endStream:182–193` deletes the stream, stops its timer, and releases held-byte credit **before** `writeLoop` writes those frames/flushes. An empty queue, deleted stream, released flow-control budget, detached completion queue, or successful `SubmitBatch` therefore does not establish that all payload owners are finished.

Never overwrite/pool response bytes when `endStream` runs: pending `frames` still alias them. Preserve ownership through the writer's last read. A conservative recycle boundary is successful flush/abort cleanup; a tighter boundary needs explicit confirmation that synchronous bufio `Write` copied/consumed every fragment (and that no frame alias remains). Flush failure is not successful delivery; abort the connection and retire all outstanding owners. Do not count bytes both as free ring capacity and as still-referenced writer payloads.

`resetStream:338–350` keeps node-owned request charge until its late completion is dropped; `complete:200–207` recognizes reset/already-ended streams. Pooling a stream when the map entry disappears can invalidate still-owned completion tags, ready-list/frame refs, or callbacks. `Timer.Stop` does not wait for a callback already running. Late completions and old timer callbacks need generation/lifetime validation before a stream slot is reusable.

### D. Batch handoff and progress capacity

- `flush:103–112` transfers req/tag arrays to node batch; they cannot be immediately overwritten by the reader. The driver copies Request descriptors to pending state and stream tags to waiters; byte aliases outlive the outer batch.
- `Completer` explicitly takes ownership of its Completion slice (`node.go:138–143`). Clearing reusable grouping workspace is safe only because it does not clear the transferred inner array. Connection detachment at `takeCompletions` is another handoff, not destruction.
- Disk/peer queues hold entry refs beyond the turn that emits Effects. Per-turn scratch can be reset only after consumers cease referencing it, not at the next `Process`.
- Keep control/progress capacity independent of saturated client-data arenas. Current reader continues handling pings/resets/window updates under held-byte pressure; node can keep ticking and receive persisted notifications with a full backlog; peer control/data queues have separate budgets. A single exhausted ring must not prevent progress required to free itself.
- Limits differ: per-connection batching is 256 ops / 4 MiB, held-byte throttling is 64 MiB with a 512-byte call charge, writer take budget is 1 MiB, and raw message/decoded request/core-batch/peer-frame limits serve different purposes. `maxHeld` is a flow-control threshold, not an instantaneous global hard arena cap. The largest accepted request plus framing must fit a progress-safe allocation path; replacing growth with fixed capacity must not silently reduce acceptance.

## 7. Implications for the throwaway allocation experiment

1. Start comparison with the **16/Put** stable explicit request/completion-workspace subset and **6/Put across replicas** lookup/key-conversion subset; preserve batching classification rather than promising 16 savings at all batch sizes. History/log bytes are not disposable scratch.
2. Separate app workspace savings from Raft's storage-message/scalar churn, peer protobuf decoding, connection/TLS pool changes, and tick/election noise. Profile exact flat sites plus global MemStats; a leader-only claim needs different instrumentation.
3. Compare startup-fixed byte-budget rings versus pooled per-batch arenas using allocations/Put, allocated bytes, GC CPU, CPU/Put, live/retained bytes, maximum accepted values, batching/load shape, and slow-client/peer/disk behavior. This source/profile audit supplies no GC/CPU or slow-client measurements.
4. Preserve PR77 wire parsing, one batch encoding, and entry-backed apply; avoid introducing per-request generic PutResponse objects or independent MVCC value copies. Existing handwritten-encoding tests and simulator helper responses are not by themselves evidence of actual socket reply behavior; the supplied raw TLS/HTTP2 profile does exercise that path.

This report is analysis only. It neither changes production ownership contracts nor establishes that either experimental reuse strategy is correct or meets a numeric acceptance target.
