# PR77 source audit — client wire → batch → core → Raft → WAL → apply/index → reply
## Final target binding and method

This report audits only the requester-confirmed **rerun** targets, resolved **2026-10-13T08:56:32Z**, [Actions run 38372738426](https://github.com/djosh34/tigerstore/actions/runs/38372738426). Later branch heads are not substituted. The separate profile run is reported by the requester to use this same PR77 commit; no profile-hook code is charged to Put.

| Label | Branch / role | Exact commit | Archived directory |
| --- | --- | --- | --- |
| M | main | `e1fd889c345bc3c06d8100eb6d635c88ad6967cd` | `sources/run38372738426/main/` |
| B | work/50-batch / PR76 | `29c3f8f95e3c9bcecf5bb39aa1f3bca49fe99c1f` | `sources/run38372738426/batch/` |
| W | work/50-wire / PR77 | `f2edb7b2b1e81f65d7ca7592638654f09217b933` | `sources/run38372738426/wire/` |

All work products are under `/home/joshazimullah.linux/work_mounts/scratch/bench77-source-audit`. The checkout at `/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75` was read only. Sources were extracted with `git archive <exact SHA> | tar -x -C <absolute scratch directory>`. `gh` was used only to fetch issue/PR/run/commit metadata. No checkout edits, comments, branch changes, subagents, Go executions, tests, or benchmarks were made.

The starting evidence was the checkout's `evidence/bench75/source-audit/audit.md` and `verified-sites.md`. Their definitions informed this census, **not their old counts or line numbers**. The older PR76 `room` signal/backpressure loop is gone at these final targets. The completion path now uses a CAS list rather than the earlier completion mutex. `verified-sites.md/json`, `site-specs.json`, `source-file-hashes.json`, and `archive-file-manifest.json` bind the checked text and files to the final git commits. `verify-sites.py` independently asserts each expected line and compares every cited repository file to `git show <SHA>:<path>`. `dependency-proof.md` and ZIP/hash evidence independently establish dependency behavior.

Earlier first-run targets (PR76 `751f69c76560d7e68fc76e9c91760020bfa21f72`, PR77 `fcd54b1ba2ddf120be3d83f5070cf13592a9f6d8`, same main; resolved 08:48:45Z, run 38371820704) are retained only as provenance in `earlier-run38371820704/` and `metadata/first-to-rerun-*.diff`. They are **not** the targets of the final tables. `observed/` is preliminary extraction provenance, also excluded.

### Scope and counting rules

A successful ordinary unary Put with a **1,024-byte nonempty value**, valid nonempty key, one canonical key/value field, no lease (including the old key version), no watch, no PrevKv, no IgnoreValue/IgnoreLease, no planted bug, no reset/deadline/cancellation/error, submitted to a stable leader over an already established client HTTP/2 connection. “Leader-only” means clients target the leader, **not no replication**. Insertion and overwrite have the same enumerated value stages; metadata/history retention may differ. Other RPCs, startup, recovery, shutdown, metrics and observation-only loopback runtime/pprof handlers are excluded. The measurement hook is not in these archives and was not audited as part of Put.

- A successful send and a selected receive are counted separately. Default branches, failed nonblocking attempts, unselected select arms, `len(ch)`, obtaining `Done()` and nil-channel attempts are not operations. Channel creation and `close` are separate.
- A full-value copy means all 1,024 value bytes are materialized in a different byte backing store. Encoding a bytes field, owning decode, copying a containing entry, and WAL staging qualify. Fragmented chunks into one destination count as one full-payload equivalent. Copying pointers, interfaces, descriptor structs, slice headers, keys or reply metadata does not copy this payload. Sizing/checksum scans are not copies.
- The census counts named materialization boundaries, including their actual pinned dependency implementations. A call and its dependency instruction are **one stage**, not two. It does not enumerate TLS/bufio/socket/kernel/page-cache movement, HTTP/gRPC transport buffering, runtime growth relocation, or all library lifecycle channels. The channel totals below are explicitly **repository-level**; transport/library channels are unknown here, not assumed zero. No precise physical memcpy, allocations/op, live-byte, CPU/op or performance total follows from these subtotals.

## Concise table — per successful ordinary 1 KiB Put

M = main, B = PR76 batch, W = PR77 wire, **exact rerun commits below**. Send and receive are separate operations; closes are separate. N is the number of successful Puts in a fully accounted cohort. A is actual admission batches; Q is persist-queue arrays; F is sync completion groups; O/I are queued-and-consumed outbound / delivered-and-consumed inbound peer messages on the leader; T is selected ticker receives; Sw/Rw are successful writer-wake sends / selected wake receives. These populations differ. No batch sizes or message counts were measured or inferred.

| Source-defined cost per Put | M | B | W |
|---|---:|---:|---:|
| Fixed non-amortized request/reply channel operations | **2 sends + 2 receives** | **0** | **0** |
| Admission channel send / receive | Included above | **A/N sends + A/N receives** | Same as B |
| Completion reply-channel send / receive | Included above | **0**, direct grouped call + atomic list | Same as B |
| Custom frontend writer-wake send / receive | Library transport excluded | **Sw/N sends + Rw/N receives**, conditional/coalesced | Same as B |
| `room` channel operations | — | **0: absent at final target** | **0: absent at final target** |
| Disk queue + sync completion send / receive | **(Q+F)/N sends + (Q+F)/N receives** | Same | Same |
| Leader peer queue + inbox send / receive | **(O+I)/N sends + (O+I)/N receives** | Same | Same |
| Background tick receive | **T/N receives** | Same | Same |
| Explicit repository channel closes on normal Put | **0** | **0** | **0** |
| Fresh explicit per-Put reply channels | **1** | **0** | **0** |
| Named direct leader value materializations, excluding peers/growth/transport | **4** | **5** | **3** |
| Additional verified Raft Entry.Data deep clone on leader | **1** | **1** | **1** |
| **Known leader payload-copy stage subtotal** | **5** | **6** | **4** |
| Known follower stages for one received, newly persisted, applied occurrence | **3** | **3** | **2** |
| Sender + that one follower (peer encode + follower stages) | **4** | **4** | **3** |
| Unenumerated transport/runtime copies and library lifecycle channels | **Excluded, not zero** | **Excluded, not zero** | **Excluded, not zero** |

**Not total physical memcpy counts or total channels/op.** The main and custom frontends have different excluded transport internals, so 5 versus 4 is not an end-to-end physical-copy comparison. The same-frontend B→W comparison removes **two known full-value materializations on the leader and one at apply on each follower**. Payload bytes do not amortize as 1/batch-size: an entry copy contains every Put's full 1,024 bytes.

## Repository channel ledger, sends / receives / closes

For a matched cohort where all counted handoffs are eventually consumed:

```
M sends:    2N + Q + F + O + I
M receives: 2N + Q + F + O + I + T
B/W sends:    A + Q + F + O + I + Sw
B/W receives: A + Q + F + O + I + T + Rw
Normal Put explicit repository closes: 0, in every target
```

These are accounting identities, not measured attribution. A time window can begin/end with queued work, so send/receive counters must be counted separately rather than presumed matched in-window. A, Q, F, core entry batches, and completion groups G are not interchangeable. `LocalAppendThread`/`LocalApplyThread` are synchronous core interpretation of Raft messages, not Go channel operations.

### Admission and completion

| Executed site / purpose | M | B | W | Accounting |
| --- | --- | --- | --- | --- |
| Inbox admission send | `internal/node/node.go:122` | `internal/node/node.go:136` | `internal/node/node.go:136` | M: one send/Put; B/W: one send/admission batch |
| Inbox receive, outer alternative | `internal/node/node.go:264` | `internal/node/node.go:333` | `internal/node/node.go:335` | one selected receive per input, admission or inbound peer |
| Inbox receive, drain alternative | `internal/node/node.go:269` | `internal/node/node.go:338` | `internal/node/node.go:340` | alternative receipt; do NOT add to outer receipt for same input |
| Do reply send | `internal/node/node.go:301` | `internal/node/node.go:418` | `internal/node/node.go:420` | M: one send/Put; B/W retained Do, NOT executed by ordinary Put |
| Do reply receive | `internal/node/node.go:217` | `internal/node/node.go:284` | `internal/node/node.go:286` | M: one receive/Put; B/W retained Do, NOT executed by ordinary Put |
| Explicit reply channel creation | `internal/node/node.go:212` | `internal/node/node.go:279` | `internal/node/node.go:281` | M: one fresh channel/Put, not operation; B/W normal Put: zero |
| Grouped completion callback | — | `internal/node/node.go:430` | `internal/node/node.go:432` | B/W direct Complete call: zero channel ops |

B/W connection reader admission is `internal/server/h2.go:522` / `internal/server/h2.go:522`, batch API calls are `internal/server/batch.go:109` / `internal/server/batch.go:125`, and core submission is `internal/node/node.go:444` / `internal/node/node.go:447`. Request descriptors and slice headers are handed over; no payload copy there. Node.Do survives but is not the batch Put path.

The final completion publisher is `internal/server/batch.go:170` / `internal/server/batch.go:186`; the writer atomically swaps the list at `internal/server/batch.go:180` / `internal/server/batch.go:196`. No completion-channel handoff or completion mutex remains. This does **not** prove the whole hot path cannot block: inbox sends can wait, connection byte/output bookkeeping uses its ordinary mutex, and CAS may retry. Allocating a completion-list node is not a measured allocations/op result.

### Custom frontend wake and flow control

| Executed site / purpose | M | B | W | Accounting |
| --- | --- | --- | --- | --- |
| Successful writer wake send | — | `internal/server/h2.go:190` | `internal/server/h2.go:190` | Sw, conditional/coalesced; capacity-one channel |
| Selected idle-writer receive | — | `internal/server/write.go:54` | `internal/server/write.go:54` | Rw, only while writer finds no ready frames/control work |
| Credit calls signal | — | `internal/server/h2.go:369` | `internal/server/h2.go:369` | attempt per DATA frame, not a separate channel site |
| Complete calls signal | — | `internal/server/batch.go:174` | `internal/server/batch.go:190` | attempt per completion group, not a separate channel site |
| Unary end attempts streaming input signal | — | `internal/server/h2.go:514` | `internal/server/h2.go:514` | nil unary channel: zero successful sends |
| Writer attempts streaming-space signal | — | `internal/server/write.go:132` | `internal/server/write.go:132` | nil unary channel: zero successful sends |

**Important correction to the prior census:** these final B/W implementations call `signal` on **every DATA-frame credit**, not just at the 4 MiB receive-credit threshold. They also signal each completion group. Actual successful Sw can therefore include request-data wakes as well as completion wakes; do not bound it by G alone or assign a fixed two wake operations per Put/group. No fabricated DATA frame count is assumed for a 1 KiB value; framing/key/message bytes and client fragmentation matter.

For a connection interval, `Rw = Sw + initial_token_occupancy - final_token_occupancy`. A running writer can collect replies without a fresh receive, and a token can remain buffered after a reply is written. The caller's signal attempt can hit default. Byte-charge backpressure now defers connection window credit, with no `room` channel. Request-charge release at `internal/server/batch.go:209` / `internal/server/batch.go:229`, reply charge at `internal/server/batch.go:215` / `internal/server/batch.go:235`, and reply-charge release at `internal/server/write.go:193` / `internal/server/write.go:193` are arithmetic/flow-control bookkeeping, not additional channel exchanges or value copies.

Other shared wake causes (not fixed per Put):

| Executed site / purpose | M | B | W | Accounting |
| --- | --- | --- | --- | --- |
| GOAWAY | — | `internal/server/h2.go:179` | `internal/server/h2.go:179` | lifecycle, excluded from normal Put fixed count |
| Queued control frame / ping ACK | — | `internal/server/h2.go:203` | `internal/server/h2.go:203` | conditional control traffic |
| Settings ACK / change | — | `internal/server/h2.go:316` | `internal/server/h2.go:316` | setup or later control traffic |
| Send-window update | — | `internal/server/h2.go:331` | `internal/server/h2.go:331` | conditional receive-side control traffic |
| Reset | — | `internal/server/h2.go:359` | `internal/server/h2.go:359` | exceptional for scoped request |

Normal B/W Put has no per-stream context at `internal/server/h2.go:445` / `internal/server/h2.go:445`. An optional grpc-timeout uses `time.AfterFunc` at `internal/server/h2.go:457` / `internal/server/h2.go:457`: a callback timer, not a per-Put Timer.C receive. A deadline firing is outside this success scope. Streaming receive/send/watch signals do not execute for this Put.

### Disk and tick

| Executed site / purpose | M | B | W | Accounting |
| --- | --- | --- | --- | --- |
| Persist array → worker send | `internal/node/node.go:282` | `internal/node/node.go:351` | `internal/node/node.go:353` | Q sends |
| Disk receives initial array | `internal/node/node.go:369` | `internal/node/node.go:483` | `internal/node/node.go:486` | one of Q total receives |
| Disk receives another drained array | `internal/node/node.go:376` | `internal/node/node.go:490` | `internal/node/node.go:493` | another array, not a second receipt of initial array |
| Worker → committer synced group send | `internal/node/node.go:390` | `internal/node/node.go:504` | `internal/node/node.go:507` | F sends |
| Committer synced group receive | `internal/node/node.go:275` | `internal/node/node.go:344` | `internal/node/node.go:346` | F receives |
| Selected ticker receive | `internal/node/node.go:286` | `internal/node/node.go:355` | `internal/node/node.go:357` | T background receives; timer internals excluded |

The worker can merge multiple queue arrays into one sync group. Hard-state-only/commit-only jobs can still pay channel operations without copying this value. It is not defensible to assume Q=F=number of entries or one sync per Put. A hypothetical Q=F=1 for B writes gives 4/B disk ops, but that is **not an observed or general batch size/count**.

### Peer handoffs and exceptional exclusions

| Executed site / purpose | M | B | W | Accounting |
| --- | --- | --- | --- | --- |
| Outbound peer message queue send | `internal/peer/peer.go:294` | `internal/peer/peer.go:294` | `internal/peer/peer.go:297` | O sends on leader, data/control |
| Peer writer receive fast control alternative | `internal/peer/peer.go:305` | `internal/peer/peer.go:305` | `internal/peer/peer.go:308` | one of O total receives |
| Peer writer receive control alternative | `internal/peer/peer.go:311` | `internal/peer/peer.go:311` | `internal/peer/peer.go:314` | one of O total receives |
| Peer writer receive data alternative | `internal/peer/peer.go:314` | `internal/peer/peer.go:314` | `internal/peer/peer.go:317` | one of O total receives |
| Inbound peer delivery → inbox send | `internal/node/node.go:197` | `internal/node/node.go:264` | `internal/node/node.go:266` | I sends; consumed by one of inbox receive alternatives above |
| Unreachable report inbox send | `internal/node/node.go:205` | `internal/node/node.go:272` | `internal/node/node.go:274` | fault/drop-only successful nonblocking sends, excluded healthy ledger |
| Failed-connection drain control/data | `internal/peer/peer.go:328` | `internal/peer/peer.go:328` | `internal/peer/peer.go:331` | fault-only receives; data alternative in verified-sites |
| Peer dial backoff timer receive | `internal/peer/peer.go:347` | `internal/peer/peer.go:347` | `internal/peer/peer.go:350` | fault-only selected receives |
| Node done close | `internal/node/node.go:239` | `internal/node/node.go:306` | `internal/node/node.go:308` | shutdown-only close |
| Watch overflow close | `internal/node/node.go:320` | `internal/node/node.go:392` | `internal/node/node.go:394` | outside no-watch scope |
| Server Stop wait-channel close | — | `internal/server/server.go:189` | `internal/server/server.go:189` | outside normal Put lifecycle |

The separate failure-only data-drain sites are `internal/peer/peer.go:330`, `internal/peer/peer.go:330`, `internal/peer/peer.go:333`. Cancellation receives, including `internal/server/write.go:56` / `internal/server/write.go:56` and peer context arms in verified-sites, do not execute in the scoped healthy request. Closing a socket or HTTP/2 stream is not closing a Go channel. Main HTTP/gRPC/context library lifecycle closes are not enumerated; the explicit repository normal-Put close count is **not** a claim of zero closes in all dependencies.

A fully consumed directed peer message pays **2 repository channel operations per endpoint**: sender queue send+receive, receiver inbox send+receive; **4 across both endpoints**. Append messages may carry many entries, each containing many Puts. Acks, empty appends, commit propagation and heartbeats still pay handoffs but copy no 1 KiB value. Client success needs quorum progress, not all followers. Fanout, Raft progress/probing, retransmission, queue drops and work after client reply make messages/Put conditional.

## Full-payload copy ledger

Repository call sites plus the dependency proof below identify each counted stage. A cited codec call and its module copy implementation are the same materialization, not additive copies.

| Executed site / purpose | M | B | W | Accounting |
| --- | --- | --- | --- | --- |
| Framer DATA → owned request-message accumulation | — | `internal/server/stream.go:127` | `internal/server/stream.go:127` | M: transport internals unenumerated; B/W: 1 full-value equivalent |
| Wire → fresh frontend Put.Value | — | `internal/server/rpc.go:62` | `internal/server/rpc.go:63` | M: generated/grpc codec proof below; B: 1; W ordinary Put bypasses: 0 |
| Put request → encoded Raft-entry batch | `internal/core/codec.go:64` | `internal/core/codec.go:64` | `internal/core/codec.go:68` | 1 per Put, all targets (protobuf encode M/B; raw Wire append W) |
| Leader proposal boundary → Raft deep clone | `internal/core/node.go:522` | `internal/core/node.go:522` | `internal/core/node.go:571` | 1 per Put all targets, actual module clone proof below |
| Entry.Data → WAL staging buffer | `internal/wal/wal.go:351` | `internal/wal/wal.go:351` | `internal/wal/wal.go:351` | 1 direct full-value append all targets; WAL caller below |
| Apply generic decode → fresh Put.Value | `internal/core/codec.go:114` | `internal/core/codec.go:114` | — | 1 main/batch; absent from wire Put apply: 0 |
| MVCC version retains Value slice header | `internal/mvcc/ops.go:235` | `internal/mvcc/ops.go:235` | `internal/mvcc/ops.go:259` | 0 payload copies; W aliases entry-backed bytes |
| Version metadata append | `internal/mvcc/ops.go:227` | `internal/mvcc/ops.go:227` | `internal/mvcc/ops.go:250` | 0 payload copies |
| Peer sender Message/Entry.Data encode | `internal/peer/peer.go:416` | `internal/peer/peer.go:416` | `internal/peer/peer.go:419` | 1 per transmitted occurrence of this payload, per recipient |
| Peer receiver Message/Entry.Data decode | `internal/peer/peer.go:238` | `internal/peer/peer.go:238` | `internal/peer/peer.go:241` | 1 per received occurrence, including duplicates |

Main's frontend is **not** a zero-copy exception: `internal/server/http.go:29` routes into grpc, the handler dispatch is `internal/server/server.go:160`, the generated decoder is `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc_grpc.pb.go:248`, grpc invokes protobuf at `dependency-sources/grpc-v1.84.0/encoding/proto/proto.go:96`, and the proto3 bytes decoder copies the whole value at `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5497`. B's frontend uses `internal/server/batch.go:52` → its unmarshal site above → the same proto3 copy instruction. For M/B apply, `internal/core/codec.go:96` / `internal/core/codec.go:96` creates fresh PutRequest before the apply decoder. W removes both owning decodes on the ordinary Put path.

The Raft clone is **still present**: M/B/W proposals enter `internal/core/node.go:522`, `internal/core/node.go:522`, `internal/core/node.go:571`; RawNode initially aliases the buffer at `dependency-sources/raft-v3.7.0/rawnode.go:94`, but leader appendEntry clones the entry at `dependency-sources/raft-v3.7.0/raft.go:817`. Entry.Data is **proto2**, so the actual clone copy is `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188`, NOT the Put proto3 decode site at codec_gen.go:5497. See dependency-proof.md for descriptors, coder selection and call chain.

WAL append is `internal/wal/wal.go:368`, `internal/wal/wal.go:368`, `internal/wal/wal.go:368` → the one copying record site above. This remains a separate staging copy even though the index aliases the Raft entry. File Write at `internal/wal/wal.go:386` and Sync at `internal/wal/wal.go:416` are durability I/O, not additional enumerated userspace backing-store materializations. CRC scanning is not copying. WAL buffer capacity may be reused; a copy stage is not necessarily a new allocation.

### Verifying PR77's actual stage removal

1. Reader accumulation still copies into owned request storage; capacity comes from the full request size at `internal/server/stream.go:124`. Fragmentation appends each next chunk into the same destination: one equivalent copy of this value, not one per DATA frame. The request includes protobuf/key metadata; it is not an exactly 1 KiB buffer.
2. Admission uses ParsePut at `internal/server/batch.go:59`. Its bytes-field parsing calls ConsumeBytes at `internal/core/wire.go:61`; the module returns a subslice at `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:468`. Capacity clipping at `internal/core/wire.go:67` only changes the slice header, and `internal/core/wire.go:71` retains it. Validation at `internal/core/node.go:372` does not rebuild a protobuf PutRequest or copy the value.
3. Batch descriptors reach core submission without a payload copy. Queuing charges len(Wire)+framing at `internal/core/node.go:501`; Process proposes through `internal/core/node.go:566` and encodes client wire once at the ledger site. The preallocation at `internal/core/codec.go:37` reserves `size+MaxVarintLen64`. Each request charged its full wire length plus up to four 10-byte varints and one kind byte; the extra 10 bytes cover the count. Pruning settled proposals can only over-reserve. Thus scoped wire batch encoding has no append-growth relocation; it still performs the one explicit payload append. M/B start at `internal/core/codec.go:35` / `internal/core/codec.go:35` and can grow.
4. Raft then deep-clones the encoded entry. Async local append/apply messages retain references: `dependency-sources/raft-v3.7.0/rawnode.go:228` / `dependency-sources/raft-v3.7.0/rawnode.go:393`. Driver sends persist jobs; WAL stages bytes and syncs; Persisted updates storage, which appends entry pointers at `internal/core/storage.go:39` → `dependency-sources/raft-v3.7.0/storage.go:318` or `dependency-sources/raft-v3.7.0/storage.go:320`, not another Data copy.
5. **Every replica's Put apply actually bypasses generic unmarshal:** `internal/core/node.go:839` walks entry bytes; `internal/core/codec.go:103` gives a capped entry subslice; `internal/core/node.go:844` chooses applyPut before `internal/core/node.go:851`. applyPut parses in place at `internal/core/node.go:880`, forwards fields to State at `internal/core/node.go:884` and MVCC at `internal/core/state.go:110`. MVCC retains value/key slice headers at the ledger site and `internal/mvcc/ops.go:277`. The retained value therefore aliases the **Raft-owned entry backing store**, not the earlier frontend buffer or WAL staging buffer. The successful no-watch path exits matching at `internal/core/watch.go:272` without event value encoding.
6. The normal wire reply is metadata at `internal/core/node.go:905`. Writer method dispatch chooses `internal/server/batch.go:220`, buffer prefix/encode at `internal/server/batch.go:269` / `internal/server/batch.go:270`. No ordinary PutResponse protobuf message is built on this wire path. M/B encode a small response with no value; B at `internal/server/batch.go:202`. **All normal replies copy zero of this 1 KiB input payload.** PrevKv/watch/ignore paths can behave differently and are excluded.

`DecodeBatch` still contains generic decoding at `internal/core/codec.go:145`, but is a helper used by checks, not this production Put apply path. Txn/DeleteRange/lease writes retain generic apply decode at the other site above. A retained helper is not evidence that the removed Put stage still executes.

### What “stored once / applied in place” does and does not establish

The scoped PR77 requirement **“Every replica applies puts by walking the entry in place … The index keeps the key and value as slices of the entry”** is implemented by the traced branch, not just asserted in a comment. It removes the decoded MVCC payload backing store. The stronger issue50 addendum says **“The index holds a reference (segment, offset, length) into the log”** and **“without a generic protobuf decode per command”**. Put now fulfills the in-place walking part and retains in-memory log-entry slices; it does **not** implement a WAL-segment offset/addressing layer or generic in-place apply for every command type. It is intentionally scoped to Put.

“Stored once” is defensible for the relationship between the in-memory applied entry and its index values, **not** for the count of physical byte copies or all concurrently live buffers: reader accumulation, encoded proposal, Raft clone, WAL staging, peer buffers, and disk representation still exist. A small retained value can keep an entire entry/batch backing array reachable; live bytes/GC effects need measurement. Header-only batch/list objects and existing mutexes are not proof of zero churn or blocking in the whole pipeline.

### Peer payload stages and no follower clone

For one occurrence transmitted by the leader, newly received/persisted/applied by a follower:

| Stage | M/B | W | Proof |
|---|---:|---:|---|
| Leader peer encoding of Entry.Data | 1 | 1 | peer-value-encode sites in ledger; bytes encoder in dependency proof |
| Follower owning decode of Entry.Data | 1 | 1 | peer-value-decode sites; **proto2** codec_gen.go:5418 |
| Follower WAL staging of Entry.Data | 1 | 1 | same WAL sites, if newly appended |
| Follower apply owning decode of Put.Value | 1 | 0 | main/batch apply-decode; wire ParsePut aliases entry |
| Follower index assignment | 0 | 0 | index-value/header assignment |
| **Sender + that follower subtotal** | **4** | **3** | named stages only, not all transport copies |

Followers process MsgApp via `dependency-sources/raft-v3.7.0/raft.go:1800` → `dependency-sources/raft-v3.7.0/log.go:127` → `dependency-sources/raft-v3.7.0/log_unstable.go:205`, retaining entry pointers rather than taking the leader proposal appendEntry clone. Raft outbound append Message construction aliases entries at `dependency-sources/raft-v3.7.0/raft.go:656`. No follower re-encode through encodeBatch occurs for received appends. The receiver's decoded Entry.Data remains owned and safe despite transport-buffer reuse. These statements apply to all three commits' identically pinned Raft dependency.

Peer multiplicity is not fixed per Put. Each transmitted occurrence/recipient contributes a sender materialization; each received occurrence contributes a receiver materialization; only newly persisted/applied occurrences pay those stages. A duplicate can decode without re-persisting/re-applying. Ack/heartbeat/empty append messages copy no payload value. No fixed two-followers count or assumption that all follower work precedes the client reply is made.

### Conditional copies not promoted to fixed totals

- M/B growing entry encoding can relocate earlier payloads. W's full-batch allocation removes that particular growth mechanism for the scoped path; no fixed number of growth copies is invented for the old path.
- WAL append and reused peer MarshalAppend buffers can grow depending on history, capacity and ordering. Those extra full/partial relocations are conditional, beyond the named direct append/codec stage.
- Main grpc's multi-buffer materialization at `dependency-sources/grpc-v1.84.0/mem/buffer_slice.go:115` executes byte copies at `dependency-sources/grpc-v1.84.0/mem/buffer_slice.go:82`; a single constituent buffer is aliased instead. It is a conditional additional full-request materialization, not an always-copying field decode and not in the fixed payload subtotal.
- Framer/body input buffering, TLS, bufio, sockets, kernel/page cache and outgoing response transport are not enumerated. Excluded is not zero. Compression and noncanonical repeated-field requests are outside the scoped ordinary Put; they can change copying or stored-wire size.

## Conclusions and measurement limits

- **Channels:** M still pays two fixed request-specific exchanges (four repository operations). B and W have the same batch admission and direct atomic-list completion design, with conditional DATA/completion/control wakes. No total channels/op or observed batch size is established.
- **Copies/stage removal:** B→W removes the frontend owning Put.Value decode and the apply owning decode on the leader; each follower loses the latter. Known named leader stages are M=5, B=6, W=4 **including** the Raft clone and WAL staging, excluding peers/growth/transport. Known follower stages for the bounded single-delivery case drop 3→2. PR77's body omits the retained Raft clone from its informal copy list; the clone cannot defensibly be called removed.
- **Spec:** scoped Put in-place apply and entry-backed index values are real; a general WAL-segment reference implementation, all-command no-decode path, all-path no blocking/churn and zero physical copying are not established.
- **Failure/performance:** requester reports the first run's 64-client repetition logged ErrDiskStalled and metrics refusal, with a fresh rerun dispatched. This source census neither attributes that failure to physical VM versus code nor infers throughput/CPU/GC improvement. Observation-only pprof and metrics requests are excluded rather than charged to Put. No local benchmark or heavy test was run.

