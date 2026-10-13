# Paired PR79/80 source audit — Put and revision-CAS Txn

## Binding and method

**FINAL measurement binding — requester-confirmed; branch heads are NOT substituted.**

| Label | Role | Exact audited commit | Source directory |
|---|---|---|---|
| M | main | `8efd83a173394dd94a3981c72b4c51a1f3d061f1` | `sources/main/` |
| W | wire | `936bc5b69ae8a0c59585521734b41222006a0a2c` | `sources/wire/` |
| H | hpack | `129e49a842b4802e9a36b74c830b3538525f846f` | `sources/hpack/` |
| T | txn | `6179f12c2bbaaffc9960782533ad83c292605962` | `sources/txn/` |

Requester-confirmed resolver binding: **2026-10-13T11:04:41Z**, [Actions run 38387706444](https://github.com/djosh34/tigerstore/actions/runs/38387706444). Only these four exact commits are audited in the final report. PR77 uses retained `refs/pull/77/head` after its branch deletion; main includes merged PR75, PR76 and PR77. Main and PR77 have byte-identical production sources (indeed identical complete Git tree IDs, independently checked in `tree-control.json`): they are a **same-code control**, not a claimed source optimization comparison. Later branch heads are not substituted.

Abandoned run **38387063293** used an empty Put CANDIDATE after PR77 branch deletion and is **not** measurement binding, including any CAS result from it. Older pre-merge preliminary files retained under `earlier-preliminary/` are provenance only, explicitly excluded from final counts/sites.


**Work products only:** `/home/joshazimullah.linux/work_mounts/scratch/bench7980-source-audit`. Repository `/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75` was read-only. No subagents, Go executions, builds, tests, benchmarks, checkout edits, or GitHub writes/posts. The deadline is `2026-10-13T22:00:00Z`; verification scripts refuse to start after it.

Starting evidence: `evidence/bench75/run-38372738426/source-audit/audit.md`, its exact verified sites and dependency source manifest. Previous text was located anew by source/diff context, then exact line text asserted. **Old counts and line numbers were not inherited.** Every extracted repository file is compared byte-for-byte to `git archive <SHA>`; `source-file-hashes.json` records SHA256 and commit. `verified-sites.md/json` and `site-specs.json` bind every cited line to that SHA and file hash. Dependencies are separately proved by recomputed module ZIP h1, target go.sum and ZIP/cache/evidence byte equality (`dependency-integrity.json`, `dependency-source-manifest.json`, `dependency-proof.md`). Scripts are source-only Python, not Go.

The old report's main was pre-#75/#76. **This main has #75's custom frontend, #76's whole-batch path and merged #77's wire Put path**, so the old grpc-go frontend, 2 fixed request sends + 2 fixed receives, and one fresh reply channel/Put do not describe it. M/W/H/T below mean main, PR77 wire, PR79 hpack, PR80 txn at the binding above.

### Scope and counting

Established client HTTP/2 connection, stable leader client endpoint, 3-node replicated cluster, canonical unary requests, no compression, lease (including previous version), watches, PrevKv, IgnoreValue/IgnoreLease, planted bugs, reset, deadline firing, transport/disk failure, restart or recovery. Ordinary Put carries one nonempty **1,024-byte value**. CAS uses an existing key, **EQUAL MOD comparison with cached positive ModRevision**, Success = one Put of a new 1,024-byte value to that key; Failure = one current-revision point Range of the same key, returning its value (not KeysOnly/CountOnly). Positive cached revision may be stale; that is a compare failure, not an RPC error. Requester-confirmed generator: 4,096 existing keys, original global multiplicative permutation, 256 outstanding calls; before each warmup/measured subprocess an **untimed keys-only prefix Range** seeds positive cached mod revisions. Each call compares its cached revision, executes Put or failure Range, validates returned 1 KiB value plus ModRevision on failure, and monotonically refreshes a shared atomic cache. **No per-operation client pre-read, no retries.** Do not turn the expectation of mostly compare-pass into a source assumption; actual reported pass/fail counts determine weighting. No seed/cache-population Range is charged to this Txn. Client construction/encoding, validation/decoding of the client response and seed work are not server stages of this call.

- A successful send and a selected receive are separate channel operations; close is separate. Failed nonblocking sends, default/unselected arms, obtaining Done(), nil-channel attempts and creation are not sends/receives. Explicit repository channels are counted; unenumerated library/runtime/context lifecycle channels are **excluded, not zero**.
- A named value materialization copies all of this payload into another byte backing store: accumulation, owning bytes decode, containing-entry encode/deep clone, WAL staging, peer codecs, and returned-value response encode. Chunks into one destination are one full-value equivalent, not a copy per frame. Slice headers, request/response structs, pointers, interfaces, key bytes, metadata, sizing/checksum scans are not value-byte copies. A repository codec call and its dependency instruction are **one stage**.
- This is not a physical memcpy count, allocation count/op, CPU model or performance prediction. TLS, bufio/Framer transport buffers, sockets, kernel/page cache and runtime append-growth relocation are not enumerated. No fabricated number is assigned to them. All targets have the same custom transport at the payload boundary; none is a zero-copy physical end-to-end path.

## Tables — fixed value stages, not performance results

### Ordinary successful Put, one 1 KiB input value

| Named stage on leader | M | W | H | T |
|---|---:|---:|---:|---:|
| Framer DATA → owned request accumulation | 1 | 1 | 1 | 1 |
| Frontend owning Put.Value decode | 0 | 0 | 0 | 0 |
| Request → encoded Raft entry | 1 | 1 | 1 | 1 |
| Raft **proto2 Entry.Data deep clone** | 1 | 1 | 1 | 1 |
| Entry.Data → WAL staging | 1 | 1 | 1 | 1 |
| Apply owning Put.Value decode | 0 | 0 | 0 | 0 |
| Index value assignment | 0 | 0 | 0 | 0 |
| Ordinary Put reply's input value | 0 | 0 | 0 | 0 |
| **Known leader input-value subtotal, peers excluded** | **4** | **4** | **4** | **4** |
| One follower: decode + newly persisted WAL + apply | **2** | **2** | **2** | **2** |
| Sender peer encode + that follower | **3** | **3** | **3** | **3** |

Payload copying does **not** amortize as 1/batch-size: copying an entry copies every contained Put's full value. **M and W are a same-production-code control**, not an optimization delta. W→H and H→T remove **zero ordinary Put value stages**. The pre-merge preliminary main's 6-stage Put path is no longer the measured main and is excluded. H affects header and bookkeeping churn, not the 1 KiB payload census.

### CAS Txn — input new value versus returned stored value

| Named stage / payload identity | M compare succeeds | M compare fails | T compare succeeds | T compare fails |
|---|---:|---:|---:|---:|
| Owned request accumulation, **new input value** | 1 | 1 | 1 | 1 |
| Frontend owning bytes decode, **new input value** | 1 | 1 | 0 | 0 |
| Full Txn entry encode/append, **new input value** | 1 | 1 | 1 | 1 |
| Raft Entry.Data deep clone, **new input value** | 1 | 1 | 1 | 1 |
| WAL stage, **new input value** | 1 | 1 | 1 | 1 |
| Apply owning bytes decode, **new input value** | 1 | 1 | 0 | 0 |
| New value assigned to index | 0-byte copy; retained | **not assigned** | 0-byte copy; retained | **not assigned** |
| **Known leader new-input-value stages, peers excluded** | **6** | **6** | **4** | **4** |
| Comparing existing key's **stored value** | 0 | 0 | 0 | 0 |
| Range KeyValue construction, **stored value** | not executed | 0 | not executed | 0 |
| Server reply encode, **returned stored value** | absent / 0 | **1** | absent / 0 | **1** |
| One newly persisted/applied follower, **new input value** | **3** | **3** | **2** | **2** |
| Sender peer encode + that follower, **new input value** | **4** | **4** | **3** | **3** |

**Failure does not eliminate write-path costs.** The entire Txn, including both branches and the unexecuted Success Put's 1 KiB value, is logged, cloned, staged and decoded/walked on every replica. Main's owning apply decoder copies that unexecuted value too, then it is not retained by MVCC. T's alias decoder creates the request graph but no owning bytes buffer for it. On failure the returned bytes are the existing stored value, not the unexecuted new value; they receive one separately counted leader reply materialization of their own length. If that stored value is also 1 KiB, 6+1 versus 4+1 is a sum over **two distinct payloads**, not seven or five copies of the new Put value. Existing stored bytes' historical seed/write copies are not charged again. Followers can construct Range response objects during apply, but do not serialize a client reply for this leader-origin request.

For **C completed Txns with f actual compare failures**, known leader materializations average **M: 6 input + f/C returned-stored-value stages**, **T: 4 input + f/C returned-stored-value stages**. Given the confirmed 1 KiB returned values, the enumerated copied payload bytes are `1024*(6*C+f)` versus `1024*(4*C+f)` on the leader, excluding peers/transport/growth. This arithmetic covers two payload identities and is not physical bytes/op. Use actual observed f; no retries or per-op pre-reads add hidden calls. RPC errors, if any, need separate path accounting and must not be silently counted as compare failures.

### Channels — same topology for all four Put targets and M/T CAS, either compare result

Let N = requests in a fully consumed cohort; A = actual connection admission batches; Q = persist-queue arrays sent/consumed; F = worker sync completion groups; O/I = outbound queued-and-consumed / inbound delivered-and-consumed peer messages **at this node**; U = successful nonblocking Unreachable reports consumed by the inbox (conditional on queue/frame drops or faults); Ticks = selected ticker receives; Sw/Rw = successful writer wake sends / selected wake receives. None is an observed batch size. Admission, core entry, disk sync and completion groups are different populations.

| Cost per request | Sends | Receives | Closes | Fixed or conditional |
|---|---:|---:|---:|---|
| Request's own inbox event / reply channel | **0** | **0** | **0** | no private reply channel; Node.Do is not this route |
| Admission handoff | A/N | A/N | 0 | amortized per actual admission batch |
| Grouped completion direct call / atomic list | 0 | 0 | 0 | no completion channel |
| Persist queue | Q/N | Q/N | 0 | amortized by actual queue arrays |
| Disk completion | F/N | F/N | 0 | actual sync groups, not one/entry or one/request |
| Peer queues + inbound inbox | (O+I)/N | (O+I)/N | 0 | data/control, fanout and protocol dependent |
| Unreachable reports → inbox | U/N | U/N | 0 | conditional successful reports, zero if none; queue saturation can trigger without connection failure |
| Frontend writer wake | Sw/N | Rw/N | 0 | conditional/coalesced; not fixed per request/group |
| Core background tick | 0 | Ticks/N | 0 | background |
| **Explicit normal-request repository closes** | — | — | **0** | lifecycle/fault/watch closes outside scope |
| **Fresh explicit request reply channels created** | — | — | — | **0**, creation is not channel operation |

```
All four targets, and either M/T CAS outcome:
  sends   = A + Q + F + O + I + U + Sw
  receives= A + Q + F + O + I + U + Ticks + Rw
  explicit repository normal-request closes = 0

Follower serving no client calls in this leader-targeted workload:
  sends   = Q_f + F_f + O_f + I_f + U_f
  receives= Q_f + F_f + O_f + I_f + U_f + Ticks_f
  normal-request closes = 0
```

The follower has no client admission/completion writer wake for this leader-origin request; its own Q/F and peer populations are not the leader's and cannot be assumed equal.

Matched sends/receives are an accounting identity for consumed handoffs, **not** presumed equal within a measurement window: queued work may cross its boundaries. One directed peer message costs sender queue send+receive and receiver inbox send+receive: 2 repository operations/endpoint, 4 across both endpoints. Heartbeats, acks, empty appends and commit-only/hard-state-only disk work can pay channels without moving this value. Quorum client success need not wait for all follower work. No assumed two follower deliveries, one disk sync/request, or batch size appears in the totals.

## Verified payload and channel sites

The following tables give the current `file:line` at each bound SHA, not the prior audit's line numbers. `verified-sites.md` includes exact source text, classification and dependency sites for every target. Dependencies below use module-relative file identity, verified against each target's pins.

### Put input-value boundaries (a codec call and dependency instruction count once)
| Executed site / cost | M | W | H | T |
|---|---|---|---|---|
| Owned DATA accumulation: 1 | `internal/server/stream.go:127` | `internal/server/stream.go:127` | `internal/server/stream.go:127` | `internal/server/stream.go:127` |
| Full request-sized accumulation capacity: no growth copy | `internal/server/stream.go:124` | `internal/server/stream.go:124` | `internal/server/stream.go:124` | `internal/server/stream.go:124` |
| Owning frontend decode: bypassed for Put ALL targets | not this path | not this path | not this path | not this path |
| Wire frontend aliases body: no decode copy | `internal/server/batch.go:59` | `internal/server/batch.go:59` | `internal/server/batch.go:59` | `internal/server/batch.go:59` |
| Entry encode/append: 1 each | `internal/core/codec.go:68` | `internal/core/codec.go:68` | `internal/core/codec.go:68` | `internal/core/codec.go:68` |
| Proposal → retained Raft clone: 1 each | `internal/core/node.go:571` | `internal/core/node.go:571` | `internal/core/node.go:585` | `internal/core/node.go:591` |
| WAL caller (no extra stage) | `internal/wal/wal.go:368` | `internal/wal/wal.go:368` | `internal/wal/wal.go:368` | `internal/wal/wal.go:368` |
| WAL record append: 1 each | `internal/wal/wal.go:351` | `internal/wal/wal.go:351` | `internal/wal/wal.go:351` | `internal/wal/wal.go:351` |
| Apply owning decode: bypassed for Put ALL targets | not this path | not this path | not this path | not this path |
| Wire apply branch: no value copy | `internal/core/node.go:844` | `internal/core/node.go:844` | `internal/core/node.go:858` | `internal/core/node.go:864` |
| Wire apply ParsePut: aliases entry | `internal/core/node.go:880` | `internal/core/node.go:880` | `internal/core/node.go:894` | `internal/core/node.go:901` |
| Index Value slice retention: 0 | `internal/mvcc/ops.go:259` | `internal/mvcc/ops.go:259` | `internal/mvcc/ops.go:259` | `internal/mvcc/ops.go:259` |
| Version-array append: metadata only | `internal/mvcc/ops.go:250` | `internal/mvcc/ops.go:250` | `internal/mvcc/ops.go:250` | `internal/mvcc/ops.go:250` |
| Put reply encoding: 0 ordinary input-value copies | `internal/server/batch.go:259` | `internal/server/batch.go:259` | `internal/server/batch.go:265` | `internal/server/batch.go:267` |
| Peer sender Entry.Data encoder: 1/transmitted occurrence | `internal/peer/peer.go:419` | `internal/peer/peer.go:419` | `internal/peer/peer.go:419` | `internal/peer/peer.go:419` |
| Peer receiver Entry.Data owning decoder: 1/received occurrence | `internal/peer/peer.go:241` | `internal/peer/peer.go:241` | `internal/peer/peer.go:241` | `internal/peer/peer.go:241` |
| Storage append: entry pointers only | `internal/core/storage.go:39` | `internal/core/storage.go:39` | `internal/core/storage.go:39` | `internal/core/storage.go:39` |
| No-watch fast path: no event value encoding | `internal/core/watch.go:272` | `internal/core/watch.go:272` | `internal/core/watch.go:272` | `internal/core/watch.go:272` |

### Channel sites: each receive alternative is ONE receipt, never additive for the same message
| Executed site / cost | M | W | H | T |
|---|---|---|---|---|
| A admission sends | `internal/node/node.go:136` | `internal/node/node.go:136` | `internal/node/node.go:136` | `internal/node/node.go:136` |
| Inbox receives: outer alternative | `internal/node/node.go:335` | `internal/node/node.go:335` | `internal/node/node.go:335` | `internal/node/node.go:335` |
| Inbox receives: drained-input alternative | `internal/node/node.go:340` | `internal/node/node.go:340` | `internal/node/node.go:340` | `internal/node/node.go:340` |
| Completion: direct call, 0 channel ops | `internal/node/node.go:432` | `internal/node/node.go:432` | `internal/node/node.go:432` | `internal/node/node.go:432` |
| Completion publish: CAS, 0 channel ops | `internal/server/batch.go:212` | `internal/server/batch.go:212` | `internal/server/batch.go:212` | `internal/server/batch.go:214` |
| Writer completion collect: atomic Swap, 0 channel ops | `internal/server/batch.go:225` | `internal/server/batch.go:225` | `internal/server/batch.go:225` | `internal/server/batch.go:227` |
| Q disk queue sends | `internal/node/node.go:353` | `internal/node/node.go:353` | `internal/node/node.go:353` | `internal/node/node.go:353` |
| Q disk receives: initial array | `internal/node/node.go:486` | `internal/node/node.go:486` | `internal/node/node.go:486` | `internal/node/node.go:486` |
| Q disk receives: another drained array | `internal/node/node.go:493` | `internal/node/node.go:493` | `internal/node/node.go:493` | `internal/node/node.go:493` |
| F synced-group sends | `internal/node/node.go:507` | `internal/node/node.go:507` | `internal/node/node.go:507` | `internal/node/node.go:507` |
| F synced-group receives | `internal/node/node.go:346` | `internal/node/node.go:346` | `internal/node/node.go:346` | `internal/node/node.go:346` |
| Selected tick receive | `internal/node/node.go:357` | `internal/node/node.go:357` | `internal/node/node.go:357` | `internal/node/node.go:357` |
| O peer queue sends | `internal/peer/peer.go:297` | `internal/peer/peer.go:297` | `internal/peer/peer.go:297` | `internal/peer/peer.go:297` |
| O peer queue receive: fast control alternative | `internal/peer/peer.go:308` | `internal/peer/peer.go:308` | `internal/peer/peer.go:308` | `internal/peer/peer.go:308` |
| O peer queue receive: control alternative | `internal/peer/peer.go:314` | `internal/peer/peer.go:314` | `internal/peer/peer.go:314` | `internal/peer/peer.go:314` |
| O peer queue receive: data alternative | `internal/peer/peer.go:317` | `internal/peer/peer.go:317` | `internal/peer/peer.go:317` | `internal/peer/peer.go:317` |
| I inbound peer → inbox sends (same inbox receive sites above) | `internal/node/node.go:266` | `internal/node/node.go:266` | `internal/node/node.go:266` | `internal/node/node.go:266` |
| U successful Unreachable → inbox sends (same receive alternatives) | `internal/node/node.go:274` | `internal/node/node.go:274` | `internal/node/node.go:274` | `internal/node/node.go:274` |
| Sw successful writer wake sends | `internal/server/h2.go:195` | `internal/server/h2.go:195` | `internal/server/h2.go:201` | `internal/server/h2.go:201` |
| Rw selected idle writer receives | `internal/server/write.go:52` | `internal/server/write.go:52` | `internal/server/write.go:52` | `internal/server/write.go:52` |
| DATA credit wake ATTEMPT, not guaranteed send | `internal/server/h2.go:374` | `internal/server/h2.go:374` | `internal/server/h2.go:392` | `internal/server/h2.go:392` |
| Completion wake ATTEMPT, not guaranteed send | `internal/server/batch.go:216` | `internal/server/batch.go:216` | `internal/server/batch.go:216` | `internal/server/batch.go:218` |
| Unary body delivery returns before streaming signal | `internal/server/stream.go:139` | `internal/server/stream.go:139` | `internal/server/stream.go:139` | `internal/server/stream.go:139` |
| Unary end nil-channel ATTEMPT: no send | `internal/server/h2.go:526` | `internal/server/h2.go:526` | `internal/server/h2.go:650` | `internal/server/h2.go:650` |
| Unary writer space nil-channel ATTEMPT: no send | `internal/server/write.go:130` | `internal/server/write.go:130` | `internal/server/write.go:130` | `internal/server/write.go:130` |
| Retained Do reply-channel creation NOT this path | `internal/node/node.go:281` | `internal/node/node.go:281` | `internal/node/node.go:281` | `internal/node/node.go:281` |
| Node shutdown close, NOT normal request | `internal/node/node.go:308` | `internal/node/node.go:308` | `internal/node/node.go:308` | `internal/node/node.go:308` |
| Watch overflow close, NOT no-watch request | `internal/node/node.go:394` | `internal/node/node.go:394` | `internal/node/node.go:394` | `internal/node/node.go:394` |
| Server Stop close, NOT normal request | `internal/server/server.go:189` | `internal/server/server.go:189` | `internal/server/server.go:189` | `internal/server/server.go:189` |

### Admission, completion and wakes

The reader accumulates a full owned request buffer, then calls synchronous unary admission; Put/CAS use SubmitBatch and not the retained Do. M/W pass wire Put descriptors (generic protobuf request structs for CAS) to individual core.Submit calls; H/T use SubmitAll with a batch-owned pending-record array. All descriptor/slice-header handoffs copy **zero value bytes**. Atomic completion publication and writer collection copy no payload and do not send a Go channel.

Every received DATA frame calls credit→signal, not only a receive-window threshold; completion groups, control/window changes and setup/lifecycle events also attempt the same capacity-one writer wake. Sw counts only successful sends, Rw only selected receives while the writer is idle. For a connection interval, `Rw = Sw + initial_token_occupancy - final_token_occupancy`; no fixed two operations per reply or bound Sw≤completion_groups follows. Unary delivery returns with `st.body = m` before the streaming body-signal select, so that select is **not executed**. Unary st.inSignal and st.space are nil; the end-of-input and writer space nonblocking attempts execute default and contribute zero. Optional grpc-timeout is AfterFunc, not a per-request Timer.C receive; firing/cancellation is outside the scope.

SubmitAll, pending-array ownership and new wire fields are **direct method/struct operations, not extra channel handoffs**:

### Submission and owner fields: zero additional channel operations
| Executed site / cost | M | W | H | T |
|---|---|---|---|---|
| Connection opBatch.reqs descriptor storage | `internal/server/batch.go:39` | `internal/server/batch.go:39` | `internal/server/batch.go:39` | `internal/server/batch.go:39` |
| Reader → Node.SubmitBatch direct call (actual A send is separately listed) | `internal/server/batch.go:126` | `internal/server/batch.go:126` | `internal/server/batch.go:126` | `internal/server/batch.go:128` |
| Node batch.reqs descriptor storage | `internal/node/node.go:169` | `internal/node/node.go:169` | `internal/node/node.go:169` | `internal/node/node.go:169` |
| Batch wrapper retains request/tag arrays | `internal/node/node.go:199` | `internal/node/node.go:199` | `internal/node/node.go:199` | `internal/node/node.go:199` |
| Wrapper → inbox admission via put (not a second send) | `internal/node/node.go:200` | `internal/node/node.go:200` | `internal/node/node.go:200` | `internal/node/node.go:200` |
| waiter.to / waiter.tag metadata; no reply channel | `internal/node/node.go:446` | `internal/node/node.go:446` | `internal/node/node.go:446` | `internal/node/node.go:446` |
| Core Submit vs SubmitAll: zero sends/receives/closes | `internal/node/node.go:447` | `internal/node/node.go:447` | `internal/node/node.go:448` | `internal/node/node.go:448` |
| H/T batch-owned pending array; zero channel operations | not this path | not this path | `internal/core/node.go:373` | `internal/core/node.go:376` |
| H/T SubmitAll direct per-record calls | not this path | not this path | `internal/core/node.go:375` | `internal/core/node.go:378` |
| pending.req retains descriptors, no value copies | `internal/core/node.go:145` | `internal/core/node.go:145` | `internal/core/node.go:145` | `internal/core/node.go:148` |
| PR80 Request.Wire new alias field | not this path | not this path | not this path | `internal/core/node.go:108` |

The disk worker drains multiple Q arrays and sends one F completion group after Sync. RawNode local append/apply thread IDs are synchronously interpreted messages, **not Go channel operations**. Node shutdown, server Stop and watch overflow closes, selected cancellation/Done receives, peer failure draining/backoff, streaming watch/keepalive operations and observer health/pprof RPCs are not normal Put/CAS costs. Library context/transport lifecycle channels are not asserted zero.

### Raft, WAL, index and peers

Leader proposal initially aliases entry Data in RawNode, then leader appendEntry **deep clones** Entry via proto.Clone. The proto2 Entry.Data byte copy is `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188`; it is not the proto3 Put.Value owning decoder `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5497`. WAL.record appends the entry's full Data to its staging buffer once; its caller is not a second copy. WAL File.Write/Sync and CRC scans are durability/checksum work, not additional enumerated userspace value buffers. Storage append retains entry pointers; index assignment retains a value slice header.

Sender peer protobuf encoding copies Entry.Data once per transmitted occurrence/recipient; receiver owning **proto2** decoding copies it once per received occurrence. Followers use MsgApp→maybeAppend→log/unstable append of entry pointers, not leader appendEntry's proposal clone, and do not re-encode commands through encodeBatch. Newly persisted follower entries pay WAL staging and apply. Duplicate receipt can decode without new persist/apply; retransmission/fanout multiplicity is conditional. Peer acknowledgements contain no 1 KiB input value. The stored value returned by failure Range is not replicated again by this request: no same-request peer copy of that old value is charged.

## CAS branch and alias proof

### M/T CAS sites: both compare outcomes follow the full request write path
| Executed site / cost | M | T |
|---|---|---|
| Frontend fresh protobuf request graph | `internal/server/batch.go:66` | `internal/server/batch.go:68` |
| Frontend owning vs alias decoder | `internal/server/rpc.go:63` | `internal/server/batch.go:69` |
| Classify both branches, before compare | `internal/mvcc/validate.go:185` | `internal/mvcc/validate.go:185` |
| Success Put makes write (not read-only) | `internal/mvcc/validate.go:187` | `internal/mvcc/validate.go:187` |
| Write/read routing | `internal/core/node.go:394` | `internal/core/node.go:411` |
| Entry entire-Txn input-value copy | `internal/core/codec.go:73` | `internal/core/codec.go:73` |
| Apply owning vs alias decode | `internal/core/node.go:851` | `internal/core/node.go:872` |
| State → MVCC Txn | `internal/core/state.go:140` | `internal/core/state.go:140` |
| Compute compare path | `internal/mvcc/ops.go:320` | `internal/mvcc/ops.go:320` |
| Existing key → KeyValue slice-header alias | `internal/mvcc/ops.go:366` | `internal/mvcc/ops.go:366` |
| MOD int64 comparison, zero stored-value copy | `internal/mvcc/ops.go:390` | `internal/mvcc/ops.go:390` |
| Executed Success Put only | `internal/mvcc/ops.go:485` | `internal/mvcc/ops.go:485` |
| Executed Failure Range only | `internal/mvcc/ops.go:478` | `internal/mvcc/ops.go:478` |
| Range builds KeyValue objects, no value bytes copy | `internal/mvcc/ops.go:85` | `internal/mvcc/ops.go:85` |
| KeyValue.Value retained header: no bytes copy | `internal/mvcc/store.go:73` | `internal/mvcc/store.go:73` |
| Response succeeded flag | `internal/mvcc/ops.go:470` | `internal/mvcc/ops.go:470` |
| MVCC revision depends on actual mutation | `internal/mvcc/ops.go:328` | `internal/mvcc/ops.go:328` |
| CAS generic response-encode caller | `internal/server/batch.go:261` | `internal/server/batch.go:272` |
| One full returned-stored-value response encode | `internal/server/stream.go:207` | `internal/server/stream.go:207` |
| Fresh apply Txn struct remains in both | `internal/core/codec.go:122` | `internal/core/codec.go:127` |
| Alias repeated submessage reconstruction | not this path | `internal/core/alias.go:100` |
| Alias singular/oneof submessage reconstruction | not this path | `internal/core/alias.go:104` |
| Alias bytes slice + cap clip | not this path | `internal/core/alias.go:160` |
| Alias field setter | not this path | `internal/core/alias.go:130` |

Both Success and Failure are inspected by IsTxnReadOnly before compare evaluation; a Success Put makes this a write request, even if the MOD compare will fail. It queues through Raft without a separate client Range/ReadIndex round trip. MVCC computes the compare path at replicated apply, checks and executes **only that branch**. Existing-key MOD compare builds a KeyValue with a header-only Value alias and compares two int64 revisions, not value bytes. Successful Put increments the MVCC revision; failure returns the current Range at the applied state's current revision and does not change MVCC revision (the Raft log still advances). Failure is Succeeded=false with a normal RPC response, not an RPC error. No lease/watch/PrevKv copies are hidden in these scoped tables.

Main CAS frontend proto.Unmarshal owns nested Put.Value bytes; generic entry MarshalAppend copies the full Txn; the non-Put walkBatch branch proto.Unmarshal owns all nested request bytes before branch choice. Main's ordinary Put instead follows ParsePut and skips both owning decodes. In T, frontend Request.Wire aliases the already owned body, encodeBatch appends Wire as-is, walkBatch supplies capped entry subslices, UnmarshalAlias's ValueOfBytes(slices.Clip(x)) retains them, and MVCC put retains those bytes. Thus the successful Txn's indexed new value aliases **Raft-owned Entry.Data**, not frontend or WAL staging storage. Failed Txn's new value remains in the log but not in the index. Range response KeyValue similarly aliases the existing indexed bytes; one response encoder append finally materializes that old value into the client reply buffer.

**Alias reflection is still decoding and rebuilding request structs.** T constructs the top-level protobuf TxnRequest at the frontend and at apply; Mutable/AppendMutable reconstruct Compare, RequestOp, PutRequest/RangeRequest and oneof wrappers. Field Set copies slice headers, not full bytes. The pinned protobuf Set→scalar setter→bytes converter chain proves `[]byte` retains the backing store (no hidden bytes clone). It does not prove zero objects, zero allocations, or “decoded never.” Unknown/mistyped fields are explicitly copied into unknown storage, string fields can materialize strings, and unusual repeated encodings change work; canonical scoped CAS has none of those additional full-value copies.

## PR79 claims and dependency/spec limits

H's per-connection HPACK decoder consumes fields directly rather than x/net MetaHeadersFrame.Fields. Warm fully indexed header blocks can avoid the field-list/literal churn targeted by its test; header literals, first request, changing grpc-timeout fields, decoder buffering and dynamic-table updates are conditional. Headers are **not the Put/CAS 1 KiB value**, and a source/test claim is not measured allocations/op here. H/T reserve unary output capacity and group core pending records in one array, but still allocate per-stream objects/output storage and request/reply objects on relevant paths. Exact sites: H/T core pending batch array ``internal/core/node.go:373`` / ``internal/core/node.go:376``; output capacity ``internal/server/stream.go:280`` / ``internal/server/stream.go:280``; HPACK decode ``internal/server/h2.go:422`` / ``internal/server/h2.go:422``.

**PR79 body drift:** it describes one shared Put reply buffer per completion batch. At the code bound above, H/T explicitly call `appendPutReply(make([]byte, 0, 5+putResponseSize), ...)` **once per reply**, with a comment that a slow stream pins only its own charged buffer. Do not repeat the shared-buffer claim without rechecking the final measurement SHA. Small ordinary Put replies carry no 1 KiB value in either version.

Issue50 Design change, addendum and correction are preserved in `issue50.md/json` and `issue50-comments.json`: comments 6140814920 (custom frontend), 6140846333 (log-backed values/in-place apply), 6140902413 (per-request churn/blocking and allocation-strategy experiment correction), and latest observed 6142147602 (seed gate). A final metadata refresh (`issue50-final.json`, `issue50-comments-final.json`) independently confirmed that Design/addendum/correction text was unchanged. Current docs/design.md in the four source snapshots was reviewed, including the proposed value-entry seams. Correction supersedes the specific ban on channels and mandate for one batch allocation lifetime; it does not withdraw log-backed values and no request-struct rebuild at apply goals.

- Main CAS still retains a decoded input value in MVCC separate from entry bytes. **All four ordinary Put paths** bypass generic protobuf decode and keep entry slices. T extends **bytes ownership aliasing** to CAS and other generic write decodes, not zero decode/zero request structs.
- These are in-memory entry slices, **not a (WAL segment, offset, length) index addressing implementation**. “Stored once” is valid narrowly for successful wire/alias apply's entry/index relationship, not the whole pipeline: frontend storage, entry encode, Raft clone, WAL/peer buffers and disk representations still exist. One small value can pin its whole entry backing array.
- All versions still have blocking-capable SubmitBatch inbox sends and connection mutexes. H's pending array is per batch, not a startup fixed-ring runtime or demonstrated recycled arena. Completion's atomic list doesn't establish no churn/blocking across the entire path. Source does not supply allocations/op, GC CPU, retained bytes, or response to every 1.5 MiB/slow-client configuration.
- WAL staging and reused peer encode buffers can relocate with growth. **All four targets'** entry upper-bound capacity includes full wire/proto size plus per-command framing and count, preventing that particular scoped entry growth; the one explicit append is still a copy. No count is invented for other relocation or physical TLS/kernel transport.
- No throughput/CPU/GC win, all-path correctness, real power-loss durability or full issue50 completion is inferred. Requester-requested `/proc/PID/io` syscr/syscw runtime counts and PR80 profile/Syscall6 inspection are **not source-audit results**; this report does not infer runtime syscall counts from named source I/O sites. PR79's allocation and PR80's reflection-cost claims require the paired measurement, not this static census.

## Bottom line

Put known leader payload stages are **4 in all four final targets**; M/W are a verified same-code control. M→T CAS removes frontend and apply **owning bytes decodes**, **6→4 new-input stages for either compare result**, while failure Range separately adds **one returned-stored-value encoding** in both. No ordinary Put copy-stage improvement is attributable to H or T over W. All four use the same batch-amortized repository channel topology: zero fixed request-private sends/receives/closes and no new per-request reply channel, plus actual admission/disk/peer and conditional wake/tick work. These are verified named boundaries, not physical-copy totals or measured performance.
