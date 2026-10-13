# PR80 lean-generator follow-up: Put / revision-CAS source audit

## Binding and controls

**FINAL.** Parent-confirmed immutable server and generator SHAs; no later branch heads substituted.

Actions binding: run **38397810888**, resolve **2026-10-13T12:30:50Z** ([run](https://github.com/djosh34/tigerstore/actions/runs/38397810888)).

| Target | Exact source commit |
|---|---|
| M — main | `aa9d9b6eea79c73f12fe0153aefa809841967e88` |
| P80 — PR80 | `67ff792a72f78e83cb6905563dcb6bc0e9bb2764` |
| Stock etcd v3.7.2 (unchanged release) | `68c065e562994b89e333e77b039ad066f933c586` |

Requested paired run: **Put256 and CAS256, five repetitions**, main vs PR80 and stock etcd, new lean raw HTTP/2 generator. No result, pass/failure population, or performance claim is inferred here. Generator status: **FINAL immutable generator source, parent-confirmed**; current observed engine path `experiments/bench75/load/newlean.go`. Server and generator bindings are distinct. Generator/harness exact commit **`e3bc29f5de3b9184b8aae5f64ed290681d6e1d6f`**, parent-confirmed for this run. No mutable-source substitution. No profiles requested; benchmarks are ARM-only and run by the parent/workflow, not this source audit. Actual generator CPU and other runtime results are reported separately by the parent.

All writes confined to `/home/joshazimullah.linux/work_mounts/scratch/bench80-lean-source-audit`. Supplied repository read-only. No subagents, GitHub writes, Go execution, builds, benchmarks or tests. Deadline `2026-10-13T22:00:00Z`. Source-only Python asserts exact sites and byte equality to selected `git show <SHA>:<path>` inputs. No whole module ZIPs, whole-repo export, or bare object repository copied. `verified-sites.md/json` binds every listed site to its target commit, exact text and file SHA256; `source-file-hashes.json`, `reference-manifest.json`, `workload-manifest.json`, `SHA256SUMS` provide small integrity manifests.

Starting audit retained by reference: `evidence/bench75/run-38387706444/source-audit/audit.md`. Its old 6→4 CAS conclusion is **superseded for these refs**, not inherited. Main includes merged **#79 HPACK and batch-owned pending array**, sites `internal/server/h2.go:422` / M `internal/core/node.go:373`; P80 `internal/core/node.go:376`; these change headers/bookkeeping, not full-value payload stages. Selected current production source/diff was reviewed; unchanged exact source spans were relocated and asserted, not old line numbers blindly reused. The final main is **aa9d9b6**, not preliminary **549e0c9**. Their complete diff changes only `internal/peer/peer_test.go` and `internal/peer/stall_test.go`; production code/dependency pins are unchanged. The earlier peer-listener setup change shifts peer sites +7 lines but does not add timed value/channel boundaries. The separate new read-clock branch is **not this run** and contributes nothing to the formulas.

## Scope and meaning

Established leader-targeted unary requests in a replicated three-node cluster; existing keys; canonical protobuf; no compression, lease, watches, PrevKv, IgnoreValue/IgnoreLease, auth, deadline firing, faults, restart or recovery. Put proposes one 1,024-byte value. CAS compares **EQUAL MOD** to a positive cached revision; Success = one Put; Failure = current point Range with the stored value, not KeysOnly or CountOnly. A stale revision is a compare failure, not an RPC error. Cache seeding/probes/warm requests and response validation are outside the server stages of a timed request.

A named full-value stage materializes all of that payload in another byte backing store. Chunks into one destination count once; caller and implementing codec instruction count once. Structs, interfaces, slice headers, entry pointers, keys, sizing/checksum scans and metadata are not value copies. **No physical-copy totals or ratios**: TLS/Framer/bufio/socket/kernel/page-cache buffers, runtime growth relocation and unenumerated lifecycle channels are excluded, **not zero**. Stages do not establish allocations/op, GC, CPU, syscalls, throughput or durability. No request-count-derived peer fanout, batching or sync multiplicity is assumed.

## 1. Leader: new input payload

Numbers apply to either CAS comparison result; the entire Txn is logged before comparison.

| Named full-new-value stage | M Put | P80 Put | M CAS | P80 CAS | Exact site(s) |
|---|---:|---:|---:|---:|---|
| Framer DATA → owned request accumulation | 1 | 1 | 1 | 1 | `internal/server/stream.go:127` |
| Frontend owning nested Put.Value decode | 0 | 0 | 1 | 1 | CAS caller M `internal/server/batch.go:67`; P80 `internal/server/batch.go:71` → helper `internal/server/rpc.go:63` → `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5497` |
| Request → encoded Raft entry | 1 | 1 | 1 | 1 | Put `internal/core/codec.go:68`; M CAS `internal/core/codec.go:73`; P80 CAS `internal/core/codec.go:73` |
| Retained Raft proto2 Entry.Data deep clone | 1 | 1 | 1 | 1 | proposal M `internal/core/node.go:585`; P80 `internal/core/node.go:591` → `dependency-sources/raft-v3.7.0/raft.go:817` → `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188` |
| Entry.Data → WAL staging | 1 | 1 | 1 | 1 | `internal/wal/wal.go:368` → `internal/wal/wal.go:351` (one boundary) |
| Apply owning nested Put.Value decode | 0 | 0 | 1 | 0 | M CAS `internal/core/node.go:865` → `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5497`; P80 aliases at `internal/core/node.go:872` |
| Index value assignment | 0 | 0 | 0 | 0 | retained slice header `internal/mvcc/ops.go:259` |
| Ordinary successful reply: new input value absent | 0 | 0 | 0 | 0 | Put M `internal/server/batch.go:265`; P80 `internal/server/batch.go:269`; CAS M `internal/server/batch.go:270`; P80 `internal/server/batch.go:274` |
| **Known leader new-input subtotal, peers excluded** | **4** | **4** | **6** | **5** | named source boundaries, not physical totals |

Frontend request-size reserve is `internal/server/stream.go:124`; entry reserve `internal/core/codec.go:37`, P80 wire sizing `internal/core/node.go:521`. This prevents a fixed extra growth stage at those specific boundaries; it does not imply no transport/growth copies elsewhere. Put frontend ParsePut `internal/server/batch.go:59` and apply M `internal/core/node.go:894`; P80 `internal/core/node.go:901` alias input/entry slices; Value alias `internal/core/wire.go:71`, wire byte subslice `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:468`. Storage `internal/core/storage.go:39` and MVCC version array `internal/mvcc/ops.go:250` retain pointers/metadata, not new value bytes. No-watch fast path `internal/core/watch.go:272` excludes event encodes in scope.

**PR80 frontend reset verified:** its generic admission now calls `unmarshal` → `proto.Unmarshal`, not `core.UnmarshalAlias`. It still sets `Request.Wire = st.body` at `internal/server/batch.go:70` and appends the original complete Txn body to the entry. The temporary frontend-decoded value is therefore an owning materialization even though it is not used to re-encode the entry. The `Request.Wire` comment at `internal/core/node.go:108` still says decoded with UnmarshalAlias: that comment is stale for this frontend. **6→5, not 6→4.** Apply still uses UnmarshalAlias and successfully indexed CAS bytes alias Raft-owned Entry.Data. Main CAS index instead retains apply-decoded bytes.

Alias apply is still decoding: fresh Txn struct M `internal/core/codec.go:122`; P80 `internal/core/codec.go:127`; P80 repeated/singular submessage construction `internal/core/alias.go:100` / `internal/core/alias.go:104`; capped bytes `internal/core/alias.go:160` and field setter `internal/core/alias.go:130`. Protobuf reflection Set → scalar setter → bytes converter `dependency-sources/protobuf-v1.36.11/internal/impl/message_reflect_gen.go:97` + `dependency-sources/protobuf-v1.36.11/internal/impl/message_reflect_field.go:317` + `dependency-sources/protobuf-v1.36.11/internal/impl/convert.go:358` retains the backing store, not a full-value clone. Unknown fields can be copied at `internal/core/alias.go:47`; canonical scoped requests have no such full-value unknown field. This proves bytes ownership, not zero request objects/allocations, never-decoded execution, or complete arbitrary-wire compatibility.

## 2. CAS: branch selection and old stored payload

Both Success and Failure are inspected before compare by `internal/mvcc/validate.go:185`; detecting Success Put at `internal/mvcc/validate.go:187` selects write/Raft routing M `internal/core/node.go:408`; P80 `internal/core/node.go:411`. The full Txn, including unexecuted Success Put.Value, is encoded, cloned, WAL-staged and decoded/walked on replicas **regardless of compare outcome**. M's owning apply decode copies that new value even on failure; P80's alias apply does not. Compare path `internal/mvcc/ops.go:320` runs at replicated apply through `internal/core/state.go:140`; only the chosen branch executes.

| Existing stored-value stage on leader | CAS pass | CAS fail | Both-target site |
|---|---:|---:|---|
| Construct existing KV / MOD integer compare | 0 | 0 | `internal/mvcc/ops.go:366`, `internal/mvcc/ops.go:390`, slice alias `internal/mvcc/store.go:73` |
| Failure.Range KV construction | absent | 0 | Range execution `internal/mvcc/ops.go:478` → `internal/mvcc/ops.go:85` |
| Encode returned stored value in TxnResponse | absent | **1** | M `internal/server/batch.go:270`; P80 `internal/server/batch.go:274` → `internal/server/stream.go:207` → bytes append `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` |

That conditional **one reply copy is of the old stored value**, not the unexecuted new input value; historical write/seed copies are not charged again. Successful Put branch `internal/mvcc/ops.go:485`; Succeeded flag `internal/mvcc/ops.go:470`; actual mutation controls revision `internal/mvcc/ops.go:32` / `internal/mvcc/ops.go:328`. Failure leaves MVCC revision unchanged while the Raft log advances. Followers may build Range responses but do not serialize this leader client's reply. Failure stored value is not re-replicated by this Txn.

For C completed timed CAS calls and actual f compare failures: known leader stages average **M: 6 new input + f/C returned stored**; **P80: 5 new input + f/C returned stored**. These are two payload identities. Use this run's actual f; **the previous audit's zero failures are not evidence for this run**. RPC errors require separate accounting; no retries or timed client pre-reads are silently added.

## 3. Peer/follower new-input stages: per occurrence, not a batch assumption

| Named stage on one newly received, persisted and applied follower | Put M/P80 | CAS M | CAS P80 | Exact site |
|---|---:|---:|---:|---|
| Peer owning proto2 Entry.Data decode | 1 | 1 | 1 | `internal/peer/peer.go:248` → `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5418` |
| Newly persisted entry WAL staging | 1 | 1 | 1 | `internal/wal/wal.go:368` → `internal/wal/wal.go:351` |
| Apply owning nested Put.Value decode | 0 | 1 | 0 | M `internal/core/node.go:865`; P80 `internal/core/node.go:872`; Put ParsePut M `internal/core/node.go:894`; P80 `internal/core/node.go:901` |
| **Follower new-input subtotal** | **2** | **3** | **2** | same for CAS pass/fail |
| Sender Entry.Data encode, separate transmitted occurrence | **+1** | **+1** | **+1** | `internal/peer/peer.go:426` → bytes append `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` |
| **One sender occurrence + that new follower entry** | **3** | **4** | **3** | not a cluster-wide per-request total |

Follower MsgApp path `dependency-sources/raft-v3.7.0/raft.go:1800` → `dependency-sources/raft-v3.7.0/log.go:127` → `dependency-sources/raft-v3.7.0/log_unstable.go:205` retains entry pointers; it does **not** run leader proposal appendEntry's deep clone. Payload bytes cannot amortize as 1/batch-size: each contained value is present in each encoded/copied entry. Retransmit/duplicate decode, recipient fanout, catch-up and newly persisted/apply occurrences differ. Acks and heartbeats may pay channels with no input value. Client quorum success does not imply all followers finished. No assumed peer batch sizes, exactly two deliveries, or one fsync/request.

## 4. Repository channels: same main/PR80 Put/CAS topology

Count successful send and selected receive separately; close and creation separately. Default/unselected arms, nil-channel attempts, Done() access, atomic operations and synchronous method calls are not handoffs. Let N be a fully consumed request cohort; A actual admission batches; Q actual persist-queue arrays; F actual worker sync-completion groups; O/I outbound queued-and-consumed / inbound delivered-and-consumed peer messages at this node; U successful nonblocking Unreachable reports consumed; Sw/Rw successful writer wake sends / selected receives; Ticks selected core tick receives. These populations are distinct and are **not observed batch sizes**.

| Population / classification | Sends/N | Receives/N | Normal closes | Exact counted site(s), both targets |
|---|---:|---:|---:|---|
| Fixed request-private inbox/reply | **0** | **0** | **0** | SubmitBatch route M `internal/server/batch.go:126`; P80 `internal/server/batch.go:130`; retained Do sites excluded below |
| A — amortized admission | A/N | A/N | 0 | `internal/node/node.go:136`; receive alternatives `internal/node/node.go:335` / `internal/node/node.go:340` |
| Completion — direct call/atomic list | 0 | 0 | 0 | `internal/node/node.go:432` + M `internal/server/batch.go:212`; P80 `internal/server/batch.go:216` + M `internal/server/batch.go:225`; P80 `internal/server/batch.go:229` |
| Q — amortized persist queue | Q/N | Q/N | 0 | `internal/node/node.go:353`; alternatives `internal/node/node.go:486` / `internal/node/node.go:493` |
| F — actual synced worker group | F/N | F/N | 0 | `internal/node/node.go:507` / `internal/node/node.go:346` |
| O — outgoing peer queue, conditional protocol population | O/N | O/N | 0 | `internal/peer/peer.go:304`; alternatives `internal/peer/peer.go:315` + `internal/peer/peer.go:321` + `internal/peer/peer.go:324` |
| I — inbound peer → inbox | I/N | I/N | 0 | `internal/node/node.go:266`; same inbox receive alternatives |
| U — successful conditional Unreachable report | U/N | U/N | 0 | `internal/node/node.go:274`; same inbox receive alternatives |
| Coalesced frontend wake | Sw/N | Rw/N | 0 | `internal/server/h2.go:201` / `internal/server/write.go:52` |
| Background clock tick | 0 | Ticks/N | 0 | `internal/node/node.go:357` |

```
Leader sends    = A + Q + F + O + I + U + Sw
Leader receives = A + Q + F + O + I + U + Rw + Ticks
Follower sends    = Q_f + F_f + O_f + I_f + U_f
Follower receives = Q_f + F_f + O_f + I_f + U_f + Ticks_f
Fixed request-private sends/receives/closes = 0 / 0 / 0
Fresh explicit private request reply channels = 0
Explicit repository normal-request closes = 0
```

Followers serving no client calls have no A/client-completion wake for these leader-origin calls. Their Q/F/peer populations cannot be set equal to the leader's. Receive alternatives are **one receipt**, never added for the same message. Matched consumed handoffs are not presumed balanced inside a timed window; queue occupancy can cross boundaries. One directed peer message has sender queue send+receive and receiver inbox send+receive, four operations across endpoints, **not four fixed operations/request**.

Both use SubmitAll `internal/node/node.go:448` with one batch-owned pending array M `internal/core/node.go:373`; P80 `internal/core/node.go:376` and direct per-record calls M `internal/core/node.go:375`; P80 `internal/core/node.go:378`. Descriptors/waiter tags `internal/server/batch.go:39` + `internal/node/node.go:169` + `internal/node/node.go:199` + `internal/node/node.go:446` are no private reply channel. RawNode async append/apply message IDs are synchronous core messages, not Go channels. Disk worker drains Q arrays and Syncs `internal/node/node.go:503` before sending F; Q, F and requests are not interchangeable.

Every received DATA frame credits and attempts writer signal at `internal/server/h2.go:392`; completion attempts at M `internal/server/batch.go:216`; P80 `internal/server/batch.go:220`; other control/window/setup signals share capacity-one wake. Thus wake counts are conditional/coalesced, not a fixed pair per RPC or completion group. For a connection interval `Rw = Sw + initial_wake_occupancy - final_wake_occupancy`. Unary delivery `internal/server/stream.go:139` returns before streaming body-signal `internal/server/stream.go:148`; unary inSignal/space remain nil (streaming-only allocation `internal/server/h2.go:578`). End-input `internal/server/h2.go:650` and writer-space `internal/server/write.go:130` nonblocking nil sends select default: zero.

Retained Node.Do creation/send/receive `internal/node/node.go:281` + `internal/node/node.go:420` + `internal/node/node.go:286` is **not this route**. Shutdown/watch/server-stop closes `internal/node/node.go:308` + `internal/node/node.go:394` + `internal/server/server.go:189`, streaming operations, faults/backoff, cancellation and library/runtime/context lifecycle channels are excluded, not asserted nonexistent. Timed lean requests send no grpc-timeout, so guard `internal/server/h2.go:590` does not arm `internal/server/h2.go:593`; node tick-based request timeout remains. Control grpc Range deadlines are separate untimed work.

## 5. Lean framing / workload observations

The raw engine derives from `exp/pre-47-grpc`, immutable commit `d2408301966b99471bfed06d4c2dc1cd1e5af462`, `experiments/grpc/lean.go`, SHA256 `9553545389d7ae3d246af1ecf03acc26b7a3f167a7f33883c046054c6ed1f07e` (independently recomputed). Only that base file is retained; actual engine/integration snapshot hashes are in `workload-manifest.json`.

Observed/requested contract: 4,096 Kubernetes-shaped keys; global permutation `i*2654435761%4096`; independently SHA256-derived 1 KiB constant value per key; 16 active TLS 1.3 mTLS raw h2 connections, 256 total outstanding closed-loop slots; one idle grpc-go connection used for untimed control probes/cache Range. Raw connections complete SETTINGS and **one untimed valid RPC each before the before revision probe and CPU/IO/heap snapshots**. Startup Put/CAS writes are outside N and revision fencing. CAS cache uses positive shared atomic revisions, monotonically updated even for out-of-order replies. Pass returns revision from Put; failure Range validates exact key/value and positive ModRevision. No timed pre-read or retry.

Put/compare/branch templates are prebuilt; CAS changes canonical comparison lengths and minimal dynamic ModRevision varint in slot-owned scratch. HPACK uses per-connection encoder state, first block then warmed indexed block, with SETTINGS-dependent rebuild. There is **no grpc-timeout header** in the timed raw field list; its local socket/RPC timeout is not a transmitted deadline. The final committed implementation permits **conditional DATA fragmentation for flow control**, even though each scoped request is below 4 KiB. This supersedes the earlier single-WriteData description. TLS record/socket segmentation does **not** mean HTTP/2 DATA-frame segmentation. The committed engine slices DATA by actual connection/stream send credit and maxFrame at `experiments/bench75/load/newlean.go:589`. A short request can fit one DATA frame when credit suffices, but partial credit can fragment it; no unconditional one-frame claim or C=0 follows. Writes may still be coalesced in the client buffer. Response connection credit is coalesced at **64 KiB** (`experiments/bench75/load/newlean.go:30` / `experiments/bench75/load/newlean.go:709`); stream credit is similarly coalesced for sufficiently large still-open replies (`experiments/bench75/load/newlean.go:717`). The cached 10-second write deadline is renewed only with **<=5 seconds remaining** (`experiments/bench75/load/newlean.go:278`), not once per reply; it is still not a transmitted grpc-timeout. Therefore client HTTP/2 framing, HPACK table history, metadata, transport scheduling and potential stock grpc codec segmentation differ from the previous grpc-go benchmark. Do not claim identical framing or import its grpc-timeout channel close, allocs/op, failure populations or wake/batch populations.

**Server request-value stage counts do not otherwise change with this client**: Tiger still accumulates one owned full request, followed by the counted frontend/entry/clone/WAL/apply boundaries above. Different frames may change chunks, wake attempts or batching, not the number of scoped full-request accumulation destinations. Source templates/HPACK are client-side optimizations, not extra server new-value copies. The final generator/integration files were re-read from the parent-confirmed immutable commit and byte/hash verified; no modification of helper/repository files was made.

**Observed workload citations** (snapshot hashes and exact text in `workload-sites.md/json`): TLS/mTLS `experiments/bench75/load/main.go:96`; connection/slot population `experiments/bench75/load/newlean.go:198` / `experiments/bench75/load/newlean.go:182` with one grpc control `experiments/bench75/load/main.go:100`; keys/value derivation `experiments/bench75/load/main.go:121` / `experiments/bench75/load/main.go:125` / `experiments/bench75/load/main.go:128`; global claim/permutation `experiments/bench75/load/newlean.go:128` / `experiments/bench75/load/newlean.go:551`; templates and revision/length varints `experiments/bench75/load/newlean.go:42` / `experiments/bench75/load/newlean.go:75` / `experiments/bench75/load/newlean.go:77`; complete timed metadata field list `experiments/bench75/load/newlean.go:253`; first/warm HPACK blocks `experiments/bench75/load/newlean.go:296` / `experiments/bench75/load/newlean.go:303`; actual DATA source call `experiments/bench75/load/newlean.go:589`; one raw warm RPC/connection `experiments/bench75/load/newlean.go:224` precedes caller pool completion `experiments/bench75/load/main.go:146`, revision fence `experiments/bench75/load/main.go:153`, CPU sample `experiments/bench75/load/main.go:262` and phase start `experiments/bench75/load/main.go:279`; cached CAS branch shape `experiments/bench75/load/cas.go:16` / `experiments/bench75/load/cas.go:17` / `experiments/bench75/load/cas.go:18`, untimed seed `experiments/bench75/load/cas.go:25`, cached compare input `experiments/bench75/load/newlean.go:561`, failure validation `experiments/bench75/load/cas.go:67`, monotonic cache `experiments/bench75/load/cas.go:76` / `experiments/bench75/load/newlean.go:733`; five reps `experiments/bench75/run.py:277`, lean env `.github/workflows/bench75.yml:98`, requested Put/CAS256 suite `.github/workflows/bench75.yml:88`. Client sites are **not counted server channels/value stages**.

## 6. Unchanged stock etcd: previous verified source audit by reference

Use `evidence/bench75/run-38387706444/etcd-source-audit/audit.md` and its `verified-sites.md`, `verification.json`, `dependency-integrity.json`, release-tag/binary records, with hashes in `reference-manifest.json`. No release/dependency refetch. Stock server pins remain grpc-go **v1.83.2**, Google protobuf **v1.36.11**, Raft **v3.7.0**, bbolt **v1.5.0**. Tiger server `go.mod` and `go.sum` are byte-identical to prior verified pins. The bound harness moves x/net v0.58.0 from direct to indirect in `go.mod` only: all module version pins are identical and `go.sum` is byte-identical; committed harness pin files are retained. Client grpc v1.84.0 is **not** stock server grpc. Reuse dependency source verification only; old measured zero-failure population and old generator metadata are **not this run**.

Prior source conclusions still applicable: fixed explicit etcd/Raft request core **3 sends + 3 selected receives, 2 closes, 2 fresh reply channels**; Ready/scheduler/peers remain symbolic/amortized. Known immediate leader Put **8 new-input stages, including WAL PageWriter staging exactly as previously verified**; CAS pass **8 new + 1 stored-comparison decode**; fail **7 new + 3 stored**. Additional codec coalescing **C** is conditional on actual grpc BufferSlice segmentation (one extra full-input destination if MaterializeToBuffer must coalesce; zero otherwise). Deferred bbolt leaf-page serialization is **separate and excluded from those immediate subtotals**; peers have separate occurrence populations. Do not infer segmentation from the new raw client's intended frames, assume old observed peer batches, or interpret Tiger 4 vs etcd 8 as physical-copy totals/ratio.

**Framing correction to the prior etcd audit:** its extra grpc timeout synchronization close was explicitly conditional on grpc-timeout. Stock `deps/google.golang.org_grpc@v1.83.2/internal/transport/http2_server.go:602-615` encloses creation/callback/close in `if timeoutSet`. New timed raw requests omit that header, so that extra frontend close is absent for them; the core 2 closes above are unchanged. Server-internal request timeout/context and other grpc lifecycle channels remain excluded, not zero. Untimed control grpc Range still has a deadline and is not charged per timed Put/CAS.

### Lean-vs-grpc scope addendum to reused etcd evidence

The old audit's measured grpc-go client emitted grpc-timeout; this raw cohort does not. Its explicit additional deadline-sync close must **not** be imported. The fixed core **3s/3r/2 closes/2 fresh reply channels** are unchanged. Ten-second grpc control probe/cache deadlines remain **outside the cohort**; local raw-client time limits do not transmit server grpc-timeout.

The committed engine can emit the complete sub-4-KiB gRPC message in one DATA frame **when actual credit suffices**, but its loop permits fragments when credit is partial. No unconditional single-frame statement is made. Transport TLS segmentation cannot by itself create additional HTTP/2 DATA frames. **C remains conditional here, not asserted zero**: a C=0 proof would require actual one-buffer codec input through stock recvReader's 5-byte header read, remaining body buffer, parser and MaterializeToBuffer; that is not established for every request in this implemented flow-control path. Do not blindly reuse an old fragmentation assumption or equate TLS records with codec buffers. Put therefore remains **8 immediate + conditional C** in this scope, including WAL PageWriter and excluding deferred bbolt pages; none is a physical-copy total. `lean-etcd-scope.md` records the final scope, superseding the earlier single-frame description.

## Bottom line

**Put: 4→4. CAS: 6→5 new-input stages, both compare outcomes.** PR80 removes only the owning **apply** bytes decode in this comparison; frontend owning decode is restored. CAS failure adds **one separate stored-value reply encode** to either Tiger target. One newly persisted/applied follower: Put 2→2; CAS 3→2; sender encode adds one per actual transmitted occurrence. Fixed request-private channels are zero in both; admission/disk/peer/wake/tick work remains actual-population dependent. These are named-source-stage counts, not performance or physical-copy totals. **Final server and generator/harness bindings are parent-confirmed for Actions run 38397810888.**
