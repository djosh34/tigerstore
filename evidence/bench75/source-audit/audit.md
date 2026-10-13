# Source audit: main, PR75 and PR76 — 1 KiB leader-only Put

## Targets, evidence and limits

Final measurement refs, confirmed by the requester as resolved **2026-10-13 08:23:29Z**, with paired jobs starting **08:23:35Z**:

| Label | Exact commit | Extracted source directory |
|---|---|---|
| M (main) | `e1fd889c345bc3c06d8100eb6d635c88ad6967cd` | `main/` |
| P75 (`work/50-frontend`) | `8fcdd021aea23eca9b9d012f56faee13be39f1fc` | `pr75-latest/` |
| P76 (`work/50-batch`) | `b3e6f6237c6d6a7fb1ad6018efffe8cee6bf4c56` | `pr76/` |

Everything is under `/home/joshazimullah.linux/work_mounts/scratch/bench75-source-audit`. Sources were extracted with `git archive <SHA> | tar -x -C <absolute scratch directory>` from the specified checkout. No checkout writes, branch changes, subagents, Go executions, tests, benchmarks, or GitHub comments were made. `pr75/` preserves the originally requested `042281a5d65da2987b393c5e7d4083b35f9e717f`; its only `internal/` difference from final P75 is `h2_test.go`, so the production-path counts below also apply to that original SHA.

`issue50.json`, `issue50-design.md`, `pr75-latest.json`, `pr76.json`, and `refs.json` preserve the fetched issue/PR metadata. `all-common-source-hashes.json` verifies byte-identical core, codec, storage, state, watch, MVCC, WAL, peer and OSFS files across all four extracted snapshots. M and P75 also have byte-identical `internal/node/node.go`; P76 changes that driver. Relevant installed dependency sources were read and copied into `library-evidence/` without building or downloading anything. `verify-sites.py` checks the census against exact source line arrays; `verified-sites.md` and `verified-sites.json` preserve each checked line, classification and target SHA.

**Scope:** a successful ordinary unary Put submitted to the stable leader, with an exactly 1,024-byte nonempty value, valid nonempty key, no watches, no leases (including the overwritten key), `PrevKv=false`, `IgnoreValue=false`, `IgnoreLease=false`, no planted bugs, no reset/cancellation/error, and an already established HTTP/2 client connection. “Leader-only” means clients target the leader, **not** that replication is disabled. Both insertion and overwrite have the same payload-copy stages; metadata allocations and history growth can differ. Peer counts depend on membership/progress and are reported separately. Work after the client reply, such as a second follower catching up, is not silently assigned to that reply's critical path.

This is a **source census**, not measured channels/op, allocs/op, CPU/op, or an end-to-end performance claim.

## Definitions

- **Channel operation:** one executed successful send and one executed receive count **separately**. A handoff normally costs two operations. A selected receive from a closed channel also counts as a receive, if that exceptional path is being counted. A failed nonblocking send/default branch, an unselected `select` arm, `len(channel)`, and evaluating `ctx.Done()` are **not** operations. A `close` is reported separately, never as a send/receive. Channel creation is also separate.
- **Full value copy:** all 1,024 payload bytes are materialized in a different byte backing store. Serializing a bytes field, decoding a bytes field into owned storage, and copying an entry that contains that field qualify. Fragmented copying into one destination counts as **one full-payload equivalent**, not one per frame. Copying a pointer, interface, slice header, descriptor, revision, or key is not a value copy. A scan for size/checksum is not a copy. Reply bytes do not contain the value in this scope.
- The copy census includes **named direct codec materializations**, even when the actual bytes-copy instruction is in pinned protobuf/grpc/Raft module code. It separately identifies the extra Raft-internal deep clone. It **excludes unenumerated transport/runtime copies**: TLS, socket/kernel/page-cache movement, HTTP/2/HTTP body buffering, bufio copies, and buffer-growth relocation. Those exclusions mean the stated subtotals are **not exact physical memcpy totals**.

## Concise defensible report table

Let `N` be successful Puts in a matched, fully accounted cohort. `A` is the number of P76 admission batches; `G` is the number of completion groups handed to connections. `Q` is the number of persist-queue arrays handed to disk; `F` is the number of disk completion groups. These batch populations are **different**, and none is inferred from concurrency or a configured cap. `Sw/Rw` count actual writer-wake sends/receives; `Sr/Rr` count P76 inflight-room sends/receives. `O/I` count queued-and-consumed outbound peer messages and delivered-and-consumed inbound peer messages on the leader. `T` counts selected leader tick receives.

| Cost, per successful Put unless stated otherwise | M | P75 | P76 |
|---|---:|---:|---:|
| Fixed, non-amortized application request/reply channel operations | **4 = 2 sends + 2 receives** | **4 = 2 sends + 2 receives** | **0** on the batch Put path |
| Admission channel operations | Included in fixed 4 | Included in fixed 4 | **`2A/N`**; one send + receive per admission batch |
| Completion handoff | Included in fixed 4 | Included in fixed 4 | Direct `Complete` call: **0 channel ops**; writer wake below |
| Application writer wake | Inside excluded libraries | **`(Sw+Rw)/N`**, not a fixed 2 | **`(Sw+Rw)/N`**, not a fixed 2 per group or Put |
| P76 inflight-space notification/backpressure | — | — | **`(Sr+Rr)/N`**, conditional/coalesced |
| Disk handoff/completion | **`(2Q+2F)/N`** | Same | Same |
| Leader peer queues/inbox | **`(2O+2I)/N`** | Same | Same |
| Background selected ticker receives | **`T/N`**, not request-fixed | Same | Same |
| Explicit normal-Put channel closes | **0** | **0** | **0** |
| Explicit per-Put reply-channel creation | **1** | **1** | **0** on batch Put path |
| Named direct full-payload copies: frontend decode + entry encode + WAL append + apply decode, plus custom frontend message accumulation | **4** | **5** | **5** |
| Additional pinned Raft-internal full-entry deep clone | **1** | **1** | **1** |
| Subtotal of these known leader payload-copy stages, excluding peers, growth and transport | **5** | **6** | **6** |
| Peer payload copies | Conditional; see peer ledger | Identical | Identical |
| Unenumerated library/runtime/transport copies/channels | **Excluded, not zero** | **Excluded, not zero** | **Excluded, not zero** |

**Do not publish “4/4/0 total channel operations” or “5/6/6 total value copies.”** The first row counts only the fixed request-specific exchanges; the payload subtotal counts only the named materialization stages. The extra visible custom-frontend accumulation copy does **not** establish that P75 physically copies more bytes than M overall: M's removed HTTP/gRPC transport also has excluded buffering/copying.

For matched sends/receives with all counted request/peer/disk handoffs consumed, repository-level leader channel ledgers are:

```
M:   4N + 2Q + 2F + 2O + 2I + T
P75: 4N + 2Q + 2F + 2O + 2I + T + Sw + Rw
P76: 2A + 2Q + 2F + 2O + 2I + T + Sw + Rw + Sr + Rr
```

These are **conditional accounting identities**, not measured totals or an attribution of all background messages to individual Puts. In a time window with queued work at either boundary, count sends and receives individually instead of assuming every handoff has both ends in-window. Control-frame wakes are part of `Sw/Rw` if included; startup, shutdown and other RPCs must be removed or separately reported. Ordinary successful requests do not select context/node-done receive arms.

## Issue50's latest applicable design

Read through `gh issue view 50 --repo djosh34/tigerstore --json title,body,comments`:

1. [Design change: custom client frontend](https://github.com/djosh34/tigerstore/issues/50#issuecomment-6140814920), created 07:44:35Z: own Framer/HPACK HTTP/2 frontend instead of `http.Server` + grpc `ServeHTTP`; frames are intended eventually to feed batch-owned descriptors with wire-form values.
2. [Addendum](https://github.com/djosh34/tigerstore/issues/50#issuecomment-6140846333), created 07:46:34Z: values stored once in the log; index references log storage; batch encoded once and applied in place, without generic per-command protobuf decoding. Counts belong beside benchmark CPU/op.
3. [Correction](https://github.com/djosh34/tigerstore/issues/50#issuecomment-6140902413), created 07:49:59Z: **“Channels are not the problem”**; the target is per-request churn/blocking, and startup-fixed storage versus pooled batch arenas is an experiment. The “stored once” and in-place apply requirements stand. Textual owner-question times differ from comment creation times; the URLs identify the actual statements.

P75's PR body explicitly scopes itself to the custom frontend, retaining one goroutine and `Node.Do` per unary call. P76 explicitly scopes itself to admission/completion batching and defers wire values, in-place apply and allocation strategy. Neither candidate implements the complete addendum. P76 removes the explicit per-Put reply channel/request wrapper/handler goroutine/context, **not** all request objects or allocations: it still constructs a protobuf PutRequest, core pending request, apply PutRequest and PutResponse. Do not turn source `make/new` sites into measured heap allocations/op. Neither candidate proves the absence of hot-path blocking: P76's `Complete` takes a mutex, and `SubmitBatch` may wait for inbox capacity. Those are source facts, not a measured contention claim.

## Channel-site ledger, exact targets

All source citations below are relative to the extracted commit root. M/P75/P76 columns explicitly identify the site in each commit; `—` means it is absent or not executed by this Put path. Alternatives are mutually exclusive and **must not be summed**.

### Request admission and completion

| Executed site | M file:line | P75 file:line | P76 file:line | Count |
|---|---|---|---|---|
| Inbox send in `put` | `internal/node/node.go:122` | `internal/node/node.go:122` | `internal/node/node.go:123` | M/P75: 1/Put; P76: 1/admission batch |
| Inbox receive, outer select OR drain | `internal/node/node.go:264` OR `:269` | `internal/node/node.go:264` OR `:269` | `internal/node/node.go:311` OR `:316` | Exactly one corresponding receive per admitted inbox event |
| Per-request reply send | `internal/node/node.go:301` | `internal/node/node.go:301` | `internal/node/node.go:388` only for retained **Do** path | 1/Put M/P75; **not executed for P76 batch Put** |
| `Do` reply receive | `internal/node/node.go:217` | `internal/node/node.go:217` | `internal/node/node.go:262` only for retained **Do** path | 1/Put M/P75; **not executed for P76 batch Put** |
| Explicit reply-channel creation (not channel op) | `internal/node/node.go:212` | `internal/node/node.go:212` | `internal/node/node.go:257` only for retained **Do** path | M/P75: one fresh channel/Put; P76 batch: zero |
| Grouped completion, ordinary function call | — | — | `internal/node/node.go:400` → `internal/server/batch.go:129` | No channel op |

M/P75 inbox draining groups **processing**, but each request still paid its own send and receive. P76's `internal/server/batch.go:83` calls `SubmitBatch`; `internal/node/node.go:176-179` wraps the batch and calls `put` once. `handle`, `internal/node/node.go:408-415`, submits every op to the core directly. `completeAll`, `internal/node/node.go:377-406`, groups replies by completer before calling `Complete` once per group. Admission groups can split/merge relative to core proposal or completion groups.

### Custom frontend wakes and backpressure

| Site | P75 file:line | P76 file:line | Meaning |
|---|---|---|---|
| Successful writer wake send | `internal/server/h2.go:175` | `internal/server/h2.go:188` | Capacity-one channel, default if already signaled |
| Writer wake receive | `internal/server/write.go:50` | `internal/server/write.go:58` | Only when writer finds no ready frames/control work |
| Completion invokes signal | `internal/server/stream.go:255` | `internal/server/batch.go:135` | P75 once per queued reply; P76 once per completion group |
| Inflight-room send | — | `internal/server/batch.go:122` | Attempt once when each stream's inflight bytes are released; default coalesces |
| Inflight-room receive | — | `internal/server/batch.go:101` | Only while `flush` sees more than 64 MiB unanswered |
| Unary end's streaming-signal attempt | `internal/server/h2.go:444` | `internal/server/h2.go:467` | **0 successful operations**: unary Put's `inSignal` is nil |
| Writer's streaming-space attempt | `internal/server/write.go:112` | `internal/server/write.go:124` | **0 successful operations**: unary Put's `space` is nil |

P75's reply attempts at most one successful wake send each; P76's completion groups attempt at most one each. A writer can already be running and collect replies without a new receive. The token can remain buffered after the reply has been written. For any connection interval, `Rw = Sw + initial_occupancy - final_occupancy`; do not assume one receive per reply/group. With equal endpoint occupancy and no unrelated signals, the aggregate contribution is between zero and two ops per P75 reply, or zero and two per P76 completion group, **not a fixed two**.

P76's room channel is also capacity one. Below the inflight limit throughout an epoch, `Rr=0` and at most **one** room send succeeds per connection, unless a prior token already occupies the channel. It is not one successful send per Put. Under backpressure, receipts allow later sends; actual `Sr/Rr` are conditional. The request-release loop is `internal/server/batch.go:160-170`; the wait checks are `:94-105`.

Other signals that share the writer wake channel, not fixed per Put:

| Cause | P75 file:line | P76 file:line |
|---|---|---|
| GOAWAY | `internal/server/h2.go:164` | `internal/server/h2.go:177` |
| Queued control frame (including ping ack/receive credit) | `internal/server/h2.go:186` | `internal/server/h2.go:199` |
| Settings change/ack | `internal/server/h2.go:279` | `internal/server/h2.go:297` |
| Received send-window update | `internal/server/h2.go:294` | `internal/server/h2.go:312` |
| Reset | `internal/server/h2.go:316` | `internal/server/h2.go:334` |

Connection receive-credit threshold is 4 MiB: P75 `internal/server/h2.go:322-327`, P76 `:340-345`. Its control-frame signal is byte-amortized, not a fixed channel operation per 1 KiB Put. Setup, resets and shutdown are outside the normal scope. Streaming `Recv/Send` paths and their signals do not execute for this Put.

### Disk and timer

| Site | M file:line | P75 file:line | P76 file:line | Count |
|---|---|---|---|---|
| Send array of persist jobs to disk | `internal/node/node.go:282` | `internal/node/node.go:282` | `internal/node/node.go:329` | `Q` sends |
| Disk receives initial queue array OR additional drained array | `internal/node/node.go:369` OR `:376` | Same `:369` OR `:376` | `internal/node/node.go:453` OR `:460` | `Q` receives total, not both for each array |
| Disk sends synced completion group | `internal/node/node.go:390` | `internal/node/node.go:390` | `internal/node/node.go:474` | `F` sends |
| Committer receives synced completion group | `internal/node/node.go:275` | `internal/node/node.go:275` | `internal/node/node.go:322` | `F` receives |
| Background ticker receive | `internal/node/node.go:286` | `internal/node/node.go:286` | `internal/node/node.go:333` | `T` receives; library timer send excluded |

Disk worker drain merges queue arrays, so generally `Q` and `F` differ. Even one core Raft batch can generate further hard-state/commit-only jobs. If a specially bounded case has one queue array and one sync completion for `B` writes, this term is `4/B`; **that is not a general result**. Hard-state-only work does not copy the value but still uses disk handoffs. Apply's `LocalApplyThread` is a Raft message handled synchronously by the core, not another Go channel.

### Peer transport (unchanged code, P76 driver line shifts)

| Site | M file:line | P75 file:line | P76 file:line | Meaning |
|---|---|---|---|---|
| Queue peer message | `internal/peer/peer.go:294` | `internal/peer/peer.go:294` | `internal/peer/peer.go:294` | One successful send per queued message, either data/control |
| Sender takes message | `internal/peer/peer.go:305` OR `:311` OR `:314` | Same sites | Same sites | One receive per message, **not all alternatives** |
| Received peer message → node inbox | `internal/node/node.go:197` | `internal/node/node.go:197` | `internal/node/node.go:242` | One send per delivered message |
| Committer takes inbound message | `internal/node/node.go:264` OR `:269` | Same sites | `internal/node/node.go:311` OR `:316` | One receive per inbound event |
| Unreachable callback event | `internal/node/node.go:205` | `internal/node/node.go:205` | `internal/node/node.go:250` | Conditional nonblocking inbox send on drop/failure; not ordinary successful transport |

On the leader, each fully consumed outbound message costs two application channel operations and each fully consumed inbound message costs two. Across both endpoints a delivered directed message costs four. Message counts are **not request counts**: an append may carry multiple entries, each entry multiple Puts; acknowledgments/empty append/commit propagation/heartbeats have no value but still have channel handoffs. Fanout can reach all other voters, while client success needs a quorum rather than all followers. Slow/probing followers, inflight limits, retransmissions and queue drops change message counts. There is no defensible fixed `messages/Put` total here.

Failure-only peer receives are `internal/peer/peer.go:328` or `:330` (draining dropped queue items), `:347` (backoff timer), `:348` and `:317` (context cancellation), in **all three commits**. Drop reports use the conditional unreachable inbox event above, followed by one of the normal inbox receive sites if consumed. They must be a separate fault ledger, not included in a normal-success fixed count.

### Closes and cancellation exclusions

There is **no explicit application channel close per ordinary successful Put**. Node shutdown closes `n.done` at M/P75 `internal/node/node.go:239`, P76 `:284`; custom-server Stop closes a temporary wait channel at P75/P76 `internal/server/server.go:189`. Watch closes at M/P75 `internal/node/node.go:320`, P76 `:362` are outside the no-watch scope. Closing a socket (`c.close`/`raw.Close`) is not closing a Go channel.

P75 creates a per-call context at `internal/server/h2.go:388` or `:390`, and calls `st.cancel` at `internal/server/stream.go:276`. Context's lazy done-channel allocation/close is **library-internal and excluded from the application close count**. M has analogous grpc/HTTP-library lifecycle channels, also excluded. P76 Put avoids this per-stream context: `internal/server/h2.go:416-423` only constructs one for HTTP and non-`op` methods. Node and connection cancellation remain shared lifecycles. P76 `internal/server/write.go:37` obtains `c.s.n.Done()`; this is **not** a channel operation. Its selected receive at `:60` is shutdown-only. Selected cancellation/node-done arms belong to exceptional/shutdown paths, not the stated successful path.

## Full-value-copy ledger

Dependencies pinned identically by all targets: etcd API **v3.7.2**, raft **v3.7.0**, protobuf **v1.36.11**, grpc-go **v1.84.0**, x/net **v0.58.0**. They are module sources, **not files in the audited git commits**. Module-relative paths below can be inspected under the named matching `library-evidence/` directory; original cache roots were `/home/joshazimullah.linux/go/pkg/mod/` plus their module/version names.

| Payload materialization site | M source | P75 source | P76 source | Full payload equivalents |
|---|---|---|---|---:|
| Custom HTTP/2 DATA → owned protobuf message buffer | Not repository code; HTTP/gRPC buffering excluded | `internal/server/stream.go:100` | `internal/server/stream.go:103` | M unenumerated transport; P75/P76 **1** |
| Client-wire protobuf → fresh `PutRequest.Value` | Library decode described below; repo dispatch `internal/server/server.go:159-160` | `internal/server/rpc.go:59`, called at `:71` | `internal/server/rpc.go:62`, called from `internal/server/batch.go:43` | **1 each** |
| `PutRequest.Value` → encoded Raft-entry batch | `internal/core/codec.go:64` | `internal/core/codec.go:64` | `internal/core/codec.go:64` | **1 each**, not `1/B` |
| Proposal entry → Raft-owned deep clone | Repo call `internal/core/node.go:522`; module `raft.go:817` | Same | Same | **1 each**, library-internal but verified separately |
| Entry data → framed WAL buffer | `internal/wal/wal.go:351`, reached at `:368` | Same | Same | **1 each** |
| Committed wire request → apply `PutRequest.Value` | `internal/core/codec.go:114`; new Put struct at `:96` | Same | Same | **1 each** |
| Apply value → MVCC version/index | `internal/mvcc/ops.go:235`, `:227`, `:256` | Same | Same | **0**; slice/header retained |
| Ordinary PutResponse encode/send | Repo handler return; library response encode | `internal/server/stream.go:179`, `:188` | `internal/server/batch.go:153`; `internal/server/stream.go:182` | **0 value copies**; no PrevKv/value in reply |

### Module proof of copies, including main's frontend decode

**Main client-wire decode is a real full-value copy, not an assumed zero because it is outside the repository.** TLS request routing calls `s.g.ServeHTTP` at M `internal/server/http.go:29`. The generated etcd API adapter is `go.etcd.io/etcd/api/v3@v3.7.2/etcdserverpb/rpc_grpc.pb.go:246-252`: it allocates a fresh PutRequest, runs the decoder at `:248`, then invokes KV.Put at `:252`. grpc's default protobuf codec calls `proto.Unmarshal` at `google.golang.org/grpc@v1.84.0/encoding/proto/proto.go:96`. PutRequest's field is a proto3 byte slice (`go.etcd.io/etcd/api/v3@v3.7.2/etcdserverpb/rpc.pb.go:760`). For a nonempty proto3 bytes field, `google.golang.org/protobuf@v1.36.11/internal/impl/codec_gen.go:5489-5497` consumes the wire bytes and executes `append(([]byte)(nil), v...)` at **`:5497`**. Thus the value is not aliased to the input wire buffer. The same bytes-field decoder backs P75/P76 client decode and all replicas' apply decode.

Encoding a bytes field uses protobuf `internal/impl/codec_gen.go:5477-5484` (`appendBytesNoZero`), calling `encoding/protowire/wire.go:454-455`, whose `AppendBytes` appends all bytes to the output. This substantiates the repository's entry encode and peer-message encode boundaries.

Raft `go.etcd.io/raft/v3@v3.7.0/rawnode.go:90-95` initially passes `Data: data` into a proposal without a payload copy. **But** leader `raft.go:812-819` calls `proto.Clone` at **`:817`**, copying the whole entry including `Data`. The proposal invokes `appendEntry` at `raft.go:1349`. Raft Entry is **proto2**, not PutRequest's proto3: `raftpb/raft.proto:1` and the `Entry.Data` byte field at `raftpb/raft.pb.go:348`. Protobuf's proto2 bytes merge is `internal/impl/merge.go:187-188`, executing `append(emptyBuf[:], *src.Bytes()...)` at **`:188`**. (`mergeBytesNoZero` at `:191-195` is the analogous proto3 helper, not the helper cited for Entry.Data.) Thus one clone copies all entry bytes, including this Put's 1,024 value bytes, into independent storage. Therefore “RawNode.Propose only keeps the slice” is not an accurate statement about the full leader proposal path. Batch cloning is once per entry, but each Put's 1 KiB is copied once: payload bytes do not amortize as `1/B`.

MemoryStorage append is different: repository `internal/core/storage.go:39` calls the library; raft `storage.go:293-326`, specifically `:318` and `:320`, appends **entry pointers**. It does not copy `Entry.Data`. The storage lock is real library synchronization, but it is not a repository channel operation or a full payload copy.

### Peer full-payload copies (all three commits)

For **one transmitted occurrence** of an entry containing this Put that a follower receives, persists and applies normally:

| Site | M file:line | P75 file:line | P76 file:line | Payload copies |
|---|---|---|---|---:|
| Sender marshals raft Message/Entry.Data into peer wire buffer | `internal/peer/peer.go:416` | Same | Same | **1 on leader**, per transmitted entry occurrence per recipient |
| Receiver unmarshals Message/Entry.Data into owned bytes | `internal/peer/peer.go:238` | Same | Same | **1 on receiving replica** |
| Receiver appends Entry.Data to WAL buffer | `internal/wal/wal.go:351`, `:368` | Same | Same | **1 on receiving replica**, if appended |
| Receiver apply decodes PutRequest.Value | `internal/core/codec.go:114` | Same | Same | **1 on receiving replica**, when applied |

The receiver's Entry.Data is proto2; its decoder copies the bytes at `protobuf@v1.36.11/internal/impl/codec_gen.go:5418`, distinct from the PutRequest proto3 decoder at `:5497`. These are **four named stages across sender + follower**, three on the follower, for that bounded single-delivery/persist/apply case. They are not four per client Put independent of fanout. Raft append message construction itself aliases entry pointers (`raft@v3.7.0/raft.go:648-658`, `Entries: ents` at `:656`). Entry-bearing send multiplicity is conditional; a duplicate received entry need not be persisted or applied again. Acks/heartbeats/empty appends do not carry this value. Their protobuf buffers still have allocation/copy cost, but not a copy of this 1 KiB payload. No follower re-encoding via `encodeBatch` occurs in this path.

### Growth, physical transport and conditional codec copies not totaled

- Custom frontend accumulation preallocates the full message capacity (P75 `internal/server/stream.go:97`, P76 `:100`), so fragmented DATA accumulation does not grow that message buffer. Each append copies only the next chunk: over the one complete protobuf request, every one of the 1,024 value bytes is copied exactly once into this destination. Total accumulated message bytes also include key/protobuf metadata; the request buffer is **not** an exactly 1 KiB buffer. The five-byte envelope copy (P75 `:84`, P76 `:87`) is **not** a value copy.
- Batch encoding starts with a small growing slice (`internal/core/codec.go:35`, `:58-64` in all three commits). Appending later requests/framing can relocate earlier requests' already encoded payloads. WAL record framing similarly grows `w.buf` at `internal/wal/wal.go:348-351`. These **additional** full/partial payload relocations depend on batch order, capacity and prior buffer history; the direct one-copy-per-materialization census does not count them as a fixed extra value copy. Peer MarshalAppend output reuse/growth is at `internal/peer/peer.go:416` in all commits.
- Main grpc's input `MaterializeToBuffer` at `grpc@v1.84.0/encoding/proto/proto.go:91` aliases one constituent buffer, but copies a multi-buffer input: `grpc@v1.84.0/mem/buffer_slice.go:105-116`, `CopyTo` at `:115`. This is an extra **conditional** library materialization, separate from the always-copying protobuf field decode. It is not included as an unconditional frontend copy.
- Peer framed I/O uses `internal/peer/peer.go:234` (`ReadFull`), `:424` (write length) and `:427` (write message) in all commits. Client response buffered writes are P75 `internal/server/write.go:71-72`, P76 `:83-84`. bufio/TLS/socket and kernel copies behind these calls are excluded, not claimed absent.
- WAL flush writes the buffer at `internal/wal/wal.go:386` and syncs at `:416`, all three commits. These are I/O, not another enumerated userspace backing-store copy. Rotation adds header write/sync/directory sync at `:325`, `:328`, `:334`, but no copy of this Put's value. Recovery/read-all at startup is outside the successful online Put scope.

## End-to-end trace

### Main frontend → Node.Do

M `internal/server/server.go:95` runs `http.Server.ServeTLS`; HTTP/2 gRPC is routed by `internal/server/http.go:27-29` to grpc ServeHTTP. The module-generated Put adapter decodes a fresh PutRequest, including the value copy proved above. M `internal/server/server.go:159-160` calls the generic adapter; `:129` calls `Node.Do`. M/P75 `internal/node/node.go:212-217` creates request/reply channel, sends through `put`, and awaits reply. There is no generic request clone when entering the core.

### PR75 frontend → Node.Do

P75 `internal/server/server.go:126-150` negotiates TLS/h2 and constructs the connection; `internal/server/h2.go:215` reads frames. `onData` at `:425` hands DATA to `receive`; `internal/server/stream.go:100` copies into owned wire message storage, and `:113-117` records unary body. End-of-request at `internal/server/h2.go:452` starts a handler goroutine. `internal/server/stream.go:285` calls the method, `internal/server/rpc.go:27` maps Put to `nodeCall`; `:70-74` allocates/decodes the request and calls Node.Do. After core completion, `internal/server/stream.go:290` replies, `:179` marshals the small response, `:253-255` lists/signals output. Writer `internal/server/write.go:42` takes it, `:71-72` writes frame header/body, `:77` flushes; `:162-168` ends the HTTP/2 stream. This stream end is not a Go-channel close.

### PR76 frontend → SubmitBatch (not Node.Do for Put)

P76 `internal/server/h2.go:228` reads frames and `:239-240` flushes due batches. DATA is accumulated at `internal/server/stream.go:103`. Put maps to `newOp` at `internal/server/rpc.go:30`; `internal/server/h2.go:474-475` calls `admit` synchronously on the connection reader, not a per-Put goroutine. `internal/server/batch.go:42-55` creates/decodes the op, clears body, tracks inflight bytes and adds op/tag descriptors. `:61-74` tests batch due: 256 ops, 4 MiB, or next frame not fully buffered; no timer. `:83` calls `SubmitBatch`, driver `internal/node/node.go:176-179` sends one batch event. The committer receives it at `:311` or `:316` and `:408-415` directly submits each request to the unchanged core. **Node.Do survives at `:256-269` but is not the ordinary Put path.**

After Process, `internal/node/node.go:345` calls `completeAll`; `:400` calls one Complete per connection group. P76 `internal/server/batch.go:129-135` appends completion batches and signals; writer `internal/server/write.go:40-44` takes and encodes them. `internal/server/batch.go:153` marshals the response, `:160-170` releases inflight bytes and calls `pushLocked` without a per-reply writer wake. P76 `internal/server/write.go:50`, `:83-84`, `:89` takes/writes/flushes frames; `:174-180` ends the stream. One reader batch is not guaranteed to equal one completion batch, disk group or Raft entry.

### Unchanged core → Raft → WAL → apply/index → reply

The following identical citations apply **at exactly the same file:line in M, P75 and P76** (verified hashes):

1. Driver core submission enters `internal/core/node.go:358`; validation attaches request pointers for size checking (`:415-442`), not payload copies. `queueWrite`, `:465-472`, sizes and queues the op. Ordinary Put does not execute the LeaseGrant clone at `:391`.
2. `Process`, `internal/core/node.go:591-604`, uses append-clocked proposal gating. `propose` calls `encodeBatch` at `:517`, then RawNode.Propose at `:522`. The bytes field is encoded once into that entry, followed by the separate library deep clone described above.
3. Raft asynchronous storage writes are enabled at `internal/core/node.go:223`. `handleReady` interprets local append messages at `:667-685`, producing `PersistJob`s. Outbound messages go through `send`, `:877-886`, or are held while a vote is unsynced. **Raft messages, effects slices and LocalAppendThread/LocalApplyThread IDs are not Go channel operations.** Library local append message contains entry references (`raft@v3.7.0/rawnode.go:223-229`); local apply message does likewise (`:372-379`).
4. Driver hands queue arrays to the disk worker. M/P75 `internal/node/node.go:383`, P76 `:467`, calls WAL.Append. `internal/wal/wal.go:363-368` frames entry metadata and appends Entry.Data through the copying `record` site at `:351`. M/P75 driver `:386`, P76 `:470`, calls WAL.Sync. Flush/write/sync and optional rotation execute as cited above. Disk completion handoffs are in the channel ledger.
5. Durable notification calls `internal/core/node.go:553` into storage; `internal/core/storage.go:39` retains entry pointers, then hard-state metadata is updated if present. `internal/core/node.go:571` delivers the append message's responses; `:581` steps self-directed messages directly, while others become peer effects. Quorum/self-durability progress allows committed entries to be applied; exact Ready/message/job count depends on Raft state.
6. Local apply at `internal/core/node.go:688-693` calls apply. `:790` calls DecodeBatch; `internal/core/codec.go:96` constructs a fresh PutRequest and `:114` generically protobuf-decodes it. `internal/core/node.go:818` dispatches State.Apply; `internal/core/state.go:113` calls MVCC.Put. `internal/mvcc/ops.go:235` stores `p.Value` as a slice reference in the version; `:227` appends metadata and that slice header. This is **a reference to decoded apply-owned bytes**, not `(segment, offset, length)` into WAL/wire data. Generic apply decoding already created the independent payload copy. The store's comment suggesting values are usually Raft-entry slices (`internal/mvcc/store.go:5-6`) does not override the actual protobuf decoder's copy behavior.
7. With no watchers, `internal/core/watch.go:272` exits watch matching. No watch value encoding or channel sends occur. Lease tracking in `internal/core/state.go:91` converts only the key, not the value.
8. `internal/mvcc/ops.go:233` constructs PutResponse/header without PrevKv; `internal/core/node.go:826` fills header. `:798-800` matches origin/boot/request ID and finishes; `:505` queues reply metadata. The driver then follows M/P75 reply-channel or P76 grouped-completion path above. An ordinary reply does not copy the 1 KiB payload back to the client.

### Peers and quorum

All three commits dispatch peer effects via driver M/P75 `internal/node/node.go:296`, P76 `:343`. `internal/peer/peer.go:137` calls sender.send; `:294` queues the pointer, writer `:416` serializes it. Receiver `:238` decodes the message and `:244` delivers it. Driver M/P75 `internal/node/node.go:347-350`, P76 `:430-433`, steps it into the core. Followers reuse the same WAL/storage/apply code, but do not create a client reply-channel or leader proposal encode/clone for this received append. Their append response is released through persisted responses, preserving durable-ack ordering. Reverse acknowledgments use the same peer queue and inbox exchanges without copying this value.

Leader success therefore cannot justify charging exactly two followers, one acknowledgment each, one disk job or one sync to every Put. Fanout batching, commit propagation, background heartbeats, Raft progress and which work lies before/after the response are all conditional. A fixed **scoped** subtotal plus the explicit batch/message ledgers is the source-supported conclusion.

## Bottom line

- **M and P75:** four fixed application channel ops per Put remain; P75 moves frontend work into repository code and introduces conditional/coalesced writer wakes. Core/storage/peer costs are unchanged.
- **P76:** no per-Put `Node.Do` request/reply-channel exchange; admission costs two ops per actual batch, completion is a direct grouped call, with conditional wake/room signals. This is not zero total channels/op, and actual batch sizes are not established by source.
- **Value storage/apply:** unchanged across candidates. Known leader materializations are 4/5/5 direct stages plus one verified Raft clone, excluding conditional transport/growth/peer work. All still generically decode each applied Put and retain its decoded value separately from wire/log storage. Neither is “stored once/applied in place.”
- Publish measured allocations, GC/retained bytes, CPU and actual batch/channel counters separately. This audit supplies source-defined boundaries and sites, not replacement benchmark observations.
