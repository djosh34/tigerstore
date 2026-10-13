#!/usr/bin/env python3
from pathlib import Path
import json,datetime,hashlib
ROOT=Path('/home/joshazimullah.linux/work_mounts/scratch/bench80-lean-source-audit')
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
b=json.loads((ROOT/'binding.json').read_text());w=json.loads((ROOT/'workload-manifest.json').read_text())
rows=json.loads((ROOT/'verified-sites.json').read_text());sites={(r['target'],r['id']):r for r in rows}
def site(id,t='main'):
 r=sites[t,id];return f"`{r['file']}:{r['line']}`"
def both(id):
 a=site(id);z=site(id,'pr80');return a if a==z else f'M {a}; P80 {z}'
def pair(ids):return ' + '.join(both(x) for x in ids)
worksites={r['id']:r for r in json.loads((ROOT/'workload-sites.json').read_text())}
def work(id):
 r=worksites[id];return f"`{r['file']}:{r['line']}`"
framing_observation='The committed engine slices DATA by actual connection/stream send credit and maxFrame at '+work('data-framing')+'. A short request can fit one DATA frame when credit suffices, but partial credit can fragment it; no unconditional one-frame claim or C=0 follows.'
status=b['status']
final=b.get('status')=='FINAL'
text=f'''# PR80 lean-generator follow-up: Put / revision-CAS source audit

## Binding and controls

**{status}.** {'Parent-confirmed immutable server and generator SHAs; no later branch heads substituted.' if final else 'These are provisional source observations, NOT the final Actions measurement binding. Await parent-supplied immutable resolve SHAs; do not publish as a final audit.'}

Actions binding: run **{b.get('actions_run','pending')}**, resolve **{b.get('resolved_utc','pending')}** ([run](https://github.com/djosh34/tigerstore/actions/runs/{b.get('actions_run','pending')})).

| Target | Exact source commit |
|---|---|
| M — main | `{b['targets']['main']}` |
| P80 — PR80 | `{b['targets']['pr80']}` |
| Stock etcd v3.7.2 (unchanged release) | `68c065e562994b89e333e77b039ad066f933c586` |

Requested paired run: **Put256 and CAS256, five repetitions**, main vs PR80 and stock etcd, new lean raw HTTP/2 generator. No result, pass/failure population, or performance claim is inferred here. Generator status: **{w['status']}**; current observed engine path `{w['engine_path']}`. Server and generator bindings are distinct. Generator/harness exact commit **`{json.loads((ROOT/'generator-binding.json').read_text())['commit']}`**, parent-confirmed for this run. No mutable-source substitution. No profiles requested; benchmarks are ARM-only and run by the parent/workflow, not this source audit. Actual generator CPU and other runtime results are reported separately by the parent.

All writes confined to `/home/joshazimullah.linux/work_mounts/scratch/bench80-lean-source-audit`. Supplied repository read-only. No subagents, GitHub writes, Go execution, builds, benchmarks or tests. Deadline `2026-10-13T22:00:00Z`. Source-only Python asserts exact sites and byte equality to selected `git show <SHA>:<path>` inputs. No whole module ZIPs, whole-repo export, or bare object repository copied. `verified-sites.md/json` binds every listed site to its target commit, exact text and file SHA256; `source-file-hashes.json`, `reference-manifest.json`, `workload-manifest.json`, `SHA256SUMS` provide small integrity manifests.

Starting audit retained by reference: `evidence/bench75/run-38387706444/source-audit/audit.md`. Its old 6→4 CAS conclusion is **superseded for these refs**, not inherited. Main includes merged **#79 HPACK and batch-owned pending array**, sites {both('hpack-decode')} / {both('batch-pending-allocation')}; these change headers/bookkeeping, not full-value payload stages. Selected current production source/diff was reviewed; unchanged exact source spans were relocated and asserted, not old line numbers blindly reused. The final main is **aa9d9b6**, not preliminary **549e0c9**. Their complete diff changes only `internal/peer/peer_test.go` and `internal/peer/stall_test.go`; production code/dependency pins are unchanged. The earlier peer-listener setup change shifts peer sites +7 lines but does not add timed value/channel boundaries. The separate new read-clock branch is **not this run** and contributes nothing to the formulas.

## Scope and meaning

Established leader-targeted unary requests in a replicated three-node cluster; existing keys; canonical protobuf; no compression, lease, watches, PrevKv, IgnoreValue/IgnoreLease, auth, deadline firing, faults, restart or recovery. Put proposes one 1,024-byte value. CAS compares **EQUAL MOD** to a positive cached revision; Success = one Put; Failure = current point Range with the stored value, not KeysOnly or CountOnly. A stale revision is a compare failure, not an RPC error. Cache seeding/probes/warm requests and response validation are outside the server stages of a timed request.

A named full-value stage materializes all of that payload in another byte backing store. Chunks into one destination count once; caller and implementing codec instruction count once. Structs, interfaces, slice headers, entry pointers, keys, sizing/checksum scans and metadata are not value copies. **No physical-copy totals or ratios**: TLS/Framer/bufio/socket/kernel/page-cache buffers, runtime growth relocation and unenumerated lifecycle channels are excluded, **not zero**. Stages do not establish allocations/op, GC, CPU, syscalls, throughput or durability. No request-count-derived peer fanout, batching or sync multiplicity is assumed.

## 1. Leader: new input payload

Numbers apply to either CAS comparison result; the entire Txn is logged before comparison.

| Named full-new-value stage | M Put | P80 Put | M CAS | P80 CAS | Exact site(s) |
|---|---:|---:|---:|---:|---|
| Framer DATA → owned request accumulation | 1 | 1 | 1 | 1 | {both('frontend-accumulation')} |
| Frontend owning nested Put.Value decode | 0 | 0 | 1 | 1 | CAS caller {both('owning-frontend-caller')} → helper {both('frontend-decode')} → {both('lib-proto3-decode-copy')} |
| Request → encoded Raft entry | 1 | 1 | 1 | 1 | Put {both('entry-value-encode')}; M CAS {site('generic-entry-value-encode')}; P80 CAS {site('txn-entry-copy','pr80')} |
| Retained Raft proto2 Entry.Data deep clone | 1 | 1 | 1 | 1 | proposal {both('raft-propose')} → {both('lib-raft-clone')} → {both('lib-proto2-clone-copy')} |
| Entry.Data → WAL staging | 1 | 1 | 1 | 1 | {both('wal-value-caller')} → {both('wal-value-copy')} (one boundary) |
| Apply owning nested Put.Value decode | 0 | 0 | 1 | 0 | M CAS {site('generic-apply-still-exists')} → {site('lib-proto3-decode-copy')}; P80 aliases at {site('alias-apply','pr80')} |
| Index value assignment | 0 | 0 | 0 | 0 | retained slice header {both('index-value')} |
| Ordinary successful reply: new input value absent | 0 | 0 | 0 | 0 | Put {both('wire-reply-encode')}; CAS {both('batch-reply-encode')} |
| **Known leader new-input subtotal, peers excluded** | **4** | **4** | **6** | **5** | named source boundaries, not physical totals |

Frontend request-size reserve is {both('frontend-prealloc')}; entry reserve {both('entry-buffer-init')}, P80 wire sizing {site('txn-wire-size','pr80')}. This prevents a fixed extra growth stage at those specific boundaries; it does not imply no transport/growth copies elsewhere. Put frontend ParsePut {both('wire-frontend-parse')} and apply {both('apply-wire-parse')} alias input/entry slices; Value alias {both('wire-value-alias')}, wire byte subslice {both('lib-bytes-consume-alias')}. Storage {both('storage-pointer-append')} and MVCC version array {both('index-version-append')} retain pointers/metadata, not new value bytes. No-watch fast path {both('no-watch-fastpath')} excludes event encodes in scope.

**PR80 frontend reset verified:** its generic admission now calls `unmarshal` → `proto.Unmarshal`, not `core.UnmarshalAlias`. It still sets `Request.Wire = st.body` at {site('frontend-new-op','pr80')} and appends the original complete Txn body to the entry. The temporary frontend-decoded value is therefore an owning materialization even though it is not used to re-encode the entry. The `Request.Wire` comment at {site('request-wire-field','pr80')} still says decoded with UnmarshalAlias: that comment is stale for this frontend. **6→5, not 6→4.** Apply still uses UnmarshalAlias and successfully indexed CAS bytes alias Raft-owned Entry.Data. Main CAS index instead retains apply-decoded bytes.

Alias apply is still decoding: fresh Txn struct {both('new-txn-op')}; P80 repeated/singular submessage construction {site('alias-nested-list','pr80')} / {site('alias-nested-message','pr80')}; capped bytes {site('alias-value','pr80')} and field setter {site('alias-set','pr80')}. Protobuf reflection Set → scalar setter → bytes converter {pair(['reflect-set','reflect-scalar-set','reflect-bytes-convert'])} retains the backing store, not a full-value clone. Unknown fields can be copied at {site('alias-unknown','pr80')}; canonical scoped requests have no such full-value unknown field. This proves bytes ownership, not zero request objects/allocations, never-decoded execution, or complete arbitrary-wire compatibility.

## 2. CAS: branch selection and old stored payload

Both Success and Failure are inspected before compare by {both('txn-readonly')}; detecting Success Put at {both('txn-write-detect')} selects write/Raft routing {both('txn-write-route')}. The full Txn, including unexecuted Success Put.Value, is encoded, cloned, WAL-staged and decoded/walked on replicas **regardless of compare outcome**. M's owning apply decode copies that new value even on failure; P80's alias apply does not. Compare path {both('compare-path')} runs at replicated apply through {both('state-txn')}; only the chosen branch executes.

| Existing stored-value stage on leader | CAS pass | CAS fail | Both-target site |
|---|---:|---:|---|
| Construct existing KV / MOD integer compare | 0 | 0 | {both('compare-kv')}, {both('compare-mod')}, slice alias {both('kv-value-alias')} |
| Failure.Range KV construction | absent | 0 | Range execution {both('txn-range')} → {both('range-kv')} |
| Encode returned stored value in TxnResponse | absent | **1** | {both('batch-reply-encode')} → {both('response-proto-encode')} → bytes append {both('lib-bytes-encode-copy')} |

That conditional **one reply copy is of the old stored value**, not the unexecuted new input value; historical write/seed copies are not charged again. Successful Put branch {both('txn-put')}; Succeeded flag {both('txn-success')}; actual mutation controls revision {both('revision-changed')} / {both('txn-revision')}. Failure leaves MVCC revision unchanged while the Raft log advances. Followers may build Range responses but do not serialize this leader client's reply. Failure stored value is not re-replicated by this Txn.

For C completed timed CAS calls and actual f compare failures: known leader stages average **M: 6 new input + f/C returned stored**; **P80: 5 new input + f/C returned stored**. These are two payload identities. Use this run's actual f; **the previous audit's zero failures are not evidence for this run**. RPC errors require separate accounting; no retries or timed client pre-reads are silently added.

## 3. Peer/follower new-input stages: per occurrence, not a batch assumption

| Named stage on one newly received, persisted and applied follower | Put M/P80 | CAS M | CAS P80 | Exact site |
|---|---:|---:|---:|---|
| Peer owning proto2 Entry.Data decode | 1 | 1 | 1 | {both('peer-value-decode')} → {both('lib-proto2-decode-copy')} |
| Newly persisted entry WAL staging | 1 | 1 | 1 | {both('wal-value-caller')} → {both('wal-value-copy')} |
| Apply owning nested Put.Value decode | 0 | 1 | 0 | M {site('generic-apply-still-exists')}; P80 {site('alias-apply','pr80')}; Put ParsePut {both('apply-wire-parse')} |
| **Follower new-input subtotal** | **2** | **3** | **2** | same for CAS pass/fail |
| Sender Entry.Data encode, separate transmitted occurrence | **+1** | **+1** | **+1** | {both('peer-value-encode')} → bytes append {both('lib-bytes-encode-copy')} |
| **One sender occurrence + that new follower entry** | **3** | **4** | **3** | not a cluster-wide per-request total |

Follower MsgApp path {both('follower-maybeappend')} → {both('follower-logappend')} → {both('unstable-append')} retains entry pointers; it does **not** run leader proposal appendEntry's deep clone. Payload bytes cannot amortize as 1/batch-size: each contained value is present in each encoded/copied entry. Retransmit/duplicate decode, recipient fanout, catch-up and newly persisted/apply occurrences differ. Acks and heartbeats may pay channels with no input value. Client quorum success does not imply all followers finished. No assumed peer batch sizes, exactly two deliveries, or one fsync/request.

## 4. Repository channels: same main/PR80 Put/CAS topology

Count successful send and selected receive separately; close and creation separately. Default/unselected arms, nil-channel attempts, Done() access, atomic operations and synchronous method calls are not handoffs. Let N be a fully consumed request cohort; A actual admission batches; Q actual persist-queue arrays; F actual worker sync-completion groups; O/I outbound queued-and-consumed / inbound delivered-and-consumed peer messages at this node; U successful nonblocking Unreachable reports consumed; Sw/Rw successful writer wake sends / selected receives; Ticks selected core tick receives. These populations are distinct and are **not observed batch sizes**.

| Population / classification | Sends/N | Receives/N | Normal closes | Exact counted site(s), both targets |
|---|---:|---:|---:|---|
| Fixed request-private inbox/reply | **0** | **0** | **0** | SubmitBatch route {both('batch-submit-call')}; retained Do sites excluded below |
| A — amortized admission | A/N | A/N | 0 | {both('admission-send')}; receive alternatives {both('inbox-receive-outer')} / {both('inbox-receive-drain')} |
| Completion — direct call/atomic list | 0 | 0 | 0 | {pair(['completion-direct','completion-cas','completion-swap'])} |
| Q — amortized persist queue | Q/N | Q/N | 0 | {both('disk-send')}; alternatives {both('disk-receive-outer')} / {both('disk-receive-drain')} |
| F — actual synced worker group | F/N | F/N | 0 | {both('disk-complete-send')} / {both('disk-complete-receive')} |
| O — outgoing peer queue, conditional protocol population | O/N | O/N | 0 | {both('peer-queue-send')}; alternatives {pair(['peer-queue-receive-fast','peer-queue-receive-control','peer-queue-receive-data'])} |
| I — inbound peer → inbox | I/N | I/N | 0 | {both('peer-inbox-send')}; same inbox receive alternatives |
| U — successful conditional Unreachable report | U/N | U/N | 0 | {both('unreachable-send')}; same inbox receive alternatives |
| Coalesced frontend wake | Sw/N | Rw/N | 0 | {both('writer-wake-send')} / {both('writer-wake-receive')} |
| Background clock tick | 0 | Ticks/N | 0 | {both('tick-receive')} |

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

Both use SubmitAll {both('batch-core-submit')} with one batch-owned pending array {both('batch-pending-allocation')} and direct per-record calls {both('submitall-loop')}. Descriptors/waiter tags {pair(['opbatch-owner','driver-batch-owner','batch-wrapper','batch-waiter'])} are no private reply channel. RawNode async append/apply message IDs are synchronous core messages, not Go channels. Disk worker drains Q arrays and Syncs {both('wal-sync-call')} before sending F; Q, F and requests are not interchangeable.

Every received DATA frame credits and attempts writer signal at {both('credit-wake-attempt')}; completion attempts at {both('completion-wake-attempt')}; other control/window/setup signals share capacity-one wake. Thus wake counts are conditional/coalesced, not a fixed pair per RPC or completion group. For a connection interval `Rw = Sw + initial_wake_occupancy - final_wake_occupancy`. Unary delivery {both('unary-delivery')} returns before streaming body-signal {both('unary-nil-body-signal')}; unary inSignal/space remain nil (streaming-only allocation {both('stream-channel-guard')}). End-input {both('unary-nil-signal')} and writer-space {both('unary-nil-space')} nonblocking nil sends select default: zero.

Retained Node.Do creation/send/receive {pair(['do-reply-create','do-reply-send','do-reply-receive'])} is **not this route**. Shutdown/watch/server-stop closes {pair(['shutdown-close','watch-close','server-stop-close'])}, streaming operations, faults/backoff, cancellation and library/runtime/context lifecycle channels are excluded, not asserted nonexistent. Timed lean requests send no grpc-timeout, so guard {both('timeout-guard')} does not arm {both('stream-deadline')}; node tick-based request timeout remains. Control grpc Range deadlines are separate untimed work.

## 5. Lean framing / workload observations

The raw engine derives from `exp/pre-47-grpc`, immutable commit `d2408301966b99471bfed06d4c2dc1cd1e5af462`, `experiments/grpc/lean.go`, SHA256 `9553545389d7ae3d246af1ecf03acc26b7a3f167a7f33883c046054c6ed1f07e` (independently recomputed). Only that base file is retained; actual engine/integration snapshot hashes are in `workload-manifest.json`.

Observed/requested contract: 4,096 Kubernetes-shaped keys; global permutation `i*2654435761%4096`; independently SHA256-derived 1 KiB constant value per key; 16 active TLS 1.3 mTLS raw h2 connections, 256 total outstanding closed-loop slots; one idle grpc-go connection used for untimed control probes/cache Range. Raw connections complete SETTINGS and **one untimed valid RPC each before the before revision probe and CPU/IO/heap snapshots**. Startup Put/CAS writes are outside N and revision fencing. CAS cache uses positive shared atomic revisions, monotonically updated even for out-of-order replies. Pass returns revision from Put; failure Range validates exact key/value and positive ModRevision. No timed pre-read or retry.

Put/compare/branch templates are prebuilt; CAS changes canonical comparison lengths and minimal dynamic ModRevision varint in slot-owned scratch. HPACK uses per-connection encoder state, first block then warmed indexed block, with SETTINGS-dependent rebuild. There is **no grpc-timeout header** in the timed raw field list; its local socket/RPC timeout is not a transmitted deadline. The final committed implementation permits **conditional DATA fragmentation for flow control**, even though each scoped request is below 4 KiB. This supersedes the earlier single-WriteData description. TLS record/socket segmentation does **not** mean HTTP/2 DATA-frame segmentation. {framing_observation} Writes may still be coalesced in the client buffer. Response connection credit is coalesced at **64 KiB** ({work('receive-credit-size')} / {work('connection-credit')}); stream credit is similarly coalesced for sufficiently large still-open replies ({work('stream-credit')}). The cached 10-second write deadline is renewed only with **<=5 seconds remaining** ({work('deadline-renewal')}), not once per reply; it is still not a transmitted grpc-timeout. Therefore client HTTP/2 framing, HPACK table history, metadata, transport scheduling and potential stock grpc codec segmentation differ from the previous grpc-go benchmark. Do not claim identical framing or import its grpc-timeout channel close, allocs/op, failure populations or wake/batch populations.

**Server request-value stage counts do not otherwise change with this client**: Tiger still accumulates one owned full request, followed by the counted frontend/entry/clone/WAL/apply boundaries above. Different frames may change chunks, wake attempts or batching, not the number of scoped full-request accumulation destinations. Source templates/HPACK are client-side optimizations, not extra server new-value copies. The final generator/integration files were re-read from the parent-confirmed immutable commit and byte/hash verified; no modification of helper/repository files was made.

**Observed workload citations** (snapshot hashes and exact text in `workload-sites.md/json`): TLS/mTLS {work('tls-config')}; connection/slot population {work('active-connections')} / {work('connection-slots')} with one grpc control {work('control-count')}; keys/value derivation {work('keyspace')} / {work('keys')} / {work('values')}; global claim/permutation {work('global-claim')} / {work('permutation')}; templates and revision/length varints {work('templates')} / {work('cas-revision')} / {work('cas-length')}; complete timed metadata field list {work('fields')}; first/warm HPACK blocks {work('first-headers')} / {work('warm-headers')}; actual DATA source call {work('data-framing')}; one raw warm RPC/connection {work('warm-rpc')} precedes caller pool completion {work('raw-pool')}, revision fence {work('before-revision')}, CPU sample {work('before-cpu')} and phase start {work('start')}; cached CAS branch shape {work('compare')} / {work('success-put')} / {work('failure-range')}, untimed seed {work('seed-range')}, cached compare input {work('cached-revision')}, failure validation {work('validate-range')}, monotonic cache {work('revision-monotonic')} / {work('response-cache')}; five reps {work('five-reps')}, lean env {work('lean-env')}, requested Put/CAS256 suite {work('matrix')}. Client sites are **not counted server channels/value stages**.

## 6. Unchanged stock etcd: previous verified source audit by reference

Use `evidence/bench75/run-38387706444/etcd-source-audit/audit.md` and its `verified-sites.md`, `verification.json`, `dependency-integrity.json`, release-tag/binary records, with hashes in `reference-manifest.json`. No release/dependency refetch. Stock server pins remain grpc-go **v1.83.2**, Google protobuf **v1.36.11**, Raft **v3.7.0**, bbolt **v1.5.0**. Tiger server `go.mod` and `go.sum` are byte-identical to prior verified pins. The bound harness moves x/net v0.58.0 from direct to indirect in `go.mod` only: all module version pins are identical and `go.sum` is byte-identical; committed harness pin files are retained. Client grpc v1.84.0 is **not** stock server grpc. Reuse dependency source verification only; old measured zero-failure population and old generator metadata are **not this run**.

Prior source conclusions still applicable: fixed explicit etcd/Raft request core **3 sends + 3 selected receives, 2 closes, 2 fresh reply channels**; Ready/scheduler/peers remain symbolic/amortized. Known immediate leader Put **8 new-input stages, including WAL PageWriter staging exactly as previously verified**; CAS pass **8 new + 1 stored-comparison decode**; fail **7 new + 3 stored**. Additional codec coalescing **C** is conditional on actual grpc BufferSlice segmentation (one extra full-input destination if MaterializeToBuffer must coalesce; zero otherwise). Deferred bbolt leaf-page serialization is **separate and excluded from those immediate subtotals**; peers have separate occurrence populations. Do not infer segmentation from the new raw client's intended frames, assume old observed peer batches, or interpret Tiger 4 vs etcd 8 as physical-copy totals/ratio.

**Framing correction to the prior etcd audit:** its extra grpc timeout synchronization close was explicitly conditional on grpc-timeout. Stock `deps/google.golang.org_grpc@v1.83.2/internal/transport/http2_server.go:602-615` encloses creation/callback/close in `if timeoutSet`. New timed raw requests omit that header, so that extra frontend close is absent for them; the core 2 closes above are unchanged. Server-internal request timeout/context and other grpc lifecycle channels remain excluded, not zero. Untimed control grpc Range still has a deadline and is not charged per timed Put/CAS.

### Lean-vs-grpc scope addendum to reused etcd evidence

The old audit's measured grpc-go client emitted grpc-timeout; this raw cohort does not. Its explicit additional deadline-sync close must **not** be imported. The fixed core **3s/3r/2 closes/2 fresh reply channels** are unchanged. Ten-second grpc control probe/cache deadlines remain **outside the cohort**; local raw-client time limits do not transmit server grpc-timeout.

The committed engine can emit the complete sub-4-KiB gRPC message in one DATA frame **when actual credit suffices**, but its loop permits fragments when credit is partial. No unconditional single-frame statement is made. Transport TLS segmentation cannot by itself create additional HTTP/2 DATA frames. **C remains conditional here, not asserted zero**: a C=0 proof would require actual one-buffer codec input through stock recvReader's 5-byte header read, remaining body buffer, parser and MaterializeToBuffer; that is not established for every request in this implemented flow-control path. Do not blindly reuse an old fragmentation assumption or equate TLS records with codec buffers. Put therefore remains **8 immediate + conditional C** in this scope, including WAL PageWriter and excluding deferred bbolt pages; none is a physical-copy total. `lean-etcd-scope.md` records the final scope, superseding the earlier single-frame description.

## Bottom line

**Put: 4→4. CAS: 6→5 new-input stages, both compare outcomes.** PR80 removes only the owning **apply** bytes decode in this comparison; frontend owning decode is restored. CAS failure adds **one separate stored-value reply encode** to either Tiger target. One newly persisted/applied follower: Put 2→2; CAS 3→2; sender encode adds one per actual transmitted occurrence. Fixed request-private channels are zero in both; admission/disk/peer/wake/tick work remains actual-population dependent. These are named-source-stage counts, not performance or physical-copy totals. **Final measurement binding deferred until parent supplies immutable Actions resolve SHAs.**
'''
# Final report must not accidentally claim final while generator/metadata remains unbound.
if final:
 text=text.replace('**Final measurement binding deferred until parent supplies immutable Actions resolve SHAs.**', '**Final server and generator/harness bindings are parent-confirmed for Actions run 38397810888.**')
(ROOT/'audit.md').write_text(text)
print('Rendered',len(text),'characters;',status)
