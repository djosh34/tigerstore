#!/usr/bin/env python3
"""Generate the final source audit using ONLY verified final-target citations."""
from pathlib import Path
import datetime,json
ROOT=Path('/home/joshazimullah.linux/work_mounts/scratch/bench77-source-audit')
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc), 'deadline passed; start nothing'
refs=json.loads((ROOT/'targets.json').read_text())
sites=json.loads((ROOT/'verified-sites.json').read_text())
lookup={(s['target'],s['id']):s for s in sites}
citations=[]
def c(label,id):
    s=lookup[(label,id)];assert s['sha']==refs[label]['sha'];citations.append(s)
    return '`'+s['file']+':'+str(s['line'])+'`'
def cols(id):
    return [c(l,id) if (l,id) in lookup else '—' for l in ('main','batch','wire')]
def table(rows):
    return '\n'.join('| '+' | '.join(row)+' |' for row in rows)+'\n'
def site_table(rows):
    return table([['Executed site / purpose','M','B','W','Accounting'],['---','---','---','---','---']]+[[title,*cols(id),count] for title,id,count in rows])

summary='''## Concise table — per successful ordinary 1 KiB Put

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
'''
(ROOT/'table.md').write_text('# PR77 final source-audit table\n\n'+summary+'\n')

report=['# PR77 source audit — client wire → batch → core → Raft → WAL → apply/index → reply',
'''## Final target binding and method

This report audits only the requester-confirmed **rerun** targets, resolved **2026-10-13T08:56:32Z**, [Actions run 38372738426](https://github.com/djosh34/tigerstore/actions/runs/38372738426). Later branch heads are not substituted. The separate profile run is reported by the requester to use this same PR77 commit; no profile-hook code is charged to Put.
''',table([['Label','Branch / role','Exact commit','Archived directory'],['---','---','---','---']]+[[{'main':'M','batch':'B','wire':'W'}[l],{'main':'main','batch':'work/50-batch / PR76','wire':'work/50-wire / PR77'}[l],'`'+refs[l]['sha']+'`','`'+refs[l]['source']+'/`'] for l in refs]),
'''All work products are under `/home/joshazimullah.linux/work_mounts/scratch/bench77-source-audit`. The checkout at `/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75` was read only. Sources were extracted with `git archive <exact SHA> | tar -x -C <absolute scratch directory>`. `gh` was used only to fetch issue/PR/run/commit metadata. No checkout edits, comments, branch changes, subagents, Go executions, tests, or benchmarks were made.

The starting evidence was the checkout's `evidence/bench75/source-audit/audit.md` and `verified-sites.md`. Their definitions informed this census, **not their old counts or line numbers**. The older PR76 `room` signal/backpressure loop is gone at these final targets. The completion path now uses a CAS list rather than the earlier completion mutex. `verified-sites.md/json`, `site-specs.json`, `source-file-hashes.json`, and `archive-file-manifest.json` bind the checked text and files to the final git commits. `verify-sites.py` independently asserts each expected line and compares every cited repository file to `git show <SHA>:<path>`. `dependency-proof.md` and ZIP/hash evidence independently establish dependency behavior.

Earlier first-run targets (PR76 `751f69c76560d7e68fc76e9c91760020bfa21f72`, PR77 `fcd54b1ba2ddf120be3d83f5070cf13592a9f6d8`, same main; resolved 08:48:45Z, run 38371820704) are retained only as provenance in `earlier-run38371820704/` and `metadata/first-to-rerun-*.diff`. They are **not** the targets of the final tables. `observed/` is preliminary extraction provenance, also excluded.

### Scope and counting rules

A successful ordinary unary Put with a **1,024-byte nonempty value**, valid nonempty key, one canonical key/value field, no lease (including the old key version), no watch, no PrevKv, no IgnoreValue/IgnoreLease, no planted bug, no reset/deadline/cancellation/error, submitted to a stable leader over an already established client HTTP/2 connection. “Leader-only” means clients target the leader, **not no replication**. Insertion and overwrite have the same enumerated value stages; metadata/history retention may differ. Other RPCs, startup, recovery, shutdown, metrics and observation-only loopback runtime/pprof handlers are excluded. The measurement hook is not in these archives and was not audited as part of Put.

- A successful send and a selected receive are counted separately. Default branches, failed nonblocking attempts, unselected select arms, `len(ch)`, obtaining `Done()` and nil-channel attempts are not operations. Channel creation and `close` are separate.
- A full-value copy means all 1,024 value bytes are materialized in a different byte backing store. Encoding a bytes field, owning decode, copying a containing entry, and WAL staging qualify. Fragmented chunks into one destination count as one full-payload equivalent. Copying pointers, interfaces, descriptor structs, slice headers, keys or reply metadata does not copy this payload. Sizing/checksum scans are not copies.
- The census counts named materialization boundaries, including their actual pinned dependency implementations. A call and its dependency instruction are **one stage**, not two. It does not enumerate TLS/bufio/socket/kernel/page-cache movement, HTTP/gRPC transport buffering, runtime growth relocation, or all library lifecycle channels. The channel totals below are explicitly **repository-level**; transport/library channels are unknown here, not assumed zero. No precise physical memcpy, allocations/op, live-byte, CPU/op or performance total follows from these subtotals.
''',summary,
'''## Repository channel ledger, sends / receives / closes

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
''',site_table([
('Inbox admission send','admission-send','M: one send/Put; B/W: one send/admission batch'),
('Inbox receive, outer alternative','inbox-receive-outer','one selected receive per input, admission or inbound peer'),
('Inbox receive, drain alternative','inbox-receive-drain','alternative receipt; do NOT add to outer receipt for same input'),
('Do reply send','do-reply-send','M: one send/Put; B/W retained Do, NOT executed by ordinary Put'),
('Do reply receive','do-reply-receive','M: one receive/Put; B/W retained Do, NOT executed by ordinary Put'),
('Explicit reply channel creation','do-reply-create','M: one fresh channel/Put, not operation; B/W normal Put: zero'),
('Grouped completion callback','completion-direct','B/W direct Complete call: zero channel ops')]),
f'''B/W connection reader admission is {c('batch','stream-admit')} / {c('wire','stream-admit')}, batch API calls are {c('batch','batch-submit-call')} / {c('wire','batch-submit-call')}, and core submission is {c('batch','batch-core-submit')} / {c('wire','batch-core-submit')}. Request descriptors and slice headers are handed over; no payload copy there. Node.Do survives but is not the batch Put path.

The final completion publisher is {c('batch','completion-cas')} / {c('wire','completion-cas')}; the writer atomically swaps the list at {c('batch','completion-swap')} / {c('wire','completion-swap')}. No completion-channel handoff or completion mutex remains. This does **not** prove the whole hot path cannot block: inbox sends can wait, connection byte/output bookkeeping uses its ordinary mutex, and CAS may retry. Allocating a completion-list node is not a measured allocations/op result.

### Custom frontend wake and flow control
''',site_table([
('Successful writer wake send','writer-wake-send','Sw, conditional/coalesced; capacity-one channel'),
('Selected idle-writer receive','writer-wake-receive','Rw, only while writer finds no ready frames/control work'),
('Credit calls signal','credit-wake-attempt','attempt per DATA frame, not a separate channel site'),
('Complete calls signal','completion-wake-attempt','attempt per completion group, not a separate channel site'),
('Unary end attempts streaming input signal','unary-nil-signal','nil unary channel: zero successful sends'),
('Writer attempts streaming-space signal','unary-nil-space','nil unary channel: zero successful sends')]),
f'''**Important correction to the prior census:** these final B/W implementations call `signal` on **every DATA-frame credit**, not just at the 4 MiB receive-credit threshold. They also signal each completion group. Actual successful Sw can therefore include request-data wakes as well as completion wakes; do not bound it by G alone or assign a fixed two wake operations per Put/group. No fabricated DATA frame count is assumed for a 1 KiB value; framing/key/message bytes and client fragmentation matter.

For a connection interval, `Rw = Sw + initial_token_occupancy - final_token_occupancy`. A running writer can collect replies without a fresh receive, and a token can remain buffered after a reply is written. The caller's signal attempt can hit default. Byte-charge backpressure now defers connection window credit, with no `room` channel. Request-charge release at {c('batch','release-request')} / {c('wire','release-request')}, reply charge at {c('batch','charge-reply')} / {c('wire','charge-reply')}, and reply-charge release at {c('batch','stream-end-release')} / {c('wire','stream-end-release')} are arithmetic/flow-control bookkeeping, not additional channel exchanges or value copies.

Other shared wake causes (not fixed per Put):
''',site_table([
('GOAWAY','goaway-wake-attempt','lifecycle, excluded from normal Put fixed count'),
('Queued control frame / ping ACK','control-wake-attempt','conditional control traffic'),
('Settings ACK / change','settings-wake-attempt','setup or later control traffic'),
('Send-window update','window-wake-attempt','conditional receive-side control traffic'),
('Reset','reset-wake-attempt','exceptional for scoped request')]),
f'''Normal B/W Put has no per-stream context at {c('batch','stream-context-gate')} / {c('wire','stream-context-gate')}. An optional grpc-timeout uses `time.AfterFunc` at {c('batch','stream-deadline')} / {c('wire','stream-deadline')}: a callback timer, not a per-Put Timer.C receive. A deadline firing is outside this success scope. Streaming receive/send/watch signals do not execute for this Put.

### Disk and tick
''',site_table([
('Persist array → worker send','disk-send','Q sends'),
('Disk receives initial array','disk-receive-outer','one of Q total receives'),
('Disk receives another drained array','disk-receive-drain','another array, not a second receipt of initial array'),
('Worker → committer synced group send','disk-complete-send','F sends'),
('Committer synced group receive','disk-complete-receive','F receives'),
('Selected ticker receive','tick-receive','T background receives; timer internals excluded')]),
'''The worker can merge multiple queue arrays into one sync group. Hard-state-only/commit-only jobs can still pay channel operations without copying this value. It is not defensible to assume Q=F=number of entries or one sync per Put. A hypothetical Q=F=1 for B writes gives 4/B disk ops, but that is **not an observed or general batch size/count**.

### Peer handoffs and exceptional exclusions
''',site_table([
('Outbound peer message queue send','peer-queue-send','O sends on leader, data/control'),
('Peer writer receive fast control alternative','peer-queue-receive-fast','one of O total receives'),
('Peer writer receive control alternative','peer-queue-receive-control','one of O total receives'),
('Peer writer receive data alternative','peer-queue-receive-data','one of O total receives'),
('Inbound peer delivery → inbox send','peer-inbox-send','I sends; consumed by one of inbox receive alternatives above'),
('Unreachable report inbox send','unreachable-send','fault/drop-only successful nonblocking sends, excluded healthy ledger'),
('Failed-connection drain control/data','peer-drop-control','fault-only receives; data alternative in verified-sites'),
('Peer dial backoff timer receive','peer-backoff-receive','fault-only selected receives'),
('Node done close','shutdown-close','shutdown-only close'),
('Watch overflow close','watch-close','outside no-watch scope'),
('Server Stop wait-channel close','server-stop-close','outside normal Put lifecycle')]),
f'''The separate failure-only data-drain sites are {c('main','peer-drop-data')}, {c('batch','peer-drop-data')}, {c('wire','peer-drop-data')}. Cancellation receives, including {c('batch','node-done-receive')} / {c('wire','node-done-receive')} and peer context arms in verified-sites, do not execute in the scoped healthy request. Closing a socket or HTTP/2 stream is not closing a Go channel. Main HTTP/gRPC/context library lifecycle closes are not enumerated; the explicit repository normal-Put close count is **not** a claim of zero closes in all dependencies.

A fully consumed directed peer message pays **2 repository channel operations per endpoint**: sender queue send+receive, receiver inbox send+receive; **4 across both endpoints**. Append messages may carry many entries, each containing many Puts. Acks, empty appends, commit propagation and heartbeats still pay handoffs but copy no 1 KiB value. Client success needs quorum progress, not all followers. Fanout, Raft progress/probing, retransmission, queue drops and work after client reply make messages/Put conditional.

## Full-payload copy ledger

Repository call sites plus the dependency proof below identify each counted stage. A cited codec call and its module copy implementation are the same materialization, not additive copies.
''',site_table([
('Framer DATA → owned request-message accumulation','frontend-accumulation','M: transport internals unenumerated; B/W: 1 full-value equivalent'),
('Wire → fresh frontend Put.Value','frontend-decode','M: generated/grpc codec proof below; B: 1; W ordinary Put bypasses: 0'),
('Put request → encoded Raft-entry batch','entry-value-encode','1 per Put, all targets (protobuf encode M/B; raw Wire append W)'),
('Leader proposal boundary → Raft deep clone','raft-propose','1 per Put all targets, actual module clone proof below'),
('Entry.Data → WAL staging buffer','wal-value-copy','1 direct full-value append all targets; WAL caller below'),
('Apply generic decode → fresh Put.Value','apply-decode','1 main/batch; absent from wire Put apply: 0'),
('MVCC version retains Value slice header','index-value','0 payload copies; W aliases entry-backed bytes'),
('Version metadata append','index-version-append','0 payload copies'),
('Peer sender Message/Entry.Data encode','peer-value-encode','1 per transmitted occurrence of this payload, per recipient'),
('Peer receiver Message/Entry.Data decode','peer-value-decode','1 per received occurrence, including duplicates')]),
f'''Main's frontend is **not** a zero-copy exception: {c('main','main-grpc-route')} routes into grpc, the handler dispatch is {c('main','main-client-handler')}, the generated decoder is {c('main','lib-put-decoder')}, grpc invokes protobuf at {c('main','lib-grpc-decode')}, and the proto3 bytes decoder copies the whole value at {c('main','lib-proto3-decode-copy')}. B's frontend uses {c('batch','batch-frontend-decode')} → its unmarshal site above → the same proto3 copy instruction. For M/B apply, {c('main','apply-fresh-put')} / {c('batch','apply-fresh-put')} creates fresh PutRequest before the apply decoder. W removes both owning decodes on the ordinary Put path.

The Raft clone is **still present**: M/B/W proposals enter {c('main','raft-propose')}, {c('batch','raft-propose')}, {c('wire','raft-propose')}; RawNode initially aliases the buffer at {c('wire','lib-raft-propose-alias')}, but leader appendEntry clones the entry at {c('wire','lib-raft-clone')}. Entry.Data is **proto2**, so the actual clone copy is {c('wire','lib-proto2-clone-copy')}, NOT the Put proto3 decode site at codec_gen.go:5497. See dependency-proof.md for descriptors, coder selection and call chain.

WAL append is {c('main','wal-value-caller')}, {c('batch','wal-value-caller')}, {c('wire','wal-value-caller')} → the one copying record site above. This remains a separate staging copy even though the index aliases the Raft entry. File Write at {c('wire','wal-write')} and Sync at {c('wire','wal-sync')} are durability I/O, not additional enumerated userspace backing-store materializations. CRC scanning is not copying. WAL buffer capacity may be reused; a copy stage is not necessarily a new allocation.

### Verifying PR77's actual stage removal

1. Reader accumulation still copies into owned request storage; capacity comes from the full request size at {c('wire','frontend-prealloc')}. Fragmentation appends each next chunk into the same destination: one equivalent copy of this value, not one per DATA frame. The request includes protobuf/key metadata; it is not an exactly 1 KiB buffer.
2. Admission uses ParsePut at {c('wire','wire-frontend-parse')}. Its bytes-field parsing calls ConsumeBytes at {c('wire','wire-parse-consume')}; the module returns a subslice at {c('wire','lib-bytes-consume-alias')}. Capacity clipping at {c('wire','wire-parse-clip')} only changes the slice header, and {c('wire','wire-value-alias')} retains it. Validation at {c('wire','wire-check')} does not rebuild a protobuf PutRequest or copy the value.
3. Batch descriptors reach core submission without a payload copy. Queuing charges len(Wire)+framing at {c('wire','wire-size')}; Process proposes through {c('wire','core-encode-call')} and encodes client wire once at the ledger site. The preallocation at {c('wire','entry-buffer-init')} reserves `size+MaxVarintLen64`. Each request charged its full wire length plus up to four 10-byte varints and one kind byte; the extra 10 bytes cover the count. Pruning settled proposals can only over-reserve. Thus scoped wire batch encoding has no append-growth relocation; it still performs the one explicit payload append. M/B start at {c('main','entry-buffer-init')} / {c('batch','entry-buffer-init')} and can grow.
4. Raft then deep-clones the encoded entry. Async local append/apply messages retain references: {c('wire','async-append-alias')} / {c('wire','async-apply-alias')}. Driver sends persist jobs; WAL stages bytes and syncs; Persisted updates storage, which appends entry pointers at {c('wire','storage-pointer-append')} → {c('wire','lib-storage-pointer-append1')} or {c('wire','lib-storage-pointer-append2')}, not another Data copy.
5. **Every replica's Put apply actually bypasses generic unmarshal:** {c('wire','apply-dispatch')} walks entry bytes; {c('wire','wire-batch-slice')} gives a capped entry subslice; {c('wire','apply-wire-kind')} chooses applyPut before {c('wire','generic-apply-still-exists')}. applyPut parses in place at {c('wire','apply-wire-parse')}, forwards fields to State at {c('wire','apply-wire-state')} and MVCC at {c('wire','wire-state-index')}. MVCC retains value/key slice headers at the ledger site and {c('wire','index-key')}. The retained value therefore aliases the **Raft-owned entry backing store**, not the earlier frontend buffer or WAL staging buffer. The successful no-watch path exits matching at {c('wire','no-watch-fastpath')} without event value encoding.
6. The normal wire reply is metadata at {c('wire','apply-wire-reply')}. Writer method dispatch chooses {c('wire','wire-reply-encode')}, buffer prefix/encode at {c('wire','manual-response-prefix')} / {c('wire','manual-response-append')}. No ordinary PutResponse protobuf message is built on this wire path. M/B encode a small response with no value; B at {c('batch','batch-reply-encode')}. **All normal replies copy zero of this 1 KiB input payload.** PrevKv/watch/ignore paths can behave differently and are excluded.

`DecodeBatch` still contains generic decoding at {c('wire','decode-batch-still-exists')}, but is a helper used by checks, not this production Put apply path. Txn/DeleteRange/lease writes retain generic apply decode at the other site above. A retained helper is not evidence that the removed Put stage still executes.

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

Followers process MsgApp via {c('wire','follower-maybeappend')} → {c('wire','follower-logappend')} → {c('wire','unstable-append')}, retaining entry pointers rather than taking the leader proposal appendEntry clone. Raft outbound append Message construction aliases entries at {c('wire','lib-peer-construction-alias')}. No follower re-encode through encodeBatch occurs for received appends. The receiver's decoded Entry.Data remains owned and safe despite transport-buffer reuse. These statements apply to all three commits' identically pinned Raft dependency.

Peer multiplicity is not fixed per Put. Each transmitted occurrence/recipient contributes a sender materialization; each received occurrence contributes a receiver materialization; only newly persisted/applied occurrences pay those stages. A duplicate can decode without re-persisting/re-applying. Ack/heartbeat/empty append messages copy no payload value. No fixed two-followers count or assumption that all follower work precedes the client reply is made.

### Conditional copies not promoted to fixed totals

- M/B growing entry encoding can relocate earlier payloads. W's full-batch allocation removes that particular growth mechanism for the scoped path; no fixed number of growth copies is invented for the old path.
- WAL append and reused peer MarshalAppend buffers can grow depending on history, capacity and ordering. Those extra full/partial relocations are conditional, beyond the named direct append/codec stage.
- Main grpc's multi-buffer materialization at {c('main','lib-grpc-materialize')} executes byte copies at {c('main','lib-grpc-copyto')}; a single constituent buffer is aliased instead. It is a conditional additional full-request materialization, not an always-copying field decode and not in the fixed payload subtotal.
- Framer/body input buffering, TLS, bufio, sockets, kernel/page cache and outgoing response transport are not enumerated. Excluded is not zero. Compression and noncanonical repeated-field requests are outside the scoped ordinary Put; they can change copying or stored-wire size.

## Conclusions and measurement limits

- **Channels:** M still pays two fixed request-specific exchanges (four repository operations). B and W have the same batch admission and direct atomic-list completion design, with conditional DATA/completion/control wakes. No total channels/op or observed batch size is established.
- **Copies/stage removal:** B→W removes the frontend owning Put.Value decode and the apply owning decode on the leader; each follower loses the latter. Known named leader stages are M=5, B=6, W=4 **including** the Raft clone and WAL staging, excluding peers/growth/transport. Known follower stages for the bounded single-delivery case drop 3→2. PR77's body omits the retained Raft clone from its informal copy list; the clone cannot defensibly be called removed.
- **Spec:** scoped Put in-place apply and entry-backed index values are real; a general WAL-segment reference implementation, all-command no-decode path, all-path no blocking/churn and zero physical copying are not established.
- **Failure/performance:** requester reports the first run's 64-client repetition logged ErrDiskStalled and metrics refusal, with a fresh rerun dispatched. This source census neither attributes that failure to physical VM versus code nor infers throughput/CPU/GC improvement. Observation-only pprof and metrics requests are excluded rather than charged to Put. No local benchmark or heavy test was run.
''']
(ROOT/'audit.md').write_text('\n'.join(report)+'\n')

proof=['# Dependency proof — exact final measurement targets','', 'Dependency sources are not repository-commit files. The module/version + module-relative path is the citation identity. Each final commit uses the same listed version and matching go.sum hash. `dependency-source-manifest.json` records the original cache path and SHA256 of each copied source. `verify-dependencies.py` independently recomputes the Go module ZIP h1 (sorted per-file SHA256/name lines), checks each target go.sum and cache .ziphash, and verifies evidence files equal both ZIP and cache bytes. No Go command, build or download was used. `dependency-integrity.json` preserves the results.','',table([['Target','Commit'],['---','---']]+[[l,'`'+r['sha']+'`'] for l,r in refs.items()]),table([['Module','Version','Computed ZIP h1'],['---','---','---']]+[[row['module'].split('@')[0],row['module'].split('@')[1],'`'+row['computed_h1']+'`'] for row in json.loads((ROOT/'dependency-integrity.json').read_text())]),
'''The project's go.mod has no replacement for these modules; main differs only in x/net being indirect rather than direct. go.sum is identical across the three final targets. Copied dependency go.mod files also show the relevant cross-pins (Raft/API/grpc protobuf v1.36.11; no higher cross-version for these cited dependencies). These source conclusions concern the archived module pins, not a disassembly or assertion about compiler/runtime physical copy expansion.

## Codec/ownership chain
''',table([['Proof','Pinned file:line (same dependency in M/B/W)','Effect / applicable path'],['---','---','---']]+[[title,c('wire',id),effect] for title,id,effect in [
('PutRequest.Value descriptor','lib-put-value-field','proto3 nonempty bytes'),
('Main generated decoder call','lib-put-decoder','allocates PutRequest then dec(in), only main frontend uses this adapter'),
('grpc protobuf codec','lib-grpc-decode','main proto.Unmarshal of request bytes'),
('Proto3 bytes codec selection','lib-proto3-codec-selection','no-presence []byte uses coderBytesNoZero'),
('Proto3 Put.Value owning decode','lib-proto3-decode-copy','append(nil, v...) full copy: M/B frontend + M/B apply; wire Put bypasses'),
('Proto3 bytes encoder boundary','proto3-bytes-encoder','AppendBytes used for entry encoding M/B; no duplicate stage'),
('Actual bytes encoder','lib-bytes-encode-copy','append length-prefixed v to output: M/B entry + all peer Entry.Data encode'),
('RawNode proposal','lib-raft-propose-alias','Data initially aliases caller data; not the end of leader path'),
('Leader proposal dispatch','lib-raft-propose-clone-call','calls appendEntry'),
('Leader clone call','lib-raft-clone','proto.Clone Entry once, all targets'),
('Entry protocol syntax','lib-entry-proto2','Entry is proto2'),
('Entry.Data descriptor','lib-entry-data-field','bytes with presence, NOT proto3 Value field'),
('Proto2 bytes coder selection','lib-proto2-codec-selection','presence []byte uses coderBytes'),
('Clone implementation','lib-proto-clone-merge','new destination then mergeMessage'),
('Actual proto2 clone copy','lib-proto2-clone-copy','mergeBytes append(emptyBuf[:], *src.Bytes()...): full Entry.Data copy, all leaders'),
('Actual proto2 owning decode','lib-proto2-decode-copy','consumeBytes append(emptyBuf[:], v...): full Entry.Data copy, all peer receivers'),
('Wire parse consume','lib-bytes-consume-alias','subslice only; no copy on wire frontend/apply'),
('MemoryStorage append alternative 1','lib-storage-pointer-append1','entry pointers only; zero Data copy'),
('MemoryStorage append alternative 2','lib-storage-pointer-append2','entry pointers only; zero Data copy'),
('Outbound append construction','lib-peer-construction-alias','entry pointers aliased, before peer codec materialization'),
('Follower append path','follower-maybeappend','maybeAppend, not leader appendEntry clone'),
('Follower log append','follower-logappend','pointer entries passed to append'),
('Unstable log append','unstable-append','[]*Entry append, not Data bytes'),
('Async append message','async-append-alias','rd.Entries retained as pointers'),
('Async apply message','async-apply-alias','ents retained as pointers'),
('Conditional grpc materialization','lib-grpc-materialize','multi-buffer only; excluded fixed subtotal'),
('Actual grpc CopyTo instruction','lib-grpc-copyto','chunks copied into one destination, one full-request equivalent')]]),
'''**Critical distinction:** `protobuf-v1.36.11/internal/impl/merge.go:188` is the Entry.Data proto2 deep-clone byte copy. `internal/impl/codec_gen.go:5497` is Put.Value proto3 owning decode, not that clone. Entry.Data peer receive uses its proto2 decoder at `codec_gen.go:5418`. `AppendBytes` at `encoding/protowire/wire.go:455` copies during encoding; `ConsumeBytes` at `:468` aliases during wire parsing. None of these lines can be substituted for another stage merely because all operate on byte slices.

The default generated message fast-path is established by the byte-field descriptors and codec selection. Clone's documented deep-copy semantics and merge fallback also require independent byte ownership; the identified fast-path instruction is not a speculative optimization. The source proof does not count extra growth relocations or physical transport/runtime copies.
''']
(ROOT/'dependency-proof.md').write_text('\n'.join(proof)+'\n')
# Every report citation was selected from already asserted rows, not handwritten inferred line numbers.
used={(s['target'],s['id']):s for s in citations}
(ROOT/'report-citations.json').write_text(json.dumps(list(used.values()),indent=2)+'\n')
print('Wrote audit.md, table.md, dependency-proof.md using',len(used),'verified target-bound citation identities.')
