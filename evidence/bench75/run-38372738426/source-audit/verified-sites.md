# Verified census sites

**Only requester-confirmed rerun measurement targets**, resolved 2026-10-13T08:56:32Z, Actions run 38372738426.

Exact expected source lines asserted by `verify-sites.py`. Every repository evidence file was compared byte-for-byte with `git show <SHA>:<path>` after git-archive extraction. Dependency files are module sources, not files in the git commit; each row is bound to that commit’s dependency pins. `verify-dependencies.py` recomputes the module ZIP h1 hash against every target’s go.sum and compares evidence bytes to ZIP and cache. A call-site and its copying dependency implementation describe ONE stage, never two copies. Alternatives describe one possible receipt, never multiple receipts per event.

## main — `e1fd889c345bc3c06d8100eb6d635c88ad6967cd`

| File:line | ID / classification | Exact source line (trimmed) |
|---|---|---|
| `internal/node/node.go:122` | admission-send: counted send: 1/Put main, 1/admission batch batch/wire | <code>case n.inbox &lt;- in:</code> |
| `internal/node/node.go:264` | inbox-receive-outer: counted receive: alternative outer inbox receive; admission or inbound peer | <code>case in := &lt;-inbox:</code> |
| `internal/node/node.go:269` | inbox-receive-drain: counted receive: alternative drain inbox receive, NOT an extra receipt | <code>case in := &lt;-n.inbox:</code> |
| `internal/node/node.go:301` | do-reply-send: counted main reply send 1/Put; retained Do only, excluded on batch/wire Put | <code>ch &lt;- r</code> |
| `internal/node/node.go:217` | do-reply-receive: counted main reply receive 1/Put; retained Do only, excluded on batch/wire Put | <code>case rep := &lt;-r.reply:</code> |
| `internal/node/node.go:212` | do-reply-create: creation, not send/receive: 1/Put main; excluded batch/wire Put | <code>r := &amp;request{id: n.nextID.Add(1), op: op, reply: make(chan core.Reply, 1)}</code> |
| `internal/node/node.go:282` | disk-send: counted send: 1/persist queue array | <code>case toDisk &lt;- next:</code> |
| `internal/node/node.go:369` | disk-receive-outer: counted receive: initial array; alternative to drain | <code>case jobs = &lt;-n.persistQ:</code> |
| `internal/node/node.go:376` | disk-receive-drain: counted receive: drained array; NOT an extra receive for initial array | <code>case js := &lt;-n.persistQ:</code> |
| `internal/node/node.go:390` | disk-complete-send: counted send: 1/sync completion group | <code>case n.persisted &lt;- done:</code> |
| `internal/node/node.go:275` | disk-complete-receive: counted receive: 1/sync completion group | <code>case done := &lt;-n.persisted:</code> |
| `internal/node/node.go:286` | tick-receive: counted selected background receive, not fixed per Put | <code>case &lt;-ticker.C:</code> |
| `internal/node/node.go:197` | peer-inbox-send: counted send: 1/delivered inbound peer message | <code>case n.inbox &lt;- input{msg: m}:</code> |
| `internal/node/node.go:205` | unreachable-send: fault/drop-only successful send, excluded normal healthy transport ledger | <code>case n.inbox &lt;- input{unreachable: id}:</code> |
| `internal/node/node.go:239` | shutdown-close: close: shutdown-only, zero per normal Put | <code>close(n.done)</code> |
| `internal/node/node.go:320` | watch-close: close: watch overflow, outside no-watch scope | <code>close(ws.done)</code> |
| `internal/node/node.go:383` | wal-append-call: driver disk trace; no extra payload copy beyond WAL record | <code>n.wal.Append(j.HardState, j.Entries)</code> |
| `internal/node/node.go:386` | wal-sync-call: driver disk trace; I/O, not counted as extra userspace payload materialization | <code>if err := n.wal.Sync(); err != nil {</code> |
| `internal/peer/peer.go:294` | peer-queue-send: counted send: 1/queued outbound peer message | <code>case q &lt;- m:</code> |
| `internal/peer/peer.go:305` | peer-queue-receive-fast: counted receive alternative 1: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:311` | peer-queue-receive-control: counted receive alternative 2: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:314` | peer-queue-receive-data: counted receive alternative 3: one receipt per message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:328` | peer-drop-control: fault-only receive: dropped queued control message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:330` | peer-drop-data: fault-only receive: dropped queued data message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:347` | peer-backoff-receive: fault-only timer receive, excluded healthy path | <code>case &lt;-time.After(backoff):</code> |
| `internal/peer/peer.go:317` | peer-shutdown-receive-1: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:348` | peer-shutdown-receive-2: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:416` | peer-value-encode: counted copy: 1/serialized occurrence of Entry.Data containing this Put, on sender | <code>buf, err = proto.MarshalOptions{}.MarshalAppend(buf[:0], m)</code> |
| `internal/peer/peer.go:238` | peer-value-decode: counted copy: 1/received occurrence of Entry.Data, on receiver; dependency decoder below | <code>if err := proto.Unmarshal(buf, m); err != nil {</code> |
| `internal/core/codec.go:64` | entry-value-encode: counted copy: 1 per Put into encoded entry; NOT 1/batch-size per Put | <code>buf, err = proto.MarshalOptions{Deterministic: true}.MarshalAppend(buf, p.req.Op)</code> |
| `internal/core/codec.go:35` | entry-buffer-init: main/batch grow; wire whole-batch upper-bound capacity, no encode growth for scoped path | <code>buf := binary.AppendUvarint(nil, uint64(len(ps)))</code> |
| `internal/core/node.go:522` | raft-propose: leader proposal -> dependency clone: counted separately below | <code>if err := n.rn.Propose(data); err != nil {</code> |
| `internal/core/node.go:517` | core-encode-call: entry encode caller, not an additional copy | <code>data, err := encodeBatch(n.cfg.ID, n.cfg.Boot, props)</code> |
| `internal/core/codec.go:114` | apply-decode: counted copy: 1 applied Put.Value main/batch, removed wire | <code>if err := proto.Unmarshal(data[:size], op); err != nil {</code> |
| `internal/core/codec.go:96` | apply-fresh-put: main/batch fresh apply protobuf request | <code>op = &amp;pb.PutRequest{}</code> |
| `internal/core/node.go:790` | apply-dispatch: main/batch decoded apply vs wire in-place walk | <code>err := DecodeBatch(e.GetData(), func(origin, boot, id uint64, op proto.Message) error {</code> |
| `internal/mvcc/ops.go:235` | index-value: zero value copy: store slice header; wire aliases entry, others decoded bytes | <code>v := version{create: w.begin + 1, ver: 1, lease: p.Lease, value: p.Value}</code> |
| `internal/mvcc/ops.go:227` | index-version-append: metadata/slice headers only: zero payload copies | <code>k.vers = append(k.vers, v)</code> |
| `internal/mvcc/ops.go:253` | index-key: zero key bytes copy at insert: retains key slice; key copies are not value copies | <code>k = &amp;keyIndex{key: p.Key}</code> |
| `internal/core/storage.go:39` | storage-pointer-append: MemoryStorage appends entry pointers, not Entry.Data bytes | <code>if err := s.Append(job.Entries); err != nil {</code> |
| `internal/wal/wal.go:351` | wal-value-copy: counted copy: 1 direct Entry.Data append per persisted entry containing Put | <code>w.buf = append(w.buf, p...)</code> |
| `internal/wal/wal.go:368` | wal-value-caller: WAL Entry.Data caller, not a second copy | <code>w.record(recEntry, h[:], e.GetData())</code> |
| `internal/wal/wal.go:386` | wal-write: file I/O/physical copies not enumerated in payload subtotal | <code>n, err := w.seg.Write(w.buf)</code> |
| `internal/wal/wal.go:416` | wal-sync: durability I/O, not byte materialization counted here | <code>if err := w.seg.Sync(); err != nil {</code> |
| `internal/server/server.go:160` | main-client-handler: main client decode boundary is generated etcd/grpc adapter below | <code>return do[*pb.PutResponse](ctx, s.n, r)</code> |
| `internal/server/http.go:29` | main-grpc-route: main HTTP2/gRPC route; transport buffering/channels excluded, NOT zero | <code>s.g.ServeHTTP(w, r)</code> |
| `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc.pb.go:760` | lib-put-value-field: Put.Value is proto3 bytes | <code>Value []byte `protobuf:&quot;bytes,2,opt,name=value,proto3&quot; json:&quot;value,omitempty&quot;`</code> |
| `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc_grpc.pb.go:248` | lib-put-decoder: main generated decoder; batch/wire use own frontend | <code>if err := dec(in); err != nil {</code> |
| `dependency-sources/grpc-v1.84.0/encoding/proto/proto.go:96` | lib-grpc-decode: main default protobuf decode | <code>return proto.Unmarshal(buf.ReadOnlyData(), vv)</code> |
| `dependency-sources/grpc-v1.84.0/mem/buffer_slice.go:115` | lib-grpc-materialize: conditional additional main multi-buffer materialization, excluded fixed subtotal | <code>s.CopyTo(*buf)</code> |
| `dependency-sources/grpc-v1.84.0/mem/buffer_slice.go:82` | lib-grpc-copyto: conditional materialization actual byte-copy instruction | <code>off += copy(dst[off:], b.ReadOnlyData())</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5497` | lib-proto3-decode-copy: counted codec materialization proof for main/batch frontend and apply; removed wire Put | <code>*p.Bytes() = append(([]byte)(nil), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5418` | lib-proto2-decode-copy: counted receiver Entry.Data decode copy all targets | <code>*p.Bytes() = append(emptyBuf[:], v...)</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` | lib-bytes-encode-copy: counted bytes encode copy proof main/batch entry encoding and all-target peer encoding | <code>return append(AppendVarint(b, uint64(len(v))), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:468` | lib-bytes-consume-alias: wire ParsePut gets subslice, not copied bytes | <code>return b[n:][:m], n + int(m)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:817` | lib-raft-clone: counted leader Entry.Data deep-clone boundary: 1/Put all targets | <code>cloned[i] = proto.Clone(es[i]).(*pb.Entry)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1349` | lib-raft-propose-clone-call: leader successful proposal invokes appendEntry clone | <code>if !r.appendEntry(m.GetEntries()...) {</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:94` | lib-raft-propose-alias: RawNode boundary aliases data; later appendEntry clones it | <code>Entries: []*pb.Entry{{Data: data}},</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.proto:1` | lib-entry-proto2: Entry.Data uses proto2 bytes, not proto3 Put decoder | <code>syntax = &quot;proto2&quot;;</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.pb.go:348` | lib-entry-data-field: Entry.Data bytes descriptor | <code>Data          []byte                 `protobuf:&quot;bytes,4,opt,name=Data&quot; json:&quot;Data,omitempty&quot;`</code> |
| `dependency-sources/protobuf-v1.36.11/proto/merge.go:58` | lib-proto-clone-merge: Clone creates destination then merges fields | <code>mergeOptions{}.mergeMessage(dst, src)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188` | lib-proto2-clone-copy: counted full Entry.Data byte-copy instruction for leader Raft clone | <code>*dst.Bytes() = append(emptyBuf[:], *src.Bytes()...)</code> |
| `dependency-sources/raft-v3.7.0/storage.go:318` | lib-storage-pointer-append1: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents[:offset:offset], entries...)</code> |
| `dependency-sources/raft-v3.7.0/storage.go:320` | lib-storage-pointer-append2: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents, entries...)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:656` | lib-peer-construction-alias: zero payload copies when constructing append Message | <code>Entries: ents,</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:278` | lib-proto3-codec-selection: no-presence bytes coder selection for Put.Value | <code>return nil, coderBytesNoZero</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:428` | lib-proto2-codec-selection: presence bytes coder selection for Entry.Data | <code>return nil, coderBytes</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1800` | follower-maybeappend: follower uses maybeAppend, not leader appendEntry deep clone | <code>if mlastIndex, ok := r.raftLog.maybeAppend(a, m.GetCommit()); ok {</code> |
| `dependency-sources/raft-v3.7.0/log.go:127` | follower-logappend: follower retains entry pointers | <code>l.append(a.entries[ci-offset:]...)</code> |
| `dependency-sources/raft-v3.7.0/log_unstable.go:205` | unstable-append: append []*Entry pointers, not Data bytes | <code>u.entries = append(u.entries, ents...)</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:228` | async-append-alias: local Raft async append message retains entry pointers | <code>Entries: rd.Entries,</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:393` | async-apply-alias: local Raft async apply message retains entry pointers | <code>Entries: ents,</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5483` | proto3-bytes-encoder: bytes encoders call AppendBytes; not an additional copy | <code>b = protowire.AppendBytes(b, v)</code> |
| `internal/core/watch.go:272` | no-watch-fastpath: no-watch match returns without value encoding | <code>if len(changes) == 0 &#124;&#124; len(n.watchers) == 0 {</code> |

## batch — `29c3f8f95e3c9bcecf5bb39aa1f3bca49fe99c1f`

| File:line | ID / classification | Exact source line (trimmed) |
|---|---|---|
| `internal/node/node.go:136` | admission-send: counted send: 1/Put main, 1/admission batch batch/wire | <code>case n.inbox &lt;- in:</code> |
| `internal/node/node.go:333` | inbox-receive-outer: counted receive: alternative outer inbox receive; admission or inbound peer | <code>case in := &lt;-inbox:</code> |
| `internal/node/node.go:338` | inbox-receive-drain: counted receive: alternative drain inbox receive, NOT an extra receipt | <code>case in := &lt;-n.inbox:</code> |
| `internal/node/node.go:418` | do-reply-send: counted main reply send 1/Put; retained Do only, excluded on batch/wire Put | <code>w.reply &lt;- r</code> |
| `internal/node/node.go:284` | do-reply-receive: counted main reply receive 1/Put; retained Do only, excluded on batch/wire Put | <code>case rep := &lt;-r.reply:</code> |
| `internal/node/node.go:279` | do-reply-create: creation, not send/receive: 1/Put main; excluded batch/wire Put | <code>r := &amp;request{id: n.nextID.Add(1), op: op, reply: make(chan core.Reply, 1)}</code> |
| `internal/node/node.go:430` | completion-direct: direct grouped call: zero channel ops | <code>g.to.Complete(g.cs)</code> |
| `internal/node/node.go:351` | disk-send: counted send: 1/persist queue array | <code>case toDisk &lt;- next:</code> |
| `internal/node/node.go:483` | disk-receive-outer: counted receive: initial array; alternative to drain | <code>case jobs = &lt;-n.persistQ:</code> |
| `internal/node/node.go:490` | disk-receive-drain: counted receive: drained array; NOT an extra receive for initial array | <code>case js := &lt;-n.persistQ:</code> |
| `internal/node/node.go:504` | disk-complete-send: counted send: 1/sync completion group | <code>case n.persisted &lt;- done:</code> |
| `internal/node/node.go:344` | disk-complete-receive: counted receive: 1/sync completion group | <code>case done := &lt;-n.persisted:</code> |
| `internal/node/node.go:355` | tick-receive: counted selected background receive, not fixed per Put | <code>case &lt;-ticker.C:</code> |
| `internal/node/node.go:264` | peer-inbox-send: counted send: 1/delivered inbound peer message | <code>case n.inbox &lt;- input{msg: m}:</code> |
| `internal/node/node.go:272` | unreachable-send: fault/drop-only successful send, excluded normal healthy transport ledger | <code>case n.inbox &lt;- input{unreachable: id}:</code> |
| `internal/node/node.go:306` | shutdown-close: close: shutdown-only, zero per normal Put | <code>close(n.done)</code> |
| `internal/node/node.go:392` | watch-close: close: watch overflow, outside no-watch scope | <code>close(ws.done)</code> |
| `internal/node/node.go:497` | wal-append-call: driver disk trace; no extra payload copy beyond WAL record | <code>n.wal.Append(j.HardState, j.Entries)</code> |
| `internal/node/node.go:500` | wal-sync-call: driver disk trace; I/O, not counted as extra userspace payload materialization | <code>if err := n.wal.Sync(); err != nil {</code> |
| `internal/node/node.go:191` | batch-api: direct API, not channel operation | <code>func (n *Node) SubmitBatch(ctx context.Context, ops []proto.Message, tags []any, to Completer) error {</code> |
| `internal/node/node.go:444` | batch-core-submit: descriptor/header handoff: zero payload copies | <code>n.core.Submit(core.Request{ID: id, Op: op})</code> |
| `internal/peer/peer.go:294` | peer-queue-send: counted send: 1/queued outbound peer message | <code>case q &lt;- m:</code> |
| `internal/peer/peer.go:305` | peer-queue-receive-fast: counted receive alternative 1: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:311` | peer-queue-receive-control: counted receive alternative 2: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:314` | peer-queue-receive-data: counted receive alternative 3: one receipt per message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:328` | peer-drop-control: fault-only receive: dropped queued control message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:330` | peer-drop-data: fault-only receive: dropped queued data message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:347` | peer-backoff-receive: fault-only timer receive, excluded healthy path | <code>case &lt;-time.After(backoff):</code> |
| `internal/peer/peer.go:317` | peer-shutdown-receive-1: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:348` | peer-shutdown-receive-2: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:416` | peer-value-encode: counted copy: 1/serialized occurrence of Entry.Data containing this Put, on sender | <code>buf, err = proto.MarshalOptions{}.MarshalAppend(buf[:0], m)</code> |
| `internal/peer/peer.go:238` | peer-value-decode: counted copy: 1/received occurrence of Entry.Data, on receiver; dependency decoder below | <code>if err := proto.Unmarshal(buf, m); err != nil {</code> |
| `internal/server/h2.go:190` | writer-wake-send: counted successful conditional/coalesced wake send; default is not operation | <code>case c.wake &lt;- struct{}{}:</code> |
| `internal/server/write.go:54` | writer-wake-receive: counted selected wake receive only while idle | <code>case &lt;-c.wake:</code> |
| `internal/server/h2.go:369` | credit-wake-attempt: wake attempt per DATA frame, not a guaranteed successful send | <code>c.signal()</code> |
| `internal/server/batch.go:174` | completion-wake-attempt: wake attempt per completion group, not guaranteed successful send | <code>c.signal()</code> |
| `internal/server/h2.go:179` | goaway-wake-attempt: lifecycle wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:203` | control-wake-attempt: control wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:316` | settings-wake-attempt: settings wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:331` | window-wake-attempt: send-window wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:359` | reset-wake-attempt: reset wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:514` | unary-nil-signal: nil unary channel: zero successful sends; default wins | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/write.go:132` | unary-nil-space: nil unary channel: zero successful sends; default wins | <code>case st.space &lt;- struct{}{}:</code> |
| `internal/server/stream.go:127` | frontend-accumulation: counted copy: 1 full-value equivalent over all request chunks | <code>st.msg = append(st.msg, d[:n]...)</code> |
| `internal/server/stream.go:124` | frontend-prealloc: full request-sized capacity prevents accumulation growth; not a separate copy | <code>st.msg = make([]byte, 0, size)</code> |
| `internal/server/rpc.go:62` | frontend-decode: counted batch frontend copy 1/Put; wire ordinary Put bypasses this site | <code>if err := proto.Unmarshal(b, m); err != nil {</code> |
| `internal/server/batch.go:52` | batch-frontend-decode: batch decode call: not a second copy beyond frontend-decode | <code>if err := unmarshal(st.body, op); err != nil {</code> |
| `internal/server/batch.go:109` | batch-submit-call: one admission API call per actual batch | <code>err := c.s.n.SubmitBatch(c.ctx, b.ops, b.tags, c)</code> |
| `internal/server/batch.go:202` | batch-reply-encode: zero input-value copies: ordinary reply contains no value; wire bypasses | <code>f.msg, f.err = marshal(cp.Resp)</code> |
| `internal/server/stream.go:207` | response-proto-encode: ordinary response materialization: zero input-value copies | <code>b, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(b, m)</code> |
| `internal/server/write.go:56` | node-done-receive: shutdown-only receive; obtaining Done() itself is not operation | <code>case &lt;-nodeDone:</code> |
| `internal/server/server.go:189` | server-stop-close: close: server Stop wait channel, not per Put | <code>close(done)</code> |
| `internal/server/h2.go:522` | stream-admit: ordinary Put is synchronous reader admission, no per-Put handler goroutine | <code>c.admit(st)</code> |
| `internal/server/h2.go:445` | stream-context-gate: ordinary batch/wire Put creates no per-stream context | <code>if !grpc &#124;&#124; (st.rpc != nil &amp;&amp; st.rpc.op == nil) {</code> |
| `internal/server/h2.go:457` | stream-deadline: optional timer callback, not a Timer.C send/receive | <code>st.deadline = time.AfterFunc(timeout, st.expire)</code> |
| `internal/server/write.go:193` | stream-end-release: release accounting/flow control; no room channel exists at these targets | <code>c.releaseLocked(&amp;st.replyHeld)</code> |
| `internal/core/codec.go:64` | entry-value-encode: counted copy: 1 per Put into encoded entry; NOT 1/batch-size per Put | <code>buf, err = proto.MarshalOptions{Deterministic: true}.MarshalAppend(buf, p.req.Op)</code> |
| `internal/core/codec.go:35` | entry-buffer-init: main/batch grow; wire whole-batch upper-bound capacity, no encode growth for scoped path | <code>buf := binary.AppendUvarint(nil, uint64(len(ps)))</code> |
| `internal/core/node.go:522` | raft-propose: leader proposal -> dependency clone: counted separately below | <code>if err := n.rn.Propose(data); err != nil {</code> |
| `internal/core/node.go:517` | core-encode-call: entry encode caller, not an additional copy | <code>data, err := encodeBatch(n.cfg.ID, n.cfg.Boot, props)</code> |
| `internal/core/codec.go:114` | apply-decode: counted copy: 1 applied Put.Value main/batch, removed wire | <code>if err := proto.Unmarshal(data[:size], op); err != nil {</code> |
| `internal/core/codec.go:96` | apply-fresh-put: main/batch fresh apply protobuf request | <code>op = &amp;pb.PutRequest{}</code> |
| `internal/core/node.go:790` | apply-dispatch: main/batch decoded apply vs wire in-place walk | <code>err := DecodeBatch(e.GetData(), func(origin, boot, id uint64, op proto.Message) error {</code> |
| `internal/mvcc/ops.go:235` | index-value: zero value copy: store slice header; wire aliases entry, others decoded bytes | <code>v := version{create: w.begin + 1, ver: 1, lease: p.Lease, value: p.Value}</code> |
| `internal/mvcc/ops.go:227` | index-version-append: metadata/slice headers only: zero payload copies | <code>k.vers = append(k.vers, v)</code> |
| `internal/mvcc/ops.go:253` | index-key: zero key bytes copy at insert: retains key slice; key copies are not value copies | <code>k = &amp;keyIndex{key: p.Key}</code> |
| `internal/core/storage.go:39` | storage-pointer-append: MemoryStorage appends entry pointers, not Entry.Data bytes | <code>if err := s.Append(job.Entries); err != nil {</code> |
| `internal/wal/wal.go:351` | wal-value-copy: counted copy: 1 direct Entry.Data append per persisted entry containing Put | <code>w.buf = append(w.buf, p...)</code> |
| `internal/wal/wal.go:368` | wal-value-caller: WAL Entry.Data caller, not a second copy | <code>w.record(recEntry, h[:], e.GetData())</code> |
| `internal/wal/wal.go:386` | wal-write: file I/O/physical copies not enumerated in payload subtotal | <code>n, err := w.seg.Write(w.buf)</code> |
| `internal/wal/wal.go:416` | wal-sync: durability I/O, not byte materialization counted here | <code>if err := w.seg.Sync(); err != nil {</code> |
| `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc.pb.go:760` | lib-put-value-field: Put.Value is proto3 bytes | <code>Value []byte `protobuf:&quot;bytes,2,opt,name=value,proto3&quot; json:&quot;value,omitempty&quot;`</code> |
| `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc_grpc.pb.go:248` | lib-put-decoder: main generated decoder; batch/wire use own frontend | <code>if err := dec(in); err != nil {</code> |
| `dependency-sources/grpc-v1.84.0/encoding/proto/proto.go:96` | lib-grpc-decode: main default protobuf decode | <code>return proto.Unmarshal(buf.ReadOnlyData(), vv)</code> |
| `dependency-sources/grpc-v1.84.0/mem/buffer_slice.go:115` | lib-grpc-materialize: conditional additional main multi-buffer materialization, excluded fixed subtotal | <code>s.CopyTo(*buf)</code> |
| `dependency-sources/grpc-v1.84.0/mem/buffer_slice.go:82` | lib-grpc-copyto: conditional materialization actual byte-copy instruction | <code>off += copy(dst[off:], b.ReadOnlyData())</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5497` | lib-proto3-decode-copy: counted codec materialization proof for main/batch frontend and apply; removed wire Put | <code>*p.Bytes() = append(([]byte)(nil), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5418` | lib-proto2-decode-copy: counted receiver Entry.Data decode copy all targets | <code>*p.Bytes() = append(emptyBuf[:], v...)</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` | lib-bytes-encode-copy: counted bytes encode copy proof main/batch entry encoding and all-target peer encoding | <code>return append(AppendVarint(b, uint64(len(v))), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:468` | lib-bytes-consume-alias: wire ParsePut gets subslice, not copied bytes | <code>return b[n:][:m], n + int(m)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:817` | lib-raft-clone: counted leader Entry.Data deep-clone boundary: 1/Put all targets | <code>cloned[i] = proto.Clone(es[i]).(*pb.Entry)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1349` | lib-raft-propose-clone-call: leader successful proposal invokes appendEntry clone | <code>if !r.appendEntry(m.GetEntries()...) {</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:94` | lib-raft-propose-alias: RawNode boundary aliases data; later appendEntry clones it | <code>Entries: []*pb.Entry{{Data: data}},</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.proto:1` | lib-entry-proto2: Entry.Data uses proto2 bytes, not proto3 Put decoder | <code>syntax = &quot;proto2&quot;;</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.pb.go:348` | lib-entry-data-field: Entry.Data bytes descriptor | <code>Data          []byte                 `protobuf:&quot;bytes,4,opt,name=Data&quot; json:&quot;Data,omitempty&quot;`</code> |
| `dependency-sources/protobuf-v1.36.11/proto/merge.go:58` | lib-proto-clone-merge: Clone creates destination then merges fields | <code>mergeOptions{}.mergeMessage(dst, src)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188` | lib-proto2-clone-copy: counted full Entry.Data byte-copy instruction for leader Raft clone | <code>*dst.Bytes() = append(emptyBuf[:], *src.Bytes()...)</code> |
| `dependency-sources/raft-v3.7.0/storage.go:318` | lib-storage-pointer-append1: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents[:offset:offset], entries...)</code> |
| `dependency-sources/raft-v3.7.0/storage.go:320` | lib-storage-pointer-append2: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents, entries...)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:656` | lib-peer-construction-alias: zero payload copies when constructing append Message | <code>Entries: ents,</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:278` | lib-proto3-codec-selection: no-presence bytes coder selection for Put.Value | <code>return nil, coderBytesNoZero</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:428` | lib-proto2-codec-selection: presence bytes coder selection for Entry.Data | <code>return nil, coderBytes</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1800` | follower-maybeappend: follower uses maybeAppend, not leader appendEntry deep clone | <code>if mlastIndex, ok := r.raftLog.maybeAppend(a, m.GetCommit()); ok {</code> |
| `dependency-sources/raft-v3.7.0/log.go:127` | follower-logappend: follower retains entry pointers | <code>l.append(a.entries[ci-offset:]...)</code> |
| `dependency-sources/raft-v3.7.0/log_unstable.go:205` | unstable-append: append []*Entry pointers, not Data bytes | <code>u.entries = append(u.entries, ents...)</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:228` | async-append-alias: local Raft async append message retains entry pointers | <code>Entries: rd.Entries,</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:393` | async-apply-alias: local Raft async apply message retains entry pointers | <code>Entries: ents,</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5483` | proto3-bytes-encoder: bytes encoders call AppendBytes; not an additional copy | <code>b = protowire.AppendBytes(b, v)</code> |
| `internal/core/watch.go:272` | no-watch-fastpath: no-watch match returns without value encoding | <code>if len(changes) == 0 &#124;&#124; len(n.watchers) == 0 {</code> |
| `internal/server/h2.go:495` | receive-data-caller: Framer DATA dispatched to owned message accumulation | <code>if err := st.receive(f.Data()); err != nil {</code> |
| `internal/server/batch.go:170` | completion-cas: completion publish: CAS, no Go channel and no completion mutex | <code>if c.completions.CompareAndSwap(n.next, n) {</code> |
| `internal/server/batch.go:180` | completion-swap: writer collects completion list atomically; zero channel ops | <code>for n := c.completions.Swap(nil); n != nil; n = n.next {</code> |
| `internal/server/batch.go:100` | charge-request: accounting metadata only; no payload copy | <code>c.chargeLocked(&amp;st.reqHeld, st.reqSize+callCost)</code> |
| `internal/server/batch.go:209` | release-request: writer retires request charge; no room send | <code>c.releaseLocked(&amp;f.st.reqHeld)</code> |
| `internal/server/batch.go:215` | charge-reply: accounts encoded reply until sent; no payload copy | <code>c.chargeLocked(&amp;f.st.replyHeld, len(f.msg))</code> |

## wire — `f2edb7b2b1e81f65d7ca7592638654f09217b933`

| File:line | ID / classification | Exact source line (trimmed) |
|---|---|---|
| `internal/node/node.go:136` | admission-send: counted send: 1/Put main, 1/admission batch batch/wire | <code>case n.inbox &lt;- in:</code> |
| `internal/node/node.go:335` | inbox-receive-outer: counted receive: alternative outer inbox receive; admission or inbound peer | <code>case in := &lt;-inbox:</code> |
| `internal/node/node.go:340` | inbox-receive-drain: counted receive: alternative drain inbox receive, NOT an extra receipt | <code>case in := &lt;-n.inbox:</code> |
| `internal/node/node.go:420` | do-reply-send: counted main reply send 1/Put; retained Do only, excluded on batch/wire Put | <code>w.reply &lt;- r</code> |
| `internal/node/node.go:286` | do-reply-receive: counted main reply receive 1/Put; retained Do only, excluded on batch/wire Put | <code>case rep := &lt;-r.reply:</code> |
| `internal/node/node.go:281` | do-reply-create: creation, not send/receive: 1/Put main; excluded batch/wire Put | <code>r := &amp;request{id: n.nextID.Add(1), op: op, reply: make(chan core.Reply, 1)}</code> |
| `internal/node/node.go:432` | completion-direct: direct grouped call: zero channel ops | <code>g.to.Complete(g.cs)</code> |
| `internal/node/node.go:353` | disk-send: counted send: 1/persist queue array | <code>case toDisk &lt;- next:</code> |
| `internal/node/node.go:486` | disk-receive-outer: counted receive: initial array; alternative to drain | <code>case jobs = &lt;-n.persistQ:</code> |
| `internal/node/node.go:493` | disk-receive-drain: counted receive: drained array; NOT an extra receive for initial array | <code>case js := &lt;-n.persistQ:</code> |
| `internal/node/node.go:507` | disk-complete-send: counted send: 1/sync completion group | <code>case n.persisted &lt;- done:</code> |
| `internal/node/node.go:346` | disk-complete-receive: counted receive: 1/sync completion group | <code>case done := &lt;-n.persisted:</code> |
| `internal/node/node.go:357` | tick-receive: counted selected background receive, not fixed per Put | <code>case &lt;-ticker.C:</code> |
| `internal/node/node.go:266` | peer-inbox-send: counted send: 1/delivered inbound peer message | <code>case n.inbox &lt;- input{msg: m}:</code> |
| `internal/node/node.go:274` | unreachable-send: fault/drop-only successful send, excluded normal healthy transport ledger | <code>case n.inbox &lt;- input{unreachable: id}:</code> |
| `internal/node/node.go:308` | shutdown-close: close: shutdown-only, zero per normal Put | <code>close(n.done)</code> |
| `internal/node/node.go:394` | watch-close: close: watch overflow, outside no-watch scope | <code>close(ws.done)</code> |
| `internal/node/node.go:500` | wal-append-call: driver disk trace; no extra payload copy beyond WAL record | <code>n.wal.Append(j.HardState, j.Entries)</code> |
| `internal/node/node.go:503` | wal-sync-call: driver disk trace; I/O, not counted as extra userspace payload materialization | <code>if err := n.wal.Sync(); err != nil {</code> |
| `internal/node/node.go:190` | batch-api: direct API, not channel operation | <code>func (n *Node) SubmitBatch(ctx context.Context, reqs []core.Request, tags []any, to Completer) error {</code> |
| `internal/node/node.go:447` | batch-core-submit: descriptor/header handoff: zero payload copies | <code>n.core.Submit(*r)</code> |
| `internal/peer/peer.go:297` | peer-queue-send: counted send: 1/queued outbound peer message | <code>case q &lt;- m:</code> |
| `internal/peer/peer.go:308` | peer-queue-receive-fast: counted receive alternative 1: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:314` | peer-queue-receive-control: counted receive alternative 2: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:317` | peer-queue-receive-data: counted receive alternative 3: one receipt per message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:331` | peer-drop-control: fault-only receive: dropped queued control message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:333` | peer-drop-data: fault-only receive: dropped queued data message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:350` | peer-backoff-receive: fault-only timer receive, excluded healthy path | <code>case &lt;-time.After(backoff):</code> |
| `internal/peer/peer.go:320` | peer-shutdown-receive-1: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:351` | peer-shutdown-receive-2: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:419` | peer-value-encode: counted copy: 1/serialized occurrence of Entry.Data containing this Put, on sender | <code>buf, err = proto.MarshalOptions{}.MarshalAppend(buf[:0], m)</code> |
| `internal/peer/peer.go:241` | peer-value-decode: counted copy: 1/received occurrence of Entry.Data, on receiver; dependency decoder below | <code>if err := proto.Unmarshal(buf, m); err != nil {</code> |
| `internal/server/h2.go:190` | writer-wake-send: counted successful conditional/coalesced wake send; default is not operation | <code>case c.wake &lt;- struct{}{}:</code> |
| `internal/server/write.go:54` | writer-wake-receive: counted selected wake receive only while idle | <code>case &lt;-c.wake:</code> |
| `internal/server/h2.go:369` | credit-wake-attempt: wake attempt per DATA frame, not a guaranteed successful send | <code>c.signal()</code> |
| `internal/server/batch.go:190` | completion-wake-attempt: wake attempt per completion group, not guaranteed successful send | <code>c.signal()</code> |
| `internal/server/h2.go:179` | goaway-wake-attempt: lifecycle wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:203` | control-wake-attempt: control wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:316` | settings-wake-attempt: settings wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:331` | window-wake-attempt: send-window wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:359` | reset-wake-attempt: reset wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:514` | unary-nil-signal: nil unary channel: zero successful sends; default wins | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/write.go:132` | unary-nil-space: nil unary channel: zero successful sends; default wins | <code>case st.space &lt;- struct{}{}:</code> |
| `internal/server/stream.go:127` | frontend-accumulation: counted copy: 1 full-value equivalent over all request chunks | <code>st.msg = append(st.msg, d[:n]...)</code> |
| `internal/server/stream.go:124` | frontend-prealloc: full request-sized capacity prevents accumulation growth; not a separate copy | <code>st.msg = make([]byte, 0, size)</code> |
| `internal/server/rpc.go:63` | frontend-decode: counted batch frontend copy 1/Put; wire ordinary Put bypasses this site | <code>if err := proto.Unmarshal(b, m); err != nil {</code> |
| `internal/server/batch.go:59` | wire-frontend-parse: wire frontend parse: zero value copies | <code>put, err := core.ParsePut(st.body)</code> |
| `internal/server/batch.go:125` | batch-submit-call: one admission API call per actual batch | <code>err := c.s.n.SubmitBatch(c.ctx, b.reqs, b.tags, c)</code> |
| `internal/server/batch.go:222` | batch-reply-encode: zero input-value copies: ordinary reply contains no value; wire bypasses | <code>f.msg, f.err = marshal(cp.Resp)</code> |
| `internal/server/batch.go:220` | wire-reply-encode: wire reply branch: no generic PutResponse construction; zero input-value copies | <code>f.msg, f.err = c.putResponse(&amp;cp.Reply)</code> |
| `internal/server/stream.go:207` | response-proto-encode: ordinary response materialization: zero input-value copies | <code>b, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(b, m)</code> |
| `internal/server/write.go:56` | node-done-receive: shutdown-only receive; obtaining Done() itself is not operation | <code>case &lt;-nodeDone:</code> |
| `internal/server/server.go:189` | server-stop-close: close: server Stop wait channel, not per Put | <code>close(done)</code> |
| `internal/server/h2.go:522` | stream-admit: ordinary Put is synchronous reader admission, no per-Put handler goroutine | <code>c.admit(st)</code> |
| `internal/server/h2.go:445` | stream-context-gate: ordinary batch/wire Put creates no per-stream context | <code>if !grpc &#124;&#124; (st.rpc != nil &amp;&amp; st.rpc.op == nil) {</code> |
| `internal/server/h2.go:457` | stream-deadline: optional timer callback, not a Timer.C send/receive | <code>st.deadline = time.AfterFunc(timeout, st.expire)</code> |
| `internal/server/write.go:193` | stream-end-release: release accounting/flow control; no room channel exists at these targets | <code>c.releaseLocked(&amp;st.replyHeld)</code> |
| `internal/server/batch.go:269` | manual-response-prefix: wire small response buffer, no value bytes | <code>b := make([]byte, 5, 5+putResponseSize)</code> |
| `internal/server/batch.go:270` | manual-response-append: wire small response, no value bytes when PrevKv nil | <code>b, err := appendPutResponse(b, c.s.n.ClusterID(), c.s.n.ID(), r.Rev, r.Term, r.PrevKv)</code> |
| `internal/core/codec.go:68` | entry-value-encode: counted copy: 1 per Put into encoded entry; NOT 1/batch-size per Put | <code>buf = append(buf, p.req.Put.Wire...)</code> |
| `internal/core/codec.go:37` | entry-buffer-init: main/batch grow; wire whole-batch upper-bound capacity, no encode growth for scoped path | <code>buf := binary.AppendUvarint(make([]byte, 0, size+binary.MaxVarintLen64), uint64(len(ps)))</code> |
| `internal/core/node.go:571` | raft-propose: leader proposal -> dependency clone: counted separately below | <code>if err := n.rn.Propose(data); err != nil {</code> |
| `internal/core/node.go:566` | core-encode-call: entry encode caller, not an additional copy | <code>data, err := encodeBatch(n.cfg.ID, n.cfg.Boot, props, size)</code> |
| `internal/core/node.go:839` | apply-dispatch: main/batch decoded apply vs wire in-place walk | <code>err := walkBatch(e.GetData(), func(origin, boot, id uint64, kind byte, b []byte) error {</code> |
| `internal/core/node.go:844` | apply-wire-kind: Put bypasses generic apply protobuf decode on every replica | <code>if kind == kindPut {</code> |
| `internal/core/node.go:880` | apply-wire-parse: parse entry in place: zero value copies | <code>put, err := ParsePut(b)</code> |
| `internal/core/node.go:884` | apply-wire-state: state fields forwarded: zero value copies | <code>prev, err := n.state.ApplyPut(&amp;put.PutOp)</code> |
| `internal/core/node.go:905` | apply-wire-reply: header/metadata reply, not a value copy | <code>n.out.Replies = append(n.out.Replies, Reply{ID: p.req.ID, Rev: n.kv.Rev(), Term: n.term, PrevKv: prev})</code> |
| `internal/core/node.go:851` | generic-apply-still-exists: Txn/Delete/lease/etc apply decode still exists; excluded ordinary Put | <code>if err := proto.Unmarshal(b, op); err != nil {</code> |
| `internal/core/codec.go:145` | decode-batch-still-exists: generic helper used by checks, NOT ordinary production Put apply | <code>if err := proto.Unmarshal(b, op); err != nil {</code> |
| `internal/core/wire.go:61` | wire-parse-consume: dependency returns subslice: zero copy | <code>v, n := protowire.ConsumeBytes(b)</code> |
| `internal/core/wire.go:67` | wire-parse-clip: slice capacity clip, no byte copy | <code>v = slices.Clip(v)</code> |
| `internal/core/wire.go:71` | wire-value-alias: value slice aliases Wire/Entry.Data | <code>p.Value = v</code> |
| `internal/core/node.go:501` | wire-size: whole client request size plus framing upper bound | <code>size := len(p.req.Put.Wire) + batchFraming</code> |
| `internal/core/node.go:372` | wire-check: validate fields/size without generic protobuf copy | <code>if err := checkPut(&amp;r.Put); err != nil {</code> |
| `internal/mvcc/ops.go:259` | index-value: zero value copy: store slice header; wire aliases entry, others decoded bytes | <code>v := version{create: w.begin + 1, ver: 1, lease: p.Lease, value: p.Value}</code> |
| `internal/mvcc/ops.go:250` | index-version-append: metadata/slice headers only: zero payload copies | <code>k.vers = append(k.vers, v)</code> |
| `internal/mvcc/ops.go:277` | index-key: zero key bytes copy at insert: retains key slice; key copies are not value copies | <code>k = &amp;keyIndex{key: p.Key}</code> |
| `internal/core/state.go:110` | wire-state-index: zero value copy: direct MVCC fields path | <code>prev, err := st.kv.PutKV(op)</code> |
| `internal/core/storage.go:39` | storage-pointer-append: MemoryStorage appends entry pointers, not Entry.Data bytes | <code>if err := s.Append(job.Entries); err != nil {</code> |
| `internal/wal/wal.go:351` | wal-value-copy: counted copy: 1 direct Entry.Data append per persisted entry containing Put | <code>w.buf = append(w.buf, p...)</code> |
| `internal/wal/wal.go:368` | wal-value-caller: WAL Entry.Data caller, not a second copy | <code>w.record(recEntry, h[:], e.GetData())</code> |
| `internal/wal/wal.go:386` | wal-write: file I/O/physical copies not enumerated in payload subtotal | <code>n, err := w.seg.Write(w.buf)</code> |
| `internal/wal/wal.go:416` | wal-sync: durability I/O, not byte materialization counted here | <code>if err := w.seg.Sync(); err != nil {</code> |
| `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc.pb.go:760` | lib-put-value-field: Put.Value is proto3 bytes | <code>Value []byte `protobuf:&quot;bytes,2,opt,name=value,proto3&quot; json:&quot;value,omitempty&quot;`</code> |
| `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc_grpc.pb.go:248` | lib-put-decoder: main generated decoder; batch/wire use own frontend | <code>if err := dec(in); err != nil {</code> |
| `dependency-sources/grpc-v1.84.0/encoding/proto/proto.go:96` | lib-grpc-decode: main default protobuf decode | <code>return proto.Unmarshal(buf.ReadOnlyData(), vv)</code> |
| `dependency-sources/grpc-v1.84.0/mem/buffer_slice.go:115` | lib-grpc-materialize: conditional additional main multi-buffer materialization, excluded fixed subtotal | <code>s.CopyTo(*buf)</code> |
| `dependency-sources/grpc-v1.84.0/mem/buffer_slice.go:82` | lib-grpc-copyto: conditional materialization actual byte-copy instruction | <code>off += copy(dst[off:], b.ReadOnlyData())</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5497` | lib-proto3-decode-copy: counted codec materialization proof for main/batch frontend and apply; removed wire Put | <code>*p.Bytes() = append(([]byte)(nil), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5418` | lib-proto2-decode-copy: counted receiver Entry.Data decode copy all targets | <code>*p.Bytes() = append(emptyBuf[:], v...)</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` | lib-bytes-encode-copy: counted bytes encode copy proof main/batch entry encoding and all-target peer encoding | <code>return append(AppendVarint(b, uint64(len(v))), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:468` | lib-bytes-consume-alias: wire ParsePut gets subslice, not copied bytes | <code>return b[n:][:m], n + int(m)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:817` | lib-raft-clone: counted leader Entry.Data deep-clone boundary: 1/Put all targets | <code>cloned[i] = proto.Clone(es[i]).(*pb.Entry)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1349` | lib-raft-propose-clone-call: leader successful proposal invokes appendEntry clone | <code>if !r.appendEntry(m.GetEntries()...) {</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:94` | lib-raft-propose-alias: RawNode boundary aliases data; later appendEntry clones it | <code>Entries: []*pb.Entry{{Data: data}},</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.proto:1` | lib-entry-proto2: Entry.Data uses proto2 bytes, not proto3 Put decoder | <code>syntax = &quot;proto2&quot;;</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.pb.go:348` | lib-entry-data-field: Entry.Data bytes descriptor | <code>Data          []byte                 `protobuf:&quot;bytes,4,opt,name=Data&quot; json:&quot;Data,omitempty&quot;`</code> |
| `dependency-sources/protobuf-v1.36.11/proto/merge.go:58` | lib-proto-clone-merge: Clone creates destination then merges fields | <code>mergeOptions{}.mergeMessage(dst, src)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188` | lib-proto2-clone-copy: counted full Entry.Data byte-copy instruction for leader Raft clone | <code>*dst.Bytes() = append(emptyBuf[:], *src.Bytes()...)</code> |
| `dependency-sources/raft-v3.7.0/storage.go:318` | lib-storage-pointer-append1: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents[:offset:offset], entries...)</code> |
| `dependency-sources/raft-v3.7.0/storage.go:320` | lib-storage-pointer-append2: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents, entries...)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:656` | lib-peer-construction-alias: zero payload copies when constructing append Message | <code>Entries: ents,</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:278` | lib-proto3-codec-selection: no-presence bytes coder selection for Put.Value | <code>return nil, coderBytesNoZero</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:428` | lib-proto2-codec-selection: presence bytes coder selection for Entry.Data | <code>return nil, coderBytes</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1800` | follower-maybeappend: follower uses maybeAppend, not leader appendEntry deep clone | <code>if mlastIndex, ok := r.raftLog.maybeAppend(a, m.GetCommit()); ok {</code> |
| `dependency-sources/raft-v3.7.0/log.go:127` | follower-logappend: follower retains entry pointers | <code>l.append(a.entries[ci-offset:]...)</code> |
| `dependency-sources/raft-v3.7.0/log_unstable.go:205` | unstable-append: append []*Entry pointers, not Data bytes | <code>u.entries = append(u.entries, ents...)</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:228` | async-append-alias: local Raft async append message retains entry pointers | <code>Entries: rd.Entries,</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:393` | async-apply-alias: local Raft async apply message retains entry pointers | <code>Entries: ents,</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5483` | proto3-bytes-encoder: bytes encoders call AppendBytes; not an additional copy | <code>b = protowire.AppendBytes(b, v)</code> |
| `internal/core/watch.go:272` | no-watch-fastpath: no-watch match returns without value encoding | <code>if len(changes) == 0 &#124;&#124; len(n.watchers) == 0 {</code> |
| `internal/core/codec.go:103` | wire-batch-slice: walkBatch calls apply with capped entry subslice | <code>if err := fn(origin, boot, id, kind, data[:size:size]); err != nil {</code> |
| `internal/server/h2.go:495` | receive-data-caller: Framer DATA dispatched to owned message accumulation | <code>if err := st.receive(f.Data()); err != nil {</code> |
| `internal/server/batch.go:186` | completion-cas: completion publish: CAS, no Go channel and no completion mutex | <code>if c.completions.CompareAndSwap(n.next, n) {</code> |
| `internal/server/batch.go:196` | completion-swap: writer collects completion list atomically; zero channel ops | <code>for n := c.completions.Swap(nil); n != nil; n = n.next {</code> |
| `internal/server/batch.go:116` | charge-request: accounting metadata only; no payload copy | <code>c.chargeLocked(&amp;st.reqHeld, st.reqSize+callCost)</code> |
| `internal/server/batch.go:229` | release-request: writer retires request charge; no room send | <code>c.releaseLocked(&amp;f.st.reqHeld)</code> |
| `internal/server/batch.go:235` | charge-reply: accounts encoded reply until sent; no payload copy | <code>c.chargeLocked(&amp;f.st.replyHeld, len(f.msg))</code> |

