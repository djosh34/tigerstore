# Exact verified sites
Every line asserted; every repository source file compared to git archive of its SHA. Dependency evidence matches module ZIP bytes and go.sum h1. A call and its dependency instruction are ONE materialization.

## main — `8efd83a173394dd94a3981c72b4c51a1f3d061f1`
| File:line | Site / classification | Exact text (trimmed) |
|---|---|---|
| `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc.pb.go:760` | lib-put-value-field: Put.Value is proto3 bytes | <code>Value []byte `protobuf:&quot;bytes,2,opt,name=value,proto3&quot; json:&quot;value,omitempty&quot;`</code> |
| `dependency-sources/etcd-api-v3.7.2/mvccpb/kv.pb.go:84` | lib-returned-value-field: returned stored value is proto3 bytes: same owning encoder behavior | <code>Value []byte `protobuf:&quot;bytes,5,opt,name=value,proto3&quot; json:&quot;value,omitempty&quot;`</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` | lib-bytes-encode-copy: counted encoding instruction: main entry or peer Entry.Data or CAS failure reply, each boundary counted only once | <code>return append(AppendVarint(b, uint64(len(v))), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:468` | lib-bytes-consume-alias: wire ParsePut gets subslice, not copied bytes | <code>return b[n:][:m], n + int(m)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5418` | lib-proto2-decode-copy: counted receiver Entry.Data decode copy all targets | <code>*p.Bytes() = append(emptyBuf[:], v...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5483` | proto3-bytes-encoder: bytes encoders call AppendBytes; not an additional copy | <code>b = protowire.AppendBytes(b, v)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5497` | lib-proto3-decode-copy: one owning proto3 bytes decode stage: main/wire/hpack CAS frontend/apply; ordinary Put bypasses ALL targets; not txn CAS | <code>*p.Bytes() = append(([]byte)(nil), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:278` | lib-proto3-codec-selection: proto3 Put.Value and KeyValue.Value non-presence bytes codec selection | <code>return nil, coderBytesNoZero</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:428` | lib-proto2-codec-selection: presence bytes coder selection for Entry.Data | <code>return nil, coderBytes</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/convert.go:358` | reflect-bytes-convert: []byte-to-[]byte conversion retains backing store; no append/copy | <code>return reflect.ValueOf(v.Bytes()).Convert(c.goType)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188` | lib-proto2-clone-copy: counted: Raft leader Entry.Data deep clone, proto2 (not Put proto3 decoder) | <code>*dst.Bytes() = append(emptyBuf[:], *src.Bytes()...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/message_reflect_field.go:317` | reflect-scalar-set: Put.Value scalar setter copies slice header, not value bytes | <code>rv.Set(conv.GoValueOf(v))</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/message_reflect_gen.go:97` | reflect-set: reflection Set dispatch, no byte clone | <code>fi.set(m.pointer(), v)</code> |
| `dependency-sources/protobuf-v1.36.11/proto/merge.go:58` | lib-proto-clone-merge: Clone creates destination then merges fields | <code>mergeOptions{}.mergeMessage(dst, src)</code> |
| `dependency-sources/raft-v3.7.0/log.go:127` | follower-logappend: follower retains entry pointers | <code>l.append(a.entries[ci-offset:]...)</code> |
| `dependency-sources/raft-v3.7.0/log_unstable.go:205` | unstable-append: append []*Entry pointers, not Data bytes | <code>u.entries = append(u.entries, ents...)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:656` | lib-peer-construction-alias: zero payload copies when constructing append Message | <code>Entries: ents,</code> |
| `dependency-sources/raft-v3.7.0/raft.go:817` | lib-raft-clone: counted leader Entry.Data deep-clone boundary: 1/Put all targets | <code>cloned[i] = proto.Clone(es[i]).(*pb.Entry)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1349` | lib-raft-propose-clone-call: leader successful proposal invokes appendEntry clone | <code>if !r.appendEntry(m.GetEntries()...) {</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1800` | follower-maybeappend: follower uses maybeAppend, not leader appendEntry deep clone | <code>if mlastIndex, ok := r.raftLog.maybeAppend(a, m.GetCommit()); ok {</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.pb.go:348` | lib-entry-data-field: Entry.Data bytes descriptor | <code>Data          []byte                 `protobuf:&quot;bytes,4,opt,name=Data&quot; json:&quot;Data,omitempty&quot;`</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.proto:1` | lib-entry-proto2: Entry.Data uses proto2 bytes, not proto3 Put decoder | <code>syntax = &quot;proto2&quot;;</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:94` | lib-raft-propose-alias: RawNode boundary aliases data; later appendEntry clones it | <code>Entries: []*pb.Entry{{Data: data}},</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:228` | async-append-alias: local Raft async append message retains entry pointers | <code>Entries: rd.Entries,</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:393` | async-apply-alias: local Raft async apply message retains entry pointers | <code>Entries: ents,</code> |
| `dependency-sources/raft-v3.7.0/storage.go:318` | lib-storage-pointer-append1: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents[:offset:offset], entries...)</code> |
| `dependency-sources/raft-v3.7.0/storage.go:320` | lib-storage-pointer-append2: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents, entries...)</code> |
| `internal/core/codec.go:37` | entry-buffer-init: entry capacity: main/wire/hpack/txn fully size-reserved; no extra fixed relocation invented | <code>buf := binary.AppendUvarint(make([]byte, 0, size+binary.MaxVarintLen64), uint64(len(ps)))</code> |
| `internal/core/codec.go:68` | entry-value-encode: counted: one copy of each full input value into encoded entry, NOT amortized payload bytes | <code>buf = append(buf, p.req.Put.Wire...)</code> |
| `internal/core/codec.go:73` | generic-entry-value-encode: one entire CAS input-value copy for main/wire/hpack; txn Wire bypasses generic encoder | <code>buf, err = proto.MarshalOptions{Deterministic: true, UseCachedSize: true}.MarshalAppend(buf, p.req.Op)</code> |
| `internal/core/codec.go:103` | wire-batch-slice: zero copy: capped command subslice passed to apply | <code>if err := fn(origin, boot, id, kind, data[:size:size]); err != nil {</code> |
| `internal/core/codec.go:122` | new-txn-op: apply still creates fresh TxnRequest struct, including txn alias path | <code>return &amp;pb.TxnRequest{}, nil</code> |
| `internal/core/codec.go:145` | decode-batch-still-exists: generic helper used by checks, NOT ordinary production Put apply | <code>if err := proto.Unmarshal(b, op); err != nil {</code> |
| `internal/core/node.go:145` | pending-owner: pending record retains request slice headers; zero value copy | <code>req      Request</code> |
| `internal/core/node.go:372` | wire-check: validate fields/size without generic protobuf copy | <code>if err := checkPut(&amp;r.Put); err != nil {</code> |
| `internal/core/node.go:394` | txn-write-route: only wholly read-only transactions take read route | <code>if mvcc.IsTxnReadOnly(op) {</code> |
| `internal/core/node.go:501` | wire-size: whole client request size plus framing upper bound | <code>size := len(p.req.Put.Wire) + batchFraming</code> |
| `internal/core/node.go:566` | core-encode-call: entry encode caller, not an additional copy | <code>data, err := encodeBatch(n.cfg.ID, n.cfg.Boot, props, size)</code> |
| `internal/core/node.go:571` | raft-propose: leader proposal -> dependency clone: counted separately below | <code>if err := n.rn.Propose(data); err != nil {</code> |
| `internal/core/node.go:839` | apply-dispatch: main/batch decoded apply vs wire in-place walk | <code>err := walkBatch(e.GetData(), func(origin, boot, id uint64, kind byte, b []byte) error {</code> |
| `internal/core/node.go:844` | apply-wire-kind: Put bypasses generic apply protobuf decode on every replica | <code>if kind == kindPut {</code> |
| `internal/core/node.go:851` | generic-apply-still-exists: main/wire/hpack CAS owning apply decode; bypassed for ordinary Put | <code>if err := proto.Unmarshal(b, op); err != nil {</code> |
| `internal/core/node.go:880` | apply-wire-parse: parse entry in place: zero value copies | <code>put, err := ParsePut(b)</code> |
| `internal/core/node.go:884` | apply-wire-state: state fields forwarded: zero value copies | <code>prev, err := n.state.ApplyPut(&amp;put.PutOp)</code> |
| `internal/core/node.go:905` | apply-wire-reply: header/metadata reply, not a value copy | <code>n.out.Replies = append(n.out.Replies, Reply{ID: p.req.ID, Rev: n.kv.Rev(), Term: n.term, PrevKv: prev})</code> |
| `internal/core/state.go:110` | wire-state-index: zero copy: direct wire Put fields path into MVCC | <code>prev, err := st.kv.PutKV(op)</code> |
| `internal/core/state.go:140` | state-txn: replicated CAS apply: MVCC chooses branch | <code>r, err := st.kv.Txn(op)</code> |
| `internal/core/storage.go:39` | storage-pointer-append: MemoryStorage appends entry pointers, not Entry.Data bytes | <code>if err := s.Append(job.Entries); err != nil {</code> |
| `internal/core/watch.go:272` | no-watch-fastpath: no-watch match returns without value encoding | <code>if len(changes) == 0 &#124;&#124; len(n.watchers) == 0 {</code> |
| `internal/core/wire.go:61` | wire-parse-consume: dependency returns subslice: zero copy | <code>v, n := protowire.ConsumeBytes(b)</code> |
| `internal/core/wire.go:67` | wire-parse-clip: slice capacity clip, no byte copy | <code>v = slices.Clip(v)</code> |
| `internal/core/wire.go:71` | wire-value-alias: value slice aliases Wire/Entry.Data | <code>p.Value = v</code> |
| `internal/mvcc/ops.go:32` | revision-changed: MVCC revision changes only for actual state mutation | <code>if w.changed {</code> |
| `internal/mvcc/ops.go:85` | range-kv: CAS failure Range: KeyValue objects and slice headers only; zero value copy | <code>kvs = append(kvs, k.kv(v))</code> |
| `internal/mvcc/ops.go:191` | putop-alias: PutRequest-to-PutOp copies slice headers, not bytes | <code>return PutOp{Key: p.Key, Value: p.Value, Lease: p.Lease, PrevKv: p.PrevKv, IgnoreValue: p.IgnoreValue, IgnoreLease: p.IgnoreLease}</code> |
| `internal/mvcc/ops.go:250` | index-version-append: metadata/slice headers only: zero payload copies | <code>k.vers = append(k.vers, v)</code> |
| `internal/mvcc/ops.go:259` | index-value: zero payload copies: entry backing store for ordinary Put ALL targets and txn CAS; decoded bytes for main/wire/hpack CAS | <code>v := version{create: w.begin + 1, ver: 1, lease: p.Lease, value: p.Value}</code> |
| `internal/mvcc/ops.go:277` | index-key: zero key bytes copy at insert: retains key slice; key copies are not value copies | <code>k = &amp;keyIndex{key: p.Key}</code> |
| `internal/mvcc/ops.go:320` | compare-path: compare selects branch at replicated apply | <code>path := w.compareToPath(r)</code> |
| `internal/mvcc/ops.go:328` | txn-revision: CAS success increments revision; compare failure keeps current MVCC revision | <code>resp.Header = header(w.cur())</code> |
| `internal/mvcc/ops.go:366` | compare-kv: existing-key compare constructs KeyValue with aliased stored value; zero value copy | <code>if !compareKV(c, k.kv(v)) {</code> |
| `internal/mvcc/ops.go:390` | compare-mod: CAS compare uses integer mod revision, not value bytes | <code>result = cmpInt(kv.ModRevision, c.GetModRevision())</code> |
| `internal/mvcc/ops.go:470` | txn-success: Succeeded reports compare result; failure is still successful RPC | <code>Succeeded: path[0],</code> |
| `internal/mvcc/ops.go:478` | txn-range: compare failure: executes current Range only; return existing stored value | <code>rr, err := w.rangeKeys(t.RequestRange)</code> |
| `internal/mvcc/ops.go:485` | txn-put: success Put executes; failure skips this branch | <code>out.Response = &amp;pb.ResponseOp_ResponsePut{ResponsePut: &amp;pb.PutResponse{Header: header(w.begin + 1), PrevKv: w.put(&amp;op)}}</code> |
| `internal/mvcc/store.go:73` | kv-value-alias: zero stored-value copy: response/compare KeyValue retains value slice header | <code>Value:          v.value,</code> |
| `internal/mvcc/validate.go:185` | txn-readonly: both branches classified before compare; a Put in Success forces write/Raft route | <code>for _, ops := range [][]*pb.RequestOp{r.Success, r.Failure} {</code> |
| `internal/mvcc/validate.go:187` | txn-write-detect: success Put means not read-only even when compare will fail | <code>if op.GetRequestRange() == nil {</code> |
| `internal/node/node.go:136` | admission-send: counted: one send per actual admission batch, all targets | <code>case n.inbox &lt;- in:</code> |
| `internal/node/node.go:169` | driver-batch-owner: driver batch owns/shared request descriptors; header-only, not channel | <code>reqs  []core.Request</code> |
| `internal/node/node.go:190` | batch-api: direct API, not channel operation | <code>func (n *Node) SubmitBatch(ctx context.Context, reqs []core.Request, tags []any, to Completer) error {</code> |
| `internal/node/node.go:199` | batch-wrapper: batch wrapper retains descriptor/tag backing stores; no payload copy or channel exchange | <code>b := &amp;batch{first: n.nextID.Add(k) - k + 1, reqs: reqs, tags: tags, to: to}</code> |
| `internal/node/node.go:200` | batch-inbox-call: one inbox admission handoff per batch via actual send site; this call is not an extra exchange | <code>return n.put(ctx, input{batch: b})</code> |
| `internal/node/node.go:266` | peer-inbox-send: counted send: 1/delivered inbound peer message | <code>case n.inbox &lt;- input{msg: m}:</code> |
| `internal/node/node.go:274` | unreachable-send: conditional counted U send when nonblocking Unreachable report succeeds; saturation/drop/fault, not fixed per request | <code>case n.inbox &lt;- input{unreachable: id}:</code> |
| `internal/node/node.go:281` | do-reply-create: excluded retained Node.Do channel creation: zero per ordinary Put or CAS | <code>r := &amp;request{id: n.nextID.Add(1), op: op, reply: make(chan core.Reply, 1)}</code> |
| `internal/node/node.go:286` | do-reply-receive: excluded retained Node.Do: not ordinary Put or CAS | <code>case rep := &lt;-r.reply:</code> |
| `internal/node/node.go:308` | shutdown-close: close: shutdown-only, zero per normal Put | <code>close(n.done)</code> |
| `internal/node/node.go:335` | inbox-receive-outer: counted selected receive: outer alternative for admission, inbound peer or successful Unreachable report | <code>case in := &lt;-inbox:</code> |
| `internal/node/node.go:340` | inbox-receive-drain: counted selected receive: drain alternative for another input; not extra receipt of outer input | <code>case in := &lt;-n.inbox:</code> |
| `internal/node/node.go:346` | disk-complete-receive: counted receive: 1/sync completion group | <code>case done := &lt;-n.persisted:</code> |
| `internal/node/node.go:353` | disk-send: counted send: 1/persist queue array | <code>case toDisk &lt;- next:</code> |
| `internal/node/node.go:357` | tick-receive: counted selected background receive, not fixed per Put | <code>case &lt;-ticker.C:</code> |
| `internal/node/node.go:394` | watch-close: close: watch overflow, outside no-watch scope | <code>close(ws.done)</code> |
| `internal/node/node.go:420` | do-reply-send: excluded retained Node.Do: not ordinary Put or CAS | <code>w.reply &lt;- r</code> |
| `internal/node/node.go:432` | completion-direct: direct grouped call: zero channel ops | <code>g.to.Complete(g.cs)</code> |
| `internal/node/node.go:446` | batch-waiter: per-request completer/tag metadata, no private reply channel | <code>waiters[r.ID] = waiter{to: b.to, tag: b.tags[i]}</code> |
| `internal/node/node.go:447` | batch-core-submit: descriptor handoff, zero payload copy; main submits individually, wire likewise | <code>n.core.Submit(*r)</code> |
| `internal/node/node.go:486` | disk-receive-outer: counted receive: initial array; alternative to drain | <code>case jobs = &lt;-n.persistQ:</code> |
| `internal/node/node.go:493` | disk-receive-drain: counted receive: drained array; NOT an extra receive for initial array | <code>case js := &lt;-n.persistQ:</code> |
| `internal/node/node.go:500` | wal-append-call: driver disk trace; no extra payload copy beyond WAL record | <code>n.wal.Append(j.HardState, j.Entries)</code> |
| `internal/node/node.go:503` | wal-sync-call: driver disk trace; I/O, not counted as extra userspace payload materialization | <code>if err := n.wal.Sync(); err != nil {</code> |
| `internal/node/node.go:507` | disk-complete-send: counted send: 1/sync completion group | <code>case n.persisted &lt;- done:</code> |
| `internal/peer/peer.go:241` | peer-value-decode: counted copy: 1/received occurrence of Entry.Data, on receiver; dependency decoder below | <code>if err := proto.Unmarshal(buf, m); err != nil {</code> |
| `internal/peer/peer.go:297` | peer-queue-send: counted send: 1/queued outbound peer message | <code>case q &lt;- m:</code> |
| `internal/peer/peer.go:308` | peer-queue-receive-fast: counted receive alternative 1: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:314` | peer-queue-receive-control: counted receive alternative 2: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:317` | peer-queue-receive-data: counted receive alternative 3: one receipt per message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:320` | peer-shutdown-receive-1: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:331` | peer-drop-control: fault-only receive: dropped queued control message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:333` | peer-drop-data: fault-only receive: dropped queued data message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:350` | peer-backoff-receive: fault-only timer receive, excluded healthy path | <code>case &lt;-time.After(backoff):</code> |
| `internal/peer/peer.go:351` | peer-shutdown-receive-2: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:419` | peer-value-encode: counted copy: 1/serialized occurrence of Entry.Data containing this Put, on sender | <code>buf, err = proto.MarshalOptions{}.MarshalAppend(buf[:0], m)</code> |
| `internal/server/batch.go:39` | opbatch-owner: connection admission batch owns request descriptors; header-only, not channel | <code>reqs  []core.Request</code> |
| `internal/server/batch.go:59` | wire-frontend-parse: wire frontend parse: zero value copies | <code>put, err := core.ParsePut(st.body)</code> |
| `internal/server/batch.go:66` | frontend-new-op: request structs still constructed for CAS; ordinary Put ParsePut bypasses ALL targets | <code>req.Op = st.rpc.op()</code> |
| `internal/server/batch.go:116` | charge-request: accounting metadata only; no payload copy | <code>c.chargeLocked(&amp;st.reqHeld, st.reqSize+callCost)</code> |
| `internal/server/batch.go:126` | batch-submit-call: one admission API call per actual batch | <code>err := c.s.n.SubmitBatch(c.ctx, b.reqs, b.tags, c)</code> |
| `internal/server/batch.go:212` | completion-cas: completion publish: CAS, no Go channel and no completion mutex | <code>if c.completions.CompareAndSwap(n.next, n) {</code> |
| `internal/server/batch.go:216` | completion-wake-attempt: wake attempt per completion group, not guaranteed successful send | <code>c.signal()</code> |
| `internal/server/batch.go:225` | completion-swap: writer collects completion list atomically; zero channel ops | <code>for n := c.completions.Swap(nil); n != nil; n = n.next {</code> |
| `internal/server/batch.go:259` | wire-reply-encode: wire Put metadata response: zero ordinary input-value copy | <code>f.msg, f.err = c.putResponse(&amp;cp.Reply)</code> |
| `internal/server/batch.go:261` | batch-reply-encode: Put replies contain no input payload; CAS failure reply contains stored value and copies it once via response codec | <code>f.msg, f.err = marshal(cp.Resp)</code> |
| `internal/server/batch.go:268` | release-request: release accounting, zero channel exchange / payload copy | <code>c.releaseReqLocked(f.st)</code> |
| `internal/server/batch.go:274` | charge-reply: accounts encoded reply until sent; no payload copy | <code>c.chargeLocked(&amp;f.st.replyHeld, len(f.msg))</code> |
| `internal/server/batch.go:308` | manual-response-prefix: wire small response buffer, no value bytes | <code>b := make([]byte, 5, 5+putResponseSize)</code> |
| `internal/server/batch.go:309` | manual-response-append: wire small response, no value bytes when PrevKv nil | <code>b, err := appendPutResponse(b, c.s.n.ClusterID(), c.s.n.ID(), r.Rev, r.Term, r.PrevKv)</code> |
| `internal/server/h2.go:184` | goaway-wake-attempt: lifecycle wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:195` | writer-wake-send: counted successful conditional/coalesced wake send; default is not operation | <code>case c.wake &lt;- struct{}{}:</code> |
| `internal/server/h2.go:208` | control-wake-attempt: control wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:321` | settings-wake-attempt: settings wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:336` | window-wake-attempt: send-window wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:364` | reset-wake-attempt: reset wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:374` | credit-wake-attempt: wake attempt per DATA frame, not a guaranteed successful send | <code>c.signal()</code> |
| `internal/server/h2.go:450` | stream-context-gate: ordinary batch/wire Put creates no per-stream context | <code>if !grpc &#124;&#124; (st.rpc != nil &amp;&amp; st.rpc.op == nil) {</code> |
| `internal/server/h2.go:469` | stream-deadline: optional timer callback, not a Timer.C send/receive | <code>st.deadline = time.AfterFunc(timeout, st.expire)</code> |
| `internal/server/h2.go:507` | receive-data-caller: Framer DATA dispatched to owned message accumulation | <code>if err := st.receive(f.Data()); err != nil {</code> |
| `internal/server/h2.go:526` | unary-nil-signal: nil unary channel: zero successful sends; default wins | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/h2.go:534` | stream-admit: ordinary Put is synchronous reader admission, no per-Put handler goroutine | <code>c.admit(st)</code> |
| `internal/server/rpc.go:63` | frontend-decode: owning frontend decode: counted main/wire/hpack CAS only; ordinary Put bypasses on ALL targets; txn CAS bypasses | <code>if err := proto.Unmarshal(b, m); err != nil {</code> |
| `internal/server/server.go:189` | server-stop-close: close: server Stop wait channel, not per Put | <code>close(done)</code> |
| `internal/server/stream.go:124` | frontend-prealloc: full request-sized capacity prevents accumulation growth; not a separate copy | <code>st.msg = make([]byte, 0, size)</code> |
| `internal/server/stream.go:127` | frontend-accumulation: counted copy: 1 full-value equivalent over all request chunks | <code>st.msg = append(st.msg, d[:n]...)</code> |
| `internal/server/stream.go:139` | unary-delivery: retains owned body header and returns before streaming-signal select | <code>st.body = m</code> |
| `internal/server/stream.go:148` | unary-nil-body-signal: streaming-only select NOT executed by unary delivery: zero sends | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/stream.go:207` | response-proto-encode: CAS failure: one full stored-value copy into reply; ordinary Put/CAS success: zero value copy | <code>b, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(b, m)</code> |
| `internal/server/write.go:52` | writer-wake-receive: counted selected wake receive only while idle | <code>case &lt;-c.wake:</code> |
| `internal/server/write.go:54` | node-done-receive: shutdown-only receive; obtaining Done() itself is not operation | <code>case &lt;-nodeDone:</code> |
| `internal/server/write.go:130` | unary-nil-space: nil unary channel: zero successful sends; default wins | <code>case st.space &lt;- struct{}{}:</code> |
| `internal/server/write.go:191` | stream-end-release: release accounting/flow control; no room channel exists at these targets | <code>c.releaseLocked(&amp;st.replyHeld)</code> |
| `internal/wal/wal.go:351` | wal-value-copy: counted copy: 1 direct Entry.Data append per persisted entry containing Put | <code>w.buf = append(w.buf, p...)</code> |
| `internal/wal/wal.go:368` | wal-value-caller: WAL Entry.Data caller, not a second copy | <code>w.record(recEntry, h[:], e.GetData())</code> |
| `internal/wal/wal.go:386` | wal-write: file I/O/physical copies not enumerated in payload subtotal | <code>n, err := w.seg.Write(w.buf)</code> |
| `internal/wal/wal.go:416` | wal-sync: durability I/O, not byte materialization counted here | <code>if err := w.seg.Sync(); err != nil {</code> |

## wire — `936bc5b69ae8a0c59585521734b41222006a0a2c`
| File:line | Site / classification | Exact text (trimmed) |
|---|---|---|
| `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc.pb.go:760` | lib-put-value-field: Put.Value is proto3 bytes | <code>Value []byte `protobuf:&quot;bytes,2,opt,name=value,proto3&quot; json:&quot;value,omitempty&quot;`</code> |
| `dependency-sources/etcd-api-v3.7.2/mvccpb/kv.pb.go:84` | lib-returned-value-field: returned stored value is proto3 bytes: same owning encoder behavior | <code>Value []byte `protobuf:&quot;bytes,5,opt,name=value,proto3&quot; json:&quot;value,omitempty&quot;`</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` | lib-bytes-encode-copy: counted encoding instruction: main entry or peer Entry.Data or CAS failure reply, each boundary counted only once | <code>return append(AppendVarint(b, uint64(len(v))), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:468` | lib-bytes-consume-alias: wire ParsePut gets subslice, not copied bytes | <code>return b[n:][:m], n + int(m)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5418` | lib-proto2-decode-copy: counted receiver Entry.Data decode copy all targets | <code>*p.Bytes() = append(emptyBuf[:], v...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5483` | proto3-bytes-encoder: bytes encoders call AppendBytes; not an additional copy | <code>b = protowire.AppendBytes(b, v)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5497` | lib-proto3-decode-copy: one owning proto3 bytes decode stage: main/wire/hpack CAS frontend/apply; ordinary Put bypasses ALL targets; not txn CAS | <code>*p.Bytes() = append(([]byte)(nil), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:278` | lib-proto3-codec-selection: proto3 Put.Value and KeyValue.Value non-presence bytes codec selection | <code>return nil, coderBytesNoZero</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:428` | lib-proto2-codec-selection: presence bytes coder selection for Entry.Data | <code>return nil, coderBytes</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/convert.go:358` | reflect-bytes-convert: []byte-to-[]byte conversion retains backing store; no append/copy | <code>return reflect.ValueOf(v.Bytes()).Convert(c.goType)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188` | lib-proto2-clone-copy: counted: Raft leader Entry.Data deep clone, proto2 (not Put proto3 decoder) | <code>*dst.Bytes() = append(emptyBuf[:], *src.Bytes()...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/message_reflect_field.go:317` | reflect-scalar-set: Put.Value scalar setter copies slice header, not value bytes | <code>rv.Set(conv.GoValueOf(v))</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/message_reflect_gen.go:97` | reflect-set: reflection Set dispatch, no byte clone | <code>fi.set(m.pointer(), v)</code> |
| `dependency-sources/protobuf-v1.36.11/proto/merge.go:58` | lib-proto-clone-merge: Clone creates destination then merges fields | <code>mergeOptions{}.mergeMessage(dst, src)</code> |
| `dependency-sources/raft-v3.7.0/log.go:127` | follower-logappend: follower retains entry pointers | <code>l.append(a.entries[ci-offset:]...)</code> |
| `dependency-sources/raft-v3.7.0/log_unstable.go:205` | unstable-append: append []*Entry pointers, not Data bytes | <code>u.entries = append(u.entries, ents...)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:656` | lib-peer-construction-alias: zero payload copies when constructing append Message | <code>Entries: ents,</code> |
| `dependency-sources/raft-v3.7.0/raft.go:817` | lib-raft-clone: counted leader Entry.Data deep-clone boundary: 1/Put all targets | <code>cloned[i] = proto.Clone(es[i]).(*pb.Entry)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1349` | lib-raft-propose-clone-call: leader successful proposal invokes appendEntry clone | <code>if !r.appendEntry(m.GetEntries()...) {</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1800` | follower-maybeappend: follower uses maybeAppend, not leader appendEntry deep clone | <code>if mlastIndex, ok := r.raftLog.maybeAppend(a, m.GetCommit()); ok {</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.pb.go:348` | lib-entry-data-field: Entry.Data bytes descriptor | <code>Data          []byte                 `protobuf:&quot;bytes,4,opt,name=Data&quot; json:&quot;Data,omitempty&quot;`</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.proto:1` | lib-entry-proto2: Entry.Data uses proto2 bytes, not proto3 Put decoder | <code>syntax = &quot;proto2&quot;;</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:94` | lib-raft-propose-alias: RawNode boundary aliases data; later appendEntry clones it | <code>Entries: []*pb.Entry{{Data: data}},</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:228` | async-append-alias: local Raft async append message retains entry pointers | <code>Entries: rd.Entries,</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:393` | async-apply-alias: local Raft async apply message retains entry pointers | <code>Entries: ents,</code> |
| `dependency-sources/raft-v3.7.0/storage.go:318` | lib-storage-pointer-append1: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents[:offset:offset], entries...)</code> |
| `dependency-sources/raft-v3.7.0/storage.go:320` | lib-storage-pointer-append2: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents, entries...)</code> |
| `internal/core/codec.go:37` | entry-buffer-init: entry capacity: main/wire/hpack/txn fully size-reserved; no extra fixed relocation invented | <code>buf := binary.AppendUvarint(make([]byte, 0, size+binary.MaxVarintLen64), uint64(len(ps)))</code> |
| `internal/core/codec.go:68` | entry-value-encode: counted: one copy of each full input value into encoded entry, NOT amortized payload bytes | <code>buf = append(buf, p.req.Put.Wire...)</code> |
| `internal/core/codec.go:73` | generic-entry-value-encode: one entire CAS input-value copy for main/wire/hpack; txn Wire bypasses generic encoder | <code>buf, err = proto.MarshalOptions{Deterministic: true, UseCachedSize: true}.MarshalAppend(buf, p.req.Op)</code> |
| `internal/core/codec.go:103` | wire-batch-slice: zero copy: capped command subslice passed to apply | <code>if err := fn(origin, boot, id, kind, data[:size:size]); err != nil {</code> |
| `internal/core/codec.go:122` | new-txn-op: apply still creates fresh TxnRequest struct, including txn alias path | <code>return &amp;pb.TxnRequest{}, nil</code> |
| `internal/core/codec.go:145` | decode-batch-still-exists: generic helper used by checks, NOT ordinary production Put apply | <code>if err := proto.Unmarshal(b, op); err != nil {</code> |
| `internal/core/node.go:145` | pending-owner: pending record retains request slice headers; zero value copy | <code>req      Request</code> |
| `internal/core/node.go:372` | wire-check: validate fields/size without generic protobuf copy | <code>if err := checkPut(&amp;r.Put); err != nil {</code> |
| `internal/core/node.go:394` | txn-write-route: only wholly read-only transactions take read route | <code>if mvcc.IsTxnReadOnly(op) {</code> |
| `internal/core/node.go:501` | wire-size: whole client request size plus framing upper bound | <code>size := len(p.req.Put.Wire) + batchFraming</code> |
| `internal/core/node.go:566` | core-encode-call: entry encode caller, not an additional copy | <code>data, err := encodeBatch(n.cfg.ID, n.cfg.Boot, props, size)</code> |
| `internal/core/node.go:571` | raft-propose: leader proposal -> dependency clone: counted separately below | <code>if err := n.rn.Propose(data); err != nil {</code> |
| `internal/core/node.go:839` | apply-dispatch: main/batch decoded apply vs wire in-place walk | <code>err := walkBatch(e.GetData(), func(origin, boot, id uint64, kind byte, b []byte) error {</code> |
| `internal/core/node.go:844` | apply-wire-kind: Put bypasses generic apply protobuf decode on every replica | <code>if kind == kindPut {</code> |
| `internal/core/node.go:851` | generic-apply-still-exists: main/wire/hpack CAS owning apply decode; bypassed for ordinary Put | <code>if err := proto.Unmarshal(b, op); err != nil {</code> |
| `internal/core/node.go:880` | apply-wire-parse: parse entry in place: zero value copies | <code>put, err := ParsePut(b)</code> |
| `internal/core/node.go:884` | apply-wire-state: state fields forwarded: zero value copies | <code>prev, err := n.state.ApplyPut(&amp;put.PutOp)</code> |
| `internal/core/node.go:905` | apply-wire-reply: header/metadata reply, not a value copy | <code>n.out.Replies = append(n.out.Replies, Reply{ID: p.req.ID, Rev: n.kv.Rev(), Term: n.term, PrevKv: prev})</code> |
| `internal/core/state.go:110` | wire-state-index: zero copy: direct wire Put fields path into MVCC | <code>prev, err := st.kv.PutKV(op)</code> |
| `internal/core/state.go:140` | state-txn: replicated CAS apply: MVCC chooses branch | <code>r, err := st.kv.Txn(op)</code> |
| `internal/core/storage.go:39` | storage-pointer-append: MemoryStorage appends entry pointers, not Entry.Data bytes | <code>if err := s.Append(job.Entries); err != nil {</code> |
| `internal/core/watch.go:272` | no-watch-fastpath: no-watch match returns without value encoding | <code>if len(changes) == 0 &#124;&#124; len(n.watchers) == 0 {</code> |
| `internal/core/wire.go:61` | wire-parse-consume: dependency returns subslice: zero copy | <code>v, n := protowire.ConsumeBytes(b)</code> |
| `internal/core/wire.go:67` | wire-parse-clip: slice capacity clip, no byte copy | <code>v = slices.Clip(v)</code> |
| `internal/core/wire.go:71` | wire-value-alias: value slice aliases Wire/Entry.Data | <code>p.Value = v</code> |
| `internal/mvcc/ops.go:32` | revision-changed: MVCC revision changes only for actual state mutation | <code>if w.changed {</code> |
| `internal/mvcc/ops.go:85` | range-kv: CAS failure Range: KeyValue objects and slice headers only; zero value copy | <code>kvs = append(kvs, k.kv(v))</code> |
| `internal/mvcc/ops.go:191` | putop-alias: PutRequest-to-PutOp copies slice headers, not bytes | <code>return PutOp{Key: p.Key, Value: p.Value, Lease: p.Lease, PrevKv: p.PrevKv, IgnoreValue: p.IgnoreValue, IgnoreLease: p.IgnoreLease}</code> |
| `internal/mvcc/ops.go:250` | index-version-append: metadata/slice headers only: zero payload copies | <code>k.vers = append(k.vers, v)</code> |
| `internal/mvcc/ops.go:259` | index-value: zero payload copies: entry backing store for ordinary Put ALL targets and txn CAS; decoded bytes for main/wire/hpack CAS | <code>v := version{create: w.begin + 1, ver: 1, lease: p.Lease, value: p.Value}</code> |
| `internal/mvcc/ops.go:277` | index-key: zero key bytes copy at insert: retains key slice; key copies are not value copies | <code>k = &amp;keyIndex{key: p.Key}</code> |
| `internal/mvcc/ops.go:320` | compare-path: compare selects branch at replicated apply | <code>path := w.compareToPath(r)</code> |
| `internal/mvcc/ops.go:328` | txn-revision: CAS success increments revision; compare failure keeps current MVCC revision | <code>resp.Header = header(w.cur())</code> |
| `internal/mvcc/ops.go:366` | compare-kv: existing-key compare constructs KeyValue with aliased stored value; zero value copy | <code>if !compareKV(c, k.kv(v)) {</code> |
| `internal/mvcc/ops.go:390` | compare-mod: CAS compare uses integer mod revision, not value bytes | <code>result = cmpInt(kv.ModRevision, c.GetModRevision())</code> |
| `internal/mvcc/ops.go:470` | txn-success: Succeeded reports compare result; failure is still successful RPC | <code>Succeeded: path[0],</code> |
| `internal/mvcc/ops.go:478` | txn-range: compare failure: executes current Range only; return existing stored value | <code>rr, err := w.rangeKeys(t.RequestRange)</code> |
| `internal/mvcc/ops.go:485` | txn-put: success Put executes; failure skips this branch | <code>out.Response = &amp;pb.ResponseOp_ResponsePut{ResponsePut: &amp;pb.PutResponse{Header: header(w.begin + 1), PrevKv: w.put(&amp;op)}}</code> |
| `internal/mvcc/store.go:73` | kv-value-alias: zero stored-value copy: response/compare KeyValue retains value slice header | <code>Value:          v.value,</code> |
| `internal/mvcc/validate.go:185` | txn-readonly: both branches classified before compare; a Put in Success forces write/Raft route | <code>for _, ops := range [][]*pb.RequestOp{r.Success, r.Failure} {</code> |
| `internal/mvcc/validate.go:187` | txn-write-detect: success Put means not read-only even when compare will fail | <code>if op.GetRequestRange() == nil {</code> |
| `internal/node/node.go:136` | admission-send: counted: one send per actual admission batch, all targets | <code>case n.inbox &lt;- in:</code> |
| `internal/node/node.go:169` | driver-batch-owner: driver batch owns/shared request descriptors; header-only, not channel | <code>reqs  []core.Request</code> |
| `internal/node/node.go:190` | batch-api: direct API, not channel operation | <code>func (n *Node) SubmitBatch(ctx context.Context, reqs []core.Request, tags []any, to Completer) error {</code> |
| `internal/node/node.go:199` | batch-wrapper: batch wrapper retains descriptor/tag backing stores; no payload copy or channel exchange | <code>b := &amp;batch{first: n.nextID.Add(k) - k + 1, reqs: reqs, tags: tags, to: to}</code> |
| `internal/node/node.go:200` | batch-inbox-call: one inbox admission handoff per batch via actual send site; this call is not an extra exchange | <code>return n.put(ctx, input{batch: b})</code> |
| `internal/node/node.go:266` | peer-inbox-send: counted send: 1/delivered inbound peer message | <code>case n.inbox &lt;- input{msg: m}:</code> |
| `internal/node/node.go:274` | unreachable-send: conditional counted U send when nonblocking Unreachable report succeeds; saturation/drop/fault, not fixed per request | <code>case n.inbox &lt;- input{unreachable: id}:</code> |
| `internal/node/node.go:281` | do-reply-create: excluded retained Node.Do channel creation: zero per ordinary Put or CAS | <code>r := &amp;request{id: n.nextID.Add(1), op: op, reply: make(chan core.Reply, 1)}</code> |
| `internal/node/node.go:286` | do-reply-receive: excluded retained Node.Do: not ordinary Put or CAS | <code>case rep := &lt;-r.reply:</code> |
| `internal/node/node.go:308` | shutdown-close: close: shutdown-only, zero per normal Put | <code>close(n.done)</code> |
| `internal/node/node.go:335` | inbox-receive-outer: counted selected receive: outer alternative for admission, inbound peer or successful Unreachable report | <code>case in := &lt;-inbox:</code> |
| `internal/node/node.go:340` | inbox-receive-drain: counted selected receive: drain alternative for another input; not extra receipt of outer input | <code>case in := &lt;-n.inbox:</code> |
| `internal/node/node.go:346` | disk-complete-receive: counted receive: 1/sync completion group | <code>case done := &lt;-n.persisted:</code> |
| `internal/node/node.go:353` | disk-send: counted send: 1/persist queue array | <code>case toDisk &lt;- next:</code> |
| `internal/node/node.go:357` | tick-receive: counted selected background receive, not fixed per Put | <code>case &lt;-ticker.C:</code> |
| `internal/node/node.go:394` | watch-close: close: watch overflow, outside no-watch scope | <code>close(ws.done)</code> |
| `internal/node/node.go:420` | do-reply-send: excluded retained Node.Do: not ordinary Put or CAS | <code>w.reply &lt;- r</code> |
| `internal/node/node.go:432` | completion-direct: direct grouped call: zero channel ops | <code>g.to.Complete(g.cs)</code> |
| `internal/node/node.go:446` | batch-waiter: per-request completer/tag metadata, no private reply channel | <code>waiters[r.ID] = waiter{to: b.to, tag: b.tags[i]}</code> |
| `internal/node/node.go:447` | batch-core-submit: descriptor handoff, zero payload copy; main submits individually, wire likewise | <code>n.core.Submit(*r)</code> |
| `internal/node/node.go:486` | disk-receive-outer: counted receive: initial array; alternative to drain | <code>case jobs = &lt;-n.persistQ:</code> |
| `internal/node/node.go:493` | disk-receive-drain: counted receive: drained array; NOT an extra receive for initial array | <code>case js := &lt;-n.persistQ:</code> |
| `internal/node/node.go:500` | wal-append-call: driver disk trace; no extra payload copy beyond WAL record | <code>n.wal.Append(j.HardState, j.Entries)</code> |
| `internal/node/node.go:503` | wal-sync-call: driver disk trace; I/O, not counted as extra userspace payload materialization | <code>if err := n.wal.Sync(); err != nil {</code> |
| `internal/node/node.go:507` | disk-complete-send: counted send: 1/sync completion group | <code>case n.persisted &lt;- done:</code> |
| `internal/peer/peer.go:241` | peer-value-decode: counted copy: 1/received occurrence of Entry.Data, on receiver; dependency decoder below | <code>if err := proto.Unmarshal(buf, m); err != nil {</code> |
| `internal/peer/peer.go:297` | peer-queue-send: counted send: 1/queued outbound peer message | <code>case q &lt;- m:</code> |
| `internal/peer/peer.go:308` | peer-queue-receive-fast: counted receive alternative 1: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:314` | peer-queue-receive-control: counted receive alternative 2: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:317` | peer-queue-receive-data: counted receive alternative 3: one receipt per message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:320` | peer-shutdown-receive-1: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:331` | peer-drop-control: fault-only receive: dropped queued control message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:333` | peer-drop-data: fault-only receive: dropped queued data message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:350` | peer-backoff-receive: fault-only timer receive, excluded healthy path | <code>case &lt;-time.After(backoff):</code> |
| `internal/peer/peer.go:351` | peer-shutdown-receive-2: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:419` | peer-value-encode: counted copy: 1/serialized occurrence of Entry.Data containing this Put, on sender | <code>buf, err = proto.MarshalOptions{}.MarshalAppend(buf[:0], m)</code> |
| `internal/server/batch.go:39` | opbatch-owner: connection admission batch owns request descriptors; header-only, not channel | <code>reqs  []core.Request</code> |
| `internal/server/batch.go:59` | wire-frontend-parse: wire frontend parse: zero value copies | <code>put, err := core.ParsePut(st.body)</code> |
| `internal/server/batch.go:66` | frontend-new-op: request structs still constructed for CAS; ordinary Put ParsePut bypasses ALL targets | <code>req.Op = st.rpc.op()</code> |
| `internal/server/batch.go:116` | charge-request: accounting metadata only; no payload copy | <code>c.chargeLocked(&amp;st.reqHeld, st.reqSize+callCost)</code> |
| `internal/server/batch.go:126` | batch-submit-call: one admission API call per actual batch | <code>err := c.s.n.SubmitBatch(c.ctx, b.reqs, b.tags, c)</code> |
| `internal/server/batch.go:212` | completion-cas: completion publish: CAS, no Go channel and no completion mutex | <code>if c.completions.CompareAndSwap(n.next, n) {</code> |
| `internal/server/batch.go:216` | completion-wake-attempt: wake attempt per completion group, not guaranteed successful send | <code>c.signal()</code> |
| `internal/server/batch.go:225` | completion-swap: writer collects completion list atomically; zero channel ops | <code>for n := c.completions.Swap(nil); n != nil; n = n.next {</code> |
| `internal/server/batch.go:259` | wire-reply-encode: wire Put metadata response: zero ordinary input-value copy | <code>f.msg, f.err = c.putResponse(&amp;cp.Reply)</code> |
| `internal/server/batch.go:261` | batch-reply-encode: Put replies contain no input payload; CAS failure reply contains stored value and copies it once via response codec | <code>f.msg, f.err = marshal(cp.Resp)</code> |
| `internal/server/batch.go:268` | release-request: release accounting, zero channel exchange / payload copy | <code>c.releaseReqLocked(f.st)</code> |
| `internal/server/batch.go:274` | charge-reply: accounts encoded reply until sent; no payload copy | <code>c.chargeLocked(&amp;f.st.replyHeld, len(f.msg))</code> |
| `internal/server/batch.go:308` | manual-response-prefix: wire small response buffer, no value bytes | <code>b := make([]byte, 5, 5+putResponseSize)</code> |
| `internal/server/batch.go:309` | manual-response-append: wire small response, no value bytes when PrevKv nil | <code>b, err := appendPutResponse(b, c.s.n.ClusterID(), c.s.n.ID(), r.Rev, r.Term, r.PrevKv)</code> |
| `internal/server/h2.go:184` | goaway-wake-attempt: lifecycle wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:195` | writer-wake-send: counted successful conditional/coalesced wake send; default is not operation | <code>case c.wake &lt;- struct{}{}:</code> |
| `internal/server/h2.go:208` | control-wake-attempt: control wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:321` | settings-wake-attempt: settings wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:336` | window-wake-attempt: send-window wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:364` | reset-wake-attempt: reset wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:374` | credit-wake-attempt: wake attempt per DATA frame, not a guaranteed successful send | <code>c.signal()</code> |
| `internal/server/h2.go:450` | stream-context-gate: ordinary batch/wire Put creates no per-stream context | <code>if !grpc &#124;&#124; (st.rpc != nil &amp;&amp; st.rpc.op == nil) {</code> |
| `internal/server/h2.go:469` | stream-deadline: optional timer callback, not a Timer.C send/receive | <code>st.deadline = time.AfterFunc(timeout, st.expire)</code> |
| `internal/server/h2.go:507` | receive-data-caller: Framer DATA dispatched to owned message accumulation | <code>if err := st.receive(f.Data()); err != nil {</code> |
| `internal/server/h2.go:526` | unary-nil-signal: nil unary channel: zero successful sends; default wins | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/h2.go:534` | stream-admit: ordinary Put is synchronous reader admission, no per-Put handler goroutine | <code>c.admit(st)</code> |
| `internal/server/rpc.go:63` | frontend-decode: owning frontend decode: counted main/wire/hpack CAS only; ordinary Put bypasses on ALL targets; txn CAS bypasses | <code>if err := proto.Unmarshal(b, m); err != nil {</code> |
| `internal/server/server.go:189` | server-stop-close: close: server Stop wait channel, not per Put | <code>close(done)</code> |
| `internal/server/stream.go:124` | frontend-prealloc: full request-sized capacity prevents accumulation growth; not a separate copy | <code>st.msg = make([]byte, 0, size)</code> |
| `internal/server/stream.go:127` | frontend-accumulation: counted copy: 1 full-value equivalent over all request chunks | <code>st.msg = append(st.msg, d[:n]...)</code> |
| `internal/server/stream.go:139` | unary-delivery: retains owned body header and returns before streaming-signal select | <code>st.body = m</code> |
| `internal/server/stream.go:148` | unary-nil-body-signal: streaming-only select NOT executed by unary delivery: zero sends | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/stream.go:207` | response-proto-encode: CAS failure: one full stored-value copy into reply; ordinary Put/CAS success: zero value copy | <code>b, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(b, m)</code> |
| `internal/server/write.go:52` | writer-wake-receive: counted selected wake receive only while idle | <code>case &lt;-c.wake:</code> |
| `internal/server/write.go:54` | node-done-receive: shutdown-only receive; obtaining Done() itself is not operation | <code>case &lt;-nodeDone:</code> |
| `internal/server/write.go:130` | unary-nil-space: nil unary channel: zero successful sends; default wins | <code>case st.space &lt;- struct{}{}:</code> |
| `internal/server/write.go:191` | stream-end-release: release accounting/flow control; no room channel exists at these targets | <code>c.releaseLocked(&amp;st.replyHeld)</code> |
| `internal/wal/wal.go:351` | wal-value-copy: counted copy: 1 direct Entry.Data append per persisted entry containing Put | <code>w.buf = append(w.buf, p...)</code> |
| `internal/wal/wal.go:368` | wal-value-caller: WAL Entry.Data caller, not a second copy | <code>w.record(recEntry, h[:], e.GetData())</code> |
| `internal/wal/wal.go:386` | wal-write: file I/O/physical copies not enumerated in payload subtotal | <code>n, err := w.seg.Write(w.buf)</code> |
| `internal/wal/wal.go:416` | wal-sync: durability I/O, not byte materialization counted here | <code>if err := w.seg.Sync(); err != nil {</code> |

## hpack — `129e49a842b4802e9a36b74c830b3538525f846f`
| File:line | Site / classification | Exact text (trimmed) |
|---|---|---|
| `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc.pb.go:760` | lib-put-value-field: Put.Value is proto3 bytes | <code>Value []byte `protobuf:&quot;bytes,2,opt,name=value,proto3&quot; json:&quot;value,omitempty&quot;`</code> |
| `dependency-sources/etcd-api-v3.7.2/mvccpb/kv.pb.go:84` | lib-returned-value-field: returned stored value is proto3 bytes: same owning encoder behavior | <code>Value []byte `protobuf:&quot;bytes,5,opt,name=value,proto3&quot; json:&quot;value,omitempty&quot;`</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` | lib-bytes-encode-copy: counted encoding instruction: main entry or peer Entry.Data or CAS failure reply, each boundary counted only once | <code>return append(AppendVarint(b, uint64(len(v))), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:468` | lib-bytes-consume-alias: wire ParsePut gets subslice, not copied bytes | <code>return b[n:][:m], n + int(m)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5418` | lib-proto2-decode-copy: counted receiver Entry.Data decode copy all targets | <code>*p.Bytes() = append(emptyBuf[:], v...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5483` | proto3-bytes-encoder: bytes encoders call AppendBytes; not an additional copy | <code>b = protowire.AppendBytes(b, v)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5497` | lib-proto3-decode-copy: one owning proto3 bytes decode stage: main/wire/hpack CAS frontend/apply; ordinary Put bypasses ALL targets; not txn CAS | <code>*p.Bytes() = append(([]byte)(nil), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:278` | lib-proto3-codec-selection: proto3 Put.Value and KeyValue.Value non-presence bytes codec selection | <code>return nil, coderBytesNoZero</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:428` | lib-proto2-codec-selection: presence bytes coder selection for Entry.Data | <code>return nil, coderBytes</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/convert.go:358` | reflect-bytes-convert: []byte-to-[]byte conversion retains backing store; no append/copy | <code>return reflect.ValueOf(v.Bytes()).Convert(c.goType)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188` | lib-proto2-clone-copy: counted: Raft leader Entry.Data deep clone, proto2 (not Put proto3 decoder) | <code>*dst.Bytes() = append(emptyBuf[:], *src.Bytes()...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/message_reflect_field.go:317` | reflect-scalar-set: Put.Value scalar setter copies slice header, not value bytes | <code>rv.Set(conv.GoValueOf(v))</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/message_reflect_gen.go:97` | reflect-set: reflection Set dispatch, no byte clone | <code>fi.set(m.pointer(), v)</code> |
| `dependency-sources/protobuf-v1.36.11/proto/merge.go:58` | lib-proto-clone-merge: Clone creates destination then merges fields | <code>mergeOptions{}.mergeMessage(dst, src)</code> |
| `dependency-sources/raft-v3.7.0/log.go:127` | follower-logappend: follower retains entry pointers | <code>l.append(a.entries[ci-offset:]...)</code> |
| `dependency-sources/raft-v3.7.0/log_unstable.go:205` | unstable-append: append []*Entry pointers, not Data bytes | <code>u.entries = append(u.entries, ents...)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:656` | lib-peer-construction-alias: zero payload copies when constructing append Message | <code>Entries: ents,</code> |
| `dependency-sources/raft-v3.7.0/raft.go:817` | lib-raft-clone: counted leader Entry.Data deep-clone boundary: 1/Put all targets | <code>cloned[i] = proto.Clone(es[i]).(*pb.Entry)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1349` | lib-raft-propose-clone-call: leader successful proposal invokes appendEntry clone | <code>if !r.appendEntry(m.GetEntries()...) {</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1800` | follower-maybeappend: follower uses maybeAppend, not leader appendEntry deep clone | <code>if mlastIndex, ok := r.raftLog.maybeAppend(a, m.GetCommit()); ok {</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.pb.go:348` | lib-entry-data-field: Entry.Data bytes descriptor | <code>Data          []byte                 `protobuf:&quot;bytes,4,opt,name=Data&quot; json:&quot;Data,omitempty&quot;`</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.proto:1` | lib-entry-proto2: Entry.Data uses proto2 bytes, not proto3 Put decoder | <code>syntax = &quot;proto2&quot;;</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:94` | lib-raft-propose-alias: RawNode boundary aliases data; later appendEntry clones it | <code>Entries: []*pb.Entry{{Data: data}},</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:228` | async-append-alias: local Raft async append message retains entry pointers | <code>Entries: rd.Entries,</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:393` | async-apply-alias: local Raft async apply message retains entry pointers | <code>Entries: ents,</code> |
| `dependency-sources/raft-v3.7.0/storage.go:318` | lib-storage-pointer-append1: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents[:offset:offset], entries...)</code> |
| `dependency-sources/raft-v3.7.0/storage.go:320` | lib-storage-pointer-append2: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents, entries...)</code> |
| `internal/core/codec.go:37` | entry-buffer-init: entry capacity: main/wire/hpack/txn fully size-reserved; no extra fixed relocation invented | <code>buf := binary.AppendUvarint(make([]byte, 0, size+binary.MaxVarintLen64), uint64(len(ps)))</code> |
| `internal/core/codec.go:68` | entry-value-encode: counted: one copy of each full input value into encoded entry, NOT amortized payload bytes | <code>buf = append(buf, p.req.Put.Wire...)</code> |
| `internal/core/codec.go:73` | generic-entry-value-encode: one entire CAS input-value copy for main/wire/hpack; txn Wire bypasses generic encoder | <code>buf, err = proto.MarshalOptions{Deterministic: true, UseCachedSize: true}.MarshalAppend(buf, p.req.Op)</code> |
| `internal/core/codec.go:103` | wire-batch-slice: zero copy: capped command subslice passed to apply | <code>if err := fn(origin, boot, id, kind, data[:size:size]); err != nil {</code> |
| `internal/core/codec.go:122` | new-txn-op: apply still creates fresh TxnRequest struct, including txn alias path | <code>return &amp;pb.TxnRequest{}, nil</code> |
| `internal/core/codec.go:145` | decode-batch-still-exists: generic helper used by checks, NOT ordinary production Put apply | <code>if err := proto.Unmarshal(b, op); err != nil {</code> |
| `internal/core/node.go:145` | pending-owner: pending record retains request slice headers; zero value copy | <code>req      Request</code> |
| `internal/core/node.go:373` | batch-pending-allocation: one pending-record backing array per SubmitAll; not startup arena | <code>ps := make([]pending, len(rs))</code> |
| `internal/core/node.go:375` | submitall-loop: direct per-record method call into batch-owned pending array; zero sends/receives/closes | <code>n.submit(rs[i], &amp;ps[i])</code> |
| `internal/core/node.go:386` | wire-check: validate fields/size without generic protobuf copy | <code>if err := checkPut(&amp;r.Put); err != nil {</code> |
| `internal/core/node.go:408` | txn-write-route: only wholly read-only transactions take read route | <code>if mvcc.IsTxnReadOnly(op) {</code> |
| `internal/core/node.go:515` | wire-size: whole client request size plus framing upper bound | <code>size := len(p.req.Put.Wire) + batchFraming</code> |
| `internal/core/node.go:580` | core-encode-call: entry encode caller, not an additional copy | <code>data, err := encodeBatch(n.cfg.ID, n.cfg.Boot, props, size)</code> |
| `internal/core/node.go:585` | raft-propose: leader proposal -> dependency clone: counted separately below | <code>if err := n.rn.Propose(data); err != nil {</code> |
| `internal/core/node.go:853` | apply-dispatch: main/batch decoded apply vs wire in-place walk | <code>err := walkBatch(e.GetData(), func(origin, boot, id uint64, kind byte, b []byte) error {</code> |
| `internal/core/node.go:858` | apply-wire-kind: Put bypasses generic apply protobuf decode on every replica | <code>if kind == kindPut {</code> |
| `internal/core/node.go:865` | generic-apply-still-exists: main/wire/hpack CAS owning apply decode; bypassed for ordinary Put | <code>if err := proto.Unmarshal(b, op); err != nil {</code> |
| `internal/core/node.go:894` | apply-wire-parse: parse entry in place: zero value copies | <code>put, err := ParsePut(b)</code> |
| `internal/core/node.go:898` | apply-wire-state: state fields forwarded: zero value copies | <code>prev, err := n.state.ApplyPut(&amp;put.PutOp)</code> |
| `internal/core/node.go:919` | apply-wire-reply: header/metadata reply, not a value copy | <code>n.out.Replies = append(n.out.Replies, Reply{ID: p.req.ID, Rev: n.kv.Rev(), Term: n.term, PrevKv: prev})</code> |
| `internal/core/state.go:110` | wire-state-index: zero copy: direct wire Put fields path into MVCC | <code>prev, err := st.kv.PutKV(op)</code> |
| `internal/core/state.go:140` | state-txn: replicated CAS apply: MVCC chooses branch | <code>r, err := st.kv.Txn(op)</code> |
| `internal/core/storage.go:39` | storage-pointer-append: MemoryStorage appends entry pointers, not Entry.Data bytes | <code>if err := s.Append(job.Entries); err != nil {</code> |
| `internal/core/watch.go:272` | no-watch-fastpath: no-watch match returns without value encoding | <code>if len(changes) == 0 &#124;&#124; len(n.watchers) == 0 {</code> |
| `internal/core/wire.go:61` | wire-parse-consume: dependency returns subslice: zero copy | <code>v, n := protowire.ConsumeBytes(b)</code> |
| `internal/core/wire.go:67` | wire-parse-clip: slice capacity clip, no byte copy | <code>v = slices.Clip(v)</code> |
| `internal/core/wire.go:71` | wire-value-alias: value slice aliases Wire/Entry.Data | <code>p.Value = v</code> |
| `internal/mvcc/ops.go:32` | revision-changed: MVCC revision changes only for actual state mutation | <code>if w.changed {</code> |
| `internal/mvcc/ops.go:85` | range-kv: CAS failure Range: KeyValue objects and slice headers only; zero value copy | <code>kvs = append(kvs, k.kv(v))</code> |
| `internal/mvcc/ops.go:191` | putop-alias: PutRequest-to-PutOp copies slice headers, not bytes | <code>return PutOp{Key: p.Key, Value: p.Value, Lease: p.Lease, PrevKv: p.PrevKv, IgnoreValue: p.IgnoreValue, IgnoreLease: p.IgnoreLease}</code> |
| `internal/mvcc/ops.go:250` | index-version-append: metadata/slice headers only: zero payload copies | <code>k.vers = append(k.vers, v)</code> |
| `internal/mvcc/ops.go:259` | index-value: zero payload copies: entry backing store for ordinary Put ALL targets and txn CAS; decoded bytes for main/wire/hpack CAS | <code>v := version{create: w.begin + 1, ver: 1, lease: p.Lease, value: p.Value}</code> |
| `internal/mvcc/ops.go:277` | index-key: zero key bytes copy at insert: retains key slice; key copies are not value copies | <code>k = &amp;keyIndex{key: p.Key}</code> |
| `internal/mvcc/ops.go:320` | compare-path: compare selects branch at replicated apply | <code>path := w.compareToPath(r)</code> |
| `internal/mvcc/ops.go:328` | txn-revision: CAS success increments revision; compare failure keeps current MVCC revision | <code>resp.Header = header(w.cur())</code> |
| `internal/mvcc/ops.go:366` | compare-kv: existing-key compare constructs KeyValue with aliased stored value; zero value copy | <code>if !compareKV(c, k.kv(v)) {</code> |
| `internal/mvcc/ops.go:390` | compare-mod: CAS compare uses integer mod revision, not value bytes | <code>result = cmpInt(kv.ModRevision, c.GetModRevision())</code> |
| `internal/mvcc/ops.go:470` | txn-success: Succeeded reports compare result; failure is still successful RPC | <code>Succeeded: path[0],</code> |
| `internal/mvcc/ops.go:478` | txn-range: compare failure: executes current Range only; return existing stored value | <code>rr, err := w.rangeKeys(t.RequestRange)</code> |
| `internal/mvcc/ops.go:485` | txn-put: success Put executes; failure skips this branch | <code>out.Response = &amp;pb.ResponseOp_ResponsePut{ResponsePut: &amp;pb.PutResponse{Header: header(w.begin + 1), PrevKv: w.put(&amp;op)}}</code> |
| `internal/mvcc/store.go:73` | kv-value-alias: zero stored-value copy: response/compare KeyValue retains value slice header | <code>Value:          v.value,</code> |
| `internal/mvcc/validate.go:185` | txn-readonly: both branches classified before compare; a Put in Success forces write/Raft route | <code>for _, ops := range [][]*pb.RequestOp{r.Success, r.Failure} {</code> |
| `internal/mvcc/validate.go:187` | txn-write-detect: success Put means not read-only even when compare will fail | <code>if op.GetRequestRange() == nil {</code> |
| `internal/node/node.go:136` | admission-send: counted: one send per actual admission batch, all targets | <code>case n.inbox &lt;- in:</code> |
| `internal/node/node.go:169` | driver-batch-owner: driver batch owns/shared request descriptors; header-only, not channel | <code>reqs  []core.Request</code> |
| `internal/node/node.go:190` | batch-api: direct API, not channel operation | <code>func (n *Node) SubmitBatch(ctx context.Context, reqs []core.Request, tags []any, to Completer) error {</code> |
| `internal/node/node.go:199` | batch-wrapper: batch wrapper retains descriptor/tag backing stores; no payload copy or channel exchange | <code>b := &amp;batch{first: n.nextID.Add(k) - k + 1, reqs: reqs, tags: tags, to: to}</code> |
| `internal/node/node.go:200` | batch-inbox-call: one inbox admission handoff per batch via actual send site; this call is not an extra exchange | <code>return n.put(ctx, input{batch: b})</code> |
| `internal/node/node.go:266` | peer-inbox-send: counted send: 1/delivered inbound peer message | <code>case n.inbox &lt;- input{msg: m}:</code> |
| `internal/node/node.go:274` | unreachable-send: conditional counted U send when nonblocking Unreachable report succeeds; saturation/drop/fault, not fixed per request | <code>case n.inbox &lt;- input{unreachable: id}:</code> |
| `internal/node/node.go:281` | do-reply-create: excluded retained Node.Do channel creation: zero per ordinary Put or CAS | <code>r := &amp;request{id: n.nextID.Add(1), op: op, reply: make(chan core.Reply, 1)}</code> |
| `internal/node/node.go:286` | do-reply-receive: excluded retained Node.Do: not ordinary Put or CAS | <code>case rep := &lt;-r.reply:</code> |
| `internal/node/node.go:308` | shutdown-close: close: shutdown-only, zero per normal Put | <code>close(n.done)</code> |
| `internal/node/node.go:335` | inbox-receive-outer: counted selected receive: outer alternative for admission, inbound peer or successful Unreachable report | <code>case in := &lt;-inbox:</code> |
| `internal/node/node.go:340` | inbox-receive-drain: counted selected receive: drain alternative for another input; not extra receipt of outer input | <code>case in := &lt;-n.inbox:</code> |
| `internal/node/node.go:346` | disk-complete-receive: counted receive: 1/sync completion group | <code>case done := &lt;-n.persisted:</code> |
| `internal/node/node.go:353` | disk-send: counted send: 1/persist queue array | <code>case toDisk &lt;- next:</code> |
| `internal/node/node.go:357` | tick-receive: counted selected background receive, not fixed per Put | <code>case &lt;-ticker.C:</code> |
| `internal/node/node.go:394` | watch-close: close: watch overflow, outside no-watch scope | <code>close(ws.done)</code> |
| `internal/node/node.go:420` | do-reply-send: excluded retained Node.Do: not ordinary Put or CAS | <code>w.reply &lt;- r</code> |
| `internal/node/node.go:432` | completion-direct: direct grouped call: zero channel ops | <code>g.to.Complete(g.cs)</code> |
| `internal/node/node.go:446` | batch-waiter: per-request completer/tag metadata, no private reply channel | <code>waiters[r.ID] = waiter{to: b.to, tag: b.tags[i]}</code> |
| `internal/node/node.go:448` | batch-core-submit: direct batch submission, zero payload copy | <code>n.core.SubmitAll(b.reqs)</code> |
| `internal/node/node.go:486` | disk-receive-outer: counted receive: initial array; alternative to drain | <code>case jobs = &lt;-n.persistQ:</code> |
| `internal/node/node.go:493` | disk-receive-drain: counted receive: drained array; NOT an extra receive for initial array | <code>case js := &lt;-n.persistQ:</code> |
| `internal/node/node.go:500` | wal-append-call: driver disk trace; no extra payload copy beyond WAL record | <code>n.wal.Append(j.HardState, j.Entries)</code> |
| `internal/node/node.go:503` | wal-sync-call: driver disk trace; I/O, not counted as extra userspace payload materialization | <code>if err := n.wal.Sync(); err != nil {</code> |
| `internal/node/node.go:507` | disk-complete-send: counted send: 1/sync completion group | <code>case n.persisted &lt;- done:</code> |
| `internal/peer/peer.go:241` | peer-value-decode: counted copy: 1/received occurrence of Entry.Data, on receiver; dependency decoder below | <code>if err := proto.Unmarshal(buf, m); err != nil {</code> |
| `internal/peer/peer.go:297` | peer-queue-send: counted send: 1/queued outbound peer message | <code>case q &lt;- m:</code> |
| `internal/peer/peer.go:308` | peer-queue-receive-fast: counted receive alternative 1: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:314` | peer-queue-receive-control: counted receive alternative 2: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:317` | peer-queue-receive-data: counted receive alternative 3: one receipt per message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:320` | peer-shutdown-receive-1: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:331` | peer-drop-control: fault-only receive: dropped queued control message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:333` | peer-drop-data: fault-only receive: dropped queued data message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:350` | peer-backoff-receive: fault-only timer receive, excluded healthy path | <code>case &lt;-time.After(backoff):</code> |
| `internal/peer/peer.go:351` | peer-shutdown-receive-2: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:419` | peer-value-encode: counted copy: 1/serialized occurrence of Entry.Data containing this Put, on sender | <code>buf, err = proto.MarshalOptions{}.MarshalAppend(buf[:0], m)</code> |
| `internal/server/batch.go:39` | opbatch-owner: connection admission batch owns request descriptors; header-only, not channel | <code>reqs  []core.Request</code> |
| `internal/server/batch.go:59` | wire-frontend-parse: wire frontend parse: zero value copies | <code>put, err := core.ParsePut(st.body)</code> |
| `internal/server/batch.go:66` | frontend-new-op: request structs still constructed for CAS; ordinary Put ParsePut bypasses ALL targets | <code>req.Op = st.rpc.op()</code> |
| `internal/server/batch.go:116` | charge-request: accounting metadata only; no payload copy | <code>c.chargeLocked(&amp;st.reqHeld, st.reqSize+callCost)</code> |
| `internal/server/batch.go:126` | batch-submit-call: one admission API call per actual batch | <code>err := c.s.n.SubmitBatch(c.ctx, b.reqs, b.tags, c)</code> |
| `internal/server/batch.go:212` | completion-cas: completion publish: CAS, no Go channel and no completion mutex | <code>if c.completions.CompareAndSwap(n.next, n) {</code> |
| `internal/server/batch.go:216` | completion-wake-attempt: wake attempt per completion group, not guaranteed successful send | <code>c.signal()</code> |
| `internal/server/batch.go:225` | completion-swap: writer collects completion list atomically; zero channel ops | <code>for n := c.completions.Swap(nil); n != nil; n = n.next {</code> |
| `internal/server/batch.go:265` | wire-reply-encode: one buffer PER Put reply at verified target, PR body shared-buffer claim is stale | <code>msg, err := c.appendPutReply(make([]byte, 0, 5+putResponseSize), &amp;cp.Reply)</code> |
| `internal/server/batch.go:270` | batch-reply-encode: Put replies contain no input payload; CAS failure reply contains stored value and copies it once via response codec | <code>f.msg, f.err = marshal(cp.Resp)</code> |
| `internal/server/batch.go:277` | release-request: release accounting, zero channel exchange / payload copy | <code>c.releaseReqLocked(f.st)</code> |
| `internal/server/batch.go:283` | charge-reply: accounts encoded reply until sent; no payload copy | <code>c.chargeLocked(&amp;f.st.replyHeld, len(f.msg))</code> |
| `internal/server/batch.go:318` | manual-response-prefix: gRPC small reply prefix, zero input-value copy | <code>b = append(b, 0, 0, 0, 0, 0)</code> |
| `internal/server/batch.go:319` | manual-response-append: wire small response, no value bytes when PrevKv nil | <code>b, err := appendPutResponse(b, c.s.n.ClusterID(), c.s.n.ID(), r.Rev, r.Term, r.PrevKv)</code> |
| `internal/server/h2.go:190` | goaway-wake-attempt: lifecycle wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:201` | writer-wake-send: counted successful conditional/coalesced wake send; default is not operation | <code>case c.wake &lt;- struct{}{}:</code> |
| `internal/server/h2.go:214` | control-wake-attempt: control wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:339` | settings-wake-attempt: settings wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:354` | window-wake-attempt: send-window wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:382` | reset-wake-attempt: reset wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:392` | credit-wake-attempt: wake attempt per DATA frame, not a guaranteed successful send | <code>c.signal()</code> |
| `internal/server/h2.go:422` | hpack-decode: per-connection HPACK decoder, header bytes are not Put/CAS payload | <code>if _, err := c.hdec.Write(b); err != nil {</code> |
| `internal/server/h2.go:473` | hpack-field: indexed header fields retained without fresh MetaHeaders field list; literal decoding may allocate | <code>bit, h.path = 2, f.Value</code> |
| `internal/server/h2.go:574` | stream-context-gate: ordinary batch/wire Put creates no per-stream context | <code>if !grpc &#124;&#124; (st.rpc != nil &amp;&amp; st.rpc.op == nil) {</code> |
| `internal/server/h2.go:593` | stream-deadline: optional deadline callback timer, not Timer.C exchange | <code>st.deadline = time.AfterFunc(h.timeout, st.expire)</code> |
| `internal/server/h2.go:631` | receive-data-caller: Framer DATA dispatched to owned message accumulation | <code>if err := st.receive(f.Data()); err != nil {</code> |
| `internal/server/h2.go:650` | unary-nil-signal: nil unary channel: zero successful sends; default wins | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/h2.go:658` | stream-admit: ordinary Put is synchronous reader admission, no per-Put handler goroutine | <code>c.admit(st)</code> |
| `internal/server/rpc.go:63` | frontend-decode: owning frontend decode: counted main/wire/hpack CAS only; ordinary Put bypasses on ALL targets; txn CAS bypasses | <code>if err := proto.Unmarshal(b, m); err != nil {</code> |
| `internal/server/server.go:189` | server-stop-close: close: server Stop wait channel, not per Put | <code>close(done)</code> |
| `internal/server/stream.go:124` | frontend-prealloc: full request-sized capacity prevents accumulation growth; not a separate copy | <code>st.msg = make([]byte, 0, size)</code> |
| `internal/server/stream.go:127` | frontend-accumulation: counted copy: 1 full-value equivalent over all request chunks | <code>st.msg = append(st.msg, d[:n]...)</code> |
| `internal/server/stream.go:139` | unary-delivery: retains owned body header and returns before streaming-signal select | <code>st.body = m</code> |
| `internal/server/stream.go:148` | unary-nil-body-signal: streaming-only select NOT executed by unary delivery: zero sends | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/stream.go:207` | response-proto-encode: CAS failure: one full stored-value copy into reply; ordinary Put/CAS success: zero value copy | <code>b, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(b, m)</code> |
| `internal/server/stream.go:280` | out-capacity: per-stream output array still allocated, room for ordinary unary reply | <code>st.out = make([]outItem, 0, 3)</code> |
| `internal/server/write.go:52` | writer-wake-receive: counted selected wake receive only while idle | <code>case &lt;-c.wake:</code> |
| `internal/server/write.go:54` | node-done-receive: shutdown-only receive; obtaining Done() itself is not operation | <code>case &lt;-nodeDone:</code> |
| `internal/server/write.go:130` | unary-nil-space: nil unary channel: zero successful sends; default wins | <code>case st.space &lt;- struct{}{}:</code> |
| `internal/server/write.go:191` | stream-end-release: release accounting/flow control; no room channel exists at these targets | <code>c.releaseLocked(&amp;st.replyHeld)</code> |
| `internal/wal/wal.go:351` | wal-value-copy: counted copy: 1 direct Entry.Data append per persisted entry containing Put | <code>w.buf = append(w.buf, p...)</code> |
| `internal/wal/wal.go:368` | wal-value-caller: WAL Entry.Data caller, not a second copy | <code>w.record(recEntry, h[:], e.GetData())</code> |
| `internal/wal/wal.go:386` | wal-write: file I/O/physical copies not enumerated in payload subtotal | <code>n, err := w.seg.Write(w.buf)</code> |
| `internal/wal/wal.go:416` | wal-sync: durability I/O, not byte materialization counted here | <code>if err := w.seg.Sync(); err != nil {</code> |

## txn — `6179f12c2bbaaffc9960782533ad83c292605962`
| File:line | Site / classification | Exact text (trimmed) |
|---|---|---|
| `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc.pb.go:760` | lib-put-value-field: Put.Value is proto3 bytes | <code>Value []byte `protobuf:&quot;bytes,2,opt,name=value,proto3&quot; json:&quot;value,omitempty&quot;`</code> |
| `dependency-sources/etcd-api-v3.7.2/mvccpb/kv.pb.go:84` | lib-returned-value-field: returned stored value is proto3 bytes: same owning encoder behavior | <code>Value []byte `protobuf:&quot;bytes,5,opt,name=value,proto3&quot; json:&quot;value,omitempty&quot;`</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` | lib-bytes-encode-copy: counted encoding instruction: main entry or peer Entry.Data or CAS failure reply, each boundary counted only once | <code>return append(AppendVarint(b, uint64(len(v))), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:468` | lib-bytes-consume-alias: wire ParsePut gets subslice, not copied bytes | <code>return b[n:][:m], n + int(m)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5418` | lib-proto2-decode-copy: counted receiver Entry.Data decode copy all targets | <code>*p.Bytes() = append(emptyBuf[:], v...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5483` | proto3-bytes-encoder: bytes encoders call AppendBytes; not an additional copy | <code>b = protowire.AppendBytes(b, v)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5497` | lib-proto3-decode-copy: one owning proto3 bytes decode stage: main/wire/hpack CAS frontend/apply; ordinary Put bypasses ALL targets; not txn CAS | <code>*p.Bytes() = append(([]byte)(nil), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:278` | lib-proto3-codec-selection: proto3 Put.Value and KeyValue.Value non-presence bytes codec selection | <code>return nil, coderBytesNoZero</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:428` | lib-proto2-codec-selection: presence bytes coder selection for Entry.Data | <code>return nil, coderBytes</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/convert.go:358` | reflect-bytes-convert: []byte-to-[]byte conversion retains backing store; no append/copy | <code>return reflect.ValueOf(v.Bytes()).Convert(c.goType)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188` | lib-proto2-clone-copy: counted: Raft leader Entry.Data deep clone, proto2 (not Put proto3 decoder) | <code>*dst.Bytes() = append(emptyBuf[:], *src.Bytes()...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/message_reflect_field.go:317` | reflect-scalar-set: Put.Value scalar setter copies slice header, not value bytes | <code>rv.Set(conv.GoValueOf(v))</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/message_reflect_gen.go:97` | reflect-set: reflection Set dispatch, no byte clone | <code>fi.set(m.pointer(), v)</code> |
| `dependency-sources/protobuf-v1.36.11/proto/merge.go:58` | lib-proto-clone-merge: Clone creates destination then merges fields | <code>mergeOptions{}.mergeMessage(dst, src)</code> |
| `dependency-sources/raft-v3.7.0/log.go:127` | follower-logappend: follower retains entry pointers | <code>l.append(a.entries[ci-offset:]...)</code> |
| `dependency-sources/raft-v3.7.0/log_unstable.go:205` | unstable-append: append []*Entry pointers, not Data bytes | <code>u.entries = append(u.entries, ents...)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:656` | lib-peer-construction-alias: zero payload copies when constructing append Message | <code>Entries: ents,</code> |
| `dependency-sources/raft-v3.7.0/raft.go:817` | lib-raft-clone: counted leader Entry.Data deep-clone boundary: 1/Put all targets | <code>cloned[i] = proto.Clone(es[i]).(*pb.Entry)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1349` | lib-raft-propose-clone-call: leader successful proposal invokes appendEntry clone | <code>if !r.appendEntry(m.GetEntries()...) {</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1800` | follower-maybeappend: follower uses maybeAppend, not leader appendEntry deep clone | <code>if mlastIndex, ok := r.raftLog.maybeAppend(a, m.GetCommit()); ok {</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.pb.go:348` | lib-entry-data-field: Entry.Data bytes descriptor | <code>Data          []byte                 `protobuf:&quot;bytes,4,opt,name=Data&quot; json:&quot;Data,omitempty&quot;`</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.proto:1` | lib-entry-proto2: Entry.Data uses proto2 bytes, not proto3 Put decoder | <code>syntax = &quot;proto2&quot;;</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:94` | lib-raft-propose-alias: RawNode boundary aliases data; later appendEntry clones it | <code>Entries: []*pb.Entry{{Data: data}},</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:228` | async-append-alias: local Raft async append message retains entry pointers | <code>Entries: rd.Entries,</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:393` | async-apply-alias: local Raft async apply message retains entry pointers | <code>Entries: ents,</code> |
| `dependency-sources/raft-v3.7.0/storage.go:318` | lib-storage-pointer-append1: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents[:offset:offset], entries...)</code> |
| `dependency-sources/raft-v3.7.0/storage.go:320` | lib-storage-pointer-append2: zero Entry.Data copies: append []*Entry pointers | <code>ms.ents = append(ms.ents, entries...)</code> |
| `internal/core/alias.go:47` | alias-unknown: conditional unknown/mistyped field copy, excluded canonical scoped request | <code>m.SetUnknown(append(append(m.GetUnknown(), tag...), b[:n]...))</code> |
| `internal/core/alias.go:100` | alias-nested-list: rebuilds repeated Compare/RequestOp messages via reflection | <code>sub = m.Mutable(fd).List().AppendMutable().Message()</code> |
| `internal/core/alias.go:104` | alias-nested-message: rebuilds singular/oneof nested request messages | <code>sub = m.Mutable(fd).Message()</code> |
| `internal/core/alias.go:130` | alias-set: reflection bytes field setter retains slice, dependency proof verifies no clone | <code>m.Set(fd, x)</code> |
| `internal/core/alias.go:160` | alias-value: bytes fields are capped aliases, not owning byte buffers | <code>return protoreflect.ValueOfBytes(slices.Clip(x)), n, nil</code> |
| `internal/core/codec.go:37` | entry-buffer-init: entry capacity: main/wire/hpack/txn fully size-reserved; no extra fixed relocation invented | <code>buf := binary.AppendUvarint(make([]byte, 0, size+binary.MaxVarintLen64), uint64(len(ps)))</code> |
| `internal/core/codec.go:68` | entry-value-encode: counted: one copy of each full input value into encoded entry, NOT amortized payload bytes | <code>buf = append(buf, p.req.Put.Wire...)</code> |
| `internal/core/codec.go:73` | txn-entry-copy: one input full-value materialization into Raft entry; both CAS outcomes | <code>buf = append(buf, p.req.Wire...)</code> |
| `internal/core/codec.go:78` | generic-entry-value-encode: one entire CAS input-value copy for main/wire/hpack; txn Wire bypasses generic encoder | <code>buf, err = proto.MarshalOptions{Deterministic: true, UseCachedSize: true}.MarshalAppend(buf, p.req.Op)</code> |
| `internal/core/codec.go:108` | wire-batch-slice: zero copy: capped command subslice passed to apply | <code>if err := fn(origin, boot, id, kind, data[:size:size]); err != nil {</code> |
| `internal/core/codec.go:127` | new-txn-op: apply still creates fresh TxnRequest struct, including txn alias path | <code>return &amp;pb.TxnRequest{}, nil</code> |
| `internal/core/codec.go:150` | decode-batch-still-exists: generic helper used by checks, NOT ordinary production Put apply | <code>if err := proto.Unmarshal(b, op); err != nil {</code> |
| `internal/core/node.go:108` | request-wire-field: PR80 Request.Wire is an alias slice header, not channel or owning byte copy | <code>Wire []byte</code> |
| `internal/core/node.go:148` | pending-owner: pending record retains request slice headers; zero value copy | <code>req      Request</code> |
| `internal/core/node.go:376` | batch-pending-allocation: one pending-record backing array per SubmitAll; not startup arena | <code>ps := make([]pending, len(rs))</code> |
| `internal/core/node.go:378` | submitall-loop: direct per-record method call into batch-owned pending array; zero sends/receives/closes | <code>n.submit(rs[i], &amp;ps[i])</code> |
| `internal/core/node.go:389` | wire-check: validate fields/size without generic protobuf copy | <code>if err := checkPut(&amp;r.Put); err != nil {</code> |
| `internal/core/node.go:411` | txn-write-route: only wholly read-only transactions take read route | <code>if mvcc.IsTxnReadOnly(op) {</code> |
| `internal/core/node.go:518` | wire-size: whole client request size plus framing upper bound | <code>size := len(p.req.Put.Wire) + batchFraming</code> |
| `internal/core/node.go:586` | core-encode-call: entry encode caller, not an additional copy | <code>data, err := encodeBatch(n.cfg.ID, n.cfg.Boot, props, size)</code> |
| `internal/core/node.go:591` | raft-propose: leader proposal -> dependency clone: counted separately below | <code>if err := n.rn.Propose(data); err != nil {</code> |
| `internal/core/node.go:859` | apply-dispatch: main/batch decoded apply vs wire in-place walk | <code>err := walkBatch(e.GetData(), func(origin, boot, id uint64, kind byte, b []byte) error {</code> |
| `internal/core/node.go:864` | apply-wire-kind: Put bypasses generic apply protobuf decode on every replica | <code>if kind == kindPut {</code> |
| `internal/core/node.go:872` | alias-apply: CAS apply aliases entry bytes even for unexecuted success branch; fresh request structs remain | <code>if err := UnmarshalAlias(b, op); err != nil {</code> |
| `internal/core/node.go:901` | apply-wire-parse: parse entry in place: zero value copies | <code>put, err := ParsePut(b)</code> |
| `internal/core/node.go:905` | apply-wire-state: state fields forwarded: zero value copies | <code>prev, err := n.state.ApplyPut(&amp;put.PutOp)</code> |
| `internal/core/node.go:926` | apply-wire-reply: header/metadata reply, not a value copy | <code>n.out.Replies = append(n.out.Replies, Reply{ID: p.req.ID, Rev: n.kv.Rev(), Term: n.term, PrevKv: prev})</code> |
| `internal/core/state.go:110` | wire-state-index: zero copy: direct wire Put fields path into MVCC | <code>prev, err := st.kv.PutKV(op)</code> |
| `internal/core/state.go:140` | state-txn: replicated CAS apply: MVCC chooses branch | <code>r, err := st.kv.Txn(op)</code> |
| `internal/core/storage.go:39` | storage-pointer-append: MemoryStorage appends entry pointers, not Entry.Data bytes | <code>if err := s.Append(job.Entries); err != nil {</code> |
| `internal/core/watch.go:272` | no-watch-fastpath: no-watch match returns without value encoding | <code>if len(changes) == 0 &#124;&#124; len(n.watchers) == 0 {</code> |
| `internal/core/wire.go:61` | wire-parse-consume: dependency returns subslice: zero copy | <code>v, n := protowire.ConsumeBytes(b)</code> |
| `internal/core/wire.go:67` | wire-parse-clip: slice capacity clip, no byte copy | <code>v = slices.Clip(v)</code> |
| `internal/core/wire.go:71` | wire-value-alias: value slice aliases Wire/Entry.Data | <code>p.Value = v</code> |
| `internal/mvcc/ops.go:32` | revision-changed: MVCC revision changes only for actual state mutation | <code>if w.changed {</code> |
| `internal/mvcc/ops.go:85` | range-kv: CAS failure Range: KeyValue objects and slice headers only; zero value copy | <code>kvs = append(kvs, k.kv(v))</code> |
| `internal/mvcc/ops.go:191` | putop-alias: PutRequest-to-PutOp copies slice headers, not bytes | <code>return PutOp{Key: p.Key, Value: p.Value, Lease: p.Lease, PrevKv: p.PrevKv, IgnoreValue: p.IgnoreValue, IgnoreLease: p.IgnoreLease}</code> |
| `internal/mvcc/ops.go:250` | index-version-append: metadata/slice headers only: zero payload copies | <code>k.vers = append(k.vers, v)</code> |
| `internal/mvcc/ops.go:259` | index-value: zero payload copies: entry backing store for ordinary Put ALL targets and txn CAS; decoded bytes for main/wire/hpack CAS | <code>v := version{create: w.begin + 1, ver: 1, lease: p.Lease, value: p.Value}</code> |
| `internal/mvcc/ops.go:277` | index-key: zero key bytes copy at insert: retains key slice; key copies are not value copies | <code>k = &amp;keyIndex{key: p.Key}</code> |
| `internal/mvcc/ops.go:320` | compare-path: compare selects branch at replicated apply | <code>path := w.compareToPath(r)</code> |
| `internal/mvcc/ops.go:328` | txn-revision: CAS success increments revision; compare failure keeps current MVCC revision | <code>resp.Header = header(w.cur())</code> |
| `internal/mvcc/ops.go:366` | compare-kv: existing-key compare constructs KeyValue with aliased stored value; zero value copy | <code>if !compareKV(c, k.kv(v)) {</code> |
| `internal/mvcc/ops.go:390` | compare-mod: CAS compare uses integer mod revision, not value bytes | <code>result = cmpInt(kv.ModRevision, c.GetModRevision())</code> |
| `internal/mvcc/ops.go:470` | txn-success: Succeeded reports compare result; failure is still successful RPC | <code>Succeeded: path[0],</code> |
| `internal/mvcc/ops.go:478` | txn-range: compare failure: executes current Range only; return existing stored value | <code>rr, err := w.rangeKeys(t.RequestRange)</code> |
| `internal/mvcc/ops.go:485` | txn-put: success Put executes; failure skips this branch | <code>out.Response = &amp;pb.ResponseOp_ResponsePut{ResponsePut: &amp;pb.PutResponse{Header: header(w.begin + 1), PrevKv: w.put(&amp;op)}}</code> |
| `internal/mvcc/store.go:73` | kv-value-alias: zero stored-value copy: response/compare KeyValue retains value slice header | <code>Value:          v.value,</code> |
| `internal/mvcc/validate.go:185` | txn-readonly: both branches classified before compare; a Put in Success forces write/Raft route | <code>for _, ops := range [][]*pb.RequestOp{r.Success, r.Failure} {</code> |
| `internal/mvcc/validate.go:187` | txn-write-detect: success Put means not read-only even when compare will fail | <code>if op.GetRequestRange() == nil {</code> |
| `internal/node/node.go:136` | admission-send: counted: one send per actual admission batch, all targets | <code>case n.inbox &lt;- in:</code> |
| `internal/node/node.go:169` | driver-batch-owner: driver batch owns/shared request descriptors; header-only, not channel | <code>reqs  []core.Request</code> |
| `internal/node/node.go:190` | batch-api: direct API, not channel operation | <code>func (n *Node) SubmitBatch(ctx context.Context, reqs []core.Request, tags []any, to Completer) error {</code> |
| `internal/node/node.go:199` | batch-wrapper: batch wrapper retains descriptor/tag backing stores; no payload copy or channel exchange | <code>b := &amp;batch{first: n.nextID.Add(k) - k + 1, reqs: reqs, tags: tags, to: to}</code> |
| `internal/node/node.go:200` | batch-inbox-call: one inbox admission handoff per batch via actual send site; this call is not an extra exchange | <code>return n.put(ctx, input{batch: b})</code> |
| `internal/node/node.go:266` | peer-inbox-send: counted send: 1/delivered inbound peer message | <code>case n.inbox &lt;- input{msg: m}:</code> |
| `internal/node/node.go:274` | unreachable-send: conditional counted U send when nonblocking Unreachable report succeeds; saturation/drop/fault, not fixed per request | <code>case n.inbox &lt;- input{unreachable: id}:</code> |
| `internal/node/node.go:281` | do-reply-create: excluded retained Node.Do channel creation: zero per ordinary Put or CAS | <code>r := &amp;request{id: n.nextID.Add(1), op: op, reply: make(chan core.Reply, 1)}</code> |
| `internal/node/node.go:286` | do-reply-receive: excluded retained Node.Do: not ordinary Put or CAS | <code>case rep := &lt;-r.reply:</code> |
| `internal/node/node.go:308` | shutdown-close: close: shutdown-only, zero per normal Put | <code>close(n.done)</code> |
| `internal/node/node.go:335` | inbox-receive-outer: counted selected receive: outer alternative for admission, inbound peer or successful Unreachable report | <code>case in := &lt;-inbox:</code> |
| `internal/node/node.go:340` | inbox-receive-drain: counted selected receive: drain alternative for another input; not extra receipt of outer input | <code>case in := &lt;-n.inbox:</code> |
| `internal/node/node.go:346` | disk-complete-receive: counted receive: 1/sync completion group | <code>case done := &lt;-n.persisted:</code> |
| `internal/node/node.go:353` | disk-send: counted send: 1/persist queue array | <code>case toDisk &lt;- next:</code> |
| `internal/node/node.go:357` | tick-receive: counted selected background receive, not fixed per Put | <code>case &lt;-ticker.C:</code> |
| `internal/node/node.go:394` | watch-close: close: watch overflow, outside no-watch scope | <code>close(ws.done)</code> |
| `internal/node/node.go:420` | do-reply-send: excluded retained Node.Do: not ordinary Put or CAS | <code>w.reply &lt;- r</code> |
| `internal/node/node.go:432` | completion-direct: direct grouped call: zero channel ops | <code>g.to.Complete(g.cs)</code> |
| `internal/node/node.go:446` | batch-waiter: per-request completer/tag metadata, no private reply channel | <code>waiters[r.ID] = waiter{to: b.to, tag: b.tags[i]}</code> |
| `internal/node/node.go:448` | batch-core-submit: direct batch submission, zero payload copy | <code>n.core.SubmitAll(b.reqs)</code> |
| `internal/node/node.go:486` | disk-receive-outer: counted receive: initial array; alternative to drain | <code>case jobs = &lt;-n.persistQ:</code> |
| `internal/node/node.go:493` | disk-receive-drain: counted receive: drained array; NOT an extra receive for initial array | <code>case js := &lt;-n.persistQ:</code> |
| `internal/node/node.go:500` | wal-append-call: driver disk trace; no extra payload copy beyond WAL record | <code>n.wal.Append(j.HardState, j.Entries)</code> |
| `internal/node/node.go:503` | wal-sync-call: driver disk trace; I/O, not counted as extra userspace payload materialization | <code>if err := n.wal.Sync(); err != nil {</code> |
| `internal/node/node.go:507` | disk-complete-send: counted send: 1/sync completion group | <code>case n.persisted &lt;- done:</code> |
| `internal/peer/peer.go:241` | peer-value-decode: counted copy: 1/received occurrence of Entry.Data, on receiver; dependency decoder below | <code>if err := proto.Unmarshal(buf, m); err != nil {</code> |
| `internal/peer/peer.go:297` | peer-queue-send: counted send: 1/queued outbound peer message | <code>case q &lt;- m:</code> |
| `internal/peer/peer.go:308` | peer-queue-receive-fast: counted receive alternative 1: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:314` | peer-queue-receive-control: counted receive alternative 2: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:317` | peer-queue-receive-data: counted receive alternative 3: one receipt per message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:320` | peer-shutdown-receive-1: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:331` | peer-drop-control: fault-only receive: dropped queued control message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:333` | peer-drop-data: fault-only receive: dropped queued data message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:350` | peer-backoff-receive: fault-only timer receive, excluded healthy path | <code>case &lt;-time.After(backoff):</code> |
| `internal/peer/peer.go:351` | peer-shutdown-receive-2: shutdown-only receive, excluded | <code>case &lt;-ctx.Done():</code> |
| `internal/peer/peer.go:419` | peer-value-encode: counted copy: 1/serialized occurrence of Entry.Data containing this Put, on sender | <code>buf, err = proto.MarshalOptions{}.MarshalAppend(buf[:0], m)</code> |
| `internal/server/batch.go:39` | opbatch-owner: connection admission batch owns request descriptors; header-only, not channel | <code>reqs  []core.Request</code> |
| `internal/server/batch.go:59` | wire-frontend-parse: wire frontend parse: zero value copies | <code>put, err := core.ParsePut(st.body)</code> |
| `internal/server/batch.go:68` | frontend-new-op: request structs still constructed for CAS; ordinary Put ParsePut bypasses ALL targets | <code>req.Op, req.Wire = st.rpc.op(), st.body</code> |
| `internal/server/batch.go:69` | alias-frontend: CAS frontend alias decode: zero owning input bytes decode; request graph remains | <code>if err := core.UnmarshalAlias(st.body, req.Op); err != nil {</code> |
| `internal/server/batch.go:118` | charge-request: accounting metadata only; no payload copy | <code>c.chargeLocked(&amp;st.reqHeld, st.reqSize+callCost)</code> |
| `internal/server/batch.go:128` | batch-submit-call: one admission API call per actual batch | <code>err := c.s.n.SubmitBatch(c.ctx, b.reqs, b.tags, c)</code> |
| `internal/server/batch.go:214` | completion-cas: completion publish: CAS, no Go channel and no completion mutex | <code>if c.completions.CompareAndSwap(n.next, n) {</code> |
| `internal/server/batch.go:218` | completion-wake-attempt: wake attempt per completion group, not guaranteed successful send | <code>c.signal()</code> |
| `internal/server/batch.go:227` | completion-swap: writer collects completion list atomically; zero channel ops | <code>for n := c.completions.Swap(nil); n != nil; n = n.next {</code> |
| `internal/server/batch.go:267` | wire-reply-encode: one buffer PER Put reply at verified target, PR body shared-buffer claim is stale | <code>msg, err := c.appendPutReply(make([]byte, 0, 5+putResponseSize), &amp;cp.Reply)</code> |
| `internal/server/batch.go:272` | batch-reply-encode: Put replies contain no input payload; CAS failure reply contains stored value and copies it once via response codec | <code>f.msg, f.err = marshal(cp.Resp)</code> |
| `internal/server/batch.go:279` | release-request: release accounting, zero channel exchange / payload copy | <code>c.releaseReqLocked(f.st)</code> |
| `internal/server/batch.go:285` | charge-reply: accounts encoded reply until sent; no payload copy | <code>c.chargeLocked(&amp;f.st.replyHeld, len(f.msg))</code> |
| `internal/server/batch.go:320` | manual-response-prefix: gRPC small reply prefix, zero input-value copy | <code>b = append(b, 0, 0, 0, 0, 0)</code> |
| `internal/server/batch.go:321` | manual-response-append: wire small response, no value bytes when PrevKv nil | <code>b, err := appendPutResponse(b, c.s.n.ClusterID(), c.s.n.ID(), r.Rev, r.Term, r.PrevKv)</code> |
| `internal/server/h2.go:190` | goaway-wake-attempt: lifecycle wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:201` | writer-wake-send: counted successful conditional/coalesced wake send; default is not operation | <code>case c.wake &lt;- struct{}{}:</code> |
| `internal/server/h2.go:214` | control-wake-attempt: control wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:339` | settings-wake-attempt: settings wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:354` | window-wake-attempt: send-window wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:382` | reset-wake-attempt: reset wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:392` | credit-wake-attempt: wake attempt per DATA frame, not a guaranteed successful send | <code>c.signal()</code> |
| `internal/server/h2.go:422` | hpack-decode: per-connection HPACK decoder, header bytes are not Put/CAS payload | <code>if _, err := c.hdec.Write(b); err != nil {</code> |
| `internal/server/h2.go:473` | hpack-field: indexed header fields retained without fresh MetaHeaders field list; literal decoding may allocate | <code>bit, h.path = 2, f.Value</code> |
| `internal/server/h2.go:574` | stream-context-gate: ordinary batch/wire Put creates no per-stream context | <code>if !grpc &#124;&#124; (st.rpc != nil &amp;&amp; st.rpc.op == nil) {</code> |
| `internal/server/h2.go:593` | stream-deadline: optional deadline callback timer, not Timer.C exchange | <code>st.deadline = time.AfterFunc(h.timeout, st.expire)</code> |
| `internal/server/h2.go:631` | receive-data-caller: Framer DATA dispatched to owned message accumulation | <code>if err := st.receive(f.Data()); err != nil {</code> |
| `internal/server/h2.go:650` | unary-nil-signal: nil unary channel: zero successful sends; default wins | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/h2.go:658` | stream-admit: ordinary Put is synchronous reader admission, no per-Put handler goroutine | <code>c.admit(st)</code> |
| `internal/server/rpc.go:63` | frontend-decode: owning frontend decode: counted main/wire/hpack CAS only; ordinary Put bypasses on ALL targets; txn CAS bypasses | <code>if err := proto.Unmarshal(b, m); err != nil {</code> |
| `internal/server/server.go:189` | server-stop-close: close: server Stop wait channel, not per Put | <code>close(done)</code> |
| `internal/server/stream.go:124` | frontend-prealloc: full request-sized capacity prevents accumulation growth; not a separate copy | <code>st.msg = make([]byte, 0, size)</code> |
| `internal/server/stream.go:127` | frontend-accumulation: counted copy: 1 full-value equivalent over all request chunks | <code>st.msg = append(st.msg, d[:n]...)</code> |
| `internal/server/stream.go:139` | unary-delivery: retains owned body header and returns before streaming-signal select | <code>st.body = m</code> |
| `internal/server/stream.go:148` | unary-nil-body-signal: streaming-only select NOT executed by unary delivery: zero sends | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/stream.go:207` | response-proto-encode: CAS failure: one full stored-value copy into reply; ordinary Put/CAS success: zero value copy | <code>b, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(b, m)</code> |
| `internal/server/stream.go:280` | out-capacity: per-stream output array still allocated, room for ordinary unary reply | <code>st.out = make([]outItem, 0, 3)</code> |
| `internal/server/write.go:52` | writer-wake-receive: counted selected wake receive only while idle | <code>case &lt;-c.wake:</code> |
| `internal/server/write.go:54` | node-done-receive: shutdown-only receive; obtaining Done() itself is not operation | <code>case &lt;-nodeDone:</code> |
| `internal/server/write.go:130` | unary-nil-space: nil unary channel: zero successful sends; default wins | <code>case st.space &lt;- struct{}{}:</code> |
| `internal/server/write.go:191` | stream-end-release: release accounting/flow control; no room channel exists at these targets | <code>c.releaseLocked(&amp;st.replyHeld)</code> |
| `internal/wal/wal.go:351` | wal-value-copy: counted copy: 1 direct Entry.Data append per persisted entry containing Put | <code>w.buf = append(w.buf, p...)</code> |
| `internal/wal/wal.go:368` | wal-value-caller: WAL Entry.Data caller, not a second copy | <code>w.record(recEntry, h[:], e.GetData())</code> |
| `internal/wal/wal.go:386` | wal-write: file I/O/physical copies not enumerated in payload subtotal | <code>n, err := w.seg.Write(w.buf)</code> |
| `internal/wal/wal.go:416` | wal-sync: durability I/O, not byte materialization counted here | <code>if err := w.seg.Sync(); err != nil {</code> |

