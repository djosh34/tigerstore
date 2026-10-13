# Exact verified Put/channel sites — FINAL

Every source line asserted; immutable commit and file hashes attached. Dependencies match unchanged prior-verified bytes/pins. A codec caller and instruction count once.

## main — `aa9d9b6eea79c73f12fe0153aefa809841967e88`
| File:line | Site/classification | Exact text |
|---|---|---|
| `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc.pb.go:760` | lib-put-value-field: Put.Value is nonempty proto3 bytes; frontend/apply Put path bypasses owning decoder | <code>Value []byte `protobuf:&quot;bytes,2,opt,name=value,proto3&quot; json:&quot;value,omitempty&quot;`</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` | lib-bytes-encode-copy: peer encoding implementing full Entry.Data append; caller and instruction ONE stage; ordinary Put metadata reply has no input value | <code>return append(AppendVarint(b, uint64(len(v))), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:468` | lib-bytes-consume-alias: wire ParsePut gets subslice, not copied bytes | <code>return b[n:][:m], n + int(m)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5418` | lib-proto2-decode-copy: counted one receiver owning Entry.Data decode per transmitted occurrence | <code>*p.Bytes() = append(emptyBuf[:], v...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:428` | lib-proto2-codec-selection: presence bytes coder selection for Entry.Data | <code>return nil, coderBytes</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188` | lib-proto2-clone-copy: counted proto2 Entry.Data leader deep clone, not proto3 owning decoder | <code>*dst.Bytes() = append(emptyBuf[:], *src.Bytes()...)</code> |
| `dependency-sources/protobuf-v1.36.11/proto/merge.go:58` | lib-proto-clone-merge: Clone creates destination then merges fields | <code>mergeOptions{}.mergeMessage(dst, src)</code> |
| `dependency-sources/raft-v3.7.0/log.go:127` | follower-logappend: follower retains entry pointers | <code>l.append(a.entries[ci-offset:]...)</code> |
| `dependency-sources/raft-v3.7.0/log_unstable.go:205` | unstable-append: append []*Entry pointers, not Data bytes | <code>u.entries = append(u.entries, ents...)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:656` | lib-peer-construction-alias: zero payload copies when constructing append Message | <code>Entries: ents,</code> |
| `dependency-sources/raft-v3.7.0/raft.go:817` | lib-raft-clone: counted one leader Entry.Data deep clone per proposed entry containing full value | <code>cloned[i] = proto.Clone(es[i]).(*pb.Entry)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1349` | lib-raft-propose-clone-call: leader successful proposal invokes appendEntry clone | <code>if !r.appendEntry(m.GetEntries()...) {</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1800` | follower-maybeappend: follower uses maybeAppend, not leader appendEntry deep clone | <code>if mlastIndex, ok := r.raftLog.maybeAppend(a, m.GetCommit()); ok {</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.pb.go:348` | lib-entry-data-field: Entry.Data bytes descriptor | <code>Data          []byte                 `protobuf:&quot;bytes,4,opt,name=Data&quot; json:&quot;Data,omitempty&quot;`</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.proto:1` | lib-entry-proto2: Entry.Data uses proto2 bytes, not proto3 Put decoder | <code>syntax = &quot;proto2&quot;;</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:94` | lib-raft-propose-alias: RawNode boundary aliases data; later appendEntry clones it | <code>Entries: []*pb.Entry{{Data: data}},</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:228` | async-append-alias: local Raft async append message retains entry pointers | <code>Entries: rd.Entries,</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:393` | async-apply-alias: local Raft async apply message retains entry pointers | <code>Entries: ents,</code> |
| `internal/core/codec.go:37` | entry-buffer-init: entry capacity fully reserved in BOTH; no fixed extra growth relocation assigned | <code>buf := binary.AppendUvarint(make([]byte, 0, size+binary.MaxVarintLen64), uint64(len(ps)))</code> |
| `internal/core/codec.go:68` | entry-value-encode: counted one full new Put value append to entry BOTH targets | <code>buf = append(buf, p.req.Put.Wire...)</code> |
| `internal/core/codec.go:103` | wire-batch-slice: capped entry subslice passed to apply, zero owning bytes copy | <code>if err := fn(origin, boot, id, kind, data[:size:size]); err != nil {</code> |
| `internal/core/node.go:145` | pending-owner: request descriptor retention, not copied value bytes or channel | <code>req      Request</code> |
| `internal/core/node.go:373` | batch-pending-allocation: one pending-record backing array per SubmitAll BOTH, not startup arena or channel | <code>ps := make([]pending, len(rs))</code> |
| `internal/core/node.go:375` | submitall-loop: direct per-record method call into batch-owned pending array; zero sends/receives/closes | <code>n.submit(rs[i], &amp;ps[i])</code> |
| `internal/core/node.go:386` | wire-check: validate fields/size without generic protobuf copy | <code>if err := checkPut(&amp;r.Put); err != nil {</code> |
| `internal/core/node.go:515` | wire-size: full Put wire length plus batch framing; metadata sizing, no value copy | <code>size := len(p.req.Put.Wire) + batchFraming</code> |
| `internal/core/node.go:580` | core-encode-call: entry encode caller, not an additional copy | <code>data, err := encodeBatch(n.cfg.ID, n.cfg.Boot, props, size)</code> |
| `internal/core/node.go:585` | raft-propose: leader proposal -> dependency clone: counted separately below | <code>if err := n.rn.Propose(data); err != nil {</code> |
| `internal/core/node.go:853` | apply-dispatch: ordinary Put walks encoded entry and takes in-place ParsePut branch BOTH targets | <code>err := walkBatch(e.GetData(), func(origin, boot, id uint64, kind byte, b []byte) error {</code> |
| `internal/core/node.go:858` | apply-wire-kind: Put bypasses generic apply protobuf decode on every replica | <code>if kind == kindPut {</code> |
| `internal/core/node.go:894` | apply-wire-parse: parse entry in place: zero value copies | <code>put, err := ParsePut(b)</code> |
| `internal/core/node.go:898` | apply-wire-state: state fields forwarded: zero value copies | <code>prev, err := n.state.ApplyPut(&amp;put.PutOp)</code> |
| `internal/core/node.go:919` | apply-wire-reply: header/metadata reply, not a value copy | <code>n.out.Replies = append(n.out.Replies, Reply{ID: p.req.ID, Rev: n.kv.Rev(), Term: n.term, PrevKv: prev})</code> |
| `internal/core/state.go:110` | wire-state-index: zero copy: direct wire Put fields path into MVCC | <code>prev, err := st.kv.PutKV(op)</code> |
| `internal/core/storage.go:39` | storage-pointer-append: MemoryStorage appends entry pointers, not Entry.Data bytes | <code>if err := s.Append(job.Entries); err != nil {</code> |
| `internal/core/watch.go:272` | no-watch-fastpath: no-watch match returns without value encoding | <code>if len(changes) == 0 &#124;&#124; len(n.watchers) == 0 {</code> |
| `internal/core/wire.go:61` | wire-parse-consume: dependency returns subslice: zero copy | <code>v, n := protowire.ConsumeBytes(b)</code> |
| `internal/core/wire.go:67` | wire-parse-clip: slice capacity clip, no byte copy | <code>v = slices.Clip(v)</code> |
| `internal/core/wire.go:71` | wire-value-alias: value slice aliases Wire/Entry.Data | <code>p.Value = v</code> |
| `internal/mvcc/ops.go:191` | putop-alias: PutRequest-to-PutOp copies slice headers, not bytes | <code>return PutOp{Key: p.Key, Value: p.Value, Lease: p.Lease, PrevKv: p.PrevKv, IgnoreValue: p.IgnoreValue, IgnoreLease: p.IgnoreLease}</code> |
| `internal/mvcc/ops.go:250` | index-version-append: metadata/slice headers only: zero payload copies | <code>k.vers = append(k.vers, v)</code> |
| `internal/mvcc/ops.go:259` | index-value: Put retains Raft entry value slice header in MVCC; zero full-value copy BOTH targets | <code>v := version{create: w.begin + 1, ver: 1, lease: p.Lease, value: p.Value}</code> |
| `internal/mvcc/ops.go:277` | index-key: key metadata excluded from full-value stage census | <code>k = &amp;keyIndex{key: p.Key}</code> |
| `internal/node/node.go:136` | admission-send: counted: one send per actual admission batch, all targets | <code>case n.inbox &lt;- in:</code> |
| `internal/node/node.go:169` | driver-batch-owner: driver batch owns/shared request descriptors; header-only, not channel | <code>reqs  []core.Request</code> |
| `internal/node/node.go:190` | batch-api: SubmitBatch admission route for Put BOTH, not retained Node.Do | <code>func (n *Node) SubmitBatch(ctx context.Context, reqs []core.Request, tags []any, to Completer) error {</code> |
| `internal/node/node.go:199` | batch-wrapper: batch wrapper retains descriptor/tag backing stores; no payload copy or channel exchange | <code>b := &amp;batch{first: n.nextID.Add(k) - k + 1, reqs: reqs, tags: tags, to: to}</code> |
| `internal/node/node.go:200` | batch-inbox-call: one inbox admission handoff per batch via actual send site; this call is not an extra exchange | <code>return n.put(ctx, input{batch: b})</code> |
| `internal/node/node.go:266` | peer-inbox-send: counted send: 1/delivered inbound peer message | <code>case n.inbox &lt;- input{msg: m}:</code> |
| `internal/node/node.go:274` | unreachable-send: conditional U: successful nonblocking inbox report; saturation/drop/fault, not fixed per request | <code>case n.inbox &lt;- input{unreachable: id}:</code> |
| `internal/node/node.go:281` | do-reply-create: excluded retained Node.Do; zero fresh private reply channels for ordinary Put BOTH | <code>r := &amp;request{id: n.nextID.Add(1), op: op, reply: make(chan core.Reply, 1)}</code> |
| `internal/node/node.go:286` | do-reply-receive: excluded retained Node.Do; not ordinary Put route | <code>case rep := &lt;-r.reply:</code> |
| `internal/node/node.go:308` | shutdown-close: close: shutdown-only, zero per normal Put | <code>close(n.done)</code> |
| `internal/node/node.go:335` | inbox-receive-outer: counted selected receive: outer alternative for admission, inbound peer or successful Unreachable report | <code>case in := &lt;-inbox:</code> |
| `internal/node/node.go:340` | inbox-receive-drain: counted selected receive: drain alternative for another input; not extra receipt of outer input | <code>case in := &lt;-n.inbox:</code> |
| `internal/node/node.go:346` | disk-complete-receive: counted receive: 1/sync completion group | <code>case done := &lt;-n.persisted:</code> |
| `internal/node/node.go:353` | disk-send: counted send: 1/persist queue array | <code>case toDisk &lt;- next:</code> |
| `internal/node/node.go:357` | tick-receive: counted selected background receive, not fixed per Put | <code>case &lt;-ticker.C:</code> |
| `internal/node/node.go:394` | watch-close: close: watch overflow, outside no-watch scope | <code>close(ws.done)</code> |
| `internal/node/node.go:420` | do-reply-send: excluded retained Node.Do; not ordinary Put route | <code>w.reply &lt;- r</code> |
| `internal/node/node.go:432` | completion-direct: direct grouped call: zero channel ops | <code>g.to.Complete(g.cs)</code> |
| `internal/node/node.go:446` | batch-waiter: per-request completer/tag metadata, no private reply channel | <code>waiters[r.ID] = waiter{to: b.to, tag: b.tags[i]}</code> |
| `internal/node/node.go:448` | batch-core-submit: SubmitAll direct method, zero channel operations BOTH targets | <code>n.core.SubmitAll(b.reqs)</code> |
| `internal/node/node.go:486` | disk-receive-outer: counted receive: initial array; alternative to drain | <code>case jobs = &lt;-n.persistQ:</code> |
| `internal/node/node.go:493` | disk-receive-drain: counted receive: drained array; NOT an extra receive for initial array | <code>case js := &lt;-n.persistQ:</code> |
| `internal/node/node.go:500` | wal-append-call: driver disk trace; no extra payload copy beyond WAL record | <code>n.wal.Append(j.HardState, j.Entries)</code> |
| `internal/node/node.go:503` | wal-sync-call: driver disk trace; I/O, not counted as extra userspace payload materialization | <code>if err := n.wal.Sync(); err != nil {</code> |
| `internal/node/node.go:507` | disk-complete-send: counted send: 1/sync completion group | <code>case n.persisted &lt;- done:</code> |
| `internal/peer/peer.go:248` | peer-value-decode: counted copy: 1/received occurrence of Entry.Data, on receiver; dependency decoder below | <code>if err := proto.Unmarshal(buf, m); err != nil {</code> |
| `internal/peer/peer.go:304` | peer-queue-send: counted send: 1/queued outbound peer message | <code>case q &lt;- m:</code> |
| `internal/peer/peer.go:315` | peer-queue-receive-fast: counted receive alternative 1: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:321` | peer-queue-receive-control: counted receive alternative 2: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:324` | peer-queue-receive-data: counted receive alternative 3: one receipt per message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:426` | peer-value-encode: counted copy: 1/serialized occurrence of Entry.Data containing this Put, on sender | <code>buf, err = proto.MarshalOptions{}.MarshalAppend(buf[:0], m)</code> |
| `internal/server/batch.go:39` | opbatch-owner: connection batch owns descriptors; slice headers, not payload bytes or channels | <code>reqs  []core.Request</code> |
| `internal/server/batch.go:59` | wire-frontend-parse: wire frontend parse: zero value copies | <code>put, err := core.ParsePut(st.body)</code> |
| `internal/server/batch.go:93` | peek-header: buffered header peek, not full input-value materialization | <code>h, _ := c.br.Peek(frameHeaderLen)</code> |
| `internal/server/batch.go:116` | charge-request: accounting metadata only; no payload copy | <code>c.chargeLocked(&amp;st.reqHeld, st.reqSize+callCost)</code> |
| `internal/server/batch.go:126` | batch-submit-call: one admission API call per actual batch | <code>err := c.s.n.SubmitBatch(c.ctx, b.reqs, b.tags, c)</code> |
| `internal/server/batch.go:212` | completion-cas: completion publish: atomic compare-and-swap, no Go channel and no completion mutex | <code>if c.completions.CompareAndSwap(n.next, n) {</code> |
| `internal/server/batch.go:216` | completion-wake-attempt: wake attempt per completion group, not guaranteed successful send | <code>c.signal()</code> |
| `internal/server/batch.go:225` | completion-swap: writer collects completion list atomically; zero channel ops | <code>for n := c.completions.Swap(nil); n != nil; n = n.next {</code> |
| `internal/server/batch.go:265` | wire-reply-encode: one small buffer per ordinary Put metadata reply; no input value when PrevKv disabled | <code>msg, err := c.appendPutReply(make([]byte, 0, 5+putResponseSize), &amp;cp.Reply)</code> |
| `internal/server/batch.go:277` | release-request: release accounting, zero channel exchange / payload copy | <code>c.releaseReqLocked(f.st)</code> |
| `internal/server/batch.go:283` | charge-reply: accounts encoded reply until sent; no payload copy | <code>c.chargeLocked(&amp;f.st.replyHeld, len(f.msg))</code> |
| `internal/server/batch.go:318` | manual-response-prefix: gRPC small reply prefix, zero input-value copy | <code>b = append(b, 0, 0, 0, 0, 0)</code> |
| `internal/server/batch.go:319` | manual-response-append: wire small response, no value bytes when PrevKv nil | <code>b, err := appendPutResponse(b, c.s.n.ClusterID(), c.s.n.ID(), r.Rev, r.Term, r.PrevKv)</code> |
| `internal/server/h2.go:43` | io-buffer-size: existing transport buffer size, no per-request copy count implied | <code>ioBuffer     = 64 &lt;&lt; 10</code> |
| `internal/server/h2.go:121` | existing-read-buffer: existing 64KiB bufio reader BOTH; not a new PR84 owning-request stage | <code>br:       bufio.NewReaderSize(nc, ioBuffer),</code> |
| `internal/server/h2.go:201` | writer-wake-send: counted successful conditional/coalesced wake send; default is not operation | <code>case c.wake &lt;- struct{}{}:</code> |
| `internal/server/h2.go:214` | control-wake-attempt: control wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:263` | frame-header-read: existing frame-header read call; not a runtime syscall count | <code>fh, err := c.fr.ReadFrameHeader()</code> |
| `internal/server/h2.go:267` | frame-read: existing Framer payload read; transport work excluded, no new clock copy destination | <code>f, err := c.fr.ReadFrameForHeader(fh)</code> |
| `internal/server/h2.go:339` | settings-wake-attempt: settings wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:354` | window-wake-attempt: send-window wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:392` | credit-wake-attempt: wake attempt per DATA frame, not a guaranteed successful send | <code>c.signal()</code> |
| `internal/server/h2.go:422` | hpack-decode: per-connection HPACK decoder, header bytes are not Put payload | <code>if _, err := c.hdec.Write(b); err != nil {</code> |
| `internal/server/h2.go:473` | hpack-field: indexed header fields retained without fresh MetaHeaders field list; literal decoding may allocate | <code>bit, h.path = 2, f.Value</code> |
| `internal/server/h2.go:574` | stream-context-gate: ordinary admitted unary Put does not create private per-stream context | <code>if !grpc &#124;&#124; (st.rpc != nil &amp;&amp; st.rpc.op == nil) {</code> |
| `internal/server/h2.go:578` | stream-channel-guard: streaming-only conditional allocation; unary Put leave inSignal and space nil | <code>st.inSignal, st.space = make(chan struct{}, 1), make(chan struct{}, 1)</code> |
| `internal/server/h2.go:590` | timeout-guard: no timed lean grpc-timeout: per-stream deadline timer not armed | <code>if grpc &amp;&amp; h.hasTimeout {</code> |
| `internal/server/h2.go:593` | stream-deadline: optional grpc-timeout AfterFunc absent for timed lean Put; control Range deadlines outside cohort | <code>st.deadline = time.AfterFunc(h.timeout, st.expire)</code> |
| `internal/server/h2.go:631` | receive-data-caller: Framer DATA dispatched to owned message accumulation | <code>if err := st.receive(f.Data()); err != nil {</code> |
| `internal/server/h2.go:650` | unary-nil-signal: nil unary channel: zero successful sends; default wins | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/h2.go:658` | stream-admit: ordinary Put is synchronous reader admission, no per-Put handler goroutine | <code>c.admit(st)</code> |
| `internal/server/server.go:189` | server-stop-close: close: server Stop wait channel, not per Put | <code>close(done)</code> |
| `internal/server/stream.go:124` | frontend-prealloc: full request-sized capacity prevents accumulation growth; not a separate copy | <code>st.msg = make([]byte, 0, size)</code> |
| `internal/server/stream.go:127` | frontend-accumulation: counted one owned full Put value accumulation from reusable Framer DATA payload BOTH | <code>st.msg = append(st.msg, d[:n]...)</code> |
| `internal/server/stream.go:139` | unary-delivery: retains owned body header and returns before streaming-signal select | <code>st.body = m</code> |
| `internal/server/stream.go:148` | unary-nil-body-signal: streaming-only select NOT executed by unary delivery: zero sends | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/stream.go:280` | out-capacity: per-stream output array still allocated, room for ordinary unary reply | <code>st.out = make([]outItem, 0, 3)</code> |
| `internal/server/write.go:52` | writer-wake-receive: counted selected wake receive only while idle | <code>case &lt;-c.wake:</code> |
| `internal/server/write.go:54` | node-done-receive: shutdown-only receive; obtaining Done() itself is not operation | <code>case &lt;-nodeDone:</code> |
| `internal/server/write.go:130` | unary-nil-space: nil unary channel: zero successful sends; default wins | <code>case st.space &lt;- struct{}{}:</code> |
| `internal/server/write.go:191` | stream-end-release: release accounting/flow control; no room channel exists at these targets | <code>c.releaseLocked(&amp;st.replyHeld)</code> |
| `internal/wal/wal.go:351` | wal-value-copy: counted copy: 1 direct Entry.Data append per persisted entry containing Put | <code>w.buf = append(w.buf, p...)</code> |
| `internal/wal/wal.go:368` | wal-value-caller: WAL Entry.Data caller, not a second copy | <code>w.record(recEntry, h[:], e.GetData())</code> |
| `internal/wal/wal.go:386` | wal-write: file I/O/physical copies not enumerated in payload subtotal | <code>n, err := w.seg.Write(w.buf)</code> |
| `internal/wal/wal.go:416` | wal-sync: durability I/O, not byte materialization counted here | <code>if err := w.seg.Sync(); err != nil {</code> |

## pr84 — `d5681651cd554b8804ed3a77b9ff019ade6815bf`
| File:line | Site/classification | Exact text |
|---|---|---|
| `dependency-sources/etcd-api-v3.7.2/etcdserverpb/rpc.pb.go:760` | lib-put-value-field: Put.Value is nonempty proto3 bytes; frontend/apply Put path bypasses owning decoder | <code>Value []byte `protobuf:&quot;bytes,2,opt,name=value,proto3&quot; json:&quot;value,omitempty&quot;`</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` | lib-bytes-encode-copy: peer encoding implementing full Entry.Data append; caller and instruction ONE stage; ordinary Put metadata reply has no input value | <code>return append(AppendVarint(b, uint64(len(v))), v...)</code> |
| `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:468` | lib-bytes-consume-alias: wire ParsePut gets subslice, not copied bytes | <code>return b[n:][:m], n + int(m)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5418` | lib-proto2-decode-copy: counted one receiver owning Entry.Data decode per transmitted occurrence | <code>*p.Bytes() = append(emptyBuf[:], v...)</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/codec_tables.go:428` | lib-proto2-codec-selection: presence bytes coder selection for Entry.Data | <code>return nil, coderBytes</code> |
| `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188` | lib-proto2-clone-copy: counted proto2 Entry.Data leader deep clone, not proto3 owning decoder | <code>*dst.Bytes() = append(emptyBuf[:], *src.Bytes()...)</code> |
| `dependency-sources/protobuf-v1.36.11/proto/merge.go:58` | lib-proto-clone-merge: Clone creates destination then merges fields | <code>mergeOptions{}.mergeMessage(dst, src)</code> |
| `dependency-sources/raft-v3.7.0/log.go:127` | follower-logappend: follower retains entry pointers | <code>l.append(a.entries[ci-offset:]...)</code> |
| `dependency-sources/raft-v3.7.0/log_unstable.go:205` | unstable-append: append []*Entry pointers, not Data bytes | <code>u.entries = append(u.entries, ents...)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:656` | lib-peer-construction-alias: zero payload copies when constructing append Message | <code>Entries: ents,</code> |
| `dependency-sources/raft-v3.7.0/raft.go:817` | lib-raft-clone: counted one leader Entry.Data deep clone per proposed entry containing full value | <code>cloned[i] = proto.Clone(es[i]).(*pb.Entry)</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1349` | lib-raft-propose-clone-call: leader successful proposal invokes appendEntry clone | <code>if !r.appendEntry(m.GetEntries()...) {</code> |
| `dependency-sources/raft-v3.7.0/raft.go:1800` | follower-maybeappend: follower uses maybeAppend, not leader appendEntry deep clone | <code>if mlastIndex, ok := r.raftLog.maybeAppend(a, m.GetCommit()); ok {</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.pb.go:348` | lib-entry-data-field: Entry.Data bytes descriptor | <code>Data          []byte                 `protobuf:&quot;bytes,4,opt,name=Data&quot; json:&quot;Data,omitempty&quot;`</code> |
| `dependency-sources/raft-v3.7.0/raftpb/raft.proto:1` | lib-entry-proto2: Entry.Data uses proto2 bytes, not proto3 Put decoder | <code>syntax = &quot;proto2&quot;;</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:94` | lib-raft-propose-alias: RawNode boundary aliases data; later appendEntry clones it | <code>Entries: []*pb.Entry{{Data: data}},</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:228` | async-append-alias: local Raft async append message retains entry pointers | <code>Entries: rd.Entries,</code> |
| `dependency-sources/raft-v3.7.0/rawnode.go:393` | async-apply-alias: local Raft async apply message retains entry pointers | <code>Entries: ents,</code> |
| `internal/core/codec.go:37` | entry-buffer-init: entry capacity fully reserved in BOTH; no fixed extra growth relocation assigned | <code>buf := binary.AppendUvarint(make([]byte, 0, size+binary.MaxVarintLen64), uint64(len(ps)))</code> |
| `internal/core/codec.go:68` | entry-value-encode: counted one full new Put value append to entry BOTH targets | <code>buf = append(buf, p.req.Put.Wire...)</code> |
| `internal/core/codec.go:103` | wire-batch-slice: capped entry subslice passed to apply, zero owning bytes copy | <code>if err := fn(origin, boot, id, kind, data[:size:size]); err != nil {</code> |
| `internal/core/node.go:145` | pending-owner: request descriptor retention, not copied value bytes or channel | <code>req      Request</code> |
| `internal/core/node.go:373` | batch-pending-allocation: one pending-record backing array per SubmitAll BOTH, not startup arena or channel | <code>ps := make([]pending, len(rs))</code> |
| `internal/core/node.go:375` | submitall-loop: direct per-record method call into batch-owned pending array; zero sends/receives/closes | <code>n.submit(rs[i], &amp;ps[i])</code> |
| `internal/core/node.go:386` | wire-check: validate fields/size without generic protobuf copy | <code>if err := checkPut(&amp;r.Put); err != nil {</code> |
| `internal/core/node.go:515` | wire-size: full Put wire length plus batch framing; metadata sizing, no value copy | <code>size := len(p.req.Put.Wire) + batchFraming</code> |
| `internal/core/node.go:580` | core-encode-call: entry encode caller, not an additional copy | <code>data, err := encodeBatch(n.cfg.ID, n.cfg.Boot, props, size)</code> |
| `internal/core/node.go:585` | raft-propose: leader proposal -> dependency clone: counted separately below | <code>if err := n.rn.Propose(data); err != nil {</code> |
| `internal/core/node.go:853` | apply-dispatch: ordinary Put walks encoded entry and takes in-place ParsePut branch BOTH targets | <code>err := walkBatch(e.GetData(), func(origin, boot, id uint64, kind byte, b []byte) error {</code> |
| `internal/core/node.go:858` | apply-wire-kind: Put bypasses generic apply protobuf decode on every replica | <code>if kind == kindPut {</code> |
| `internal/core/node.go:894` | apply-wire-parse: parse entry in place: zero value copies | <code>put, err := ParsePut(b)</code> |
| `internal/core/node.go:898` | apply-wire-state: state fields forwarded: zero value copies | <code>prev, err := n.state.ApplyPut(&amp;put.PutOp)</code> |
| `internal/core/node.go:919` | apply-wire-reply: header/metadata reply, not a value copy | <code>n.out.Replies = append(n.out.Replies, Reply{ID: p.req.ID, Rev: n.kv.Rev(), Term: n.term, PrevKv: prev})</code> |
| `internal/core/state.go:110` | wire-state-index: zero copy: direct wire Put fields path into MVCC | <code>prev, err := st.kv.PutKV(op)</code> |
| `internal/core/storage.go:39` | storage-pointer-append: MemoryStorage appends entry pointers, not Entry.Data bytes | <code>if err := s.Append(job.Entries); err != nil {</code> |
| `internal/core/watch.go:272` | no-watch-fastpath: no-watch match returns without value encoding | <code>if len(changes) == 0 &#124;&#124; len(n.watchers) == 0 {</code> |
| `internal/core/wire.go:61` | wire-parse-consume: dependency returns subslice: zero copy | <code>v, n := protowire.ConsumeBytes(b)</code> |
| `internal/core/wire.go:67` | wire-parse-clip: slice capacity clip, no byte copy | <code>v = slices.Clip(v)</code> |
| `internal/core/wire.go:71` | wire-value-alias: value slice aliases Wire/Entry.Data | <code>p.Value = v</code> |
| `internal/mvcc/ops.go:191` | putop-alias: PutRequest-to-PutOp copies slice headers, not bytes | <code>return PutOp{Key: p.Key, Value: p.Value, Lease: p.Lease, PrevKv: p.PrevKv, IgnoreValue: p.IgnoreValue, IgnoreLease: p.IgnoreLease}</code> |
| `internal/mvcc/ops.go:250` | index-version-append: metadata/slice headers only: zero payload copies | <code>k.vers = append(k.vers, v)</code> |
| `internal/mvcc/ops.go:259` | index-value: Put retains Raft entry value slice header in MVCC; zero full-value copy BOTH targets | <code>v := version{create: w.begin + 1, ver: 1, lease: p.Lease, value: p.Value}</code> |
| `internal/mvcc/ops.go:277` | index-key: key metadata excluded from full-value stage census | <code>k = &amp;keyIndex{key: p.Key}</code> |
| `internal/node/node.go:136` | admission-send: counted: one send per actual admission batch, all targets | <code>case n.inbox &lt;- in:</code> |
| `internal/node/node.go:173` | driver-batch-owner: driver batch owns/shared request descriptors; header-only, not channel | <code>reqs  []core.Request</code> |
| `internal/node/node.go:194` | batch-api: SubmitBatch admission route for Put BOTH, not retained Node.Do | <code>func (n *Node) SubmitBatch(ctx context.Context, reqs []core.Request, tags []any, to Completer) error {</code> |
| `internal/node/node.go:203` | batch-wrapper: batch wrapper retains descriptor/tag backing stores; no payload copy or channel exchange | <code>b := &amp;batch{first: n.nextID.Add(k) - k + 1, reqs: reqs, tags: tags, to: to}</code> |
| `internal/node/node.go:204` | batch-inbox-call: one inbox admission handoff per batch via actual send site; this call is not an extra exchange | <code>return n.put(ctx, input{batch: b})</code> |
| `internal/node/node.go:270` | peer-inbox-send: counted send: 1/delivered inbound peer message | <code>case n.inbox &lt;- input{msg: m}:</code> |
| `internal/node/node.go:278` | unreachable-send: conditional U: successful nonblocking inbox report; saturation/drop/fault, not fixed per request | <code>case n.inbox &lt;- input{unreachable: id}:</code> |
| `internal/node/node.go:285` | do-reply-create: excluded retained Node.Do; zero fresh private reply channels for ordinary Put BOTH | <code>r := &amp;request{id: n.nextID.Add(1), op: op, reply: make(chan core.Reply, 1)}</code> |
| `internal/node/node.go:290` | do-reply-receive: excluded retained Node.Do; not ordinary Put route | <code>case rep := &lt;-r.reply:</code> |
| `internal/node/node.go:312` | shutdown-close: close: shutdown-only, zero per normal Put | <code>close(n.done)</code> |
| `internal/node/node.go:339` | inbox-receive-outer: counted selected receive: outer alternative for admission, inbound peer or successful Unreachable report | <code>case in := &lt;-inbox:</code> |
| `internal/node/node.go:344` | inbox-receive-drain: counted selected receive: drain alternative for another input; not extra receipt of outer input | <code>case in := &lt;-n.inbox:</code> |
| `internal/node/node.go:350` | disk-complete-receive: counted receive: 1/sync completion group | <code>case done := &lt;-n.persisted:</code> |
| `internal/node/node.go:357` | disk-send: counted send: 1/persist queue array | <code>case toDisk &lt;- next:</code> |
| `internal/node/node.go:361` | tick-receive: counted selected background receive, not fixed per Put | <code>case &lt;-ticker.C:</code> |
| `internal/node/node.go:398` | watch-close: close: watch overflow, outside no-watch scope | <code>close(ws.done)</code> |
| `internal/node/node.go:424` | do-reply-send: excluded retained Node.Do; not ordinary Put route | <code>w.reply &lt;- r</code> |
| `internal/node/node.go:436` | completion-direct: direct grouped call: zero channel ops | <code>g.to.Complete(g.cs)</code> |
| `internal/node/node.go:450` | batch-waiter: per-request completer/tag metadata, no private reply channel | <code>waiters[r.ID] = waiter{to: b.to, tag: b.tags[i]}</code> |
| `internal/node/node.go:452` | batch-core-submit: SubmitAll direct method, zero channel operations BOTH targets | <code>n.core.SubmitAll(b.reqs)</code> |
| `internal/node/node.go:453` | taken-direct: direct call once after handling an admission batch, not itself a channel operation | <code>b.to.Taken()</code> |
| `internal/node/node.go:491` | disk-receive-outer: counted receive: initial array; alternative to drain | <code>case jobs = &lt;-n.persistQ:</code> |
| `internal/node/node.go:498` | disk-receive-drain: counted receive: drained array; NOT an extra receive for initial array | <code>case js := &lt;-n.persistQ:</code> |
| `internal/node/node.go:505` | wal-append-call: driver disk trace; no extra payload copy beyond WAL record | <code>n.wal.Append(j.HardState, j.Entries)</code> |
| `internal/node/node.go:508` | wal-sync-call: driver disk trace; I/O, not counted as extra userspace payload materialization | <code>if err := n.wal.Sync(); err != nil {</code> |
| `internal/node/node.go:512` | disk-complete-send: counted send: 1/sync completion group | <code>case n.persisted &lt;- done:</code> |
| `internal/peer/peer.go:248` | peer-value-decode: counted copy: 1/received occurrence of Entry.Data, on receiver; dependency decoder below | <code>if err := proto.Unmarshal(buf, m); err != nil {</code> |
| `internal/peer/peer.go:304` | peer-queue-send: counted send: 1/queued outbound peer message | <code>case q &lt;- m:</code> |
| `internal/peer/peer.go:315` | peer-queue-receive-fast: counted receive alternative 1: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:321` | peer-queue-receive-control: counted receive alternative 2: one receipt per message | <code>case m := &lt;-s.ctrl:</code> |
| `internal/peer/peer.go:324` | peer-queue-receive-data: counted receive alternative 3: one receipt per message | <code>case m := &lt;-s.data:</code> |
| `internal/peer/peer.go:426` | peer-value-encode: counted copy: 1/serialized occurrence of Entry.Data containing this Put, on sender | <code>buf, err = proto.MarshalOptions{}.MarshalAppend(buf[:0], m)</code> |
| `internal/server/batch.go:39` | opbatch-owner: connection batch owns descriptors; slice headers, not payload bytes or channels | <code>reqs  []core.Request</code> |
| `internal/server/batch.go:59` | wire-frontend-parse: wire frontend parse: zero value copies | <code>put, err := core.ParsePut(st.body)</code> |
| `internal/server/batch.go:103` | peek-header: buffered header peek, not full input-value materialization | <code>h, _ := c.br.Peek(frameHeaderLen)</code> |
| `internal/server/batch.go:108` | next-buffered: whole-frame availability predicate, no owned value-buffer allocation | <code>return c.br.Buffered() &gt;= frameHeaderLen+n</code> |
| `internal/server/batch.go:126` | charge-request: accounting metadata only; no payload copy | <code>c.chargeLocked(&amp;st.reqHeld, st.reqSize+callCost)</code> |
| `internal/server/batch.go:136` | batch-submit-call: one admission API call per actual batch | <code>err := c.s.n.SubmitBatch(c.ctx, b.reqs, b.tags, c)</code> |
| `internal/server/batch.go:138` | clock-set: reader flag after successful SubmitBatch; metadata only | <code>c.untaken = true</code> |
| `internal/server/batch.go:215` | clock-send: K_s: successful conditional/coalesced Taken send; full-buffer default contributes zero | <code>case c.taken &lt;- struct{}{}:</code> |
| `internal/server/batch.go:227` | clock-gate: skip receive when no untaken flag or next full frame buffered | <code>if !c.untaken &#124;&#124; c.nextFrameBuffered() {</code> |
| `internal/server/batch.go:231` | clock-receive: K_r: selected reader clock receipt, not fixed one/request or one/batch | <code>case &lt;-c.taken:</code> |
| `internal/server/batch.go:234` | clock-node-stop: conditional node lifecycle receive, excluded stable normal Put scope | <code>case &lt;-c.s.n.Done():</code> |
| `internal/server/batch.go:255` | completion-cas: completion publish: atomic compare-and-swap, no Go channel and no completion mutex | <code>if c.completions.CompareAndSwap(n.next, n) {</code> |
| `internal/server/batch.go:259` | completion-wake-attempt: wake attempt per completion group, not guaranteed successful send | <code>c.signal()</code> |
| `internal/server/batch.go:268` | completion-swap: writer collects completion list atomically; zero channel ops | <code>for n := c.completions.Swap(nil); n != nil; n = n.next {</code> |
| `internal/server/batch.go:308` | wire-reply-encode: one small buffer per ordinary Put metadata reply; no input value when PrevKv disabled | <code>msg, err := c.appendPutReply(make([]byte, 0, 5+putResponseSize), &amp;cp.Reply)</code> |
| `internal/server/batch.go:320` | release-request: release accounting, zero channel exchange / payload copy | <code>c.releaseReqLocked(f.st)</code> |
| `internal/server/batch.go:326` | charge-reply: accounts encoded reply until sent; no payload copy | <code>c.chargeLocked(&amp;f.st.replyHeld, len(f.msg))</code> |
| `internal/server/batch.go:361` | manual-response-prefix: gRPC small reply prefix, zero input-value copy | <code>b = append(b, 0, 0, 0, 0, 0)</code> |
| `internal/server/batch.go:362` | manual-response-append: wire small response, no value bytes when PrevKv nil | <code>b, err := appendPutResponse(b, c.s.n.ClusterID(), c.s.n.ID(), r.Rev, r.Term, r.PrevKv)</code> |
| `internal/server/h2.go:43` | io-buffer-size: existing transport buffer size, no per-request copy count implied | <code>ioBuffer     = 64 &lt;&lt; 10</code> |
| `internal/server/h2.go:125` | taken-channel-create: one capacity-one Taken channel per connection, NOT fresh per-request reply channel | <code>taken:    make(chan struct{}, 1),</code> |
| `internal/server/h2.go:126` | existing-read-buffer: existing 64KiB bufio reader BOTH; not a new PR84 owning-request stage | <code>br:       bufio.NewReaderSize(nc, ioBuffer),</code> |
| `internal/server/h2.go:206` | writer-wake-send: counted successful conditional/coalesced wake send; default is not operation | <code>case c.wake &lt;- struct{}{}:</code> |
| `internal/server/h2.go:219` | control-wake-attempt: control wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:268` | await-before-read: clock gate before existing frame reads, method call not another handoff | <code>if err := c.awaitTaken(); err != nil {</code> |
| `internal/server/h2.go:271` | frame-header-read: existing frame-header read call; not a runtime syscall count | <code>fh, err := c.fr.ReadFrameHeader()</code> |
| `internal/server/h2.go:275` | frame-read: existing Framer payload read; transport work excluded, no new clock copy destination | <code>f, err := c.fr.ReadFrameForHeader(fh)</code> |
| `internal/server/h2.go:347` | settings-wake-attempt: settings wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:362` | window-wake-attempt: send-window wake attempt; shares writer-wake channel, not separate counted site | <code>c.signal()</code> |
| `internal/server/h2.go:400` | credit-wake-attempt: wake attempt per DATA frame, not a guaranteed successful send | <code>c.signal()</code> |
| `internal/server/h2.go:430` | hpack-decode: per-connection HPACK decoder, header bytes are not Put payload | <code>if _, err := c.hdec.Write(b); err != nil {</code> |
| `internal/server/h2.go:481` | hpack-field: indexed header fields retained without fresh MetaHeaders field list; literal decoding may allocate | <code>bit, h.path = 2, f.Value</code> |
| `internal/server/h2.go:582` | stream-context-gate: ordinary admitted unary Put does not create private per-stream context | <code>if !grpc &#124;&#124; (st.rpc != nil &amp;&amp; st.rpc.op == nil) {</code> |
| `internal/server/h2.go:586` | stream-channel-guard: streaming-only conditional allocation; unary Put leave inSignal and space nil | <code>st.inSignal, st.space = make(chan struct{}, 1), make(chan struct{}, 1)</code> |
| `internal/server/h2.go:598` | timeout-guard: no timed lean grpc-timeout: per-stream deadline timer not armed | <code>if grpc &amp;&amp; h.hasTimeout {</code> |
| `internal/server/h2.go:601` | stream-deadline: optional grpc-timeout AfterFunc absent for timed lean Put; control Range deadlines outside cohort | <code>st.deadline = time.AfterFunc(h.timeout, st.expire)</code> |
| `internal/server/h2.go:639` | receive-data-caller: Framer DATA dispatched to owned message accumulation | <code>if err := st.receive(f.Data()); err != nil {</code> |
| `internal/server/h2.go:658` | unary-nil-signal: nil unary channel: zero successful sends; default wins | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/h2.go:666` | stream-admit: ordinary Put is synchronous reader admission, no per-Put handler goroutine | <code>c.admit(st)</code> |
| `internal/server/server.go:189` | server-stop-close: close: server Stop wait channel, not per Put | <code>close(done)</code> |
| `internal/server/stream.go:124` | frontend-prealloc: full request-sized capacity prevents accumulation growth; not a separate copy | <code>st.msg = make([]byte, 0, size)</code> |
| `internal/server/stream.go:127` | frontend-accumulation: counted one owned full Put value accumulation from reusable Framer DATA payload BOTH | <code>st.msg = append(st.msg, d[:n]...)</code> |
| `internal/server/stream.go:139` | unary-delivery: retains owned body header and returns before streaming-signal select | <code>st.body = m</code> |
| `internal/server/stream.go:148` | unary-nil-body-signal: streaming-only select NOT executed by unary delivery: zero sends | <code>case st.inSignal &lt;- struct{}{}:</code> |
| `internal/server/stream.go:280` | out-capacity: per-stream output array still allocated, room for ordinary unary reply | <code>st.out = make([]outItem, 0, 3)</code> |
| `internal/server/write.go:52` | writer-wake-receive: counted selected wake receive only while idle | <code>case &lt;-c.wake:</code> |
| `internal/server/write.go:54` | node-done-receive: shutdown-only receive; obtaining Done() itself is not operation | <code>case &lt;-nodeDone:</code> |
| `internal/server/write.go:130` | unary-nil-space: nil unary channel: zero successful sends; default wins | <code>case st.space &lt;- struct{}{}:</code> |
| `internal/server/write.go:191` | stream-end-release: release accounting/flow control; no room channel exists at these targets | <code>c.releaseLocked(&amp;st.replyHeld)</code> |
| `internal/wal/wal.go:351` | wal-value-copy: counted copy: 1 direct Entry.Data append per persisted entry containing Put | <code>w.buf = append(w.buf, p...)</code> |
| `internal/wal/wal.go:368` | wal-value-caller: WAL Entry.Data caller, not a second copy | <code>w.record(recEntry, h[:], e.GetData())</code> |
| `internal/wal/wal.go:386` | wal-write: file I/O/physical copies not enumerated in payload subtotal | <code>n, err := w.seg.Write(w.buf)</code> |
| `internal/wal/wal.go:416` | wal-sync: durability I/O, not byte materialization counted here | <code>if err := w.seg.Sync(); err != nil {</code> |

