"""Read-only source-citation verification; no Go or application/test execution."""
from pathlib import Path
import json

BASE = Path(__file__).resolve().parent
REFS = json.loads((BASE / 'refs.json').read_text())
rows = []

def site(label, path, line, expected, role):
    root = {'M': 'main', 'P75': 'pr75-latest', 'P76': 'pr76', 'LIB': 'library-evidence'}[label]
    text = (BASE / root / path).read_text().splitlines()[line - 1]
    if expected not in text:
        raise ValueError(f'{label} {path}:{line}: expected {expected!r}, found {text!r}')
    sha = {'M': REFS['main'], 'P75': REFS['pr75-latest'], 'P76': REFS['pr76']}.get(label)
    rows.append(dict(target=label, commit=sha, path=path, line=line, role=role, source=text.strip()))

for label in ('M', 'P75'):
    p = 'internal/node/node.go'
    for n, text, role in [
        (122, 'n.inbox <- in', 'fixed admission send'),
        (264, 'in := <-inbox', 'admission/peer inbox receive alternative 1'),
        (269, 'in := <-n.inbox', 'admission/peer inbox receive alternative 2'),
        (301, 'ch <- r', 'fixed reply send'), (217, 'rep := <-r.reply', 'fixed reply receive'),
        (212, 'make(chan core.Reply, 1)', 'per-request reply channel creation, not operation'),
        (282, 'toDisk <- next', 'disk queue send'),
        (369, 'jobs = <-n.persistQ', 'disk queue receive initial'),
        (376, 'js := <-n.persistQ', 'disk queue receive drained'),
        (390, 'n.persisted <- done', 'disk completion send'),
        (275, 'done := <-n.persisted', 'disk completion receive'),
        (286, '<-ticker.C', 'background ticker receive'),
        (197, 'n.inbox <- input{msg: m}', 'peer inbound inbox send'),
        (205, 'n.inbox <- input{unreachable: id}', 'conditional unreachable send'),
        (239, 'close(n.done)', 'shutdown close, excluded'),
    ]:
        site(label, p, n, text, role)
for n, text, role in [
    (123, 'n.inbox <- in', 'batch admission send'),
    (311, 'in := <-inbox', 'admission/peer inbox receive alternative 1'),
    (316, 'in := <-n.inbox', 'admission/peer inbox receive alternative 2'),
    (388, 'w.reply <- r', 'retained Do reply send, not batch Put'),
    (262, 'rep := <-r.reply', 'retained Do reply receive, not batch Put'),
    (257, 'make(chan core.Reply, 1)', 'retained Do creation, not batch Put'),
    (176, 'func (n *Node) SubmitBatch', 'batch API, no operation itself'),
    (400, 'g.to.Complete(g.cs)', 'grouped direct call, no channel operation'),
    (329, 'toDisk <- next', 'disk queue send'),
    (453, 'jobs = <-n.persistQ', 'disk queue receive initial'),
    (460, 'js := <-n.persistQ', 'disk queue receive drained'),
    (474, 'n.persisted <- done', 'disk completion send'),
    (322, 'done := <-n.persisted', 'disk completion receive'),
    (333, '<-ticker.C', 'background ticker receive'),
    (242, 'n.inbox <- input{msg: m}', 'peer inbound inbox send'),
    (250, 'n.inbox <- input{unreachable: id}', 'conditional unreachable send'),
    (284, 'close(n.done)', 'shutdown close, excluded'),
]:
    site('P76', 'internal/node/node.go', n, text, role)
for label in ('M', 'P75', 'P76'):
    p = 'internal/peer/peer.go'
    for n, text, role in [
        (294, 'q <- m', 'peer queue send'),
        (305, 'm := <-s.ctrl', 'peer queue receive alternative 1'),
        (311, 'm := <-s.ctrl', 'peer queue receive alternative 2'),
        (314, 'm := <-s.data', 'peer queue receive alternative 3'),
        (328, 'm := <-s.ctrl', 'failure-only drop receive alternative 1'),
        (330, 'm := <-s.data', 'failure-only drop receive alternative 2'),
        (347, '<-time.After(backoff)', 'failure-only backoff receive'),
        (348, '<-ctx.Done()', 'shutdown receive'),
        (317, '<-ctx.Done()', 'shutdown receive'),
        (416, 'MarshalAppend', 'one direct Entry.Data encode per occurrence'),
        (238, 'proto.Unmarshal', 'one direct Entry.Data decode per received occurrence'),
    ]:
        site(label, p, n, text, role)
    for p, n, text, role in [
        ('internal/core/codec.go', 64, 'MarshalAppend', 'one direct value encode'),
        ('internal/core/codec.go', 114, 'proto.Unmarshal', 'one direct apply value decode'),
        ('internal/core/codec.go', 96, 'op = &pb.PutRequest{}', 'fresh apply request'),
        ('internal/core/node.go', 522, 'n.rn.Propose(data)', 'Raft proposal boundary'),
        ('internal/wal/wal.go', 351, 'append(w.buf, p...)', 'one direct WAL payload copy'),
        ('internal/wal/wal.go', 368, 'e.GetData()', 'WAL entry payload caller'),
        ('internal/mvcc/ops.go', 235, 'value: p.Value', 'zero payload copy, retained header'),
        ('internal/mvcc/ops.go', 227, 'append(k.vers, v)', 'zero payload copy, metadata/header append'),
        ('internal/core/storage.go', 39, 's.Append(job.Entries)', 'pointer-retaining MemoryStorage boundary'),
    ]:
        site(label, p, n, text, role)
for label, wake, recv, end, space, accumulation, unmarshal, response in [
    ('P75', 175, 50, 444, 112, 100, 59, 179),
    ('P76', 188, 58, 467, 124, 103, 62, 182),
]:
    site(label, 'internal/server/h2.go', wake, 'c.wake <- struct{}{}', 'conditional/coalesced writer wake send')
    site(label, 'internal/server/write.go', recv, '<-c.wake', 'conditional writer wake receive')
    site(label, 'internal/server/h2.go', end, 'st.inSignal <-', 'nil-channel attempt: zero for unary Put')
    site(label, 'internal/server/write.go', space, 'st.space <-', 'nil-channel attempt: zero for unary Put')
    site(label, 'internal/server/stream.go', accumulation, 'append(st.msg, d[:n]...)', 'one full-value-equivalent accumulation')
    site(label, 'internal/server/rpc.go', unmarshal, 'proto.Unmarshal', 'one direct frontend value decode')
    site(label, 'internal/server/stream.go', response, 'MarshalAppend', 'response encode: zero value copies without PrevKv')
    site(label, 'internal/server/server.go', 189, 'close(done)', 'server Stop close, excluded')
for p, n, text, role in [
    ('internal/server/batch.go', 83, 'c.s.n.SubmitBatch', 'batch admission call'),
    ('internal/server/batch.go', 129, 'func (c *conn) Complete', 'grouped completion API'),
    ('internal/server/batch.go', 122, 'c.room <- struct{}{}', 'conditional/coalesced room send'),
    ('internal/server/batch.go', 101, '<-c.room', 'backpressure-only room receive'),
    ('internal/server/batch.go', 135, 'c.signal()', 'one signal attempt per completion group'),
    ('internal/server/batch.go', 153, 'marshal(cp.Resp)', 'group response encode, no 1 KiB payload'),
    ('internal/server/write.go', 37, 'nodeDone := c.s.n.Done()', 'channel acquisition, NOT an operation'),
    ('internal/server/write.go', 60, '<-nodeDone', 'shutdown-only receive, excluded'),
]:
    site('P76', p, n, text, role)
site('P75', 'internal/server/stream.go', 255, 'c.signal()', 'one signal attempt per queued reply')
for label, ns in [('P75', [164,186,279,294,316]), ('P76', [177,199,297,312,334])]:
    for n in ns:
        site(label, 'internal/server/h2.go', n, 'c.signal()', 'conditional control/lifecycle signal call')
for n, text, role in [(159, 'func (s *kvServer) Put', 'main Put handler'), (160, 'return do[*pb.PutResponse]', 'main Put dispatch'), (129, 'n.Do(ctx, op)', 'main Node.Do call')]:
    site('M', 'internal/server/server.go', n, text, role)
site('M', 'internal/server/http.go', 29, 's.g.ServeHTTP', 'main HTTP2 grpc routing')
for p, n, text, role in [
    ('etcd-api-v3.7.2/rpc_grpc.pb.go', 248, 'dec(in)', 'main client-wire decoder call'),
    ('etcd-api-v3.7.2/rpc.pb.go', 760, 'Value []byte', 'proto3 PutRequest value field'),
    ('grpc-v1.84.0/encoding/proto/proto.go', 96, 'proto.Unmarshal', 'main client-wire protobuf decode'),
    ('grpc-v1.84.0/mem/buffer_slice.go', 115, 's.CopyTo(*buf)', 'conditional multi-buffer library copy'),
    ('protobuf-v1.36.11/internal/impl/codec_gen.go', 5497, 'append(([]byte)(nil), v...)', 'proto3 Put Value full decode copy'),
    ('protobuf-v1.36.11/internal/impl/codec_gen.go', 5418, 'append(emptyBuf[:], v...)', 'proto2 Entry.Data full decode copy'),
    ('protobuf-v1.36.11/encoding/protowire/wire.go', 455, 'v...)', 'full bytes encode copy'),
    ('raft-v3.7.0/raft.go', 817, 'proto.Clone(es[i])', 'leader full Entry deep clone'),
    ('raft-v3.7.0/raft.go', 1349, 'r.appendEntry(m.GetEntries()...)', 'leader proposal invokes clone path'),
    ('raft-v3.7.0/raftpb/raft.proto', 1, 'syntax = "proto2"', 'Entry.Data is proto2, not Put proto3'),
    ('raft-v3.7.0/raftpb/raft.pb.go', 348, 'Data', 'Entry.Data bytes field'),
    ('protobuf-v1.36.11/internal/impl/merge.go', 188, 'append(emptyBuf[:], *src.Bytes()...)', 'proto2 Entry.Data clone full copy'),
    ('raft-v3.7.0/storage.go', 318, 'entries...)', 'pointer append; zero full-payload copies'),
    ('raft-v3.7.0/storage.go', 320, 'entries...)', 'pointer append; zero full-payload copies'),
]:
    site('LIB', p, n, text, role)

(BASE / 'verified-sites.json').write_text(json.dumps(rows, indent=2) + '\n')
md = ['# Verified census sites', '', 'Generated by `python3 verify-sites.py`; exact source line arrays and expected text were checked. This is source inspection, not an application test.', '', '| Target | File:line | Classification | Exact source line |', '|---|---|---|---|']
for r in rows:
    text = r['source'].replace('|', '\\|')
    md.append(f"| {r['target']} | `{r['path']}:{r['line']}` | {r['role']} | `{text}` |")
(BASE / 'verified-sites.md').write_text('\n'.join(md) + '\n')
print(f'Verified {len(rows)} exact census source sites; wrote verified-sites.json and verified-sites.md')
