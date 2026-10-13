#!/usr/bin/env python3
from pathlib import Path
import json,datetime,subprocess,hashlib,tarfile,io,html
ROOT=Path('/home/joshazimullah.linux/work_mounts/scratch/bench7980-source-audit')
REPO=Path('/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75')
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
targets=json.loads((ROOT/('final-targets.json' if (ROOT/'final-targets.json').exists() else 'preliminary-targets.json')).read_text())
rows=json.loads((ROOT/'relocated-sites.json').read_text())
# Do is not the ordinary Put/CAS route on ANY target in this audit.
fix={
'admission-send':'counted: one send per actual admission batch, all targets',
'inbox-receive-outer':'counted selected receive: outer alternative for admission, inbound peer or successful Unreachable report',
'inbox-receive-drain':'counted selected receive: drain alternative for another input; not extra receipt of outer input',
'do-reply-send':'excluded retained Node.Do: not ordinary Put or CAS',
'do-reply-receive':'excluded retained Node.Do: not ordinary Put or CAS',
'do-reply-create':'excluded retained Node.Do channel creation: zero per ordinary Put or CAS',
'unreachable-send':'conditional counted U send when nonblocking Unreachable report succeeds; saturation/drop/fault, not fixed per request',
'frontend-decode':'owning frontend decode: counted main/wire/hpack CAS only; ordinary Put bypasses on ALL targets; txn CAS bypasses',
'entry-value-encode':'counted: one copy of each full input value into encoded entry, NOT amortized payload bytes',
'apply-decode':'counted main owning apply decode: includes unexecuted success-branch Put.Value on CAS failure',
'index-value':'zero payload copies: entry backing store for ordinary Put ALL targets and txn CAS; decoded bytes for main/wire/hpack CAS',
'lib-proto3-decode-copy':'one owning proto3 bytes decode stage: main/wire/hpack CAS frontend/apply; ordinary Put bypasses ALL targets; not txn CAS',
'lib-grpc-decode':'excluded grpc server codec: current main uses custom frontend, not this adapter',
'lib-grpc-materialize':'excluded grpc server transport: not current main frontend',
'lib-grpc-copyto':'excluded grpc server transport: not current main frontend',
'lib-put-decoder':'excluded generated grpc server adapter: not current main frontend',
'batch-reply-encode':'Put replies contain no input payload; CAS failure reply contains stored value and copies it once via response codec',
'generic-apply-still-exists':'main/wire/hpack CAS owning apply decode; bypassed for ordinary Put',
'lib-proto2-clone-copy':'counted: Raft leader Entry.Data deep clone, proto2 (not Put proto3 decoder)',
'entry-buffer-init':'entry capacity: main/wire/hpack/txn fully size-reserved; no extra fixed relocation invented',
'lib-bytes-encode-copy':'counted encoding instruction: main entry or peer Entry.Data or CAS failure reply, each boundary counted only once',
'wire-batch-slice':'zero copy: capped command subslice passed to apply',
'wire-state-index':'zero copy: direct wire Put fields path into MVCC',
}
for r in rows:
 if r['id'] in fix:r['classification']=fix[r['id']]
 if r['id']=='batch-core-submit':r['classification']='descriptor handoff, zero payload copy; main submits individually, wire likewise'
 if r['id']=='response-proto-encode':r['classification']='CAS failure: one full stored-value copy into reply; ordinary Put/CAS success: zero value copy'
 if r['id']=='lib-proto3-codec-selection':r['classification']='proto3 Put.Value and KeyValue.Value non-presence bytes codec selection'
 if r['id']=='wire-reply-encode':r['classification']='wire Put metadata response: zero ordinary input-value copy'
# Drop grpc-only evidence from the census, and test-helper decoder (retain where explicitly relevant).
rows=[r for r in rows if r['id'] not in {'lib-grpc-decode','lib-grpc-materialize','lib-grpc-copyto','lib-put-decoder'}]

def add(target,file,id,expected,classification,scope=None):
 path=ROOT/file if file.startswith('dependency-sources/') else ROOT/'sources'/target/file
 ls=path.read_text().splitlines(); start=0;end=len(ls)
 if scope:
  starts=[i for i,l in enumerate(ls) if scope in l];assert len(starts)==1,(file,scope,starts)
  start=starts[0];end=next((i for i in range(start+1,len(ls)) if ls[i].startswith('func ')),len(ls))
 matches=[i for i in range(start,end) if ls[i].strip()==expected];assert len(matches)==1,(target,file,id,matches)
 rows[:]=[r for r in rows if not(r['target']==target and r['id']==id)]
 rows.append(dict(target=target,file=file,id=id,line=matches[0]+1,expected=expected,classification=classification))
for t in targets:
 add(t,'internal/server/batch.go','opbatch-owner','reqs  []core.Request','connection admission batch owns request descriptors; header-only, not channel',scope=None)
 add(t,'internal/node/node.go','driver-batch-owner','reqs  []core.Request','driver batch owns/shared request descriptors; header-only, not channel',scope=None)
 add(t,'internal/node/node.go','batch-wrapper','b := &batch{first: n.nextID.Add(k) - k + 1, reqs: reqs, tags: tags, to: to}','batch wrapper retains descriptor/tag backing stores; no payload copy or channel exchange')
 add(t,'internal/node/node.go','batch-inbox-call','return n.put(ctx, input{batch: b})','one inbox admission handoff per batch via actual send site; this call is not an extra exchange')
 add(t,'internal/node/node.go','batch-waiter','waiters[r.ID] = waiter{to: b.to, tag: b.tags[i]}','per-request completer/tag metadata, no private reply channel')
 add(t,'internal/core/node.go','pending-owner','req      Request','pending record retains request slice headers; zero value copy')
 add(t,'internal/server/stream.go','unary-delivery','st.body = m','retains owned body header and returns before streaming-signal select')
 add(t,'internal/server/batch.go','release-request','c.releaseReqLocked(f.st)','release accounting, zero channel exchange / payload copy')
 add(t,'internal/server/batch.go','frontend-new-op', 'req.Op, req.Wire = st.rpc.op(), st.body' if t=='txn' else 'req.Op = st.rpc.op()','request structs still constructed for CAS; ordinary Put ParsePut bypasses ALL targets')
 add(t,'internal/core/codec.go','generic-entry-value-encode','buf, err = proto.MarshalOptions{Deterministic: true, UseCachedSize: true}.MarshalAppend(buf, p.req.Op)','one entire CAS input-value copy for main/wire/hpack; txn Wire bypasses generic encoder')
 add(t,'internal/server/stream.go','unary-nil-body-signal','case st.inSignal <- struct{}{}:','streaming-only select NOT executed by unary delivery: zero sends')
 add(t,'internal/mvcc/store.go','kv-value-alias','Value:          v.value,','zero stored-value copy: response/compare KeyValue retains value slice header')
 add(t,'internal/mvcc/ops.go','range-kv','kvs = append(kvs, k.kv(v))','CAS failure Range: KeyValue objects and slice headers only; zero value copy')
 add(t,'internal/mvcc/ops.go','compare-kv','if !compareKV(c, k.kv(v)) {','existing-key compare constructs KeyValue with aliased stored value; zero value copy')
 add(t,'internal/mvcc/ops.go','compare-mod','result = cmpInt(kv.ModRevision, c.GetModRevision())','CAS compare uses integer mod revision, not value bytes')
 add(t,'internal/mvcc/ops.go','compare-path','path := w.compareToPath(r)','compare selects branch at replicated apply')
 add(t,'internal/mvcc/ops.go','txn-range','rr, err := w.rangeKeys(t.RequestRange)','compare failure: executes current Range only; return existing stored value')
 add(t,'internal/mvcc/ops.go','txn-success','Succeeded: path[0],','Succeeded reports compare result; failure is still successful RPC')
 add(t,'internal/mvcc/ops.go','txn-revision','resp.Header = header(w.cur())','CAS success increments revision; compare failure keeps current MVCC revision',scope='func (s *Store) Txn(')
 add(t,'internal/mvcc/ops.go','revision-changed','if w.changed {','MVCC revision changes only for actual state mutation',scope='func (w *writer) finish()')
 add(t,'internal/mvcc/validate.go','txn-readonly','for _, ops := range [][]*pb.RequestOp{r.Success, r.Failure} {','both branches classified before compare; a Put in Success forces write/Raft route',scope='func IsTxnReadOnly(')
 add(t,'internal/mvcc/validate.go','txn-write-detect','if op.GetRequestRange() == nil {','success Put means not read-only even when compare will fail',scope='func IsTxnReadOnly(')
 add(t,'internal/core/node.go','txn-write-route','if mvcc.IsTxnReadOnly(op) {','only wholly read-only transactions take read route')
 add(t,'internal/core/state.go','state-txn','r, err := st.kv.Txn(op)','replicated CAS apply: MVCC chooses branch')
 if True:
  add(t,'internal/mvcc/ops.go','putop-alias','return PutOp{Key: p.Key, Value: p.Value, Lease: p.Lease, PrevKv: p.PrevKv, IgnoreValue: p.IgnoreValue, IgnoreLease: p.IgnoreLease}','PutRequest-to-PutOp copies slice headers, not bytes')
  add(t,'internal/mvcc/ops.go','txn-put','out.Response = &pb.ResponseOp_ResponsePut{ResponsePut: &pb.PutResponse{Header: header(w.begin + 1), PrevKv: w.put(&op)}}','success Put executes; failure skips this branch')
  add(t,'internal/core/codec.go','new-txn-op','return &pb.TxnRequest{}, nil','apply still creates fresh TxnRequest struct, including txn alias path')
 if t in ('hpack','txn'):
  add(t,'internal/node/node.go','batch-core-submit','n.core.SubmitAll(b.reqs)','direct batch submission, zero payload copy')
  add(t,'internal/core/node.go','batch-pending-allocation','ps := make([]pending, len(rs))','one pending-record backing array per SubmitAll; not startup arena')
  add(t,'internal/core/node.go','submitall-loop','n.submit(rs[i], &ps[i])','direct per-record method call into batch-owned pending array; zero sends/receives/closes')
  add(t,'internal/server/batch.go','wire-reply-encode','msg, err := c.appendPutReply(make([]byte, 0, 5+putResponseSize), &cp.Reply)','one buffer PER Put reply at verified target, PR body shared-buffer claim is stale')
  add(t,'internal/server/batch.go','manual-response-prefix','b = append(b, 0, 0, 0, 0, 0)','gRPC small reply prefix, zero input-value copy')
  add(t,'internal/server/h2.go','stream-deadline','st.deadline = time.AfterFunc(h.timeout, st.expire)','optional deadline callback timer, not Timer.C exchange')
  add(t,'internal/server/h2.go','hpack-decode','if _, err := c.hdec.Write(b); err != nil {','per-connection HPACK decoder, header bytes are not Put/CAS payload')
  add(t,'internal/server/h2.go','hpack-field','bit, h.path = 2, f.Value','indexed header fields retained without fresh MetaHeaders field list; literal decoding may allocate')
  add(t,'internal/server/stream.go','out-capacity','st.out = make([]outItem, 0, 3)','per-stream output array still allocated, room for ordinary unary reply')
 if t=='txn':
  add(t,'internal/core/node.go','request-wire-field','Wire []byte','PR80 Request.Wire is an alias slice header, not channel or owning byte copy')
  add(t,'internal/server/batch.go','alias-frontend','if err := core.UnmarshalAlias(st.body, req.Op); err != nil {','CAS frontend alias decode: zero owning input bytes decode; request graph remains')
  add(t,'internal/core/codec.go','txn-entry-copy','buf = append(buf, p.req.Wire...)','one input full-value materialization into Raft entry; both CAS outcomes')
  add(t,'internal/core/node.go','alias-apply','if err := UnmarshalAlias(b, op); err != nil {','CAS apply aliases entry bytes even for unexecuted success branch; fresh request structs remain')
  add(t,'internal/core/alias.go','alias-nested-list','sub = m.Mutable(fd).List().AppendMutable().Message()','rebuilds repeated Compare/RequestOp messages via reflection')
  add(t,'internal/core/alias.go','alias-nested-message','sub = m.Mutable(fd).Message()','rebuilds singular/oneof nested request messages')
  add(t,'internal/core/alias.go','alias-set','m.Set(fd, x)','reflection bytes field setter retains slice, dependency proof verifies no clone')
  add(t,'internal/core/alias.go','alias-value','return protoreflect.ValueOfBytes(slices.Clip(x)), n, nil','bytes fields are capped aliases, not owning byte buffers')
  add(t,'internal/core/alias.go','alias-unknown','m.SetUnknown(append(append(m.GetUnknown(), tag...), b[:n]...))','conditional unknown/mistyped field copy, excluded canonical scoped request')
# Dependency additions: each is pinned to all four targets and independently ZIP verified.
for t in targets:
 for id,file,line,desc,scope in [
  ('reflect-set','internal/impl/message_reflect_gen.go','fi.set(m.pointer(), v)','reflection Set dispatch, no byte clone','func (m *messageState) Set('),
  ('reflect-scalar-set','internal/impl/message_reflect_field.go','rv.Set(conv.GoValueOf(v))','Put.Value scalar setter copies slice header, not value bytes','func fieldInfoForScalar('),
  ('reflect-bytes-convert','internal/impl/convert.go','return reflect.ValueOf(v.Bytes()).Convert(c.goType)','[]byte-to-[]byte conversion retains backing store; no append/copy',None),
 ]:add(t,'dependency-sources/protobuf-v1.36.11/'+file,id,line,desc,scope)
 add(t,'dependency-sources/etcd-api-v3.7.2/mvccpb/kv.pb.go','lib-returned-value-field','Value []byte `protobuf:"bytes,5,opt,name=value,proto3" json:"value,omitempty"`','returned stored value is proto3 bytes: same owning encoder behavior')
rows.sort(key=lambda r:(list(targets).index(r['target']),r['file'],r['line'],r['id']))
# Verify every source file (not just cited snippets) against immutable Git archive bytes.
files=[]
for t,sha in targets.items():
 data=subprocess.check_output(['git','archive',sha],cwd=ROOT/'object-repo')
 with tarfile.open(fileobj=io.BytesIO(data)) as ar:
  for m in ar.getmembers():
   if m.isfile():
    b=ar.extractfile(m).read();p=ROOT/'sources'/t/m.name;assert p.read_bytes()==b,(t,m.name)
    files.append(dict(target=t,sha=sha,file=m.name,sha256=hashlib.sha256(b).hexdigest()))
for r in rows:
 p=ROOT/r['file'] if r['file'].startswith('dependency-sources/') else ROOT/'sources'/r['target']/r['file']
 assert p.read_text().splitlines()[r['line']-1].strip()==r['expected'],r
 r['sha']=targets[r['target']];r['file_sha256']=hashlib.sha256(p.read_bytes()).hexdigest()
(ROOT/'site-specs.json').write_text(json.dumps(rows,indent=2)+'\n');(ROOT/'verified-sites.json').write_text(json.dumps(rows,indent=2)+'\n');(ROOT/'source-file-hashes.json').write_text(json.dumps(files,indent=2)+'\n')
out=['# Exact verified sites','Every line asserted; every repository source file compared to git archive of its SHA. Dependency evidence matches module ZIP bytes and go.sum h1. A call and its dependency instruction are ONE materialization.','']
for t,sha in targets.items():
 out += [f'## {t} — `{sha}`','| File:line | Site / classification | Exact text (trimmed) |','|---|---|---|']
 for r in rows:
  if r['target']==t:out.append(f"| `{r['file']}:{r['line']}` | {r['id']}: {r['classification']} | <code>{html.escape(r['expected']).replace('|','&#124;')}</code> |")
 out.append('')
(ROOT/'verified-sites.md').write_text('\n'.join(out)+'\n')
print('Verified',len(rows),'exact sites;',len(files),'repository files match immutable commits')
