#!/usr/bin/env python3
"""Relocate exact reviewed sites in selected immutable files, then assert text."""
from pathlib import Path
import json, difflib, hashlib, html, datetime, subprocess
ROOT=Path('/home/joshazimullah.linux/work_mounts/scratch/bench80-lean-source-audit')
REPO=Path('/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75')
OLD=REPO/'evidence/bench75/run-38387706444/source-audit'
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
binding=json.loads((ROOT/'binding.json').read_text());targets=binding['targets']
rows=[]; previous=json.loads((OLD/'site-specs.json').read_text())
fix={
 'lib-proto3-decode-copy':'counted owning CAS frontend decode BOTH targets; additionally main apply; Put bypasses both; PR80 apply aliases',
 'frontend-decode':'counted owning CAS frontend decode BOTH targets via unmarshal helper; ordinary Put bypasses',
 'batch-core-submit':'SubmitAll direct call BOTH targets; no channel operation or payload copy',
 'generic-entry-value-encode':'counted one new-value entry encode for main CAS; PR80 CAS uses Wire append instead',
 'index-value':'zero payload copy: retains entry bytes for Put BOTH and PR80 CAS; main CAS retains owning apply-decoded bytes',
 'frontend-new-op':'fresh CAS protobuf request graph BOTH; PR80 additionally retains original body in Request.Wire',
 'request-wire-field':'PR80 original-body alias; comment says UnmarshalAlias but actual frontend uses owning proto.Unmarshal',
 'response-proto-encode':'CAS failure: one full returned stored-value reply encode; Put/CAS success: no value in reply',
 'batch-reply-encode':'CAS response encode caller, NOT another copy in addition to response-proto-encode',
 'entry-value-encode':'counted one full input-value Wire append for ordinary Put BOTH',
 'txn-entry-copy':'counted one full original-Txn-body append for PR80 CAS, either compare result',
 'alias-apply':'PR80 CAS apply aliases nested bytes even in unexecuted Success Put; still builds fresh request graph',
 'generic-apply-still-exists':'main CAS owning apply decode before branch selection; ordinary Put bypasses',
 'apply-decode':'main CAS owning apply decode before branch selection; ordinary Put bypasses',
 'wire-reply-encode':'one small buffer PER ordinary Put reply; no full input value (PrevKv disabled)',
 'wire-size':'request entry sizing, no payload copy; PR80 CAS instead sizes retained Request.Wire',
 'stream-deadline':'AfterFunc only when grpc-timeout supplied; absent on timed lean Put/CAS (control grpc Range differs)',
 'lib-bytes-encode-copy':'counted underlying entry/peer/reply bytes instruction; caller and implementation are ONE stage',
 'lib-proto2-clone-copy':'counted proto2 Entry.Data leader deep clone, not proto3 owning decoder',
 'lib-raft-clone':'counted one leader Entry.Data deep clone per proposed entry containing full value',
 'lib-returned-value-field':'stored reply Value is proto3 bytes, separate payload identity from proposed new input',
 'lib-put-value-field':'new Put.Value is proto3 bytes, nested in generic CAS request',
 'lib-proto2-decode-copy':'counted one receiver owning Entry.Data decode per transmitted occurrence',
 'lib-proto3-codec-selection':'proto3 Put.Value and returned KeyValue.Value bytes coder selection',
 'entry-buffer-init':'entry capacity fully reserved in BOTH; no fixed extra growth relocation assigned',
 'opbatch-owner':'connection batch owns descriptors; slice headers, not payload bytes or channels',
 'batch-pending-allocation':'one pending-record backing array per SubmitAll BOTH, not startup arena or channel',
 'index-key':'key metadata excluded from full-value stage census',
 'batch-api':'SubmitBatch admission route for Put/CAS BOTH, not retained Node.Do',
 'unreachable-send':'conditional U: successful nonblocking inbox report; saturation/drop/fault, not fixed per request',
 'do-reply-create':'excluded retained Node.Do; zero fresh private reply channels for ordinary Put/CAS BOTH',
 'do-reply-send':'excluded retained Node.Do; not ordinary Put/CAS route',
 'do-reply-receive':'excluded retained Node.Do; not ordinary Put/CAS route',
}
for t,oldlabel in [('main','hpack'),('pr80','txn')]:
 for x in previous:
  if x['target']!=oldlabel:continue
  x=dict(x);f=x['file'];p=ROOT/f if f.startswith('dependency-sources/') else ROOT/'sources'/t/f
  ls=p.read_text().splitlines();expected=x['expected'];oldline=x['line'];line=None
  if x['id']=='alias-frontend':
   x['id']='owning-frontend-caller';expected='if err := unmarshal(st.body, req.Op); err != nil {'
   matches=[i+1 for i,s in enumerate(ls) if s.strip()==expected];assert len(matches)==1;line=matches[0]
   x['classification']='PR80 frontend RESET: generic CAS now owning unmarshal -> proto.Unmarshal; retained Wire body still encoded as-is'
  elif f.startswith('dependency-sources/'):
   assert hashlib.sha256(p.read_bytes()).hexdigest()==x['file_sha256'];line=oldline
  else:
   oldls=(OLD/'sources'/oldlabel/f).read_text().splitlines()
   for b in difflib.SequenceMatcher(None,oldls,ls,autojunk=False).get_matching_blocks():
    if b.a<=oldline-1<b.a+b.size:line=b.b+oldline-1-b.a+1;break
   assert line is not None,(t,x['id'],'site changed: manual review required')
  assert ls[line-1].strip()==expected,(t,x['id'],line)
  desc=fix.get(x['id'],x['classification'])
  desc=desc.replace('main/wire/hpack','main').replace('all four targets','both targets').replace('ALL targets','BOTH targets').replace('txn CAS','PR80 CAS').replace('txn alias','PR80 alias')
  x.update(target=t,line=line,expected=expected,classification=desc,sha=targets[t],file_sha256=hashlib.sha256(p.read_bytes()).hexdigest())
  rows.append(x)
# Explicit frontend caller and header timeout guard on both targets.
def add(t,f,id,text,desc):
 p=ROOT/'sources'/t/f;ls=p.read_text().splitlines();idx=[i+1 for i,l in enumerate(ls) if l.strip()==text];assert len(idx)==1,(t,f,id,idx)
 rows.append(dict(target=t,file=f,id=id,line=idx[0],expected=text,classification=desc,sha=targets[t],file_sha256=hashlib.sha256(p.read_bytes()).hexdigest()))
for t in targets:
 if t=='main':add(t,'internal/server/batch.go','owning-frontend-caller','if err := unmarshal(st.body, req.Op); err != nil {','main CAS owning frontend caller; ordinary Put bypasses')
 add(t,'internal/server/h2.go','timeout-guard','if grpc && h.hasTimeout {','no timed lean grpc-timeout: per-stream deadline timer not armed')
 add(t,'internal/server/h2.go','stream-channel-guard','st.inSignal, st.space = make(chan struct{}, 1), make(chan struct{}, 1)','streaming-only conditional allocation; unary Put/CAS leave inSignal and space nil')
 if t=='pr80':add(t,'internal/core/node.go','txn-wire-size','size = len(p.req.Wire) + batchFraming','CAS reserves entire original body in entry capacity, no input-copy amortization')
rows.sort(key=lambda r:(r['target'],r['file'],r['line'],r['id']))
assert len({(r['target'],r['id']) for r in rows})==len(rows)
(ROOT/'verified-sites.json').write_text(json.dumps(rows,indent=2)+'\n')
out=['# Verified sites — '+binding['status'],'','Exact trimmed line text asserted against selected immutable Git source snapshots. Dependency files are unchanged prior-verified bytes; pins independently checked. A codec caller and its implementing byte instruction are one stage.','']
for t,sha in targets.items():
 out += [f'## {t} — `{sha}`','| File:line | Site / classification | Exact text |','|---|---|---|']
 for r in rows:
  if r['target']==t:out.append(f"| `{r['file']}:{r['line']}` | {r['id']}: {r['classification']} | <code>{html.escape(r['expected']).replace('|','&#124;')}</code> |")
 out.append('')
(ROOT/'verified-sites.md').write_text('\n'.join(out)+'\n')
# Every retained repo snapshot is compared byte-for-byte anew with Git show.
for r in json.loads((ROOT/'source-file-hashes.json').read_text()):
 p=ROOT/r['path'];assert hashlib.sha256(p.read_bytes()).hexdigest()==r['sha256']
 if 'commit' in r:
  f='/'.join(Path(r['path']).parts[2:]);assert p.read_bytes()==subprocess.check_output(['git','show',r['commit']+':'+f],cwd=REPO)
(ROOT/'verification.json').write_text(json.dumps({'status':binding['status'],'targets':targets,'verified_sites':len(rows),'method':'Selected Git show byte equality, exact site assertions, unchanged dependency source hashes and go.mod/go.sum equality; no Go execution','verified_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n')
print('Verified',len(rows),'exact sites at',binding['status'])
