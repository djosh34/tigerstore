#!/usr/bin/env python3
from pathlib import Path
import json,difflib,hashlib,html,datetime,subprocess
ROOT=Path('/home/joshazimullah.linux/work_mounts/scratch/bench84-lean-source-audit')
REPO=Path('/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75')
OLD=Path('/home/joshazimullah.linux/work_mounts/scratch/bench80-lean-source-audit')
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
b=json.loads((ROOT/'binding.json').read_text());targets=b['targets'];prior=json.loads((ROOT/'prior-put-specs.json').read_text());rows=[]
fix={'wire-size':'full Put wire length plus batch framing; metadata sizing, no value copy','lib-put-value-field':'Put.Value is nonempty proto3 bytes; frontend/apply Put path bypasses owning decoder','index-value':'Put retains Raft entry value slice header in MVCC; zero full-value copy BOTH targets','entry-value-encode':'counted one full new Put value append to entry BOTH targets','apply-dispatch':'ordinary Put walks encoded entry and takes in-place ParsePut branch BOTH targets','lib-bytes-encode-copy':'peer encoding implementing full Entry.Data append; caller and instruction ONE stage; ordinary Put metadata reply has no input value','batch-core-submit':'SubmitAll direct method, zero channel operations BOTH targets','wire-reply-encode':'one small buffer per ordinary Put metadata reply; no input value when PrevKv disabled','stream-deadline':'optional grpc-timeout AfterFunc absent for timed lean Put; control Range deadlines outside cohort','stream-context-gate':'ordinary admitted unary Put does not create private per-stream context','pending-owner':'request descriptor retention, not copied value bytes or channel','frontend-accumulation':'counted one owned full Put value accumulation from reusable Framer DATA payload BOTH','wire-batch-slice':'capped entry subslice passed to apply, zero owning bytes copy'}
for t,sha in targets.items():
 for x in prior:
  r=dict(x);f=r['file'];p=ROOT/f if f.startswith('dependency-sources/') else ROOT/'sources'/t/f;ls=p.read_text().splitlines();line=None
  if f.startswith('dependency-sources/'):
   assert hashlib.sha256(p.read_bytes()).hexdigest()==r['file_sha256'];line=r['line']
  else:
   prev=(OLD/'sources/main'/f).read_text().splitlines()
   for block in difflib.SequenceMatcher(None,prev,ls,autojunk=False).get_matching_blocks():
    if block.a<=r['line']-1<block.a+block.size:line=block.b+r['line']-1-block.a+1;break
  assert line is not None and ls[line-1].strip()==r['expected'],(t,r['id'],'changed counted/support site requires review')
  desc=fix.get(r['id'],r['classification']);desc=desc.replace('PR80','PR84').replace('all four targets','both targets').replace('ALL targets','BOTH targets').replace('Put/CAS','Put').replace('Put or CAS','Put').replace('Put and CAS','Put').replace('or CAS','').replace('publish: CAS,','publish: atomic compare-and-swap,')
  assert 'CAS' not in desc,(r['id'],desc)
  r.update(target=t,sha=sha,line=line,classification=desc,file_sha256=hashlib.sha256(p.read_bytes()).hexdigest());rows.append(r)
def add(t,f,id,text,desc):
 p=ROOT/'sources'/t/f;ls=p.read_text().splitlines();idx=[i+1 for i,l in enumerate(ls) if l.strip()==text];assert len(idx)==1,(t,id,idx)
 rows.append({'target':t,'sha':targets[t],'file':f,'id':id,'line':idx[0],'expected':text,'classification':desc,'file_sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
for t in targets:
 add(t,'internal/server/h2.go','existing-read-buffer','br:       bufio.NewReaderSize(nc, ioBuffer),','existing 64KiB bufio reader BOTH; not a new PR84 owning-request stage')
 add(t,'internal/server/h2.go','frame-header-read','fh, err := c.fr.ReadFrameHeader()','existing frame-header read call; not a runtime syscall count')
 add(t,'internal/server/h2.go','frame-read','f, err := c.fr.ReadFrameForHeader(fh)','existing Framer payload read; transport work excluded, no new clock copy destination')
 add(t,'internal/server/h2.go','io-buffer-size','ioBuffer     = 64 << 10','existing transport buffer size, no per-request copy count implied')
 add(t,'internal/server/batch.go','peek-header','h, _ := c.br.Peek(frameHeaderLen)','buffered header peek, not full input-value materialization')
for f,id,text,desc in [
 ('internal/server/h2.go','taken-channel-create','taken:    make(chan struct{}, 1),','one capacity-one Taken channel per connection, NOT fresh per-request reply channel'),
 ('internal/server/h2.go','await-before-read','if err := c.awaitTaken(); err != nil {','clock gate before existing frame reads, method call not another handoff'),
 ('internal/node/node.go','taken-direct','b.to.Taken()','direct call once after handling an admission batch, not itself a channel operation'),
 ('internal/server/batch.go','clock-send','case c.taken <- struct{}{}:','K_s: successful conditional/coalesced Taken send; full-buffer default contributes zero'),
 ('internal/server/batch.go','clock-receive','case <-c.taken:','K_r: selected reader clock receipt, not fixed one/request or one/batch'),
 ('internal/server/batch.go','clock-gate','if !c.untaken || c.nextFrameBuffered() {','skip receive when no untaken flag or next full frame buffered'),
 ('internal/server/batch.go','clock-set','c.untaken = true','reader flag after successful SubmitBatch; metadata only'),
 ('internal/server/batch.go','next-buffered','return c.br.Buffered() >= frameHeaderLen+n','whole-frame availability predicate, no owned value-buffer allocation'),
 ('internal/server/batch.go','clock-node-stop','case <-c.s.n.Done():','conditional node lifecycle receive, excluded stable normal Put scope'),
 ]:add('pr84',f,id,text,desc)
rows.sort(key=lambda r:(r['target'],r['file'],r['line'],r['id']))
assert len(rows)==len({(r['target'],r['id']) for r in rows})
(ROOT/'verified-sites.json').write_text(json.dumps(rows,indent=2)+'\n')
out=['# Exact verified Put/channel sites — '+b['status'],'','Every source line asserted; immutable commit and file hashes attached. Dependencies match unchanged prior-verified bytes/pins. A codec caller and instruction count once.','']
for t,sha in targets.items():
 out += [f'## {t} — `{sha}`','| File:line | Site/classification | Exact text |','|---|---|---|']
 for r in rows:
  if r['target']==t:out.append(f"| `{r['file']}:{r['line']}` | {r['id']}: {r['classification']} | <code>{html.escape(r['expected']).replace('|','&#124;')}</code> |")
 out.append('')
(ROOT/'verified-sites.md').write_text('\n'.join(out)+'\n')
for r in json.loads((ROOT/'source-file-hashes.json').read_text()):
 p=ROOT/r['path'];assert hashlib.sha256(p.read_bytes()).hexdigest()==r['sha256']
 if 'commit' in r:assert p.read_bytes()==subprocess.check_output(['git','show',r['commit']+':'+r['repo_path']],cwd=REPO)
(ROOT/'verification.json').write_text(json.dumps({'status':b['status'],'targets':targets,'verified_sites':len(rows),'source_only':True,'method':'Selected Git show byte equality, exact relocated reviewed-site assertions, unchanged dependency hashes and pins','verified_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n')
print('Verified',len(rows),'exact server/dependency Put and channel sites;',b['status'])
