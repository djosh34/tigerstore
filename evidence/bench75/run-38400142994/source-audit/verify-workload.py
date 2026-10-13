#!/usr/bin/env python3
from pathlib import Path
import json,subprocess,hashlib,html,datetime,re
ROOT=Path('/home/joshazimullah.linux/work_mounts/scratch/bench84-lean-source-audit');REPO=Path('/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75')
OLD=Path('/home/joshazimullah.linux/work_mounts/scratch/bench80-lean-source-audit')
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
b=json.loads((ROOT/'generator-binding.json').read_text());assert b['status']=='FINAL' and b['parent_confirmed'];sha=b['commit'];engine=b['engine_path']
paths=['experiments/bench75/load/main.go',engine,'experiments/bench75/load/io.go','experiments/bench75/run.py','.github/workflows/bench75.yml','go.mod','go.sum'];data={};manifest=[]
for f in paths:
 x=subprocess.check_output(['git','show',sha+':'+f],cwd=REPO);data[f]=x;p=ROOT/'workload'/f;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(x)
 manifest.append({'file':f,'path':str(p.relative_to(ROOT)),'commit':sha,'sha256':hashlib.sha256(x).hexdigest()})
oldengine=subprocess.check_output(['git','show',b['previous_engine_commit']+':'+engine],cwd=REPO)
assert data[engine]==oldengine,'engine changed: re-read/re-audit required'
pins=lambda x:dict(re.findall(r'^\s*(\S+)\s+(v\S+)\s*(?://.*)?$',x.decode(),re.MULTILINE))
assert pins(data['go.mod'])==pins((ROOT/'sources/main/go.mod').read_bytes())
assert data['go.sum']==(ROOT/'sources/main/go.sum').read_bytes()
rows=[]
def add(f,id,needle,desc):
 ls=data[f].decode().splitlines();idx=[i+1 for i,l in enumerate(ls) if needle in l];assert len(idx)==1,(id,idx)
 rows.append({'file':f,'id':id,'line':idx[0],'expected':ls[idx[0]-1].strip(),'classification':desc,'commit':sha,'file_sha256':hashlib.sha256(data[f]).hexdigest()})
main='experiments/bench75/load/main.go'
for f,id,needle,desc in [
 (main,'tls','cfg := &tls.Config','TLS1.3 mTLS certificate/roots'),(main,'control','controlConns = 1','one untimed grpc control connection'),(main,'keyspace','const keyspace = 4096','4096 existing keyspace'),(main,'keys','/registry/pods/ns-%03d/pod-%08d','original Kube-shaped keys'),(main,'values','s := sha256.Sum256','independent SHA256-derived constant 1KiB value per key'),(main,'pool','lean, e = newLeanPool','untimed raw warm RPCs before accounting'),(main,'before','before, e := probe()','revision probe after warm writes'),(main,'pre-cpu','pre[i], e = sample(p)','CPU/IO sampling before phase start'),(main,'start','close(start)','release measured phase'),(main,'post-cpu','post[i], e = sample(p)','CPU/IO sampling after measured phase'),(main,'proc-stat','os.ReadFile(fmt.Sprintf("/proc/%d/stat"','read cumulative process CPU counters; not inferred server syscalls'),(main,'proc-io','os.ReadFile(fmt.Sprintf("/proc/%d/io"','read cumulative syscr/syscw counters'),(main,'cpu-per-op','cpus[i] = secs * 1e6','CPU microseconds per completed op arithmetic'),(main,'syscr-per-op','result["leader_syscr_per_op"]','actual leader syscr delta/completed calls'),(main,'syscw-per-op','result["leader_syscw_per_op"]','actual leader syscw delta/completed calls'),
 (engine,'slot-count','counts[i] = workers / len(counts)','64/256 total slots split across 16 active raw connections'),(engine,'active','for _, slots := range leanSlotCounts','one raw connection per slot-count entry'),(engine,'warm','warmKey: i}','one untimed warm RPC per raw connection, outside N'),(engine,'global-claim','p.next.CompareAndSwap(i, i+1)','global bounded operation claim, no retries'),(engine,'permutation','2654435761','original global permutation'),(engine,'put-template','r[k].put = grpcWire(p)','prebuilt ordinary Put message'),(engine,'fields','l.fields = []hpack.HeaderField','full timed metadata list omits grpc-timeout'),(engine,'hpack-first','l.firstHeaders = bytes.Clone','first HPACK template'),(engine,'hpack-warm','l.headers = bytes.Clone','warm HPACK template'),(engine,'data','l.f.WriteData(id,','DATA may fragment for actual flow credit/maxFrame'),(engine,'credit-size','const leanReceiveCredit','64KiB coalesced receive credit'),(engine,'conn-credit','if l.received >= leanReceiveCredit','coalesced connection WINDOW_UPDATE'),(engine,'stream-credit','if p.received >= leanReceiveCredit && !ev.end','coalesced open-stream receive credit'),(engine,'write-deadline','if l.writeDeadline.Sub(now) > leanTimeout/2','cached 10s deadline renew only <=5s remaining, no grpc-timeout header'),('experiments/bench75/run.py','five-reps','for rep in range(1,6):','five reps per selected workload and store'),('.github/workflows/bench75.yml','matrix','include: ${{ fromJSON','put-64-256 suite selects two Put jobs'),('.github/workflows/bench75.yml','lean-env','GENERATOR: lean','raw lean generator selected')
]:add(f,id,needle,desc)
assert 'grpc-timeout' not in next(x for x in rows if x['id']=='fields')['expected']
(ROOT/'workload-manifest.json').write_text(json.dumps({'status':'FINAL','commit':sha,'engine_byte_identical_to':b['previous_engine_commit'],'engine_sha256':hashlib.sha256(data[engine]).hexdigest(),'pins':'same module versions and go.sum as Tiger main; x/net direct/indirect status only','files':manifest},indent=2)+'\n')
(ROOT/'workload-sites.json').write_text(json.dumps(rows,indent=2)+'\n')
out=['# FINAL workload / accounting source sites',f'Commit `{sha}`; engine bytes identical to `{b["previous_engine_commit"]}`. No runtime results inferred.','| File:line | Site/classification | Exact text |','|---|---|---|']
for r in rows:out.append(f"| `{r['file']}:{r['line']}` | {r['id']}: {r['classification']} | <code>{html.escape(r['expected']).replace('|','&#124;')}</code> |")
(ROOT/'workload-sites.md').write_text('\n'.join(out)+'\n');print('Verified',len(rows),'final generator/harness source sites; engine unchanged')
