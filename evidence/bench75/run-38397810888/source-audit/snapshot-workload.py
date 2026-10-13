#!/usr/bin/env python3
"""Snapshot only read workload files; mutable observations are explicitly unbound."""
from pathlib import Path
import subprocess,hashlib,json,datetime,html,re
ROOT=Path('/home/joshazimullah.linux/work_mounts/scratch/bench80-lean-source-audit')
REPO=Path('/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75')
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
origin='d2408301966b99471bfed06d4c2dc1cd1e5af462'
base=subprocess.check_output(['git','show',origin+':experiments/grpc/lean.go'],cwd=REPO)
assert hashlib.sha256(base).hexdigest()=='9553545389d7ae3d246af1ecf03acc26b7a3f167a7f33883c046054c6ed1f07e'
p=ROOT/'workload/base/experiments/grpc/lean.go';p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(base)
rows=[{'path':str(p.relative_to(ROOT)),'sha256':hashlib.sha256(base).hexdigest(),'commit':origin,'repo_path':'experiments/grpc/lean.go'}]
bp=ROOT/'generator-binding.json'
if bp.exists():
 binding=json.loads(bp.read_text());assert binding['parent_confirmed'] is True and binding['status']=='FINAL'
 sha=binding['commit'];engine=binding['engine_path']
 paths=['experiments/bench75/load/main.go','experiments/bench75/load/cas.go',engine,'.github/workflows/bench75.yml','experiments/bench75/run.py','go.mod','go.sum']
 data={f:subprocess.check_output(['git','show',sha+':'+f],cwd=REPO) for f in paths}
 def version_pins(content):
  return dict(re.findall(r'^\s*(\S+)\s+(v\S+)\s*(?://.*)?$',content.decode(),re.MULTILINE))
 assert version_pins(data['go.mod'])==version_pins((ROOT/'sources/main/go.mod').read_bytes()),'generator module version pins changed: re-audit required'
 assert data['go.sum']==(ROOT/'sources/main/go.sum').read_bytes(),'generator checksum pins changed: re-audit required'
 # x/net v0.58.0 moves direct -> indirect only; version and checksum identity unchanged.
 status='FINAL immutable generator source, parent-confirmed'
else:
 paths=['experiments/bench75/load/main.go','experiments/bench75/load/cas.go','.github/workflows/bench75.yml','experiments/bench75/run.py']
 engines=[f for f in ['experiments/bench75/load/lean.go','experiments/bench75/load/newlean.go'] if (REPO/f).exists()]
 assert len(engines)==1,engines
 engine=engines[0];paths.append(engine)
 data={f:(REPO/f).read_bytes() for f in paths}
 assert all(b==(REPO/f).read_bytes() for f,b in data.items()),'helper changed a file during snapshot; rerun later'
 sha=None;status='UNBOUND WORKING-TREE OBSERVATION; independent helper still editing; NOT final run source'
for f,b in data.items():
 p=ROOT/'workload/observed'/f;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
 rows.append({'path':str(p.relative_to(ROOT)),'sha256':hashlib.sha256(b).hexdigest(),'commit':sha,'repo_path':f,'status':status})
(ROOT/'workload-manifest.json').write_text(json.dumps({'status':status,'observed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'engine_path':engine,'files':rows},indent=2)+'\n')
specs=[]
def record(f,id,needle,description):
 lines=data[f].decode().splitlines();idx=[i+1 for i,l in enumerate(lines) if needle in l];assert len(idx)==1,(f,id,idx)
 specs.append({'id':id,'file':f,'line':idx[0],'expected':lines[idx[0]-1].strip(),'description':description,'sha256':hashlib.sha256(data[f]).hexdigest(),'commit':sha,'status':status})
main='experiments/bench75/load/main.go';cas='experiments/bench75/load/cas.go'
for f,id,needle,description in [
 (main,'tls-config','cfg := &tls.Config','client certificate + roots; TLS1.3 min/max'),
 (main,'workers','workers := flag.Int','256 total closed-loop slots by default'),
 (main,'connections','conns := flag.Int','16 active raw connections by default'),
 (main,'control-count','controlConns = 1','one grpc control connection when lean enabled'),
 (main,'keyspace','const keyspace = 4096','4096 keys'),
 (main,'keys','/registry/pods/ns-%03d/pod-%08d','Kubernetes-shaped key naming'),
 (main,'values','s := sha256.Sum256','32 independent SHA256 chunks per 1KiB constant per-key value'),
 (main,'cache-seed','revisions, e = seedRevisions','untimed CAS cache seed before pool construction'),
 (main,'raw-pool','lean, e = newLeanPool','pool construction warms raw connections before before probe'),
 (main,'before-revision','before, e := probe()','revision fence after startup writes'),
 (main,'before-cpu','pre[i], e = sample(p)','CPU/io sampling after warm and before start'),
 (main,'start','close(start)','timed phase start after snapshots'),
 (cas,'compare','Compare: []*pb.Compare','positive cached MOD EQUAL comparison'),
 (cas,'success-put','Success: []*pb.RequestOp','one same-key Put on success'),
 (cas,'failure-range','Failure: []*pb.RequestOp','one same-key value-returning point Range on failure'),
 (cas,'seed-range','KeysOnly: true','untimed keys-only prefix cache seeding'),
 (cas,'validate-range','!bytes.Equal(q.Kvs[0].Value, value)','fallback validates shape, keys, exact values and positive revision'),
 (cas,'revision-monotonic','slot.CompareAndSwap(old, revision)','shared cache only advances monotonically'),
 (engine,'templates','func leanTemplates(','prebuilt Put or CAS compare/branches templates'),
 (engine,'cas-revision','n := binary.PutUvarint','minimal dynamic positive ModRevision varint'),
 (engine,'cas-length','dst = binary.AppendUvarint','dynamic Compare byte length'),
 (engine,'global-claim','p.next.CompareAndSwap(i, i+1)','bounded global operation claim, no retries'),
 (engine,'active-connections','for _, slots := range leanSlotCounts(workers, conns)','active raw connection population from min(workers,conns) slot counts'),
 (engine,'connection-slots','counts[i] = workers / len(counts)','total concurrency evenly spread across raw connections'),
 (engine,'warm-rpc','warmKey: i}','one untimed raw RPC per connection'),
 (engine,'fields','l.fields = []hpack.HeaderField','complete timed field list; no grpc-timeout'),
 (engine,'first-headers','l.firstHeaders = bytes.Clone','prebuilt first HPACK block'),
 (engine,'warm-headers','l.headers = bytes.Clone','prebuilt warmed HPACK block'),
 (engine,'permutation','2654435761','global multiplicative permutation'),
 (engine,'cached-revision','l.templates[k].casWire','uses shared positive cached revision without timed pre-read'),
 (engine,'data-framing','l.f.WriteData(id,','actual DATA can fragment according to flow credit/maxFrame; no unconditional single-frame assertion'),
 (engine,'receive-credit-size','const leanReceiveCredit','64KiB response credit threshold'),
 (engine,'connection-credit','if l.received >= leanReceiveCredit','coalesced connection receive credit, includes padding'),
 (engine,'stream-credit','if p.received >= leanReceiveCredit && !ev.end','coalesced stream receive credit for sufficiently large open replies'),
 (engine,'deadline-renewal','if l.writeDeadline.Sub(now) > leanTimeout/2','renew 10s write deadline only when remaining time <=5s'),
 (engine,'response-cache','storeRevision(&l.revisions[p.key], revision)','valid response monotonically refreshes shared cache'),
 ('experiments/bench75/run.py','five-reps','for rep in range(1,6):','five paired repetitions'),
 ('.github/workflows/bench75.yml','lean-env','GENERATOR: lean','Actions timed generator choice'),
 ('.github/workflows/bench75.yml','matrix','include: ${{ fromJSON','Put256 and CAS256 requested default suite'),
]:record(f,id,needle,description)
field=next(x for x in specs if x['id']=='fields');assert 'grpc-timeout' not in field['expected']
(ROOT/'workload-sites.json').write_text(json.dumps(specs,indent=2)+'\n')
out=['# Workload observed sites — '+status,'','| File:line | Observation | Exact text |','|---|---|---|']
for s in specs:out.append(f"| `{s['file']}:{s['line']}` | {s['id']}: {s['description']} | <code>{html.escape(s['expected']).replace('|','&#124;')}</code> |")
(ROOT/'workload-sites.md').write_text('\n'.join(out)+'\n')
print(status,engine,';',len(specs),'workload source sites asserted')
