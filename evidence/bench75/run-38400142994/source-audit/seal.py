#!/usr/bin/env python3
from pathlib import Path
import json,hashlib,datetime,subprocess,re
ROOT=Path('/home/joshazimullah.linux/work_mounts/scratch/bench84-lean-source-audit');REPO=Path('/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75')
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
b=json.loads((ROOT/'binding.json').read_text());g=json.loads((ROOT/'generator-binding.json').read_text());assert b['status']==g['status']=='FINAL'
for r in json.loads((ROOT/'source-file-hashes.json').read_text()):
 p=ROOT/r['path'];assert hashlib.sha256(p.read_bytes()).hexdigest()==r['sha256']
 if 'commit' in r:assert p.read_bytes()==subprocess.check_output(['git','show',r['commit']+':'+r['repo_path']],cwd=REPO)
for r in json.loads((ROOT/'workload-manifest.json').read_text())['files']:
 p=ROOT/r['path'];assert hashlib.sha256(p.read_bytes()).hexdigest()==r['sha256'];assert p.read_bytes()==subprocess.check_output(['git','show',r['commit']+':'+r['file']],cwd=REPO)
for r in json.loads((ROOT/'reference-manifest.json').read_text())['references']:assert hashlib.sha256(Path(r['path']).read_bytes()).hexdigest()==r['sha256']
ss=json.loads((ROOT/'verified-sites.json').read_text());ww=json.loads((ROOT/'workload-sites.json').read_text())
for s in ss:
 p=ROOT/s['file'] if s['file'].startswith('dependency-sources/') else ROOT/'sources'/s['target']/s['file']
 assert hashlib.sha256(p.read_bytes()).hexdigest()==s['file_sha256'] and p.read_text().splitlines()[s['line']-1].strip()==s['expected']
for s in ww:
 p=ROOT/'workload'/s['file'];assert hashlib.sha256(p.read_bytes()).hexdigest()==s['file_sha256'] and p.read_text().splitlines()[s['line']-1].strip()==s['expected']
a=(ROOT/'audit.md').read_text();known={(s['file'],s['line']) for s in ss+ww}
for f,n in re.findall(r'`((?:internal/|dependency-sources/|experiments/bench75/|\.github/)[^`]+):(\d+)`',a):assert (f,int(n)) in known,(f,n)
for sha in list(b['targets'].values())+[g['commit']]:assert sha in a
for claim in ('**4** | **4**','+ K_s','+ K_r','64KiB','8 new input + conditional','No presumed 2.8','38400142994'):assert claim in a,claim
# Put payload-path identity checked independently, not inferred from clock PR title.
for f in ['internal/server/stream.go','internal/core/codec.go','internal/core/node.go','internal/core/wire.go','internal/mvcc/ops.go','internal/wal/wal.go','internal/peer/peer.go']:
 assert (ROOT/'sources/main'/f).read_bytes()==(ROOT/'sources/pr84'/f).read_bytes(),('value path changed',f)
(ROOT/'artifact-check.json').write_text(json.dumps({'status':'FINAL','actions_run':b['actions_run'],'targets':b['targets'],'generator_commit':g['commit'],'verified_server_sites':len(ss),'verified_workload_sites':len(ww),'payload_path_byte_identity':True,'sealed80_reference_hashes_unchanged':True,'source_only':True,'final_ready':True,'checked_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n')
files=sorted(p for p in ROOT.rglob('*') if p.is_file() and p.name!='SHA256SUMS')
(ROOT/'SHA256SUMS').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+str(p.relative_to(ROOT))+'\n' for p in files));print('FINAL sealed',len(files),'hashes;',len(ss),'server sites;',len(ww),'workload/accounting sites')
