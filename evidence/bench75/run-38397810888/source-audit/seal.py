#!/usr/bin/env python3
"""Small manifest and source-only artifact checks. Never runs Go or network."""
from pathlib import Path
import json,hashlib,datetime,re,subprocess
ROOT=Path('/home/joshazimullah.linux/work_mounts/scratch/bench80-lean-source-audit')
REPO=Path('/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75')
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
b=json.loads((ROOT/'binding.json').read_text());w=json.loads((ROOT/'workload-manifest.json').read_text())
for r in json.loads((ROOT/'source-file-hashes.json').read_text()):
 assert hashlib.sha256((ROOT/r['path']).read_bytes()).hexdigest()==r['sha256']
for r in w['files']:
 p=ROOT/r['path'];assert hashlib.sha256(p.read_bytes()).hexdigest()==r['sha256']
 if r.get('commit'):
  assert p.read_bytes()==subprocess.check_output(['git','show',r['commit']+':'+r['repo_path']],cwd=REPO)
for r in json.loads((ROOT/'reference-manifest.json').read_text())['references']:
 assert hashlib.sha256((REPO/r['path']).read_bytes()).hexdigest()==r['sha256']
sites=json.loads((ROOT/'verified-sites.json').read_text())
for s in sites:
 p=ROOT/s['file'] if s['file'].startswith('dependency-sources/') else ROOT/'sources'/s['target']/s['file']
 assert hashlib.sha256(p.read_bytes()).hexdigest()==s['file_sha256']
 assert p.read_text().splitlines()[s['line']-1].strip()==s['expected']
for s in json.loads((ROOT/'workload-sites.json').read_text()):
 p=ROOT/'workload/observed'/s['file'];assert hashlib.sha256(p.read_bytes()).hexdigest()==s['sha256']
 assert p.read_text().splitlines()[s['line']-1].strip()==s['expected']
a=(ROOT/'audit.md').read_text()
for t,sha in b['targets'].items():assert sha in a
assert b['status'] in a and w['status'] in a
for claim in ('**4** | **4** | **6** | **5**','old stored value','no grpc-timeout','not physical','Final measurement binding deferred' if b['status']!='FINAL' else 'Final server and generator/harness bindings are parent-confirmed'):
 assert claim in a,claim
# Every report citation to retained server/dependency sources is an asserted site.
known={(s['file'],s['line']) for s in sites}
for path,line in re.findall(r'`((?:internal/|dependency-sources/)[^`]+):(\d+)`',a):assert (path,int(line)) in known,(path,line)
knownw={(s['file'],s['line']) for s in json.loads((ROOT/'workload-sites.json').read_text())}
for path,line in re.findall(r'`((?:experiments/bench75/|\.github/)[^`]+):(\d+)`',a):assert (path,int(line)) in knownw,(path,line)
status={'server_binding':b['status'],'generator_binding':w['status'],'final_ready':b['status']=='FINAL' and w['status'].startswith('FINAL'),'verified_server_sites':len(sites),'verified_workload_sites':len(knownw),'checked_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_only':True,'writes_owned_root_only':str(ROOT)}
(ROOT/'artifact-check.json').write_text(json.dumps(status,indent=2)+'\n')
files=sorted(p for p in ROOT.rglob('*') if p.is_file() and p.name!='SHA256SUMS')
(ROOT/'SHA256SUMS').write_text(''.join(hashlib.sha256(p.read_bytes()).hexdigest()+'  '+str(p.relative_to(ROOT))+'\n' for p in files))
print('Sealed',len(files),'small artifact/source hashes;',len(sites),'server sites;',len(knownw),'workload sites; final_ready=',status['final_ready'])
