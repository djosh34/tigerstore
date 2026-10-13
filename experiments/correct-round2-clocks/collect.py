#!/usr/bin/env python3
"""Download compact evidence, or stream selected legacy ZIP members without extraction."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

p=argparse.ArgumentParser()
p.add_argument('run_id')
p.add_argument('--legacy-zip', '--legacy-full', dest='legacy_full', action='store_true',
               help='stream legacy raw artifact ZIP members directly into gzip; never extract disks')
a=p.parse_args()
base=Path(__file__).resolve().parent
out=base/'results'/f'run-{a.run_id}'
out.mkdir(parents=True,exist_ok=True)
def api(path):
    return json.loads(subprocess.check_output(['gh','api',f'repos/djosh34/tigerstore/{path}']))
run=api(f'actions/runs/{a.run_id}')
if run['status']!='completed':raise SystemExit('only collect completed runs')
jobs=api(f'actions/runs/{a.run_id}/jobs?per_page=100')['jobs']
artifacts=api(f'actions/runs/{a.run_id}/artifacts?per_page=100')['artifacts']
prefix='clocks-' if a.legacy_full else 'compact-clocks-'
summaries=[]
(base/'.downloads').mkdir(exist_ok=True)
for art in artifacts:
    if not art['name'].startswith(prefix):continue
    name=art['name'] if a.legacy_full else art['name'][8:]
    with tempfile.TemporaryDirectory(dir=base/'.downloads') as temp:
        root=Path(temp)
        if a.legacy_full:
            zipped=root/'artifact.zip'
            with zipped.open('wb') as stream:
                subprocess.run(['gh','api',f"repos/djosh34/tigerstore/actions/artifacts/{art['id']}/zip"],stdout=stream,check=True)
            subprocess.run(['python3',str(base/'archive-zip.py'),str(zipped),name,str(out)],check=True)
            summaries+=json.loads((out/'summary.json').read_text())
        else:
            subprocess.run(['gh','run','download',a.run_id,'-n',art['name'],'-D',str(root)],check=True)
            shutil.copytree(root/name,out/name,dirs_exist_ok=True)
            summaries+=json.loads((root/'summary.json').read_text())
    target=out/name
    with gzip.open(target/'attempt.json.gz','rt') as f:case=json.load(f)['case']
    matched=[]
    for job in jobs:
        fields=[x.strip() for x in job['name'][8:-1].split(',')] if job['name'].startswith('faults ') else []
        if all(str(case[k]).lower() in fields for k in ('runner','target','scenario','seed','rep','role')):
            matched.append(job)
    if len(matched)!=1:raise RuntimeError(f'job identity match failed {name}: {matched}')
    job=matched[0]
    manifest=json.loads((target/'manifest.json').read_text())
    files={'job.json':json.dumps(job,indent=2).encode()+b'\n',
           'actions-job.log':subprocess.check_output(['gh','api',f"repos/djosh34/tigerstore/actions/jobs/{job['id']}/logs"])}
    for path,data in files.items():
        with (target/(path+'.gz')).open('wb') as raw:
            with gzip.GzipFile(filename='',fileobj=raw,mode='wb',mtime=0) as dst:dst.write(data)
        manifest=[x for x in manifest if x['path']!=path]
        manifest.append({'path':path,'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)})
    (target/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(name,'collected',flush=True)
(out/'summary.json').write_text(json.dumps(sorted(summaries,key=lambda x:x['artifact']),indent=2)+'\n')
(out/'run.json').write_text(json.dumps(run,indent=2)+'\n')
(out/'jobs.json').write_text(json.dumps(jobs,indent=2)+'\n')
if len(summaries)!=len([j for j in jobs if j['name'].startswith('faults ')]):raise RuntimeError('missing attempt artifacts')
