#!/usr/bin/env python3
"""Stream selected ZIP members into gzip evidence. Never extract server disks."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import zipfile
p=argparse.ArgumentParser()
p.add_argument('zip',type=Path)
p.add_argument('name')
p.add_argument('destination',type=Path)
a=p.parse_args()
dst=a.destination/a.name
dst.mkdir(parents=True,exist_ok=True)
manifest=[]
with zipfile.ZipFile(a.zip) as z:
    names=z.namelist()
    def load(name,default):return json.loads(z.read(name)) if name in names else default
    for name in names:
        rel=Path(name)
        if rel.is_absolute() or '..' in rel.parts:raise RuntimeError('unsafe ZIP path')
        if rel.suffix not in ('.json','.jsonl','.log','.txt') or any(x in rel.parts for x in ('data','original','certs','n1','n2','n3')):continue
        target=dst/rel.with_suffix(rel.suffix+'.gz')
        target.parent.mkdir(parents=True,exist_ok=True)
        digest=hashlib.sha256();size=0
        with z.open(name) as inp,target.open('wb') as raw:
            with gzip.GzipFile(filename='',fileobj=raw,mode='wb',mtime=0) as out:
                while chunk:=inp.read(1024*1024):digest.update(chunk);size+=len(chunk);out.write(chunk)
        manifest.append({'path':name,'sha256':digest.hexdigest(),'bytes':size})
    faults=load('run/faults.json',{})
    result=load('run/result.json',{})
    offline=load('run/offline-results.json',[])
    receipt=load('run/workload/receipt.json',{})
    reports=[r for name in names if name.startswith('run/check-') and name.endswith('.json') and not name.endswith('.coverage.json') for r in load(name,[])]
    covs=[load(name,{}) for name in names if name.startswith('run/check-') and name.endswith('.coverage.json')]
    summary={'artifact':a.name,'attempt':load('attempt.json',{}),
        'reached_controller':'run/faults.json' in names,'recorded_history':'run/workload/history.json' in names,
        'controller_pass':result.get('controller_pass',False),
        'offline_pass':bool(offline) and all(x['returncode']==0 for x in offline),
        'oracle_sets':len(load('run/oracles.json',[])),
        'checker_verdicts':len(reports),'checker_passes':sum(x['passed'] for x in reports),
        'synthetic_missing_verdicts':sum(x.get('synthetic_missing_verdict',False) for x in reports),
        'operations':receipt.get('ops',0),'target':receipt.get('target',0),
        'faults':{k:sorted(v) for k,v in faults.get('faults',{}).items()},
        'controller_seconds':max((x['monotonic_ns'] for x in faults.get('events',[])),default=0)/1e9,
        'failures':[x.get('error') for x in faults.get('events',[]) if x['name']=='controller-failure'],
        'coverage':covs[:1]}
(dst/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
(a.destination/'summary.json').write_text(json.dumps([summary],indent=2)+'\n')
