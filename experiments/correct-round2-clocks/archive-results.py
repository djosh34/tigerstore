#!/usr/bin/env python3
"""Archive every attempt, including setup/build failure, with streaming full-value compression."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('source',type=Path)
p.add_argument('destination',type=Path)
a=p.parse_args()
a.destination.mkdir(parents=True,exist_ok=True)
summaries=[]
def load(path,default):
    return json.loads(path.read_text()) if path.exists() else default
for artifact in sorted(a.source.glob('clocks-*')):
    dst=a.destination/artifact.name
    dst.mkdir(exist_ok=True)
    manifest=[]
    for src in sorted(artifact.rglob('*')):
        if not src.is_file() or src.suffix not in ('.json','.jsonl','.log','.txt'):
            continue
        rel=src.relative_to(artifact)
        # No mutable server disks or generated private keys. Full JSON values stay.
        if any(x in rel.parts for x in ('data','original','certs','n1','n2','n3')):
            continue
        target=dst/rel.with_suffix(rel.suffix+'.gz')
        target.parent.mkdir(parents=True,exist_ok=True)
        digest=hashlib.sha256(); size=0
        with src.open('rb') as inp,target.open('wb') as raw:
            with gzip.GzipFile(filename='',fileobj=raw,mode='wb',mtime=0) as out:
                while chunk:=inp.read(1024*1024):
                    digest.update(chunk);size+=len(chunk);out.write(chunk)
        manifest.append({'path':str(rel),'sha256':digest.hexdigest(),'bytes':size})
    (dst/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    run=artifact/'run'
    attempt=load(artifact/'attempt.json',{})
    faults=load(run/'faults.json',{})
    result=load(run/'result.json',{})
    offline=load(run/'offline-results.json',[])
    receipt=load(run/'workload/receipt.json',{})
    reports=[r for path in run.glob('check-*.json') if not path.name.endswith('.coverage.json') for r in load(path,[])]
    covs=[load(path,{}) for path in run.glob('check-*.json.coverage.json')]
    reached=(run/'faults.json').exists()
    summary={'artifact':artifact.name,'attempt':attempt,'reached_controller':reached,
        'recorded_history':(run/'workload/history.json').exists(),
        'controller_pass':result.get('controller_pass',False),
        'offline_pass':bool(offline) and all(x['returncode']==0 for x in offline),
        'oracle_sets':len(load(run/'oracles.json',[])),
        'checker_verdicts':len(reports),'checker_passes':sum(x['passed'] for x in reports),
        'synthetic_missing_verdicts':sum(x.get('synthetic_missing_verdict',False) for x in reports),
        'operations':receipt.get('ops',0),'target':receipt.get('target',0),
        'faults':{k:sorted(v) for k,v in faults.get('faults',{}).items()},
        'controller_seconds':max((x['monotonic_ns'] for x in faults.get('events',[])),default=0)/1e9,
        'failures':[x.get('error') for x in faults.get('events',[]) if x['name']=='controller-failure'],
        'coverage':covs[:1]}
    summaries.append(summary)
(a.destination/'summary.json').write_text(json.dumps(summaries,indent=2)+'\n')
print(json.dumps(summaries,indent=2))
