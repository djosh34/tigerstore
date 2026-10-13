#!/usr/bin/env python3
"""Keep all attempts, including setup failures, with round-1 audit-compatible manifests."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path

def load(path,default):
    return json.loads(path.read_text()) if path.exists() else default

def main():
    p=argparse.ArgumentParser();p.add_argument('source',type=Path);p.add_argument('destination',type=Path)
    a=p.parse_args();a.destination.mkdir(parents=True,exist_ok=True);summaries=[]
    for artifact in sorted(a.source.glob('correct-round2-semantics-*')):
        run=artifact/'run';dst=a.destination/artifact.name;dst.mkdir(exist_ok=True)
        paths=list(artifact.glob('*.log'))+list(artifact.glob('*.txt'))+list(artifact.glob('*.json'))
        paths+=list(run.glob('*.json'))+list(run.glob('*.jsonl'))+list(run.glob('*.log'))
        paths+=list((run/'workload').glob('*.json'))+list(run.glob('cold-*/inspect.json'))+list(run.glob('cold-*/stopped.json'))
        manifest=[]
        for src in sorted(set(paths)):
            relative=src.relative_to(artifact);target=dst/relative.with_suffix(relative.suffix+'.gz')
            target.parent.mkdir(parents=True,exist_ok=True);digest=hashlib.sha256();size=0
            with src.open('rb') as inp,target.open('wb') as raw:
                with gzip.GzipFile(filename='',mode='wb',fileobj=raw,mtime=0) as out:
                    while block:=inp.read(1024*1024):digest.update(block);size+=len(block);out.write(block)
            manifest.append({'path':str(relative),'sha256':digest.hexdigest(),'bytes':size})
        (dst/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
        f=load(run/'faults.json',{});r=load(run/'result.json',{});h=load(run/'workload/history.json',{})
        offline=load(run/'offline-results.json',[]);oracles=load(run/'oracles.json',[])
        checks=[]
        for report in run.glob('check-*.json'):checks+=load(report,[])
        fault_complete=bool(f.get('required')) and all(all(phase in f.get('faults',{}).get(name,{}) for phase in ('requested','installed','exercised','healed')) for name in f.get('required',[]))
        passed=bool(r.get('controller_pass')) and len(oracles)==4 and len(checks)==16 and all(x['passed'] for x in checks) and bool(offline) and all(x['returncode']==0 for x in offline) and fault_complete
        summary={'artifact':artifact.name,'attempt':load(artifact/'attempt.json',{}),'reached_controller':bool(f),
                 'recorded_history':bool(h),'controller_pass':bool(r.get('controller_pass')),'passed':passed,
                 'offline_pass':bool(offline) and all(x['returncode']==0 for x in offline),
                 'oracle_sets':len(oracles),'checker_verdicts':len(checks),'checker_passes':sum(x['passed'] for x in checks),
                 'operations':len(h.get('ops',[])),'target':h.get('target',0),'watches':len(h.get('watches',[])),
                 'faults':{k:sorted(v) for k,v in f.get('faults',{}).items()},
                 'failures':[e.get('error') for e in f.get('events',[]) if e['name']=='controller-failure'],
                 'checker_failures':[x for x in checks if not x['passed']]}
        summaries.append(summary)
    (a.destination/'summary.json').write_text(json.dumps(summaries,indent=2)+'\n')
    print(json.dumps(summaries,indent=2))
if __name__=='__main__':main()
