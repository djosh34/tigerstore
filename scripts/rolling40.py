#!/usr/bin/env python3
"""Fixed manager comparison: exactly 20 original rolling(race) jobs per ref."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

REFS={'main':'55b32bb9e6ef692cdb9d1b0e3cdb16542811dec4','candidate':'9fad820b8f02882e47952d17013c6541f62734e7'}
COMPONENT='Test(TheClusterTakesAFormat|AMemberThatIsDownHoldsTheFormat|AStaleRunsAnnouncement|AMembershipChangeAsksForConfirmations|ACheckpointKeepsTheGate|TheApplyChecksABatchsFormat|EachStartHasALaterRun|EachRunNamesItsBatchesAfresh|ANamedBatchOfAnEarlierRun|TheLogOfAStoppedNodeReadsBack)'
START=re.compile(r'chaos: start incarnation (\d+) at (\S+)')
UP=re.compile(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+) node .* up: clients')

def startups(text,path):
    found=list(START.finditer(text));result=[]
    for i,m in enumerate(found):
        body=text[m.end():found[i+1].start() if i+1<len(found) else len(text)]
        up=UP.search(body);elapsed=None
        if up:
            start=datetime.datetime.fromisoformat(m[2].rstrip(',').replace('Z','+00:00'))
            end=datetime.datetime.strptime(up[1],'%Y/%m/%d %H:%M:%S.%f').replace(tzinfo=datetime.timezone.utc)
            elapsed=(end-start).total_seconds()
        dump=f'chaos: incarnation {m[1]} is not up' in body
        stacks=dump and bool(re.search(r'goroutine \d+ \[',body))
        stalled=dump or elapsed is not None and elapsed>10
        result.append(dict(log=str(path),incarnation=int(m[1]),started_at=m[2].rstrip(','),time_to_up_seconds=elapsed,stall_over_10s=stalled,watchdog_dump_requested=dump,goroutine_dump_present=stacks,expected_old_binary_refusal='this binary is too old for the cluster' in body))
    return result

def save(path,value):path.write_text(json.dumps(value,indent=2)+'\n')

def execute(args):
    if os.environ.get('GITHUB_ACTIONS')!='true':raise ValueError('Actions only')
    source=args.source.resolve();out=args.out.resolve();out.mkdir(parents=True,exist_ok=True)
    if (out/'result.json').exists():raise ValueError('run output already exists; never overwrite a fixed-count result')
    ref=subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()
    if ref!=REFS[args.side]:raise ValueError('immutable runtime ref mismatch')
    result=dict(side=args.side,ordinal=args.ordinal,source_ref=ref,expected_per_side=20,run_url=os.environ['RUN_URL'],job_url_hint=os.environ.get('GITHUB_JOB'),components={},startups=[],rolling_returncode=None)
    save(out/'result.json',result)
    # Existing watchdog sends SIGUSR1 after 10s and the unmodified binaries
    # already expose stacksOnSignal. No profiling observer/source injection.
    cluster=(source/'cmd/chaos/cluster.go').read_text()
    if 'const slowStart = 10 * time.Second' not in cluster or 'go n.stacksIfSlow' not in cluster:raise ValueError('startup watchdog drift')
    for name,number in [('old',1),('new',2)]:
        text=(source/'internal/wl/format.go').read_text();anchor='const maxFormat = 1\n'
        if text.count(anchor)!=1:raise ValueError('format overlay drift')
        target=out/f'{name}-format.go';target.write_text(text.replace(anchor,f'const maxFormat = {number}\n'))
        patches={str(source/'internal/wl/format.go'):str(target)}
        if name=='old':
            original=source/'internal/wl/refusal.go';text=original.read_text();anchor='const peerRefusals = true\n'
            if text.count(anchor)!=1:raise ValueError('refusal overlay drift')
            target=out/'old-refusal.go';target.write_text(text.replace(anchor,'const peerRefusals = false\n'));patches[str(original)]=str(target)
        save(out/f'{name}.json',{'Replace':patches})
        subprocess.run(['go','build','-race','-overlay',str(out/f'{name}.json'),'-o',str(out/f'tigerstore-{name}'),'./cmd/tigerstore'],cwd=source,check=True)
    save(out/'binary-sha256.json',{n:hashlib.sha256((out/f'tigerstore-{n}').read_bytes()).hexdigest() for n in ('old','new')})
    for name in ('old','new'):
        with (out/f'components-{name}.log').open('w') as log:
            p=subprocess.run(['go','test','-race','-overlay',str(out/f'{name}.json'),'-count=1','-v','./internal/wl','-run',COMPONENT],cwd=source,stdout=log,stderr=subprocess.STDOUT)
        result['components'][name]=p.returncode;save(out/'result.json',result)
    env=dict(os.environ,ROLLING_OLD=str(out/'tigerstore-old'),ROLLING_NEW=str(out/'tigerstore-new'),ROLLING_OUT=str(out/'rolling'),ETCD_UNSAFE_LOG_RING_BYTES='536870912')
    with (out/'rolling.log').open('w') as log:
        p=subprocess.run(['go','test','-race','-count=1','-timeout','8m','-v','./cmd/chaos','-run','^TestRollingUpgrade$'],cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT)
    result['rolling_returncode']=p.returncode
    for path in sorted((out/'rolling').glob('seed-*/node*.log')):result['startups']+=startups(path.read_text(errors='replace'),path.relative_to(out))
    result['stall_count']=sum(x['stall_over_10s'] for x in result['startups'])
    result['run_stalled']=result['stall_count']>0
    result['dump_capture_gaps']=[x for x in result['startups'] if x['stall_over_10s'] and not x['goroutine_dump_present']]
    result['complete']=bool(result['startups'])
    save(out/'result.json',result)
    # Keep every log, stack and input overlay, but avoid duplicating gigabytes
    # of sparse rings and executable files across the forty artifacts.
    if not result['run_stalled'] and p.returncode==0:
        for path in (out/'rolling').glob('seed-*/data'):
            subprocess.run(['rm','-rf',str(path)],check=True)
    for name in ('old','new'):(out/f'tigerstore-{name}').unlink()
    return int(p.returncode!=0 or any(result['components'].values()) or not result['complete'] or bool(result['dump_capture_gaps']))

def summary(args):
    files=list(args.artifacts.glob('**/result.json'));report={'expected_runs_per_side':20,'classification':'manager compares rates; no automatic clearance','sides':{}}
    for side in REFS:
        rows=[json.loads(p.read_text()) for p in files if json.loads(p.read_text()).get('side')==side]
        ordinals=[r['ordinal'] for r in rows]
        report['sides'][side]=dict(ref=REFS[side],runs=rows,complete=len(rows)==20 and sorted(ordinals)==list(range(1,21)) and all(r.get('complete') and r.get('rolling_returncode') is not None for r in rows),stall_runs=sum(bool(r.get('run_stalled')) for r in rows),stall_incarnations=sum(r.get('stall_count',0) for r in rows),failed_runs=sum(r.get('rolling_returncode')!=0 for r in rows),component_failed_runs=sum(any(r.get('components',{}).values()) for r in rows),dump_capture_gaps=sum(len(r.get('dump_capture_gaps',[])) for r in rows))
    save(args.out,report);print(json.dumps(report,indent=2))
    return int(not all(x['complete'] for x in report['sides'].values()))

if __name__=='__main__':
    p=argparse.ArgumentParser();s=p.add_subparsers(dest='mode',required=True)
    r=s.add_parser('run');r.add_argument('--side',choices=REFS,required=True);r.add_argument('--ordinal',type=int,choices=range(1,21),required=True);r.add_argument('--source',type=Path,required=True);r.add_argument('--out',type=Path,required=True)
    r=s.add_parser('summary');r.add_argument('--artifacts',type=Path,required=True);r.add_argument('--out',type=Path,required=True)
    a=p.parse_args();raise SystemExit(execute(a) if a.mode=='run' else summary(a))
