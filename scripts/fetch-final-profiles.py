#!/usr/bin/env python3
"""Actions-only named/Kubernetes paired profiles and per-job #602 ceilings.
Ruling A only; never use the obsolete median/extra-five prototype.
"""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import subprocess

HERE=Path(__file__).resolve().parent
BASE='55b32bb9e6ef692cdb9d1b0e3cdb16542811dec4'
HEAD='9fad820b8f02882e47952d17013c6541f62734e7'

def module(name,file):
 s=importlib.util.spec_from_file_location(name,HERE/file);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
resident=module('resident','fetch-resident-measure.py');policy=resident.policy

def plan(document):
 jobs=[]
 for name in ('range','list','watch','put','txn'):
  for size in ('1k','10k'):
   for rtt in (0,2,10):
    if (name,size,rtt)==('range','1k',0):continue # manager accepts the frozen9f control
    jobs.append(dict(case=f'{name}-{size}',bench=f'{name}-{size}',seed=94125874,rtt=rtt,shape=''))
 for shape,cell in document['shapes'].items():
  for seed in cell['seeds']:
   for rtt in (0,2,10):jobs.append(dict(case='k8s-'+shape,bench='k8s',seed=seed,rtt=rtt,shape=shape))
 return jobs

def finite(x):return isinstance(x,(int,float)) and not isinstance(x,bool) and math.isfinite(x)

def adapt(data,side,case):
 if data.get('valid') is not True or data.get('errors') or data.get('unsent') or data.get('cohort_failed'):raise ValueError(f'{side}: invalid/incomplete native cohort')
 if data.get('ramp') or data.get('ramp_climbing'):raise ValueError(f'{side}: unmatched adaptive load')
 conns,streams=(3,64) if case['shape'] else ((2,16) if case['bench'].startswith('list-') else (6,256))
 for name,value in dict(conns=conns,streams_per_conn=streams,offered_rate=0,warmup_s=3,duration_s=10).items():
  if data.get(name)!=value:raise ValueError(f'{side}: unmatched fixed-load {name}')
 if not finite(data.get('client_cores')) or not 0<=data['client_cores']<=.8:raise ValueError(f'{side}: generator-limited')
 if data.get('ops',0)<=0 or data.get('cohort',0)<=0 or data['latency_us']['count']!=data['cohort']:raise ValueError(f'{side}: missing native cohort samples')
 if data.get('workload')!=case['bench'].split('-')[0]:raise ValueError(f'{side}: workload mismatch')
 if case['shape'] and data.get('k8s',{}).get('shape')!=case['shape']:raise ValueError(f'{side}: recorded shape mismatch')
 if not case['shape'] and data.get('value_size')!={'1k':1024,'10k':10240}[case['bench'].split('-')[1]]:raise ValueError(f'{side}: value size mismatch')
 q=data['watch']['latency_us'] if case['bench'].startswith('watch-') else data['latency_us']
 if not all(finite(q.get(k)) and q[k]>0 for k in ('p95','p99','max')) or not q['p95']<=q['p99']<=q['max']:raise ValueError(f'{side}: missing/invalid p95/p99/max')
 samples=data['fetch_measure']
 if len(samples)!=2 or any(len(x)!=3 for x in samples):raise ValueError(f'{side}: need before/after all-member observations')
 for snapshot in samples:
  for node in snapshot:
   if node is None or set(node)!={resident.PREFIX+n for n in resident.NAMES} or any(not finite(v) or v<0 for v in node.values()):raise ValueError(f'{side}: missing/nonfinite source observer')
   if node.get(resident.PREFIX+'memory_budget_bytes')!=3<<30 or node.get(resident.PREFIX+'cursor_routing')!=(side=='candidate'):raise ValueError(f'{side}: actual budget/routing mismatch')
 if not math.isclose(data['throughput'],data['ops']/data['duration_s'],rel_tol=1e-12):raise ValueError('throughput mismatch')
 return dict(throughput=data['throughput'],wall_ms=data['duration_s']*1000,latency_ms={k:q[k]/1000 for k in ('p95','p99','max')},refusals=dict(total=0,max_per_range=0),source_samples=samples,client_cores=data['client_cores'],qualification='native fixed-load profile; actual budget and routing observed, not cold/reuse evidence')

def run(a):
 if os.environ.get('GITHUB_ACTIONS')!='true':raise ValueError('Actions only')
 out=a.out.resolve();out.mkdir(parents=True,exist_ok=False)
 case=json.loads(a.cell);root=Path.cwd();refs=dict(baseline=BASE,candidate=HEAD)
 manifest=dict(schema=1,status='INCOMPLETE',scope='named-k8s-profile-and-602',refs=refs,case=case,pairs=[],run_url=os.environ['FETCH_MEASURE_RUN_URL'],tooling_ref=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),noise_method=policy.NOISE_METHOD)
 def save(): (out/'manifest.json').write_text(json.dumps(manifest,indent=2,allow_nan=False)+'\n')
 save()
 if case['shape']:
  declared=json.loads((root/'bench/fetch-measure-plan.json').read_text())['shapes'][case['shape']]
  trace=root/declared['trace'];digest=hashlib.sha256(trace.read_bytes()).hexdigest()
  if digest!=declared['trace_sha256'] or case['seed'] not in declared['seeds']:raise ValueError('recording/seed provenance mismatch')
  manifest['recording_sha256']=digest
 bins=out/'bins';bins.mkdir()
 for name,pkg in [('loadgen','./cmd/loadgen'),('ceiling','./cmd/ceiling')]:subprocess.run(['go','build','-o',str(bins/name),pkg],check=True)
 for side,ref in refs.items():
  checkout=Path(os.environ['RUNNER_TEMP'])/f'profile-{side}'
  subprocess.run(['git','worktree','add','--detach',str(checkout),ref],check=True)
  resident.prepare(checkout,out,side)
  subprocess.run(['go','build','-o',str(bins/side),'./cmd/tigerstore'],cwd=checkout,check=True)
 (bins/'etcdctl').write_bytes(Path(os.environ['FETCH_ETCDCTL']).read_bytes());(bins/'etcdctl').chmod(0o755)
 certs=out/'certs';subprocess.run(['bash','scripts/dev-certs.sh',str(certs)],check=True)
 env=dict(os.environ,CERTS=str(certs),BINS=str(bins),LOADGEN=str(bins/'loadgen'),BENCH_GOMEMLIMIT='3GiB',BENCH_FLAGS='--memory-budget-bytes 3221225472',ETCD_UNSAFE_LOG_RING_BYTES='536870912',FETCH_MEASURE_FIXED_LOAD='1',MODES='max',SEEDS=str(case['seed']),REPS='1',LAYOUT='3 0 1 2',LAYOUTS='',LEAD='1',DATA=str(out/'data'))
 disk=subprocess.check_output(['findmnt','-T',str(out),'-n','-o','SOURCE'],text=True).strip()
 def ceiling(name):
  with (out/name).open('w') as f:subprocess.run(['taskset','-c','3',str(bins/'ceiling'),'-dir',str(out),'-disk',disk],stdout=f,check=True)
 applicable=case['bench'].startswith(('put-','txn-')) or case['bench']=='k8s'
 if applicable:ceiling('ceiling.json')
 subprocess.run(['sudo','bash','bench/netem.sh','on',str(case['rtt'])],check=True)
 try:
  for number in range(1,6):
   order='AB' if number%2 else 'BA';pair=dict(pair=number,order=order,case=case['case'])
   for side in (('baseline','candidate') if order=='AB' else ('candidate','baseline')):
    dest=out/f'{number:02}-{side}';logfile=out/f'{number:02}-{side}.log'
    with logfile.open('w') as log:p=subprocess.run(['bash','bench/run.sh',case['bench'],str(dest)],env=dict(env,TARGETS=side),stdout=log,stderr=subprocess.STDOUT)
    failures=list(dest.rglob('*.failed'))
    if p.returncode or failures:
     manifest.update(status='RULING',reasons=['benchmark/leader check failed'],failed_steps=[str(x.relative_to(out)) for x in failures]);save();return 1
    raw=dest/case['bench']/f'{side}-max-{case["seed"]}.1.json'
    try:pair[side]=adapt(json.loads(raw.read_text()),side,case)
    except (ValueError,KeyError,TypeError) as e:manifest.update(status='INCOMPLETE',reasons=[str(e)]);save();return 2
    pair[side].update(raw_file=str(raw.relative_to(out)),raw_sha256=hashlib.sha256(raw.read_bytes()).hexdigest());manifest['partial_pair']=pair;save()
   manifest['pairs'].append(pair);manifest.pop('partial_pair',None);report=policy.assess(manifest['pairs']);manifest.update(status=report['status'],report=report);save()
   if report['status']=='RULING':return 1
 finally:subprocess.run(['sudo','bash','bench/netem.sh','off'],check=True)
 if applicable:
  ceiling('ceiling-end.json')
  helper=root/'cmd/benchreport/fetch_ceiling_test.go'
  if helper.exists():raise ValueError('ceiling helper collision')
  helper.write_bytes((root/'bench/fetch-measure/ceiling_test.go.txt').read_bytes())
  try:
   with (out/'ceiling-model.log').open('w') as log:
    p=subprocess.run(['go','test','./cmd/benchreport','-run','^TestFetchCeilingMeasurement$','-count=1','-v'],env=dict(env,FETCH_CEILING_DIR=str(out),FETCH_BENCH=case['bench'],FETCH_SEED=str(case['seed']),FETCH_RTT=str(case['rtt']),FETCH_TRACES=str(root/'bench/recordings')),stdout=log,stderr=subprocess.STDOUT)
   if (out/'ceiling-gate.json').exists():manifest['ceiling']=json.loads((out/'ceiling-gate.json').read_text())
   if p.returncode:manifest.update(status='RULING',reasons=['#602 measured-ceiling/disk gate failed']);save();return 1
  finally:helper.unlink()
 save();return 0

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--plan',action='store_true');p.add_argument('--cell');p.add_argument('--out',type=Path);a=p.parse_args()
 if a.plan:print(json.dumps({'include':plan(json.loads(Path('bench/fetch-measure-plan.json').read_text()))}))
 else:raise SystemExit(run(a))
