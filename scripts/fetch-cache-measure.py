#!/usr/bin/env python3
"""Actions-only matched external-member cache/reuse experiment, not resident data.
No per-RPC/cache-hit observer: immutable offset/version + retained cache timestamp
snapshots bracket genuinely different, preconnected client requests.
"""
import argparse
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time
import urllib.request

HERE=Path(__file__).parent
s=importlib.util.spec_from_file_location('resident',HERE/'fetch-resident-measure.py');resident=importlib.util.module_from_spec(s);s.loader.exec_module(resident)
policy=resident.policy
BASE='55b32bb9e6ef692cdb9d1b0e3cdb16542811dec4';HEAD='9fad820b8f02882e47952d17013c6541f62734e7'
CACHES=('past-ram-560m','tmpfs-nowait-refused','evicted-cache')
COHORTS=('get-get','list-get','overlapping-list','failed-cas-get','overlapping-prev-kv')
NAMES=('member','budget','routing','jobs','values','loads','cache_entries','blocking_reads','blocking_bytes','nowait_calls','nowait_bytes','nowait_refusals','live_bytes','slab_used')+tuple(f'{key}_{name}' for key in ('a','b') for name in ('off_hi','off_lo','mod','size','sum','cached','at_sec','at_nsec'))

def prepare(checkout,out,side,root):
 # Only scrape capture and identical syscall wrappers; no cache policy/lookup
 # or production admission/representation changes.
 p=checkout/'internal/wl/prom.go';text=p.read_text()
 anchors=[('type promSnap struct {\n','type promSnap struct {\n fetchProbe []float64\n'),('p := &promSnap{stats: n.gs, format: n.format}','p := &promSnap{stats: n.gs, format: n.format}\n p.fetchProbe=fetchMeasureCacheNode(n)'),('func (c *promCollector) Describe(ch chan<- *prometheus.Desc) {\n','func (c *promCollector) Describe(ch chan<- *prometheus.Desc) {\n for _,d:=range fetchProbeDescs { ch<-d }\n'),('\tif p == nil {\n\t\treturn\n\t}\n\tg := &p.stats','\tif p == nil {\n\t\treturn\n\t}\n for i,d:=range fetchProbeDescs { constGauge(ch,d,p.fetchProbe[i]) }\n\tg := &p.stats')]
 for old,new in anchors:text=resident.replace(text,old,new)
 p.write_text(text)
 helper=(root/'bench/fetch-measure/cache_observer.go.txt').read_text().replace('FETCH_ROUTE','if n.cursorFetch { route=1 }' if side=='candidate' else '')
 helper=helper.replace('package wl','package wl\nimport "github.com/prometheus/client_golang/prometheus"\nvar fetchProbeDescs=[]*prometheus.Desc{'+','.join(f'prometheus.NewDesc("tigerstore_fetch_probe_{name}","Measurement-only scrape",nil,nil)' for name in NAMES)+',}\n',1)
 (checkout/'internal/wl/fetch_cache_observer.go').write_text(helper)
 (checkout/'internal/wl/fetch_cache_io.go').write_bytes((root/'bench/fetch-measure/io.go.txt').read_bytes())
 p=checkout/'internal/wl/sys_os.go';text=p.read_text();text=resident.replace(text,'return unix.Pread(fd, p, off)','return fetchMeasurePread(fd, p, off)');text=resident.replace(text,'return unix.Preadv2(fd, [][]byte{p}, off, unix.RWF_NOWAIT)','return fetchMeasurePreadNoWait(fd, p, off)');p.write_text(text)
 subprocess.run(['gofmt','-w',str(checkout/'internal/wl/fetch_cache_observer.go'),str(checkout/'internal/wl/fetch_cache_io.go'),str(checkout/'internal/wl/prom.go'),str(p)],check=True)
 dest=out/f'{side}-overlay';dest.mkdir()
 for name in ('fetch_cache_observer.go','fetch_cache_io.go','prom.go','sys_os.go'):(dest/name).write_bytes((checkout/'internal/wl'/name).read_bytes())
 (dest/'tracked.diff').write_bytes(subprocess.check_output(['git','diff','--binary'],cwd=checkout))

def adapt(probe,side,cache,budget):
 if probe.get('error') or len(probe['Results'])!=probe['Clients'] or any(x.get('error') for x in probe['Results']):raise ValueError('incomplete/incorrect request cohort')
 if not 0<=probe['ClientCores']<=.8:raise ValueError('reuse generator-limited; cannot qualify')
 snaps=[probe[n] for n in ('OriginBefore','OriginAfter','ReuseBefore','ReuseAfter')]
 for x in snaps:
  if set(x)!=set(NAMES) or any(not isinstance(v,(float,int)) or not math.isfinite(v) for v in x.values()):raise ValueError('missing/nonfinite source snapshot')
  if x['budget']!=budget or x['routing']!=(side=='candidate'):raise ValueError('actual budget/routing mismatch')
 if len({x['member'] for x in snaps})!=1:raise ValueError('member changed')
 for key in ('a','b'):
  descriptor=[tuple(x[f'{key}_{k}'] for k in ('off_hi','off_lo','mod','size','sum')) for x in snaps]
  if len(set(descriptor))!=1 or descriptor[0][3]!=probe['Size']:raise ValueError('source offset/version/length/checksum changed')
 if cache=='past-ram-560m' and snaps[0]['live_bytes']<=640<<20:raise ValueError('population does not exceed member RAM')
 before,origin,reuse,after=snaps
 if side=='baseline':
  keys=('a','b') if probe['Case']=='overlapping-list' else ('a',)
  if origin['jobs']<=before['jobs'] or origin['values']<=before['values']:raise ValueError('no main off-loop origin fetch')
  if probe['ElapsedCacheMS']>=1000:raise ValueError('reuse not within main one-second cache lifetime')
  for key in keys:
   if before[f'{key}_cached']!=0:raise ValueError('origin already cached; not cold fulfillment')
   if any(x[f'{key}_cached']!=1 for x in (origin,reuse,after)):raise ValueError('no main genuine different-request reuse')
   if len({(x[f'{key}_at_sec'],x[f'{key}_at_nsec']) for x in (origin,reuse,after)})!=1:raise ValueError('main cache was replaced/refetched during reuse')
   age=probe['FinishedUnixNano']-(int(origin[f'{key}_at_sec'])*1000000000+int(origin[f'{key}_at_nsec']))
   if not 0<=age<1000000000:raise ValueError('absolute cache timestamp is outside one-second reuse')
  if probe['Case']=='overlapping-prev-kv' and not probe['WatchValidated']:raise ValueError('prev_kv catchup not validated')
 else:
  if any(x['cache_entries']!=0 for x in snaps):raise ValueError('candidate unexpectedly retained fetched cache')
 q=probe['Latency']
 if not 0<q['p95']<=q['p99']<=q['max']:raise ValueError('invalid tails')
 return dict(throughput=probe['Throughput'],wall_ms=probe['WallMS'],latency_ms=q,refusals=probe['Refusals'],device_read_bytes=sum(probe['DeviceReadAfter'][pid]-v for pid,v in probe['DeviceReadBefore'].items()),io_delta={name:after[name]-before[name] for name in ('blocking_reads','blocking_bytes','nowait_calls','nowait_bytes','nowait_refusals')},origin_fulfillment=origin['values']-before['values'],same_consumer_retries=probe['OriginRetries']+sum(x['busy_retries'] for x in probe['Results']),distinct_request_reuses=probe['Clients'] if side=='baseline' else 0,source_snapshots=snaps)

def fields(path):
 out={}
 for l in path.read_text().splitlines():
  a=l.split()
  if len(a)==2:out[a[0]]=int(a[1])
 return out

def experiment(a,out,binary,driver,ordinal,side):
 budget=560<<20 if a.cache=='past-ram-560m' else 3<<30
 limit=640<<20 if a.cache=='past-ram-560m' else 3<<30
 parent=Path('/sys/fs/cgroup')/f'fetch897-{os.getpid()}-{ordinal}-{side}';parent.mkdir()
 Path('/sys/fs/cgroup/cgroup.subtree_control').write_text('+memory +cpu +io')
 (parent/'cgroup.subtree_control').write_text('+memory +cpu +io')
 data=out/'data';data.mkdir();nodes=[];groups=[];logs=[]
 subprocess.run(['bash','bench/netem.sh','on',str(a.rtt)],check=True)
 with (out/'netem-probe.txt').open('w') as f:subprocess.run(['bash','bench/netem.sh','probe'],stdout=f,check=True)
 with (out/'netem-config.txt').open('w') as f:subprocess.run(['bash','bench/netem.sh','show'],stdout=f,check=True)
 mounted=a.cache=='tmpfs-nowait-refused'
 if mounted:subprocess.run(['mount','-t','tmpfs','-o','size=4G','tmpfs',str(data)],check=True)
 initial=','.join(f'n{i}=https://127.0.0.1:{20001+10*i}' for i in (1,2,3));endpoints=','.join(f'http://127.0.0.1:{20000+10*i}' for i in (1,2,3))
 try:
  for i in (1,2,3):
   cg=parent/str(i);cg.mkdir();groups.append(cg);(cg/'memory.max').write_text(str(limit));(cg/'memory.swap.max').write_text('0')
   log=(out/f'node{i}.log').open('w');logs.append(log)
   command=['taskset','-c',str(i-1),str(binary),'--name',f'n{i}','--data-dir',str(data/f'n{i}'),'--listen-client-urls',f'http://127.0.0.1:{20000+10*i}','--advertise-client-urls',f'http://127.0.0.1:{20000+10*i}','--listen-peer-urls',f'https://127.0.0.1:{20001+10*i}','--initial-advertise-peer-urls',f'https://127.0.0.1:{20001+10*i}','--initial-cluster',initial,'--initial-cluster-token','fetch897-cache','--peer-auto-tls','--memory-budget-bytes',str(budget),'--disk-budget-bytes',str(8<<30 if a.cache=='past-ram-560m' else 1<<30)]
   env=dict(os.environ,GOMEMLIMIT='560MiB' if a.cache=='past-ram-560m' else '3GiB',GOMAXPROCS='2',FETCH_MEASURE_NOWAIT_REFUSED='1' if mounted else '0')
   nodes.append(subprocess.Popen(['sh','-ec','echo $$ > "$1/cgroup.procs"; shift; exec "$@"','probe',str(cg),*command],env=env,stdout=log,stderr=subprocess.STDOUT))
  deadline=time.monotonic()+180
  while True:
   ready=False
   try:
    statuses=json.loads(subprocess.check_output([a.etcdctl,'--endpoints',endpoints,'--command-timeout','2s','endpoint','status','-w','json'],stderr=subprocess.DEVNULL))
    ready=len(statuses)==3 and len({x['Status']['leader'] for x in statuses})==1 and statuses[0]['Status']['leader']!=0
   except (subprocess.CalledProcessError,KeyError,json.JSONDecodeError):pass
   if ready:break
   if time.monotonic()>deadline or any(n.poll() is not None for n in nodes):raise ValueError('cold cluster startup failed')
   time.sleep(.3)
  wanted=statuses[0]['Status']['header'].get('member_id',statuses[0]['Status']['header'].get('memberId'))
  if statuses[0]['Status']['leader']!=wanted:subprocess.run([a.etcdctl,'--endpoints',endpoints,'move-leader',f'{wanted:x}'],check=True)
  # Source actor and all peer ports share the exact same netem path.
  subprocess.run(['bash','bench/netem.sh','on',str(a.rtt)],check=True)
  with (out/'driver.log').open('w') as log:
   result=subprocess.run(['taskset','-c','3',str(driver),'-endpoint','http://127.0.0.1:20010','-endpoints',endpoints,'-pids',','.join(str(p.pid) for p in nodes),'-data',str(data),'-out',str(out/'raw.json'),'-cohort',a.cohort,'-cache',a.cache,'-preload-keys','16384' if a.cache=='past-ram-560m' else '1024'],stdout=log,stderr=subprocess.STDOUT)
  if result.returncode:raise ValueError('cache driver correctness/preparation failed; see raw.json/driver.log')
  return json.loads((out/'raw.json').read_text())
 finally:
  states=[]
  for cg in groups:
   states.append(dict(memory_max=int((cg/'memory.max').read_text()),memory_peak=int((cg/'memory.peak').read_text()),events=fields(cg/'memory.events'),stat=fields(cg/'memory.stat'),io=(cg/'io.stat').read_text()))
  (out/'cgroup-final.json').write_text(json.dumps(states,indent=2)+'\n')
  for n in nodes:
   if n.poll() is None:n.send_signal(signal.SIGTERM)
  for n in nodes:
   try:n.wait(timeout=20)
   except subprocess.TimeoutExpired:n.kill();n.wait()
  for l in logs:l.close()
  if mounted:subprocess.run(['umount',str(data)],check=True)
  shutil.rmtree(data)
  for cg in groups:cg.rmdir()
  parent.rmdir()
  subprocess.run(['bash','bench/netem.sh','off'],check=True)

def run(a):
 if os.environ.get('GITHUB_ACTIONS')!='true' or os.geteuid()!=0:raise ValueError('Actions/root only')
 root=Path.cwd();out=a.out.resolve();out.mkdir(parents=True,exist_ok=False);bins=out/'bins';bins.mkdir()
 manifest=dict(schema=1,status='INCOMPLETE',refs={'baseline':BASE,'candidate':HEAD},cache=a.cache,cohort=a.cohort,rtt=a.rtt,cells={},pairs_completed=0,noise_method=policy.NOISE_METHOD,run_url=os.environ['FETCH_MEASURE_RUN_URL'],tooling_ref=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),proof='historical same-member offset/mod/length/checksum and unchanged fetched timestamp bracket distinct preconnected client requests; no cache-hit observer')
 def save():(out/'manifest.json').write_text(json.dumps(manifest,indent=2,allow_nan=False)+'\n')
 save()
 code=root/'bench/fetch-measure/cache-driver-build';code.mkdir();(code/'main.go').write_bytes((root/'bench/fetch-measure/cache_driver.go.txt').read_bytes())
 try:subprocess.run(['go','build','-o',str(bins/'driver'),'./bench/fetch-measure/cache-driver-build'],check=True)
 finally:shutil.rmtree(code)
 for side,ref in manifest['refs'].items():
  checkout=Path(os.environ['RUNNER_TEMP'])/f'cache-{side}';subprocess.run(['git','worktree','add','--detach',str(checkout),ref],check=True);prepare(checkout,out,side,root);subprocess.run(['go','build','-o',str(bins/side),'./cmd/tigerstore'],cwd=checkout,check=True)
 for number in range(1,6):
  order='AB' if number%2 else 'BA';results={}
  for side in (('baseline','candidate') if order=='AB' else ('candidate','baseline')):
   dest=out/f'{number:02}-{side}';dest.mkdir()
   try:raw=experiment(a,dest,bins/side,bins/'driver',number,side)
   except Exception as e:manifest.update(status='INCOMPLETE',reasons=[str(e)]);save();raise
   if raw.get('complete') is not True or len(raw['probes'])!=18:raise ValueError('missing cache probe inventory')
   result={}
   for p in raw['probes']:
    key=f'{p["Size"]}bytes-{p["Clients"]}clients-{p["Delay"]}ms'
    try:
     result[key]=adapt(p,side,a.cache,560<<20 if a.cache=='past-ram-560m' else 3<<30)
     result[key].update(raw_file=str((dest/'raw.json').relative_to(out)),raw_sha256=hashlib.sha256((dest/'raw.json').read_bytes()).hexdigest())
    except (ValueError,KeyError,TypeError) as e:
     manifest.update(status='INCOMPLETE',reasons=[f'{side} {key}: {e}']);save();return 2
   results[side]=result
   manifest['partial_pair']=dict(pair=number,order=order,side=side,raw_file=str((dest/'raw.json').relative_to(out)),raw_sha256=hashlib.sha256((dest/'raw.json').read_bytes()).hexdigest());save()
  if set(results['baseline'])!=set(results['candidate']):raise ValueError('unmatched cache probes')
  for key in results['baseline']:
   cell=manifest['cells'].setdefault(key,dict(pairs=[]));cell['pairs'].append(dict(pair=number,order=order,baseline=results['baseline'][key],candidate=results['candidate'][key]));cell['report']=policy.assess(cell['pairs'])
  manifest['pairs_completed']=number;manifest.pop('partial_pair',None)
  failed=[key for key,c in manifest['cells'].items() if c['report']['status']=='RULING']
  manifest['status']='RULING' if failed else ('PASS' if number==5 else 'INCOMPLETE');manifest['failed_cells']=failed;save()
  if failed:return 1
 return 0

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--cache',choices=CACHES,required=True);p.add_argument('--cohort',choices=COHORTS,required=True);p.add_argument('--rtt',type=int,choices=(0,2,10),required=True);p.add_argument('--etcdctl',required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args();raise SystemExit(run(a))
