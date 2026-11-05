#!/usr/bin/env python3
"""Derive quantities exclusively from paired readouts; preserve every source."""
import collections, json, pathlib, statistics, sys
root=pathlib.Path(sys.argv[1]); groups=collections.defaultdict(list); derived=[]
def metric(n,k): return n['metrics'].get(k,0)
def io(text): return {k:int(v) for k,v in (line.split(':') for line in text.splitlines())}
def disk(text,name):
 for line in text.splitlines():
  f=line.split()
  if f[2]==name: return list(map(int,f[3:]))
 return []
for p in sorted(root.glob('*/result.json')):
 cfg=p.parent.name; wl,place,rtt,variant,rep=cfg.split('-')
 r=json.loads(p.read_text()); assert r['valid'],p
 d=dict(config=cfg,workload=wl,place=place,rtt=int(rtt),variant=variant,rep=int(rep),ops=r['throughput'],p50_ms=r['latency_us']['p50']/1000,mean_ms=r['latency_us']['mean']/1000,leader_core=r['server_cores_by_node'][0],f1_core=r['server_cores_by_node'][1],f2_core=r['server_cores_by_node'][2],client_core=r['client_cores'],leader_us=r['server_cpu_us_per_op_by_node'][0],follower_us=r['server_cpu_us_per_op_by_node'][1],device_B_op=r.get('disk_bytes_per_op',0),nodes=[])
 samples=json.loads((p.parent/'samples.json').read_text()); a,b=samples; dt=b['at']-a['at']
 # Parent device stats include flush statistics; locate root's partition and parent in snapshots.
 for name in ['sda','sda1']:
  x,y=disk(a['diskstats'],name),disk(b['diskstats'],name)
  if x and y:
   dx=[j-i for i,j in zip(x,y)]
   d[name+'_MiB_s']=dx[6]*512/dt/2**20
   d[name+'_util_pct']=dx[9]/dt/10
   d[name+'_write_await_ms']=dx[7]/dx[4] if dx[4] else 0
   d[name+'_write_IOPS']=dx[4]/dt
   if len(dx)>=17: d[name+'_flush_await_ms']=dx[16]/dx[15] if dx[15] else 0
 for i,(x,y) in enumerate(zip(a['nodes'],b['nodes'])):
  t=y['at']-x['at']; fs=metric(y,'etcd_disk_wal_fsync_duration_seconds_count')-metric(x,'etcd_disk_wal_fsync_duration_seconds_count')
  byte=metric(y,'etcd_disk_wal_write_bytes_total')-metric(x,'etcd_disk_wal_write_bytes_total')
  ops=metric(y,'etcd_debugging_mvcc_current_revision')-metric(x,'etcd_debugging_mvcc_current_revision')
  writes=metric(y,'etcd_disk_wal_write_duration_seconds_count')-metric(x,'etcd_disk_wal_write_duration_seconds_count')
  nd={'applied_ops_s':ops/t,'ops_fsync':ops/fs if fs else 0,'fsync_s':fs/t,'wal_B_s':byte/t,'wal_B_fsync':byte/fs if fs else 0,'fsync_mean_ms':1000*(metric(y,'etcd_disk_wal_fsync_duration_seconds_sum')-metric(x,'etcd_disk_wal_fsync_duration_seconds_sum'))/fs if fs else 0,'wal_B_op':byte/ops if ops else 0,'wal_writes_s':writes/t,'wal_B_write':byte/writes if writes else 0,'wal_write_mean_us':1e6*(metric(y,'etcd_disk_wal_write_duration_seconds_sum')-metric(x,'etcd_disk_wal_write_duration_seconds_sum'))/writes if writes else 0}
  ix,iy=io(x['io']),io(y['io'])
  for k in ix: nd[k+'_s']=(iy[k]-ix[k])/t
  gx,gy=x['vars'].get('ceiling_gap',{}),y['vars'].get('ceiling_gap',{})
  dg={k:gy.get(k,0)-gx.get(k,0) for k in gy}; count=dg.get('groups',0)
  if count:
   nd['persist_groups_s']=count/t
   nd['stages_us']={k.removesuffix('_ns'):v/count/1000 for k,v in dg.items() if k.endswith('_ns')}
  d['nodes'].append(nd)
 progress=json.loads((p.parent/'progress.json').read_text()); counts=[]; full=[]
 for s in progress:
  pr=s['vars'].get('ceiling_gap_progress',{})
  lead=s['vars'].get('raft.status',{}).get('id')
  for id,v in pr.items():
   if id==lead: continue
   counts.append(v['inflight_count']); full.append(v['full'])
 d['inflight_max']=max(counts,default=0); d['inflight_full_fraction']=statistics.mean(full) if full else 0
 derived.append(d); groups[(wl,place,int(rtt),variant)].append(d)
(root/'derived.json').write_text(json.dumps(derived,indent=2))
def val(v,fmt='.2f'):
 return f'{statistics.median(v):{fmt}} [{min(v):{fmt}},{max(v):{fmt}}]'
print('# Ceiling-gap: median [min,max], three repetitions on this runner')
print('| workload | layout | RTT | variant | ops/s | p50 ms | leader cores | f1 cores | f2 cores | generator cores | device B/op |')
print('|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|')
for key,rows in sorted(groups.items()):
 print('| '+' | '.join(map(str,key))+' | '+' | '.join(val([r[k] for r in rows], '.0f' if k in ['ops','device_B_op'] else '.3f') for k in ['ops','p50_ms','leader_core','f1_core','f2_core','client_core','device_B_op'])+' |')
print('\n## Leader disk/persistence (eight-second in-load subwindow)')
print('| workload | layout | RTT | variant | fsync/s | ms/fsync | WAL B/fsync | device MiB/s | device util % | in-flight max |')
print('|---|---|---:|---|---:|---:|---:|---:|---:|---:|')
for key,rows in sorted(groups.items()):
 print('| '+' | '.join(map(str,key))+' | '+' | '.join([val([r['nodes'][0][k] for r in rows]) for k in ['fsync_s','fsync_mean_ms','wal_B_fsync']]+[val([r.get(k,0) for r in rows]) for k in ['sda_MiB_s','sda_util_pct','inflight_max']])+' |')
print('\nRaw JSON contains every node\'s fsync rate/mean latency, WAL bytes, /proc/io rates, group-stage means and Raft window samples. Group-stage means overlap and are NOT an additive request-latency decomposition. Profiles cover t=5..13 from generator launch; whole-load CPU/ops cover its 14 s window after 3 s warmup. Diagnostic sampler runs on generator CPU; server timing/block instrumentation is enabled except pure controls. No disk benchmarks run on the development host.')
