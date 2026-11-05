#!/usr/bin/env python3
"""Exclusive stack classification, same steps as bench/profile.sh.
CPU us/op = full-window process CPU/ops * diagnostic-window sampled share.
This is not wall-time latency. Raw profiles remain authoritative.
"""
import collections,json,pathlib,re,statistics,sys
steps=[
 ('client TLS read',r'crypto/tls\.\(\*Conn\)\.Read',r'internal/peer'),
 ('HTTP2/read body',r'server\.\(\*conn\)\.readLoop',r'crypto/tls\.\(\*Conn\)\.Read|server\.\(\*conn\)\.(admit|flush)'),
 ('decode/batch',r'server\.\(\*conn\)\.(admit|flush)',r'^$'),
 ('committer checks/queue',r'node\.\(\*Node\)\.handle',r'^$'),
 ('entry encode',r'core\.encodeBatch',r'^$'),
 ('apply/reads',r'core\.\(\*Node\)\.(runApply|serveRead)',r'^$'),
 ('completions',r'node\.completeAll',r'^$'),
 ('Raft/committer rest',r'node\.\(\*Node\)\.commit',r'node\.\(\*Node\)\.handle|core.encodeBatch|core\.\(\*Node\)\.(runApply|serveRead)|node.completeAll'),
 ('WAL write/sync',r'node\.\(\*Node\)\.(persist|syncGroups)',r'^$'),
 ('peer send',r'peer\.\(\*sender\)',r'^$'),
 ('peer receive',r'peer\.\(\*TCP\)\.(receive|frames|accept)',r'^$'),
 ('reply encode',r'server\.\(\*conn\)\.completeWaiting',r'^$'),
 ('reply TLS/write',r'server\.\(\*conn\)\.writeLoop',r'server\.\(\*conn\)\.completeWaiting'),
 ('GC',r'runtime.gcBgMarkWorker|runtime.bgsweep|runtime.bgscavenge',r'^$'),
]
steps=[(n,re.compile(f),re.compile(i)) for n,f,i in steps]
def parse(path):
 text=path.read_text(); sampletext,locs=text.split('Locations\n',1); locs=locs.split('Mappings\n')[0]
 locations={}; cur=None
 for line in locs.splitlines():
  m=re.match(r'\s*(\d+):',line)
  if m: cur=m[1];locations[cur]=line
  elif cur: locations[cur]+='\n'+line
 result=collections.Counter(); total=0
 for line in sampletext.splitlines():
  m=re.match(r'\s*(\d+)\s+(\d+):\s+([\d ]+)',line)
  if not m: continue
  ns=int(m[2]); total+=ns; stack='\n'.join(locations.get(id,'') for id in m[3].split()); name='other'
  for n,f,i in steps:
   if f.search(stack) and not i.search(stack): name=n;break
  result[name]+=ns
 return {k:v/total for k,v in result.items()},total
allrows=[]; group=collections.defaultdict(list)
for root in map(pathlib.Path,sys.argv[1:]):
 for p in sorted(root.glob('*/result.json')):
  r=json.loads(p.read_text()); cfg=p.parent.name; wl,place,rtt,variant,rep=cfg.split('-')
  for node,idx in [('leader',0),('follower',1)]:
   raw=p.parent/(node+'.pprof.raw.txt')
   if not raw.exists(): continue
   frac,total=parse(raw)
   row={'runner':root.name,'config':cfg,'node':node,'sampled_ns':total,'cpu_us_op':r['server_cpu_us_per_op_by_node'][idx],'steps_us_op':{k:v*r['server_cpu_us_per_op_by_node'][idx] for k,v in frac.items()},'shares':frac}
   allrows.append(row);group[(wl,place,int(rtt),variant,node)].append(row)
(pathlib.Path(sys.argv[1])/'profile-derived.json').write_text(json.dumps(allrows,indent=2))
print('| workload | layout | RTT | variant | node | top steps us/op (median [min,max]) |')
print('|---|---|---:|---|---|---|')
for key,rows in sorted(group.items()):
 names=set(k for r in rows for k in r['steps_us_op'])
 def v(k): return [r['steps_us_op'].get(k,0) for r in rows]
 top=sorted(names,key=lambda k:statistics.median(v(k)),reverse=True)[:6]
 def fmt(k):
  vals=v(k);return f'{k} {statistics.median(vals):.2f} [{min(vals):.2f},{max(vals):.2f}]'
 print('| '+' | '.join(map(str,key))+' | '+'; '.join(map(fmt,top))+' |')
