#!/usr/bin/env python3
"""Make the dispatch manifest, including jobs that failed before a plan existed."""
import json
from pathlib import Path
import sys

here=Path(__file__).parent
metadata=json.loads(Path(sys.argv[1]).read_text())
meta={r['databaseId']:r for r in metadata}
runs=[]
closed=[f'{work}-{size}-{ingress}-c{c}' for work,size in [('put','1k'),('put','10k'),('range','1k')]
        for ingress in ['leader','balanced'] for c in [256,1024,4096]]
lists=[f'list-1k-{ingress}-c{c}' for ingress in ['leader','balanced'] for c in [1,16,64,256]]

def add(rid,folder,role,cases,targets='tigerstore etcd',rates=None,reps=5,duration='3s',profile='none',conns=18):
    m=meta[rid]
    inputs={'cases':cases,'targets':targets.split(),'rates':rates or {},'reps':reps,
            'duration':duration,'warmup':'1s','profile':profile,'conns':conns}
    runs.append({'id':rid,'folder':folder,'role':role,'commit':m['headSha'],
                 'created_at':m['createdAt'],'status':m['status'],'conclusion':m['conclusion'],
                 'url':m.get('url',f'https://github.com/djosh34/tigerstore/actions/runs/{rid}'),
                 'inputs':inputs})

add(38412800347,'closed-complete','primary-closed',closed)
add(38412803420,'lists','primary-list',lists,duration='6s')
add(38415476997,'range4096-retry','setup-retry',['range-1k-leader-c4096'])
add(38415480337,'client-controls','closed-control',[f'put-1k-leader-c{c}' for c in [256,1024,4096]],
    targets='tigerstore tigerstore-static etcd',conns=16)
add(38415945699,'cpu','profile',['put-1k-leader-c256','put-1k-leader-c4096','put-1k-balanced-c256',
    'put-1k-balanced-c4096','put-10k-leader-c256','range-1k-leader-c1024','range-1k-balanced-c1024',
    'list-1k-leader-c64','list-1k-balanced-c64'],reps=1,duration='5s',profile='cpu')
add(38415950650,'trace','profile',['put-1k-leader-c256','put-1k-leader-c4096','put-10k-leader-c256'],
    reps=1,duration='4s',profile='trace')
add(38418280627,'caps/38418280627','closed-control',['put-1k-leader-c256','put-1k-leader-c1024',
    'put-1k-leader-c4096','put-1k-balanced-c4096','put-10k-leader-c256'],targets='tigerstore cap1m cap256k etcd')
add(38421648705,'static-caps','closed-control',['put-1k-leader-c256','put-1k-leader-c4096'],
    targets='tigerstore-static cap1m-static etcd',conns=16)
add(38421653015,'inflight','closed-control',['put-1k-balanced-c1024','put-1k-balanced-c4096'],
    targets='tigerstore inflight1024 etcd')
add(38424937611,'observation-controls/38424937611','observation-control',
    ['put-1k-leader-c256','put-1k-leader-c4096','put-10k-leader-c256','range-1k-leader-c1024'],
    targets='pristine tigerstore-off tigerstore etcd')
add(38424941260,'observation-controls/38424941260','observation-control',
    ['list-1k-leader-c64','list-1k-balanced-c64'],targets='pristine tigerstore-off tigerstore etcd',duration='6s')

open_cases=json.loads((here/'open-cases.json').read_text())
matched=json.loads((here/'matched-cases.json').read_text())
extended=json.loads((here/'extended-cases.json').read_text())
old_open=[38414314087,38414453254,38414457842,38414462519,38414466511,38414470922,38414474484,38414478091,
          38414481126,38414483910,38414487584,38414490696,38414493802,38414497031,38414500773,38414504323]
old_matched=[38416417154,38416421023,38416424649,38416428166,38416431454,38416435674,38416439805,38416443586]
old_extended=[38417472751,38417476888,38417480966]
new=sorted(r['databaseId'] for r in metadata if r['headSha']=='223f8f6aad7923ff186ac77a2c9fc29b9f140c86')
assert len(new)==28, new
for ids,prefix,role in [(old_open,'open','superseded-arrival-bug'),(new[:16],'open-fixed','primary-open')]:
    i=0
    for case,rates in open_cases.items():
        for half in [0,1]:
            rid=ids[i];i+=1
            add(rid,f'{prefix}/{rid}',role,[case],rates={k:v[half*3:half*3+3] for k,v in rates.items()})
for ids,prefix,role in [(old_matched,'matched','superseded-arrival-bug'),(new[16:24],'matched-fixed','matched-open')]:
    for rid,(case,rates) in zip(ids,matched.items()):
        add(rid,f'{prefix}/{rid}',role,[case],rates={k:rates for k in ['tigerstore','etcd']})
for ids,prefix,role in [(old_extended,'extended','superseded-arrival-bug'),(new[24:27],'extended-fixed','primary-open')]:
    for rid,(case,rates) in zip(ids,extended.items()):
        add(rid,f'{prefix}/{rid}',role,[case],rates=rates)
cap_rates={'tigerstore':[95000,110000,125000],'cap1m':[95000,110000,125000],'etcd':[7000,8000,9000]}
add(38421656211,'cap-open','superseded-arrival-bug',['put-1k-leader-open'],targets='tigerstore cap1m etcd',rates=cap_rates)
add(new[27],'cap-open-fixed','open-control',['put-1k-leader-open'],targets='tigerstore cap1m etcd',rates=cap_rates)

result={'main_commit':'e5c3327e3800edf64f86ed8c1971ba7ebc1e8342',
        'etcd_commit':'68c065e562994b89e333e77b039ad066f933c586','etcd_version':'3.7.2',
        'deadline':'2026-10-13T22:00:00Z','runs':sorted(runs,key=lambda r:r['id']),
        'excluded_pilots':[{'id':38410853336,'reason':'canceled after metric parser defect'},
                           {'id':38411023412,'reason':'pilot before owner persistence-job fence'}]}
(here/'campaign.json').write_text(json.dumps(result,indent=2)+'\n')
print(len(runs),'runs')
