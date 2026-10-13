#!/usr/bin/env python3
"""Summarize event clocks. Raw events remain the source of truth.

Times are process clock timestamps on the same VM. Fsync intervals include
scheduler delay before the goroutine resumes, not only kernel device time.
"""
import bisect
from collections import Counter, defaultdict
import gzip
import json
from pathlib import Path
import statistics


def dist(xs):
    xs=sorted(xs)
    if not xs:
        return {'n':0}
    return {'n':len(xs),'mean':statistics.mean(xs),'p50':xs[len(xs)//2],
            'p90':xs[min(len(xs)-1,int(len(xs)*.9))],
            'p99':xs[min(len(xs)-1,int(len(xs)*.99))],'max':xs[-1]}


def analyze(p):
    data=json.loads(gzip.open(p,'rt').read())
    assert data['dropped']==0, p
    es=sorted(data['events'],key=lambda e:e['ns'])
    by=defaultdict(list)
    for e in es:
        for k in 'abcd': e.setdefault(k,0)
        by[e['k']].append(e)
    pairs={}
    for label,start,end,key in [('process','process_begin','process_end',None),
                                ('apply','apply_begin','apply_end',None),
                                ('fsync','fsync_begin','fsync_end',None),
                                ('persist','persist_start','persist_end','a'),
                                ('completion','persist_end','completion','a')]:
        if key is None:
            spans=list(zip(by[start],by[end]))
        else:
            ends={e[key]:e for e in by[end]}
            spans=[(e,ends[e[key]]) for e in by[start] if e[key] in ends]
        pairs[label]=spans
    arrivals=[e['ns'] for e in by['arrival']]
    def count(a,b,ts=arrivals): return bisect.bisect_left(ts,b)-bisect.bisect_left(ts,a)
    syncs=pairs['fsync']
    sync_arrivals=[count(a['ns'],b['ns']) for a,b in syncs]
    wall=(es[-1]['ns']-es[0]['ns'])/1e9
    groups={e['a']:dict(ns=e['ns'],entries=[],ids=[]) for e in by['group']}
    index_group={}
    for e in by['group_entry']:
        if e['a'] in groups:
            groups[e['a']]['entries'].append(e['b'])
            index_group[e['b']]=e['a']
    applied={e['a']:e for e in by['apply_req']}
    for e in by['apply_req']:
        if e['b'] in index_group:
            groups[index_group[e['b']]]['ids'].append(e['a'])
    stages={k:{e['a']:e for e in by[k]} for k in ('arrival','submit','apply_req','reply','do_reply','persist_end','completion')}
    timings={}
    for name,a,b in [('arrival_submit','arrival','submit'),('submit_apply','submit','apply_req'),('apply_reply','apply_req','reply'),('reply_do','reply','do_reply')]:
        timings[name]=dist([(stages[b][i]['ns']-e['ns'])/1e3 for i,e in stages[a].items() if i in stages[b]])
    for label, spans in pairs.items():
        timings[label]=dist([(b['ns']-a['ns'])/1e3 for a,b in spans])
    begin=by['grpc_begin']
    # Count admissions inside intervals rather than infer them from total concurrency.
    grpc_ts=[e['ns'] for e in begin]
    grpc_during=sum(count(a['ns'],b['ns'],grpc_ts) for a,b in syncs)
    result={'file':str(p),'wall_s':wall,'counts':dict(Counter(e['k'] for e in es)),
      'timing_us':timings,'arrivals_during_fsync':sum(sync_arrivals),
      'grpc_begins_during_fsync':grpc_during,'fsync_arrivals':dist(sync_arrivals),
      'fsync_no_arrivals_fraction':sum(x==0 for x in sync_arrivals)/len(sync_arrivals) if sync_arrivals else 0,
      'fsync_wall_s':sum((b['ns']-a['ns'])/1e9 for a,b in syncs),
      'group_requests':dist([len(g['ids']) for g in groups.values()]),
      'empty_entry_groups':sum(not g['entries'] for g in groups.values()),
      'group_entries':dist([len(g['entries']) for g in groups.values()]),
      'completion_inbox':dist([e['b'] for e in by['completion']]),
      'completion_deferred':dist([e['c'] for e in by['completion']]),
      'arrival_inbox':dist([e['b'] for e in by['arrival']]),
      'grpc_active_max':max((e['c'] for e in begin),default=0),
      'grpc_conn_active_max':max((e['d'] for e in begin),default=0)}
    (p.parent/(p.name.replace('.json.gz','-summary.json'))).write_text(json.dumps(result,indent=2)+'\n')
    return result

if __name__=='__main__':
    for p in sorted(Path('evidence/pre47batch256').glob('*/c*/*/n*-trace.json.gz')):
        r=analyze(p)
        print(p,'groups',r['group_requests'],'sync_us',r['timing_us']['fsync'],
              'arrivals_in_sync',r['arrivals_during_fsync'],'sync_wall',r['fsync_wall_s'],
              'active',r['grpc_active_max'])
