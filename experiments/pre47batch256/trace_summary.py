#!/usr/bin/env python3
"""Summarize event clocks. Raw events remain the source of truth.

Times are process clock timestamps on the same VM. Fsync intervals include
scheduler delay before the goroutine resumes, not only kernel device time.
"""
import bisect
from collections import Counter, defaultdict, deque
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
    send_queues=defaultdict(deque)
    send_delays=[]
    for e in es:
        key=tuple(e[k] for k in 'abcd')
        if e['k']=='send': send_queues[key].append(e['ns'])
        elif e['k']=='peer_write' and send_queues[key]:
            send_delays.append((e['ns']-send_queues[key].popleft())/1e3)
    timings['send_to_peer_write']=dist(send_delays)
    ack_indexes=[]; ack_times=[]; high=0
    for e in by['recv']:
        if e['a']==4 and e['c']>high:  # raftpb.MsgAppResp, healthy runs only.
            high=e['c']; ack_indexes.append(high); ack_times.append(e['ns'])
    ack_after_persist=[]; apply_after_persist=[]; later_groups_before_ack=0; later_groups_before_apply=0; checked=0
    gs=sorted(groups.items())
    for pos,(gid,g) in enumerate(gs[:-1]):
        if not g['entries'] or gid not in stages['persist_end'] or not g['ids']: continue
        ai=bisect.bisect_left(ack_indexes,max(g['entries']))
        if ai==len(ack_times): continue
        ack=ack_times[ai]
        applied_ns=min(applied[i]['ns'] for i in g['ids'])
        durable=stages['persist_end'][gid]['ns']
        ack_after_persist.append((ack-durable)/1e3)
        apply_after_persist.append((applied_ns-durable)/1e3)
        checked+=1
        later_groups_before_ack+=gs[pos+1][1]['ns'] < ack
        later_groups_before_apply+=gs[pos+1][1]['ns'] < applied_ns
    timings['first_follower_ack_minus_local_persist_end']=dist(ack_after_persist)
    timings['first_apply_minus_local_persist_end']=dist(apply_after_persist)
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
      'groups_checked_for_overlap':checked,
      'next_group_before_this_group_first_ack':later_groups_before_ack,
      'next_group_before_this_group_first_apply':later_groups_before_apply,
      'grpc_active_max':max((e['c'] for e in begin),default=0),
      'grpc_conn_active_max':max((e['d'] for e in begin),default=0)}
    (p.parent/(p.name.replace('.json.gz','-summary.json'))).write_text(json.dumps(result,indent=2)+'\n')
    # One row per physical group. Request membership comes from the existing
    # DecodeBatch at apply, joined to persisted log indexes, not a second decode.
    process_ends=[e['ns'] for e in by['process_end']]
    commit_events=[e for e in by['ready'] if e['a']]
    commit_indexes=[e['a'] for e in commit_events]
    group_stages={k:{e['a']:e for e in by[k]} for k in ('persist_start','persist_end','completion','group','disk_enqueue')}
    with gzip.open(p.with_name(p.name.replace('-trace.json.gz','-groups.jsonl.gz')),'wt') as output:
        for gid,g in gs:
            row={'group':gid,'created_ns':g['ns'],'indexes':g['entries'],'requests':len(g['ids'])}
            pi=bisect.bisect_right(process_ends,g['ns'])-1
            if pi>=0 and pi<len(by['process_begin']):
                row['process_begin_ns']=by['process_begin'][pi]['ns']; row['process_end_ns']=process_ends[pi]
                row['ready']=[{'ns':e['ns'],'commit':e['a']} for e in by['ready'] if row['process_begin_ns']<=e['ns']<=row['process_end_ns']]
            for k,lookup in group_stages.items():
                if gid in lookup: row[k]=lookup[gid]
            for k in ('arrival','submit','apply_req','reply','do_reply'):
                found=[stages[k][i]['ns'] for i in g['ids'] if i in stages[k]]
                if found: row[k+'_first_ns']=min(found); row[k+'_last_ns']=max(found)
            if g['entries']:
                ai=bisect.bisect_left(ack_indexes,max(g['entries']))
                if ai<len(ack_times): row['first_follower_ack_ns']=ack_times[ai]
                ci=bisect.bisect_left(commit_indexes,max(g['entries']))
                if ci<len(commit_events): row['commit_observed_in_ready_ns']=commit_events[ci]['ns']
            if 'persist_start' in row and 'persist_end' in row:
                start=row['persist_start']['ns']; end=row['persist_end']['ns']
                row['arrivals_during_persist']=count(start,end)
            output.write(json.dumps(row,separators=(',',':'))+'\n')
    return result

if __name__=='__main__':
    for p in sorted(Path('evidence/pre47batch256').glob('*/c*/*/n*-trace.json.gz')):
        r=analyze(p)
        print(p,'groups',r['group_requests'],'sync_us',r['timing_us']['fsync'],
              'arrivals_in_sync',r['arrivals_during_fsync'],'sync_wall',r['fsync_wall_s'],
              'active',r['grpc_active_max'])
