#!/usr/bin/env python3
"""Summarize sanitized recorder JSONL. Percentiles are nearest-rank.

Calls table counts client request *messages*, including messages on persistent
Watch/LeaseKeepAlive streams. rpc_starts counts transport RPCs separately.
In-flight percentiles are exact time-weighted state durations, not per-call
samples; both all RPCs and non-streaming RPCs are reported. Writes are successful
puts/deletes only, with sizes for puts. Values never enter this process.
"""
import collections as C
import gzip
import json
import math
from pathlib import Path
import sys


def prefix(key):
    p = key.split('/')
    if len(p) >= 3 and p[1] == 'registry':
        # API groups such as leases/events are stored in the registry prefix name.
        return '/registry/' + p[2] + '/'
    return key


def stats(a):
    a = sorted(a)
    if not a:
        return {'n': 0, 'mean': None, 'p50': None, 'p99': None}
    return {'n': len(a), 'mean': sum(a)/len(a), 'p50': a[math.ceil(.5*len(a))-1], 'p99': a[math.ceil(.99*len(a))-1]}


def weighted(hist):
    total = sum(hist.values())
    def q(p):
        v = 0
        for n, w in sorted(hist.items()):
            v += w
            if v >= p*total:
                return n
        return 0
    return {'max': max((n for n, w in hist.items() if w > 0), default=0), 'p50': q(.5), 'p99': q(.99), 'mean': sum(n*w for n,w in hist.items())/total if total else 0}


def request_shape(method, d):
    if method == 'Range':
        if d.get('count_only'): return 'Range count_only'
        if not d.get('range_end'): return 'Range single limit=' + str(d.get('limit', 0))
        lim = d.get('limit', 0)
        return 'Range prefix limit=' + ('1 probe' if lim == 1 else '>1 page' if lim > 1 else '0 unbounded')
    if method == 'Txn':
        comp = ','.join(c['target'] + ':' + c['result'] for c in d.get('compare', [])) or 'none'
        def ops(a):
            return ','.join(('put+lease' if x['request_put'].get('lease') else 'put') if 'request_put' in x else 'delete' if 'request_delete_range' in x else 'range' if 'request_range' in x else 'txn' for x in a) or 'none'
        return f"Txn {comp} success={ops(d.get('success', []))} failure={ops(d.get('failure', []))}"
    if method == 'Watch':
        return 'Watch ' + next((k.replace('_request', '') for k in ('create_request','cancel_request','progress_request') if k in d), 'other')
    if method == 'LeaseKeepAlive': return 'LeaseKeepAlive message'
    return method


def primary_keys(method, d):
    if 'key' in d: return [d['key']]
    if method == 'Txn':
        keys = [c['key'] for c in d.get('compare', [])]
        if not keys:
            for op in d.get('success', []):
                for k in ('request_put', 'request_range', 'request_delete_range'):
                    if k in op: keys.append(op[k]['key'])
        return list(set(keys))
    if 'create_request' in d: return [d['create_request']['key']]
    return []


def summarize(events, start, end):
    duration = end-start
    shapes, methods, rpcstarts, errors = C.Counter(), C.Counter(), C.Counter(), C.Counter()
    pending = {}
    all_conns, open_conns, seen_conns = set(), set(), set()
    active, hist_all, hist_unary, hist_conn = {}, C.Counter(), C.Counter(), C.Counter()
    prev = start
    resources = C.defaultdict(lambda: {'calls': C.Counter(), 'puts': [], 'deletes': 0, 'read_bytes': [], 'events': 0, 'event_bytes': 0, 'watch_creates': 0, 'progress_responses': 0})
    writes = C.defaultdict(list)
    range_keys = C.Counter()
    txns = C.Counter()
    compacts = []
    streams = set()
    watch_defs = {}
    watch_pending = C.defaultdict(list)
    requests_in_window = set()
    incomplete = 0
    def put(d,t):
        k=d['key']; resources[prefix(k)]['puts'].append(d.get('value_bytes',0));writes[k].append(t)
    def delete(d): resources[prefix(d['key'])]['deletes'] += 1
    def op_response(op, response, t):
        if 'request_put' in op: put(op['request_put'],t)
        if 'request_delete_range' in op: delete(op['request_delete_range'])
        if 'request_range' in op:
            rr=response.get('response_range',{});resources[prefix(op['request_range']['key'])]['read_bytes'].append(rr.get('_bytes',0))
    for e in events:
        t=e['t']; phase=e['phase']; cid=e.get('call'); method=e.get('method','').split('/')[-1];d=e.get('data',{})
        inside=start <= t < end
        if t >= start:
            stop=min(t,end)
            dt=max(0,stop-prev)
            hist_all[len(active)] += dt
            hist_unary[sum(x not in ('Watch','LeaseKeepAlive') for x in active.values())] += dt
            hist_conn[len(open_conns)] += dt
            prev=stop
        if t >= end: break
        if phase=='conn_open':
            open_conns.add(e['conn']);all_conns.add(e['conn'])
        if phase=='conn_close': open_conns.discard(e['conn'])
        if inside:
            seen_conns.update(open_conns)
            if e.get('conn'): seen_conns.add(e['conn'])
        if phase=='start':
            active[cid]=method
            if inside: rpcstarts[method]+=1
        if phase=='end':
            active.pop(cid,None)
            if inside and e.get('error'): errors[method+': '+e['error']]+=1
        if method=='Watch' and phase=='start': streams.add(cid)
        if phase=='request':
            if method not in ('Watch','LeaseKeepAlive'): pending[cid]=(method,d,t)
            if method=='Watch' and 'create_request' in d:
                watch_pending[cid].append(d['create_request'])
                if inside: resources[prefix(d['create_request']['key'])]['watch_creates']+=1
            if inside:
                requests_in_window.add(cid);methods[method]+=1;shapes[request_shape(method,d)]+=1
                for p in set(prefix(k) for k in primary_keys(method,d)):
                    resources[p]['calls'][method]+=1
                if method=='Range' and d.get('range_end'):
                    range_keys[(d['key'],d.get('limit',0),bool(d.get('count_only')))]+=1
                if method=='Compaction' or method=='Compact': compacts.append({'t':t,'revision':d.get('revision'),'physical':d.get('physical')})
        if phase=='response':
            if method=='Watch':
                wid=d.get('watch_id',0)
                if d.get('created') and watch_pending[cid]: watch_defs[(cid,wid)]=watch_pending[cid].pop(0)
                if inside:
                    for event in d.get('events',[]):
                        p=prefix(event.get('kv',{}).get('key','unknown'));resources[p]['events']+=1;resources[p]['event_bytes']+=event.get('_bytes',0)
                    if not d.get('events') and not d.get('created') and not d.get('canceled'):
                        p=prefix(watch_defs.get((cid,wid),{}).get('key','unknown'));resources[p]['progress_responses']+=1
            elif method!='LeaseKeepAlive' and cid in pending:
                _,req,reqt=pending.pop(cid)
                if inside:
                    if method=='Range': resources[prefix(req['key'])]['read_bytes'].append(e['bytes'])
                    elif method=='Put': put(req,t)
                    elif method=='DeleteRange': delete(req)
                    elif method=='Txn':
                        txns['responses']+=1;txns['succeeded' if d.get('succeeded') else 'failed']+=1
                        branch=req.get('success' if d.get('succeeded') else 'failure',[])
                        for i,op in enumerate(branch): op_response(op,(d.get('responses',[])+[{}]*len(branch))[i],t)
    dt=max(0,end-prev)
    hist_all[len(active)]+=dt;hist_unary[sum(x not in ('Watch','LeaseKeepAlive') for x in active.values())]+=dt;hist_conn[len(open_conns)]+=dt
    top=sorted((len(ts) for ts in writes.values()),reverse=True);nw=sum(top)
    intervals=[b-a for ts in writes.values() for a,b in zip(ts,ts[1:])]
    outresources={}
    for p,r in sorted(resources.items()):
        outresources[p]={'calls':dict(r['calls']),'put_count':len(r['puts']),'delete_count':r['deletes'],'writes_per_s':(len(r['puts'])+r['deletes'])/duration,'put_value_bytes':stats(r['puts']),'read_response_bytes':stats(r['read_bytes']),'watch_events_per_s':r['events']/duration,'watch_event_bytes':r['event_bytes'],'watch_events':r['events'],'watch_creates':r['watch_creates'],'watch_progress_responses':r['progress_responses']}
    # Reconstruct active watches at the START independently. A create in boot
    # need not recur in a steady window, so do not confuse zero new with none.
    wactive={}; wqueue=C.defaultdict(list); wstreams=set(); active_at_start={};active_streams_at_start=[]
    for e in events:
        if e['t']>=start: break
        if e.get('method','').endswith('/Watch'):
            c=e.get('call');d=e.get('data',{})
            if e['phase']=='start':wstreams.add(c)
            if e['phase']=='request' and 'create_request' in d:wqueue[c].append(d['create_request'])
            if e['phase']=='response' and d.get('created') and wqueue[c]:wactive[(c,d.get('watch_id',0))]=wqueue[c].pop(0)
            if e['phase']=='response' and d.get('canceled'):wactive.pop((c,d.get('watch_id',0)),None)
            if e['phase']=='end':
                wstreams.discard(c);wactive={k:v for k,v in wactive.items() if k[0]!=c}
    active_at_start=dict(C.Counter(prefix(v['key']) for v in wactive.values()))
    total=sum(shapes.values())
    return {'duration_s':duration,'request_messages':total,'request_messages_per_s':total/duration,'rpc_starts':dict(rpcstarts),'rpc_starts_per_s':sum(rpcstarts.values())/duration,'methods':dict(methods),'shapes':{k:{'n':v,'share_pct':100*v/total} for k,v in shapes.most_common()},'resources':outresources,'key_popularity':{'distinct_keys_put':len(writes),'successful_puts':nw,'top_1pct_keys_put_share_pct':100*sum(top[:math.ceil(.01*len(top))])/nw if nw else 0,'top_10pct_keys_put_share_pct':100*sum(top[:math.ceil(.1*len(top))])/nw if nw else 0,'repeat_put_intervals_s':stats(intervals),'per_key_put_count':dict(sorted((k,len(ts)) for k,ts in writes.items()))},'ranges': [{'key':k,'limit':l,'count_only':c,'n':n} for (k,l,c),n in range_keys.most_common()],'connections_seen':len(seen_conns),'connection_concurrency':weighted(hist_conn),'inflight_all_rpc':weighted(hist_all),'inflight_unary':weighted(hist_unary),'inflight_hist_duration_s':dict(hist_all),'watch_streams_at_start':len(wstreams),'watch_stream_starts':rpcstarts['Watch'],'active_watches_at_start':active_at_start,'watch_progress_requests':shapes['Watch progress'],'compactions':compacts,'txn_results':dict(txns),'txn_failure_pct':100*txns['failed']/txns['responses'] if txns['responses'] else 0,'rpc_errors':dict(errors)}


def markdown(name,s):
    lines=[f'## {name}',f"{s['duration_s']:.2f}s; {s['request_messages']} request messages ({s['request_messages_per_s']:.3f}/s); {s['rpc_starts_per_s']:.3f} new RPCs/s.",'','| Request shape | Count | Share |','|---|---:|---:|']
    for k,v in s['shapes'].items():lines.append(f"| {k} | {v['n']} | {v['share_pct']:.3f}% |")
    lines+=['','| Resource prefix | Puts | Deletes | Writes/s | Value B mean / p50 / p99 | Read response B mean | Watch events/s |','|---|---:|---:|---:|---:|---:|---:|']
    for p,r in s['resources'].items():
        z=r['put_value_bytes'];read=r['read_response_bytes']['mean'];zstr=' / '.join('-' if z[k] is None else f'{z[k]:.1f}' for k in ('mean','p50','p99'))
        lines.append(f"| {p} | {r['put_count']} | {r['delete_count']} | {r['writes_per_s']:.3f} | {zstr} | {read if read is not None else '-'} | {r['watch_events_per_s']:.3f} |")
    kp=s['key_popularity'];iv=kp['repeat_put_intervals_s']
    lines+=['',f"Put key popularity: {kp['distinct_keys_put']} distinct keys; top 1% / 10% receive {kp['top_1pct_keys_put_share_pct']:.2f}% / {kp['top_10pct_keys_put_share_pct']:.2f}% of puts. Repeat intervals: {iv}.",f"Connections: {s['connections_seen']}; in-flight all RPCs {s['inflight_all_rpc']}; unary {s['inflight_unary']}.",f"Watch streams at start / started: {s['watch_streams_at_start']} / {s['watch_stream_starts']}; progress requests {s['watch_progress_requests']}.",f"Compactions: {s['compactions']}. Txn CAS failure: {s['txn_results']} ({s['txn_failure_pct']:.3f}%).",'','Full per-prefix call kinds, LIST/probe keys, watches and errors are in the JSON summary.','']
    return '\n'.join(lines)


def main():
    log,windows,out=map(Path,sys.argv[1:]); opener=gzip.open if log.suffix=='.gz' else open
    with opener(log,'rt') as f: events=[json.loads(l) for l in f if l.strip()]
    events.sort(key=lambda e:e['t'])
    bounds={}
    for l in windows.read_text().splitlines():
        w=json.loads(l);bounds.setdefault(w['scenario'],{})[w['phase']]=w['t']
    for name,b in bounds.items():
        if 'end' not in b: b['end']=events[-1]['t']
        s=summarize(events,b['start'],b['end']);s['start_unix']=b['start'];s['end_unix']=b['end']
        (out/f'{name}-summary.json').write_text(json.dumps(s,indent=2)+'\n')
        (out/f'{name}-summary.md').write_text(markdown(name,s))

if __name__=='__main__':main()
