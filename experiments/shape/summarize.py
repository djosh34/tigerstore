#!/usr/bin/env python3
"""Offline summaries. Preserve every row; never silently drop an invalid window."""
import argparse
import csv
import json
import math
from pathlib import Path
import re
import statistics


def prometheus(text):
    values = {}
    for line in text.splitlines():
        if not line or line.startswith('#'):
            continue
        key, val, *_ = line.split()
        values[key] = float(val)
    return values


def hist_upper(bounds, counts, q):
    total = sum(counts)
    if total <= 0:
        return None
    target = q * total
    n = 0
    for b, c in zip(bounds, counts):
        n += c
        if n >= target:
            return b
    return math.inf


def disk_node(before, after, kind, nops):
    if kind == 'etcd':
        a, b = prometheus(before), prometheus(after)
        name = 'etcd_disk_wal_fsync_duration_seconds'
        count = b[name+'_count'] - a[name+'_count']
        wall = (b[name+'_sum'] - a[name+'_sum']) * 1e6
        buckets = []
        for k in b:
            if k.startswith(name+'_bucket{'):
                bound = float(re.search(r'le="([^"]+)"', k).group(1)) * 1e6
                buckets.append((bound, b[k]-a[k]))
        buckets.sort()
        cumulative = 0
        bounds, counts = [], []
        for bound, c in buckets:
            bounds.append(bound)
            counts.append(c-cumulative)
            cumulative = c
        other = {'backend_commits': b.get('etcd_disk_backend_commit_duration_seconds_count', 0)-a.get('etcd_disk_backend_commit_duration_seconds_count', 0)}
    else:
        a, b = before['files']['fsync'], after['files']['fsync']
        count = b['calls']-a['calls']
        wall = (b['wall_ns_sum']-a['wall_ns_sum']) / 1000
        bounds = [x/1000 for x in after['wall_ns_bounds']] + [math.inf]
        counts = [y-x for x,y in zip(a['wall_ns_buckets'], b['wall_ns_buckets'])]
        other = {'write_calls':after['files']['write']['calls']-before['files']['write']['calls'],
                 'write_bytes':after['files']['write']['bytes']-before['files']['write']['bytes'],
                 'write_wall_s':(after['files']['write']['wall_ns_sum']-before['files']['write']['wall_ns_sum'])/1e9,
                 'fsync_errors':b['errors']-a['errors']}
        an, bn = before.get('node') or {}, after.get('node') or {}
        if 'persist_jobs_issued' in an and 'persist_jobs_issued' in bn:
            jobs = bn['persist_jobs_issued']-an['persist_jobs_issued']
            other['persist_jobs'] = jobs
            other['jobs_per_fsync'] = jobs/count if count else None
    return {'fsyncs':count, 'ops_per_fsync':nops/count if count else None,
            'fsync_mean_us':wall/count if count else None,
            'fsync_p50_upper_us':hist_upper(bounds,counts,.5),
            'fsync_p99_upper_us':hist_upper(bounds,counts,.99),
            'fsync_wall_s':wall/1e6, **other}


def queues(samples, kind, index):
    columns = {}
    for sample in samples:
        node = sample['nodes'][index]
        if kind == 'etcd':
            p = prometheus(node)
            fields = {k:p.get(k,0) for k in ['etcd_server_proposals_pending',
                      'etcd_server_proposals_committed_total', 'etcd_server_proposals_applied_total']}
            # The first is a post-WAL.Save gauge, not the live commit index.
            fields['post_save_minus_applied'] = fields['etcd_server_proposals_committed_total']-fields['etcd_server_proposals_applied_total']
        else:
            fields = node.get('node') or {}
            fields = {k:v for k,v in fields.items() if k in ['inbox_events', 'persist_queue_batches',
                      'persist_backlog_batches', 'pending_proposals', 'queued_writes', 'queued_reads',
                      'read_index_reads', 'read_index_batches']}
            peers = node.get('peers')
            if peers is not None:
                fields['peer_data_bytes'] = sum(p['data_bytes'] for p in peers)
                fields['peer_control_bytes'] = sum(p['control_bytes'] for p in peers)
            if node.get('node_sample_age_ns',-1) >= 0:
                fields['sample_age_us'] = node['node_sample_age_ns']/1000
            if node.get('peers_sample_age_ns',-1) >= 0:
                fields['peer_sample_age_us'] = node['peers_sample_age_ns']/1000
            for p in (node.get('node') or {}).get('raft_progress',[]):
                fields[f'raft_{p["id"]}_inflight_count'] = p['inflight_count']
                fields[f'raft_{p["id"]}_inflight_full'] = int(p['inflight_full'])
                fields[f'raft_{p["id"]}_flow_paused'] = int(p['msg_app_flow_paused'])
                fields[f'raft_{p["id"]}_next_match_gap'] = p['next']-p['match']-1
        for k,v in fields.items():
            columns.setdefault(k,[]).append(v)
    return {k:{'mean':statistics.mean(v), 'max':max(v), 'last':v[-1]} for k,v in columns.items()}


def diskstats(text):
    devices = {}
    for line in text.splitlines():
        fields = line.split()
        # Whole devices only. Keep every matching device in raw summaries.
        if re.fullmatch(r'(sd[a-z]+|vd[a-z]+|nvme[0-9]+n[0-9]+)', fields[2]):
            devices[fields[2]] = list(map(int,fields[3:]))
    return devices


def disk_window(before, after, samples):
    result = {}
    a,b = diskstats(before['diskstats']),diskstats(after['diskstats'])
    seconds = after['monotonic']-before['monotonic']
    for device,x in a.items():
        y=b[device]
        inflight=[diskstats(s['diskstats'])[device][8] for s in samples]
        result[device] = {'write_mib_s':(y[6]-x[6])*512/(1<<20)/seconds,
                          'writes_s':(y[4]-x[4])/seconds,
                          'write_await_ms':(y[7]-x[7])/(y[4]-x[4]) if y[4]!=x[4] else 0,
                          'busy_fraction':(y[9]-x[9])/1000/seconds,
                          'mean_io_queue':(y[10]-x[10])/1000/seconds,
                          'sampled_inflight_max':max(inflight,default=0)}
    return result


def flatten(prefix, obj, row):
    for k,v in obj.items():
        key = f'{prefix}{k}'
        if isinstance(v,dict):
            flatten(key+'_',v,row)
        else:
            row[key] = v


def one(path):
    load = json.loads(path.read_text())
    audit = json.loads(path.with_name('audit.json').read_text()) if path.with_name('audit.json').exists() else {'valid':False}
    row = {'path':str(path.parent), 'case':path.parent.parent.name,
           'kind':load['label'], 'valid':load['valid'] and audit['valid'],
           'observation_control':audit.get('observation_control',False),
           'physical_counter_fence':audit.get('physical_counter_fence',True)}
    for k in ['ops','cohort','cohort_failed','unsent','throughput','offered_rate','client_cores','client_cpu_us_per_op']:
        row[k] = load.get(k)
    flatten('latency_',load['latency_us'],row)
    flatten('service_',load['service_latency_us'],row)
    shape=load.get('shape',{})
    for k in ['concurrency','peak_inflight','expected','success','queue_max_observed','queue_mean_sampled',
              'queue_at_arrival_stop','inflight_at_arrival_stop','through_drain_s','drain_s','cohort_throughput']:
        row[k]=shape.get(k)
    flatten('response_',shape.get('response_buffers',{}),row)
    cpu=shape.get('cpu_through_drain') or {}
    for i,v in enumerate(cpu.get('server_us_per_op_by_node',[])):
        row[f'n{i}_cohort_cpu_us_op']=v
    row['client_cohort_cpu_us_op']=cpu.get('client_us_per_op')
    for i,v in enumerate(load.get('server_cpu_us_per_op_by_node',[])):
        row[f'n{i}_window_cpu_us_op'] = v
    for i,v in enumerate(load.get('server_cores_by_node',[])):
        row[f'n{i}_window_cores'] = v
    a = json.loads(path.with_name('before.json').read_text())
    b = json.loads(path.with_name('after.json').read_text())
    samples = [json.loads(x) for x in path.with_name('samples.jsonl').read_text().splitlines()]
    if samples and 'wall_unix_ns' in samples[0]:
        samples=[s for s in samples if shape['start_unix_ns'] <= s['wall_unix_ns'] <= shape['done_unix_ns']]
    row['samples'] = len(samples)
    row['fenced_wall_s'] = b['monotonic']-a['monotonic']
    for i,(x,y) in enumerate(zip(a['proc'],b['proc'])):
        assert x['starttime']==y['starttime']
        row[f'n{i}_fenced_cpu_us_op'] = (y['utime']+y['stime']-x['utime']-x['stime'])*10000/load['cohort']
    if row['observation_control']:
        # Deliberately omit all disk/fsync/owner-queue claims from controls.
        # Their common native-applied fence does not drain all disk jobs.
        return row
    flatten('disk_', disk_window(a,b,samples),row)
    for i,(before,after) in enumerate(zip(a['nodes'],b['nodes'])):
        flatten(f'n{i}_', disk_node(before,after,load['label'],load['cohort']),row)
        flatten(f'n{i}_q_', queues(samples,load['label'],i),row)
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('root',type=Path)
    args = ap.parse_args()
    rows = []
    failures = []
    expected = {}
    failed_roots = {p.parent for p in args.root.rglob('failures.json')}
    for plan_path in list(args.root.rglob('plan.json')) + list(args.root.rglob('expected-plan.json')):
        plan = json.loads(plan_path.read_text())
        for item in plan['windows']:
            expected[str(plan_path.parent/item['path'])] = item
    paths = set(map(str,args.root.rglob('load.json'))) | set(expected)
    for name in sorted(paths):
        p=Path(name)
        try:
            row=one(p)
            row['planned']=name in expected
            row['window_valid']=row['valid']
            row['job_failed']=any(p.is_relative_to(root) for root in failed_roots)
            if row['job_failed']:
                row['valid']=False
            rows.append(row)
        except Exception as ex:
            failure={'path':str(p),'error':repr(ex)}
            failures.append(failure)
            item=expected.get(name)
            if item:
                rows.append({'path':str(p.parent),'case':item['case'],'kind':item['kind'],
                             'offered_rate':item['rate'],'valid':False,'planned':True,'error':repr(ex)})
    (args.root/'summary-errors.json').write_text(json.dumps(failures,indent=2))
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with (args.root/'windows.csv').open('w') as f:
        wr=csv.DictWriter(f,fieldnames=fields); wr.writeheader(); wr.writerows(rows)
    groups = {}
    for r in rows:
        groups.setdefault((r['case'],r['kind'],r['offered_rate']),[]).append(r)
    summary=[]
    for (case,kind,rate), values in sorted(groups.items()):
        expected_count=sum(1 for x in expected.values() if (x['case'],x['kind'],x['rate'])==(case,kind,rate))
        s={'case':case,'kind':kind,'rate':rate,'repetitions':len(values),'expected_repetitions':expected_count,
           'complete':expected_count>0 and len(values)==expected_count and all('error' not in x for x in values),
           'parsed_windows':sum('error' not in x for x in values),
           'valid_windows':sum(1 for x in values if x.get('window_valid',x['valid'])),
           'all_valid':all(x['valid'] for x in values) and expected_count>0 and len(values)==expected_count,
           'observation_control':any(x.get('observation_control',False) for x in values),
           'physical_counter_fence':all(x.get('physical_counter_fence',False) for x in values)}
        for k in fields:
            v=[x[k] for x in values if k in x and isinstance(x[k],(float,int)) and not isinstance(x[k],bool)]
            if v:
                s[k]=statistics.median(v)
                s[k+'_min']=min(v);s[k+'_max']=max(v)
        summary.append(s)
    (args.root/'summary.json').write_text(json.dumps(summary,indent=2))
    lines=['| Case | Store | Rate | n | Valid | ops/s | p50 ms | p99 ms | p99.9 ms | CPU us/op L/F1/F2 | Generator cores |',
           '| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | --- | ---: |']
    for r in summary:
        if 'throughput' not in r:
            lines.append(f'| {r["case"]} | {r["kind"]} | {r["rate"]} | {r["repetitions"]} | False | unavailable | | | | | |')
            continue
        cpu='/'.join(f'{r.get(f"n{i}_window_cpu_us_op",0):.2f}' for i in range(3))
        lines.append(f'| {r["case"]} | {r["kind"]} | {r["rate"]:g} | {r["repetitions"]} | {r["all_valid"]} | {r["throughput"]:.0f} | {r["latency_p50"]/1000:.3f} | {r["latency_p99"]/1000:.3f} | {r["latency_p999"]/1000:.3f} | {cpu} | {r["client_cores"]:.2f} |')
    (args.root/'tables.md').write_text('\n'.join(lines)+'\n')
    print('\n'.join(lines))
    if failures:
        raise SystemExit(f'{len(failures)} unparsed windows; see summary-errors.json')


if __name__ == '__main__':
    main()
