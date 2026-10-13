#!/usr/bin/env python3
"""Offline audit of replacement fixed-arrival artifacts; never launches a process.

Usage: python3 -B open-calculations.py --results ../results \
    --campaign ../repo/experiments/shape/campaign.json --output /tmp/open-audit
All planned repetitions come from the external campaign, including missing jobs.
Cell aggregates are withheld if any repetition is missing or invalid. The
tracking convention is explicit and adjustable; it is not a steady-state SLO.
"""
import argparse
import collections
import csv
import json
import math
from pathlib import Path
import re
import statistics

PIN = '223f8f6aad7923ff186ac77a2c9fc29b9f140c86'
FOLDERS = {'open-fixed', 'extended-fixed', 'matched-fixed'}
METRICS = ['cohort_tp', 'tp_ratio', 'p50_ms', 'p99_ms', 'p999_ms',
           'service_p99_ms', 'mean_admission_ms', 'drain_s', 'stop_queue',
           'stop_queue_ms', 'queue_max', 'queue_max_ms', 'queue_mean',
           'leader_cpu_us', 'f1_cpu_us', 'f2_cpu_us', 'generator_cpu_us',
           'leader_cores', 'f1_cores', 'f2_cores', 'generator_cores']


def read(path):
    return json.loads(path.read_text())


def statuses(snapshot):
    def value(obj, *keys):
        return next(int(obj[k]) for k in keys if k in obj)
    return [(value(s['Status'], 'raftIndex', 'raft_index'),
             value(s['Status'], 'raftAppliedIndex', 'raft_applied_index'),
             int(s['Status']['header']['revision']), int(s['Status']['leader']),
             value(s['Status'], 'raftTerm', 'raft_term')) for s in snapshot['status']]


def inspect(path, item, run, tracking_ratio, tracking_lag_ms):
    row = dict(run_id=run['id'], folder=run['folder'], role=run['role'],
               case=item['case'], kind=item['kind'], rep=item['rep'], rate=item['rate'],
               path=str(path), missing=not path.exists(), valid=False,
               job_failed=run['conclusion'] != 'success', problems=[])
    if row['missing']:
        row['problems'].append('missing load record')
        return row
    load = read(path)
    audit = read(path.with_name('audit.json'))
    before, after = read(path.with_name('before.json')), read(path.with_name('after.json'))
    shape = load['shape']
    checks = []
    def require(condition, description):
        if not condition:
            checks.append(description)
    require(load['label'] == item['kind'] and load['offered_rate'] == item['rate'], 'target/rate mismatch')
    require(load['duration_s'] == 3 and load['warmup_s'] == 1 and load['conns'] == 18, 'duration/warmup/connections')
    require(load['workload'] == item['case'].split('-')[0], 'workload mismatch')
    require(load['value_size'] == (10240 if '-10k-' in item['case'] else 1024), 'value size')
    require(load['keys'] == (1000 if item['case'].startswith('list-') else 4096), 'keys')
    require(shape['arrival'] == 'fixed' and shape['dispatch'] == 'global-ticket-round-robin', 'arrival/dispatch mode')
    expected = 3 * item['rate']
    require(load['cohort'] == shape['expected'] == expected, 'scheduled cohort count')
    require(shape['peak_inflight'] <= shape['concurrency'] == (256 if item['case'].startswith('list-') else 4096), 'global concurrency')
    require(audit.get('physical_counter_fence') is True and audit.get('observation_control') is False, 'wrong fence campaign')
    require(not audit['sampling_errors'], 'sampling errors')
    require(shape['response_buffers']['live_bytes_at_done'] == 0, 'live response data at done')
    require(math.isclose(shape['cohort_throughput'], shape['success'] / shape['through_drain_s'], rel_tol=1e-12), 'through-drain denominator')
    require(math.isclose(shape['drain_s'], shape['through_drain_s'] - 3, abs_tol=1e-7), 'drain interval')
    a, b = statuses(before), statuses(after)
    require(len(a) == len(b) == 3 and len(set(a)) == len(set(b)) == 1, 'replica status equality')
    require(all(x[0] == x[1] for x in a + b) and a[0][3:] == b[0][3:], 'apply/commit or leadership')
    require(b[0][2] - a[0][2] == audit['revision_delta'], 'recorded revision delta')
    require(all(x['pid'] == y['pid'] and x['starttime'] == y['starttime'] for x, y in zip(before['proc'], after['proc'])), 'process identity')
    require(before['generator_affinity'] == [3], 'generator affinity')
    for snap in [before, after]:
        require(len(snap['nodes']) == len(snap['proc']) == 3, 'node/process snapshot count')
        for i, proc in enumerate(snap['proc']):
            require(all(snap['affinity'].get(tid) == str(i) for tid in proc['schedstat']), 'server thread affinity')
        proof = snap['persistence_fence']
        if item['kind'] == 'etcd':
            require(len(proof['post_save_commit']) == 3 and all(x >= max(z[0] for z in statuses(snap)) for x in proof['post_save_commit']), 'etcd post-save fence')
            require(proof['target_applied'] == max(z[1] for z in statuses(snap)), 'etcd target fence')
        else:
            samples = proof['samples']
            require(len(samples) == 2 and all(len(sample) == 3 for sample in samples), 'owner proof count')
            require(all(y['sequence'] > x['sequence'] for x, y in zip(*samples)), 'owner proof sequence')
            for sample in samples:
                require(all(x is not None and x['sampled_unix_ns'] >= proof['fresh_after_unix_ns']
                            and x['applied'] >= proof['target_applied'] and x['revision'] == proof['target_revision']
                            and x['persist_jobs_issued'] == x['persist_jobs_completed'] for x in sample), 'owner persistence fence')
            for node in snap['nodes']:
                for op in ['write', 'fsync']:
                    counter = node['files'][op]
                    require(not counter['errors'] and sum(counter['wall_ns_buckets']) == counter['calls'], 'file counters/histogram')
    if item['kind'] != 'etcd':
        for x, y in zip(before['nodes'], after['nodes']):
            for op in ['write', 'fsync']:
                for key in ['calls', 'errors', 'bytes', 'wall_ns_sum']:
                    require(y['files'][op][key] >= x['files'][op][key], 'file counter regression')
                require(all(v >= u for u, v in zip(x['files'][op]['wall_ns_buckets'], y['files'][op]['wall_ns_buckets'])), 'histogram regression')
    endpoints = shape['endpoint_counts']
    require(sum(x['sent'] + x['unsent'] for x in endpoints) == expected, 'endpoint cohort')
    if '-balanced-' in item['case']:
        counts = [x['sent'] + x['unsent'] for x in endpoints]
        require(len(counts) == 3 and max(counts) - min(counts) <= 1, 'balanced tickets')
    else:
        require(len(endpoints) == 1, 'leader endpoint count')
    # Invalid replies are evidence, not grounds to discard the raw window.
    require(load['latency_us']['count'] == expected and not load['cohort_failed']
            and not load['unsent'] and shape['success'] == expected, 'reply cohort incomplete')
    require(not load['errors'], 'request errors')
    wanted_revision = expected if item['case'].startswith('put-') else 0
    require(audit['revision_delta'] == audit['expected'] == wanted_revision, 'revision cohort mismatch')
    require(not audit['invariant_errors'] and audit['returncode'] == 0, 'recorded invariant/process failure')
    row.update(cohort=expected, success=shape['success'], failed=load['cohort_failed'],
               unsent=load['unsent'], revision_delta=audit['revision_delta'], errors=load['errors'],
               fence_checks_passed=not any('fence' in c or 'proof' in c for c in checks),
               native_status_passed=not any('status' in c or 'leadership' in c or 'apply/commit' in c for c in checks))
    cpu = shape['cpu_through_drain']
    row.update(cohort_tp=shape['cohort_throughput'], tp_ratio=shape['cohort_throughput'] / item['rate'],
               p50_ms=load['latency_us']['p50'] / 1000, p99_ms=load['latency_us']['p99'] / 1000,
               p999_ms=load['latency_us']['p999'] / 1000,
               service_p99_ms=load['service_latency_us']['p99'] / 1000,
               mean_admission_ms=(load['latency_us']['mean'] - load['service_latency_us']['mean']) / 1000,
               drain_s=shape['drain_s'], stop_queue=shape['queue_at_arrival_stop'],
               stop_queue_ms=shape['queue_at_arrival_stop'] / item['rate'] * 1000,
               queue_max=shape['queue_max_observed'], queue_max_ms=shape['queue_max_observed'] / item['rate'] * 1000,
               queue_mean=shape['queue_mean_sampled'], leader_cpu_us=cpu['server_us_per_op_by_node'][0],
               f1_cpu_us=cpu['server_us_per_op_by_node'][1], f2_cpu_us=cpu['server_us_per_op_by_node'][2],
               generator_cpu_us=cpu['client_us_per_op'], leader_cores=load['server_cores_by_node'][0],
               f1_cores=load['server_cores_by_node'][1], f2_cores=load['server_cores_by_node'][2],
               generator_cores=load['client_cores'])
    row['problems'] = sorted(set(checks))
    row['valid'] = bool(load['valid'] and audit['valid'] and not checks)
    if load['valid'] and audit['valid'] and checks:
        raise AssertionError((str(path), checks))
    row['tracking'] = row['valid'] and row['tp_ratio'] >= tracking_ratio and row['stop_queue_ms'] <= tracking_lag_ms
    row['clearly_backlogged'] = row['valid'] and (row['tp_ratio'] < .95 or row['stop_queue_ms'] > 50)
    if item['kind'] == 'etcd' and not row['valid']:
        def prometheus(text):
            return {line.split()[0]: float(line.split()[1]) for line in text.splitlines()
                    if line and not line.startswith('#')}
        deltas = []
        for x, y in zip(before['nodes'], after['nodes']):
            x, y = prometheus(x), prometheus(y)
            deltas.append({k: y[k] - x.get(k, 0) for k in y if
                           any(name in k for name in ['snapshot_receive_success', 'snapshot_send_success',
                               'snap_db_save_total_duration_seconds_count', 'proposals_failed_total',
                               'leader_changes_seen_total'])})
        row['etcd_metric_deltas_leader_first'] = deltas
    return row


def cells(rows):
    groups = collections.defaultdict(list)
    for row in rows:
        groups[(row['role'], row['case'], row['kind'], row['rate'])].append(row)
    out = []
    for key, records in sorted(groups.items()):
        role, case, kind, rate = key
        complete = len(records) == 5 and {r['rep'] for r in records} == set(range(1, 6))
        valid_count = sum(r['valid'] for r in records)
        cell = dict(role=role, case=case, kind=kind, rate=rate, planned=len(records),
                    complete=complete, valid_count=valid_count, job_failed=any(r['job_failed'] for r in records),
                    all_valid=complete and valid_count == 5,
                    tracking_count=sum(r.get('tracking', False) for r in records),
                    backlogged_count=sum(r.get('clearly_backlogged', False) for r in records),
                    reported_p99_le_10ms=sum(r['valid'] and r['p99_ms'] <= 10 for r in records),
                    run_ids=sorted({r['run_id'] for r in records}), metrics={})
        if cell['all_valid']:
            for metric in METRICS:
                values = [r[metric] for r in records]
                cell['metrics'][metric] = dict(median=statistics.median(values), min=min(values), max=max(values))
        out.append(cell)
    return out


def knees(grouped):
    groups = collections.defaultdict(list)
    for cell in grouped:
        if cell['role'] == 'primary-open':
            groups[(cell['case'], cell['kind'])].append(cell)
    out = []
    for (case, kind), records in sorted(groups.items()):
        records.sort(key=lambda c: c['rate'])
        tracking = [c for c in records if c['all_valid'] and c['tracking_count'] == 5]
        lower = tracking[-1]['rate'] if tracking else None
        later = [c for c in records if lower is not None and c['rate'] > lower]
        backlogged = [c for c in later if c['all_valid'] and c['backlogged_count'] == 5]
        upper = backlogged[0]['rate'] if backlogged else None
        out.append(dict(case=case, kind=kind, last_all_five_tracking=lower,
                        first_all_five_backlogged=upper,
                        later_cells=[dict(rate=c['rate'], valid_count=c['valid_count'],
                                          tracking_count=c['tracking_count'], backlogged_count=c['backlogged_count'])
                                     for c in later]))
    return out


def tables(grouped):
    lines = ['| Role | Case | Store | Rate | Valid | Tracking / backlogged | Cohort TP | TP/rate range | Stop queue ms range | Scheduled p99 ms range |',
             '| --- | --- | --- | ---: | --- | --- | ---: | --- | --- | --- |']
    for c in grouped:
        m = c['metrics']
        def span(key, percent=False):
            if not m:
                return 'withheld: invalid cell'
            scale = 100 if percent else 1
            return f'{m[key]["min"] * scale:.3f}..{m[key]["max"] * scale:.3f}'
        tp = f'{m["cohort_tp"]["median"]:.3f}' if m else 'withheld'
        lines.append(f'| {c["role"]} | {c["case"]} | {c["kind"]} | {c["rate"]} | {c["valid_count"]}/5 | {c["tracking_count"]}/5 / {c["backlogged_count"]}/5 | {tp} | {span("tp_ratio", True)}% | {span("stop_queue_ms")} | {span("p99_ms")} |')
    return '\n'.join(lines) + '\n'


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--results', type=Path, default=Path('../results'))
    ap.add_argument('--campaign', type=Path, default=Path('../repo/experiments/shape/campaign.json'))
    ap.add_argument('--output', type=Path)
    ap.add_argument('--tracking-ratio', type=float, default=.98)
    ap.add_argument('--tracking-lag-ms', type=float, default=20)
    args = ap.parse_args()
    campaign = read(args.campaign)
    runs = [r for r in campaign['runs'] if r['folder'].split('/')[0] in FOLDERS]
    rows, provenance = [], []
    assert len(runs) == 27, 'replacement dispatch manifest changed; review the expected scope'
    for run in runs:
        assert run['commit'] == PIN and run['status'] == 'completed'
        inp = run['inputs']
        assert inp['reps'] == 5 and inp['duration'] == '3s' and inp['warmup'] == '1s' and inp['profile'] == 'none' and inp['conns'] == 18
        assert inp['targets'] == ['tigerstore', 'etcd'] and set(inp['rates']) == set(inp['targets'])
        for case in inp['cases']:
            root = args.results / run['folder'] / ('shape-' + case)
            expected = [dict(case=case, kind=kind, rep=rep, rate=rate,
                             path=f'{case}/{kind}-{rep}-r{rate}/load.json')
                        for rep in range(1, 6) for kind in inp['targets'] for rate in inp['rates'][kind]]
            plan_path = root / 'plan.json'
            if plan_path.exists():
                plan = read(plan_path)
                assert plan['case'] == case and plan['reps'] == 5 and plan['rates'] == inp['rates'] and plan['observation_control'] is False
                assert all(plan[key] == inp[key] for key in ['targets', 'duration', 'warmup', 'profile'])
                assert sorted(expected, key=lambda x:x['path']) == sorted(plan['windows'], key=lambda x:x['path'])
                assert {str(root / x['path']) for x in expected} == {str(p) for p in root.rglob('load.json')}
                runner = (root / 'runner.txt').read_text()
                assert runner.splitlines()[1] == PIN
                hashes = {m.group(2):m.group(1) for m in re.finditer(r'^([a-f0-9]{64})  bin/(\S+)$', runner, re.M)}
                provenance.append(dict(run_id=run['id'], case=case, hashes=hashes))
            for item in expected:
                rows.append(inspect(root / item['path'], item, run, args.tracking_ratio, args.tracking_lag_ms))
    for binary in ['tigerstore', 'loadgen', 'etcd']:
        assert len({p['hashes'][binary] for p in provenance}) == 1, 'binary mismatch across replacement jobs'
    grouped = cells(rows)
    report = dict(source=PIN, tracking_ratio=args.tracking_ratio, tracking_lag_ms=args.tracking_lag_ms,
                  planned=len(rows), valid=sum(r['valid'] for r in rows), missing=sum(r['missing'] for r in rows),
                  provenance=provenance, knees=knees(grouped), cells=grouped, windows=rows)
    if args.output:
        args.output.mkdir(parents=True, exist_ok=True)
        (args.output / 'calculations.json').write_text(json.dumps(report, indent=2))
        (args.output / 'cells.md').write_text(tables(grouped))
        fields = list(dict.fromkeys(k for r in rows for k in r))
        with (args.output / 'windows.csv').open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        print(f'planned={report["planned"]} valid={report["valid"]} missing={report["missing"]} cells={len(grouped)} output={args.output}')
    else:
        print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
