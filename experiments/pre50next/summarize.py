#!/usr/bin/env python3
"""Report valid windows only. Raw JSON/Prometheus snapshots remain authoritative."""
import csv
import json
from pathlib import Path
import statistics
import sys


def summarize(root):
    rows = []
    for path in sorted(root.glob('*/put.json')):
        p = json.loads(path.read_text())
        if not p['valid'] or p['errors']:
            raise ValueError(f'invalid window: {path}')
        snap = json.loads((path.parent / 'snapshots.json').read_text())
        retained = json.loads((path.parent / 'retained.json').read_text())
        rep, variant = path.parent.name.split('-', 1)
        row = dict(rep=int(rep), variant=variant, clients=p['workers'], operations=p['successes'])
        for key in ('ops_s', 'mean_ms', 'p50_ms', 'p90_ms', 'p99_ms', 'p999_ms', 'elapsed_s'):
            row[key] = p[key]
        for i, role in enumerate(('leader', 'follower1', 'follower2')):
            a, b = snap['before'][i], snap['after'][i]
            n = p['successes']
            row[f'{role}_cpu_us_op'] = p['cpu_us_op'][i]
            row[f'{role}_objects_op'] = (b['mallocs']-a['mallocs'])/n
            row[f'{role}_bytes_op'] = (b['total_alloc']-a['total_alloc'])/n
            if 'gc_cpu_seconds' in a:
                row[f'{role}_gc_us_op'] = (b['gc_cpu_seconds']-a['gc_cpu_seconds'])*1e6/n
            else:
                row[f'{role}_gc_us_op'] = ''
            key = 'etcd_disk_wal_fsync_duration_seconds_count' if variant.startswith('etcd') else 'syncs'
            syncs = b['disk'][key]-a['disk'][key]
            row[f'{role}_syncs'] = syncs
            row[f'{role}_puts_sync'] = n/syncs if syncs else ''
            row[f'{role}_retained_heap_bytes'] = retained['nodes'][i]['heap_alloc']
            row[f'{role}_retained_rss_bytes'] = retained['proc'][i]['rss_bytes']
            for key in ('syscr', 'syscw', 'read_bytes', 'write_bytes'):
                row[f'{role}_{key}_op'] = (snap['proc_after'][i]['io'][key]-snap['proc_before'][i]['io'][key])/n
        row['generator_cpu_us_op'] = p['cpu_us_op'][3]
        rows.append(row)
    if not rows:
        raise ValueError('no complete measurement windows')
    schedule = json.loads((root / 'schedule.json').read_text())
    expected = {(rep, label) for rep, cohort in schedule for _, label, _ in cohort}
    observed = {(row['rep'], row['variant']) for row in rows}
    if observed != expected or len(rows) != len(expected):
        raise ValueError(f'incomplete or unexpected cohorts: expected={expected}, observed={observed}')
    def write_csv(path, data):
        with path.open('w') as f:
            w = csv.DictWriter(f, fieldnames=list(data[0]))
            w.writeheader()
            w.writerows(data)
    write_csv(root / 'runs.csv', rows)
    summaries = []
    for variant in sorted({r['variant'] for r in rows}):
        cohort = [r for r in rows if r['variant'] == variant]
        row = {'variant': variant, 'repetitions': len(cohort)}
        for k in rows[0]:
            if k not in ('variant', 'rep'):
                values = [r[k] for r in cohort if isinstance(r[k], (int, float))]
                row[k] = statistics.median(values) if values else ''
        summaries.append(row)
    write_csv(root / 'summary.csv', summaries)
    lines = ['# Put results', '', 'Medians of per-run statistics. Profile controls and primary runs are separate.', '',
             '| Variant | Reps | Put/s | p99 ms | Leader CPU us/Put | Leader allocations/Put |',
             '| --- | ---: | ---: | ---: | ---: | ---: |']
    for row in summaries:
        lines.append(f"| {row['variant']} | {row['repetitions']} | {row['ops_s']:.1f} | {row['p99_ms']:.3f} | {row['leader_cpu_us_op']:.3f} | {row['leader_objects_op']:.2f} |")
    lines += ['', 'CPU comes from the generator worker interval through drain. Allocation, I/O and sync counters use the wider all-replica-fenced interval. Etcd syncs count WAL observations, not backend commits. Forced-GC retained heap is only available for Tigerstore.']
    (root / 'summary.md').write_text('\n'.join(lines)+'\n')


if __name__ == '__main__':
    summarize(Path(sys.argv[1]))
