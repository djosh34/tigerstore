#!/usr/bin/env python3
"""Regenerate run rows, five-run medians and paired ratios from raw artifacts."""
import csv
import json
from pathlib import Path
from statistics import median
import sys

ROLES = ('leader', 'follower1', 'follower2')


def read_row(file):
    j = json.loads(file.read_text())
    assert j['valid'] and not j['errors'] and j['successes'] == 60000, file
    assert j['workers'] in (16, 64, 256) and j['conns'] == 16, file
    assert j['keyspace'] == 4096 and j['value_bytes'] == 1024, file
    assert len(j['cpu_us_op']) == 4, file
    rep, store = file.parent.name.split('-', 1)
    layout = json.loads((file.parent/'layout.json').read_text())
    assert layout['server_go_processors'] == [2, 2, 2], file
    assert layout['server_cpus_in_role_order'] == [0, 1, 2], file
    assert layout['generator_cpu'] == 3, file
    assert layout['generator_connections'] == 16, file
    assert layout['variant']['name'] == store, file
    d = json.loads((file.parent/'disk.json').read_text())
    assert len(d['before']) == len(d['after']) == 3, file
    assert len(j['alloc_before']) == len(j['alloc_after']) == 3, file
    row = dict(clients=j['workers'], conns=j['conns'], store=store,
               source_ref=layout['variant']['ref'], rep=int(rep))
    for k in ('ops_s', 'mean_ms', 'p50_ms', 'p90_ms', 'p95_ms', 'p99_ms', 'p999_ms', 'elapsed_s'):
        row[k] = j[k]
    for i, role in enumerate((*ROLES, 'generator')):
        row[f'{role}_cpu_us_op'] = j['cpu_us_op'][i]
        row[f'{role}_cores'] = j['occupied_cores'][i]
        if i == 3:
            continue
        b, a = d['before'][i], d['after'][i]
        delta = {k: a[k]-b[k] for k in b}
        assert all(v >= 0 for v in delta.values()), file
        sync_key = 'etcd_disk_wal_fsync_duration_seconds_count' if store == 'etcd' else 'syncs'
        syncs = delta[sync_key]
        assert syncs > 0, file
        row[f'{role}_file_fsyncs'] = syncs
        row[f'{role}_puts_per_fsync'] = j['successes']/syncs
        # These are separate etcd backend transactions, not WAL fsyncs.
        row[f'{role}_backend_commits'] = delta.get('etcd_disk_backend_commit_duration_seconds_count', 0)
        alloc_b, alloc_a = j['alloc_before'][i], j['alloc_after'][i]
        mallocs = alloc_a['mallocs']-alloc_b['mallocs']
        size = alloc_a['bytes']-alloc_b['bytes']
        assert mallocs > 0 and size > 0, file
        row[f'{role}_allocs_op'] = mallocs/j['successes']
        row[f'{role}_bytes_op'] = size/j['successes']
    for metric in ('cpu_us_op', 'allocs_op', 'bytes_op'):
        row[f'server_{metric}'] = sum(row[f'{role}_{metric}'] for role in ROLES)
    return row


def csv_write(path, rows):
    with path.open('w', newline='') as f:
        w = csv.DictWriter(f, list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def summarize(root):
    rows = [read_row(f) for f in sorted(root.rglob('put.json'))]
    assert rows, f'no measurements in {root}'
    groups = sorted({(r['clients'], r['store']) for r in rows})
    medians = []
    fixed = ('clients', 'conns', 'store', 'source_ref')
    for clients, store in groups:
        selected = [r for r in rows if (r['clients'], r['store']) == (clients, store)]
        assert len(selected) == 5 and {r['rep'] for r in selected} == set(range(1, 6)), (clients, store)
        for k in fixed:
            assert len({r[k] for r in selected}) == 1, (clients, store, k)
        medians.append({k: selected[0][k] if k in fixed else median(r[k] for r in selected)
                        for k in rows[0] if k != 'rep'})
    for clients in {r['clients'] for r in rows}:
        manifests = list(root.rglob(f'c{clients}/variants.json'))
        if root.name == f'c{clients}':
            manifests.append(root/'variants.json')
        assert len(manifests) == 1, (clients, manifests)
        manifest = json.loads(manifests[0].read_text())
        expected = {v['name']: v['ref'] for v in manifest}
        assert len(expected) == len(manifest), manifest
        assert set(expected) == {r['store'] for r in rows if r['clients'] == clients}, (clients, expected)
        assert all(r['source_ref'] == expected[r['store']] for r in rows if r['clients'] == clients), clients
    csv_write(root/'runs.csv', rows)
    csv_write(root/'summary.csv', medians)
    pairs = []
    for r in rows:
        if r['store'] not in ('rpc', 'admit', 'full'):
            continue
        baselines = ['main', 'etcd', 'native', 'native-tuned', 'extra']
        previous_stage = {'admit': 'rpc', 'full': 'admit'}.get(r['store'])
        if previous_stage:
            baselines.append(previous_stage)
        for baseline in baselines:
            b = [b for b in rows if b['clients'] == r['clients'] and b['rep'] == r['rep'] and b['store'] == baseline]
            if not b:
                continue
            assert len(b) == 1
            pair = dict(clients=r['clients'], rep=r['rep'], candidate=r['store'], baseline=baseline)
            for k in ('ops_s', 'server_cpu_us_op', 'server_allocs_op', 'server_bytes_op', 'p95_ms', 'p99_ms'):
                pair[k+'_ratio'] = r[k]/b[0][k]
            pairs.append(pair)
    csv_write(root/'paired.csv', pairs)
    report = ['# Wholebatch runner medians', '',
              'Five repetitions per variant in the same job. 16 connections, 1 KiB Put, 4,096 keys, leader-only.',
              'Leader CPU 0, followers 1 and 2, generator 3. Servers GOMAXPROCS=2. Generator GOMAXPROCS=1.',
              'These are closed-loop capacity runs with counting-only instrumentation, not profiles.', '',
              '| Clients | Variant | Put/s | p50 ms | p95 ms | p99 ms | Total server CPU us/op |',
              '| ---: | --- | ---: | ---: | ---: | ---: | ---: |']
    for r in medians:
        report.append(f"| {r['clients']} | {r['store']} | {r['ops_s']:.1f} | {r['p50_ms']:.3f} | {r['p95_ms']:.3f} | {r['p99_ms']:.3f} | {r['server_cpu_us_op']:.3f} |")
    report += ['', '| Clients | Variant | Role | CPU us/op | Allocs/op | Bytes/op | Puts/file-fsync |',
               '| ---: | --- | --- | ---: | ---: | ---: | ---: |']
    for r in medians:
        for role in ROLES:
            report.append(f"| {r['clients']} | {r['store']} | {role} | {r[role+'_cpu_us_op']:.3f} | {r[role+'_allocs_op']:.3f} | {r[role+'_bytes_op']:.1f} | {r[role+'_puts_per_fsync']:.3f} |")
    report += ['', 'Stage ratios divide the later stage by the earlier stage within each repetition, then take the median ratio.',
               '| Clients | Stages | Put/s ratio | Total CPU/op ratio | Allocs/op ratio | Bytes/op ratio | p95 ratio | p99 ratio |',
               '| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |']
    for clients in sorted({r['clients'] for r in rows}):
        for candidate, baseline in (('admit', 'rpc'), ('full', 'admit')):
            selected = [p for p in pairs if p['clients'] == clients and p['candidate'] == candidate and p['baseline'] == baseline]
            assert len(selected) == 5, (clients, candidate, baseline)
            values = [median(p[k+'_ratio'] for p in selected) for k in ('ops_s', 'server_cpu_us_op', 'server_allocs_op', 'server_bytes_op', 'p95_ms', 'p99_ms')]
            report.append(f'| {clients} | {candidate}/{baseline} | ' + ' | '.join(f'{v:.3f}' for v in values) + ' |')
    report += ['', 'CPU is process user+system time from /proc, not owner-goroutine CPU.',
               'CPU and allocation windows do not wait for follower catch-up. Follower cost can fall outside those windows.',
               'Allocation counters bracket the CPU window and include small debug scrape overhead. They count all process allocations, not retained bytes.',
               'Fsync deltas enclose the fully applied cohort on all nodes. etcd also fences on its post-WAL.Save committed gauge.',
               'Tigerstore file syncs exclude directory syncs. etcd counts WAL fsync observations; backend commits are separate in runs.csv.',
               'Medians are computed independently. paired.csv contains per-repetition candidate/baseline ratios.', '']
    (root/'summary.md').write_text('\n'.join(report))
    print('\n'.join(report))


if __name__ == '__main__':
    summarize(Path(sys.argv[1] if len(sys.argv) > 1 else 'evidence/wholebatch'))
