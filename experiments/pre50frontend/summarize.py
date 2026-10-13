#!/usr/bin/env python3
"""Reproduce rows from fenced counters. Never mix primary and profile cohorts."""
import csv
import json
from pathlib import Path
from statistics import median
import sys

ROLES = ('leader', 'follower1', 'follower2')


def delta(before, after, key):
    b, a = before.get(key), after.get(key)
    if b is None or a is None:
        return None
    assert a >= b, (key, b, a)
    return a-b


def row_from(folder):
    j = json.loads((folder/'put.json').read_text())
    assert j['valid'] and not j['errors'] and len(j['terms']) == 1
    assert j['workers'] in (16, 64, 256) and j['conns'] == 16
    mode = folder.parent.parent.name
    assert mode in ('primary', 'profile')
    if mode == 'primary':
        assert j['successes'] == 60000
    rep, store = folder.name.split('-', 1)
    r = {'mode': mode, 'clients': j['workers'], 'rep': int(rep), 'store': store,
         'cpu_profiled': (folder/'profile-window.json').exists(), 'operations': j['successes']}
    for k in ('ops_s', 'p50_ms', 'p90_ms', 'p99_ms', 'p999_ms', 'mean_ms', 'elapsed_s'):
        r[k] = j[k]
    snapshots = json.loads((folder/'snapshots.json').read_text())
    assert len(snapshots['before']) == len(snapshots['after']) == 3
    for i, role in enumerate(ROLES):
        before, after = snapshots['before'][i], snapshots['after'][i]
        key = 'etcd_disk_wal_fsync_duration_seconds_count' if store == 'etcd' else 'syncs'
        syncs = delta(before['disk'], after['disk'], key)
        assert syncs is not None and syncs > 0
        r[f'{role}_file_syncs'] = syncs
        r[f'{role}_puts_per_file_sync'] = j['successes']/syncs
        r[f'{role}_backend_commits'] = delta(before['disk'], after['disk'], 'etcd_disk_backend_commit_duration_seconds_count')
        objects = delta(before, after, 'mallocs')
        allocated = delta(before, after, 'total_alloc')
        assert objects is not None and allocated is not None, f'missing allocation counters: {folder}, {role}'
        r[f'{role}_allocation_objects_op'] = objects/j['successes']
        r[f'{role}_allocation_bytes_op'] = allocated/j['successes']
        cb, ca = snapshots['cpu_before'][i], snapshots['cpu_after'][i]
        assert cb['pid'] == ca['pid'] and cb['start'] == ca['start'] and ca['ticks'] >= cb['ticks']
        r[f'{role}_fenced_cpu_us_op'] = (ca['ticks']-cb['ticks'])/snapshots['hz']*1e6/j['successes']
        r[f'{role}_cpu_us_op'] = j['cpu_us_op'][i]
        r[f'{role}_occupied_cores'] = j['occupied_cores'][i]
    r['generator_cpu_us_op'] = j['cpu_us_op'][3]
    r['server_cpu_us_op'] = sum(j['cpu_us_op'][:3])
    r['server_fenced_cpu_us_op'] = sum(r[f'{role}_fenced_cpu_us_op'] for role in ROLES)
    return r


def write_csv(path, rows):
    with path.open('w') as f:
        w = csv.DictWriter(f, list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def main():
    root = Path(sys.argv[1])
    rows = [row_from(p.parent) for p in sorted(root.glob('*-*/put.json'))]
    assert rows, 'no valid windows'
    modes = {r['mode'] for r in rows}
    assert len(modes) == 1
    stores = sorted({r['store'] for r in rows})
    assert (stores == ['a', 'b', 'c', 'etcd']) if modes == {'primary'} else (len(stores) == 2 and 'etcd' in stores)
    summaries = []
    identity = ('mode', 'clients', 'store', 'cpu_profiled')
    for store in stores:
        selected = [r for r in rows if r['store'] == store]
        assert {r['rep'] for r in selected} == set(range(1, 6)) and len(selected) == 5
        summary = {}
        for k in rows[0]:
            if k == 'rep':
                continue
            values = [r[k] for r in selected]
            if k in identity:
                assert all(v == values[0] for v in values)
                summary[k] = values[0]
            else:
                summary[k] = None if any(v is None for v in values) else median(values)
        summaries.append(summary)
    write_csv(root/'runs.csv', rows)
    write_csv(root/'summary.csv', summaries)
    for r in summaries:
        print(f"{r['mode']} {r['clients']} {r['store']}: {r['ops_s']:.1f} Put/s; "
              f"p50/p90/p99/p999 {r['p50_ms']:.3f}/{r['p90_ms']:.3f}/{r['p99_ms']:.3f}/{r['p999_ms']:.3f} ms; "
              f"server CPU {r['server_cpu_us_op']:.3f} us/op; profiled={r['cpu_profiled']}")


if __name__ == '__main__':
    main()
