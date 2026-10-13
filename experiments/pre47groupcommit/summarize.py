#!/usr/bin/env python3
"""Reproduce all rows and medians from archived JSON; reject invalid windows."""
import csv
import json
import sys
from pathlib import Path
from statistics import median

root = Path(sys.argv[1] if len(sys.argv) > 1 else 'evidence/bench68')
rows = []
for file in sorted(root.glob('c*/*/put.json')):
    j = json.loads(file.read_text())
    assert j['valid'] and not j['errors'] and j['successes'] == 60000, file
    rep, store = file.parent.name.split('-', 1)
    d = json.loads((file.parent/'disk.json').read_text())
    deltas = [{k:a[k]-b[k] for k in b} for b,a in zip(d['before'], d['after'])]
    key = 'etcd_disk_wal_fsync_duration_seconds_count' if store == 'etcd' else 'syncs'
    syncs = [v[key] for v in deltas]
    assert all(n > 0 for n in syncs), file
    row = {'clients':j['workers'], 'store':store, 'rep':int(rep)}
    for k in ['ops_s','mean_ms','p50_ms','p99_ms','p999_ms','elapsed_s']:
        row[k] = j[k]
    for i, role in enumerate(['leader','follower1','follower2','generator']):
        row[f'{role}_cpu_us_op'] = j['cpu_us_op'][i]
        row[f'{role}_cores'] = j['occupied_cores'][i]
        if i < 3:
            row[f'{role}_syncs'] = syncs[i]
            row[f'{role}_puts_per_sync'] = j['successes']/syncs[i]
            if store == 'etcd':
                row[f'{role}_backend_commits'] = deltas[i]['etcd_disk_backend_commit_duration_seconds_count']
                row[f'{role}_sync_wall_s'] = deltas[i]['etcd_disk_wal_fsync_duration_seconds_sum']
                row[f'{role}_mean_write_bytes'] = 0
            else:
                row[f'{role}_backend_commits'] = 0
                row[f'{role}_sync_wall_s'] = deltas[i]['sync_ns']/1e9
                row[f'{role}_mean_write_bytes'] = deltas[i]['write_bytes']/deltas[i]['writes']
    row['server_cpu_us_op'] = sum(j['cpu_us_op'][:3])
    rows.append(row)
assert rows
fields = list(rows[0])
with (root/'runs.csv').open('w') as f:
    w = csv.DictWriter(f,fields); w.writeheader(); w.writerows(rows)
medians = []
for clients in sorted({r['clients'] for r in rows}):
    for store in ['baseline','tigerstore','etcd']:
        selected = [r for r in rows if r['clients']==clients and r['store']==store]
        assert len(selected) == 5, (clients,store,len(selected))
        medians.append({k:(clients if k=='clients' else store if k=='store' else median(r[k] for r in selected)) for k in fields if k!='rep'})
with (root/'summary.csv').open('w') as f:
    w = csv.DictWriter(f,list(medians[0])); w.writeheader(); w.writerows(medians)
for r in medians:
    print(f"{r['clients']:3} {r['store']:10} {r['ops_s']:9.1f} Put/s, p50/p99 {r['p50_ms']:.3f}/{r['p99_ms']:.3f} ms, server CPU {r['server_cpu_us_op']:.3f} us/op, leader Puts/sync {r['leader_puts_per_sync']:.3f}")
