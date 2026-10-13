#!/usr/bin/env python3
"""Reproduce per-run rows and medians from JSON, rejecting invalid windows."""
import csv
import json
import sys
from pathlib import Path
from statistics import median

root = Path(sys.argv[1] if len(sys.argv) > 1 else 'evidence/bench75')
rows = []
for file in sorted(root.glob('c*/*/put.json')):
    j = json.loads(file.read_text())
    assert j['valid'] and not j['errors'] and j['successes'] == 60000, file
    rep, store = file.parent.name.split('-', 1)
    row = {'clients': j['workers'], 'store': store, 'rep': int(rep)}
    for k in ['ops_s', 'mean_ms', 'p50_ms', 'p99_ms', 'p999_ms', 'elapsed_s']:
        row[k] = j[k]
    for i, role in enumerate(['leader', 'follower1', 'follower2', 'generator']):
        row[f'{role}_cpu_us_op'] = j['cpu_us_op'][i]
        row[f'{role}_cores'] = j['occupied_cores'][i]
    row['server_cpu_us_op'] = sum(j['cpu_us_op'][:3])
    for key in ['objects_per_put', 'bytes_per_put', 'heap_bytes_after', 'gc_cpu_share', 'gc_cpu_us_per_put']:
        row[key] = j['leader_heap'].get(key)
    rows.append(row)
assert rows
fields = list(rows[0])
with (root/'runs.csv').open('w') as f:
    w = csv.DictWriter(f, fields)
    w.writeheader()
    w.writerows(rows)
medians = []
for clients in sorted({r['clients'] for r in rows}):
    stores = sorted({r['store'] for r in rows if r['clients'] == clients})
    for store in stores:
        selected = [r for r in rows if r['clients'] == clients and r['store'] == store]
        assert {r['rep'] for r in selected} == {1, 2, 3, 4, 5}, (clients, store)
        assert len(selected) == 5, (clients, store, len(selected))
        medians.append({k: (clients if k == 'clients' else store if k == 'store' else
                           None if all(r[k] is None for r in selected) else
                           median(r[k] for r in selected)) for k in fields if k != 'rep'})
with (root/'summary.csv').open('w') as f:
    w = csv.DictWriter(f, list(medians[0]))
    w.writeheader()
    w.writerows(medians)
for r in medians:
    print(f"{r['clients']:3} {r['store']:10} {r['ops_s']:9.1f} Put/s, p50/p99 {r['p50_ms']:.3f}/{r['p99_ms']:.3f} ms, leader/total CPU {r['leader_cpu_us_op']:.3f}/{r['server_cpu_us_op']:.3f} us/Put, allocations {r['objects_per_put']:.2f} objects and {r['bytes_per_put']:.1f} bytes/Put")
