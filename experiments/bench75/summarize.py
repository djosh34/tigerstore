#!/usr/bin/env python3
"""Reproduce per-run rows and medians from JSON, rejecting invalid windows."""
import csv
import json
import sys
from pathlib import Path
from statistics import median

root = Path(sys.argv[1] if len(sys.argv) > 1 else 'evidence/bench75')
rows = []
for file in sorted([*root.glob('*/*/put.json'), *root.glob('*/*/cas.json')]):
    j = json.loads(file.read_text())
    assert j['valid'] and not j['errors'] and j['successes'] == 60000, file
    rep, store = file.parent.name.split('-', 1)
    row = {'op': j['op'], 'clients': j['workers'], 'store': store, 'rep': int(rep)}
    for k in ['ops_s', 'mean_ms', 'p50_ms', 'p99_ms', 'p999_ms', 'elapsed_s']:
        row[k] = j[k]
    for i, role in enumerate(['leader', 'follower1', 'follower2', 'generator']):
        row[f'{role}_cpu_us_op'] = j['cpu_us_op'][i]
        row[f'{role}_cores'] = j['occupied_cores'][i]
    row['server_cpu_us_op'] = sum(j['cpu_us_op'][:3])
    h = j['leader_heap']
    for key in ['objects_per_put', 'bytes_per_put', 'heap_bytes_after', 'gc_cpu_share', 'gc_cpu_us_per_put']:
        row[key] = h.get(key)
    # Operation-neutral names for new Put/CAS reports; legacy columns remain.
    row['objects_per_op'] = h.get('objects_per_op', h['objects_per_put'])
    row['bytes_per_op'] = h.get('bytes_per_op', h['bytes_per_put'])
    row['leader_syscr_per_op'] = j.get('leader_syscr_per_op')
    row['leader_syscw_per_op'] = j.get('leader_syscw_per_op')
    if j['op'] == 'cas':
        assert j['cas_compare_passed'] + j['cas_compare_failed'] == j['successes'], file
        assert j['revision_after'] - j['revision_before'] == j['cas_compare_passed'], file
    row['cas_compare_passed'] = j.get('cas_compare_passed', 0)
    row['cas_compare_failed'] = j.get('cas_compare_failed', 0)
    rows.append(row)
assert rows
fields = list(rows[0])
with (root/'runs.csv').open('w', newline='') as f:
    w = csv.DictWriter(f, fields, lineterminator='\n')
    w.writeheader()
    w.writerows(rows)
medians = []
for op, clients, store in sorted({(r['op'], r['clients'], r['store']) for r in rows}):
    selected = [r for r in rows if (r['op'], r['clients'], r['store']) == (op, clients, store)]
    assert {r['rep'] for r in selected} == {1, 2, 3, 4, 5}, (op, clients, store)
    assert len(selected) == 5, (op, clients, store, len(selected))
    fixed = {'op': op, 'clients': clients, 'store': store}
    medians.append({k: (fixed[k] if k in fixed else
                       None if all(r[k] is None for r in selected) else
                       median(r[k] for r in selected)) for k in fields if k != 'rep'})
with (root/'summary.csv').open('w', newline='') as f:
    w = csv.DictWriter(f, list(medians[0]), lineterminator='\n')
    w.writeheader()
    w.writerows(medians)
for r in medians:
    print(f"{r['op']:3} {r['clients']:3} {r['store']:10} {r['ops_s']:9.1f} op/s, p50/p99 {r['p50_ms']:.3f}/{r['p99_ms']:.3f} ms, leader/total CPU {r['leader_cpu_us_op']:.3f}/{r['server_cpu_us_op']:.3f} us/op, allocations {r['objects_per_op']:.2f} objects and {r['bytes_per_op']:.1f} bytes/op, leader syscr/syscw {r['leader_syscr_per_op']}/{r['leader_syscw_per_op']}")
