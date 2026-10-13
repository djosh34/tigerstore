#!/usr/bin/env python3
"""Reproduce per-run rows and medians from JSON, rejecting invalid windows."""
import csv
import json
import sys
from pathlib import Path
from statistics import median

root = Path(sys.argv[1] if len(sys.argv) > 1 else 'evidence/alloc50')
rows = []
for file in sorted(root.glob('c*/[1-5]-*/put.json')):
    j = json.loads(file.read_text())
    assert j['valid'] and not j['errors'] and j['successes'] == 60000, file
    assert json.loads((file.parent/'validation.json').read_text())['valid'], file
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
    retained = json.loads((file.parent/'retained-after.json').read_text())
    assert retained['forced_gc'] and retained['mem_profile_rate'] != 1, file
    row['retained_heap_bytes_after_forced_gc'] = retained['heap_alloc_bytes']
    row['retained_heap_objects_after_forced_gc'] = retained['heap_objects']
    rows.append(row)
assert rows
for clients in {r['clients'] for r in rows}:
    assert {r['store'] for r in rows if r['clients'] == clients} == {'base', 'ring', 'arena'}
    for store in ('base', 'ring', 'arena'):
        profile = root/f'c{clients}'/f'profile-{store}'
        cohort = json.loads((profile/'profile-cohort.json').read_text())
        assert json.loads((profile/'validation.json').read_text())['valid'], profile
        assert cohort['successes'] == 2000 and cohort['mem_profile_rate'] == 1
    for store in ('base', 'ring', 'arena', 'default'):
        case = json.loads((root/f'c{clients}'/f'cases-{store}'/'cases.json').read_text())
        assert case['valid'] and case['healthy_successes_while_stalled'] >= 1000
        assert case['slow_requests'] == case['slow_attempted_puts'] == 4096 and case['reply_reads_after_handshake'] == 0
        assert 1 <= case['slow_committed_puts'] <= case['slow_attempted_puts']
        assert json.loads((root/f'c{clients}'/f'cases-{store}'/'validation.json').read_text())['valid']
        if store in ('ring', 'arena'):
            assert case['allocator_saturation_verified'] and case['allocator_recovery_verified']
            for phase in ('before_slow', 'slow_phase_0', 'slow_phase_1', 'slow_phase_2', 'after_slow_close'):
                stats = case[phase]['allocator']
                assert stats['mode'] == {'ring': 1, 'arena': 2}[store]
                assert stats['held_slots'] <= stats['slot_budget'] and stats['peak_slots'] <= stats['slot_budget']
                assert stats['held_bytes'] <= stats['byte_budget'] and stats['peak_bytes'] <= stats['byte_budget']
                assert stats['fallback_allocations'] == 0
            after = case['after_slow_close']['allocator']
            before = case['before_slow']['allocator']
            last = case['slow_phase_2']['allocator']
            prior = case['slow_phase_1']['allocator']
            assert last['held_slots'] >= last['per_connection_slots'] > 0
            assert last['denied_count'] > before['denied_count']
            assert (last['held_slots'], last['held_bytes']) == (prior['held_slots'], prior['held_bytes'])
            assert after['held_slots'] <= before['held_slots'] and after['held_bytes'] <= before['held_bytes']
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
