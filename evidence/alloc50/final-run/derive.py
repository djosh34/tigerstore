#!/usr/bin/env python3
"""Derive ranges and paired changes from the validated 30 primary windows."""
import csv
import json
from pathlib import Path
from statistics import median

root = Path(__file__).resolve().parent
with (root / 'runs.csv').open() as f:
    rows = [{k: v if k == 'store' else float(v) for k, v in row.items()}
            for row in csv.DictReader(f)]
assert len(rows) == 30
metrics = ['leader_cpu_us_op', 'ops_s', 'p99_ms', 'p999_ms', 'objects_per_put',
           'bytes_per_put', 'gc_cpu_share', 'retained_heap_bytes_after_forced_gc',
           'generator_cores', 'server_cpu_us_op']
result = []
for clients in (64, 256):
    base = {int(r['rep']): r for r in rows if r['clients'] == clients and r['store'] == 'base'}
    assert set(base) == set(range(1, 6))
    for store in ('base', 'ring', 'arena'):
        selected = [r for r in rows if r['clients'] == clients and r['store'] == store]
        assert {int(r['rep']) for r in selected} == set(range(1, 6))
        entry = {'clients': clients, 'store': store, 'runs': len(selected), 'metrics': {}}
        for metric in metrics:
            values = [r[metric] for r in selected]
            paired = [100 * (r[metric] / base[int(r['rep'])][metric] - 1) for r in selected]
            entry['metrics'][metric] = {'min': min(values), 'median': median(values),
                                       'max': max(values), 'paired_change_pct': paired,
                                       'median_paired_change_pct': median(paired)}
        result.append(entry)
(root / 'ranges-and-paired-changes.json').write_text(json.dumps(result, indent=2) + '\n')
for row in result:
    m = row['metrics']
    print(f"{row['clients']} {row['store']}: leader CPU "
          f"{m['leader_cpu_us_op']['median']:.3f} "
          f"[{m['leader_cpu_us_op']['min']:.3f}, {m['leader_cpu_us_op']['max']:.3f}] us/Put; "
          f"paired CPU change {m['leader_cpu_us_op']['median_paired_change_pct']:+.2f}%; "
          f"retained {m['retained_heap_bytes_after_forced_gc']['median'] / 2**20:.2f} MiB")
