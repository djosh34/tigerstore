#!/usr/bin/env python3
"""Summarize only completed measurement windows, keeping every repetition."""
import csv
import json
from pathlib import Path
import statistics

ROOT = Path('evidence/pre47batch256')
rows = []
for p in sorted(ROOT.glob('*/c*/*/put.json')):
    r = json.loads(p.read_text())
    if not (p.parent/'disk.json').exists():
        continue
    disk = json.loads((p.parent/'disk.json').read_text())
    rep,store = p.parent.name.split('-',1)
    key = 'etcd_disk_wal_fsync_duration_seconds_count' if store == 'etcd' else 'syncs'
    counts = [b[key]-a[key] for a,b in zip(disk['before'],disk['after'])]
    rows.append(dict(mode=p.parents[2].name, clients=r['workers'], store=store, rep=int(rep),
        valid=r['valid'], ops_s=r['ops_s'], mean_ms=r['mean_ms'], p50_ms=r['p50_ms'],
        p99_ms=r['p99_ms'], p999_ms=r['p999_ms'],
        **{f'cpu_{i}_us':v for i,v in enumerate(r['cpu_us_op'])},
        **{f'syncs_{i}':v for i,v in enumerate(counts)},
        **{f'puts_per_sync_{i}':r['successes']/v for i,v in enumerate(counts)}))
if rows:
    with (ROOT/'runs.csv').open('w') as f:
        w=csv.DictWriter(f,fieldnames=rows[0]); w.writeheader(); w.writerows(rows)
    groups = sorted({(r['mode'],r['clients'],r['store']) for r in rows})
    for mode,c,s in groups:
        group=[r for r in rows if (r['mode'],r['clients'],r['store'])==(mode,c,s)]
        vals={k:statistics.median(r[k] for r in group) for k in ('ops_s','mean_ms','p99_ms','puts_per_sync_0','puts_per_sync_1','puts_per_sync_2','cpu_0_us')}
        print(mode,c,s,len(group),json.dumps(vals))
