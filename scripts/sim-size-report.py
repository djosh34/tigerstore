#!/usr/bin/env python3
"""Summarize downloaded sim-size artifacts; no Go build or simulation involved.

Usage: python3 scripts/sim-size-report.py <artifact-dir> [...]
Earlier runs rounded wall time up to the five-second monitor interval. Always
read GNU time's precise elapsed measurement and recompute the rate here.
"""
import json
from pathlib import Path
import re
import sys

for arg in sys.argv[1:]:
    root = Path(arg)
    if not (root / 'size-result.json').exists():
        print(json.dumps(dict(artifact=str(root), outcome='no_final_result')))
        continue
    row = json.loads((root / 'size-result.json').read_text())
    timing = (root / 'size-time.txt').read_text()
    m = re.search(r'Elapsed \(wall clock\) time \(h:mm:ss or m:ss\): ([\d:.]+)', timing)
    if m:
        wall = 0
        for part in m.group(1).split(':'):
            wall = wall * 60 + float(part)
        row['wall_s'] = wall
        row['sim_s_per_wall_s'] = row['last_sim_s'] / wall
    row['peak_rss_gib'] = row['peak_rss_kib'] / 1024**2
    row['artifact'] = str(root)
    log = (root / 'size.log').read_text(errors='replace')
    progress = re.findall(r'SIZE_(?:PROGRESS|DONE)[^\n]*', log)
    if progress:
        for field in ('loaded', 'calls', 'ok', 'compact', 'kills'):
            m = re.search(rf'{field}=(\d+)', progress[-1])
            if m:
                row[field] = int(m.group(1))
    if 'SIZE_PRELOADED' in log:
        row['loaded'] = row['objects']
    samples = [json.loads(line) for line in (root / 'size-samples.jsonl').read_text().splitlines()]
    samples = [s for s in samples if s['rss_kib'] > 0]
    # RSS is noisy because native GC still runs. These endpoint slopes are
    # descriptive only, not fits or proofs of a hard maximum run length.
    if len(samples) > 1:
        a, b = samples[len(samples)//2], samples[-1]
        if b['wall_s'] > a['wall_s']:
            row['tail_rss_mib_per_wall_s'] = (b['rss_kib']-a['rss_kib'])/1024/(b['wall_s']-a['wall_s'])
        ma = re.search(r'(?<!churn_)sim_s=([\d.]+)', a['progress'])
        mb = re.search(r'(?<!churn_)sim_s=([\d.]+)', b['progress'])
        if ma and mb and float(mb[1]) > float(ma[1]):
            row['tail_sim_s_per_wall_s'] = (float(mb[1])-float(ma[1]))/(b['wall_s']-a['wall_s'])
            row['tail_rss_mib_per_sim_min'] = (b['rss_kib']-a['rss_kib'])/1024/(float(mb[1])-float(ma[1]))*60
    print(json.dumps(row))
