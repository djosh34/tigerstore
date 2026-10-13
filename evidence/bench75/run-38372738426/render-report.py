#!/usr/bin/env python3
"""Render the archived report tables and full pprof top-25 listings."""
import csv
from pathlib import Path

root = Path(__file__).resolve().parent
rows = list(csv.DictReader((root/'summary.csv').open()))
labels = {'baseline': 'Main', 'candidate': 'PR 76', 'second': 'PR 77', 'etcd': 'Etcd'}
performance = ['| Clients | Store | Put/s | p50 ms | p99 ms | Leader CPU us/Put | Total server CPU us/Put |',
               '| ---: | --- | ---: | ---: | ---: | ---: | ---: |']
allocation = ['| Clients | Store | Objects/Put | Bytes/Put | GC CPU share | Heap object MiB after window |',
              '| ---: | --- | ---: | ---: | ---: | ---: |']
for clients in ['64', '256']:
    for store in ['baseline', 'candidate', 'second', 'etcd']:
        r = next(r for r in rows if r['clients'] == clients and r['store'] == store)
        performance.append(f"| {clients} | {labels[store]} | {float(r['ops_s']):,.1f} | {float(r['p50_ms']):.3f} | {float(r['p99_ms']):.3f} | {float(r['leader_cpu_us_op']):.2f} | {float(r['server_cpu_us_op']):.2f} |")
        gc = f"{100*float(r['gc_cpu_share']):.2f}%" if r['gc_cpu_share'] else 'Unavailable'
        allocation.append(f"| {clients} | {labels[store]} | {float(r['objects_per_put']):.2f} | {float(r['bytes_per_put']):,.0f} | {gc} | {float(r['heap_bytes_after'])/2**20:.1f} |")
profiles = []
for kind, title in [('cpu', 'CPU'), ('alloc_space', 'Allocated bytes'), ('alloc_objects', 'Allocated objects')]:
    for sort in ['flat', 'cum']:
        text = (root/'c256/profile-second'/f'{kind}-{sort}.txt').read_text().rstrip()
        assert 'Showing top 25 nodes' in text
        lines = text.split('      flat  flat%   sum%        cum   cum%\n')[1].strip().splitlines()
        assert len(lines) == 25
        profiles.append(f"#### {title}, top 25 {'flat' if sort == 'flat' else 'cumulative'}\n\n```text\n{text}\n```")
body = (root/'report-base.md').read_text()
for key, text in [('PERFORMANCE_TABLE', '\n'.join(performance)), ('ALLOCATION_TABLE', '\n'.join(allocation)), ('PROFILE_LISTINGS', '\n\n'.join(profiles))]:
    body = body.replace('{{'+key+'}}', text)
assert len(body) < 65536, len(body)
(root/'report.md').write_text(body)
print(f'Rendered {len(body)} characters, with six verified 25-function listings.')
