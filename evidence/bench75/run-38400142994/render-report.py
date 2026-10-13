#!/usr/bin/env python3
import csv
import sys
from pathlib import Path

root = Path(__file__).resolve().parent
commit = sys.argv[1]
assert len(commit) == 40 and all(c in '0123456789abcdef' for c in commit)
rows = list(csv.DictReader((root / 'summary.csv').open()))
headline = ['| Clients | Store | Leader CPU µs/Put | Leader syscr/Put | Leader syscw/Put |',
            '|---:|---|---:|---:|---:|']
tables = []
for clients in [64, 256]:
    data = {r['store']: r for r in rows if int(r['clients']) == clients}
    lines = [f'### Put, {clients} clients', '',
             '| Store | Put/s | Mean / p50 / p99 / p99.9 ms | Leader / total CPU µs/Put | Generator µs/Put / cores | Leader objects / bytes per Put | GC CPU share |',
             '|---|---:|---:|---:|---:|---:|---:|']
    for store, label in [('baseline', 'main'), ('candidate', 'PR 84'), ('etcd', 'etcd 3.7.2')]:
        r = data[store]
        headline.append(f"| {clients} | {label} | {float(r['leader_cpu_us_op']):.2f} | {float(r['leader_syscr_per_op']):.5f} | {float(r['leader_syscw_per_op']):.5f} |")
        latency = ' / '.join(f'{float(r[k]):.3f}' for k in ['mean_ms', 'p50_ms', 'p99_ms', 'p999_ms'])
        cpu = ' / '.join(f'{float(r[k]):.2f}' for k in ['leader_cpu_us_op', 'server_cpu_us_op'])
        gen = f"{float(r['generator_cpu_us_op']):.2f} / {float(r['generator_cores']):.2f}"
        alloc = f"{float(r['objects_per_op']):.2f} / {float(r['bytes_per_op']):,.0f}"
        gc = f"{100 * float(r['gc_cpu_share']):.2f}%" if r['gc_cpu_share'] else 'n/a'
        lines.append(f"| {label} | {float(r['ops_s']):,.0f} | {latency} | {cpu} | {gen} | {alloc} | {gc} |")
    tables.append('\n'.join(lines))
path = 'evidence/bench75/run-38400142994'
base = 'https://github.com/djosh34/tigerstore'
text = (root / 'report-base.md').read_text().replace('@HEADLINE_TABLE@', '\n'.join(headline)).replace('@DETAIL_TABLES@', '\n\n'.join(tables))
text = text.replace('@TREE@', f'{base}/tree/{commit}/{path}').replace('@BLOB@', f'{base}/blob/{commit}/{path}')
assert '@' not in text and '\u2014' not in text
(root / 'report.md').write_text(text)
