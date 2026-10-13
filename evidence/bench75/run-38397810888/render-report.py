#!/usr/bin/env python3
import csv
import sys
from pathlib import Path

root = Path(__file__).resolve().parent
commit = sys.argv[1]
assert len(commit) == 40 and all(c in '0123456789abcdef' for c in commit)
rows = list(csv.DictReader((root / 'summary.csv').open()))
tables = []
for op, unit in [('put', 'Put'), ('cas', 'Txn')]:
    data = {r['store']: r for r in rows if r['op'] == op}
    lines = [f'### {unit}, 256 clients', '',
             f'| Store | {unit}/s | Mean / p50 / p99 / p99.9 ms | Leader / total CPU µs/{unit} | Generator µs/{unit} / cores | Leader objects / bytes per {unit} | GC CPU share | Leader syscr / syscw per {unit} |',
             '|---|---:|---:|---:|---:|---:|---:|---:|']
    for store, label in [('baseline', 'main'), ('candidate', 'PR 80'), ('etcd', 'etcd 3.7.2')]:
        r = data[store]
        latency = ' / '.join(f'{float(r[k]):.3f}' for k in ['mean_ms', 'p50_ms', 'p99_ms', 'p999_ms'])
        cpu = ' / '.join(f'{float(r[k]):.2f}' for k in ['leader_cpu_us_op', 'server_cpu_us_op'])
        gen = f"{float(r['generator_cpu_us_op']):.2f} / {float(r['generator_cores']):.2f}"
        alloc = f"{float(r['objects_per_op']):.2f} / {float(r['bytes_per_op']):,.0f}"
        gc = f"{100 * float(r['gc_cpu_share']):.2f}%" if r['gc_cpu_share'] else 'n/a'
        io = ' / '.join(f'{float(r[k]):.4f}' for k in ['leader_syscr_per_op', 'leader_syscw_per_op'])
        lines.append(f"| {label} | {float(r['ops_s']):,.0f} | {latency} | {cpu} | {gen} | {alloc} | {gc} | {io} |")
    tables.append('\n'.join(lines))
path = 'evidence/bench75/run-38397810888'
base = 'https://github.com/djosh34/tigerstore'
text = (root / 'report-base.md').read_text().replace('@TABLES@', '\n\n'.join(tables))
text = text.replace('@TREE@', f'{base}/tree/{commit}/{path}').replace('@BLOB@', f'{base}/blob/{commit}/{path}')
assert '@' not in text and '\u2014' not in text
(root / 'report.md').write_text(text)
