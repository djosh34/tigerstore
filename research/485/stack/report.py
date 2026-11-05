#!/usr/bin/env python3
"""Report medians of independently repeated Go/fio measurements; preserve raw data."""
import collections
import json
from pathlib import Path
import re
import statistics
import sys

out = Path(sys.argv[1])
print('# Plan 5 retained stack, native floor')
print('```\n' + (out / 'hardware.txt').read_text() + '```')
for file in sorted(out.glob('*.txt')):
    samples = collections.defaultdict(list)
    for line in file.read_text().splitlines():
        m = re.match(r'(Benchmark\S+)-\d+\s+\d+\s+([\d.]+) ns/op\s+(?:([\d.]+) MB/s\s+)?(\d+) B/op\s+(\d+) allocs/op', line)
        if m:
            name, ns, mb, size, alloc = m.groups()
            samples[name].append((float(ns), float(size), float(alloc), float(mb or 0)))
    if not samples:
        continue
    print(f'\n## {file.stem}\n')
    print('| Primitive | n | ns/op | B/op | allocs/op | MB/s |\n|---|---:|---:|---:|---:|---:|')
    for name, values in sorted(samples.items()):
        assert len(values) >= 5, (file, name, len(values))
        med = [statistics.median(col) for col in zip(*values)]
        print(f'| {name} | {len(values)} | ' + ' | '.join(f'{x:,.2f}' for x in med) + ' |')
print('\n## Disk medians (5 repetitions, 5 s + 1 s warm-up each)\n')
print('| Test | MB/s (decimal) | IOPS |\n|---|---:|---:|')
for kind in ['seqwrite','seqread','fsync','randread']:
    records = []
    for f in sorted(out.glob(f'fio-{kind}-*.json')):
        j = json.loads(f.read_text())['jobs'][0]
        assert j['error'] == 0, (f, j['error'])
        v = j['write' if kind in ['seqwrite','fsync'] else 'read']
        records.append((v['bw_bytes']/1e6, v['iops']))
    assert len(records) >= 5, (kind, len(records))
    print(f'| {kind} | ' + ' | '.join(f'{statistics.median(col):,.2f}' for col in zip(*records)) + ' |')
