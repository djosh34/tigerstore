#!/usr/bin/env python3
import json
import pathlib
import statistics

root = pathlib.Path('results/scaling-artifacts')
print('| Runner | Workload | etcd ops/s at 1024 / 4096 / 8192 | tigerstore ops/s at 1024 / 4096 / 8192 | Tiger leader cores at 1024 / 4096 / 8192 |')
print('|---|---|---|---|---|')
for os, label in [('ubuntu-24.04-arm', 'arm64'), ('ubuntu-24.04', 'amd64')]:
    for work in ['put', 'txn', 'range']:
        counts = {}
        cores = []
        for store in ['etcd', 'tigerstore']:
            counts[store] = []
            for clients in [1024, 4096, 8192]:
                paths = list((root / f'scaling-{os}-{work}').glob(f'{work}-{clients}-{store}-[123].json'))
                runs = [json.load(p.open()) for p in paths]
                complete = len(runs) == 3 and all(r['valid'] and not r['errors'] for r in runs)
                counts[store].append(f"{statistics.median(r['throughput'] for r in runs):,.0f}" if complete else 'invalid' if runs else 'not run')
                if store == 'tigerstore':
                    cores.append(f"{statistics.median(r['server_cores_by_node'][0] for r in runs):.2f}" if complete else 'not run')
        print(f"| {label} | {work} | {' / '.join(counts['etcd'])} | {' / '.join(counts['tigerstore'])} | {' / '.join(cores)} |")
