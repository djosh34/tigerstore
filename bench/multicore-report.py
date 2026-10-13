#!/usr/bin/env python3
"""Validate all repetitions and print medians without pooling latencies."""
import json
import pathlib
import statistics
import sys

root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else 'results')
venues = [('github-arm64', 'GitHub ubuntu-24.04-arm, 4 cores'),
          ('github-amd64', 'GitHub ubuntu-24.04, amd64, 4 vCPUs'),
          ('host', 'Host arm64, 8 vCPUs, 11 GiB RAM, 3 GiB scope')]
summary = {}
for venue, title in venues:
    summary[venue] = {}
    print('### ' + title + '\n')
    print('| Workload | Clients | Store | ops/s, median and min to max | p50 / p99 ms | CPU seconds/op, L / F / F / G | Average cores, L / F / F / G | Machine cores / busy % |')
    print('|---|---:|---|---|---|---|---|---|')
    for work in ['put', 'txn', 'range']:
        for clients in [64, 256, 1024]:
            for store in ['etcd', 'tigerstore']:
                paths = [root / venue / f'{work}-{clients}-{store}-{rep}.json' for rep in [1, 2, 3]]
                runs = [json.load(p.open()) for p in paths]
                for r in runs:
                    assert r['valid'] and not r['errors'] and r['cohort_failed'] == 0 and r['unsent'] == 0, paths
                    assert r['offered_rate'] == 0 and r['conns'] * r['streams_per_conn'] == clients
                    assert r['value_size'] == 1024 and r['keys'] == 10000
                    assert len(r['server_cores_by_node']) == 3
                    assert abs(r['cpu_window_s'] - r['duration_s']) < 0.2
                med = lambda key: statistics.median(key(r) for r in runs)
                throughput = [r['throughput'] for r in runs]
                cpu = [med(lambda r, i=i: r['server_cpu_us_per_op_by_node'][i]) / 1e6 for i in range(3)]
                cpu.append(med(lambda r: r['client_cpu_us_per_op']) / 1e6)
                cores = [med(lambda r, i=i: r['server_cores_by_node'][i]) for i in range(3)]
                cores.append(med(lambda r: r['client_cores']))
                entry = {'throughput': statistics.median(throughput), 'min': min(throughput), 'max': max(throughput),
                         'p50_ms': med(lambda r: r['latency_us']['p50']) / 1000,
                         'p99_ms': med(lambda r: r['latency_us']['p99']) / 1000,
                         'cpu_seconds_per_op': cpu, 'cores': cores,
                         'machine_cores': med(lambda r: r['machine_cores']),
                         'machine_pct': med(lambda r: r['machine_busy_pct']),
                         'iowait_pct': med(lambda r: r['machine_iowait_pct']),
                         'steal_pct': med(lambda r: r['machine_steal_pct']),
                         'background_cores': med(lambda r: r['machine_cores'] - sum(r['server_cores_by_node']) - r['client_cores'])}
                summary[venue][f'{work}-{clients}-{store}'] = entry
                print(f"| {work} | {clients} | {store} | {entry['throughput']:,.0f}, {min(throughput):,.0f} to {max(throughput):,.0f} | {entry['p50_ms']:.3f} / {entry['p99_ms']:.3f} | {' / '.join(f'{c:.2e}' for c in cpu)} | {' / '.join(f'{c:.2f}' for c in cores)} | {entry['machine_cores']:.2f} / {entry['machine_pct']:.1f}% |")
    print()
(root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
