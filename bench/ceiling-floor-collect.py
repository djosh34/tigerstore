#!/usr/bin/env python3
"""Collect downloaded ceiling-floor artifacts, retaining every timing sample.
Usage: python3 bench/ceiling-floor-collect.py <artifact parent> <output prefix>
"""
import collections, glob, json, pathlib, statistics, sys
root, prefix = sys.argv[1:]
prims = collections.defaultdict(lambda: collections.defaultdict(list))
nulls = collections.defaultdict(lambda: collections.defaultdict(list))
for directory in sorted(glob.glob(root + '/ceiling-floor-*')):
    runner = pathlib.Path(directory).name.rsplit('-', 1)[1]
    for line in open(directory + '/primitives.txt'):
        if not line.startswith('Benchmark'):
            continue
        fields = line.split()
        metric = 'ns/item' if 'ns/item' in fields else 'ns/op'
        item = dict(ns=float(fields[fields.index(metric)-1]),
                    batch_ns=float(fields[fields.index('ns/op')-1]),
                    bytes_per_batch=float(fields[fields.index('B/op')-1]),
                    allocs_per_batch=float(fields[fields.index('allocs/op')-1]))
        prims[fields[0]][runner].append(item)
    for file in sorted(glob.glob(directory + '/null-*.json')):
        result = json.load(open(file))
        assert result['valid']
        nulls[result['label'] + '/' + result['workload']][runner].append(result)
pathlib.Path(prefix).parent.mkdir(parents=True, exist_ok=True)
json.dump(dict(primitives=prims, nulls=nulls), open(prefix + '.json', 'w'), indent=2)

def spread(v, digits=3):
    return ' / '.join(f'{x:.{digits}f}' for x in [min(v), statistics.median(v), max(v)])

with open(prefix + '.md', 'w') as out:
    def p(s=''):
        print(s, file=out)
    p('# All timing measurements')
    p('Three repetitions per independent runner; each cell min / median / max. Raw JSON preserves every timing, batch size and allocation statistic. ns/item is per client operation for batched encode/Raft/WAL/peer/log-write; all others are per invocation. TLS is seal + open, not one direction. ACK decode and syscall rows are per ACK/syscall, not per client operation.')
    p()
    p('| Primitive (BenchmarkFloor prefix omitted) | Runner 1 ns/item | Runner 2 ns/item |')
    p('|---|---:|---:|')
    for name, runners in prims.items():
        p('| ' + name.removeprefix('BenchmarkFloor') + ' | ' + ' | '.join(spread([r['ns'] for r in runners[key]]) for key in ['1', '2']) + ' |')
    p()
    p('| Null endpoint | Runner | ops/s | server us/op | generator us/op | server cores | generator cores |')
    p('|---|---|---:|---:|---:|---:|---:|')
    for name, runners in sorted(nulls.items()):
        for runner, results in sorted(runners.items()):
            columns = [name, runner]
            for key in ['throughput', 'server_cpu_us_per_op', 'client_cpu_us_per_op', 'server_cores_by_node', 'client_cores']:
                columns.append(spread([r[key][0] if isinstance(r[key], list) else r[key] for r in results]))
            p('| ' + ' | '.join(columns) + ' |')
