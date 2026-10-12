#!/usr/bin/env python3
import collections
import json
import pathlib
import statistics
import sys

root = pathlib.Path(sys.argv[1])
rows = []
for path in sorted(root.glob('*.jsonl')):
    for line in path.read_text().splitlines():
        item = json.loads(line)
        item['_file'] = path.name
        rows.append(item)

def med(items, key, scale=1):
    return statistics.median(x[key] / scale for x in items)

def spread(items, key, scale=1):
    values = [x[key] / scale for x in items]
    return f'{min(values):.3f} to {max(values):.3f}'

print('# ARM request costs\n')
print('Five repetitions, one CPU per process, GOMAXPROCS=1. CPU is process user plus system time. Wall includes measurement-loop overhead. No overhead subtraction. These are not database rates.\n')
print('| Test | Median wall ns/op | Median CPU ns/op | CPU range ns/op |')
print('| --- | ---: | ---: | --- |')
costs = collections.defaultdict(list)
for row in rows:
    if row.get('kind') == 'cost':
        costs[row['name']].append(row)
assert len(costs) == 19, (len(costs), list(costs))
for name, items in costs.items():
    assert len(items) == 5, (name, len(items))
    print(f'| {name} | {med(items,"wall_ns_per_op"):.2f} | {med(items,"cpu_ns_per_op"):.2f} | {spread(items,"cpu_ns_per_op")} |')
print('\n## Write and fsync\n')
print('Cyclic overwrite of initialized 64MiB files. File and directory sync before timing. Two-second windows. Quantiles are medians of per-run quantiles, not pooled quantiles.\n')
print('| Batch | Wall us/batch | CPU us/batch | Fsync-only wall us | Fsync-only CPU us | Write+sync p50 us | Write+sync p99 us | Batches/s |')
print('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |')
for size in (4096, 65536, 1048576):
    items = [x for x in rows if x.get('kind') == 'fsync' and x['batch_bytes'] == size]
    assert len(items) == 5
    p50 = statistics.median(x['write_sync_quantiles']['p50_ns']/1000 for x in items)
    p99 = statistics.median(x['write_sync_quantiles']['p99_ns']/1000 for x in items)
    print(f'| {size} B | {med(items,"wall_ns_per_batch",1000):.3f} | {med(items,"cpu_ns_per_batch",1000):.3f} | {med(items,"fsync_wall_ns_per_batch",1000):.3f} | {med(items,"fsync_cpu_ns_per_batch",1000):.3f} | {p50:.3f} | {p99:.3f} | {med(items,"batches_per_second"):.1f} |')
print('\n## Three-process TLS loopback\n')
print('One leader and two followers on distinct CPUs. Wait for local completion plus first follower for quorum latency. Drain both followers before the next entry. The reported rate is all-followers-drained, not a pipelined quorum rate.\n')
print('| Mode | Quorum p50 us | Quorum p99 us | All-drained op/s | Leader CPU us/op | Follower1 CPU us/op | Follower2 CPU us/op |')
print('| --- | ---: | ---: | ---: | ---: | ---: | ---: |')
for mode in ('nondurable', 'durable'):
    group = [x for x in rows if x.get('kind') == 'quorum-process' and x['mode'] == mode]
    leaders = [x for x in group if x['role'] == 'leader']
    f1 = [x for x in group if x['role'] == 'follower1']
    f2 = [x for x in group if x['role'] == 'follower2']
    assert len(leaders) == len(f1) == len(f2) == 5
    p50 = statistics.median(x['quorum_quantiles']['p50_ns']/1000 for x in leaders)
    p99 = statistics.median(x['quorum_quantiles']['p99_ns']/1000 for x in leaders)
    print(f'| {mode} | {p50:.3f} | {p99:.3f} | {med(leaders,"ops_per_second"):.1f} | {med(leaders,"cpu_ns_per_op",1000):.3f} | {med(f1,"cpu_ns_per_op",1000):.3f} | {med(f2,"cpu_ns_per_op",1000):.3f} |')
print('\n## Complete no-op gRPC Put over TLS\n')
print('Actual grpc-go unary dispatch, actual etcd PutRequest and PutResponse. One connection, separate CPU for client and server. Closed-loop workers. No Raft, storage, apply, auth or watches.\n')
print('| Workers | RPC/s | RPC p50 us | RPC p99 us | Client CPU us/op | Server CPU us/op |')
print('| --- | ---: | ---: | ---: | ---: | ---: |')
for concurrency in ('1', '64'):
    group = [x for x in rows if x.get('kind') == 'rpc-process' and x['concurrency'] == concurrency]
    clients = [x for x in group if x['role'] == 'client']
    servers = [x for x in group if x['role'] == 'server']
    assert len(clients) == len(servers) == 5
    for c in clients:
        s = next(s for s in servers if s['rep'] == c['rep'])
        assert s['n'] == c['n']
    p50 = statistics.median(x['latency']['p50_ns']/1000 for x in clients)
    p99 = statistics.median(x['latency']['p99_ns']/1000 for x in clients)
    print(f'| {concurrency} | {med(clients,"ops_per_second"):.1f} | {p50:.3f} | {p99:.3f} | {med(clients,"cpu_ns_per_op",1000):.3f} | {med(servers,"cpu_ns_per_op",1000):.3f} |')
print('\nRaw JSONL, compiler/build info, full module versions, mount checks and thread affinity are in the artifact. See README.md in the exact experiment commit for timing and durability semantics.\n')
