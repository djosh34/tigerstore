#!/usr/bin/env python3
"""Issue #484: deterministic raw-result and exclusive CPU-profile accounting.
Usage: python3 bench/ceiling-mixed-analyze.py <downloaded-artifacts-root>
No external Python packages. pprof sample units are CPU nanoseconds.
"""
import collections
import gzip
import json
import pathlib
import re
import statistics
import sys


def varint(b, at):
    x = shift = 0
    while True:
        c = b[at]
        at += 1
        x |= (c & 127) << shift
        if not c & 128:
            return x, at
        shift += 7


def fields(b):
    at = 0
    while at < len(b):
        tag, at = varint(b, at)
        kind = tag & 7
        if kind == 0:
            v, at = varint(b, at)
        elif kind == 2:
            n, at = varint(b, at)
            v = b[at:at+n]
            at += n
        elif kind in (1, 5):
            n = 8 if kind == 1 else 4
            v = b[at:at+n]
            at += n
        else:
            raise ValueError(kind)
        yield tag >> 3, v


def packed(b):
    if isinstance(b, int):
        return [b]
    out = []
    at = 0
    while at < len(b):
        v, at = varint(b, at)
        out.append(v)
    return out


def profile(path):
    top = list(fields(gzip.decompress(path.read_bytes())))
    strings = [v.decode() for n, v in top if n == 6]
    fun = {}
    loc = {}
    samples = []
    for n, v in top:
        if n == 5:
            f = dict(fields(v))
            fun[f[1]] = strings[f.get(2, 0)]
        elif n == 4:
            lines = list(fields(v))
            ident = dict(lines)[1]
            loc[ident] = [dict(fields(line))[1] for k, line in lines if k == 4]
        elif n == 2:
            locations = []
            values = []
            for k, val in fields(v):
                if k == 1:
                    locations += packed(val)
                elif k == 2:
                    values += packed(val)
            samples.append((locations, values[-1]))
    rows = []
    for locations, ns in samples:
        stack = [fun[f] for lid in locations for f in loc[lid]]
        rows.append((stack, ns))
    return rows


# First matching category wins: exclusive categories sum to sampled CPU.
SERVER = [
    ('GC background', r'runtime\.(gcBgMarkWorker|bgsweep|bgscavenge)'),
    ('TLS incl IO', r'crypto/tls\.'),
    ('range/list encode', r'internal/server\.(appendRangeReply|appendKV|kvSize)'),
    ('range/list index/read', r'internal/mvcc\.\(\*(Store|writer)\)\.(RangeKVs|rangeResult|rangeKeys|Range|fill|value)|internal/core\.\(\*Node\)\.serveRead'),
    ('watch fanout/encode', r'internal/(core\.\(\*Node\)\.(matchWatches|flushWatches|watchReply|watchEvent)|server\.(\(\*Server\)\.watch|sendFragments))'),
    ('Raft', r'go.etcd.io/raft'),
    ('apply', r'internal/core\.\(\*Node\)\.(runApply|apply)|internal/mvcc\.\(\*(Store|writer)\)\.(Txn|txn|put|Put|deleteRange)'),
    ('peer transport', r'internal/peer\.'),
    ('WAL persist', r'internal/wal\.|internal/node\.\(\*Node\)\.(persist|syncGroups)'),
    ('other reply encode', r'internal/server\.(\(\*conn\)\.(complete|completeWaiting)|appendTxnReply|marshal)'),
    ('client admit/decode', r'internal/server\.\(\*conn\)\.(admit|flush)'),
    ('HTTP2 read/write', r'internal/server\.\(\*conn\)\.(readLoop|writeLoop)'),
]
CLIENT = [
    ('GC background', r'runtime\.(gcBgMarkWorker|bgsweep|bgscavenge)'),
    ('TLS incl IO', r'crypto/tls\.'),
    ('LIST/get validation', r'main\.(checkMixedRange|checkMixedKV)|main\.\(\*mixedLoad\)\.message'),
    ('watch validation/hash', r'main\.\(\*mixedWatch\)|main\.(eventDigest|mixedKV)|main\.\(\*mixedLedger\)'),
    ('choose/build requests', r'main\.\(\*mixedLoad\)\.(choose|start|value|newKey|eligible)|main\.mixedTxn|internal/k8ssize\.Draw'),
    ('HTTP2/control', r'main\.\(\*conn\)\.(readLoop|read|onData|onHeaderBlock|flush|startStream|messages)'),
]


def spread(v):
    return ' / '.join(f'{x:.3f}' for x in (min(v), statistics.median(v), max(v)))


def main(root):
    runs = []
    for f in sorted(root.glob('**/result.json')):
        result = json.loads(f.read_text())
        result['run'] = str(f.parent.relative_to(root))
        result['detail'] = json.loads((f.parent/'detail.json').read_text())
        result['runner'] = f.parent.parent.name
        result['rep'] = int(re.search(r'rep-(\d+)', f.parent.name)[1])
        result['rtt'] = int(re.search(r'rtt-(\d+)', f.parent.name)[1])
        result['profiles'] = {}
        result['streams'] = int(re.search(r'streams-(\d+)', f.parent.name)[1])
        assert result['valid'], f
        assert sum(x['ops'] for x in result['detail']['ops_get_list_update_create_delete']) == result['ops'], f
        place = json.loads((f.parent/'placement.json').read_text())
        leader_port = int(place['leader'].split(':')[1])
        ports = [leader_port]+[p for p in [20010,20020,20030] if p != leader_port]
        raw_start = (f.parent/'wire-start.txt').read_text()
        raw_stop = (f.parent/'wire-stop.txt').read_text()
        # No-target rules count traffic without returning from the chain.
        pattern = r'\[\d+:(\d+)\] -A R484 [^\n]*--ports (\d+)$'
        a = {int(p):int(b) for b,p in re.findall(pattern, raw_start, re.M)}
        b = {int(p):int(b) for b,p in re.findall(pattern, raw_stop, re.M)}
        if a:
            result['client_ip_bytes_per_op_by_node'] = [(b[p]-a[p])/result['ops'] for p in ports]
        if (f.parent/'diskstats-start.txt').exists():
            def diskstats(which):
                return {v[2]:[int(v[5])*512, int(v[9])*512] for line in (f.parent/f'diskstats-{which}.txt').read_text().splitlines() if len(v:=line.split()) >= 10}
            da, db = diskstats('start'), diskstats('stop')
            result['disk_read_bytes_per_op'] = (db['sda1'][0]-da['sda1'][0])/result['ops']
            result['external_disk_write_bytes_per_op'] = (db['sda1'][1]-da['sda1'][1])/result['ops']
            def proc_io(pid, which):
                return {k:int(v) for k,v in re.findall(r'(\w+): (\d+)', (f.parent/f'io-{pid}-{which}.txt').read_text())}
            # write_bytes is storage IO charged to a process. wchar is NOT
            # WAL bytes: sockets are included, so deliberately do not use it.
            result['storage_write_bytes_per_op_by_node'] = [(proc_io(p,'stop')['write_bytes']-proc_io(p,'start')['write_bytes'])/result['ops'] for p in place['pids_leader_first'].split(',')]
        starts = re.findall(r'\[(\d+):(\d+)\] -A R484 [^\n]*-j RETURN', (f.parent/'wire-start.txt').read_text())
        stops = re.findall(r'\[(\d+):(\d+)\] -A R484 [^\n]*-j RETURN', (f.parent/'wire-stop.txt').read_text())
        for i, name in enumerate(['peer_ip_bytes_per_op', 'client_ip_bytes_per_op']):
            result[name] = (int(stops[i][1])-int(starts[i][1]))/result['ops']
        result['generator_cpu_projection_ops_s'] = 1e6/result['client_cpu_us_per_op']
        result['leader_cpu_projection_ops_s'] = 1e6/result['server_cpu_us_per_op_by_node'][0]
        result['aggregate_cpu_projection_ops_s'] = 3e6/result['server_cpu_us_per_op']
        result['inflight_little_law'] = result['throughput']*result['latency_us']['mean']/1e6
        runs.append(result)
        for role in ['leader', 'follower', 'generator']:
            p = f.parent/(role+'.pprof')
            if not p.exists():
                continue
            cats = CLIENT if role == 'generator' else SERVER
            counted = collections.Counter()
            rows = profile(p)
            cpu = sum(ns for _, ns in rows)
            for stack, ns in rows:
                text = '\n'.join(stack)
                cat = next((label for label, regex in cats if re.search(regex, text)), 'other')
                counted[cat] += ns
            if role == 'generator':
                us = result['client_cpu_us_per_op']
            else:
                # placement uses arbitrary first nonleader as the follower profile.
                # Locate that node's PID in the leader-first placement.
                if role == 'leader':
                    index = 0
                else:
                    place = json.loads((f.parent/'placement.json').read_text())
                    leader_port = int(place['leader'].split(':')[1])
                    # Endpoint order is node1, node2, node3; follower = first nonleader.
                    follower_port = 20020 if leader_port == 20010 else 20010
                    nonleaders = [p for p in [20010,20020,20030] if p != leader_port]
                    index = 1+nonleaders.index(follower_port)
                us = result['server_cpu_us_per_op_by_node'][index]
            print('\nPROFILE', result['run'], role, f'{cpu/1e9:.3f}s sampled', f'{us:.3f} us/op measured')
            result['profiles'][role] = {'sampled_cpu_s':cpu/1e9, 'us_per_op':us, 'categories':{label:{'percent':100*ns/cpu, 'us_per_op':us*ns/cpu} for label,ns in counted.items()}}
            for label, ns in counted.most_common():
                print(f'{label:27s} {100*ns/cpu:7.3f}% {us*ns/cpu:9.3f} us/op')
    for rtt, streams in sorted({(r['rtt'],r['streams']) for r in runs}):
        subset = [r for r in runs if r['rtt']==rtt and r['streams']==streams]
        print('\nGROUP', rtt, streams, len(subset))
        getters = {
            'ops/s': lambda r:r['throughput'],
            'leader cores': lambda r:r['server_cores_by_node'][0],
            'follower1 cores': lambda r:r['server_cores_by_node'][1],
            'follower2 cores': lambda r:r['server_cores_by_node'][2],
            'generator cores': lambda r:r['client_cores'],
            'server us/op': lambda r:r['server_cpu_us_per_op'],
            'leader us/op': lambda r:r['server_cpu_us_per_op_by_node'][0],
            'follower1 us/op': lambda r:r['server_cpu_us_per_op_by_node'][1],
            'follower2 us/op': lambda r:r['server_cpu_us_per_op_by_node'][2],
            'generator us/op': lambda r:r['client_cpu_us_per_op'],
            'disk write bytes/op': lambda r:r['disk_bytes_per_op'],
            'peer IP bytes/op': lambda r:r['peer_ip_bytes_per_op'],
            'client IP bytes/op': lambda r:r['client_ip_bytes_per_op'],
            'unary request protobuf bytes/op': lambda r:sum(x['request_pb_bytes'] for x in r['detail']['ops_get_list_update_create_delete'])/r['ops'],
            'unary reply protobuf bytes/op': lambda r:sum(x['reply_pb_bytes'] for x in r['detail']['ops_get_list_update_create_delete'])/r['ops'],
            'watch protobuf bytes/op': lambda r:r['detail']['watch_pb_bytes']/r['ops'],
            'watch events/op':lambda r:r['watch']['events']/r['ops'],
            'mean latency ms':lambda r:r['latency_us']['mean']/1000,
            'Little law occupancy':lambda r:r['inflight_little_law'],
            'generator CPU projection ops/s':lambda r:r['generator_cpu_projection_ops_s'],
            'leader CPU projection ops/s':lambda r:r['leader_cpu_projection_ops_s'],
            '3-core CPU projection ops/s':lambda r:r['aggregate_cpu_projection_ops_s'],
        }
        for name, getter in getters.items():
            print(f'{name:33s} {spread([getter(r) for r in subset])}')
        if all('disk_read_bytes_per_op' in r for r in subset):
            for name in ['disk_read_bytes_per_op','external_disk_write_bytes_per_op']:
                print(name, spread([r[name] for r in subset]))
            for name in ['client_ip_bytes_per_op_by_node','storage_write_bytes_per_op_by_node']:
                print(name, [spread([r[name][i] for r in subset]) for i in range(3)])
        for runner in sorted({r['runner'] for r in subset}):
            print('RUNNER',runner,'throughput',spread([r['throughput'] for r in subset if r['runner']==runner]))
        for i,name in enumerate(['get','list','update','create','delete']):
            op = [r['detail']['ops_get_list_update_create_delete'][i] for r in subset]
            print('OP',name,'request bytes',spread([x['request_pb_bytes']/x['ops'] for x in op]),'reply bytes',spread([x['reply_pb_bytes']/x['ops'] for x in op]),'keys',spread([x['list_keys']/x['ops'] for x in op]),'latency ms',spread([x['latency_ns']/x['ops']/1e6 for x in op]))
    (root/'combined.json').write_text(json.dumps(runs, indent=2)+'\n')


if __name__ == '__main__':
    main(pathlib.Path(sys.argv[1]))
