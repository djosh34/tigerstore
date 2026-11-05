#!/usr/bin/env python3
"""Issue #484 tables. Run ceiling-mixed-analyze.py on both cluster artifact roots
first; then: python3 bench/ceiling-mixed-report.py <cluster-root> <diagnostic-root>
<primitive-root>. Prints Markdown, with no third-party dependencies.
"""
import collections
import json
import pathlib
import re
import statistics as st
import sys


def spread(v):
    return ' / '.join(f'{x:.4g}' for x in [min(v), st.median(v), max(v)])


def table(header, rows):
    print('| ' + ' | '.join(header) + ' |')
    print('|' + '|'.join(['---'] * len(header)) + '|')
    for row in rows:
        print('| ' + ' | '.join(map(str, row)) + ' |')
    print()


def main(old, diag, prim):
    runs = json.loads((old/'combined.json').read_text())
    diagnostics = json.loads((diag/'combined.json').read_text())
    configs = [(0,48), (2,48), (10,48), (10,96)]
    print('# Mixed #484 numerical evidence\n\nAll spreads are min / median / max; not pooled latency percentiles. Cluster cells have 6 observations (3 per runner); diagnostics 2 (1 per runner); microbenchmarks 3 per runner. CPU projections are extrapolations, NOT measured maximum throughput. IP counters include headers/ACKs; loopback OUTPUT counts each transmitted packet once.\n')
    print('## Netem verification\nEach file contains the median of 20 TCP connects: delayed peer port 20021, undelayed client port 20020.\n')
    rows=[]
    for root,run in [(old,'42019370875'),(diag,'42023145903')]:
        for f in sorted(root.glob('**/probe-*.txt')):
            measured = dict(re.findall(r'port (\d+): median connect ([\d.]+) ms', f.read_text()))
            rows.append([run,f.parent.name,f.stem.removeprefix('probe-'),measured['20021'],measured['20020']])
    table(['run','runner','RTT-streams','peer median ms','client median ms'], rows)
    print('## Cluster measurements, run 42019370875\n')
    metrics = {
        'throughput ops/s': lambda r:r['throughput'],
        **{f'{name} cores':lambda r,i=i:r['server_cores_by_node'][i] for i,name in enumerate(['leader','follower1','follower2'])},
        'generator cores':lambda r:r['client_cores'],
        **{f'{name} CPU us/op':lambda r,i=i:r['server_cpu_us_per_op_by_node'][i] for i,name in enumerate(['leader','follower1','follower2'])},
        'server sum CPU us/op':lambda r:r['server_cpu_us_per_op'],
        'generator CPU us/op':lambda r:r['client_cpu_us_per_op'],
        'disk writes B/op':lambda r:r['disk_bytes_per_op'],
        'peer IP B/op':lambda r:r['peer_ip_bytes_per_op'],
        'client IP B/op':lambda r:r['client_ip_bytes_per_op'],
        'unary request protobuf B/op':lambda r:sum(x['request_pb_bytes'] for x in r['detail']['ops_get_list_update_create_delete'])/r['ops'],
        'unary reply protobuf B/op':lambda r:sum(x['reply_pb_bytes'] for x in r['detail']['ops_get_list_update_create_delete'])/r['ops'],
        'watch protobuf B/op':lambda r:r['detail']['watch_pb_bytes']/r['ops'],
        'watch events/op':lambda r:r['watch']['events']/r['ops'],
        'mean latency ms':lambda r:r['latency_us']['mean']/1000,
        'occupancy (Little law)':lambda r:r['inflight_little_law'],
        'generator one-core projection ops/s':lambda r:r['generator_cpu_projection_ops_s'],
        'leader one-core projection ops/s':lambda r:r['leader_cpu_projection_ops_s'],
        'three-server-core projection ops/s':lambda r:r['aggregate_cpu_projection_ops_s'],
        'write-only disk projection at 343 MiB/s':lambda r:343*2**20/r['disk_bytes_per_op'],
    }
    subset = lambda source,c:[r for r in source if (r['rtt'],r['streams'])==c]
    table(['metric']+[f'RTT {r}, {s} streams/conn' for r,s in configs], [[name]+[spread([get(r) for r in subset(runs,c)]) for c in configs] for name,get in metrics.items()])
    table(['runner','RTT','streams/conn','throughput ops/s'], [[runner,c[0],c[1],spread([r['throughput'] for r in subset(runs,c) if r['runner']==runner])] for runner in sorted({r['runner'] for r in runs}) for c in configs])
    print('## Per-operation measurements, run 42019370875\nProtobuf body bytes, excluding gRPC/HTTP2/TLS/TCP/IP. LIST keys are returned keys, not Count before the 500-key limit.\n')
    rows=[]
    for c in configs:
        for i,op in enumerate(['get','list','update','create','delete']):
            a=[r['detail']['ops_get_list_update_create_delete'][i] for r in subset(runs,c)]
            rows.append([c[0],c[1],op]+[spread([x[k]/x['ops']/div for x in a]) for k,div in [('request_pb_bytes',1),('reply_pb_bytes',1),('list_keys',1),('latency_ns',1e6)]])
    table(['RTT','streams','op','request B','reply B','LIST keys','mean latency ms'], rows)
    print('## Diagnostic run 42023145903\nNew runners, not additional repetitions on the original runners. External IO boundaries follow markers with up to ~50 ms polling delay. /proc/pid/io write_bytes is storage IO charged to a process; wchar includes sockets and is deliberately NOT described as WAL bytes.\n')
    dm = {k:v for k,v in metrics.items() if k in ['throughput ops/s','generator cores','generator CPU us/op','leader CPU us/op','disk writes B/op','peer IP B/op','client IP B/op'] or k.endswith(' cores')}
    dm.update({'device read B/op':lambda r:r['disk_read_bytes_per_op'],
               **{f'{name} client IP B/op':lambda r,i=i:r['client_ip_bytes_per_op_by_node'][i] for i,name in enumerate(['leader','follower1','follower2'])},
               **{f'{name} charged storage write B/op':lambda r,i=i:r['storage_write_bytes_per_op_by_node'][i] for i,name in enumerate(['leader','follower1','follower2'])}})
    table(['metric','RTT 0','RTT 2','RTT 10'], [[name]+[spread([get(r) for r in subset(diagnostics,c)]) for c in configs[:3]] for name,get in dm.items()])
    for source,title in [(runs,'42019370875'),(diagnostics,'42023145903')]:
        print(f'## Exclusive profile attribution, run {title}\nEach sample is assigned to the first matching category in ceiling-mixed-analyze.py. GC means background GC only; assists stay charged to callers. TLS includes record handling and IO, not just AES. Shares from 9 s server profiles multiply the full 10 s window CPU/op.\n')
        for role in ['leader','follower','generator']:
            print('### '+role+'\n')
            profiles={rtt:[r['profiles'][role] for r in source if r['rtt']==rtt and role in r['profiles']] for rtt in [0,10]}
            cats=sorted({k for a in profiles.values() for p in a for k in p['categories']})
            table(['category','RTT0 %','RTT0 us/op','RTT10 %','RTT10 us/op'], [[k]+[spread([p['categories'].get(k,{}).get(unit,0) for p in profiles[rtt]]) for rtt in [0,10] for unit in ['percent','us_per_op']] for k in cats])
    print('## Microbenchmarks, run 42019785137\nNanoseconds per operation, three repeats, pinned CPU 3, -test.cpu 1. AES uses AES-128-GCM. General protobuf and production encoders are different APIs. Weighted LIST is a hot synthetic page model, not an exact replay of all measured pages.\n')
    benches=collections.defaultdict(lambda:collections.defaultdict(list))
    for f in prim.glob('**/*.txt'):
        for name,ns in re.findall(r'^(BenchmarkResearch484\S+)\s+\d+\s+([\d.]+) ns/op', f.read_text(), re.M):
            benches[name][f.parent.name].append(float(ns))
    table(['benchmark ns/op','runner1','runner2'], [[name]+[spread(byrunner[f'ceiling-mixed-{n}']) for n in [1,2]] for name,byrunner in benches.items()])
    print('## Diskbench, all runs that measured it\nMiB/s, O_DIRECT overwrite of already-written space plus fdatasync after every group. The largest group is NOT a per-request fsync latency measurement.\n')
    rows=[]
    for root,run in [(old,'42019370875'),(prim,'42019785137')]:
        for f in sorted(root.glob('**/diskbench.jsonl')):
            for line in f.read_text().splitlines():
                x=json.loads(line)
                rows.append([run,f.parent.name,x.get('group_bytes','best'),f"{x.get('mib_per_s',x.get('max_mib_per_s')):.3f}"])
    table(['run','runner','group B','MiB/s'],rows)
    print('## Partial primitive floor MODEL (not a measured end-to-end ceiling)\nASSUMPTIONS / guesses: requests and watch work evenly balanced over nodes; 4.17 GB/s raw AEAD, proportional scaling of the hot LIST encoder to the measured mean page; synthetic single-event watch sizes. Excludes indexing, checksums, Raft protobuf, storage CPU, GC, HTTP2, syscalls and kernel networking. Floor is only for the included primitive work. Request protobuf payload is replicated twice and forwarded for 2/3 of writes; leader peer crypto = 8B/3, each follower = 4B/3. WAL payload means 3 times the request body, not filesystem device bytes.\n')
    models = [
        ('get', .25, 54.6, 1407, .9*1.03, 0, 0, False),
        ('list', .10, 74.4, 291177, 103.1*291177/302044, 0, 0, False),
        ('update', .40, 1595, 33, .158, 3072, 1.54, True),
        ('create', .15, 1558.8, 33, .1598, 1546, 1.08, True),
        ('delete', .10, 120.5, 35, .1773, 1608, 1.149, True),
    ]
    rows=[]
    leader=follower=payload=0
    for op,weight,request,reply,encode,watchbytes,watchencode,write in models:
        common=(encode+(request+reply)/4170+watchencode+watchbytes/4170)/3
        l=common+(8/3*request/4170 if write else 0)
        f=common+(4/3*request/4170 if write else 0)
        disk=3*request if write else 0
        leader+=weight*l; follower+=weight*f; payload+=weight*disk
        rows.append([op,f'{l:.3f}',f'{f:.3f}',f'{disk:.1f}'])
    rows.append(['mix',f'{leader:.3f}',f'{follower:.3f}',f'{payload:.1f}'])
    table(['op','leader us/op, MODEL','each follower us/op, MODEL','WAL request payload B/op, MODEL'],rows)
    table(['MODEL bound','RTT0','RTT2','RTT10'],[
        ['leader core',*[f'{1e6/leader:.0f}']*3],
        ['three cores, balanced total',*[f'{3e6/(leader+2*follower):.0f}']*3],
        ['disk 343 MiB/s / request payload ONLY',*[f'{343*2**20/payload:.0f}']*3],
        ['288 in flight / (5 RTT / 3), no storage/CPU wait','not finite','86400','17280'],
    ])
    print('These optimistic subset calculations do not establish that tigerstore can reach them. The actual measured CPU-cost projections, storage traffic and generator ceiling above are much tighter for the present implementation.\n')
    print('## Dump table statistics\nComputed exactly by object counts from internal/k8ssize/k8ssize.go; p99 uses nearest rank. Actual generated resource sizes are conditional draws from that same histogram.\n')
    text=(pathlib.Path(__file__).parent.parent/'internal/k8ssize/k8ssize.go').read_text()
    buckets=[(r,int(n),int(c)) for r,n,c in re.findall(r'\{"([^"]+)", (\d+), (\d+)\}',text)]
    resources=list(dict.fromkeys(r for r,_,_ in buckets))
    rows=[]
    for resource in ['ALL']+resources:
        values=sorted(n for r,n,c in buckets if resource=='ALL' or r==resource for _ in range(c))
        rows.append([resource,len(values),f'{st.mean(values):.3f}',st.median(values),values[(99*len(values)+99)//100-1],min(values),max(values),'-' if resource=='ALL' else resources.index(resource)%3+1])
    table(['resource','objects','mean B','median B','p99 B','min B','max B','watch endpoint node'],rows)


if __name__=='__main__':
    main(*map(pathlib.Path,sys.argv[1:]))
