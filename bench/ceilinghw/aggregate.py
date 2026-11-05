#!/usr/bin/env python3
"""Aggregate the three downloaded artifacts; every cell is runner min / median / max.
Usage: aggregate.py /tmp/r484/hw-artifacts > report.md
"""
import json,pathlib,re,statistics,sys
root=pathlib.Path(sys.argv[1]); dirs=[root/f'ceiling-hw-{i}' for i in (1,2,3)]
def js(d,name):return json.loads((d/name).read_text())
def lines(d,name):return [json.loads(l) for l in (d/name).read_text().splitlines()]
def spread(v):return ' / '.join(f'{x:.3f}' for x in (min(v),statistics.median(v),max(v)))
def row(name,*values):print('| '+name+' | '+' | '.join(spread(v) for v in values)+' |')
def header(title,cols):print('\n## '+title+'\n\n| case | '+' | '.join(cols)+' |\n|---|'+'---:|'*len(cols))
print('# #484 hardware measurements\n\nEach cell: **minimum / median / maximum across 3 separate runners** (not pooled percentiles). Units explicitly labelled. Microbenchmarks use the median of 3 repetitions on each runner before computing spread. All other cases are one sequential measurement per runner. No confidence intervals implied; 2–6 second tests. Full runner identities/device settings and per-case raw JSON/text in the artifacts.\n\nMain measurement commit: `e475c4f0ce365a08af654fe29ab6aeea6a75e4c1`. Run: https://github.com/djosh34/tigerstore/actions/runs/42019692789\n\nActual tigerstore TLS negotiation commit: `c37a536d` (same application source). Run: https://github.com/djosh34/tigerstore/actions/runs/42020335843\n\nDisk case names: `d0` buffered / `d1` O_DIRECT; `pre` overwrites a fully written 1 GiB file per writer; `append` grows a new allocating file; w1/w3 independent writers pinned CPUs 0/0,1,2; b is write size bytes. fio uses psync qd1 with one sync per write, 4 seconds. Independent C companion uses pwrite+sync, 2 seconds, and measures the **whole durable cycle**, not a sum of phase percentiles. fio and companion are separate observations.\n')
print('**Limits:** These runners use kernel 7.0.0-1017-azure, not the earlier bench kernel 6.17.0-1022-azure. Same N2/4-core/16-GB class, same ext4 options and barrier remount. No power-loss or hypervisor-flush verification. O_DIRECT device reports write-through; durability is the guest syscall contract. p99.9 for large writes has insufficient samples (see samples column); it is an order statistic, not a precise tail estimate. TLS streaming counts bytes accepted by sender before deadline, not confirmed application bytes at receiver; at most a final socket/record backlog can remain. memcpy is cache-hot; streaming memory is 256 MiB per array per thread, above per-VM LLC. Memory numbers are useful bytes and a minimum read+write traffic count, not measured DRAM-controller bandwidth. UDP loss is receiver/kernel loss, not qdisc drops; all recorded qdiscs have zero drops. No physical NIC was measured.\n')
header('Durable append whole-cycle totals', ['C total MiB/s','fio total MiB/s'])
for f in sorted(dirs[0].glob('cycle-*.jsonl')):
 name=f.name; label=f.stem[6:]
 row(label,[lines(d,name)[-1]['total_mib_per_s'] for d in dirs],[sum(j['write']['bw_bytes'] for j in js(d,'fio-'+label+'.json')['jobs'])/2**20 for d in dirs])
header('Durable append whole-cycle per writer', ['MiB/s','writes/s','samples','p50 us','p99 us','p99.9 us'])
for f in sorted(dirs[0].glob('cycle-*.jsonl')):
 for i in range(len(lines(dirs[0],f.name))-1):
  rs=[lines(d,f.name)[i] for d in dirs];bs=int(re.search(r'-b(\d+)',f.name)[1])
  row(f.stem[6:]+f'/writer{i}',[r['mib_per_s'] for r in rs],[r['mib_per_s']*2**20/bs for r in rs],[r['writes'] for r in rs],*[ [r[k] for r in rs] for k in ('p50_us','p99_us','p999_us')])
header('fio per-writer rates and phase percentiles (NOT whole-cycle tails)', ['writes/s','MiB/s','write p50 us','write p99 us','write p99.9 us','sync p50 us','sync p99 us','sync p99.9 us'])
for f in sorted(dirs[0].glob('fio-d*.json')):
 for i in range(len(js(dirs[0],f.name)['jobs'])):
  rs=[js(d,f.name)['jobs'][i] for d in dirs]
  vals=[[r['write']['iops'] for r in rs],[r['write']['bw_bytes']/2**20 for r in rs]]
  for phase in ('write','sync'):
   for q in ('50.000000','99.000000','99.900000'):
    vals.append([r[phase]['clat_ns' if phase=='write' else 'lat_ns']['percentile'][q]/1000 for r in rs])
  row(f.stem[4:]+f'/writer{i}',*vals)
header('Sequential O_DIRECT no-sync, 1 MiB, libaio', ['MiB/s'])
for f in sorted(dirs[0].glob('fio-seq-*.json')):
 kind='read' if '-read-' in f.name else 'write'
 row(f.stem,[js(d,f.name)['jobs'][0][kind]['bw_bytes']/2**20 for d in dirs])
header('cmd/diskbench (one writer, O_DIRECT+fdatasync, prewritten, default 5s)', ['MiB/s'])
for i in range(4):
 rs=[lines(d,'diskbench.jsonl')[i] for d in dirs]
 key='max_mib_per_s' if i==3 else 'mib_per_s'
 row('maximum' if i==3 else str(rs[0]['group_bytes'])+' bytes',[r[key] for r in rs])
header('Memory (256 MiB source/destination per worker, 3s)', ['useful MiB/s','minimum traffic MiB/s'])
for f in sorted(dirs[0].glob('memory-*.json')):
 row(f.stem,*[[js(d,f.name)[k] for d in dirs] for k in ('useful_mib_per_s','minimum_traffic_mib_per_s')])
header('sysbench cpu --cpu-max-prime=20000 --threads=1 --time=4, taskset each core', ['events/s'])
for i in range(4):row('CPU '+str(i),[float(re.search(r'events per second:\s+([\d.]+)',(d/f'cpu-{i}.txt').read_text())[1]) for d in dirs])
header('Go primitives, CPU0, median of 3 x 300ms per runner', ['ns/op','ns/byte'])
for procs in (1,2):
 maps=[]
 for d in dirs:
  m={}
  for line in (d/f'primitives-p{procs}.txt').read_text().splitlines():
   match=re.match(r'BenchmarkPrimitives/(\S+?)\s+\d+\s+([\d.]+) ns/op',line)
   if match:m.setdefault(re.sub(r'-2$','',match[1]),[]).append(float(match[2]))
  maps.append(m)
 for name in maps[0]:
  n=int(name.split('/')[-1]);n=1078 if name.startswith('Put') else n
  v=[statistics.median(m[name]) for m in maps]
  row(f'P{procs}/{name}',v,[x/n for x in v])
header('TCP ping-pong 1 KiB (20 warmups, 3s), r=netem RTT ms, off=no qdisc', ['p50 us','p99 us','samples'])
for f in sorted(dirs[0].glob('ping-*.json')):
 row(f.stem[5:],*[[js(d,f.name)[k] for d in dirs] for k in ('p50_us','p99_us','n')])
header('TCP stream (4s), c=client CPU, s=server CPU, n=streams', ['MiB/s'])
for prefix in ('iperf-r','tls-stream-r'):
 for f in sorted(dirs[0].glob(prefix+'*.json')):
  if prefix=='iperf-r':v=[js(d,f.name)['end']['sum_received']['bits_per_second']/8/2**20 for d in dirs]
  else:v=[js(d,f.name)['mib_per_s'] for d in dirs]
  row(f.stem,v)
header('TLS stream sender process cost, includes handshake/startup/drain (not pure cipher)', ['sender CPU ns/plaintext byte','sender CPU cores'])
for f in sorted(dirs[0].glob('tls-stream-r*.json')):
 costs=[];cores=[]
 for d in dirs:
  text=(d/f.name.replace('tls-stream-','tls-time-').replace('.json','.txt')).read_text();cpu=sum(float(re.search(k+r' \(seconds\): ([\d.]+)',text)[1]) for k in ('User time','System time'));r=js(d,f.name);costs.append(cpu*1e9/r['bytes']);cores.append(cpu/r['seconds'])
 row(f.stem,costs,cores)
header('UDP 3s unlimited 64/1024-byte datagrams, CPU0 -> CPU1, iperf3', ['delivered packets/s','lost %','sender CPU %','receiver CPU %'])
for f in sorted(dirs[0].glob('udp-r*.json')):
 rs=[js(d,f.name)['end'] for d in dirs];ss=[r.get('sum_received',r.get('sum')) for r in rs]
 row(f.stem,[(s['packets']-s['lost_packets'])/s['seconds'] for s in ss],[s['lost_percent'] for s in ss],[r['cpu_utilization_percent']['host_total'] for r in rs],[r['cpu_utilization_percent']['remote_total'] for r in rs])
