#!/usr/bin/env python3
"""Combine independently-run matrices, retaining runner medians and all-run range."""
import collections,json,pathlib,statistics as S,sys
G=collections.defaultdict(list)
for root in map(pathlib.Path,sys.argv[1:]):
 dataset='pipeline' if 'pipeline' in str(root.parent) else 'detail' if 'detail' in str(root.parent) else 'full'
 for r in json.loads((root/'derived.json').read_text()):
  r['runner']=root.name;r['dataset']=dataset
  G[(dataset,r['workload'],r['place'],r['rtt'],r['variant'])].append(r)
def fmt(rows,f,d=2):
 values=[f(r) for r in rows]; meds=[S.median(f(r) for r in rows if r['runner']==n) for n in sorted({r['runner'] for r in rows})]
 return '/'.join(f'{v:.{d}f}' for v in meds)+f' [{min(values):.{d}f},{max(values):.{d}f}]'
def keytext(k): return '| '+' | '.join(map(str,k))+' | '
print('Values: runner 1 median / runner 2 median [minimum, maximum across all six repetitions]. Three repetitions per runner; each set uses a different pair of VMs.\n')
print('## Throughput, CPU, latency and device bytes\n')
print('| set | workload | layout | RTT ms | variant | ops/s | p50 ms | mean ms | leader us/op | leader/f1/f2 cores | generator cores | device B/op |')
print('|---|---|---|---:|---|---:|---:|---:|---:|---|---:|---:|')
for k,rs in sorted(G.items()):
 cells=[fmt(rs,lambda r:r['ops'],0),fmt(rs,lambda r:r['p50_ms'],3),fmt(rs,lambda r:r['mean_ms'],3),fmt(rs,lambda r:r['leader_us']),'; '.join(fmt(rs,lambda r,k=k:r[k],3) for k in ['leader_core','f1_core','f2_core']),fmt(rs,lambda r:r['client_core'],3),fmt(rs,lambda r:r['device_B_op'],0)]
 print(keytext(k)+' | '.join(cells)+' |')
print('\n## Per-node WAL measurements (8 s paired in-load readouts)\n')
print('| set | workload | layout | RTT ms | variant | node | fsync/s | mean fsync ms | applied ops/fsync | WAL B/fsync | WAL B/applied op | write calls/s | B/write | mean write us |')
print('|---|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|')
for k,rs in sorted(G.items()):
 for i,name in enumerate(['leader','follower1','follower2']):
  print(keytext(k)+name+' | '+' | '.join(fmt(rs,lambda r,f=f:r['nodes'][i][f],d) for f,d in [('fsync_s',1),('fsync_mean_ms',3),('ops_fsync',1),('wal_B_fsync',0),('wal_B_op',1),('wal_writes_s',1),('wal_B_write',0),('wal_write_mean_us',1)])+' |')
print('\n## Device and Raft samples\n')
print('| set | workload | layout | RTT ms | variant | device MiB/s | device busy % | write await ms | write IOPS | max inflight count | inflight-full fraction |')
print('|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|')
for k,rs in sorted(G.items()):
 print(keytext(k)+' | '.join(fmt(rs,lambda r,f=f:r.get(f,0),d) for f,d in [('sda_MiB_s',1),('sda_util_pct',1),('sda_write_await_ms',3),('sda_write_IOPS',1),('inflight_max',0),('inflight_full_fraction',3)])+' |')
print('\n## Leader persistence group-stage means, microseconds\n\nThese are overlapping groups, not an additive request-latency decomposition. Means of each group, including groups without entries; sync means here are group-weighted, whereas WAL fsync means above are syscall-weighted.\n')
print('| set | workload | layout | RTT ms | variant | queue | append | flush | sync queue | sync | report | total |')
print('|---|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|')
for k,rs in sorted(G.items()):
 if not all('stages_us' in r['nodes'][0] for r in rs):continue
 print(keytext(k)+' | '.join(fmt(rs,lambda r,f=f:r['nodes'][0]['stages_us'][f],1) for f in ['queue','append','flush','sync_queue','sync','report','total'])+' |')
