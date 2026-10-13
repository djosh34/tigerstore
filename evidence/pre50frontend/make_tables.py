#!/usr/bin/env python3
"""Render measured medians and offline CPU attribution without launching Go."""
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ROLES = ('leader', 'follower1', 'follower2')

def rows(mode):
    return [r for c in (16, 64, 256) for r in csv.DictReader((ROOT/mode/f'c{c}'/'summary.csv').open())]

def label(r):
    return f"{r['clients']} | {'Main' if r['store'] == 'a' else 'Etcd'}"

def triple(r, suffix, digits=3):
    return ' / '.join(f'{float(r[f"{role}_{suffix}"]):.{digits}f}' for role in ROLES)

def tables(mode):
    data = rows(mode)
    print('| Clients | Store | Put/s | Mean ms | p50 ms | p90 ms | p99 ms | p99.9 ms |')
    print('| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |')
    for r in data:
        cells = [f'{float(r[k]):.{1 if k == "ops_s" else 3}f}' for k in ('ops_s','mean_ms','p50_ms','p90_ms','p99_ms','p999_ms')]
        print('| '+label(r)+' | '+' | '.join(cells)+' |')
    print('\nAll triples below are leader / follower 1 / follower 2.\n')
    print('| Clients | Store | Server CPU us/Put by node | Generator CPU us/Put | Puts/file sync by node |')
    print('| ---: | --- | --- | ---: | --- |')
    for r in data:
        print(f'| {label(r)} | {triple(r,"cpu_us_op")} | {float(r["generator_cpu_us_op"]):.3f} | {triple(r,"puts_per_file_sync")} |')
    print('\n| Clients | Store | Allocated objects/Put by node | Allocated bytes/Put by node |')
    print('| ---: | --- | --- | --- |')
    for r in data:
        print(f'| {label(r)} | {triple(r,"allocation_objects_op",2)} | {triple(r,"allocation_bytes_op",1)} |')

print('## Unprofiled baseline\n')
tables('primary')
print('\n## Leader CPU profiles\n')
d = json.loads((ROOT/'outside-frontend.json').read_text())['clients']
print('| Inclusive stack cost, CPU us/Put | 16 clients | 64 clients | 256 clients |')
print('| --- | ---: | ---: | ---: |')
for label_, key in [('Owner loop','committer including descendants'), ('Core under owner','core including descendants'), ('Entry encode/decode','entry encode/decode including descendants'), ('Protobuf outside frontend','protobuf including descendants'), ('Raft','raft including descendants'), ('WAL','WAL including descendants'), ('File Sync under WAL','fsync syscall path'), ('MVCC/index','index including descendants'), ('Allocation and GC outside frontend','allocation and GC including descendants'), ('Peers including peer TLS','peer transport including descendants'), ('Scheduler outside frontend','scheduler including descendants')]:
    print('| '+label_+' | '+' | '.join(f'{d[str(c)]["categories"][key]["outside_frontend_cpu_us_op"]:.3f}' for c in (16,64,256))+' |')
print('\nThese rows overlap. Entry codec, Raft and index costs are inside the owner loop. File Sync is inside WAL. Allocators, protobuf and syscalls overlap those paths. Do not add them.\n')
print('| Profile cohort | 16 clients | 64 clients | 256 clients |')
print('| --- | ---: | ---: | ---: |')
for title,key,fmt in [('Puts across five profiles','operations',',d'),('Sampled leader CPU us/Put','total_sample_cpu_us_op','.3f'),('CPU us/Put outside frontend stacks','outside_frontend_sample_cpu_us_op','.3f'),('Outside frontend sample percent','outside_frontend_sample_pct','.2f')]:
    print('| '+title+' | '+' | '.join(format(d[str(c)][key],fmt) for c in (16,64,256))+' |')
print('\n## Separate profile controls\n\nDo not use these as headline performance. Main is CPU-profiled; stock etcd is not. These jobs use 12-second arrival windows and different VMs from the baseline. They do not isolate profiler overhead.\n')
tables('profile')
