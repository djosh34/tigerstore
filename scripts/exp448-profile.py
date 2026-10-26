#!/usr/bin/env python3
"""Disjoint flat-function costs and explicit gosim subtree samples from pprof raw.
The stack attribution charges explicit simulator helpers to gosim, then checker
stacks to checkers, then cryptography/CRC, then the nearest domain stack.
These are descriptive accounting buckets, not causal slowdown measurements.
"""
import collections, re, subprocess, sys
p = subprocess.check_output(['go','tool','pprof','-raw',sys.argv[1]], text=True)
samples=[]; loc={}; current=None; inloc=False
for line in p.splitlines():
    m=re.match(r'\s+(\d+)\s+(\d+):\s+([\d ]+)$',line)
    if m: samples.append((int(m[2]),[int(x) for x in m[3].split()]))
    if line=='Locations': inloc=True; continue
    if line=='Mappings': inloc=False
    if inloc:
        m=re.match(r'\s+(\d+): 0x\w+ M=\d+ (.*)',line)
        if m: current=int(m[1]); loc[current]=[m[2]]
        elif current and line.startswith('             '): loc[current].append(line.strip())

def kind(s):
    if '/gosim/' in s or 'runtime.coroswitch' in s: return 'gosim explicit'
    if '/tigerstore/check.' in s or '/tigerstore/check/' in s: return 'checkers'
    if '/tigerstore/internal_/sim.' in s or '/tigerstore/internal/sim.' in s: return 'sim harness/clients'
    if '/tigerstore/' in s: return 'tigerstore'
    if 'go.etcd.io/raft/' in s: return 'Raft'
    if 'grpc' in s or 'protobuf' in s or 'etcd/api/' in s or 'etcd/client/' in s: return 'gRPC/protobuf/clientv3'
    if '/crypto/' in s or '/crypto.' in s or '/crypto_' in s: return 'crypto'
    if 'crc32' in s: return 'CRC32'
    return None
flat=collections.Counter(); attributed=collections.Counter(); total=sum(x[0] for x in samples)
for weight, ids in samples:
    stack=[s for i in ids for s in loc.get(i,[])]
    leaf=stack[0] if stack else ''
    flat[kind(leaf) or 'Go runtime/stdlib/other']+=weight
    # A gosim helper before any application stack is explicit emulation cost.
    relevant=[(kind(s),s) for s in stack if kind(s)]
    first=relevant[0][0] if relevant else None
    if first=='gosim explicit': bucket=first
    elif any('/tigerstore/check' in s or '/sim/check.go' in s or '/sim/cold_restart_test.go' in s or '.hashes' in s or '.Checkable' in s for s in stack): bucket='checkers/hash/cold restart'
    elif any(kind(s)=='crypto' for s in stack): bucket='crypto'
    elif any(kind(s)=='CRC32' for s in stack): bucket='CRC32'
    else: bucket=first or 'Go runtime/stdlib/other'
    attributed[bucket]+=weight
print(sys.argv[1], 'sampled CPU seconds',total/1e9)
for title,buckets in [('flat',flat),('stack-attributed',attributed)]:
    print(title)
    for k,v in buckets.most_common(): print(f' {k}: {v/1e9:.3f}s {100*v/total:.2f}%')
