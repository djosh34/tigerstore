#!/usr/bin/env python3
"""Inclusive stack attribution. Rows overlap and must not be added."""
import collections
import json
from pathlib import Path
import re
import subprocess
import sys

PATTERNS = {
    'TLS including socket descendants': r'crypto/tls\.',
    'crypto implementation': r'crypto/(internal|aes|cipher|sha|chacha|poly)',
    'gRPC including descendants': r'google.golang.org/grpc',
    'HTTP2 including descendants': r'golang.org/x/net/http2',
    'protobuf including descendants': r'google.golang.org/protobuf|github.com/golang/protobuf',
    'committer including descendants': r'internal/node\.\(\*Node\)\.commit',
    'core including descendants': r'internal/core\.',
    'entry encode/decode including descendants': r'internal/core\.(encodeBatch|DecodeBatch)',
    'raft including descendants': r'go.etcd.io/raft/v3\.',
    'WAL including descendants': r'internal/wal\.',
    'WAL write syscall path': r'internal/wal\.\(\*WAL\)\.Flush',
    'fsync syscall path': r'os\.\(\*File\)\.Sync|syscall\.Fsync',
    'index including descendants': r'internal/mvcc\.|github.com/google/btree',
    'allocation and GC including descendants': r'runtime\.(mallocgc|gc[A-Z]|scan|sweep|mark|newobject|makeslice|growslice)',
    'peer transport including descendants': r'internal/peer\.',
    'scheduler including descendants': r'runtime\.(schedule|findRunnable|park_m|mcall|netpoll)',
    'syscalls including descendants': r'internal/runtime/syscall|syscall\.(Syscall|RawSyscall)',
}


def parse(text):
    sample_text, location_text = text.split('Locations\n', 1)
    location_text = location_text.split('Mappings\n', 1)[0]
    locations = {}
    current = None
    for line in location_text.splitlines():
        match = re.match(r'\s*(\d+): ', line)
        if match:
            current = int(match[1])
            locations[current] = line
        elif current is not None:
            locations[current] += '\n'+line
    samples = []
    for line in sample_text.splitlines():
        match = re.match(r'^\s*(\d+)\s+(\d+):\s+([\d ]+)$', line)
        if match:
            samples.append((int(match[2]), '\n'.join(locations[int(i)] for i in match[3].split())))
    assert samples
    return samples


def main():
    root = Path(sys.argv[1] if len(sys.argv)>1 else 'evidence/pre50profile/raw')
    results = {}
    for op in ['put','range']:
        folder = root/f'profile-{op}'
        load = json.loads((folder/'profile-load.json').read_text())
        placement = json.loads((folder/'placement.json').read_text())
        assert load['valid']
        n = load['successes']
        for role, node in zip(['leader','follower','follower2'], placement['node_order']):
            path = folder/f'n{node}-cpu.pb.gz'
            if not path.exists():
                continue
            text = subprocess.check_output(['go','tool','pprof','-raw',str(path)],text=True)
            samples = parse(text)
            total = sum(v for v,_ in samples)
            costs = collections.OrderedDict()
            for category, pattern in PATTERNS.items():
                value = sum(v for v,stack in samples if re.search(pattern,stack))
                costs[category] = {'cpu_us_op':value/1000/n,'sample_pct':value/total*100}
            results[f'{op}-{role}'] = {'operations':n,'sampled_cpu_us_op':total/1000/n,'categories':costs}
    print(json.dumps(results,indent=2))


if __name__ == '__main__':
    main()
