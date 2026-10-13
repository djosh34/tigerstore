#!/usr/bin/env python3
"""Inclusive stack attribution. Rows overlap and must not be added."""
import collections
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys

PATTERNS = {
    'TLS including socket descendants': r'crypto/tls\.',
    'crypto implementation': r'crypto/(internal|aes|cipher|sha|chacha|poly)',
    'frontend including descendants': r'internal/server\.',
    'HTTP ServeHTTP including descendants': r'net/http\.',
    'gRPC including descendants': r'google.golang.org/grpc',
    'HTTP2 including descendants': r'golang.org/x/net/http2',
    'protobuf including descendants': r'google.golang.org/protobuf|github.com/golang/protobuf',
    'committer including descendants': r'internal/node\.\(\*Node\)\.commit',
    'core including descendants': r'internal/core\.',
    'entry encode/decode including descendants': r'internal/core\.(encodeBatch|DecodeBatch)',
    'raft including descendants': r'go.etcd.io/raft/v3\.',
    'WAL including descendants': r'internal/wal\.',
    'WAL write syscall path': r'internal/wal\.\(\*WAL\)\.(Flush|Sync)',
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


def pprof(path, *args):
    deadline = datetime.datetime.fromisoformat('2026-10-13T22:00:00+00:00')
    assert datetime.datetime.now(datetime.timezone.utc) < deadline, 'deadline passed'
    return subprocess.check_output(['go', 'tool', 'pprof', *args, str(path)], text=True,
                                   env=os.environ | {'GOFLAGS': '-p=2', 'GOMEMLIMIT': '2GiB'}, timeout=30)


def attribution(samples, operations):
    total = sum(v for v, _ in samples)
    assert total > 0 and operations > 0
    costs = collections.OrderedDict()
    for category, pattern in PATTERNS.items():
        value = sum(v for v, stack in samples if re.search(pattern, stack))
        costs[category] = {'sample_cpu_ns': value, 'cpu_us_op': value/1000/operations,
                           'sample_pct': value/total*100}
    return {'operations': operations, 'sample_cpu_ns': total, 'sampled_cpu_us_op': total/1000/operations,
            'categories': costs}


def main():
    root = Path(sys.argv[1])
    results, combined, operations = {}, [], 0
    process_cpu_seconds = 0
    paths = sorted(root.glob('*-*/n*-cpu.pb.gz'))
    assert len(paths) == 5, 'five leader profiles required'
    for path in paths:
        folder = path.parent
        load = json.loads((folder/'put.json').read_text())
        placement = json.loads((folder/'placement.json').read_text())
        assert path.name == f"n{placement['leader_index']}-cpu.pb.gz" and load['valid']
        n = load['successes']
        raw = pprof(path, '-raw')
        (folder/'cpu-raw.txt').write_text(raw)
        (folder/'cpu-top-flat.txt').write_text(pprof(path, '-top', '-nodecount=80'))
        (folder/'cpu-top-cumulative.txt').write_text(pprof(path, '-top', '-cum', '-nodecount=80'))
        samples = parse(raw)
        item = attribution(samples, n)
        item['leader_process_cpu_us_op'] = load['cpu_us_op'][0]
        item['server_process_cpu_us_op'] = sum(load['cpu_us_op'][:3])
        item['capture'] = json.loads((folder/'profile-window.json').read_text())
        results[folder.name] = item
        combined.extend(samples)
        operations += n
        process_cpu_seconds += sum(load['cpu_us_op'][:3])*n/1e6
    aggregate = attribution(combined, operations)
    aggregate['server_process_cpu_us_op'] = process_cpu_seconds*1e6/operations
    print(json.dumps({'note': 'Inclusive categories overlap. Do not sum rows. Aggregate uses sample nanoseconds, not mean percentages. Profiles include the recorded idle lead/tail and observation requests.',
                      'repetitions': results, 'aggregate': aggregate}, indent=2))


if __name__ == '__main__':
    main()
