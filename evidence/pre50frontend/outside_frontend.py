#!/usr/bin/env python3
"""Offline attribution from the runner's raw profile text. No new measurements."""
import importlib.util
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('attribute', ROOT/'experiments/pre50frontend/attribute.py')
attribute = importlib.util.module_from_spec(spec)
spec.loader.exec_module(attribute)
# Go 1.27 moved its bundled HTTP/2 implementation under this package.
# The inherited parser's net/http\. expression misses its independent goroutines.
attribute.PATTERNS.pop('HTTP ServeHTTP including descendants')
attribute.PATTERNS['net/http and bundled HTTP2 including descendants'] = r'net/http(?:/internal/http2)?\.'
attribute.PATTERNS['HTTP2 including descendants'] = r'golang.org/x/net/http2|net/http/internal/http2'
FRONTEND = re.compile(r'google.golang.org/grpc|golang.org/x/net/http2|net/http(?:/internal/http2)?\.|internal/server\.')
results = {}
for clients in (16, 64, 256):
    root = Path(__file__).resolve().parent/'profile'/f'c{clients}'
    folders = sorted(root.glob('[1-5]-a'))
    assert len(folders) == 5
    samples, operations = [], 0
    for folder in folders:
        j = json.loads((folder/'put.json').read_text())
        assert j['valid']
        operations += j['successes']
        samples.extend(attribute.parse((folder/'cpu-raw.txt').read_text()))
    total = sum(v for v, _ in samples)
    outside = [(v, s) for v, s in samples if not FRONTEND.search(s)]
    selected = sum(v for v, _ in outside)
    result = {'operations': operations, 'total_sample_cpu_ns': total,
              'total_sample_cpu_us_op': total/1000/operations,
              'outside_frontend_sample_cpu_us_op': selected/1000/operations,
              'outside_frontend_sample_pct': selected*100/total,
              'categories': {}}
    for category, pattern in attribute.PATTERNS.items():
        all_ns = sum(v for v, s in samples if re.search(pattern, s))
        outside_ns = sum(v for v, s in outside if re.search(pattern, s))
        result['categories'][category] = {'all_stacks_cpu_us_op': all_ns/1000/operations,
                                          'outside_frontend_cpu_us_op': outside_ns/1000/operations}
    results[str(clients)] = result
print(json.dumps({'definition': 'Exclude samples with net/http including Go 1.27 net/http/internal/http2, x/net/http2, grpc-go or internal/server on the stack. This corrects the inherited attribution.json HTTP category, which missed the new bundled package. Peer TLS stays outside. Categories overlap. Background GC and scheduling cannot be causally assigned to their request origin; this is a stack partition, not an irreducible CPU floor.', 'clients': results}, indent=2))
