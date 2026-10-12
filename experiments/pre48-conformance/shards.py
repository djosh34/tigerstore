#!/usr/bin/env python3
"""Generate diagnostic shards. These are NOT a certified-conformance run."""
import argparse
import collections
import hashlib
import json
import pathlib
import re

parser = argparse.ArgumentParser()
parser.add_argument('inventory', type=pathlib.Path)
parser.add_argument('output', type=pathlib.Path)
parser.add_argument('--count', type=int, default=32)
args = parser.parse_args()
assert args.count > 0
names = sorted(args.inventory.read_text().splitlines())
assert len(names) == len(set(names)) == 462
assert all('[Conformance]' in name for name in names)
# Place the serial specs first so no shard receives two until every shard has one.
ordered = [n for n in names if '[Serial]' in n] + [n for n in names if '[Serial]' not in n]
groups = [[] for _ in range(args.count)]
for i, name in enumerate(ordered):
    groups[i % args.count].append(name)
args.output.mkdir(parents=True, exist_ok=True)
manifest = {'certified_report': False, 'version': 'v1.37.0', 'count': len(names),
            'inventory_sha256': hashlib.sha256(args.inventory.read_bytes()).hexdigest(),
            'shards': []}
for i, group in enumerate(groups):
    # Match full names. Escape Go RE2 metacharacters, not spaces.
    # Ginkgo prepends its suite description when matching regex focus.
    focus = '^Kubernetes e2e suite (' + '|'.join(re.sub(r'([\\.+*?()|\[\]{}^$])', r'\\\1', n) for n in group) + ')$'
    matched = [n for n in names if re.search(focus, 'Kubernetes e2e suite ' + n)]
    assert sorted(matched) == sorted(group)
    (args.output / f'{i:02}.focus').write_text(focus + '\n')
    manifest['shards'].append({'id': i, 'names': group, 'count': len(group),
                               'serial_count': sum('[Serial]' in n for n in group)})
coverage = collections.Counter(n for group in groups for n in group)
assert set(coverage) == set(names) and set(coverage.values()) == {1}
(args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps({'shards': len(groups), 'union': len(coverage), 'duplicate_names': 0,
                  'missing_names': 0, 'certified_report': False}))
