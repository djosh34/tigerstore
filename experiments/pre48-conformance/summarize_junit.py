#!/usr/bin/env python3
"""Analyze actual JUnit results against the official inventory, never a dry-run."""
import argparse
import collections
import json
import pathlib
import xml.etree.ElementTree as ET

p = argparse.ArgumentParser()
p.add_argument('directory', type=pathlib.Path)
p.add_argument('--inventory', type=pathlib.Path, default=pathlib.Path('evidence/conformance-names.txt'))
args = p.parse_args()
expected = set(args.inventory.read_text().splitlines())
root = args.directory
junit = root / 'junit_01.xml'
result = {'certified_pass': False, 'expected_conformance_count': len(expected)}
if not junit.exists():
    result['missing_junit'] = True
else:
    cases = []
    for x in ET.parse(junit).getroot().iter('testcase'):
        name = x.get('name', '')
        if name.startswith('[It] '):
            name = name[5:]
        if '[Conformance]' not in name:
            continue
        state = 'passed'
        text = ''
        for tag, status in [('skipped', 'skipped'), ('error', 'error'), ('failure', 'failed')]:
            child = x.find(tag)
            if child is not None:
                state = status
                text = child.get('message', '') + '\n' + ''.join(child.itertext())
        cases.append({'name': name, 'state': state, 'runtime_seconds': float(x.get('time', 0)), 'failure_or_skip': text})
    observed = collections.Counter(x['name'] for x in cases)
    states = collections.Counter(x['state'] for x in cases)
    result.update({'reported_conformance_count': len(cases), 'states': dict(states),
                   'missing_names': sorted(expected - set(observed)),
                   'unexpected_names': sorted(set(observed) - expected),
                   'duplicate_names': sorted(n for n, count in observed.items() if count != 1),
                   'sum_serial_reported_seconds': sum(x['runtime_seconds'] for x in cases if '[Serial]' in x['name']),
                   'specs': sorted(cases, key=lambda x: x['runtime_seconds'], reverse=True)})
    for category, predicate in {
        'failed': lambda x: x['state'] in {'failed', 'error'},
        'pending': lambda x: x['state'] == 'skipped',
        'passed': lambda x: x['state'] == 'passed',
    }.items():
        (root / f'{category}-names.txt').write_text('\n'.join(x['name'] for x in cases if predicate(x)) + '\n')
(root / 'junit-analysis.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k: v for k, v in result.items() if k != 'specs'}, indent=2))
