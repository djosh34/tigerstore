#!/usr/bin/env python3
"""Read Jepsen artifacts. Requires edn_format==0.7.5; no product code imports."""
import collections
from collections.abc import Sequence
import json
import pathlib
import re
import sys

import edn_format

K = edn_format.Keyword


def read_edn(path):
    text = path.read_text()
    # Checker models are Clojure records whose fields remain ordinary maps.
    for tag in set(re.findall(r'#([a-zA-Z][\w.]+)\s*\{', text)):
        edn_format.add_tag(tag, lambda value: value)
    return edn_format.loads(text)


def plain(value):
    if hasattr(value, 'items'):
        return {str(k).lstrip(':'): plain(v) for k, v in value.items()}
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (K, edn_format.Symbol)):
        return str(value).lstrip(':')
    return [plain(v) for v in value]


def summarize(root):
    root = pathlib.Path(root)
    command = (root / 'command.txt').read_text() if (root / 'command.txt').exists() else ''
    match = re.search(r'--nemesis (\S+)', command)
    faults = match[1].split(',') if match else []
    result = {'directory': root.name, 'command': command.strip(), 'coverage_errors': []}
    if (root / 'clock-blocked.txt').exists():
        return {**result, 'status': 'blocked', 'reason': (root / 'clock-blocked.txt').read_text().strip()}
    latest = root / 'store/latest'
    if not (latest / 'results.edn').exists():
        return {**result, 'status': 'execution-failure', 'reason': 'No checker result'}
    checks = read_edn(latest / 'results.edn')
    result['valid'] = plain(checks[K('valid?')])
    result['stats'] = plain(checks[K('stats')])
    # The verdict and operation counts come from the unchanged upstream checker.
    # Only parse fault records here. Some client error keywords use apostrophes,
    # which are valid Clojure but unsupported by edn_format's keyword lexer.
    with (latest / 'history.edn').open() as stream:
        nemeses = [edn_format.loads(s) for s in stream if ':process :nemesis,' in s]
    result['nemesis_counts'] = dict(collections.Counter(str(e[K('f')]) for e in nemeses))
    # Invoke/completion records both use :info. Count only actual nonempty results.
    def applied(name):
        return [e for e in nemeses if e.get(K('f')) == K(name)
                and hasattr(e.get(K('value')), 'items') and len(e[K('value')])
                and not e.get(K('error'))]
    observations = '\n'.join(p.read_text() for p in root.glob('fault-observer-*.log'))
    if 'partition' in faults:
        partitions = [e for e in nemeses if e.get(K('f')) == K('start-partition')
                      and isinstance(e.get(K('value')), Sequence)
                      and len(e[K('value')]) == 2 and e[K('value')][0] == K('isolated')]
        if not partitions:
            result['coverage_errors'].append('No completed partition')
        if not any(e.get(K('value')) == K('network-healed') for e in nemeses):
            result['coverage_errors'].append('No completed partition healing')
        if not re.search(r'^.*-A .* -j DROP', observations, re.M):
            result['coverage_errors'].append('Observer never saw a DROP rule')
    if 'pause' in faults:
        if not applied('pause'):
            result['coverage_errors'].append('No completed nonempty pause')
        if not applied('resume'):
            result['coverage_errors'].append('No completed resume')
        if not re.search(r'^\s*\d+\s+T', observations, re.M):
            result['coverage_errors'].append('Observer never saw a stopped process')
    if 'kill' in faults:
        if not applied('kill'):
            result['coverage_errors'].append('No completed nonempty kill')
        if not applied('start'):
            result['coverage_errors'].append('No completed restart')
        replaced = any(len(set(re.findall(r'^\s*(\d+)\s+[A-Z]', p.read_text(), re.M))) > 1
                       for p in root.glob('fault-observer-*.log'))
        if not replaced:
            result['coverage_errors'].append('Observer never saw a replacement PID')
    result['status'] = ('pass' if checks[K('valid?')] is True and not result['coverage_errors']
                        else 'fail')
    return result


if __name__ == '__main__':
    results = [summarize(p) for p in sys.argv[1:]]
    print(json.dumps(results, indent=2))
    sys.exit(0 if all(r['status'] == 'pass' for r in results) else 1)
