#!/usr/bin/env python3
"""Inspect complete raw histories even if the online checker never returned.

This script checks fault evidence and counts calls. It does not decide
linearizability and never substitutes a verdict for a missing Jepsen result.
"""
import argparse
import collections
import datetime
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).parent / 'analysis'))
from analyze import EDN, latency_summary, options, read_edn


def summarize(root):
    command = (root / 'command.txt').read_text().strip()
    opts = options(command)
    latest = root / 'store/latest'
    pending, calls, faults, edges = {}, [], [], []
    history = latest / 'history.edn'
    with history.open() as stream:
        for line in stream:
            if not line.strip():
                continue
            event = EDN(line).load()
            process = event[':process']
            if process == ':nemesis':
                faults.append(event)
                edges.append({'index': event[':index'], 'time_ns': event[':time'],
                              'f': event[':f'], 'value': event[':value'],
                              'open_calls': len(pending),
                              'open_by_f': dict(collections.Counter(e[':f'] for e in pending.values()))})
                continue
            assert isinstance(process, int), event
            if event[':type'] == ':invoke':
                assert process not in pending, event
                pending[process] = event
            else:
                invocation = pending.pop(process)
                assert invocation[':f'] == event[':f'], event
                assert invocation[':value'][0] == event[':value'][0], event
                assert event[':type'] in (':ok', ':fail', ':info'), event
                calls.append({'f': event[':f'], 'type': event[':type'],
                              'latency_ms': (event[':time'] - invocation[':time']) / 1e6})
    assert not pending, 'History has incomplete client calls'
    assert len(faults) % 2 == 0, 'Unpaired nemesis event'
    completions = []
    for before, after in zip(faults[::2], faults[1::2]):
        assert before[':f'] == after[':f'], 'Unpaired nemesis functions'
        completions.append(after)
    counts = dict(collections.Counter(e['type'] for e in calls))
    by_f = {f: dict(collections.Counter(e['type'] for e in calls if e['f'] == f))
            for f in (':read', ':write', ':cas')}
    def applied(f):
        return [e for e in completions if e[':f'] == f and isinstance(e[':value'], dict)
                and e[':value'] and not e.get(':error')]
    observations = '\n'.join(p.read_text() for p in root.glob('fault-observer-*.log'))
    errors = []
    requested = opts['nemesis'].split(',')
    observed = {}
    if 'kill' in requested:
        observed['kills'] = len(applied(':kill'))
        observed['restarts'] = len(applied(':start'))
        observed['replacement_pid'] = any(len(set(re.findall(r'^\s*(\d+)\s+[A-Z]', p.read_text(), re.M))) > 1
                                          for p in root.glob('fault-observer-*.log'))
        if not all(observed[k] for k in ('kills', 'restarts', 'replacement_pid')):
            errors.append('Missing kill, restart, or replacement PID')
    if 'pause' in requested:
        observed['pauses'] = len(applied(':pause'))
        observed['resumes'] = len(applied(':resume'))
        observed['stopped_process'] = bool(re.search(r'^\s*\d+\s+T', observations, re.M))
        if not all(observed[k] for k in ('pauses', 'resumes', 'stopped_process')):
            errors.append('Missing pause, resume, or stopped process')
    if 'partition' in requested:
        observed['partitions'] = sum(e[':f'] == ':start-partition' and isinstance(e[':value'], list)
                                    and len(e[':value']) == 2 and e[':value'][0] == ':isolated'
                                    and isinstance(e[':value'][1], dict) and any(e[':value'][1].values())
                                    for e in completions)
        observed['partition_heals'] = sum(e[':value'] == ':network-healed' for e in completions)
        observed['max_drop_packets'] = max([int(n) for n in re.findall(r'^\[(\d+):\d+\] -A .* -j DROP', observations, re.M)], default=0)
        if not all(observed[k] for k in ('partitions', 'partition_heals', 'max_drop_packets')):
            errors.append('Missing nonempty partition, heal, or DROP packet hits')
    result_path = latest / 'results.edn'
    original = read_edn(result_path) if result_path.exists() else None
    if original is not None:
        stats = original[':stats']
        for outcome in (':ok', ':fail', ':info'):
            assert stats[outcome + '-count'] == counts.get(outcome, 0), 'Raw count disagrees with Jepsen'
    return {'case': root.name, 'command': command,
            'store': 'tigerstore' if 'tigerstore' in root.name else 'etcd',
            'nemesis': opts['nemesis'], 'ops_per_key': int(opts['ops-per-key']),
            'operations': len(calls), 'counts': counts, 'by_f': by_f,
            'latency_by_outcome': {t: latency_summary([e['latency_ms'] for e in calls if e['type'] == t])
                                   for t in (':ok', ':fail', ':info')},
            'original_jepsen_verdict': original.get(':valid?') if original else 'missing',
            'original_non_workload_verdicts': {k: v.get(':valid?') for k, v in original.items()
                                               if isinstance(v, dict) and k != ':workload'} if original else {},
            'fault_evidence': observed, 'fault_errors': errors, 'faults_valid': not errors,
            'fault_edges': edges}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('artifacts', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    assert datetime.datetime.now(datetime.timezone.utc) < datetime.datetime.fromisoformat('2026-10-13T22:00:00+00:00')
    results = [summarize(p) for p in sorted(args.artifacts.iterdir()) if p.is_dir()]
    args.output.write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps({'cases': len(results), 'faults_valid': all(r['faults_valid'] for r in results),
                      'original_verdicts': dict(collections.Counter(str(r['original_jepsen_verdict']) for r in results))}))
    sys.exit(0 if all(r['faults_valid'] for r in results) else 1)
