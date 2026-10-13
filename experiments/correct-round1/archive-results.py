#!/usr/bin/env python3
"""Retain JSON histories and fault/checker evidence, excluding bulky server disks."""
import argparse
import gzip
import hashlib
import json
import pathlib


def main():
    p = argparse.ArgumentParser()
    p.add_argument('source', type=pathlib.Path)
    p.add_argument('destination', type=pathlib.Path)
    a = p.parse_args()
    a.destination.mkdir(parents=True, exist_ok=True)
    summaries = []
    for artifact in sorted(a.source.glob('correct-round1-*')):
        run = artifact / 'run'
        if not (run / 'faults.json').exists():
            raise RuntimeError(f'incomplete artifact {artifact}')
        destination = a.destination / artifact.name
        destination.mkdir(exist_ok=True)
        paths = list(artifact.glob('*.log')) + list(artifact.glob('*.txt'))
        paths += list(run.glob('*.json')) + list(run.glob('*.jsonl')) + list(run.glob('*.log'))
        paths += list((run / 'workload').glob('*.json'))
        paths += list(run.glob('cold-*/inspect.json')) + list(run.glob('cold-*/stopped.json'))
        manifest = []
        for source in sorted(set(paths)):
            relative = source.relative_to(artifact)
            target = destination / relative.with_suffix(relative.suffix + '.gz')
            target.parent.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256()
            size = 0
            with source.open('rb') as src, target.open('wb') as raw:
                with gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as dst:
                    while block := src.read(1024 * 1024):
                        digest.update(block)
                        size += len(block)
                        dst.write(block)
            manifest.append({'path': str(relative), 'sha256': digest.hexdigest(), 'bytes': size})
        (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        f = json.loads((run / 'faults.json').read_text())
        r = json.loads((run / 'result.json').read_text())
        h = json.loads((run / 'workload/history.json').read_text())
        offline = json.loads((run / 'offline-results.json').read_text())
        ops = h['ops']
        faults = {name: sorted(phases) for name, phases in f['faults'].items()}
        times = [event['monotonic_ns'] for event in f['events']]
        checker_count = 0
        passed_checks = 0
        for report in run.glob('check-*.json'):
            records = json.loads(report.read_text())
            if isinstance(records, dict):
                records = records.get('checks', records.get('results', []))
            if not isinstance(records, list):
                continue
            checker_count += len(records)
            passed_checks += sum(bool(row.get('passed')) for row in records)
        summary = {
            'artifact': artifact.name, 'controller_pass': r['controller_pass'],
            'offline_pass': all(row['returncode'] == 0 for row in offline),
            'oracle_sets': len([row for row in offline if row['mode'] == 'log']),
            'checker_verdicts': checker_count, 'checker_passes': passed_checks,
            'operations': len(ops), 'successful_operations': sum('resp' in op for op in ops),
            'ambiguous_operations': sum(op.get('err', {}).get('ambiguous', False) for op in ops),
            'target': h.get('target', 0), 'watches': len(h.get('watches', [])),
            'controller_seconds': max(times, default=0) / 1e9,
            'operation_span_seconds': (max((op['return'] for op in ops), default=0) - min((op['call'] for op in ops), default=0)) / 1e9,
            'faults': faults,
            'failures': [event.get('error') for event in f['events'] if event['name'] == 'controller-failure'],
        }
        summaries.append(summary)
    (a.destination / 'summary.json').write_text(json.dumps(summaries, indent=2) + '\n')
    print(json.dumps(summaries, indent=2))


if __name__ == '__main__':
    main()
