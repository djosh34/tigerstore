#!/usr/bin/env python3
"""Archive every Actions attempt, including jobs that never built or ran."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path


def load(path, default):
    return json.loads(path.read_text()) if path.exists() else default


def main():
    p = argparse.ArgumentParser()
    p.add_argument('source', type=Path)
    p.add_argument('destination', type=Path)
    a = p.parse_args()
    a.destination.mkdir(parents=True, exist_ok=True)
    summaries = []
    for artifact in sorted(a.source.glob('correct-round2-faults-*')):
        run = artifact / 'run'
        dst = a.destination / artifact.name
        dst.mkdir(exist_ok=True)
        paths = [path for path in artifact.rglob('*') if path.is_file() and
                 path.suffix in ('.json', '.jsonl', '.log', '.txt') and
                 not any(part in ('data', 'original') or part.startswith('disk-mount') for part in path.relative_to(artifact).parts)]
        manifest = []
        for src in sorted(paths):
            relative = src.relative_to(artifact)
            target = dst / relative.with_suffix(relative.suffix + '.gz')
            target.parent.mkdir(parents=True, exist_ok=True)
            digest = hashlib.sha256()
            size = 0
            with src.open('rb') as stream, target.open('wb') as raw:
                with gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as zipped:
                    while block := stream.read(1024*1024):
                        size += len(block)
                        digest.update(block)
                        zipped.write(block)
            manifest.append({'path': str(relative), 'sha256': digest.hexdigest(), 'bytes': size})
        (dst / 'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
        fault = load(run / 'faults.json', {})
        result = load(run / 'result.json', {})
        history = load(run / 'workload/history.json', {})
        offline = load(run / 'offline-results.json', [])
        attempt = load(artifact / 'attempt.json', {})
        verdicts = [row for path in sorted(run.glob('check-*.json')) for row in load(path, [])]
        events = fault.get('events', [])
        ops = history.get('ops', []) or []
        missing = [f'{name}/{phase}' for name in fault.get('required', []) for phase in
                   ['requested', 'installed', 'exercised', 'healed'] if phase not in fault.get('faults', {}).get(name, {})]
        timeout_verdicts = sum('timed out' in row.get('error', '').lower() or
                               'deadline' in row.get('error', '').lower() for row in verdicts)
        histories = list(run.glob('history-*.json'))
        reached = (run / 'faults.json').exists()
        passed = (reached and result.get('controller_pass', False) and not missing and
                  len(offline) > 0 and all(row['returncode'] == 0 for row in offline) and
                  len(verdicts) == 16 and all(row['passed'] for row in verdicts))
        summaries.append({'artifact': artifact.name, 'attempt': attempt,
            'reached_controller': reached, 'passed': passed,
            'controller_pass': result.get('controller_pass', False),
            'offline_pass': bool(offline) and all(row['returncode'] == 0 for row in offline),
            'oracle_sets': len(load(run / 'oracles.json', [])),
            'recorded_client_histories': int((run / 'workload/history.json').exists()),
            'history_variants': len(histories) or int(any(row['oracle'] == 'raw-fallback' for row in offline)),
            'checker_verdicts': len(verdicts), 'checker_passes': sum(row['passed'] for row in verdicts),
            'checker_timeout_verdicts': timeout_verdicts,
            'operations': len(ops), 'successful_operations': sum('resp' in op for op in ops),
            'ambiguous_operations': sum(op.get('err', {}).get('ambiguous', False) for op in ops),
            'target': history.get('target', 0), 'watches': len(history.get('watches', []) or []),
            'controller_seconds': max((e['monotonic_ns'] for e in events), default=0)/1e9,
            'faults': {name: sorted(phases) for name, phases in fault.get('faults', {}).items()},
            'missing_fault_phases': missing,
            'failures': [e.get('error') for e in events if e['name'] == 'controller-failure'],
            'no_history_reason': None if ops else 'No operations recorded. Count separately as setup/controller failure, never a passing checker case.'})
    (a.destination / 'summary.json').write_text(json.dumps(summaries, indent=2)+'\n')
    print(json.dumps(summaries, indent=2))


if __name__ == '__main__':
    main()
