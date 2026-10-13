#!/usr/bin/env python3
"""All four checkers run on every saved variant, even when log conversion fails."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time


def run(command, results, oracle, mode):
    start = time.monotonic()
    print('RUN', oracle, command, flush=True)
    try:
        p = subprocess.run(command, capture_output=True, text=True, timeout=120)
        code, output = p.returncode, p.stdout + p.stderr
    except subprocess.TimeoutExpired as exc:
        code, output = 124, f'COMMAND TIMEOUT: {exc}'
    print(output, flush=True)
    results.append({'oracle': oracle, 'mode': mode, 'returncode': code,
                    'seconds': time.monotonic()-start, 'output': output})
    return code


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--client', required=True)
    p.add_argument('--backend', required=True)
    p.add_argument('--out', required=True, type=Path)
    p.add_argument('--clients', required=True)
    a = p.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    raw = a.out / 'workload/history.json'
    results = []
    manifest = a.out / 'oracles.json'
    oracles = json.loads(manifest.read_text()) if manifest.exists() else []
    failed = len(oracles) != 4
    if failed:
        results.append({'oracle': 'manifest', 'mode': 'coverage', 'returncode': 1,
                        'seconds': 0, 'output': f'{len(oracles)} oracle sets; expected original and three cold pairs'})
    histories = []
    for oracle in oracles:
        name = oracle['name']
        enriched = a.out / f'history-{name}.json'
        code = run([a.client, 'log', '--backend', a.backend, '--data-dirs', ','.join(oracle['data_dirs']),
                    '--history', str(raw), '--out', str(enriched), '--stopped-file', oracle['stopped_file']],
                   results, name, 'log')
        failed |= code != 0
        # Keep any partial converted history. If none exists, check the raw
        # record under this variant name. Missing log/target fails all verdicts.
        if not enriched.exists() and raw.exists():
            shutil.copyfile(raw, enriched)
        histories.append((name, enriched))
    if not oracles and raw.exists():
        histories.append(('raw-fallback', raw))
    # Include converter outputs retained from an interrupted earlier stage.
    known = {path for _, path in histories}
    for path in sorted(a.out.glob('history-*.json')):
        if path not in known:
            histories.append((path.stem.removeprefix('history-'), path))
    if not raw.exists():
        failed = True
        results.append({'oracle': 'raw', 'mode': 'recorded-history', 'returncode': 1,
                        'seconds': 0, 'output': 'No recorded client history. Setup/build failure is not a checked case.'})
    for name, path in histories:
        report = a.out / f'check-{name}.json'
        code = run([a.client, 'check', '--history', str(path), '--clients', a.clients,
                    '--checker-timeout', '30s', '--out', str(report)], results, name, 'check')
        failed |= code != 0
        # An external process timeout or crash must never count as four checks.
        if not report.exists():
            failed = True
            results.append({'oracle': name, 'mode': 'missing-verdicts', 'returncode': 1,
                            'seconds': 0, 'output': 'Checker process produced no verdict file; FAILED, not skipped.'})
    (a.out / 'offline-results.json').write_text(json.dumps(results, indent=2)+'\n')
    return int(failed)


if __name__ == '__main__':
    sys.exit(main())
