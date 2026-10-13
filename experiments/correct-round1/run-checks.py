#!/usr/bin/env python3
"""Run every checker even if the controller or another oracle failed."""
import argparse
import json
import pathlib
import subprocess
import sys
import time


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--client', required=True)
    p.add_argument('--backend', required=True)
    p.add_argument('--out', required=True, type=pathlib.Path)
    args = p.parse_args()
    history = args.out / 'workload/history.json'
    results = []
    failed = False
    manifest = args.out / 'oracles.json'
    if not manifest.exists():
        print('FAIL: missing oracle manifest', flush=True)
        return 1
    oracles = json.loads(manifest.read_text())
    if len(oracles) != 4:
        print(f'FAIL: {len(oracles)} oracle sets; expected original and three cold pairs', flush=True)
        failed = True
    for oracle in oracles:
        name = oracle['name']
        enriched = args.out / f'history-{name}.json'
        report = args.out / f'check-{name}.json'
        commands = [
            [args.client, 'log', '--backend', args.backend, '--data-dirs', ','.join(oracle['data_dirs']),
             '--history', str(history), '--out', str(enriched), '--stopped-file', oracle['stopped_file']],
            [args.client, 'check', '--history', str(enriched), '--checker-timeout', '30s', '--out', str(report)],
        ]
        for cmd in commands:
            start = time.monotonic()
            print(f'RUN {name}: {cmd}', flush=True)
            try:
                done = subprocess.run(cmd, capture_output=True, text=True, timeout=100)
                output = done.stdout + done.stderr
                code = done.returncode
            except subprocess.TimeoutExpired as exc:
                output = f'COMMAND TIMEOUT: {exc}'
                code = 124
            print(output, flush=True)
            results.append({'oracle': name, 'mode': cmd[1], 'returncode': code,
                            'seconds': time.monotonic() - start, 'output': output})
            failed |= code != 0
    (args.out / 'offline-results.json').write_text(json.dumps(results, indent=2) + '\n')
    return int(failed)


if __name__ == '__main__':
    sys.exit(main())
