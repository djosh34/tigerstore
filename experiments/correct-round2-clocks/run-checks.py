#!/usr/bin/env python3
"""Every available oracle is checked. Missing conversion still invokes all four on raw history."""
import argparse
import json
from pathlib import Path
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument('--client', required=True)
p.add_argument('--backend', required=True)
p.add_argument('--out', type=Path, required=True)
a = p.parse_args()
a.out.mkdir(parents=True, exist_ok=True)
raw = a.out / 'workload/history.json'
manifest = a.out / 'oracles.json'
oracles = json.loads(manifest.read_text()) if manifest.exists() else []
failed = len(oracles) != 4
if not oracles:
    oracles = [{'name': 'missing-oracle', 'data_dirs': [], 'stopped_file': ''}]
results = []
for oracle in oracles:
    name = oracle['name']
    enriched = a.out / f'history-{name}.json'
    report = a.out / f'check-{name}.json'
    commands = [[a.client, 'log', '--backend', a.backend, '--data-dirs', ','.join(oracle['data_dirs']),
                 '--history', str(raw), '--out', str(enriched), '--stopped-file', oracle['stopped_file']],
                [a.client, 'check', '--history', str(enriched), '--checker-timeout', '30s', '--out', str(report)]]
    for cmd in commands:
        if cmd[1] == 'check' and not enriched.exists():
            cmd[cmd.index('--history') + 1] = str(raw)
        start = time.monotonic()
        print(f'RUN {name}: {cmd}', flush=True)
        try:
            done = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
            output, code = done.stdout + done.stderr, done.returncode
        except subprocess.TimeoutExpired as exc:
            output, code = f'COMMAND TIMEOUT: {exc}', 124
        print(output, flush=True)
        results.append({'oracle': name, 'mode': cmd[1], 'returncode': code,
                        'seconds': time.monotonic() - start, 'output': output})
        failed |= code != 0
        if cmd[1] == 'check' and not report.exists():
            report.write_text(json.dumps([{'name': n, 'passed': False, 'seconds': time.monotonic()-start,
                'error': f'Checker process failed or timed out before saving verdicts: exit={code}',
                'synthetic_missing_verdict': True} for n in ['Linearizable', 'Watches', 'Durable', 'Revisions']], indent=2)+'\n')
(a.out / 'offline-results.json').write_text(json.dumps(results, indent=2)+'\n')
raise SystemExit(int(failed))
