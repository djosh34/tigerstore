#!/usr/bin/env python3
"""Download new compact artifacts only; never request the full disk artifacts."""
import argparse
import datetime as dt
import json
from pathlib import Path
import shutil
import subprocess
import sys


def main():
    p = argparse.ArgumentParser()
    p.add_argument('run_id')
    a = p.parse_args()
    if dt.datetime.now(dt.timezone.utc) >= dt.datetime(2026, 10, 13, 22, tzinfo=dt.timezone.utc):
        raise SystemExit('deadline reached; no new downloads')
    root = Path(__file__).resolve().parent
    out = root / 'results' / f'run-{a.run_id}'
    api = subprocess.check_output(['gh', 'api', f'repos/djosh34/tigerstore/actions/runs/{a.run_id}/artifacts', '--paginate', '--slurp'], text=True)
    artifacts = [item for page in json.loads(api) for item in page['artifacts']]
    names = [item['name'] for item in artifacts if item['name'].startswith('compact-round2-faults-') and
             not (out / item['name'].replace('compact-round2-faults-', 'correct-round2-faults-', 1)).exists()]
    if not names:
        print('No new compact artifacts')
        return
    stage = root / 'artifacts' / f'new-{a.run_id}'
    if stage.exists():
        raise SystemExit(f'reconcile previous interrupted download first: {stage}')
    argv = ['gh', 'run', 'download', a.run_id, '--repo', 'djosh34/tigerstore', '--dir', str(stage)]
    for name in names:
        argv += ['-n', name]
    subprocess.run(argv, check=True)
    # Keep stage if validation fails, so no evidence disappears.
    subprocess.run([sys.executable, str(root / 'collect-compact.py'), str(stage), str(out)], check=True)
    shutil.rmtree(stage)


if __name__ == '__main__':
    main()
