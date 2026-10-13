#!/usr/bin/env python3
"""Download compact evidence only. Preserve every attempt under its run ID."""
import argparse
import json
from pathlib import Path
import subprocess
import time
import zipfile

REPO = 'repos/djosh34/tigerstore'


def api(path):
    for attempt in range(8):
        p = subprocess.run(['gh', 'api', path], capture_output=True)
        if p.returncode == 0:
            return json.loads(p.stdout)
        time.sleep(.5 + attempt * .25)
    raise RuntimeError(p.stderr.decode())


def pages(path, field):
    result = []
    page = 1
    while True:
        rows = api(f'{path}?per_page=5&page={page}')[field]
        result.extend(rows)
        if len(rows) < 5:
            return result
        page += 1


def main():
    p = argparse.ArgumentParser()
    p.add_argument('runs', nargs='+')
    p.add_argument('--out', type=Path, default=Path('experiments/correct-round3/evidence'))
    a = p.parse_args()
    for run in a.runs:
        dst = a.out / str(run)
        dst.mkdir(parents=True, exist_ok=True)
        meta = api(f'{REPO}/actions/runs/{run}')
        jobs = pages(f'{REPO}/actions/runs/{run}/jobs', 'jobs')
        artifacts = pages(f'{REPO}/actions/runs/{run}/artifacts', 'artifacts')
        (dst / 'run.json').write_text(json.dumps(meta, indent=2)+'\n')
        (dst / 'jobs.json').write_text(json.dumps(jobs, indent=2)+'\n')
        (dst / 'artifacts.json').write_text(json.dumps(artifacts, indent=2)+'\n')
        for artifact in artifacts:
            if not artifact['name'].startswith('compact-round3-combined-'):
                continue
            target = dst / artifact['name']
            if (target / 'summary.json').exists():
                continue
            archive = dst / f"{artifact['id']}.zip"
            for attempt in range(6):
                with archive.open('wb') as output:
                    result = subprocess.run(['gh', 'api', f"{REPO}/actions/artifacts/{artifact['id']}/zip"], stdout=output, stderr=subprocess.PIPE)
                if result.returncode == 0:
                    break
                time.sleep(1)
            if result.returncode:
                raise RuntimeError(result.stderr.decode())
            target.mkdir(exist_ok=True)
            with zipfile.ZipFile(archive) as zipped:
                zipped.extractall(target)
            archive.unlink()
            print(run, artifact['name'], flush=True)
        print(run, meta['status'], meta['conclusion'], len(jobs), len(artifacts), flush=True)


if __name__ == '__main__':
    main()
