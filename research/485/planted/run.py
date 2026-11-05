#!/usr/bin/env python3
"""One long-lived driver, one gosim process at a time, with /proc RSS sampling."""
from bisect import bisect_left, bisect_right
import json
import os
from pathlib import Path
import subprocess
import sys
import time

root = Path(__file__).resolve().parents[3]
out = Path(os.environ.get('EXP485_OUT', '/tmp/exp485-planted'))
out.mkdir(parents=True, exist_ok=True)
meta = {k: os.environ.get(k) for k in ('EXP485_DURATION', 'EXP485_BUDGET', 'EXP485_BUGS', 'GITHUB_SHA', 'GITHUB_RUN_ID', 'GITHUB_JOB', 'GOMAXPROCS')}
meta.update(platform=subprocess.check_output(['uname', '-a'], text=True).strip(), first_seed=1)
(out / 'metadata.json').write_text(json.dumps(meta, indent=2) + '\n')
env = dict(os.environ, GOSIMTOOL=str(root / '.gosim/bin/gosim'), SIM_SEED='1')
with (out / 'seed.log').open('w') as log, (out / 'rss.csv').open('w') as samples:
    samples.write('unix_ms,translated_rss_kb,driver_rss_kb\n')
    p = subprocess.Popen([str(root / '.gosim/bin/sim-planted.test'), '-test.run', '^TestExp485Planted$', '-test.timeout', '150m'], cwd=root / 'internal/sim', env=env, stdout=log, stderr=subprocess.STDOUT)
    while p.poll() is None:
        translated = driver = 0
        for proc in Path('/proc').iterdir():
            if not proc.name.isdigit():
                continue
            try:
                name = (proc / 'comm').read_text().strip()
                if not (name.startswith('translated') or int(proc.name) == p.pid):
                    continue
                rss = int((proc / 'statm').read_text().split()[1]) * os.sysconf('SC_PAGE_SIZE') // 1024
                if name.startswith('translated'):
                    translated += rss
                else:
                    driver += rss
            except (OSError, ValueError, IndexError):
                pass
        samples.write(f'{time.time_ns() // 1000000},{translated},{driver}\n')
        samples.flush()
        time.sleep(.1)
    status = p.returncode
rows = []
for line in (out / 'seed.log').read_text().splitlines():
    if line.startswith('EXP485 '):
        rows.append(json.loads(line[len('EXP485 '):]))
samples = [[int(x) for x in line.split(',')] for line in (out / 'rss.csv').read_text().splitlines()[1:]]
sample_times = [s[0] for s in samples]
for row in rows:
    selected = samples[bisect_left(sample_times, row['start_unix_ms']):bisect_right(sample_times, row['end_unix_ms'])]
    row['peak_rss_kb'] = max((s[1] for s in selected), default=None)
    row['peak_driver_rss_kb'] = max((s[2] for s in selected), default=None)
    row['run_id'] = os.environ.get('GITHUB_RUN_ID', 'local')
    row['commit'] = os.environ.get('GITHUB_SHA', 'local')
with (out / 'seeds.jsonl').open('w') as f:
    for row in rows:
        f.write(json.dumps(row, separators=(',', ':')) + '\n')
print(f'rows={len(rows)} status={status} out={out}', flush=True)
sys.exit(status)
