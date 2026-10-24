#!/usr/bin/env python3
"""Host-side instrumentation: never run the simulator on the research host."""
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

print(json.dumps({'cpus': os.cpu_count(), 'meminfo': Path('/proc/meminfo').read_text().splitlines()[:3]}), flush=True)
root = Path(os.environ['RUNNER_TEMP'])
n, duration, seed = (int(os.environ[k]) for k in ('OBJECTS', 'DURATION', 'SEED'))
wall_limit = min(int(os.environ['WALL_SECONDS']), 19800)
start = time.monotonic()
log = (root / 'size.log').open('w')
cmd = ['/usr/bin/time', '-v', '-o', str(root / 'size-time.txt'),
       str(root / 'size-bin/size.test'), '-test.run', f'^TestSize_{n}_{duration}$',
       '-seeds', str(seed), '-logformat', 'raw']
child_env = dict(os.environ, GODEBUG='gctrace=1')
p = subprocess.Popen(cmd, stdout=log, stderr=log, start_new_session=True, env=child_env)
peak = 0
outcome = None
with (root / 'size-samples.jsonl').open('w') as samples:
    while p.poll() is None:
        time.sleep(5)
        # GNU time is the parent; the translated binary is its direct child.
        children = Path(f'/proc/{p.pid}/task/{p.pid}/children')
        pids = children.read_text().split() if children.exists() else []
        rss = 0
        for pid in pids:
            try:
                status = Path(f'/proc/{pid}/status').read_text()
                rss += int(re.search(r'^VmRSS:\s+(\d+)', status, re.M).group(1))
            except (OSError, AttributeError):
                pass
        peak = max(peak, rss)
        text = (root / 'size.log').read_text(errors='replace')
        progress = re.findall(r'SIZE_(?:PROGRESS|DONE|PRELOADED)[^\n]*', text)
        latest = progress[-1] if progress else ''
        elapsed = time.monotonic() - start
        row = dict(wall_s=round(elapsed, 3), rss_kib=rss, progress=latest)
        print(json.dumps(row), flush=True)
        samples.write(json.dumps(row) + '\n'); samples.flush()
        if rss > 12 * 1024 * 1024:
            outcome = 'memory_guard_12GiB'
        elif elapsed >= wall_limit:
            outcome = 'wall_limit'
        if outcome:
            # Kill only the child so GNU time can collect its final rusage.
            for pid in pids:
                try:
                    os.kill(int(pid), signal.SIGQUIT if outcome.startswith('memory_guard') else signal.SIGKILL)
                except ProcessLookupError:
                    pass
            break
p.wait()
log.close()
elapsed = time.monotonic() - start
text = (root / 'size.log').read_text(errors='replace')
timing = (root / 'size-time.txt').read_text() if (root / 'size-time.txt').exists() else ''
m = re.search(r'Maximum resident set size \(kbytes\): (\d+)', timing)
if m:
    peak = max(peak, int(m.group(1)))
# Use rusage's elapsed time, not the five-second monitor polling interval.
m = re.search(r'Elapsed \(wall clock\) time \(h:mm:ss or m:ss\): ([\d:.]+)', timing)
if m:
    elapsed = 0
    for part in m.group(1).split(':'):
        elapsed = elapsed * 60 + float(part)
sim = re.findall(r'(?<!churn_)sim_s=([\d.]+)', text)
churn = re.findall(r'churn_s=([\d.]+)', text)
result = dict(objects=n, duration_s=duration, seed=seed,
              value_divisor=int(os.environ.get('VALUE_DIVISOR', '1')),
              watch_after_preload=os.environ.get('WATCH_AFTER_PRELOAD', 'false').lower() == 'true',
              compact_preload=os.environ.get('COMPACT_PRELOAD', 'false').lower() == 'true', wall_s=elapsed,
              peak_rss_kib=peak, last_sim_s=float(sim[-1]) if sim else 0,
              last_churn_s=float(churn[-1]) if churn else 0,
              exit_code=p.returncode,
              outcome=outcome or ('finished' if p.returncode == 0 and 'SIZE_DONE' in text else 'failed'))
result['sim_s_per_wall_s'] = result['last_sim_s'] / elapsed
(root / 'size-result.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result), flush=True)
print('\n'.join(line for line in text.splitlines() if 'SIZE_' in line)[-10000:])
print(timing)
with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as f:
    f.write('```json\n' + json.dumps(result, indent=2) + '\n```\n')
