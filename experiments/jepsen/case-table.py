#!/usr/bin/env python3
"""Generate the per-job table from retained Actions metadata and offline summaries."""
import collections
import datetime
import json
import pathlib
import re

root = pathlib.Path(__file__).parent / 'evidence'
counts = collections.Counter()
rows = []
run_rows = []
for path in sorted(root.glob('*-summary.json')):
    run = path.name.split('-')[0]
    summaries = json.loads(path.read_text())
    jobs = json.loads((root / f'{run}-jobs.json').read_text())['jobs']
    durations = []
    for case in summaries:
        fields = case['directory'].removeprefix('jepsen-').split('-')
        store, workload, nemesis = fields[:3]
        repetition = next((f.removeprefix('rep') for f in fields if f.startswith('rep')), None)
        label = ', '.join([store, workload, nemesis] + ([repetition] if repetition else []))
        job = next(j for j in jobs if j['name'] == f'jepsen ({label})')
        seconds = int((datetime.datetime.fromisoformat(job['completed_at'])
                       - datetime.datetime.fromisoformat(job['started_at'])).total_seconds())
        durations.append(seconds)
        status = case['status']
        if case.get('valid') == 'unknown':
            status = 'inconclusive'
        counts[(store, status)] += 1
        match = re.search(r'--ops-per-key (\d+)', case.get('command', ''))
        budget = match[1] if match else ('200' if case.get('command') else '')
        count = case.get('stats', {}).get('count', '')
        rows.append(f'| [{run}]({job["html_url"]}) | {store} | {workload} | {nemesis} | {budget} | {repetition or 1} | {seconds // 60}m{seconds % 60:02}s | {status} | {count} |')
    run_rows.append(f'| [{run}](https://github.com/djosh34/tigerstore/actions/runs/{run}) | {len(summaries)} | {min(durations)//60}m{min(durations)%60:02}s to {max(durations)//60}m{max(durations)%60:02}s |')
print('# Per-job results\n')
print('Durations are Actions job start to finish, including setup and artifact upload, excluding queue time. A pass includes the upstream checker verdict and the offline fault-observation checks. Inconclusive means `:valid? :unknown`; it is a failed gate, not a pass. Blocked clock cases never ran their workloads.\n')
print('| Store | Outcome | Jobs |\n| --- | --- | --- |')
for (store, status), count in sorted(counts.items()):
    print(f'| {store} | {status} | {count} |')
print('\n| Run | Jobs | Job duration range |\n| --- | --- | --- |')
print('\n'.join(run_rows))
print('\n| Run/job | Store | Workload | Fault | Ops/key | Repetition | Duration | Outcome | Operations |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- |')
print('\n'.join(rows))
