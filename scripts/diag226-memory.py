#!/usr/bin/env python3
"""Extract peak rings and a reproducible summary from the diag226 full series."""
import datetime as dt
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
MIB = 1 << 20

def stamp(text):
    return dt.datetime.fromisoformat(text.rstrip('Z') + '+00:00')

def parse(line):
    stats = dict((k, int(v)) for k, v in re.findall(r'(\w+) (\d+)', line.split('stat={')[1].split('}')[0]))
    metrics = dict((k, float(v)) for k, v in re.findall(r'(\w+) ([\d.]+)', line.split('; ', 1)[1]))
    return {
        'time': line.split()[0],
        'current': int(re.search(r'current=(\d+)', line)[1]),
        'peak': int(re.search(r'peak=(\d+)', line)[1]),
        'incarnation': int(re.search(r'incarnation=(\d+)', line)[1]),
        'stat': stats,
        # Count ALL kernel memory conservatively, including reclaimable slab.
        # Also add sockets, whether or not this kernel includes them in kernel.
        'nonfile': stats['anon'] + stats['kernel'] + stats['sock'],
        'metrics': metrics,
        'metrics_text': line.split('; ', 1)[1],
    }

def show(sample):
    s = sample['stat']
    return dict(time=sample['time'], incarnation=sample['incarnation'],
                current_MiB=round(sample['current']/MIB, 3),
                peak_bytes=sample['peak'],
                anon_MiB=round(s['anon']/MIB, 3),
                kernel_MiB=round(s['kernel']/MIB, 3),
                sock_MiB=round(s['sock']/MIB, 3),
                file_MiB=round(s['file']/MIB, 3),
                file_mapped_MiB=round(s['file_mapped']/MIB, 3),
                file_dirty_MiB=round(s['file_dirty']/MIB, 3),
                file_writeback_MiB=round(s['file_writeback']/MIB, 3),
                anon_kernel_sock_MiB=round(sample['nonfile']/MIB, 3),
                margin_MiB=round(640-sample['nonfile']/MIB, 3),
                pgscan_direct=s.get('pgscan_direct'), pgsteal_direct=s.get('pgsteal_direct'),
                metrics=sample['metrics_text'])

results = []
for n in (1, 2, 3):
    lines = (root / f'node{n}-memory-series.txt').read_text().splitlines()
    samples = [parse(line) for line in lines]
    current = max(range(len(samples)), key=lambda i: samples[i]['current'])
    kernel_peak = max(s['peak'] for s in samples)
    peak_first = next(i for i, s in enumerate(samples) if s['peak'] == kernel_peak)
    nonfile = max(range(len(samples)), key=lambda i: samples[i]['nonfile'])
    for label, index in [('peak', current), ('kernel-peak', peak_first), ('nonfile-peak', nonfile)]:
        t = stamp(samples[index]['time'])
        selected = [line for line, s in zip(lines, samples) if -120 <= (stamp(s['time'])-t).total_seconds() <= 5]
        (root / f'node{n}-memory-{label}.txt').write_text('\n'.join(selected)+'\n')
    logs = (root / f'node{n}.log').read_text().splitlines()
    events = []
    for line in logs:
        try:
            record = json.loads(line)
            if any(x in record.get('msg', '') for x in ('recovering the log', 'recovered the log', 'fetching the snapshot', 'installed the snapshot')):
                events.append(record)
        except (json.JSONDecodeError, TypeError):
            pass
    def nearby(index):
        t = stamp(samples[index]['time'])
        return [event for event in events if abs((stamp(event['ts'])-t).total_seconds()) <= 20]
    peaks = {}
    for metric in ('heap_alloc', 'heap_inuse', 'heap_sys', 'sys', 'go_gc_gomemlimit'):
        available = [(s['metrics'][metric], s['time']) for s in samples if metric in s['metrics']]
        if available:
            peaks[metric] = max(available)
    result = dict(node=n, samples=len(samples), highest_current=show(samples[current]),
                  first_final_kernel_peak=show(samples[peak_first]),
                  max_anon_kernel_sock=show(samples[nonfile]),
                  max_anon_MiB=round(max(s['stat']['anon'] for s in samples)/MIB, 3),
                  observed_metric_maxima_MiB=peaks,
                  events_at_current_peak=nearby(current), events_at_kernel_peak=nearby(peak_first),
                  events_at_nonfile_peak=nearby(nonfile))
    results.append(result)

summary = json.dumps(results, indent=2)
(root / 'memory-summary.json').write_text(summary+'\n')
print(summary)
