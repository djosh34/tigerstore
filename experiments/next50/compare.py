#!/usr/bin/env python3
"""Paired changes against the baseline in the same repetition and runner job."""
import argparse
import csv
import json
from pathlib import Path
import statistics


def compare(root):
    rows = list(csv.DictReader((root / 'runs.csv').open()))
    baseline = {int(r['rep']): r for r in rows if r['variant'] == 'baseline'}
    if set(baseline) != set(range(1, 6)):
        raise ValueError('need five baseline repetitions')
    results = {}
    for variant in sorted({r['variant'] for r in rows} - {'baseline', 'etcd'}):
        cohort = sorted((r for r in rows if r['variant'] == variant), key=lambda r: int(r['rep']))
        if {int(r['rep']) for r in cohort} != set(baseline) or len(cohort) != 5:
            raise ValueError(f'incomplete paired candidate {variant}')
        paired = []
        for row in cohort:
            base = baseline[int(row['rep'])]
            paired.append({
                'rep': int(row['rep']),
                'throughput_gain_percent': 100 * (float(row['ops_s']) / float(base['ops_s']) - 1),
                'leader_cpu_saved_percent': 100 * (1 - float(row['leader_cpu_us_op']) / float(base['leader_cpu_us_op'])),
                'leader_cpu_saved_us_put': float(base['leader_cpu_us_op']) - float(row['leader_cpu_us_op']),
                'leader_write_calls_saved_percent': 100 * (1 - float(row['leader_syscw_op']) / float(base['leader_syscw_op'])),
                'p99_reduction_percent': 100 * (1 - float(row['p99_ms']) / float(base['p99_ms'])),
            })
        aggregate = {key: {'median': statistics.median(p[key] for p in paired),
                           'min': min(p[key] for p in paired),
                           'max': max(p[key] for p in paired)}
                     for key in paired[0] if key != 'rep'}
        results[variant] = {'paired': paired, 'changes': aggregate}
    return results


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('root', type=Path)
    args = parser.parse_args()
    print(json.dumps(compare(args.root), indent=2))
