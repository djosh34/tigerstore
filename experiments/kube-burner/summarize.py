#!/usr/bin/env python3
"""Summarize complete repetitions without pooling their percentiles."""
import argparse
import csv
import json
import math
from pathlib import Path
import statistics


def load(path):
    return json.loads(path.read_text()) if path.exists() else None


def summarize(root, out):
    out.mkdir(parents=True, exist_ok=True)
    rows, runs = [], []
    for path in sorted(root.rglob('result.json')):
        run = load(path)
        if not {'arch', 'store', 'rep', 'experiment_ref'} <= run.keys():
            continue
        directory = path.parent
        workload = load(directory / 'workload/summary.json')
        if workload is None:
            continue
        run = dict(run, directory=str(directory), workload_passed=workload.get('workloadPassed', workload.get('passed')))
        runs.append(run)

        def number(metric, value, unit):
            if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
                rows.append(dict(arch=run['arch'], store=run['store'], rep=run['rep'],
                                 metric=metric, value=value, unit=unit,
                                 workload_passed=run['workload_passed'], run_exit=run['exit']))

        def metrics(prefix, m):
            if m is None:
                return
            number(prefix + '.window', m.get('elapsed_seconds'), 's')
            hist = m['apiserver']['request_duration']
            groups = {'all': hist['overall'], **{'verb.' + k: v for k, v in hist['by_verb'].items()},
                      **{'resource.' + k: v for k, v in hist.get('by_resource', {}).items()}}
            for key, values in groups.items():
                for quantile in ['p50', 'p99']:
                    value = values.get(quantile + '_seconds')
                    number(prefix + '.api.' + key + '.' + quantile, value * 1000 if value is not None else None, 'ms')
            requests = m['apiserver']['requests']
            number(prefix + '.api.requests', requests.get('per_second'), 'requests/s')
            for key, verbs in [('creates', {'POST', 'CREATE'}), ('deletes', {'DELETE', 'DELETECOLLECTION'}), ('updates', {'PUT', 'PATCH', 'UPDATE'})]:
                selected = [r for r in requests['series'] if r['labels'].get('verb') in verbs and r['labels'].get('code', '').startswith('2') and r['labels'].get('resource') and not r['labels'].get('subresource')]
                if selected and all(r.get('delta') is not None for r in selected) and not requests['issues']:
                    total = sum(r['delta'] for r in selected)
                    number(prefix + '.api.' + key + '.count', total, 'requests')
                    number(prefix + '.api.' + key + '.rate', total / m['elapsed_seconds'], 'requests/s')
            for code, values in m['apiserver']['status_details']['by_status_code'].items():
                if code.startswith(('4', '5')):
                    number(prefix + '.api.http' + code, values.get('delta'), 'responses')
            for key, values in m['storage_rpc']['histograms'].items():
                number(prefix + '.storage.' + key, values['overall'].get('per_second'), 'RPC/s')
            cpu = m['cpu']['containers']
            values = [cpu[f'external-etcd-{n}'].get('cpu_seconds') for n in range(1, 4)]
            cores = [cpu[f'external-etcd-{n}'].get('average_cores') for n in range(1, 4)]
            for n, value in enumerate(values, 1):
                number(prefix + f'.cpu.member{n}', value, 'CPU s')
            if all(v is not None for v in values):
                number(prefix + '.cpu.all', sum(values), 'CPU s')
                number(prefix + '.cpu.cores', sum(cores), 'cores')
            revisions = m['etcd_revision']['endpoints']
            for n in range(1, 4):
                number(prefix + f'.revision.member{n}', revisions[f'external-etcd-{n}'].get('delta'), 'revisions')
            for member, server in m['server_metrics'].items():
                for name, value in server['counters'].items():
                    number(prefix + '.server.' + member + '.' + name, value.get('per_second'), 'counter increments/s')

        number('total.script', run.get('total_seconds'), 's')
        number('total.workload', workload.get('totalSeconds'), 's')
        number('errors.observed', workload.get('errorCount'), 'records')
        number('errors.workload', workload.get('workloadFailureCount'), 'records')
        metrics('total', load(directory / 'metrics.json'))
        previous_inventory = 0
        for phase in workload['phases']:
            name = phase['name']
            inventory = phase.get('runOwnedCounts')
            if inventory is not None:
                total = sum(inventory.values())
                number(name + '.live_inventory', total, 'objects in tracked kinds')
                if name in {'mixed', 'node'}:
                    number(name + '.net_live_creation_rate', (total - previous_inventory) / phase['workloadSeconds'], 'objects/s')
                if name == 'delete' and total == 0:
                    number(name + '.verified_deletion_rate', previous_inventory / phase['workloadSeconds'], 'objects/s')
                previous_inventory = total
            number(name + '.duration', phase['workloadSeconds'], 's')
            number(name + '.exit', phase['returncode'], 'exit code')
            metrics(name, load(directory / 'workload' / name / 'diff.json'))
        native = load(directory / 'workload/native-summary.json') or []
        for doc in native:
            if doc.get('metricName') == 'podLatencyQuantilesMeasurement':
                prefix = doc['phase'] + '.pod.' + doc['quantileName']
                for quantile in ['P50', 'P99']:
                    number(prefix + '.' + quantile.lower(), doc.get(quantile), 'ms')
        event_path = directory / 'workload/events.jsonl'
        if event_path.exists():
            events = [json.loads(line)['event'] for line in event_path.read_text().splitlines() if line.strip()]
            number('events.unique', len({e['metadata']['uid'] for e in events}), 'events')
            number('events.observations', len(events), 'watch notifications')

    groups = {}
    for row in rows:
        groups.setdefault((row['arch'], row['store'], row['metric'], row['unit']), []).append(row)
    summary = []
    for (arch, store, metric, unit), members in sorted(groups.items()):
        if len({m['rep'] for m in members}) != len(members):
            raise ValueError(f'Duplicate repetition for {arch}/{store}/{metric}; use one complete run only')
        values = [m['value'] for m in members]
        summary.append(dict(arch=arch, store=store, metric=metric, unit=unit, n=len(values),
                            min=min(values), median=statistics.median(values), max=max(values),
                            all_workloads_passed=all(m['workload_passed'] for m in members)))
    for name, data in [('runs', runs), ('rows', rows), ('spread', summary)]:
        (out / (name + '.json')).write_text(json.dumps(data, indent=2) + '\n')
        if data:
            with (out / (name + '.csv')).open('w') as file:
                writer = csv.DictWriter(file, fieldnames=list(data[0]))
                writer.writeheader()
                writer.writerows(data)
    lines = ['# Per-repetition spread', '', 'Each row is the min, median and max of per-run statistics. Percentiles are not pooled.', '', '| Architecture | Store | Metric | Unit | n | Min | Median | Max | Workloads passed |', '|---|---|---|---|---:|---:|---:|---:|---|']
    for row in summary:
        lines.append('| {arch} | {store} | {metric} | {unit} | {n} | {min:.4f} | {median:.4f} | {max:.4f} | {all_workloads_passed} |'.format(**row))
    (out / 'spread.md').write_text('\n'.join(lines) + '\n')
    print(f'{len(runs)} runs; {len(rows)} values; {len(summary)} spread rows')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('out', type=Path)
    args = parser.parse_args()
    summarize(args.root, args.out)
