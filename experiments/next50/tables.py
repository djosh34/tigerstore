#!/usr/bin/env python3
"""Produce numeric tables from archived CSV and attribution/paired JSON files."""
import argparse
import contextlib
import csv
import io
import json
from pathlib import Path
import statistics
import tarfile


def number(value, digits=3):
    return f'{value:,.{digits}f}'


def table(headers, rows):
    print('| ' + ' | '.join(headers) + ' |')
    print('| ' + ' | '.join('---' for _ in headers) + ' |')
    for row in rows:
        print('| ' + ' | '.join(str(x) for x in row) + ' |')
    print()


def median(rows, name):
    values = [float(r[name]) for r in rows if r[name] != '']
    return number(statistics.median(values)) if values else '—'


def measurements(title, rows):
    print(f'### {title}\n')
    groups = [([r for r in rows if r['variant'] == variant], variant)
              for variant in sorted({r['variant'] for r in rows})]
    for cohort, _ in groups:
        for row in cohort:
            for role in ('leader', 'generator'):
                row[role + '_cores'] = float(row['ops_s']) * float(row[role + '_cpu_us_op']) / 1e6
    table(['variant', 'n', 'Put/s', 'p50 ms', 'p99 ms', 'p99.9 ms', 'leader µs/P',
           'follower1 µs/P', 'follower2 µs/P', 'generator µs/P', 'leader cores', 'generator cores'],
          [[variant, len(cohort)] + [median(cohort, name) for name in
            ('ops_s', 'p50_ms', 'p99_ms', 'p999_ms', 'leader_cpu_us_op',
             'follower1_cpu_us_op', 'follower2_cpu_us_op', 'generator_cpu_us_op',
             'leader_cores', 'generator_cores')] for cohort, variant in groups])
    table(['variant', 'role', 'alloc objects/P', 'alloc bytes/P', 'syscr/P', 'syscw/P',
           'Puts/sync', 'proc write_bytes/P', 'retained heap MiB', 'retained RSS MiB'],
          [[variant, role] + [median(cohort, role + '_' + field) for field in
            ('objects_op', 'bytes_op', 'syscr_op', 'syscw_op', 'puts_sync', 'write_bytes_op')]
           + [number(statistics.median(float(r[role + '_' + field]) / 2**20 for r in cohort))
              for field in ('retained_heap_bytes', 'retained_rss_bytes')]
           for cohort, variant in groups for role in ('leader', 'follower1', 'follower2')])


def paired(path):
    print(f'### {path.name}\n')
    headers = ['variant', 'Put/s gain %', 'leader CPU saved µs/P', 'leader CPU saved %',
               'write calls saved %', 'p99 reduction %']
    fields = ('throughput_gain_percent', 'leader_cpu_saved_us_put', 'leader_cpu_saved_percent',
              'leader_write_calls_saved_percent', 'p99_reduction_percent')
    rows = []
    for variant, result in json.loads(path.read_text()).items():
        cells = [result['changes'][field] for field in fields]
        rows.append([variant] + [f"{number(c['median'])} [{number(c['min'])}, {number(c['max'])}]" for c in cells])
    table(headers, rows)


def profiles(path):
    print(f'### {path.name}\n')
    for name, result in json.loads(path.read_text())['aggregate'].items():
        print(f"{name}: {result['operations']:,} operations; sampled total {number(result['sampled_cpu_us_put'])} µs/Put.\n")
        table(['disjoint context', 'µs/Put'],
              [[key, number(value['cpu_us_put'])] for key, value in
               sorted(result['partition'].items(), key=lambda item: -item[1]['cpu_us_put'])])
        table(['overlapping mechanism (do not add)', 'µs/Put'],
              [[key, number(value['cpu_us_put'])] for key, value in result['inclusive'].items()])


def main(root):
    print('# Generated numeric tables\n')
    print('Independent medians across five repetitions; host rows have one observation. '
          'Cores are medians of per-run CPU/elapsed, not products of separate medians. '
          'Paired cells show median [minimum, maximum]; negative savings are regressions. '
          'Profiles are operation-weighted sampled CPU, not process-CPU medians. '
          'All quantities are per successful Put unless labelled otherwise.\n')
    print('CPU spans workers through drain. Other counters span the wider replica-fenced interval. '
          'Retained heap is forced-GC for Tigerstore, not etcd. syscr/syscw include non-socket calls. '
          'Tigerstore syncs exclude directory sync; etcd WAL histogram counts can include them. '
          'See the report for interpretation and source pins.\n')
    for stage in ('lean-initial', 'lean-screen', 'host-lean', 'initial', 'screen', 'confirm', 'host'):
        directory = root / stage
        print(f'## {stage}\n')
        if stage.startswith('host'):
            for path in sorted(directory.glob('c*/runs.csv')):
                with path.open() as stream:
                    measurements(path.parent.name, list(csv.DictReader(stream)))
        else:
            for path in sorted(directory.glob('run-*.tar.gz')):
                with tarfile.open(path) as archive:
                    member = next((m for m in archive.getmembers()
                                   if m.name.removeprefix('./') == 'runs.csv'), None)
                    if member is None:
                        continue  # Race-check artifacts do not contain measurements.
                    with io.TextIOWrapper(archive.extractfile(member)) as stream:
                        measurements(path.name, list(csv.DictReader(stream)))
        for path in sorted(directory.glob('*paired-changes.json')):
            paired(path)
        for path in sorted(directory.glob('*attribution.json')):
            profiles(path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('root', type=Path)
    output = io.StringIO()
    with contextlib.redirect_stdout(output):
        main(parser.parse_args().root)
    print(output.getvalue().rstrip())
