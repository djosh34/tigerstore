#!/usr/bin/env python3
"""Validate and summarize saved closed-loop observation controls. No subprocesses or measurements."""
import argparse
import gzip
import hashlib
import json
import math
from pathlib import Path
import statistics

TARGETS = ['pristine', 'tigerstore-off', 'tigerstore', 'etcd']
COMPARE = [('tigerstore', 'tigerstore-off'), ('tigerstore-off', 'pristine'), ('tigerstore', 'pristine')]
METRICS = ['tp', 'p50', 'p99', 'p999', 'cohort_cpu', 'window_cpu', 'leader_cpu', 'client_cpu']
TICK_NS = 10_000_000


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def bounds(values):
    return {'median': statistics.median(values), 'min': min(values), 'max': max(values), 'all_five': values}


def position(status):
    s = status['Status']
    return [int(s.get('raftAppliedIndex', s.get('raft_applied_index'))),
            int(s.get('raftIndex', s.get('raft_index'))), int(s['header']['revision'])]


def cpu_ns(us_per_op, denominator):
    value = us_per_op * denominator * 1000
    rounded = round(value)
    assert abs(value - rounded) < 1, ('nonintegral CPU ns', value)
    assert rounded >= 0 and rounded % TICK_NS == 0, ('not Linux USER_HZ ticks', rounded)
    return rounded


class Analysis:
    def __init__(self, root):
        self.root = root
        self.inputs = {}

    def text(self, path):
        self.inputs[str(path.relative_to(self.root))] = {'sha256': digest(path), 'bytes': path.stat().st_size}
        return path.read_text()

    def read(self, path):
        return json.loads(self.text(path))

    def run(self):
        rows = []
        metadata = {}
        for artifact in sorted(self.root.glob('*/shape-*')):
            cell = artifact.name.removeprefix('shape-')
            assert cell not in metadata
            plan = self.read(artifact / 'plan.json')
            assert plan['observation_control'] and plan['profile'] == 'none'
            assert plan['targets'] == TARGETS and plan['reps'] == 5 and not plan['rates']
            assert [(w['rep'], w['kind']) for w in plan['windows']] == [(r, t) for r in range(1, 6) for t in TARGETS]
            hashes = {Path(line.split()[1]).name: line.split()[0] for line in
                      self.text(artifact / 'observation-control/binaries-sha256.txt').splitlines()}
            assert hashes['tigerstore'] == hashes['tigerstore-off'] != hashes['pristine']
            pristine = self.text(artifact / 'observation-control/pristine-commit.txt').strip()
            assert pristine == 'e5c3327e3800edf64f86ed8c1971ba7ebc1e8342'
            runner = self.text(artifact / 'runner.txt')
            commands = self.read(artifact / 'commands.json')
            gens = {}
            starts = {}
            for c in commands:
                argv = c['argv']
                if argv and Path(argv[0]).name == 'loadgen':
                    label = Path(argv[argv.index('-out') + 1]).parent.name
                    gens[label] = c
                if 'bench/cluster.sh' in argv and 'start' in argv:
                    starts[Path(argv[-4]).name] = argv
            metadata[cell] = {'run': artifact.parent.name, 'plan': plan, 'binary_hashes': hashes,
                              'pristine_commit': pristine, 'runner_source': runner.splitlines()[1]}
            for w in plan['windows']:
                target, rep = w['kind'], w['rep']
                base = artifact / Path(w['path']).parent
                load = self.read(base / 'load.json')
                audit = self.read(base / 'audit.json')
                before, after = [self.read(base / n) for n in ['before.json', 'after.json']]
                placement = self.read(base / 'placement.json')
                shape = load['shape']
                n = shape['success']
                assert load['valid'] and audit['valid'] and audit['returncode'] == 0
                assert not load['errors'] and not audit['sampling_errors'] and not audit['invariant_errors']
                assert audit['observation_control'] and (target == 'etcd' or not audit['physical_counter_fence'])
                start_argv = starts[f'{target}-{rep}']
                assert Path(start_argv[-5]).name == target
                assert ('SHAPE_OBSERVE=0' in start_argv) == (target in ['pristine', 'tigerstore-off'])
                if target in ['pristine', 'tigerstore-off']:
                    assert before['nodes'] == after['nodes'] == []
                elif target == 'tigerstore':
                    assert len(before['nodes']) == len(after['nodes']) == 3
                assert load['label'] == target and load['offered_rate'] == 0
                assert load['warmup_s'] == 1 and plan['warmup'] == '1s'
                assert shape['arrival'] == 'closed-loop' and shape['dispatch'] == 'global-ticket-round-robin'
                assert load['cohort'] == shape['expected'] == n == load['latency_us']['count']
                assert n > 0 and not load['cohort_failed'] and not load['unsent']
                assert load['ops'] <= n and shape['peak_inflight'] <= shape['concurrency']
                assert load['service_latency_us']['count'] == n
                assert audit['revision_delta'] == audit['expected'] == (n if load['workload'] == 'put' else 0)
                assert shape['arrival_window_s'] == load['duration_s']
                assert math.isclose(shape['through_drain_s'], (shape['done_unix_ns'] - shape['start_unix_ns']) / 1e9, abs_tol=1e-9)
                assert math.isclose(shape['arrival_window_s'], (shape['arrival_stop_unix_ns'] - shape['start_unix_ns']) / 1e9, abs_tol=1e-9)
                assert math.isclose(shape['cohort_throughput'], n / shape['through_drain_s'])
                assert math.isclose(load['throughput'], load['ops'] / load['duration_s'])
                argv = gens[f'{target}-{rep}']['argv']
                pids = list(map(int, argv[argv.index('-pids') + 1].split(',')))
                assert pids == list(map(int, placement['pids_leader_first'].split(',')))
                assert placement['cpus_leader_first'] == '0 1 2' and before['generator_affinity'] == [3]
                assert gens[f'{target}-{rep}']['env']['GOMAXPROCS'] == '1'
                cp = shape['cpu_through_drain']
                assert len(cp['server_us_per_op_by_node']) == len(load['server_cpu_us_per_op_by_node']) == 3
                cohort_ns = [cpu_ns(u, n) for u in cp['server_us_per_op_by_node']]
                window_ns = [cpu_ns(u, load['ops']) for u in load['server_cpu_us_per_op_by_node']]
                assert math.isclose(load['server_cpu_us_per_op'], sum(load['server_cpu_us_per_op_by_node']))
                clocks = [ns / 1e9 / cores for ns, cores in zip(window_ns, load['server_cores_by_node']) if cores > 0]
                assert max(clocks) - min(clocks) < 1e-8
                window_s = statistics.mean(clocks)
                client_clock = load['client_cpu_us_per_op'] * load['ops'] / 1e6 / load['client_cores']
                assert abs(client_clock - window_s) < 1e-8
                assert window_s >= load['duration_s'] and cp['elapsed_s'] >= shape['through_drain_s']
                fenced_ns = []
                for i, (a, b) in enumerate(zip(before['proc'], after['proc'])):
                    assert a['pid'] == b['pid'] == pids[i] and a['starttime'] == b['starttime']
                    delta = (b['utime'] + b['stime'] - a['utime'] - a['stime']) * TICK_NS
                    assert delta >= max(cohort_ns[i], window_ns[i])
                    fenced_ns.append(delta)
                    for snap, proc in [(before, a), (after, b)]:
                        assert snap['affinity'][str(pids[i])] == str(i)
                        for tid in proc['schedstat']:
                            if tid in snap['affinity']:
                                assert snap['affinity'][tid] == str(i)
                if target != 'etcd':
                    for snap in [before, after]:
                        fence = snap['persistence_fence']
                        assert fence['native_applied_only'] and fence['cooldown_s'] == .3
                        first = [position(s) for s in fence['first_status']]
                        second = [position(s) for s in fence['second_status']]
                        assert first == second and all(s == second[0] for s in second)
                        assert second[0][0] == second[0][1]
                        assert [position(s) for s in snap['status']] == second
                counts = shape['endpoint_counts']
                assert sum(e['success'] for e in counts) == n
                if 'balanced' in cell:
                    assert len(counts) == 3 and max(e['sent'] for e in counts) - min(e['sent'] for e in counts) <= 1
                metrics = {'tp': shape['cohort_throughput'], 'p50': load['latency_us']['p50'],
                           'p99': load['latency_us']['p99'], 'p999': load['latency_us']['p999'],
                           'cohort_cpu': sum(cp['server_us_per_op_by_node']),
                           'window_cpu': load['server_cpu_us_per_op'],
                           'leader_cpu': cp['server_us_per_op_by_node'][0], 'client_cpu': cp['client_us_per_op']}
                # Do not extract file, fsync, disk, IO or owner-queue counters.
                rows.append({'cell': cell, 'target': target, 'rep': rep, 'metrics': metrics,
                             'load': load, 'audit': audit, 'cohort_server_cpu_ns': cohort_ns,
                             'window_server_cpu_ns': window_ns, 'window_cpu_elapsed_s': window_s,
                             'external_fenced_server_cpu_ns': fenced_ns,
                             'external_fenced_elapsed_s': after['monotonic'] - before['monotonic'],
                             'pids': pids, 'starttimes': [s['starttime'] for s in before['proc']],
                             'before_revision': int(before['status'][0]['Status']['header']['revision']),
                             'native_cooldown_validated': target != 'etcd'})
        assert len(metadata) == 6 and len(rows) == 120
        summary = {}
        paired = {}
        for cell in metadata:
            summary[cell] = {}
            for target in TARGETS:
                selected = sorted((r for r in rows if r['cell'] == cell and r['target'] == target), key=lambda r: r['rep'])
                assert [r['rep'] for r in selected] == list(range(1, 6))
                summary[cell][target] = {m: bounds([r['metrics'][m] for r in selected]) for m in METRICS}
            paired[cell] = {}
            for numerator, denominator in COMPARE:
                paired[cell][numerator + '/' + denominator] = {
                    m: bounds([100 * (n / d - 1) for n, d in zip(summary[cell][numerator][m]['all_five'], summary[cell][denominator][m]['all_five'])]) for m in METRICS}
        return {'schema_version': 1, 'physical_claims': False, 'metadata': metadata,
                'rows': rows, 'summary': summary, 'paired_percent_changes': paired}


def interval(s, digits=2):
    return f"{s['median']:.{digits}f} [{s['min']:.{digits}f}, {s['max']:.{digits}f}]"


def tables(data):
    lines = ['# Observation-control computed tables', '',
             'All intervals are median [minimum, maximum] across five repetitions. Percent changes pair repetition numbers, not ratios of medians. CPU is process user plus system time. No physical-counter claims.', '']
    def table(title, headings, rows):
        lines.extend(['## ' + title, '', '| ' + ' | '.join(headings) + ' |', '| ' + ' | '.join(['---'] * len(headings)) + ' |'])
        lines.extend('| ' + ' | '.join(map(str, r)) + ' |' for r in rows)
        lines.append('')
    table('Absolute metrics, all four targets', ['Cell', 'Target', 'Through-drain ops/s', 'p50 us', 'p99 us', 'p99.9 us', 'Cluster cohort CPU us/success', 'Cluster window CPU us/window completion'],
          [[cell, target] + [interval(v[m]) for m in ['tp', 'p50', 'p99', 'p999', 'cohort_cpu', 'window_cpu']] for cell, targets in data['summary'].items() for target, v in targets.items()])
    for numerator, denominator in COMPARE:
        label = numerator + '/' + denominator
        table(label + ' paired percent changes', ['Cell', 'TP', 'p50', 'p99', 'p99.9', 'Cohort CPU', 'Window CPU', 'Leader CPU', 'Generator CPU'],
              [[cell] + [interval(v[label][m]) for m in METRICS] for cell, v in data['paired_percent_changes'].items()])
    table('Before-cohort revisions, seed and warmup state, not exact warmup counts', ['Cell', 'Pristine, reps 1-5', 'Off, reps 1-5', 'Observed, reps 1-5'],
          [[cell] + [', '.join(str(r['before_revision']) for r in data['rows'] if r['cell'] == cell and r['target'] == t) for t in TARGETS[:3]] for cell in data['metadata']])
    return '\n'.join(lines).rstrip() + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results-root', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    args = parser.parse_args()
    root, output = args.results_root.resolve(), args.output_dir.resolve()
    if output == root or root in output.parents:
        parser.error('Output must be outside the input artifact tree')
    analysis = Analysis(root)
    data = analysis.run()
    output.mkdir(parents=True, exist_ok=True)
    output.joinpath('computed.json.gz').write_bytes(gzip.compress((json.dumps(data, indent=2, sort_keys=True) + '\n').encode(), mtime=0))
    output.joinpath('computed-tables.md').write_text(tables(data))
    manifest = {'inputs': analysis.inputs, 'script_sha256': digest(Path(__file__)), 'subprocesses': [], 'physical_claims': False}
    output.joinpath('input-manifest.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
