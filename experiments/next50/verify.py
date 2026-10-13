#!/usr/bin/env python3
"""Recheck archived window accounting, placement, cohorts and pprof bounds."""
import argparse
import hashlib
import json
import math
from pathlib import Path
import tarfile
import tempfile

from check_profiles import check


def load(path):
    return json.loads(path.read_text())


def window(folder, command):
    for name, count in (('seed', 4096), ('warmup', 8000)):
        setup = load(folder / f'{name}.json')
        assert setup['valid'] and not setup['errors'] and setup['successes'] == count, folder
        assert setup['revision_after'] - setup['revision_before'] == count, folder
    p = load(folder / 'put.json')
    assert p['valid'] and not p['errors'] and p['controller_barriers'], folder
    n = p['successes']
    assert n > 0 and p['revision_after'] - p['revision_before'] == n, folder
    assert len(p['terms']) == 1 and sum(p['terms'].values()) == n, folder
    term = int(next(iter(p['terms'])))
    assert term > 0 and p['elapsed_s'] > 0, folder
    assert math.isclose(p['ops_s'], n / p['elapsed_s'], rel_tol=1e-12), folder
    assert p['conns'] == 16 and p['workers'] in (64, 256), folder
    assert p['keyspace'] == 4096 and p['value_bytes'] == 1024, folder
    argv = command['argv']
    total = int(argv[argv.index('-total') + 1])
    if total == 0:
        assert argv[argv.index('-duration') + 1] == '12s' and p['elapsed_s'] >= 11.99, folder
    else:
        assert total in (30000, 60000) and total == n, folder
    assert command['env']['GOFLAGS'] == '-p=2', folder
    assert command['env']['GOMEMLIMIT'] == '2GiB', folder
    for i, (a, b) in enumerate(zip(p['cpu_before'], p['cpu_after'], strict=True)):
        assert a['PID'] == b['PID'] and a['Start'] == b['Start'], folder
        seconds = (b['Ticks'] - a['Ticks']) / p['clock_hz']
        assert seconds >= 0, folder
        assert math.isclose(p['cpu_us_op'][i], seconds * 1e6 / n, rel_tol=1e-12), folder
        assert math.isclose(p['occupied_cores'][i], seconds / p['elapsed_s'], rel_tol=1e-12), folder
    assert len(p['cpu_before']) == 4, folder
    for phase in ('before', 'after'):
        fence = load(folder / f'{phase}-fence.json')
        assert fence['expected'] == p['revision_' + phase], folder
        assert len(fence['headers']) == 3, folder
        assert all(h['revision'] == fence['expected'] and h['raft_term'] == term
                   for h in fence['headers']), folder
    layout, placement = load(folder / 'layout.json'), load(folder / 'placement.json')
    roles = [[0, 1], [2, 3], [4, 5]] if layout['host'] else [[0], [1], [2]]
    assert layout['server_cpus_in_role_order'] == roles, folder
    assert layout['generator_cpu'] == (6 if layout['host'] else 3), folder
    assert layout['generator_gomaxprocs'] == (2 if layout['host'] else 1), folder
    assert len(layout['server_go_processors']) == 3, folder
    assert all(r.get('gomaxprocs', r.get('gomaxprocs_env')) == 2
               for r in layout['server_go_processors']), folder
    for phase in ('before', 'at-window', 'after'):
        for node, threads in load(folder / f'affinities-{phase}.json').items():
            role = placement['node_order'].index(int(node))
            assert threads and all(cpus == roles[role] for cpus in threads.values()), folder
    for phase in ('before', 'after'):
        threads = load(folder / f'generator-affinities-{phase}.json')
        assert threads and all(cpus == [layout['generator_cpu']] for cpus in threads.values()), folder
    statuses = placement['status_before'] + load(folder / 'status-after.json')
    assert len({s['leader'] for s in statuses}) == 1, folder
    assert all(s['raftTerm'] == term for s in statuses), folder
    return layout['host']


def root(path):
    plan = load(path / 'schedule.json')
    expected = {f'{rep}-{label}' for rep, cohort in plan for _, label, _ in cohort}
    windows = sorted(path.glob('*/put.json'))
    assert {p.parent.name for p in windows} == expected, path
    commands = [c for c in load(path / 'commands.json') if '-barrier-dir' in c.get('argv', [])]
    by_name = {Path(c['argv'][c['argv'].index('-out') + 1]).parent.name: c for c in commands}
    assert len(commands) == len(expected) and set(by_name) == expected, path
    hosts = [window(p.parent, by_name[p.parent.name]) for p in windows]
    assert len(set(hosts)) == 1, path
    repetitions = {1} if hosts[0] else set(range(1, 6))
    assert len(plan) == len(repetitions) and {rep for rep, _ in plan} == repetitions, path
    profiles = check(path) if (path / 'profile-manifest.json').exists() else []
    return {'windows': len(windows), 'host_windows': sum(hosts), 'profiles': len(profiles)}


def evidence(path):
    manifest = path / 'sha256.json'
    if manifest.exists():
        for name, expected in load(manifest).items():
            file = (path / name).resolve()
            assert path.resolve() in file.parents, name
            with file.open('rb') as stream:
                assert hashlib.file_digest(stream, 'sha256').hexdigest() == expected, name
    records = {}
    for archive in sorted(path.glob('*/run-*.tar.gz')):
        if '-primary-' not in archive.name and '-profile-' not in archive.name:
            continue
        with tempfile.TemporaryDirectory() as tmp, tarfile.open(archive) as source:
            members = [m for m in source.getmembers() if m.isfile()
                       and (m.name.endswith('.json') or m.name.endswith('-cpu.pb.gz'))]
            source.extractall(tmp, members=members, filter='data')
            records[str(archive.relative_to(path))] = root(Path(tmp))
    for host in sorted(path.glob('host*/c*')):
        if (host / 'schedule.json').exists():
            records[str(host.relative_to(path))] = root(host)
    for metadata in path.glob('*/run.json'):
        assert load(metadata)['conclusion'] == 'success', metadata
    for metadata in path.glob('*/jobs.json'):
        assert all(j['conclusion'] == 'success' for j in load(metadata)['jobs']), metadata
    totals = {key: sum(r[key] for r in records.values()) for key in ('windows', 'host_windows', 'profiles')}
    return {'records': records, 'totals': totals}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('evidence', type=Path)
    args = parser.parse_args()
    print(json.dumps(evidence(args.evidence), indent=2))
