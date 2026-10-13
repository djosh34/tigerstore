#!/usr/bin/env python3
"""Throwaway paired Put measurements. No builds or remounts in HOST mode."""
import concurrent.futures
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess as sp
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
HOST = os.environ.get('HOST') == '1'
WORKERS = int(os.environ.get('CLIENTS', '64'))
MODE = os.environ.get('MODE', 'primary')
OUT = Path(os.environ.get('OUT', ROOT / f'evidence/pre50next/{"host" if HOST else MODE}/c{WORKERS}')).resolve()
TMP = Path(os.environ.get('WORK_DIR', OUT / 'private')).resolve()
BIN = Path(os.environ.get('BIN_DIR', TMP / 'bin')).resolve()
CERT = Path(os.environ.get('CERT_DIR', TMP / 'certs')).resolve()
DEADLINE = datetime.datetime.fromisoformat('2026-10-13T22:00:00+00:00')
COMMANDS = []
ROLE_CPUS = [[0, 1], [2, 3], [4, 5]] if HOST else [[0], [1], [2]]
GEN_CPU = 6 if HOST else 3
GEN_PROCS = 2 if HOST else 1
ENV = {'GOFLAGS': '-p=2', 'GOMEMLIMIT': '2GiB'}


def check_time(reserve=0):
    if datetime.datetime.now(datetime.timezone.utc).timestamp() + reserve >= DEADLINE.timestamp():
        raise RuntimeError('deadline reached or insufficient time; start nothing')


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def record(args, kwargs):
    env = kwargs.get('env', os.environ)
    COMMANDS.append({'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                     'argv': [str(x) for x in args],
                     'env': {k: env[k] for k in ('GOMAXPROCS', 'GOFLAGS', 'GOMEMLIMIT', 'PRE50_DEBUG') if k in env}})
    save(OUT / 'commands.json', COMMANDS)


def run(args, **kwargs):
    check_time()
    record(args, kwargs)
    return sp.run(args, check=True, text=True, timeout=kwargs.pop('timeout', 20), **kwargs)


def spawn(args, **kwargs):
    check_time()
    record(args, kwargs)
    return sp.Popen(args, **kwargs)


def http(port, path, timeout=15):
    check_time()
    COMMANDS.append({'utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                     'http_get': f'http://127.0.0.1:{port}{path}', 'timeout': timeout})
    with urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=timeout) as response:
        return response.read()


def ctl(index, *args):
    p = run([BIN / 'etcdctl', f'--endpoints=https://127.0.0.1:{2379+index*100}',
             f'--cacert={CERT}/ca.pem', f'--cert={CERT}/client.pem',
             f'--key={CERT}/client-key.pem', '--command-timeout=5s', '-w=json', *args], capture_output=True)
    return json.loads(p.stdout)


def metrics(text):
    return {k: float(v) for k, v in re.findall(r'^([a-zA-Z_:][a-zA-Z_0-9:]*) ([^\n ]+)$', text, re.M)}


def proc_snapshot(pid):
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    io = {k: int(v) for k, v in (line.split(':') for line in Path(f'/proc/{pid}/io').read_text().splitlines())}
    status = Path(f'/proc/{pid}/status').read_text()
    return {'pid': pid, 'ticks': int(fields[11])+int(fields[12]), 'start': int(fields[19]),
            'io': io, 'rss_bytes': int(re.search(r'^VmRSS:\s+(\d+) kB$', status, re.M)[1])*1024}


def affinities(procs, order):
    result = {}
    for role, i in enumerate(order):
        for _ in range(2):
            states = {p.name: sorted(os.sched_getaffinity(int(p.name)))
                      for p in Path(f'/proc/{procs[i].pid}/task').iterdir()}
            if not states or any(a != ROLE_CPUS[role] for a in states.values()):
                raise RuntimeError(f'bad all-thread affinity: node {i}, {states}')
            result[str(i)] = states
    if os.sched_getaffinity(0) != {GEN_CPU}:
        raise RuntimeError('controller must share generator CPU')
    return result


def generator_affinity(proc):
    states = {p.name: sorted(os.sched_getaffinity(int(p.name)))
              for p in Path(f'/proc/{proc.pid}/task').iterdir()}
    assert states and all(a == [GEN_CPU] for a in states.values()), states
    env = Path(f'/proc/{proc.pid}/environ').read_bytes().split(b'\0')
    assert f'GOMAXPROCS={GEN_PROCS}'.encode() in env
    return states


def wait_file(path, proc, seconds=25):
    end = time.monotonic()+seconds
    while not path.exists():
        check_time()
        if proc.poll() is not None or time.monotonic() >= end:
            raise RuntimeError(f'generator barrier failed: {path.name}, exit={proc.poll()}')
        time.sleep(.005)


def schedule(candidates, mode, host):
    assert mode in ('primary', 'profile')
    assert not host or mode == 'primary', 'HOST permits baseline only, no profiles'
    for rep in range(1, 2 if host else 6):
        if mode == 'profile':
            cohort = [('baseline', 'baseline-control', False), ('etcd', 'etcd-control', False),
                      ('baseline', 'baseline-profile', True)]
        else:
            cohort = [('baseline', 'baseline', False), ('etcd', 'etcd', False)]
            if not host:
                cohort += [(c['name'], c['name'], False) for c in candidates]
        yield rep, cohort if rep % 2 else list(reversed(cohort))


def cluster(store, label, profile=False):
    check_time(reserve=90)
    dest, data = OUT / label, TMP / label
    dest.mkdir()
    data.mkdir()
    procs, handles = [], []
    gen = None
    initial = ','.join(f'n{i}=https://127.0.0.1:{2380+100*i}' for i in range(3))
    try:
        for i in range(3):
            cert, key = CERT / f'node{i+1}.pem', CERT / f'node{i+1}-key.pem'
            args = ['taskset', '-c', ','.join(map(str, ROLE_CPUS[i])), BIN / store,
                    '--name', f'n{i}', '--data-dir', data / f'n{i}',
                    '--initial-cluster', initial, '--initial-cluster-token', label,
                    '--listen-peer-urls', f'https://127.0.0.1:{2380+100*i}',
                    '--listen-client-urls', f'https://127.0.0.1:{2379+100*i}',
                    '--advertise-client-urls', f'https://127.0.0.1:{2379+100*i}',
                    '--cert-file', cert, '--key-file', key, '--trusted-ca-file', CERT / 'ca.pem',
                    '--client-cert-auth', '--peer-cert-file', cert, '--peer-key-file', key,
                    '--peer-trusted-ca-file', CERT / 'ca.pem']
            if store == 'etcd':
                args += ['--peer-client-cert-auth', '--initial-advertise-peer-urls',
                         f'https://127.0.0.1:{2380+100*i}', '--quota-backend-bytes', '8589934592',
                         '--tls-min-version', 'TLS1.3', '--tls-max-version', 'TLS1.3',
                         '--listen-metrics-urls', f'http://127.0.0.1:{6060+i}',
                         '--logger', 'zap', '--log-level', 'warn']
            env = {k: v for k, v in os.environ.items() if not k.startswith('TIGERSTORE_')}
            env |= ENV | {'GOMAXPROCS': '2', 'PRE50_DEBUG': f'127.0.0.1:{6060+i}'}
            log = (dest / f'n{i}.log').open('w')
            handles.append(log)
            procs.append(spawn(args, stdout=log, stderr=log, env=env))
        until = time.monotonic()+30
        while True:
            if any(p.poll() is not None for p in procs):
                raise RuntimeError('server exited during startup')
            try:
                status = [ctl(i, 'endpoint', 'status')[0]['Status'] for i in range(3)]
                leaders = {s['leader'] for s in status}
                if len(leaders) != 1 or 0 in leaders or any(s.get('errors') for s in status):
                    raise RuntimeError('no healthy common leader')
                leader = next(i for i, s in enumerate(status) if s['header']['member_id'] == s['leader'])
                break
            except Exception:
                check_time()
                if time.monotonic() >= until:
                    raise
                time.sleep(.25)
        order = [leader]+[i for i in range(3) if i != leader]
        for _ in range(3):
            for role, i in enumerate(order):
                run(['taskset', '-apc', ','.join(map(str, ROLE_CPUS[role])), str(procs[i].pid)], stdout=sp.DEVNULL)
            time.sleep(.05)
        save(dest / 'affinities-before.json', affinities(procs, order))
        runtime_layout = []
        for i in order:
            env = Path(f'/proc/{procs[i].pid}/environ').read_bytes().split(b'\0')
            assert b'GOMAXPROCS=2' in env and b'GOMEMLIMIT=2GiB' in env and b'GOFLAGS=-p=2' in env
            if store != 'etcd':
                meta = json.loads(http(6060+i, '/debug/leader'))
                assert meta['gomaxprocs'] == 2 and meta['leader'] == leader+1
                runtime_layout.append(meta)
            else:
                runtime_layout.append({'gomaxprocs_env': 2})
        save(dest / 'layout.json', {'host': HOST, 'server_go_processors': runtime_layout,
                                   'server_cpus_in_role_order': ROLE_CPUS, 'generator_cpu': GEN_CPU,
                                   'generator_gomaxprocs': GEN_PROCS, 'conns': 16})
        pids = ','.join(str(procs[i].pid) for i in order)
        save(dest / 'placement.json', {'leader_index': leader, 'node_order': order, 'pids': pids,
                                      'roles': dict(zip(('leader', 'follower1', 'follower2'), order)), 'status_before': status})
        endpoint = f'127.0.0.1:{2379+leader*100}'
        hz = int(run(['getconf', 'CLK_TCK'], capture_output=True).stdout.strip())
        # Check real client-port HTTP separately. No health requests in measured Put load.
        smoke = []
        for protocol in ('1.1', '2'):
            for path in ('version', 'health'):
                response = run(['curl', '--http'+protocol, '--fail', '--silent', '--show-error', '--max-time', '10',
                                '--cacert', CERT / 'ca.pem', '--cert', CERT / 'client.pem', '--key', CERT / 'client-key.pem',
                                '--write-out', '\n%{http_version}', f'https://{endpoint}/{path}'], capture_output=True)
                body, version = response.stdout.rsplit('\n', 1)
                value = json.loads(body)
                assert version == protocol
                assert value.get('health') == 'true' if path == 'health' else value.get('etcdserver') == '3.7.2'
                smoke.append({'protocol': version, 'path': path, 'body': value})
        save(dest / 'http-smoke.json', smoke)

        def load_args(name, total, workers=WORKERS):
            return ['taskset', '-c', str(GEN_CPU), BIN / 'load', '-endpoint', endpoint, '-certdir', CERT,
                    '-pids', pids, '-hz', str(hz), '-op', 'put', '-total', str(total),
                    '-workers', str(workers), '-conns', '16', '-out', dest / f'{name}.json']

        def applied_fence(revision, phase):
            until = time.monotonic()+15
            while True:
                headers = [ctl(i, 'get', '/registry/pods/ns-000/pod-00000000', '--consistency=s')['header'] for i in order]
                observed = [h['revision'] for h in headers]
                if all(r == revision for r in observed):
                    save(dest / f'{phase}-fence.json', {'expected': revision, 'headers': headers})
                    return [h['raft_term'] for h in headers]
                if time.monotonic() >= until:
                    raise RuntimeError(f'catch-up failed: {revision}, {observed}')
                time.sleep(.02)

        def snapshot(phase, gc=False):
            result = []
            for i in order:
                if store != 'etcd':
                    result.append(json.loads(http(6060+i, '/debug/snapshot'+('?gc=1' if gc else ''))))
                else:
                    # bench68 post-WAL.Save fence; endpoint applied status alone is insufficient.
                    target = ctl(i, 'endpoint', 'status')[0]['Status']['raftIndex']
                    until = time.monotonic()+15
                    while True:
                        m = metrics(http(6060+i, '/metrics').decode())
                        committed = m['etcd_server_proposals_committed_total']
                        if committed >= target:
                            break
                        if time.monotonic() >= until:
                            raise RuntimeError(f'WAL.Save fence failed: {target}, {committed}')
                        time.sleep(.02)
                    save(dest / f'n{i}-{phase}-wal-fence.json', {'target_raft_index': target, 'post_save_commit_gauge': committed})
                    text = http(6060+i, '/metrics').decode()
                    (dest / f'n{i}-{phase}.prom').write_text(text)
                    m = metrics(text)
                    disk = {k: v for k, v in m.items() if k.startswith('etcd_disk_')}
                    assert 'etcd_disk_wal_fsync_duration_seconds_count' in disk
                    result.append({'mallocs': m['go_memstats_mallocs_total'], 'total_alloc': m['go_memstats_alloc_bytes_total'],
                                   'heap_alloc': m['go_memstats_heap_alloc_bytes'], 'heap_inuse': m['go_memstats_heap_inuse_bytes'],
                                   'heap_objects': m['go_memstats_heap_objects'], 'disk': disk})
            return result

        gen_env = os.environ | ENV | {'GOMAXPROCS': str(GEN_PROCS)}
        run(load_args('seed', 4096, 64), env=gen_env, timeout=60)
        run(load_args('warmup', 8000), env=gen_env, timeout=60)
        time.sleep(1)
        warmup = json.loads((dest / 'warmup.json').read_text())
        barrier = data / 'barrier'
        barrier.mkdir()
        args = load_args('put', 0 if MODE == 'profile' else 30000)+['-barrier-dir', barrier]
        if MODE == 'profile':
            args += ['-duration', '12s']
        gen = spawn(args, env=gen_env, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        wait_file(barrier / 'ready', gen)
        save(dest / 'generator-affinities-before.json', generator_affinity(gen))
        terms_before = applied_fence(warmup['revision_after'], 'before')
        before = snapshot('before')
        proc_before = [proc_snapshot(procs[i].pid) for i in order]
        save(dest / 'affinities-at-window.json', affinities(procs, order))
        capture = {'nodes': {}, 'profile_seconds': 15, 'arrival_seconds': 12}

        def cpu_profile(i):
            bounds = {'request_start_monotonic': time.monotonic(),
                      'request_start_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()}
            capture['nodes'][str(i)] = bounds
            body = http(6060+i, '/debug/pprof/profile?seconds=15', timeout=25)
            bounds['response_end_monotonic'] = time.monotonic()
            if body[:2] != b'\x1f\x8b':
                raise RuntimeError('pprof did not return a gzip profile')
            (dest / f'n{i}-cpu.pb.gz').write_bytes(body)

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=3) if profile else None
        try:
            futures = [pool.submit(cpu_profile, i) for i in order] if pool else []
            if profile:
                until = time.monotonic()+2
                while len(capture['nodes']) != 3:
                    if time.monotonic() >= until:
                        raise RuntimeError('profile requests did not start together')
                    time.sleep(.005)
                time.sleep(.5)
            check_time()
            capture['barrier_start_monotonic'] = time.monotonic()
            (barrier / 'start').touch()
            wait_file(barrier / 'drained', gen, seconds=90)
            capture['drained_monotonic'] = time.monotonic()
            save(dest / 'generator-affinities-after.json', generator_affinity(gen))
            revision = ctl(leader, 'get', '/registry/pods/ns-000/pod-00000000', '--consistency=s')['header']['revision']
            terms_after = applied_fence(revision, 'after')
            after = snapshot('after')
            proc_after = [proc_snapshot(procs[i].pid) for i in order]
            check_time()
            (barrier / 'release').touch()
            stdout, stderr = gen.communicate(timeout=15)
            if gen.returncode:
                raise RuntimeError(f'generator failed: {stdout} {stderr}')
            for future in futures:
                future.result(timeout=25)
        finally:
            if pool:
                pool.shutdown(wait=True)
        result = json.loads((dest / 'put.json').read_text())
        assert result['valid'] and not result['errors'] and len(result['terms']) == 1
        assert result['successes'] == 30000 if MODE == 'primary' else result['successes'] > 0
        assert revision == result['revision_after']
        assert set(terms_before+terms_after) == {int(t) for t in result['terms']}
        assert len(result['cpu_us_op']) == 4
        for a, b in zip(proc_before, proc_after):
            assert a['start'] == b['start'] and a['pid'] == b['pid'] and b['ticks'] >= a['ticks']
        save(dest / 'snapshots.json', {'before': before, 'after': after, 'proc_before': proc_before, 'proc_after': proc_after,
                                     'hz': hz, 'scope': 'workers parked; all replicas applied; etcd also post-WAL.Save'})
        after_status = [ctl(i, 'endpoint', 'status')[0]['Status'] for i in range(3)]
        assert all(s['leader'] == status[0]['leader'] and s['raftTerm'] == status[0]['raftTerm'] and not s.get('errors') for s in after_status)
        assert set(terms_before+terms_after) == {status[0]['raftTerm']}
        assert all(p.poll() is None for p in procs)
        save(dest / 'status-after.json', after_status)
        save(dest / 'affinities-after.json', affinities(procs, order))
        if profile:
            capture.update({'operations': result['successes'], 'generator_elapsed_s': result['elapsed_s'],
                            'caveat': 'Divide sampled CPU by completed Puts, not by profile duration. HTTP bounds approximate sampler bounds. The 15s capture includes 0.5s idle lead, drain, fences, debug calls and idle tail around a 12s arrival window.'})
            for bounds in capture['nodes'].values():
                bounds['estimated_idle_lead_s'] = capture['barrier_start_monotonic']-bounds['request_start_monotonic']
                bounds['estimated_idle_tail_s'] = bounds['response_end_monotonic']-capture['drained_monotonic']
                assert bounds['estimated_idle_tail_s'] > 0, 'load drain exceeded capture'
            save(dest / 'profile-window.json', capture)
        # Retained heap is a separate observation, never inside measured CPU/profile windows.
        time.sleep(8)
        retained = snapshot('retained', gc=store != 'etcd')
        save(dest / 'retained.json', {'forced_gc': store != 'etcd', 'quiet_seconds': 8, 'nodes': retained,
                                    'proc': [proc_snapshot(procs[i].pid) for i in order]})
    finally:
        if gen and gen.poll() is None:
            gen.kill()
            gen.communicate()
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(timeout=3)
            except sp.TimeoutExpired:
                p.kill()
                p.wait()
        for f in handles:
            f.close()
        shutil.rmtree(data)
        save(OUT / 'commands.json', COMMANDS)


def verify_binaries(refs):
    manifest = json.loads((BIN / 'binaries.json').read_text())
    assert manifest['base'] == refs['base'], 'downloaded base does not match refs.json'
    required = ['baseline', 'etcd', 'etcdctl', 'load']
    if not HOST and MODE == 'primary':
        required += [c['name'] for c in refs['candidates']]
    for name in required:
        assert hashlib.sha256((BIN / name).read_bytes()).hexdigest() == manifest['sha256'][name], name
    save(OUT / 'binaries.json', manifest)


def main():
    check_time()
    assert WORKERS in (64, 256)
    assert HOST or os.environ.get('GITHUB_ACTIONS') == 'true', 'runner or explicit HOST=1 required'
    assert not HOST or MODE == 'primary', 'host profiles forbidden'
    OUT.mkdir(parents=True, exist_ok=True)
    refs = json.loads((ROOT / 'experiments/pre50next/refs.json').read_text())
    plan = list(schedule(refs['candidates'], MODE, HOST))
    # Never overwrite or combine independent runs.
    assert not any((OUT / f'{rep}-{label}').exists() for rep, cohort in plan for _, label, _ in cohort)
    verify_binaries(refs)
    available = os.sched_getaffinity(0)
    assert set(sum(ROLE_CPUS, [])+[GEN_CPU]) <= available, available
    os.sched_setaffinity(0, {GEN_CPU})
    TMP.mkdir(parents=True, exist_ok=True)
    if not CERT.exists():
        run(['bash', ROOT / 'scripts/dev-certs.sh', CERT], timeout=30)
    run(['findmnt', '-T', TMP, '-o', 'TARGET,SOURCE,FSTYPE,OPTIONS'], stdout=(OUT / 'mount.txt').open('w'))
    save(OUT / 'schedule.json', plan)
    save(OUT / 'sources.json', refs)
    for rep, cohort in plan:
        for store, label, profile in cohort:
            print(f'{"host" if HOST else MODE}, clients={WORKERS}, rep={rep}, {label}', flush=True)
            cluster(store, f'{rep}-{label}', profile)


if __name__ == '__main__':
    main()
