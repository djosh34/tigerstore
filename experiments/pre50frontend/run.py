#!/usr/bin/env python3
"""Throwaway runner. Reuses bench-68 fences and the pre50 direct pb.KVClient."""
import base64
import concurrent.futures
import datetime
import json
import os
from pathlib import Path
import re
import shutil
import subprocess as sp
import time
import urllib.request

ROOT = Path.cwd()
WORKERS = int(os.environ.get('CLIENTS', '16'))
MODE = os.environ.get('MODE', 'primary')
SELECTED = os.environ.get('SELECTED_VARIANT', '')
OUT = ROOT / f'experiments/pre50frontend/results/{MODE}/c{WORKERS}'
TMP = Path(os.environ.get('RUNNER_TEMP', '.')) / 'pre50frontend'
CERT, BIN = TMP / 'certs', TMP / 'bin'
DEADLINE = datetime.datetime.fromisoformat('2026-10-13T22:00:00+00:00')
COMMANDS = []


def check_time():
    if datetime.datetime.now(datetime.timezone.utc) >= DEADLINE:
        raise RuntimeError('deadline passed; start nothing')


def run(args, **kwargs):
    check_time()
    COMMANDS.append([str(x) for x in args])
    try:
        return sp.run(args, check=True, text=True, timeout=kwargs.pop('timeout', 20), **kwargs)
    except sp.CalledProcessError as err:
        print(f'command failed: {args}\nstdout: {err.stdout}\nstderr: {err.stderr}', flush=True)
        raise


def spawn(args, **kwargs):
    check_time()
    COMMANDS.append([str(x) for x in args])
    return sp.Popen(args, **kwargs)


def http(port, path, timeout=15):
    check_time()
    return urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=timeout).read()


def save(path, value):
    path.write_text(json.dumps(value, indent=2, default=list) + '\n')


def ctl(index, *args):
    p = run([BIN/'etcdctl', f'--endpoints=https://127.0.0.1:{2379+index*100}',
             f'--cacert={CERT}/ca.pem', f'--cert={CERT}/client.pem',
             f'--key={CERT}/client-key.pem', '--command-timeout=5s', '-w=json', *args], capture_output=True)
    return json.loads(p.stdout)


def orders(stores, repetitions=5):
    for rep in range(repetitions):
        yield rep + 1, stores if rep % 2 == 0 else list(reversed(stores))


def metrics(text):
    return {k: float(v) for k, v in re.findall(r'^([a-zA-Z_:][a-zA-Z_0-9:]*) ([^\n ]+)$', text, re.M)}


def proc_cpu(pid):
    text = Path(f'/proc/{pid}/stat').read_text()
    fields = text[text.rfind(')')+1:].split()
    return {'pid': pid, 'ticks': int(fields[11])+int(fields[12]), 'start': int(fields[19])}


def affinities(procs, order):
    result = {}
    for core, i in enumerate(order):
        pid = procs[i].pid
        # Two walks also catch threads created during the first verification.
        for _ in range(2):
            tasks = list(Path(f'/proc/{pid}/task').iterdir())
            states = {p.name: list(os.sched_getaffinity(int(p.name))) for p in tasks}
            if any(a != [core] for a in states.values()):
                raise RuntimeError(f'bad all-thread affinity: node {i}, CPU {core}, {states}')
            result[str(i)] = states
    if os.sched_getaffinity(0) != {3}:
        raise RuntimeError('controller must run on CPU3')
    return result


def generator_affinity(proc):
    states = {p.name: list(os.sched_getaffinity(int(p.name)))
              for p in Path(f'/proc/{proc.pid}/task').iterdir()}
    assert states and all(a == [3] for a in states.values()), states
    env = Path(f'/proc/{proc.pid}/environ').read_bytes().split(b'\0')
    assert b'GOMAXPROCS=1' in env
    return states


def wait_file(path, proc, seconds=25):
    end = time.monotonic()+seconds
    while not path.exists():
        check_time()
        if proc.poll() is not None:
            raise RuntimeError(f'generator exited {proc.returncode} before {path.name}')
        if time.monotonic() >= end:
            raise RuntimeError(f'generator barrier {path.name} timeout')
        time.sleep(.005)


def watch_matches(text, revision):
    for line in text.splitlines():
        try:
            response = json.loads(line)
        except json.JSONDecodeError:
            continue  # The writer may not have finished this line yet.
        for event in response.get('Events', response.get('events', [])) or []:
            kv = event.get('kv', {})
            if (event.get('type', 0) in (0, 'PUT') and
                    base64.b64decode(kv.get('key', '')) == b'/pre50-smoke' and
                    base64.b64decode(kv.get('value', '')) == b'second' and
                    int(kv.get('mod_revision', 0)) == revision):
                return True
    return False


def smoke(leader, dest):
    endpoint = f'https://127.0.0.1:{2379+leader*100}'
    # Record the missing Alarm API rather than change the measured variants.
    # etcdctl health also calls AlarmList, which pinned main does not implement.
    try:
        health = ctl(leader, 'endpoint', 'health')
        assert len(health) == 1 and health[0]['health'] is True
        save(dest/'smoke-health.json', {'exit_code': 0, 'result': health, 'compatible': True})
    except sp.CalledProcessError as err:
        save(dest/'smoke-health.json', {'exit_code': err.returncode, 'stdout': err.stdout,
                                      'stderr': err.stderr, 'compatible': False})
        assert 'Unable to fetch the alarm list' in err.stdout, (err.stdout, err.stderr)
        try:
            ctl(leader, 'alarm', 'list')
        except sp.CalledProcessError as alarm:
            save(dest/'smoke-alarm.json', {'exit_code': alarm.returncode, 'stdout': alarm.stdout,
                                         'stderr': alarm.stderr})
            assert 'Unimplemented' in alarm.stderr and 'Alarm' in alarm.stderr, alarm.stderr
        else:
            raise RuntimeError('health failed despite successful AlarmList')
    # Watch from a known revision avoids a watch-start race without sleeps.
    put = ctl(leader, 'put', '/pre50-smoke', 'first')
    revision = put['header']['revision']+1
    args = [BIN/'etcdctl', f'--endpoints={endpoint}', f'--cacert={CERT}/ca.pem',
            f'--cert={CERT}/client.pem', f'--key={CERT}/client-key.pem', '-w=json',
            'watch', '/pre50-smoke', f'--rev={revision}']
    with (dest/'smoke-watch.jsonl').open('w') as f:
        watch = spawn(args, stdout=f, stderr=sp.PIPE, text=True)
        try:
            put = ctl(leader, 'put', '/pre50-smoke', 'second')
            save(dest/'smoke-put.json', put)
            assert put['header']['revision'] == revision
            get = ctl(leader, 'get', '/pre50-smoke')
            save(dest/'smoke-get.json', get)
            assert len(get['kvs']) == 1 and base64.b64decode(get['kvs'][0]['key']) == b'/pre50-smoke'
            assert base64.b64decode(get['kvs'][0]['value']) == b'second'
            assert get['kvs'][0]['mod_revision'] == revision
            end = time.monotonic()+10
            while not watch_matches((dest/'smoke-watch.jsonl').read_text(), revision):
                check_time()
                if time.monotonic() >= end or watch.poll() is not None:
                    raise RuntimeError('TLS etcdctl watch did not deliver Put')
                time.sleep(.05)
        finally:
            watch.terminate()
            try:
                watch.communicate(timeout=2)
            except sp.TimeoutExpired:
                watch.kill()
                watch.communicate()
    for flag, expected in [('--http1.1', '1.1'), ('--http2', '2')]:
        for path in ('/version', '/health'):
            response = run(['curl', flag, '--verbose', '--fail', '--silent', '--show-error', '--max-time', '10',
                            '--cacert', CERT/'ca.pem', '--cert', CERT/'client.pem', '--key', CERT/'client-key.pem',
                            '--write-out', '\\n%{http_version}', endpoint+path], capture_output=True)
            body, version = response.stdout.rsplit('\n', 1)
            assert version.strip() == expected, f'{flag} {path} actually used HTTP/{version}'
            value = json.loads(body)
            if path == '/version':
                assert value['etcdserver'] == '3.7.2'
            else:
                assert value['health'] == 'true' and value['reason'] == ''
            save(dest/f'smoke-http{expected}-{path[1:]}.json', {'http_version': version.strip(), 'response': value})


def cluster(store, label, profile=False, smoke_only=False):
    check_time()
    dest, data = OUT / label, TMP / label
    dest.mkdir(parents=True)
    data.mkdir()
    procs, handles = [], []
    gen = None
    initial = ','.join(f'n{i}=https://127.0.0.1:{2380+100*i}' for i in range(3))
    try:
        for i in range(3):
            cert, key = CERT/f'node{i+1}.pem', CERT/f'node{i+1}-key.pem'
            args = ['taskset', '-c', str(i), BIN/store, '--name', f'n{i}', '--data-dir', data/f'n{i}',
                    '--initial-cluster', initial, '--initial-cluster-token', label,
                    '--listen-peer-urls', f'https://127.0.0.1:{2380+100*i}',
                    '--listen-client-urls', f'https://127.0.0.1:{2379+100*i}',
                    '--advertise-client-urls', f'https://127.0.0.1:{2379+100*i}',
                    '--cert-file', cert, '--key-file', key, '--trusted-ca-file', CERT/'ca.pem',
                    '--client-cert-auth', '--peer-cert-file', cert, '--peer-key-file', key,
                    '--peer-trusted-ca-file', CERT/'ca.pem']
            if store == 'etcd':
                args += ['--peer-client-cert-auth', '--initial-advertise-peer-urls',
                         f'https://127.0.0.1:{2380+100*i}', '--quota-backend-bytes', '8589934592',
                         '--tls-min-version', 'TLS1.3', '--tls-max-version', 'TLS1.3',
                         '--listen-metrics-urls', f'http://127.0.0.1:{6060+i}',
                         '--logger', 'zap', '--log-level', 'warn']
            env = os.environ | {'GOMAXPROCS': '2', 'PRE50_DEBUG': f'127.0.0.1:{6060+i}'}
            if MODE == 'smoke':
                env['GODEBUG'] = 'http2debug=2'
            log = (dest/f'n{i}.log').open('w')
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
        # Reapply after a pause, then reject any old/new thread with a wrong mask.
        # New threads inherit their creator's mask; an uncorrected creator fails verification.
        for _ in range(3):
            for core, i in enumerate(order):
                run(['taskset', '-apc', str(core), str(procs[i].pid)], stdout=sp.DEVNULL)
            time.sleep(.05)
        save(dest/'affinities-before.json', affinities(procs, order))
        if store != 'etcd':
            runtime_layout = [json.loads(http(6060+i, '/debug/leader')) for i in order]
            assert all(s['gomaxprocs'] == 2 and s['leader'] == leader+1 for s in runtime_layout)
        else:
            # Stock etcd does not expose a GOMAXPROCS gauge. Record the actual process environment.
            runtime_layout = []
            for i in order:
                env = Path(f'/proc/{procs[i].pid}/environ').read_bytes().split(b'\0')
                assert b'GOMAXPROCS=2' in env
                runtime_layout.append({'gomaxprocs_env': 2})
        save(dest/'layout.json', {'server_go_processors': runtime_layout, 'server_cpus_in_role_order': [0, 1, 2],
                                 'generator_cpu': 3, 'generator_gomaxprocs': 1, 'conns': 16,
                                 'etcd_client_port': 'stock shared TLS, ServeHTTP, no separate HTTP port'})
        pids = ','.join(str(procs[i].pid) for i in order)
        save(dest/'placement.json', {'leader_index': leader, 'node_order': order, 'pids': pids, 'status_before': status})
        if smoke_only:
            smoke(leader, dest)
            return
        endpoint = f'127.0.0.1:{2379+leader*100}'
        hz = int(run(['getconf', 'CLK_TCK'], capture_output=True).stdout.strip())

        def load_args(name, total, workers=WORKERS):
            return ['taskset', '-c', '3', BIN/'load', '-endpoint', endpoint, '-certdir', CERT,
                    '-pids', pids, '-hz', str(hz), '-op', 'put', '-total', str(total),
                    '-workers', str(workers), '-conns', '16', '-out', dest/f'{name}.json']

        def applied_fence(revision, phase):
            until = time.monotonic()+15
            while True:
                headers = [ctl(i, 'get', '/registry/pods/ns-000/pod-00000000', '--consistency=s')['header'] for i in order]
                observed = [h['revision'] for h in headers]
                if all(r == revision for r in observed):
                    save(dest/f'{phase}-fence.json', {'expected': revision, 'headers': headers})
                    return [h['raft_term'] for h in headers]
                if time.monotonic() >= until:
                    raise RuntimeError(f'catch-up failed: {revision}, {observed}')
                time.sleep(.02)

        def snapshot(phase):
            result = []
            for i in order:
                if store != 'etcd':
                    result.append(json.loads(http(6060+i, '/debug/snapshot')))
                else:
                    # Endpoint status can precede etcd WAL.Save. Keep bench-68's post-Save fence.
                    target = ctl(i, 'endpoint', 'status')[0]['Status']['raftIndex']
                    until = time.monotonic()+15
                    while True:
                        text = http(6060+i, '/metrics').decode()
                        committed = metrics(text)['etcd_server_proposals_committed_total']
                        if committed >= target:
                            break
                        if time.monotonic() >= until:
                            raise RuntimeError(f'WAL.Save fence failed: {target}, {committed}')
                        time.sleep(.02)
                    save(dest/f'n{i}-{phase}-wal-fence.json', {'target_raft_index': target, 'post_save_commit_gauge': committed})
                    text = http(6060+i, '/metrics').decode()
                    (dest/f'n{i}-{phase}.prom').write_text(text)
                    m = metrics(text)
                    disk = {k: v for k, v in m.items() if k.startswith('etcd_disk_')}
                    assert 'etcd_disk_wal_fsync_duration_seconds_count' in disk
                    result.append({'mallocs': m['go_memstats_mallocs_total'],
                                   'total_alloc': m['go_memstats_alloc_bytes_total'], 'disk': disk})
            return result

        run(load_args('seed', 4096, 64), env=os.environ | {'GOMAXPROCS': '1'}, timeout=60)
        run(load_args('warmup', 20000), env=os.environ | {'GOMAXPROCS': '1'}, timeout=60)
        time.sleep(1)
        warmup = json.loads((dest/'warmup.json').read_text())
        barrier = data/'barrier'
        barrier.mkdir()
        args = load_args('put', 0 if MODE == 'profile' else 60000)
        args += ['-barrier-dir', barrier]
        if MODE == 'profile':
            args += ['-duration', '12s']
        gen = spawn(args, env=os.environ | {'GOMAXPROCS': '1'}, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        wait_file(barrier/'ready', gen)
        save(dest/'generator-affinities-before.json', generator_affinity(gen))
        terms_before = applied_fence(warmup['revision_after'], 'before')
        before = snapshot('before')
        cpu_before = [proc_cpu(procs[i].pid) for i in order]
        save(dest/'affinities-at-window.json', affinities(procs, order))
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1) if profile else None
        capture = {}
        def cpu_profile():
            capture['request_start_monotonic'] = time.monotonic()
            capture['request_start_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            b = http(6060+leader, '/debug/pprof/profile?seconds=15', timeout=25)
            capture['response_end_monotonic'] = time.monotonic()
            (dest/f'n{leader}-cpu.pb.gz').write_bytes(b)
        try:
            future = pool.submit(cpu_profile) if profile else None
            if profile:
                time.sleep(.5)
            check_time()
            capture['barrier_start_monotonic'] = time.monotonic()
            (barrier/'start').touch()
            wait_file(barrier/'drained', gen, seconds=90)
            capture['drained_monotonic'] = time.monotonic()
            save(dest/'generator-affinities-after.json', generator_affinity(gen))
            revision = ctl(leader, 'get', '/registry/pods/ns-000/pod-00000000', '--consistency=s')['header']['revision']
            terms_after = applied_fence(revision, 'after')
            after = snapshot('after')
            cpu_after = [proc_cpu(procs[i].pid) for i in order]
            (barrier/'release').touch()
            stdout, stderr = gen.communicate(timeout=15)
            if gen.returncode:
                raise RuntimeError(f'generator failed: {stdout} {stderr}')
            if future:
                future.result(timeout=25)
        finally:
            if pool:
                pool.shutdown(wait=True)
        result = json.loads((dest/'put.json').read_text())
        assert result['valid'] and not result['errors'] and len(result['terms']) == 1
        assert revision == result['revision_after']
        assert set(terms_before+terms_after) == {int(t) for t in result['terms']}
        assert len(result['cpu_us_op']) == 4
        save(dest/'snapshots.json', {'before': before, 'after': after, 'cpu_before': cpu_before, 'cpu_after': cpu_after,
                                    'hz': hz, 'scope': 'workers parked; all replicas applied; etcd also post-WAL.Save'})
        after_status = [ctl(i, 'endpoint', 'status')[0]['Status'] for i in range(3)]
        assert all(s['leader'] == status[0]['leader'] and s['raftTerm'] == status[0]['raftTerm'] and
                   not s.get('errors') for s in after_status)
        assert set(terms_before+terms_after) == {status[0]['raftTerm']}
        assert all(p.poll() is None for p in procs)
        save(dest/'status-after.json', after_status)
        save(dest/'affinities-after.json', affinities(procs, order))
        if profile:
            start = capture['request_start_monotonic']
            end = capture['response_end_monotonic']
            capture.update({'operations': result['successes'], 'generator_elapsed_s': result['elapsed_s'],
                            'estimated_idle_lead_s': capture['barrier_start_monotonic']-start,
                            'estimated_idle_tail_s': end-capture['drained_monotonic'],
                            'caveat': 'HTTP request bounds, not exact sampler start/stop. Tail includes replica fences, observation RPCs and the final validation Range probe.'})
            assert capture['drained_monotonic'] < end, 'load drain exceeded capture; reject attribution'
            save(dest/'profile-window.json', capture)
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
        save(OUT/'commands.json', COMMANDS)


def main():
    check_time()
    assert os.environ.get('GITHUB_ACTIONS') == 'true', 'no local measurements'
    assert WORKERS in (16, 64, 256) and MODE in ('primary', 'profile', 'smoke')
    OUT.mkdir(parents=True, exist_ok=True)
    if MODE == 'smoke':
        cluster('a', 'smoke-a', smoke_only=True)
        return
    if MODE == 'primary':
        cluster('a', 'smoke-a', smoke_only=True)
    stores = ['a', 'etcd']
    assert MODE != 'profile' or SELECTED == 'a', 'architect narrowed profiling to main'
    schedule = list(orders(stores))
    save(OUT/'schedule.json', schedule)
    for rep, cohort in schedule:
        for store in cohort:
            print(f'{MODE}, clients={WORKERS}, rep={rep}, {store}', flush=True)
            cluster(store, f'{rep}-{store}', profile=MODE == 'profile' and store != 'etcd')


if __name__ == '__main__':
    main()
