#!/usr/bin/env python3
"""Paired leader-only Puts. Adapted from exp/bench-68's run.py."""
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
WORKERS = int(os.environ['CLIENTS'])
WORKLOAD = os.environ.get('WORKLOAD', 'put')
assert WORKLOAD in ('put', 'cas'), WORKLOAD
OUT = Path(os.environ['BENCH_OUT']) / f'{WORKLOAD}-c{WORKERS}'
TMP = Path(os.environ['RUNNER_TEMP']) / 'bench75'
CERT = TMP / 'certs'
BIN = TMP / 'bin'
DEADLINE = datetime.datetime.fromisoformat('2026-10-13T22:00:00+00:00')
COMMANDS = []


def check_time():
    if datetime.datetime.now(datetime.timezone.utc) >= DEADLINE:
        raise RuntimeError('deadline passed')


def run(args, **kwargs):
    check_time()
    COMMANDS.append([str(x) for x in args])
    return sp.run(args, check=True, text=True, **kwargs)


def http(port, path):
    return urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=15).read()


def ctl(index, *args):
    p = run([BIN/'etcdctl', f'--endpoints=https://127.0.0.1:{2379+index*100}',
             f'--cacert={CERT}/ca.pem', f'--cert={CERT}/client.pem',
             f'--key={CERT}/client-key.pem', '-w=json', *args], capture_output=True)
    return json.loads(p.stdout)


def cluster(store, label, profile=False):
    check_time()
    dest = OUT / label
    dest.mkdir(parents=True)
    data = TMP / label
    data.mkdir()
    procs = []
    children = []
    handles = []
    initial = ','.join(f'n{i}=https://127.0.0.1:{2380+100*i}' for i in range(3))
    try:
        for i in range(3):
            cert = CERT / f'node{i+1}.pem'
            key = CERT / f'node{i+1}-key.pem'
            args = ['taskset', '-c', str(i), str(BIN/store), '--name', f'n{i}',
                    '--data-dir', str(data/f'n{i}'), '--initial-cluster', initial,
                    '--initial-cluster-token', label,
                    '--listen-peer-urls', f'https://127.0.0.1:{2380+100*i}',
                    '--listen-client-urls', f'https://127.0.0.1:{2379+100*i}',
                    '--advertise-client-urls', f'https://127.0.0.1:{2379+100*i}',
                    '--cert-file', str(cert), '--key-file', str(key),
                    '--trusted-ca-file', str(CERT/'ca.pem'), '--client-cert-auth',
                    '--peer-cert-file', str(cert), '--peer-key-file', str(key),
                    '--peer-trusted-ca-file', str(CERT/'ca.pem')]
            if store == 'etcd':
                args += ['--peer-client-cert-auth', '--initial-advertise-peer-urls',
                         f'https://127.0.0.1:{2380+100*i}', '--quota-backend-bytes',
                         '8589934592', '--tls-min-version', 'TLS1.3', '--tls-max-version', 'TLS1.3',
                         '--listen-metrics-urls', f'http://127.0.0.1:{6060+i}',
                         '--logger', 'zap', '--log-level', 'warn']
            env = os.environ | {'GOMAXPROCS': '1', 'PRE50_DEBUG': f'127.0.0.1:{6060+i}',
                                'BENCH75_PROFILE': '1' if profile else '0'}
            log = open(dest/f'n{i}.log', 'w')
            handles.append(log)
            check_time()
            COMMANDS.append(args)
            procs.append(sp.Popen(args, stdout=log, stderr=log, env=env))
        until = time.monotonic()+30
        while True:
            if any(p.poll() is not None for p in procs):
                raise RuntimeError('server exited during startup')
            try:
                if store != 'etcd':
                    status = [json.loads(http(6060+i, '/debug/leader')) for i in range(3)]
                    leaders = {s['leader'] for s in status}
                    if len(leaders) != 1 or 0 in leaders:
                        raise RuntimeError('no common leader')
                    leader = status[0]['leader']-1
                else:
                    status = [ctl(i, 'endpoint', 'status')[0]['Status'] for i in range(3)]
                    leaders = {s['leader'] for s in status}
                    if len(leaders) != 1 or 0 in leaders:
                        raise RuntimeError('no common leader')
                    leader = next(i for i,s in enumerate(status) if s['header']['member_id'] == s['leader'])
                break
            except Exception:
                if time.monotonic() >= until:
                    raise
                time.sleep(.25)
        order = [leader]+[i for i in range(3) if i != leader]
        for core,i in enumerate(order):
            run(['taskset','-apc',str(core),str(procs[i].pid)], stdout=sp.DEVNULL)
        pids = ','.join(str(procs[i].pid) for i in order)
        go_procs = [status[i]['gomaxprocs'] for i in order] if store != 'etcd' else [1, 1, 1]
        assert all(p >= 1 for p in go_procs), (store, go_procs)
        (dest/'layout.json').write_text(json.dumps({
            'server_gomaxprocs_env': 1,
            'server_go_processors': go_procs,
            'go_processors_source': 'debug runtime.GOMAXPROCS(0)' if store != 'etcd' else 'GOMAXPROCS environment',
            'server_cpus_in_role_order': [0, 1, 2],
            'generator_cpu': 3,
            'generator_gomaxprocs': 1,
        }, indent=2))
        (dest/'placement.json').write_text(json.dumps({'leader_index':leader,'node_order':order,'pids':pids,'status_before':status},indent=2))
        endpoint = f'127.0.0.1:{2379+leader*100}'

        def load_args(name, total, workers=WORKERS, op=WORKLOAD):
            return ['taskset', '-c', '3', str(BIN/'load'), '-endpoint', endpoint,
                    '-certdir', str(CERT), '-pids', pids, '-hz', sp.check_output(['getconf','CLK_TCK'],text=True).strip(),
                    '-op',op,'-total',str(total),'-workers',str(workers),'-out',str(dest/f'{name}.json'),
                    '-metrics-url',f'http://127.0.0.1:{6060+leader}/' + ('metrics' if store == 'etcd' else 'debug/runtime'),
                    '-metrics-kind', 'etcd' if store == 'etcd' else 'runtime']

        def load(name, total, workers=WORKERS, op=WORKLOAD):
            run(load_args(name, total, workers, op), env=os.environ | {'GOMAXPROCS':'1'}, timeout=100)

        def capture_profiles():
            ready = dest/'arrivals-start.txt'
            args = load_args('profile-load', 0) + ['-duration', '25s', '-ready-file', str(ready)]
            check_time()
            COMMANDS.append(args)
            log = open(dest/'load.log', 'w')
            handles.append(log)
            generator = sp.Popen(args, env=os.environ | {'GOMAXPROCS': '1'}, stdout=log, stderr=log)
            children.append(generator)
            until = time.monotonic() + 20
            while not ready.exists():
                if generator.poll() is not None or time.monotonic() >= until:
                    raise RuntimeError('profile generator did not become ready')
                time.sleep(.02)
            time.sleep(1)
            captures = []
            for name, path in [('cpu', 'profile'), ('allocs', 'allocs')]:
                check_time()
                args = ['curl', '--fail', '--silent', '--show-error', '--max-time', '35',
                        f'http://127.0.0.1:{6060+leader}/debug/pprof/{path}?seconds=20',
                        '--output', str(dest/f'{name}.pb.gz')]
                COMMANDS.append(args)
                start = datetime.datetime.now(datetime.timezone.utc).isoformat()
                process = sp.Popen(args)
                children.append(process)
                captures.append((name, process, start))
            intervals = {}
            for name, process, start in captures:
                assert process.wait(timeout=40) == 0, name
                intervals[name] = {'request_start_utc': start,
                                   'request_end_observed_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                   'requested_seconds': 20}
            assert generator.wait(timeout=40) == 0, 'profile generator failed'
            (dest/'profile-intervals.json').write_text(json.dumps(intervals, indent=2))
            for kind, file in [('cpu', 'cpu'), ('alloc_space', 'allocs'), ('alloc_objects', 'allocs')]:
                for cumulative in [False, True]:
                    args = ['go', 'tool', 'pprof', '-top', '-nodecount=25', '-nodefraction=0',
                            '-edgefraction=0', f'-sample_index={kind}']
                    if cumulative:
                        args.append('-cum')
                    args += [str(BIN/store), str(dest/f'{file}.pb.gz')]
                    with open(dest/f'{kind}-{"cum" if cumulative else "flat"}.txt', 'w') as output:
                        run(args, stdout=output, stderr=sp.STDOUT)
            with open(dest/'cpu-peek-Syscall6.txt', 'w') as output:
                run(['go', 'tool', 'pprof', '-nodefraction=0', '-edgefraction=0', '-peek=Syscall6',
                     str(BIN/store), str(dest/'cpu.pb.gz')], stdout=output, stderr=sp.STDOUT)

        def disk(phase):
            result = []
            if store != 'etcd':
                return result
            for i in order:
                if store == 'etcd':
                    # etcd applies before its WAL.Save completes. Its committed
                    # gauge, unlike endpoint status, updates only after Save.
                    target = ctl(i, 'endpoint', 'status')[0]['Status']['raftIndex']
                    until = time.monotonic()+15
                    while True:
                        text = http(6060+i, '/metrics').decode()
                        committed = float(re.search(r'^etcd_server_proposals_committed_total ([^\n]+)$', text, re.M)[1])
                        if committed >= target:
                            break
                        if time.monotonic() >= until:
                            raise RuntimeError(f'WAL.Save fence failed: {target}, {committed}')
                        time.sleep(.02)
                    (dest/f'n{i}-{phase}-wal-fence.json').write_text(json.dumps({'target_raft_index':target,'post_save_commit_gauge':committed}))
                    # Prometheus collectors are not one atomic snapshot. Start
                    # a new scrape after the post-Save gauge fence succeeds.
                    text = http(6060+i, '/metrics').decode()
                    (dest/f'n{i}-{phase}.prom').write_text(text)
                    metrics = dict(re.findall(r'^(etcd_disk_(?:wal_fsync_duration_seconds|backend_commit_duration_seconds)_(?:count|sum)) ([^\n]+)$', text, re.M))
                    result.append({k:float(v) for k,v in metrics.items()})
                    assert 'etcd_disk_wal_fsync_duration_seconds_count' in result[-1], metrics
            return result

        def applied_fence(revision, phase):
            # Require every replica to apply the cohort. Tigerstore async raft
            # cannot apply unstable entries; etcd needs the post-Save fence in
            # disk() too. Both fences are outside the generator's CPU window.
            until = time.monotonic()+15
            while True:
                headers = [ctl(i, 'get', '/registry/pods/ns-000/pod-00000000', '--consistency=s')['header'] for i in order]
                observed = [h['revision'] for h in headers]
                if all(r == revision for r in observed):
                    (dest/f'{phase}-fence.json').write_text(json.dumps({'expected':revision,'headers':headers}))
                    return [h['raft_term'] for h in headers]
                if time.monotonic() >= until:
                    raise RuntimeError(f'catch-up failed: {revision}, {observed}')
                time.sleep(.02)

        load('seed',4096,workers=64,op='put')
        load('warmup',20000)
        time.sleep(1)
        warmup = json.loads((dest/'warmup.json').read_text())
        terms_before = applied_fence(warmup['revision_after'], 'before')
        before = disk('before')
        if profile:
            capture_profiles()
            result = json.loads((dest/'profile-load.json').read_text())
        else:
            load(WORKLOAD,60000)
            result = json.loads((dest/f'{WORKLOAD}.json').read_text())
        terms_after = applied_fence(result['revision_after'], 'after')
        assert set(terms_before+terms_after) == {int(t) for t in result['terms']}, (terms_before,terms_after,result['terms'])
        after_disk = disk('after')
        (dest/'disk.json').write_text(json.dumps({'before':before,'after':after_disk},indent=2))
        if store != 'etcd':
            after = [json.loads(http(6060+i,'/debug/leader')) for i in range(3)]
            assert all(s['leader']==leader+1 and s['gomaxprocs']==status[i]['gomaxprocs'] for i,s in enumerate(after)), after
        else:
            after = [ctl(i,'endpoint','status')[0]['Status'] for i in range(3)]
            assert all(s['leader']==status[0]['leader'] and s['raftTerm']==status[0]['raftTerm'] for s in after), after
        (dest/'status-after.json').write_text(json.dumps(after,indent=2))
        affinities = {}
        for core,i in enumerate(order):
            affinities[str(i)] = {p.name:os.sched_getaffinity(int(p.name)) for p in Path(f'/proc/{procs[i].pid}/task').iterdir()}
            assert all(a=={core} for a in affinities[str(i)].values()),affinities
        (dest/'affinities.json').write_text(json.dumps(affinities,default=list,indent=2))
    finally:
        for p in children + procs:
            if p.poll() is None:
                p.terminate()
        for p in children + procs:
            try:
                p.wait(timeout=10)
            except sp.TimeoutExpired:
                p.kill()
                p.wait()
        for f in handles:
            f.close()
        shutil.rmtree(data)
        (OUT/'commands.json').write_text(json.dumps(COMMANDS,indent=2))


def main():
    check_time()
    OUT.mkdir(parents=True,exist_ok=True)
    for rep in range(1,6):
        # Reverse every second repetition; no simultaneous stores or profile capture.
        stores = ['baseline', 'candidate']
        if os.environ.get('SECOND_CANDIDATE_REF'):
            stores.append('second')
        if os.environ.get('THIRD_CANDIDATE_REF'):
            stores.append('third')
        stores.append('etcd')
        if rep % 2 == 0:
            stores.reverse()
        for store in stores:
            print(f'{WORKLOAD}, {WORKERS} clients, repetition {rep}, {store}',flush=True)
            cluster(store,f'{rep}-{store}')
    if WORKLOAD == 'put' and WORKERS == 256 and os.environ.get('CAPTURE_PROFILES') == '1':
        store = os.environ['PROFILE_STORE']
        print(f'Separate {store} 256-client profile cluster', flush=True)
        cluster(store, f'profile-{store}', profile=True)


if __name__ == '__main__':
    main()
