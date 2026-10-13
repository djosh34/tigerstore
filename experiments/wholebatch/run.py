#!/usr/bin/env python3
"""Matched whole-batch variants, exact main, optional native ref, and etcd."""
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
OUT = ROOT / f'evidence/wholebatch/c{WORKERS}'
TMP = Path(os.environ['RUNNER_TEMP']) / 'wholebatch'
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
    check_time()
    with urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=15) as response:
        return response.read()


def ctl(index, *args):
    p = run([BIN/'etcdctl', f'--endpoints=https://127.0.0.1:{2379+index*100}',
             f'--cacert={CERT}/ca.pem', f'--cert={CERT}/client.pem',
             f'--key={CERT}/client-key.pem', '-w=json', *args], capture_output=True)
    return json.loads(p.stdout)


def cluster(variant, label):
    check_time()
    store = variant['binary']
    dest = OUT / label
    dest.mkdir(parents=True)
    data = TMP / label
    data.mkdir()
    procs = []
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
            env = {k: v for k, v in os.environ.items() if not k.startswith('TIGERSTORE_')}
            env |= {'GOMAXPROCS': '2', 'PRE50_DEBUG': f'127.0.0.1:{6060+i}'}
            if variant['frontend']:
                env['TIGERSTORE_FRONTEND'] = variant['frontend']
            if variant['batch_mode']:
                env['TIGERSTORE_BATCH_MODE'] = variant['batch_mode']
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
                check_time()
                if time.monotonic() >= until:
                    raise
                time.sleep(.25)
        order = [leader]+[i for i in range(3) if i != leader]
        for core,i in enumerate(order):
            run(['taskset','-apc',str(core),str(procs[i].pid)], stdout=sp.DEVNULL)
        pids = ','.join(str(procs[i].pid) for i in order)
        go_procs = [status[i]['gomaxprocs'] for i in order] if store != 'etcd' else [2, 2, 2]
        expected_go_procs = 2
        assert go_procs == [expected_go_procs]*3, (store, go_procs)
        (dest/'layout.json').write_text(json.dumps({
            'server_gomaxprocs_env': 2,
            'variant': variant,
            'generator_connections': 16,
            'server_go_processors': go_procs,
            'go_processors_source': 'debug runtime.GOMAXPROCS(0)' if store != 'etcd' else 'GOMAXPROCS environment',
            'server_cpus_in_role_order': [0, 1, 2],
            'generator_cpu': 3,
            'generator_gomaxprocs': 1,
        }, indent=2))
        (dest/'placement.json').write_text(json.dumps({'leader_index':leader,'node_order':order,'pids':pids,'status_before':status},indent=2))
        endpoint = f'127.0.0.1:{2379+leader*100}'

        def load(name, total, workers=WORKERS, allocations=False):
            check_time()
            args = ['taskset', '-c', '3', str(BIN/'load'), '-endpoint', endpoint,
                    '-certdir', str(CERT), '-pids', pids, '-hz', sp.check_output(['getconf','CLK_TCK'],text=True).strip(),
                    '-op','put','-total',str(total),'-workers',str(workers),'-conns','16',
                    '-out',str(dest/f'{name}.json')]
            if allocations:
                path = '/metrics' if store == 'etcd' else '/debug/alloc'
                args += ['-alloc-urls', ','.join(f'http://127.0.0.1:{6060+i}{path}' for i in order)]
            run(args, env=os.environ | {'GOMAXPROCS':'1'}, timeout=100)

        def disk(phase):
            result = []
            for i in order:
                if store != 'etcd':
                    result.append(json.loads(http(6060+i, '/debug/disk')))
                else:
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

        load('seed',4096,workers=64)
        load('warmup',20000)
        time.sleep(1)
        warmup = json.loads((dest/'warmup.json').read_text())
        terms_before = applied_fence(warmup['revision_after'], 'before')
        before = disk('before')
        load('put',60000,allocations=True)
        result = json.loads((dest/'put.json').read_text())
        terms_after = applied_fence(result['revision_after'], 'after')
        assert set(terms_before+terms_after) == {int(t) for t in result['terms']}, (terms_before,terms_after,result['terms'])
        after_disk = disk('after')
        (dest/'disk.json').write_text(json.dumps({'before':before,'after':after_disk},indent=2))
        if store != 'etcd':
            after = [json.loads(http(6060+i,'/debug/leader')) for i in range(3)]
            assert all(s['leader']==leader+1 and s['gomaxprocs']==expected_go_procs for s in after), after
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
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
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
    variants = json.loads((OUT/'variants.json').read_text())
    assert WORKERS in (16, 64, 256), WORKERS
    for rep in range(1,6):
        # Rotate the adjacent comparison order across repetitions, without overlap.
        shift = (rep-1) % len(variants)
        ordered = variants[shift:] + variants[:shift]
        for variant in ordered:
            print(f'{WORKERS} clients, repetition {rep}, {variant["name"]}',flush=True)
            cluster(variant,f'{rep}-{variant["name"]}')


if __name__ == '__main__':
    main()
