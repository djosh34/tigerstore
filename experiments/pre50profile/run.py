#!/usr/bin/env python3
"""Disposable Actions runner only. Both stores, same pinned cores and driver."""
import concurrent.futures
import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess as sp
import time
import urllib.request

ROOT = Path.cwd()
DETAIL = os.environ.get('PRE50_DETAIL') == '1'
OUT = ROOT / ('evidence/pre50profile/detail' if DETAIL else 'evidence/pre50profile/raw')
TMP = Path(os.environ['RUNNER_TEMP']) / 'pre50profile'
CERT = TMP / 'certs'
BIN = TMP / 'bin'
DEADLINE = datetime.datetime.fromisoformat('2026-10-13T22:00:00+00:00')
COMMANDS = []


def check_time():
    if datetime.datetime.now(datetime.timezone.utc) >= DEADLINE:
        raise RuntimeError('deadline passed')


def run(args, **kwargs):
    COMMANDS.append([str(x) for x in args])
    return sp.run(args, check=True, text=True, **kwargs)


def http(port, path):
    return urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=45).read()


def ctl(index, *args):
    p = run([BIN/'etcdctl', f'--endpoints=https://127.0.0.1:{2379+index*100}',
             f'--cacert={CERT}/ca.pem', f'--cert={CERT}/client.pem',
             f'--key={CERT}/client-key.pem', '-w=json', *args], capture_output=True)
    return json.loads(p.stdout)


def cluster(store, label, profile_op=None):
    check_time()
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
                         '--logger', 'zap', '--log-level', 'warn']
            env = os.environ | {'GOMAXPROCS': '1', 'PRE50_DEBUG': f'127.0.0.1:{6060+i}', 'PRE50_DISK_TIMING': '1' if DETAIL else '0'}
            log = open(dest/f'n{i}.log', 'w')
            handles.append(log)
            COMMANDS.append(args)
            procs.append(sp.Popen(args, stdout=log, stderr=log, env=env))
        until = time.monotonic()+30
        while True:
            if any(p.poll() is not None for p in procs):
                raise RuntimeError('server exited during startup')
            try:
                if store == 'tigerstore':
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
        (dest/'placement.json').write_text(json.dumps({'leader_index':leader,'node_order':order,'pids':pids,'status_before':status},indent=2))
        endpoint = f'127.0.0.1:{2379+leader*100}'

        def load(op, name, total=60000, duration=None, workers=256):
            check_time()
            args = ['taskset', '-c', '3', str(BIN/'load'), '-endpoint', endpoint,
                    '-certdir', str(CERT), '-pids', pids, '-hz', sp.check_output(['getconf','CLK_TCK'],text=True).strip(),
                    '-op',op,'-total',str(total),'-workers',str(workers),'-out',str(dest/f'{name}.json')]
            if duration:
                args += ['-duration',duration]
            run(args, env=os.environ | {'GOMAXPROCS':'1'}, timeout=100)

        load('put','seed',4096,workers=64)
        load('put','warmup',20000)
        time.sleep(1)
        if profile_op:
            def fetch_profile(i, kind):
                suffix = 'profile' if kind == 'cpu' else kind
                b = http(6060+i, f'/debug/pprof/{suffix}?seconds=30')
                (dest/f'n{i}-{kind}.pb.gz').write_bytes(b)
            selected = order if DETAIL else order[:2]
            for i in selected:
                http(6060+i,'/debug/rates?on=1')
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
                futures = [pool.submit(fetch_profile,i,k) for i in selected for k in ['cpu','mutex','block','allocs']]
                time.sleep(1)
                disk_before = [json.loads(http(6060+i,'/debug/disk')) for i in order] if DETAIL else []
                load(profile_op, 'profile-load', 0, '25s')
                disk_after = [json.loads(http(6060+i,'/debug/disk')) for i in order] if DETAIL else []
                (dest/'disk.json').write_text(json.dumps({'before':disk_before,'after':disk_after},indent=2))
                for f in futures:
                    f.result()
            for i in selected:
                http(6060+i,'/debug/rates?on=0')
        else:
            load('put','put')
            load('range','range-warmup',10000)
            load('range','range',100000)
        if store == 'tigerstore':
            after = [json.loads(http(6060+i,'/debug/leader')) for i in range(3)]
            assert all(s['leader']==leader+1 for s in after), after
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
    for rep in ([] if DETAIL else range(1,6)):
        for store in (['tigerstore','etcd'] if rep%2 else ['etcd','tigerstore']):
            print(f'{rep} {store}',flush=True)
            cluster(store,f'{rep}-{store}')
    for op in ['put','range']:
        print(f'profile {op}',flush=True)
        cluster('tigerstore',f'profile-{op}',op)


if __name__ == '__main__':
    main()
