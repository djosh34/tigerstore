"""Disposable upstream benchmark driver. CPU windows include client startup/drain."""
import json
import os
from pathlib import Path
import re
import shutil
import subprocess as sp
import sys
import time
import urllib.request

ROOT = Path.cwd()
OUT = ROOT / 'out'
BIN = ROOT / 'work/bin'
CERT = ROOT / 'work/cert/cert.pem'
KEY = ROOT / 'work/cert/key.pem'
HZ = os.sysconf('SC_CLK_TCK')
LAYOUT = sys.argv[1]
CPUS = ['0', '1', '2'] if LAYOUT == 'isolated' else ['0,1', '2', '2']
EPS = [f'https://127.0.0.1:{2379+i*100}' for i in range(3)]
PEERS = [f'https://127.0.0.1:{2380+i*100}' for i in range(3)]
TLS = [f'--cacert={CERT}', f'--cert={CERT}', f'--key={KEY}']
TOTAL = 300000


def deadline():
    if time.time() >= 1791928800:  # 2026-10-13T22:00:00Z
        raise RuntimeError('deadline reached')


def ctl(*args, endpoint=None):
    return sp.check_output([str(BIN/'etcdctl'), *TLS,
        '--endpoints='+ (endpoint or ','.join(EPS)), '--command-timeout=10s', *args], text=True)


def status():
    return json.loads(ctl('endpoint', 'status', '-w=json'))


def cpu(pid):
    # comm may contain spaces or ')'; split at the final closing parenthesis.
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return {'ticks': int(fields[11])+int(fields[12]), 'starttime': int(fields[19])}


def metrics():
    result = []
    for i in range(3):
        with urllib.request.urlopen(f'http://127.0.0.1:{2381+i*100}/metrics', timeout=5) as r:
            result.append(r.read().decode())
    return result


def command(n):
    return ['taskset', '-c', '3', str(BIN/'benchmark'), '--endpoints='+EPS[0],
        f'--cacert={CERT}', f'--cert={CERT}', f'--key={KEY}',
        '--conns=16', '--clients=256', '--precise', 'put', '--key-size=64',
        '--val-size=1024', '--key-space-size=65536', f'--total={n}']


for rep in range(1, 6):
    deadline()
    out = OUT/f'rep-{rep}'
    out.mkdir()
    data = ROOT/'work/data'
    data.mkdir()
    processes = []
    logs = []
    try:
        for i in range(3):
            env = dict(os.environ, GOMAXPROCS=str(len(CPUS[i].split(','))))
            args = ['taskset', '-c', CPUS[i], str(BIN/'etcd'), f'--name=n{i}',
                f'--data-dir={data}/n{i}', '--listen-client-urls='+EPS[i],
                '--advertise-client-urls='+EPS[i], '--listen-peer-urls='+PEERS[i],
                '--initial-advertise-peer-urls='+PEERS[i],
                '--initial-cluster='+','.join(f'n{j}={PEERS[j]}' for j in range(3)),
                f'--initial-cluster-token=pre50-{rep}', '--initial-cluster-state=new',
                '--quota-backend-bytes=8589934592', '--log-level=warn',
                '--listen-metrics-urls='+f'http://127.0.0.1:{2381+i*100}',
                f'--cert-file={CERT}', f'--key-file={KEY}', f'--trusted-ca-file={CERT}',
                '--client-cert-auth=true', f'--peer-cert-file={CERT}',
                f'--peer-key-file={KEY}', f'--peer-trusted-ca-file={CERT}',
                '--peer-client-cert-auth=true', '--tls-min-version=TLS1.3', '--tls-max-version=TLS1.3']
            logs.append(open(out/f'n{i}.log', 'w'))
            processes.append(sp.Popen(args, stdout=logs[-1], stderr=sp.STDOUT, env=env))
            (out/f'command-n{i}.json').write_text(json.dumps({'args':args,'GOMAXPROCS':env['GOMAXPROCS']}))
        for attempt in range(100):
            try:
                s = status()
                if len(s) == 3 and all(x['Status']['leader'] for x in s):
                    break
            except sp.CalledProcessError:
                pass
            if any(p.poll() is not None for p in processes):
                raise RuntimeError('etcd exited')
            time.sleep(.2)
        else:
            raise RuntimeError('cluster not ready')
        target = s[0]['Status']['header']['member_id']
        leader = s[0]['Status']['leader']
        if leader != target:
            leader_ep = next(x['Endpoint'] for x in s if x['Status']['header']['member_id'] == leader)
            ctl('move-leader', format(target, 'x'), endpoint=leader_ep)
        for attempt in range(100):
            s = status()
            if all(x['Status']['leader'] == target for x in s):
                break
            time.sleep(.1)
        else:
            raise RuntimeError('leader transfer failed')
        env = dict(os.environ, GOMAXPROCS='1')
        with open(out/'warmup.txt','w') as f:
            sp.run(command(20000), env=env, stdout=f, stderr=sp.STDOUT, check=True, timeout=90)
        time.sleep(1)
        (out/'status-before.json').write_text(json.dumps(status(), indent=2))
        for i,m in enumerate(metrics()):
            (out/f'metrics-before-n{i}.txt').write_text(m)
        affinity = {}
        for i,p in enumerate(processes):
            affinity[str(p.pid)] = {str(t.name): sorted(os.sched_getaffinity(int(t.name)))
                for t in Path(f'/proc/{p.pid}/task').iterdir()}
            assert all(v == sorted(map(int, CPUS[i].split(','))) for v in affinity[str(p.pid)].values())
        (out/'affinity.json').write_text(json.dumps(affinity))
        cmd = command(TOTAL)
        (out/'command-generator.json').write_text(json.dumps({'args':cmd,'GOMAXPROCS':'1'}))
        before = [cpu(p.pid) for p in processes]
        t0 = time.monotonic()
        with open(out/'benchmark.txt','w') as f:
            # time records generator user/system CPU without a /proc exit race.
            sp.run(['/usr/bin/time','-f','%U %S %e','-o',str(out/'generator-time.txt'),*cmd],
                env=env, stdout=f, stderr=sp.STDOUT, check=True, timeout=120)
        elapsed = time.monotonic()-t0
        after = [cpu(p.pid) for p in processes]
        for b,a in zip(before,after):
            assert b['starttime'] == a['starttime']
        text = (out/'benchmark.txt').read_text()
        if 'Error distribution:' in text:
            raise RuntimeError('benchmark reported RPC errors')
        rate = float(re.search(r'Requests/sec:\s+([\d.]+)', text).group(1))
        # The standard benchmark is closed-loop; no rate or scheduling claim.
        summary = {'layout':LAYOUT,'rep':rep,'successful_operations':TOTAL,
            'cpu_window_seconds':elapsed,'benchmark_requests_sec':rate,
            'server_cpu_us_per_op':[(a['ticks']-b['ticks'])/HZ*1e6/TOTAL for a,b in zip(after,before)],
            'before':before,'after':after,'CLK_TCK':HZ,'cpus':CPUS,'generator_cpu':'3'}
        summary['server_total_cpu_us_per_op'] = sum(summary['server_cpu_us_per_op'])
        (out/'summary.json').write_text(json.dumps(summary,indent=2))
        print(json.dumps(summary), flush=True)
        for i,m in enumerate(metrics()):
            (out/f'metrics-after-n{i}.txt').write_text(m)
        s = status()
        (out/'status-after.json').write_text(json.dumps(s, indent=2))
        assert all(x['Status']['leader'] == target for x in s)
        before_s=json.loads((out/'status-before.json').read_text())
        assert s[0]['Status']['header']['revision']-before_s[0]['Status']['header']['revision'] == TOTAL
    finally:
        for p in processes:
            if p.poll() is None:
                p.terminate()
        for p in processes:
            try:
                p.wait(timeout=10)
            except sp.TimeoutExpired:
                p.kill()
                p.wait()
        for f in logs:
            f.close()
        shutil.rmtree(data)
