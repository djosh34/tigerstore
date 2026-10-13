#!/usr/bin/env python3
"""Short paired cases using bench/cluster.sh and cmd/loadgen. No server tuning."""
import concurrent.futures
import datetime
import json
import os
import re
from pathlib import Path
import subprocess
import threading
import time
import urllib.request

ROOT = Path.cwd()
OUT = ROOT / 'results'
DATA = Path(os.environ['RUNNER_TEMP']) / 'shape-data'
DEADLINE = datetime.datetime.fromisoformat('2026-10-13T22:00:00+00:00').timestamp()
CASE = os.environ.get('CASE', 'put-1k-leader-c256')
REPS = int(os.environ.get('REPS', '5'))
DURATION = os.environ.get('DURATION', '3s')
WARMUP = os.environ.get('WARMUP', '1s')
COMMANDS = []


def check_deadline():
    if time.time() >= DEADLINE:
        raise RuntimeError('deadline passed; no new work')


def command(argv, **kwargs):
    COMMANDS.append({'time': time.time(), 'argv': list(map(str, argv)), 'env': kwargs.get('env', {})})
    (OUT / 'commands.json').write_text(json.dumps(COMMANDS, indent=2))
    return subprocess.run(list(map(str, argv)), check=True, text=True, **kwargs)


def ctl(dirpath, *args):
    cert = Path(os.environ['CERTS'])
    argv = [ROOT/'bin/etcdctl', '--endpoints', (dirpath/'endpoints').read_text().strip(),
            '--cacert', cert/'ca.pem', '--cert', cert/'client.pem', '--key', cert/'client-key.pem',
            '--command-timeout', '3s', *args]
    return json.loads(command(argv, capture_output=True).stdout)


def status(dirpath):
    return ctl(dirpath, 'endpoint', 'status', '-w', 'json')


def value(obj, *names):
    for n in names:
        if n in obj:
            return int(obj[n])
    raise KeyError(names)


def fence(dirpath, kind, ports):
    until = time.monotonic() + 15
    while time.monotonic() < until:
        st = status(dirpath)
        applied = [value(x['Status'], 'raftAppliedIndex', 'raft_applied_index') for x in st]
        committed = [value(x['Status'], 'raftIndex', 'raft_index') for x in st]
        revs = [x['Status']['header']['revision'] for x in st]
        if len(set(applied + committed)) == 1 and len(set(revs)) == 1:
            if kind == 'etcd':
                # This gauge advances after WAL.Save, unlike applied status alone.
                saved = [float(re.search(r'^etcd_server_proposals_committed_total ([^\\n]+)',
                         http(p, '/metrics'), re.M).group(1)) for p in ports]
                if not all(x >= max(committed) for x in saved):
                    time.sleep(.1)
                    continue
            return st
        time.sleep(.1)
    raise RuntimeError('replica fence timed out')


def http(port, path):
    with urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=3) as r:
        return r.read().decode()


def proc_snapshot(pids):
    result = []
    for pid in pids:
        path = Path('/proc') / str(pid)
        stat = (path/'stat').read_text().rsplit(')', 1)[1].split()
        tasks = {}
        for t in (path/'task').iterdir():
            try:
                tasks[t.name] = (t/'schedstat').read_text().strip()
            except FileNotFoundError:
                pass
        result.append({'pid': pid, 'utime': int(stat[11]), 'stime': int(stat[12]),
                       'starttime': int(stat[19]), 'schedstat': tasks,
                       'io': (path/'io').read_text(), 'status': (path/'status').read_text()})
    return result


def snapshot(kind, pids, ports):
    obj = {'wall_time': time.time(), 'monotonic': time.monotonic()}
    obj['nodes'] = [json.loads(http(p, '/debug/shape')) if kind != 'etcd'
                    else http(p, '/metrics') for p in ports]
    obj['proc'] = proc_snapshot(pids)
    obj['diskstats'] = Path('/proc/diskstats').read_text()
    return obj


def affinities(pids):
    result = {}
    for cpu, pid in enumerate(pids):
        for task in (Path('/proc')/str(pid)/'task').iterdir():
            text = (task/'status').read_text()
            actual = next(x.split(':')[1].strip() for x in text.splitlines() if x.startswith('Cpus_allowed_list:'))
            result[task.name] = actual
            if actual != str(cpu):
                raise RuntimeError(f'pid {pid} tid {task.name} has CPUs {actual}, expected {cpu}')
    return result


def wait_file(path, process, seconds=90):
    end = time.monotonic() + seconds
    while not path.exists():
        if process.poll() is not None:
            raise RuntimeError(f'generator exited {process.returncode} before {path.name}')
        if time.monotonic() >= end:
            raise RuntimeError(f'timed out waiting for {path.name}')
        time.sleep(.01)


def stop(dirpath):
    # Keep logs before the bench stop command deletes the data directory.
    subprocess.run(['bash', 'bench/cluster.sh', 'stop', str(dirpath)], check=True, timeout=40)


def once(kind, rep, rate=None):
    check_deadline()
    parts = CASE.split('-')
    workload, size_name, ingress, mode = parts[:4]
    size = 1024 if size_name == '1k' else 10240
    concurrency = int(mode[1:]) if mode.startswith('c') else int(os.environ.get('OPEN_CONCURRENCY', '4096'))
    label = f'{kind}-{rep}' + (f'-r{rate}' if rate else '')
    dst = OUT / CASE / label
    dst.mkdir(parents=True)
    data = DATA / label
    process = None
    sampling_stop = threading.Event()
    sample_thread = None
    profile_pool = None
    profile_futures = []
    try:
        command(['bash', 'bench/cluster.sh', 'start', 'etcd' if kind == 'etcd' else 'tigerstore',
                 ROOT/'bin'/kind, data, '0', '1', '2'], timeout=45)
        placement = json.loads((data/'placement.json').read_text())
        (dst/'placement.json').write_text(json.dumps(placement, indent=2))
        pids = [int(x) for x in (data/'pids').read_text().strip().split(',')]
        leader = (data/'leader').read_text().strip()
        all_ep = (data/'endpoints').read_text().strip().split(',')
        ep_order = [leader] + [x for x in all_ep if x != leader]
        ports = [42000 + (int(x.rsplit(':', 1)[1])-40000)//10 for x in ep_order]
        endpoints = ','.join(ep_order) if ingress == 'balanced' else leader
        cert = Path(os.environ['CERTS'])
        barrier = dst/'phase'
        argv = [ROOT/'bin/loadgen', '-endpoints', endpoints, '-cacert', cert/'ca.pem',
                '-cert', cert/'client.pem', '-key', cert/'client-key.pem', '-pids', ','.join(map(str,pids)),
                '-label', kind, '-workload', workload, '-value-size', str(size),
                '-keys', '1000' if workload == 'list' else '4096', '-conns', '18',
                '-concurrency', str(concurrency), '-warmup', WARMUP, '-duration', DURATION,
                '-seed', str(rep), '-barrier-prefix', barrier, '-out', dst/'load.json', '-drain', '30s']
        if ingress == 'balanced':
            argv += ['-balance', 'round-robin']
        if rate is not None:
            argv += ['-arrival', 'fixed', '-rate', str(rate)]
        env = dict(os.environ, GOMAXPROCS='1')
        COMMANDS.append({'argv': list(map(str, argv)), 'env': {'GOMAXPROCS': '1'}, 'time': time.time()})
        (OUT/'commands.json').write_text(json.dumps(COMMANDS, indent=2))
        with (dst/'load.log').open('w') as log:
            process = subprocess.Popen(list(map(str, argv)), stdout=log, stderr=log, env=env)
            wait_file(Path(str(barrier)+'.ready'), process)
            before_status = fence(data, kind, ports)
            time.sleep(.2)
            before = snapshot(kind, pids, ports)
            before['status'] = before_status
            before['affinity'] = affinities(pids)
            before['generator_affinity'] = sorted(os.sched_getaffinity(process.pid))
            if before['generator_affinity'] != [3]:
                raise RuntimeError('generator not pinned to CPU3')
            (dst/'before.json').write_text(json.dumps(before))
            errors = []
            def sample():
                try:
                    with (dst/'samples.jsonl').open('w') as f:
                        while not sampling_stop.is_set():
                            start = time.monotonic()
                            # Full node metrics and disk queues, without per-thread /proc reads.
                            row = {'monotonic': start, 'nodes': [json.loads(http(p, '/debug/shape'))
                                   if kind != 'etcd' else http(p, '/metrics') for p in ports],
                                   'diskstats': Path('/proc/diskstats').read_text()}
                            f.write(json.dumps(row)+'\n')
                            sampling_stop.wait(max(0, .1-(time.monotonic()-start)))
                except Exception as ex:
                    errors.append(repr(ex))
            sample_thread = threading.Thread(target=sample)
            sample_thread.start()
            profile = os.environ.get('PROFILE', 'none')
            if profile != 'none' and kind != 'etcd':
                def capture(index, port):
                    duration = 6 if profile == 'cpu' else 2
                    endpoint = 'profile' if profile == 'cpu' else 'trace'
                    suffix = 'cpu.pb.gz' if profile == 'cpu' else 'run.trace'
                    with urllib.request.urlopen(f'http://127.0.0.1:{port}/debug/pprof/{endpoint}?seconds={duration}', timeout=duration+10) as r:
                        (dst/f'n{index}-{suffix}').write_bytes(r.read())
                profile_pool = concurrent.futures.ThreadPoolExecutor(max_workers=3)
                if profile == 'cpu':
                    profile_futures = [profile_pool.submit(capture,i,p) for i,p in enumerate(ports)]
                    time.sleep(.2)
            check_deadline()
            Path(str(barrier)+'.go').touch()
            if profile == 'trace' and kind != 'etcd':
                time.sleep(.5)
                profile_futures = [profile_pool.submit(capture,i,p) for i,p in enumerate(ports)]
            wait_file(Path(str(barrier)+'.done'), process, 70)
            sampling_stop.set()
            sample_thread.join()
            after_status = fence(data, kind, ports)
            time.sleep(.2)
            after = snapshot(kind, pids, ports)
            after['status'] = after_status
            after['affinity'] = affinities(pids)
            (dst/'after.json').write_text(json.dumps(after))
            Path(str(barrier)+'.finish').touch()
            process.wait(timeout=15)
            for future in profile_futures:
                future.result(timeout=15)
            command(['bash', 'bench/cluster.sh', 'check', data], timeout=10)
            load = json.loads((dst/'load.json').read_text())
            revision_delta = int(after_status[0]['Status']['header']['revision']) - int(before_status[0]['Status']['header']['revision'])
            expected = load['cohort'] if workload == 'put' else 0
            invariant_errors = []
            for i, (a, b) in enumerate(zip(before['proc'],after['proc'])):
                if a['starttime'] != b['starttime']:
                    invariant_errors.append(f'node {i} changed process identity')
            if kind != 'etcd':
                for snap in [before,after]:
                    for node in snap['nodes']:
                        for operation in ['write','fsync']:
                            counters = node['files'][operation]
                            if counters['errors'] or sum(counters['wall_ns_buckets']) != counters['calls']:
                                invariant_errors.append(f'file {operation} error or inconsistent fenced histogram')
            if load['latency_us']['count'] != load['cohort'] or load['cohort_failed'] or load['unsent']:
                invariant_errors.append('arrival cohort incomplete')
            audit = {'revision_delta': revision_delta, 'expected': expected,
                     'sampling_errors': errors, 'invariant_errors': invariant_errors, 'returncode': process.returncode,
                     'valid': bool(load['valid']) and not errors and not invariant_errors and process.returncode == 0 and revision_delta == expected}
            (dst/'audit.json').write_text(json.dumps(audit, indent=2))
            if not audit['valid']:
                raise RuntimeError(f'invalid window {label}: {audit}')
    finally:
        if profile_pool is not None:
            profile_pool.shutdown(wait=True)
        sampling_stop.set()
        if sample_thread is not None:
            sample_thread.join(timeout=5)
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        for p in data.glob('*.log'):
            (dst/p.name).write_bytes(p.read_bytes())
        if data.exists():
            stop(data)


def main():
    OUT.mkdir(exist_ok=True)
    targets = os.environ.get('TARGETS', 'tigerstore etcd').split()
    rates = json.loads(os.environ.get('RATES', '{}'))
    failures = []
    for rep in range(1, REPS+1):
        order = targets if rep % 2 else targets[::-1]
        count = len(next(iter(rates.values()))) if rates else 1
        for i in range(count):
            for kind in order:
                try:
                    once(kind, rep, rates[kind][i] if rates else None)
                except Exception as ex:
                    failures.append({'rep':rep, 'kind':kind, 'rate_index':i, 'error':repr(ex)})
                    (OUT/'failures.json').write_text(json.dumps(failures, indent=2))
                    print(failures[-1], flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
