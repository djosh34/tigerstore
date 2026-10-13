#!/usr/bin/env python3
"""Exact bench75 cluster/fence mechanics; allocation strategy experiment."""
import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess as sp
import time
import urllib.request

ROOT = Path.cwd()
WORKERS = int(os.environ['CLIENTS'])
OUT = ROOT / f'evidence/alloc50/c{WORKERS}'
TMP = Path(os.environ['RUNNER_TEMP']) / 'alloc50'
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


def timing_metrics_args(mode, name, total, leader):
    # CPU-class counters may remain stale until a GC. Short seed/profile
    # cohorts must not fail independent success/revision/CPU validation.
    if mode == 'primary' and name == 'put' and total == 60000:
        return ['-metrics-url', f'http://127.0.0.1:{6060+leader}/debug/runtime',
                '-metrics-kind', 'runtime']
    return []


def cluster(store, label, mode='primary'):
    check_time()
    dest = OUT / label
    dest.mkdir(parents=True)
    data = TMP / label
    data.mkdir()
    procs = []
    handles = []
    valid = False
    initial = ','.join(f'n{i}=https://127.0.0.1:{2380+100*i}' for i in range(3))
    try:
        for i in range(3):
            cert = CERT / f'node{i+1}.pem'
            key = CERT / f'node{i+1}-key.pem'
            binary = 'base' if store == 'base' else 'experiment'
            args = ['taskset', '-c', str(i), str(BIN/binary), '--name', f'n{i}',
                    '--data-dir', str(data/f'n{i}'), '--initial-cluster', initial,
                    '--initial-cluster-token', label,
                    '--listen-peer-urls', f'https://127.0.0.1:{2380+100*i}',
                    '--listen-client-urls', f'https://127.0.0.1:{2379+100*i}',
                    '--advertise-client-urls', f'https://127.0.0.1:{2379+100*i}',
                    '--cert-file', str(cert), '--key-file', str(key),
                    '--trusted-ca-file', str(CERT/'ca.pem'), '--client-cert-auth',
                    '--peer-cert-file', str(cert), '--peer-key-file', str(key),
                    '--peer-trusted-ca-file', str(CERT/'ca.pem')]
            env = os.environ | {'GOMAXPROCS': '1', 'PRE50_DEBUG': f'127.0.0.1:{6060+i}'}
            env.pop('TIGER_ALLOC_STRATEGY', None)
            env.pop('ALLOC50_PROFILE_RATE', None)
            if store in ('ring', 'arena'):
                env['TIGER_ALLOC_STRATEGY'] = store
            if mode == 'profile':
                env['ALLOC50_PROFILE_RATE'] = '1'
            (dest/f'n{i}-environment.json').write_text(json.dumps({k: env.get(k) for k in
                ('GOMAXPROCS', 'PRE50_DEBUG', 'TIGER_ALLOC_STRATEGY', 'ALLOC50_PROFILE_RATE')}, indent=2))
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
                status = [json.loads(http(6060+i, '/debug/leader')) for i in range(3)]
                leaders = {s['leader'] for s in status}
                if len(leaders) != 1 or 0 in leaders:
                    raise RuntimeError('no common leader')
                leader = status[0]['leader']-1
                break
            except Exception:
                if time.monotonic() >= until:
                    raise
                time.sleep(.25)
        order = [leader]+[i for i in range(3) if i != leader]
        for core,i in enumerate(order):
            run(['taskset','-apc',str(core),str(procs[i].pid)], stdout=sp.DEVNULL)
        pids = ','.join(str(procs[i].pid) for i in order)
        go_procs = [status[i]['gomaxprocs'] for i in order]
        assert go_procs == [2, 2, 2], (store, go_procs)
        (dest/'layout.json').write_text(json.dumps({
            'server_gomaxprocs_env': 1,
            'server_go_processors': go_procs,
            'go_processors_source': 'debug runtime.GOMAXPROCS(0)',
            'server_cpus_in_role_order': [0, 1, 2],
            'generator_cpu': 3,
            'generator_gomaxprocs': 1,
        }, indent=2))
        (dest/'placement.json').write_text(json.dumps({'leader_index':leader,'node_order':order,'pids':pids,'status_before':status},indent=2))
        allocator_before = [json.loads(http(6060+i, '/debug/allocator')) for i in order]
        expected_mode = {'ring': 1, 'arena': 2}.get(store, 0)
        for stats in allocator_before:
            if expected_mode:
                assert stats and stats['mode'] == expected_mode and stats['slot_budget'] > 0 and stats['byte_budget'] > 0, stats
            else:
                assert stats is None or stats['mode'] == 0, stats
        (dest/'allocator-before.json').write_text(json.dumps(allocator_before, indent=2))
        endpoint = f'127.0.0.1:{2379+leader*100}'

        def load(name, total, workers=WORKERS):
            check_time()
            args = ['taskset', '-c', '3', str(BIN/'load'), '-endpoint', endpoint,
                    '-certdir', str(CERT), '-pids', pids, '-hz', sp.check_output(['getconf','CLK_TCK'],text=True).strip(),
                    '-op','put','-total',str(total),'-workers',str(workers),'-out',str(dest/f'{name}.json')]
            args += timing_metrics_args(mode, name, total, leader)
            with (dest/f'{name}.log').open('w') as log:
                run(args, env=os.environ | {'GOMAXPROCS':'1'}, timeout=100, stdout=log, stderr=sp.STDOUT)

        def applied_fence(revision, phase):
            # Require every replica to apply the cohort. Tigerstore async raft
            # cannot apply unstable entries. Fence outside the CPU window.
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
        if mode == 'profile':
            # No profiler runs in primary clusters. Rate 1 census has an exact
            # 2000-success sequential workload, not a duration-derived count.
            (dest/'allocs-before.pb.gz').write_bytes(http(6060+leader, '/debug/allocs'))
            total, workers = 2000, 1
        else:
            total, workers = 60000, WORKERS
        load('put', total, workers=workers)
        result = json.loads((dest/'put.json').read_text())
        assert result['valid'] and not result['errors'] and result['successes'] == total, result
        if mode == 'profile':
            (dest/'allocs-after.pb.gz').write_bytes(http(6060+leader, '/debug/allocs'))
            (dest/'profile-cohort.json').write_text(json.dumps({
                'successes': result['successes'], 'workers': workers, 'mem_profile_rate': 1,
                'note': 'Profiles include observation/fence/GC/background overhead; not timing results.',
                'binary': binary, 'strategy': store}, indent=2))
            for index in ('alloc_space', 'alloc_objects', 'inuse_space', 'inuse_objects'):
                args = ['go', 'tool', 'pprof', '-top', '-sample_index='+index,
                        '-base='+str(dest/'allocs-before.pb.gz'), str(BIN/binary),
                        str(dest/'allocs-after.pb.gz')]
                with (dest/f'{index}-delta.txt').open('w') as output:
                    run(args, stdout=output, stderr=sp.STDOUT)
                with (dest/f'{index}-delta.pb.gz').open('wb') as output:
                    run([*args[:3], '-proto', *args[4:]], stdout=output, stderr=sp.PIPE)
                if index in ('alloc_space', 'alloc_objects'):
                    # The baseline profile's own serialization occurs after its
                    # snapshot. Keep raw deltas, but remove only identified
                    # observation stacks in the separate attribution view.
                    with (dest/f'{index}-attribution.txt').open('w') as output:
                        run([*args[:4], '-ignore=runtime/pprof|main.bench75Observe|net/http\\.',
                             *args[4:]], stdout=output, stderr=sp.STDOUT)
        retained = json.loads(http(6060+leader, '/debug/retained'))
        if expected_mode:
            stats = retained['allocator']
            assert stats['mode'] == expected_mode and stats['held_slots'] <= stats['slot_budget'] and stats['held_bytes'] <= stats['byte_budget'], stats
            assert stats['peak_slots'] <= stats['slot_budget'] and stats['peak_bytes'] <= stats['byte_budget'] and stats['fallback_allocations'] == 0, stats
            assert stats['denied_count'] == 0, stats
        (dest/'retained-after.json').write_text(json.dumps(retained, indent=2))
        terms_after = applied_fence(result['revision_after'], 'after')
        assert set(terms_before+terms_after) == {int(t) for t in result['terms']}, (terms_before,terms_after,result['terms'])
        after = [json.loads(http(6060+i,'/debug/leader')) for i in range(3)]
        assert all(s['leader']==leader+1 and s['gomaxprocs']==status[i]['gomaxprocs'] for i,s in enumerate(after)), after
        (dest/'status-after.json').write_text(json.dumps(after,indent=2))
        affinities = {}
        for core,i in enumerate(order):
            affinities[str(i)] = {p.name:os.sched_getaffinity(int(p.name)) for p in Path(f'/proc/{procs[i].pid}/task').iterdir()}
            assert all(a=={core} for a in affinities[str(i)].values()),affinities
        (dest/'affinities.json').write_text(json.dumps(affinities,default=list,indent=2))
        if mode == 'cases':
            args = ['taskset', '-c', '3', str(BIN/'cases'), '-endpoint', endpoint,
                    '-certdir', str(CERT), '-debug-url', f'http://127.0.0.1:{6060+leader}',
                    '-strategy', store, '-out', str(dest/'cases.json')]
            with (dest/'cases.log').open('w') as log:
                run(args, env=os.environ | {'GOMAXPROCS': '1'}, timeout=100, stdout=log, stderr=sp.STDOUT)
            # Verify the case cohort reached every replica and kept the term.
            revision = ctl(leader, 'get', '/alloc50/healthy')['header']['revision']
            case_terms = applied_fence(revision, 'cases-after')
            assert set(case_terms) == set(terms_after), case_terms
            for core, i in enumerate(order):
                assert procs[i].poll() is None
                assert all(os.sched_getaffinity(int(p.name)) == {core}
                           for p in Path(f'/proc/{procs[i].pid}/task').iterdir())
        valid = True
    finally:
        (dest/'validation.json').write_text(json.dumps({'valid': valid, 'variant': store, 'mode': mode}, indent=2))
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
    for rep in range(1,6):
        # No simultaneous variants, GC forcing, or profile capture in timing.
        stores = ['base', 'ring', 'arena']
        if rep % 2 == 0:
            stores.reverse()
        for store in stores:
            print(f'{WORKERS} clients, repetition {rep}, {store}',flush=True)
            cluster(store,f'{rep}-{store}')
    for store in ('base', 'ring', 'arena'):
        cluster(store, f'profile-{store}', mode='profile')
        cluster(store, f'cases-{store}', mode='cases')
    # Default strategy of the experiment binary is checked separately; it
    # never replaces the pristine base binary in the five paired timings.
    cluster('default', 'cases-default', mode='cases')


if __name__ == '__main__':
    main()
