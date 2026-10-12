#!/usr/bin/env python3
"""Run each index in a fresh process, serially; never start after the deadline."""
import datetime, json, os, resource, subprocess, time
DEADLINE = datetime.datetime.fromisoformat('2026-10-13T22:00:00+00:00')
def check():
    if datetime.datetime.now(datetime.timezone.utc) >= DEADLINE:
        raise SystemExit('deadline reached')
def swaps():
    with open('/proc/vmstat') as f:
        return {k: int(v) for k, v in map(str.split, f) if k in ('pswpin', 'pswpout')}
check()
os.makedirs('results', exist_ok=True)
with open('results/machine.txt', 'w') as f:
    for cmd in [['date','-u'], ['uname','-a'], ['go','version'], ['lscpu'], ['free','-h'], ['git','rev-parse','HEAD']]:
        subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, check=True)
    f.write('Process affinity CPU 6; GOMAXPROCS=1; GOGC=100; GOMEMLIMIT=4GiB. Shared host.\n')
env = dict(os.environ, GOMAXPROCS='1', GOMEMLIMIT='4GiB', GOGC='100')
for name, idx, load in [('arena-random', 'arena', 'random'), ('slice','slice','bulk'), ('btree-random','btree','random'), ('arena-bulk','arena','bulk')]:
    check()
    before = swaps()
    r0 = resource.getrusage(resource.RUSAGE_CHILDREN)
    start = time.monotonic()
    command = ['taskset','-c','6','./bench','-index',idx,'-load',load]
    print('starting', name, datetime.datetime.now(datetime.timezone.utc).isoformat(), flush=True)
    with open('results/'+name+'.jsonl','w') as out, open('results/'+name+'.stderr','w') as err:
        p = subprocess.run(command, env=env, stdout=out, stderr=err)
    r1 = resource.getrusage(resource.RUSAGE_CHILDREN)
    after = swaps()
    record = dict(name=name, command=command, elapsed_s=time.monotonic()-start,
                  user_s=r1.ru_utime-r0.ru_utime, system_s=r1.ru_stime-r0.ru_stime,
                  minor_faults=r1.ru_minflt-r0.ru_minflt, major_faults=r1.ru_majflt-r0.ru_majflt,
                  host_swap_pages={k:after[k]-before[k] for k in before}, returncode=p.returncode)
    with open('results/run-metadata.jsonl','a') as f:
        f.write(json.dumps(record)+'\n')
    print(record, flush=True)
    if p.returncode:
        raise SystemExit(p.returncode)
