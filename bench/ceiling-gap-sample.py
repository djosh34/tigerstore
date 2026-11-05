#!/usr/bin/env python3
"""Eight-second in-load snapshots; metrics readouts carry individual timestamps."""
import json, os, pathlib, subprocess, sys, time
out, cluster, certs, disk = sys.argv[1:]
out = pathlib.Path(out)
pids = pathlib.Path(cluster, 'pids').read_text().strip().split(',')
eps = pathlib.Path(cluster, 'endpoints').read_text().strip().split(',')
leader = pathlib.Path(cluster, 'leader').read_text().strip()
eps = [leader] + [ep for ep in eps if ep != leader]

def get(ep, path):
    return subprocess.check_output(['curl', '-fsS', '--max-time', '4', '--cacert', certs+'/ca.pem', '--cert', certs+'/client.pem', '--key', certs+'/client-key.pem', 'https://'+ep+path]).decode()

def sample():
    r = {'at': time.monotonic(), 'nodes': [], 'diskstats': pathlib.Path('/proc/diskstats').read_text()}
    for pid, ep in zip(pids, eps):
        metrics = get(ep, '/metrics')
        vals = {}
        for line in metrics.splitlines():
            if not line.startswith('#') and line.strip():
                k,v,*_ = line.split()
                vals[k] = float(v)
        r['nodes'].append({'at':time.monotonic(), 'pid':pid, 'endpoint':ep, 'io':pathlib.Path('/proc',pid,'io').read_text(), 'metrics':vals, 'vars':json.loads(get(ep,'/debug/vars'))})
    return r

rows=[sample()]
progress=[]
for i in range(8):
    time.sleep(max(0, rows[0]['at']+i+1-time.monotonic()))
    progress.append({'at':time.monotonic(), 'vars':json.loads(get(leader,'/debug/vars'))})
    if i % 2 == 0:
        with open(out/f'tcp-{i}.txt','w') as f:
            subprocess.run(['ss','-tinp'],stdout=f,check=True)
rows.append(sample())
(out/'samples.json').write_text(json.dumps(rows))
(out/'progress.json').write_text(json.dumps(progress))
for name in ['block','mutex','goroutine']:
    (out/(name+'.pprof')).write_text(get(leader,'/debug/pprof/'+name+'?debug=1'))
