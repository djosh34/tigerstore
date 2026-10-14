#!/usr/bin/env python3
# Prototype E (#133): summarizes the bench artifacts.
#   exp/analyze.py <results dir of one bench job>
import glob, json, os, re, statistics, sys

d = sys.argv[1]
runs = {}
for f in sorted(glob.glob(os.path.join(d, '*', '*.json'))):
    m = re.match(r'(.+)-(max|rate)-(\d+)\.json', os.path.basename(f))
    if not m:
        continue
    t, mode, rep = m.group(1), m.group(2), int(m.group(3))
    j = json.load(open(f))
    r = dict(valid=j['valid'], tput=j['throughput'], p50=j['latency_us']['p50'], p99=j['latency_us']['p99'],
             cpu=j['server_cpu_us_per_op'], ops=j['ops'])
    res = f[:-5] + '.resources'
    if os.path.exists(res):
        txt = open(res).read()
        rv = re.search(r'revision=(\d+)', txt)
        r['rev'] = int(rv.group(1)) if rv else None
        r['wb'] = [int(x) for x in re.findall(r'write_bytes: (\d+)', txt)]
        r['wchar'] = [int(x) for x in re.findall(r'wchar: (\d+)', txt)]
    logs = sorted(glob.glob(f[:-5] + '-n*.log'))
    eb = []
    for lf in logs:
        lines = [l for l in open(lf) if 'E bytes:' in l]
        if lines:
            eb.append(dict(re.findall(r'(\w+)=([\d.]+)', lines[-1])))
    r['e'] = eb
    runs.setdefault((t, mode), []).append(r)

def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else float('nan')

print('target mode n  tput_median  [min..max]  p50_ms  p99_ms  cpu_us/op')
for (t, mode), rs in sorted(runs.items()):
    tp = [r['tput'] for r in rs]
    print(f"{t:15s} {mode:4s} {len(rs)} {med(tp):10.0f} [{min(tp):.0f}..{max(tp):.0f}] {med([r['p50'] for r in rs])/1000:6.2f} {med([r['p99'] for r in rs])/1000:6.2f} {med([r['cpu'] for r in rs]):6.1f}  valid={all(r['valid'] for r in rs)}")

print('\nbytes per write (revision) per node, median over max-mode runs')
for (t, mode), rs in sorted(runs.items()):
    if mode != 'max':
        continue
    per = []
    for r in rs:
        if not r.get('rev'):
            continue
        row = dict(dev_write=med(r['wb']) / r['rev'], wchar=med(r['wchar']) / r['rev'])
        if r['e']:
            keys = ['wal_data', 'wal_framing', 'wal_hardstate', 'ckpt_shared', 'ckpt_pernode', 'clean', 'clean_table']
            for k in keys:
                row[k] = med([float(e.get(k, 0)) for e in r['e']]) / r['rev']
            row['ckpts'] = med([float(e.get('ckpts', 0)) for e in r['e']])
            row['snap_sent'] = max(float(e.get('snap_sent', 0)) for e in r['e'])
        per.append(row)
    if not per:
        continue
    ks = per[0].keys()
    print(t, ' '.join(f"{k}={med([p.get(k) for p in per]):.1f}" for k in ks))
