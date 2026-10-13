#!/usr/bin/env python3
"""Summarize the no-clock observer's fenced counter deltas."""
import csv
import json
from pathlib import Path
import statistics


def histogram(before, after):
    h=[b-a for a,b in zip(before,after)]
    assert min(h)>=0
    n=sum(h)
    def percentile(fraction):
        seen=0
        for i,count in enumerate(h):
            seen+=count
            if seen >= n*fraction: return i
    return {'n':n,'zero_fraction':h[0]/n if n else 0,
            'mean':sum(i*c for i,c in enumerate(h))/n if n else 0,
            'p50':percentile(.5),'p99':percentile(.99),
            'max_bucket':max((i for i,c in enumerate(h) if c),default=0)}

rows=[]
for p in sorted(Path('evidence/pre47batch256').glob('*/c*/*/clock-before.json')):
    before=json.loads(p.read_text());after=json.loads(p.with_name('clock-after.json').read_text())
    out=[]
    for role,(a,b) in enumerate(zip(before,after)):
        r={k:b[k]-a[k] for k in a if not isinstance(a[k],list)}
        r.update({k:histogram(a[k],b[k]) for k in a if isinstance(a[k],list)})
        out.append(r)
        rows.append({'mode':p.parents[2].name,'store':p.parent.name.split('-',1)[1],
                     'rep':p.parent.name.split('-',1)[0], 'role':role,
                     'groups':r['groups'],'entry_groups':r['entry_groups'],
                     'arrivals_during_append':r['arrivals_during_append'],
                     'zero_arrival_group_fraction':r['arrivals_per_append']['zero_fraction'],
                     'deferred_p50':r['completion_deferred']['p50'],
                     'deferred_p99':r['completion_deferred']['p99'],
                     'inbox_p50':r['completion_inbox']['p50'],
                     'inbox_p99':r['completion_inbox']['p99']})
    p.with_name('clock-delta.json').write_text(json.dumps(out,indent=2)+'\n')
if rows:
    with Path('evidence/pre47batch256/clock-runs.csv').open('w') as f:
        w=csv.DictWriter(f,fieldnames=rows[0]);w.writeheader();w.writerows(rows)
    for mode,store in sorted({(r['mode'],r['store']) for r in rows}):
        rs=[r for r in rows if (r['mode'],r['store'])==(mode,store) and r['role']==0]
        print(mode,store,{k:statistics.median(r[k] for r in rs) for k in rows[0] if k not in ('mode','store','rep','role')})
