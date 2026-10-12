#!/usr/bin/env python3
import collections, json, pathlib, statistics, sys
root=pathlib.Path(sys.argv[1] if len(sys.argv)>1 else 'results')
allrows={}
checks=[]
for name in ['arena-random','slice','btree-random','arena-bulk']:
    records=[json.loads(x) for x in (root/(name+'.jsonl')).read_text().splitlines()]
    assert records[-1]['stage']=='done'
    checks.append([(r['op'],r['rep'],r['checksum']) for r in records if 'op' in r])
    loaded=records[0]
    gc=[r for r in records if r.get('stage')=='forced_gc']
    ops=collections.defaultdict(list)
    for r in records:
        if 'op' in r: ops[r['op']].append(r)
    summary={
        'index_B_key':loaded['index_B_key'],'total_B_key':loaded['heap_B_key'],
        'scan_heap_MB':loaded['scan_heap_MB'],
        'GC_wall_ms':statistics.median(r['wall_ms'] for r in gc),
        'GC_cpu_ms':statistics.median(r['cpu_ms'] for r in gc),
        'GC_STW_us':statistics.median(r['stw_ms']*1000 for r in gc),
        'operations':{}
    }
    for op, rs in ops.items():
        if op.endswith('_drain'): continue
        ns=[r['ns_op'] for r in rs]
        n=sum(r['ops'] for r in rs)
        costs=list(rs)
        if op in ('put_create','put_overwrite'):
            costs+=ops[op+'_drain']
            estimate=sum(r['ns_op']*r['ops'] for r in costs)/n
        else: estimate=statistics.median(ns)
        summary['operations'][op]={
            'ns_op':estimate,'min_ns_op':min(ns),'max_ns_op':max(ns),
            'alloc_B_op':sum(r['alloc_B_op']*r['ops'] for r in costs)/n,
            'GC_cycles':sum(r['gc_cycles'] for r in costs),
            'GC_cpu_ms':sum(r['gc_cpu_ms'] for r in costs)
        }
    for r in records:
        if r.get('stage')=='buffer': summary['buffer']=r
    allrows[name]=summary
assert all(c==checks[0] for c in checks), 'index result checksums differ'
print('All operation checksums match across four indexes.\n')
print('| Metric | Arena random | Slice + buffer | google/btree random | Arena bulk |')
print('|---|---:|---:|---:|---:|')
for k in ['index_B_key','total_B_key','scan_heap_MB','GC_wall_ms','GC_cpu_ms','GC_STW_us']:
    print('|',k,'|',' | '.join(f'{s[k]:,.3f}' for s in allrows.values()),'|')
for op in allrows['arena-random']['operations']:
    print('|',op,'ns/op |',' | '.join(f'{s["operations"][op]["ns_op"]:,.0f}' for s in allrows.values()),'|')
(root/'summary.json').write_text(json.dumps(allrows,indent=2)+'\n')
