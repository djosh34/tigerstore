#!/usr/bin/env python3
"""Summarize downloaded Actions artifacts; reject request-accounting mismatch."""
import glob,json,re,statistics,os,sys
root=sys.argv[1] if len(sys.argv)>1 else 'results'
groups={}
print('mode,connections,reply,GOMAXPROCS,cpu_us_per_req_median,rps_median,allocs_min_max')
for path in sorted(glob.glob(root+'/*.server')):
    name=os.path.basename(path)
    match=re.match(r'(\w+)-n(\d+)-b(\d+)-p(\d+)-r(\d+)(trace)?',name)
    if not match: continue
    mode,n,b,p,rep,trace=match.groups()
    records=[json.loads(line) for line in open(path) if line.startswith('{')]
    if not records: continue
    record=records[-1]
    client=open(path.replace('.server','.client')).read()
    wanted=int(re.search(r'requests=(\d+)',client)[1])
    assert record['requests']==wanted,(path,record['requests'],wanted)
    if trace:
        rows=open(path.replace('.server','.strace')).read().splitlines()
        calls={}
        for row in rows:
            parts=row.split()
            if len(parts)>=5 and re.match(r'^\d+\.\d+$',parts[0]):
                calls[parts[-1]]=int(parts[3])
        print('TRACE',mode,n,b,'requests',wanted,'total/req',calls.get('total',0)/wanted,'io_enter/req',calls.get('io_uring_enter',0)/wanted,'read/req',calls.get('read',0)/wanted,'write/req',calls.get('write',0)/wanted,'epoll/req',calls.get('epoll_pwait',0)/wanted,'futex/req',calls.get('futex',0)/wanted,file=sys.stderr)
    else: groups.setdefault((mode,int(n),int(b),int(p)),[]).append(record)
for key,rs in sorted(groups.items(),key=lambda kv:(kv[0][3],kv[0][2],kv[0][1],kv[0][0])):
    print(','.join(map(str,(*key,round(statistics.median(r['cpu_us_per_req'] for r in rs),4),round(statistics.median(r['rps'] for r in rs)),f"{min(r['mallocs'] for r in rs)}..{max(r['mallocs'] for r in rs)}"))))
