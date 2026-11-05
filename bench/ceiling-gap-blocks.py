#!/usr/bin/env python3
"""Block issue attribution: exact ext4 journal inode range, metadata flag,
and lower-bound WAL/checkpoint coverage from 1 Hz open-file FIEMAP snapshots.
No claim is made about unobserved/closed files or per-FD reuse.
"""
import bisect,collections,json,pathlib,re,statistics,sys
rows=[]
def merged(intervals):
 out=[]
 for lo,hi in sorted(intervals):
  if out and lo<=out[-1][1]:out[-1]=(out[-1][0],max(hi,out[-1][1]))
  else:out.append((lo,hi))
 return out
def overlap(lo,hi,intervals):
 total=0
 for a,b in intervals:
  if a>=hi:break
  if b>lo:total+=max(0,min(hi,b)-max(lo,a))
 return total
for root in map(pathlib.Path,sys.argv[1:]):
 for p in sorted(root.glob('*-max-*/block.txt')):
  d=p.parent; offset=int((d/'partition-start.txt').read_text());journal=[];maps=collections.defaultdict(list)
  for lo,hi in re.findall(r'\([^)]*\):(\d+)-(\d+)',(d/'journal.txt').read_text()):journal.append((offset+int(lo)*8,offset+(int(hi)+1)*8))
  for f in d.glob('extents-*.txt'):
   fds=json.loads((d/f.name.replace('extents-','fds-').replace('.txt','.json')).read_text());kind=None
   for line in f.read_text().splitlines():
    m=re.match(r'File size of (.*?) is ',line)
    if m:
     name=fds.get(m[1],'');kind='wal' if name.endswith('.wal') else 'checkpoint' if '/ckpt-' in name else 'otherfile'
    m=re.match(r'\s*\d+:\s*\d+\.\.\s*\d+:\s*(\d+)\.\.\s*(\d+):\s*(\d+):',line)
    if m and kind and 'delalloc' not in line and int(m[3]):maps[kind].append((offset+int(m[1])*8,offset+(int(m[2])+1)*8))
  maps={k:merged(v) for k,v in maps.items()};journal=merged(journal)
  c=collections.Counter()
  for line in p.read_text().splitlines():
   fields=line.split()
   if len(fields)!=4:continue
   action,start,n,flags=fields
   if action!='D' or 'W' not in flags:continue
   start,n=int(start),int(n);c['write_bytes']+=n*512;c['write_requests']+=1
   c['journal_bytes']+=overlap(start,start+n,journal)*512
   if 'M' in flags:c['metadata_flag_bytes']+=n*512
   for k,v in maps.items():c[k+'_covered_bytes']+=overlap(start,start+n,v)*512
  a,b=json.loads((d/'samples.json').read_text());delta=lambda k:b['nodes'][0]['metrics'][k]-a['nodes'][0]['metrics'][k]
  ops=delta('etcd_debugging_mvcc_current_revision');dt=b['nodes'][0]['at']-a['nodes'][0]['at'];ops8=ops/dt*8
  row={'runner':root.name,'config':d.name,**c,'ops_estimated_8s':ops8,'bytes_op':{k:v/ops8 for k,v in c.items() if k.endswith('bytes')},'fractions':{k:v/c['write_bytes'] for k,v in c.items() if k.endswith('bytes')}}
  rows.append(row)
(pathlib.Path(sys.argv[1])/'block-derived.json').write_text(json.dumps(rows,indent=2))
print('| runner | configuration | issued MiB | device B/op | journal B/op | all metadata B/op | WAL mapped lower bound B/op | checkpoint mapped lower bound B/op |')
print('|---|---|---:|---:|---:|---:|---:|---:|')
for r in rows:
 b=r['bytes_op'];print(f"| {r['runner']} | {r['config']} | {r['write_bytes']/2**20:.1f} | {b['write_bytes']:.1f} | {b['journal_bytes']:.1f} | {b.get('metadata_flag_bytes',0):.1f} | {b.get('wal_covered_bytes',0):.1f} | {b.get('checkpoint_covered_bytes',0):.1f} |")
