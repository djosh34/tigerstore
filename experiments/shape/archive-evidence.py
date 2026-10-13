#!/usr/bin/env python3
"""Archive complete runner artifacts, including retired and failed attempts."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

here=Path(__file__).parent
root=Path(sys.argv[1]).resolve()
out=here/'evidence'
out.mkdir(exist_ok=True)
manifest=json.loads((here/'campaign.json').read_text())

def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        while chunk:=f.read(1<<20):h.update(chunk)
    return h.hexdigest()

for run in manifest['runs']:
    rid=run['id']; marker=out/f'run-{rid}.json'
    if marker.exists():continue
    print('archive',rid,run['folder'],flush=True)
    archive=out/f'run-{rid}.tar.zst'
    # Generated summaries are published separately. The explicitly labeled
    # expected-plan.json remains beside each original runner plan.json.
    excludes=['.downloaded','windows.csv','summary.json','tables.md','summary-errors.json',
              'summary-stdout.txt','summary-stderr.txt']
    args=['tar','-C',str(root),'--use-compress-program=zstd -q -T1 -9 --long=27','-cf',str(archive)]
    args += [f'--exclude={x}' for x in excludes]
    args += [run['folder']]
    subprocess.run(args,check=True)
    whole=digest(archive)
    parts=[archive]
    if archive.stat().st_size >= 95<<20:
        parts=[]
        with archive.open('rb') as f:
            n=0
            while chunk:=f.read(90<<20):
                p=archive.with_name(archive.name+f'.part{n:02d}')
                p.write_bytes(chunk);parts.append(p);n+=1
        archive.unlink()
    record={'run_id':rid,'folder':run['folder'],'role':run['role'],'commit':run['commit'],
            'compression':'zstd --long=27','combined_sha256':whole,'parts':[{'file':p.name,'sha256':digest(p),'bytes':p.stat().st_size} for p in parts]}
    marker.write_text(json.dumps(record,indent=2)+'\n')
records=[json.loads((out/f'run-{r["id"]}.json').read_text()) for r in manifest['runs']]
(out/'archives.json').write_text(json.dumps(records,indent=2)+'\n')
print('done',sum(p['bytes'] for r in records for p in r['parts']),flush=True)
