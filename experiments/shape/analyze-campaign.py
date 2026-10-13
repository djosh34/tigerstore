#!/usr/bin/env python3
"""Offline summaries for non-retired runs. Keep failed and unstarted windows."""
import json
from pathlib import Path
import subprocess
import sys

here=Path(__file__).parent
root=Path(sys.argv[1])
manifest=json.loads((here/'campaign.json').read_text())
status=[]
for r in manifest['runs']:
    if r['role']=='superseded-arrival-bug':continue
    folder=root/r['folder']
    if r['commit'].startswith('223f8f6a') and not (folder/'.downloaded').exists():
        raise RuntimeError(f'run {r["id"]} is not downloaded')
    print('summarize',r['id'],r['folder'],flush=True)
    with (folder/'summary-stdout.txt').open('w') as out, (folder/'summary-stderr.txt').open('w') as err:
        cp=subprocess.run([sys.executable,str(here/'summarize.py'),str(folder)],stdout=out,stderr=err)
    status.append({'run_id':r['id'],'folder':r['folder'],'returncode':cp.returncode,
                   'errors':json.loads((folder/'summary-errors.json').read_text())})
(root/'summary-status.json').write_text(json.dumps(status,indent=2)+'\n')
print('done',flush=True)
