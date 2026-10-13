#!/usr/bin/env python3
"""Add labeled dispatch expectations without changing downloaded runner plans."""
import json
from pathlib import Path
import sys

root=Path(sys.argv[1])
manifest=json.loads(Path(__file__).with_name('campaign.json').read_text())
for r in manifest['runs']:
    inputs=r['inputs']
    for case in inputs['cases']:
        folder=root/r['folder']/f'shape-{case}'
        folder.mkdir(parents=True,exist_ok=True)
        windows=[]
        for rep in range(1,inputs['reps']+1):
            for kind in inputs['targets']:
                rates=inputs['rates'].get(kind,[0])
                for rate in rates:
                    label=f'{kind}-{rep}'+(f'-r{rate}' if rate else '')
                    windows.append({'case':case,'kind':kind,'rep':rep,'rate':rate,
                                    'path':f'{case}/{label}/load.json'})
        expected={'case':case,'reps':inputs['reps'],'targets':inputs['targets'],
                  'rates':inputs['rates'],'duration':inputs['duration'],'warmup':inputs['warmup'],
                  'profile':inputs['profile'],'windows':windows,'source':'offline workflow-dispatch manifest',
                  'run_id':r['id'],'commit':r['commit'],'role':r['role']}
        if (folder/'plan.json').exists():
            actual=json.loads((folder/'plan.json').read_text())
            for key in ['case','reps','targets','rates','duration','warmup','profile']:
                assert actual[key]==expected[key], (r['id'],case,key,actual[key],expected[key])
            assert {json.dumps(w,sort_keys=True) for w in actual['windows']} == {json.dumps(w,sort_keys=True) for w in windows}, (r['id'],case,'windows')
        (folder/'expected-plan.json').write_text(json.dumps(expected,indent=2)+'\n')
print('Dispatch expectations checked and recorded.')
