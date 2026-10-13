#!/usr/bin/env python3
# Each half-curve is one short paired runner job. Run only before the deadline.
import datetime
import json
from pathlib import Path
import subprocess
import sys

skip=int(sys.argv[1]) if len(sys.argv)>1 else 0
index=0
for case,rates in json.loads(Path('experiments/shape/open-cases.json').read_text()).items():
    for half in [0,1]:
        index+=1
        if index<=skip:
            continue
        assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
        selected={k:v[half*3:half*3+3] for k,v in rates.items()}
        args=['gh','api','repos/djosh34/tigerstore/actions/workflows/bench.yml/dispatches','-f','ref=exp/pre-50-shape']
        for k,v in {'cases':json.dumps([case]),'reps':'5','duration':'3s','warmup':'1s','rates':json.dumps(selected),'targets':'tigerstore etcd','profile':'none'}.items():
            args += ['-f',f'inputs[{k}]={v}']
        print('dispatch',index,case,half,selected,flush=True)
        subprocess.run(args,check=True)
        print('accepted',index,flush=True)
