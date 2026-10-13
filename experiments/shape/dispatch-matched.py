#!/usr/bin/env python3
import datetime
import json
from pathlib import Path
import subprocess

for case,rates in json.loads(Path('experiments/shape/matched-cases.json').read_text()).items():
    assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
    args=['gh','api','repos/djosh34/tigerstore/actions/workflows/bench.yml/dispatches','-f','ref=exp/pre-50-shape']
    for k,v in {'cases':json.dumps([case]),'reps':'5','duration':'3s','warmup':'1s','rates':json.dumps({'tigerstore':rates,'etcd':rates}),'targets':'tigerstore etcd','profile':'none','conns':'18'}.items():
        args += ['-f',f'inputs[{k}]={v}']
    print('dispatch',case,rates,flush=True)
    subprocess.run(args,check=True)
    print('accepted',case,flush=True)
