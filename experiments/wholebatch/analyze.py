#!/usr/bin/env python3
"""Offline leader CPU attribution between measured stages, not a CPU profile."""
import csv
import json
from pathlib import Path
from statistics import median
import sys

root = Path(sys.argv[1])
rows = list(csv.DictReader((root/'runs.csv').open()))
result = []
for clients in (16, 64, 256):
    selected = {(int(r['rep']), r['store']): r for r in rows if int(r['clients']) == clients}
    def cost(rep, name):
        return float(selected[rep, name]['leader_cpu_us_op'])
    stages = []
    for before, after in [('main','rpc'), ('rpc','admit'), ('admit','full'), ('main','full'), ('full','ring'), ('full','pool'), ('ring','pool')]:
        stages.append(dict(before=before, after=after,
            removed_fraction_of_main=median((cost(r,before)-cost(r,after))/cost(r,'main') for r in range(1,6)),
            removed_fraction_of_previous=median(1-cost(r,after)/cost(r,before) for r in range(1,6))))
    result.append(dict(clients=clients, stages=stages))
(root/'stage-cpu.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
