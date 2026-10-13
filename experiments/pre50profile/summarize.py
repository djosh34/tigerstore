#!/usr/bin/env python3
"""Print every unprofiled row and the medians. Reject missing/invalid rows."""
import json
from pathlib import Path
import statistics

root = Path('evidence/pre50profile/raw')
print('store,op,rep,ops_s,mean_ms,p50_ms,p99_ms,p999_ms,leader_us,follower1_us,follower2_us,total_server_us,generator_us')
for store in ['tigerstore','etcd']:
    for op in ['put','range']:
        rows = []
        for rep in range(1,6):
            d = json.loads((root/f'{rep}-{store}'/f'{op}.json').read_text())
            assert d['valid'] and not d['errors']
            c = d['cpu_us_op']
            row = [d[k] for k in ['ops_s','mean_ms','p50_ms','p99_ms','p999_ms']]+c[:3]+[sum(c[:3]),c[3]]
            rows.append(row)
            print(f'{store},{op},{rep},'+','.join(f'{v:.4f}' for v in row))
        med = [statistics.median(v) for v in zip(*rows)]
        print(f'{store},{op},median,'+','.join(f'{v:.4f}' for v in med))
