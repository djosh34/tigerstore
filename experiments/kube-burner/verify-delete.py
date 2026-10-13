#!/usr/bin/env python3
"""Check current registry keys on each replica after namespace deletion."""
import base64
import json
from pathlib import Path
import subprocess
import sys
import time

out = Path(sys.argv[1])
summary = json.loads((out / 'workload/summary.json').read_text())
prefix = summary['prefix']
namespaces = {f'{prefix}-mixed-{n}' for n in range(8)} | {f'{prefix}-node'}
rows = []
for n in range(1, 4):
    command = ['docker', 'exec', 'external-etcd-1', '/bin-etcd/etcdctl',
               f'--endpoints=https://external-etcd-{n}:2379', '--cacert=/certs/ca.crt',
               '--cert=/certs/client.crt', '--key=/certs/client.key',
               '--command-timeout=15s', 'get', '/registry/', '--prefix', '--keys-only', '-w', 'json']
    start = time.monotonic()
    row = {'member': n, 'command': command}
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=20)
        row.update(exit=result.returncode, stderr=result.stderr)
        if result.returncode == 0:
            response = json.loads(result.stdout)
            keys = [base64.b64decode(kv['key'], validate=True).decode() for kv in response.get('kvs', [])]
            assert response['count'] == len(keys), 'Incomplete registry key listing'
            leftovers = [key for key in keys if any(ns in key.split('/') for ns in namespaces)]
            row.update(registry_keys=len(keys), remaining_workload_keys=leftovers,
                       header=response['header'], passed=not leftovers)
        else:
            row['passed'] = False
    except (subprocess.TimeoutExpired, ValueError, KeyError, AssertionError) as exc:
        row.update(error=str(exc), passed=False)
    row['elapsed_seconds'] = time.monotonic() - start
    rows.append(row)
(out / 'storage-delete-verification.json').write_text(json.dumps(rows, indent=2) + '\n')
sys.exit(0 if all(row['passed'] for row in rows) else 1)
