"""Read a downloaded pre50-isolated or pre50-leader2 artifact; print JSON."""
import json
from pathlib import Path
import re
import sys

root = Path(sys.argv[1])
rows = []
for path in sorted(root.glob('rep-*')):
    row = json.loads((path / 'summary.json').read_text())
    text = (path / 'benchmark.txt').read_text()
    assert 'Error distribution:' not in text
    row['mean_ms'] = float(re.search(r'Average:\s+([\d.]+)', text)[1]) * 1000
    for pct in ['50', '99', '99.9']:
        row['p' + pct + '_ms'] = float(re.search(r' ' + re.escape(pct) + r'% in ([\d.]+)', text)[1]) * 1000
    vals = (path / 'generator-time.txt').read_text().split()
    n = row['successful_operations']
    row['generator_cpu_us_per_op'] = (float(vals[0]) + float(vals[1])) * 1e6 / n
    row['server_occupied_cores'] = [x * n / 1e6 / row['cpu_window_seconds'] for x in row['server_cpu_us_per_op']]
    row['prometheus_cpu_us_per_op'] = []
    row['leader_changes_during_window'] = []
    row['wal'] = []
    for i in range(3):
        metrics = []
        for when in ['before', 'after']:
            metrics.append({k: float(v) for k, v in
                (line.rsplit(' ', 1) for line in (path / f'metrics-{when}-n{i}.txt').read_text().splitlines()
                 if line and not line.startswith('#'))})
        def delta(key):
            return metrics[1][key] - metrics[0][key]
        row['prometheus_cpu_us_per_op'].append(delta('process_cpu_seconds_total') * 1e6 / n)
        row['leader_changes_during_window'].append(delta('etcd_server_leader_changes_seen_total'))
        count = delta('etcd_disk_wal_fsync_duration_seconds_count')
        row['wal'].append({'observations': count, 'writes_per_observation': n / count,
            'mean_wall_us': delta('etcd_disk_wal_fsync_duration_seconds_sum') * 1e6 / count})
    a = json.loads((path / 'status-before.json').read_text())
    b = json.loads((path / 'status-after.json').read_text())
    row['leader_revision_delta'] = b[0]['Status']['header']['revision'] - a[0]['Status']['header']['revision']
    assert row['leader_revision_delta'] == n
    assert row['leader_changes_during_window'] == [0, 0, 0]
    rows.append(row)
assert len(rows) == 5
print(json.dumps(rows, indent=2))
