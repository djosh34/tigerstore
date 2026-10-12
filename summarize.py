import collections
import json
import statistics

rows = [json.loads(line) for line in open('results.jsonl')]
groups = collections.defaultdict(list)
for row in rows:
    groups[row['EntryBytes'], row['Batch'], row['Transport']].append(row)
print('| Entry KiB | Batch | Transport | k messages/s | Payload MB/s | RTT p50/p99 us | CPU us/message | k messages/s min to max |')
print('|---:|---:|:---|---:|---:|---:|---:|---:|')
for (entry, batch, transport), runs in sorted(groups.items()):
    assert len(runs) == 5
    def median(key):
        return statistics.median(row[key] for row in runs)
    rates = [row['MessagesPerSec'] / 1000 for row in runs]
    print(f'| {entry // 1024} | {batch} | {transport} | '
          f'{median("MessagesPerSec") / 1000:.2f} | '
          f'{median("PayloadBytesPerSec") / 1e6:.1f} | '
          f'{median("P50US"):.1f}/{median("P99US"):.1f} | '
          f'{median("CPUUSPerMessage"):.1f} | '
          f'{min(rates):.2f} to {max(rates):.2f} |')
