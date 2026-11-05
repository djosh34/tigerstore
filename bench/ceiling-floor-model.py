#!/usr/bin/env python3
"""An optimistic component sum, NOT a measured end-to-end or universal floor."""
import json, statistics, sys
r = json.load(open(sys.argv[1]))
print('| workload | runner | batch | frontend us/op | backend primitives us/op | peer AEAD+ACK us/op | sum us/op | reciprocal ops/s | old leader minus sum us/op |')
print('|---|---|---:|---:|---:|---:|---:|---:|---:|')
for runner in ['1', '2']:
    def med(name):
        return statistics.median(v['ns'] for v in r['primitives']['BenchmarkFloor' + name][runner]) / 1000
    for wl, old in [('put', 7.573802838278819), ('txn', 10.698665662418582)]:
        front = statistics.median(v['server_cpu_us_per_op'] for v in r['nulls']['null-0/' + wl][runner])
        for batch in [64, 256, 1536]:
            suffix = '/b' + str(batch)
            # Fixture format 3: 9-byte origin, 1-byte boot, 3-byte IDs.
            bytes_op = (1090 if wl == 'put' else 1208) + (3 if batch == 64 else 4) / batch
            back = sum([med('Encode/' + wl + suffix), med('Raft' + suffix),
                        med('Record/' + wl + suffix), 2 * med('Peer/' + wl + suffix),
                        med('Apply/' + wl), med('Decode/' + wl + '/real'),
                        med('LogWrite' + suffix)])
            crypto = 2 * bytes_op / 16384 * med('GCM/seal/n16384')
            crypto += 2 / batch * (med('Peer/ack_decode') + med('GCM/open/n80'))
            total = front + back + crypto
            print(f'| {wl} | {runner} | {batch} | {front:.3f} | {back:.3f} | {crypto:.3f} | {total:.3f} | {1e6/total:.0f} | {old-total:.3f} |')
