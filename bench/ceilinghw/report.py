#!/usr/bin/env python3
"""All phase latencies explicitly labelled; cycle percentiles are measured, not added."""
import json,pathlib,sys
p=pathlib.Path(sys.argv[1])
def load(f):return json.loads(f.read_text())
print('# Hardware raw limits\n\nMiB=2^20 bytes. 2 s total-cycle companion; fio 4 s; no warmup. Full per-writer fio phase latencies in JSON.\n')
print('| Durable case | writer | MiB/s | cycle p50 us | p99 us | p99.9 us |\n|---|---:|---:|---:|---:|---:|')
for f in sorted(p.glob('cycle-*.jsonl')):
 for line in f.read_text().splitlines():
  r=json.loads(line)
  if 'writer' in r:print(f'| {f.stem[6:]} | {r["writer"]} | {r["mib_per_s"]} | {r["p50_us"]} | {r["p99_us"]} | {r["p999_us"]} |')
print('\n| Sequential | MiB/s |\n|---|---:|')
for f in sorted(p.glob('fio-seq-*.json')):
 r=load(f)['jobs'][0];kind='read' if '-read-' in f.name else 'write';print(f'| {f.stem} | {r[kind]["bw_bytes"]/2**20:.3f} |')
print('\n```jsonl\n'+(p/'diskbench.jsonl').read_text()+'```')
print('\n| Memory case | useful MiB/s | minimum traffic MiB/s |\n|---|---:|---:|')
for f in sorted(p.glob('memory-*.json')):
 r=load(f);print(f'| {f.stem} | {r["useful_mib_per_s"]} | {r["minimum_traffic_mib_per_s"]} |')
print('\n| Ping case | p50 us | p99 us | samples |\n|---|---:|---:|---:|')
for f in sorted(p.glob('ping-*.json')):
 r=load(f);print(f'| {f.stem} | {r["p50_us"]} | {r["p99_us"]} | {r["n"]} |')
print('\n| TCP stream case | MiB/s |\n|---|---:|')
for f in sorted(p.glob('iperf-r*.json')):
 r=load(f);print(f'| {f.stem} | {r["end"]["sum_received"]["bits_per_second"]/8/2**20:.3f} |')
for f in sorted(p.glob('tls-stream-*.json')):
 r=load(f);print(f'| {f.stem} | {r["mib_per_s"]:.3f} |')
print('\n| UDP small packet case | delivered packets/s | lost percent | sender CPU percent | receiver CPU percent |\n|---|---:|---:|---:|---:|')
for f in sorted(p.glob('udp-r*.json')):
 r=load(f)['end'];s=r.get('sum_received',r.get('sum'));c=r['cpu_utilization_percent'];print(f'| {f.stem} | {(s["packets"]-s["lost_packets"])/s["seconds"]:.3f} | {s["lost_percent"]:.3f} | {c["host_total"]:.2f} | {c["remote_total"]:.2f} |')
print('\n## Primitive microbenchmarks\n')
for f in sorted(p.glob('primitives-*.txt')):print(f'### {f.name}\n```\n{f.read_text()}\n```')
