#!/usr/bin/env python3
"""Recompute report tables from final raw JSON lines. No dependencies."""
import json
import statistics
from pathlib import Path

root = Path(__file__).resolve().parent
rows = [json.loads(line) for line in (root / "raw/benchmark.jsonl").read_text().splitlines()]
print("All CPU columns divide by bidirectional plaintext decimal MB.")
print("| Rep | Path | p50 us | p95 us | p99 us | Endpoint CPU ms/MB | Proxy CPU ms/MB | Total CPU ms/MB | Echo request MB/s |")
print("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
for row in rows:
    print(f"| {row['rep']} | {row['path']} | {row['latency_p50_ns']/1000:.3f} | {row['latency_p95_ns']/1000:.3f} | {row['latency_p99_ns']/1000:.3f} | {row['bulk_endpoint_cpu_ns_per_decimal_MB_bidir']/1e6:.3f} | {row['bulk_proxy_cpu_ns_per_decimal_MB_bidir']/1e6:.3f} | {row['bulk_total_cpu_ns_per_decimal_MB_bidir']/1e6:.3f} | {row['bulk_roundtrip_MB_per_s']:.3f} |")
print("\nMedians")
for path in ("direct", "proxy"):
    selected = [row for row in rows if row["path"] == path]
    for name in ("latency_p50_ns", "latency_p95_ns", "latency_p99_ns", "bulk_endpoint_cpu_ns_per_decimal_MB_bidir", "bulk_proxy_cpu_ns_per_decimal_MB_bidir", "bulk_total_cpu_ns_per_decimal_MB_bidir", "bulk_roundtrip_MB_per_s"):
        values = [row[name] for row in selected]
        print(path, name, "median", statistics.median(values), "min", min(values), "max", max(values))
print("\n| Mode | Seed 49 wall ms, 3 runs | Seed 50 wall ms, 3 runs |")
print("| --- | --- | --- |")
by_seed = {}
for seed in (49, 50):
    cases = [json.loads(line) for line in (root / f"raw/exercise-seed{seed}.jsonl").read_text().splitlines()]
    assert len(cases) == 39 and all(row["pass"] for row in cases)
    by_seed[seed] = cases
for mode in [row["mode"] for row in by_seed[49] if row["rep"] == 1]:
    outputs = []
    for seed in (49, 50):
        cases = [row for row in by_seed[seed] if row["mode"] == mode]
        assert len(set(row["policy_trace_sha256"] for row in cases)) == 1, (mode, seed)
        outputs.append(" / ".join(f"{row['wall_ns']/1e6:.3f}" for row in cases))
    print(f"| {mode} | {outputs[0]} | {outputs[1]} |")
print("All 78 cases passed. Each mode's canonical policy hash matched across its three runs for each seed.")
