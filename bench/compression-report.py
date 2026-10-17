"""Print the compression pair's measurements. Run only on Actions."""
import json
import pathlib
import statistics
import sys

root = pathlib.Path(sys.argv[1])
all_runs = {}
print("| Values | Store | Mode | Put/s min / median / max | p99 ms min / median / max | Physical B/Put min / median / max |")
print("| --- | --- | --- | ---: | ---: | ---: |")
for content in ("real", "random"):
    for target in ("etcd", "baseline", "candidate"):
        for mode in ("max", "rate"):
            paths = sorted((root / f"put-k8s{content}").glob(f"{target}-{mode}-*.json"))
            runs = [json.loads(p.read_text()) for p in paths]
            if not runs or any(not r["valid"] for r in runs):
                raise ValueError(f"missing or invalid runs: {content}/{target}/{mode}")
            all_runs[content, target, mode] = runs
            fields = ([r["throughput"] for r in runs], [r["latency_us"]["p99"] / 1000 for r in runs], [r["disk_bytes_per_op"] for r in runs])
            cells = [" / ".join(f"{x:.3f}" for x in (min(xs), statistics.median(xs), max(xs))) for xs in fields]
            print(f"| {content} | {target} | {mode} | " + " | ".join(cells) + " |")
print("\nBytes are block-device writes over the measurement window, summed across all three replicas, including filesystem journal and any other runner writes. They are not compressed payload sizes or per-replica bytes. Each operation is one Put RPC. Rate is half etcd's median max rate for that value set.")
for content in ("real", "random"):
    for mode in ("max", "rate"):
        base = all_runs[content, "baseline", mode]
        candidate = all_runs[content, "candidate", mode]
        if len(base) != len(candidate):
            raise ValueError("unequal repetition counts")
        rate_regresses = statistics.median(r["throughput"] for r in candidate) < min(r["throughput"] for r in base)
        tail_regresses = statistics.median(r["latency_us"]["p99"] for r in candidate) > max(r["latency_us"]["p99"] for r in base)
        print(f"\n{content}, {mode}, {len(base)} repetitions: throughput regression={rate_regresses}, p99 regression={tail_regresses}. Candidate median compared with baseline minimum throughput and maximum p99, as #144 defines it.")
