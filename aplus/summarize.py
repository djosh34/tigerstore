#!/usr/bin/env python3
"""Summarizes a bench/run.sh result directory of the A+ prototype (#133):
throughput, latency, and bytes written per write by component.

    aplus/summarize.py <results dir>/<bench>
"""
import glob
import json
import os
import re
import statistics
import sys

d = sys.argv[1]
rows = {}
for f in sorted(glob.glob(os.path.join(d, "*-*-*.json"))):
    base = os.path.basename(f)[:-5]
    target, mode, rep = base.rsplit("-", 2)
    r = json.load(open(f))
    res = open(os.path.join(d, base + ".resources")).read()
    io = [int(x) for x in re.findall(r"^io \d+ (\d+)$", res, re.M)]
    rev = int(re.search(r"^revision (\d+)$", res, re.M).group(1))
    stats = []
    for n in (1, 2, 3):
        lines = [l for l in open(os.path.join(d, f"{base}-n{n}.log")) if "aplus-stats" in l]
        if lines:
            stats.append(dict((k, int(v)) for k, v in re.findall(r"(\w+)=(\d+)", lines[-1].split("aplus-stats")[1])))
    rows.setdefault((target, mode), []).append((r, io, rev, stats))


def med(xs):
    return statistics.median(xs) if xs else float("nan")


print("| target | mode | reps | throughput median (min-max) | p50 ms | p99 ms | device write B/write/node | log | ckpt leaf (of it history) | ckpt interior+root | cleaning |")
print("|---|---|---|---|---|---|---|---|---|---|---|")
for (target, mode), rs in sorted(rows.items()):
    tp = [r["throughput"] for r, *_ in rs]
    p50 = [r["latency_us"]["p50"] / 1000 for r, *_ in rs]
    p99 = [r["latency_us"]["p99"] / 1000 for r, *_ in rs]
    wb = [sum(io) / len(io) / (rev - 1) for _, io, rev, _ in rs]
    comp = {}
    for _, _, rev, stats in rs:
        for k in ("log", "ckpt_leaf", "ckpt_leaf_hist", "ckpt_interior", "ckpt_root", "ckpt_metamove", "clean"):
            if stats:
                comp.setdefault(k, []).append(sum(s.get(k, 0) for s in stats) / len(stats) / (rev - 1))
    c = lambda k: f"{med(comp[k]):.0f}" if k in comp else "-"
    leaf = f"{c('ckpt_leaf')} ({c('ckpt_leaf_hist')})" if comp else "-"
    inter = f"{med(comp['ckpt_interior']) + med(comp['ckpt_root']):.0f}" if comp else "-"
    valid = all(r["valid"] for r, *_ in rs)
    print(f"| {target} | {mode} | {len(rs)}{'' if valid else ' (some invalid)'} | {med(tp):,.0f} ({min(tp):,.0f}-{max(tp):,.0f}) | {med(p50):.2f} | {med(p99):.2f} | {med(wb):.0f} | {c('log')} | {leaf} | {inter} | {c('clean')} |")
