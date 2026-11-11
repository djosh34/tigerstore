#!/usr/bin/env python3
# Summarise "pong in X via Y" lines from tailcat ping on stdin (#630).
import re, sys, collections
v, via = [], collections.Counter()
for line in sys.stdin:
    m = re.match(r"pong in ([\d.]+)(µs|ms|s) via (\S+)", line)
    if not m:
        continue
    x = float(m[1]) * {"µs": 0.001, "ms": 1, "s": 1000}[m[2]]
    v.append(x); via[m[3]] += 1
v.sort()
q = lambda f: v[min(len(v) - 1, int(f * len(v)))]
if not v:
    print("n=0"); sys.exit()
print(f"n={len(v)} min={v[0]:.1f} p50={q(.5):.1f} p90={q(.9):.1f} p99={q(.99):.1f} max={v[-1]:.1f} ms; over 3x p50: {sum(x > 3*q(.5) for x in v)}; via {dict(via)}")
