#!/usr/bin/env python3
# Question (#134): per layout, mode, group size and writer count, the median
# over repetitions of bandwidth, latency and device bytes. Usage: analyze_disk.py cells.jsonl
import json, sys, statistics as st
from collections import defaultdict

g = defaultdict(list)
for line in open(sys.argv[1]):
    c = json.loads(line)
    g[(c["Layout"], c["Mode"], c["Group"], c["Writers"])].append(c)
print("layout mode group writers | MiB/s med [reps] | ops/s | op p50/p99 us | sync p50/p99 us | dev amp | util")
for k in sorted(g, key=lambda k: (k[0], k[1], k[3], k[2])):
    cs = g[k]
    m = lambda f: st.median([c[f] for c in cs])
    reps = "/".join(f"{c['MiBps']:.0f}" for c in sorted(cs, key=lambda c: c["Rep"]))
    print(f"{k[0]:6} {k[1]:16} {k[2]>>10:5}K w{k[3]} | {m('MiBps'):7.1f} [{reps}] | {m('OpsPerS'):7.0f} | {m('OpP50'):7.0f} {m('OpP99'):7.0f} | {m('SyncP50'):7.0f} {m('SyncP99'):7.0f} | {m('DevAmp'):.3f} | {m('DevUtil'):.2f}")
