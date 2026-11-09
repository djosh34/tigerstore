import sys
from collections import defaultdict
sys.path.insert(0, "" + __import__("os").path.dirname(__import__("os").path.abspath(__file__)) + "")
from traces import parse
import concept
runs = {"leader": [("/tmp/r573/l1.traces", 37.53), ("/tmp/r573/l2.traces", 35.90)],
        "follower": [("/tmp/r573/f1.traces", 32.38), ("/tmp/r573/f2.traces", 31.14)]}
R = {}
for who, lst in runs.items():
    acc = defaultdict(list)
    for p, us in lst:
        s = parse(p); tot = sum(v for v, _ in s)
        c = defaultdict(float)
        for v, f in s: c[concept.part(f)] += v
        for k in set(c) | set(acc):
            acc[k].append((us * c.get(k, 0) / tot, 100 * c.get(k, 0) / tot))
    R[who] = {k: (sum(x[0] for x in v) / len(lst), sum(x[1] for x in v) / len(lst)) for k, v in acc.items()}
keys = sorted(set(R["leader"]) | set(R["follower"]), key=lambda k: -R["leader"].get(k, (0, 0))[0])
print("| part | leader µs/op | leader share | follower µs/op | follower share |\n|---|---:|---:|---:|---:|")
for k in keys:
    a, b = R["leader"].get(k, (0, 0)), R["follower"].get(k, (0, 0))
    print(f"| {k} | {a[0]:.2f} | {a[1]:.1f}% | {b[0]:.2f} | {b[1]:.1f}% |")
print(f"| **all** | **{sum(v[0] for v in R['leader'].values()):.1f}** | | **{sum(v[0] for v in R['follower'].values()):.1f}** | |")
