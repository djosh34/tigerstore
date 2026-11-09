"""drill.py traces anchor depth: share of samples whose stack contains anchor,
grouped by the `depth` frames called beneath the anchor (toward the leaf)."""
import sys
from collections import Counter
sys.path.insert(0, "" + __import__("os").path.dirname(__import__("os").path.abspath(__file__)) + "")
from traces import parse

path, anchor, depth = sys.argv[1], sys.argv[2], int(sys.argv[3])
s = parse(path)
tot = sum(v for v, _ in s)
c = Counter()
sub = 0
for v, f in s:
    idx = [i for i, x in enumerate(f) if anchor in x]
    if not idx:
        continue
    i = idx[-1]  # outermost occurrence
    sub += v
    below = f[max(0, i - depth):i]
    c[" > ".join(reversed(below)) or "(self)"] += v
print(f"anchor {anchor}: {100*sub/tot:.2f}% of {tot:.0f}")
for k, v in c.most_common(int(sys.argv[4]) if len(sys.argv) > 4 else 30):
    print(f"{100*v/tot:6.2f}%  {k}")
