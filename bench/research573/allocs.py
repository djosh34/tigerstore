"""allocs.py space.traces objects.traces ops: alloc bytes/op and objects/op by layer and by site."""
import sys
from collections import defaultdict
sys.path.insert(0, "" + __import__("os").path.dirname(__import__("os").path.abspath(__file__)) + "")
from traces import parse
from classify import layer

RT = ("runtime.", "internal/runtime", "bytes.growSlice", "bytes.(*Buffer).grow", "slices.Grow", "strings.(*Builder).grow", "slices.Clone", "bytes.Clone", "slices.Insert", "slices.Collect", "slices.AppendSeq", "strings.Clone", "bytes.(*Buffer).Grow", "slices.grow", "bytes.(*Buffer).Write", "reflect.", "bufio.NewWriterSize", "bufio.NewReaderSize")

def site(f):
    for x in f:
        if not x.startswith(RT):
            return x.replace("github.com/djosh34/tigerstore/internal/", "")
    return f[0]

space, objs, ops = parse(sys.argv[1]), parse(sys.argv[2]), float(sys.argv[3])
top = int(sys.argv[4]) if len(sys.argv) > 4 else 30
L = defaultdict(lambda: [0.0, 0.0])
S = defaultdict(lambda: [0.0, 0.0, ""])
for i, (data) in enumerate((space, objs)):
    for v, f in data:
        L[layer(f)][i] += v
        s = site(f)
        S[s][i] += v
        S[s][2] = layer(f)
tb = sum(v for v, _ in space)
to = sum(v for v, _ in objs)
print(f"total {tb/ops:.0f} B/op, {to/ops:.2f} objects/op (sampled estimate)")
print("\n## by layer: B/op, objects/op, share of bytes")
for l, (b, o) in sorted(L.items(), key=lambda x: -x[1][0]):
    print(f"{b/ops:8.0f} {o/ops:6.2f} {100*b/tb:5.1f}%  {l}")
print("\n## by site (bytes)")
for s, (b, o, l) in sorted(S.items(), key=lambda x: -x[1][0])[:top]:
    print(f"{b/ops:8.0f} {o/ops:6.2f} {100*b/tb:5.1f}%  {s}   [{l}]")
print("\n## by site (objects)")
for s, (b, o, l) in sorted(S.items(), key=lambda x: -x[1][1])[:top]:
    print(f"{b/ops:8.0f} {o/ops:6.2f} {100*o/to:5.1f}%  {s}   [{l}]")
