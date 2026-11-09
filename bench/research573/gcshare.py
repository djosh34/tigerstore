"""gcshare.py cpu.traces us_per_op space.traces objects.traces ops

Per owner group: allocated B/op and objects/op, the direct CPU of allocating
(mallocgc etc. under that group's stacks), and the group's share of GC CPU
charged by its share of allocated bytes (GC here runs because the heap
reaches the memory limit, so its cycles scale with bytes allocated)."""
import sys
from collections import defaultdict
sys.path.insert(0, "" + __import__("os").path.dirname(__import__("os").path.abspath(__file__)) + "")
from traces import parse
from classify import layer, kind
import concept

def group(l):
    if l.startswith("client:") or l.startswith("watch"):
        return "client gRPC, HTTP/2, protobuf: requests, unary replies, watch"
    if l == "LIST stream: protobuf encode, gRPC framing":
        return "client gRPC + protobuf: LIST reply encoding (server.marshal)"
    if l.startswith("LIST stream") or l in ("committer: reads - index lookup/walk", "committer: reads - value fetch + CRC", "committer: reads - reply build, other"):
        return "reads: index walk results, KeyValue building, value arenas"
    if l.startswith("committer: apply") :
        return "apply: index update, decode, lease tracking, watches"
    if l == "committer: value cache insert/evict" or l.startswith("cold value"):
        return "value cache fill + cold reads"
    if l in ("committer: Raft", "committer: loop, completions, other", "committer: hand to peers", "committer: submit checks, queue"):
        return "Raft, entry encode, submit, completions"
    if l.startswith("peer"):
        return "peer traffic (replication, forwarded writes)"
    if l.startswith("log"):
        return "WAL"
    if l.startswith("GC") or l.startswith("runtime"):
        return "GC itself"
    return "other"

cpu, us, sp, ob, ops = parse(sys.argv[1]), float(sys.argv[2]), parse(sys.argv[3]), parse(sys.argv[4]), float(sys.argv[5])
tot = sum(v for v, _ in cpu)
gc = 0.0
alloc = defaultdict(float)
for v, f in cpu:
    p = concept.part(f)
    if p.startswith("GC"):
        gc += v
    elif p.startswith("allocation"):
        alloc[group(layer(f))] += v
B, O = defaultdict(float), defaultdict(float)
for v, f in sp: B[group(layer(f))] += v
for v, f in ob: O[group(layer(f))] += v
tb = sum(B.values())
gcus = us * gc / tot
print(f"GC {gcus:.2f} us/op, allocation {us*sum(alloc.values())/tot:.2f} us/op")
print("| part | B/op | objects/op | share of bytes | allocation µs/op | GC µs/op charged by bytes | together µs/op |\n|---|---:|---:|---:|---:|---:|---:|")
for k in sorted(B, key=lambda k: -B[k]):
    a = us * alloc[k] / tot
    g = gcus * B[k] / tb
    if B[k] / ops < 1 and a < 0.01:
        continue
    print(f"| {k} | {B[k]/ops:.0f} | {O[k]/ops:.2f} | {100*B[k]/tb:.1f}% | {a:.2f} | {g:.2f} | {a+g:.2f} |")
print(f"| all | {tb/ops:.0f} | {sum(O.values())/ops:.2f} | | {us*sum(alloc.values())/tot:.2f} | {gcus:.2f} | {us*sum(alloc.values())/tot+gcus:.2f} |")
