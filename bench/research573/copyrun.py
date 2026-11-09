"""copyrun.py dir: memmove bytes per op by layer and by direct caller, per reply byte."""
import json, os, sys
from collections import defaultdict
sys.path.insert(0, "" + __import__("os").path.dirname(__import__("os").path.abspath(__file__)) + "")
from bpfmap import parse
from classify import layer

d = sys.argv[1]
roles = open(os.path.join(d, "roles.txt")).read().split("\n")
lp, fp = roles[0].split()[2], roles[1].split()[2]
res = json.load(open(os.path.join(d, "result.json")))
ops = res["ops"]

def metric(node, name):
    tot = []
    for ph in (0, 1):
        s = 0.0
        for line in open(os.path.join(d, f"node-{node}-{ph}.metrics")):
            if line.startswith(name):
                s += float(line.split()[-1])
        tot.append(s)
    return (tot[1] - tot[0]) / ops

print(f"ops {ops}, {res['throughput']:.0f} ops/s")
for node, pid, who in ((0, lp, "leader"), (1, fp, "follower")):
    sent = metric(node, "etcd_network_client_grpc_sent_bytes_total")
    psent = metric(node, "etcd_network_peer_sent_bytes_total")
    recv = metric(node, "etcd_network_client_grpc_received_bytes_total")
    precv = metric(node, "etcd_network_peer_received_bytes_total")
    wal = metric(node, "etcd_disk_wal_write_bytes_total")
    L = defaultdict(float)
    C = defaultdict(float)
    tot = 0
    for keys, fr, v in parse(os.path.join(d, "copy.txt"), "bytes"):
        if keys[0] != pid:
            continue
        caller = keys[1] if len(keys) > 1 else "?"
        frames = [fr[0], caller] + fr[1:] if fr else [caller]
        l = layer(frames)
        L[l] += v
        C[(caller.replace("github.com/djosh34/tigerstore/internal/", ""), l)] += v
        tot += v
    calls = sum(v for k, _, v in parse(os.path.join(d, "copy.txt"), "ncalls") if k[0] == pid)
    print(f"\n### {who}: memmove {tot/ops:.0f} B/op in {calls/ops:.1f} calls/op; client reply {sent:.0f} B/op, client requests {recv:.0f}, peer sent {psent:.0f}, peer received {precv:.0f}, WAL {wal:.0f}")
    print(f"memmove bytes per client reply byte: {tot/ops/sent:.2f}; per byte the node moves on any socket or to the WAL: {tot/ops/(sent+recv+psent+precv+wal):.2f}")
    print("| layer | B/op | share |\n|---|---:|---:|")
    for k, v in sorted(L.items(), key=lambda x: -x[1]):
        if v / tot >= 0.005:
            print(f"| {k} | {v/ops:.0f} | {100*v/tot:.1f}% |")
    print("| direct caller of memmove | layer | B/op |\n|---|---|---:|")
    for k, v in sorted(C.items(), key=lambda x: -x[1])[:18]:
        print(f"| {k[0]} | {k[1]} | {v/ops:.0f} |")
