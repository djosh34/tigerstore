"""#577 budget: least leader CPU per mixed-k8s op, from the mix and measured primitives.

Usage: python3 -I model.py <primitives.jsonl>... [--turn N] [--leader-node K]
Counts come from the mix (cmd/loadgen/mixed.go), not from a profile.
"""
import argparse
import json
import statistics as st

ap = argparse.ArgumentParser()
ap.add_argument("files", nargs="+")
ap.add_argument("--turn", type=float, default=64, help="global ops per event-loop turn (batch)")
ap.add_argument("--leader-node", type=int, default=0, help="the node whose watches this node serves (0, 1, 2)")
ap.add_argument("--role", default="leader", choices=["leader", "follower"])
ap.add_argument("--index-point-ns", type=float, default=None, help="point lookup+update floor from the index helper")
ap.add_argument("--index-ordered-ns", type=float, default=None, help="ordered insert/delete floor")
ap.add_argument("--raft-batch-ns", type=float, default=None, help="leader Raft bookkeeping per batch")
a = ap.parse_args()

prim = {}
counts = None
for f in a.files:
    for l in open(f):
        d = json.loads(l)
        if d.get("Kind") == "primitive":
            prim.setdefault((d["Name"], d["Bytes"]), []).append(d["CPU_ns"])
        elif d.get("kind") == "counts":
            counts = d
P = {k: st.median(v) for k, v in prim.items()}


def p(name, size=0):
    return P[(name, size)]


def lin(name, s1, s2):
    """per-call and per-byte cost from two sizes"""
    c1, c2 = p(name, s1), p(name, s2)
    perb = (c2 - c1) / (s2 - s1)
    return c1 - perb * s1, perb


ops = counts["by_operation"]
W = sum(ops[o]["Weight"] for o in ("update", "create", "delete"))
req_w = sum(ops[o]["Request"] for o in ("update", "create", "delete"))
req_r = sum(ops[o]["Request"] for o in ("get", "rev", "list"))
reply = sum(ops[o]["Reply"] for o in ops)
durable = sum(ops[o]["Write"] for o in ops)
kvs = sum(ops[o]["Keys"] for o in ops)
watch = counts["watch_bytes_by_node"][a.leader_node]
watch_events = counts["watch_events_by_node"][a.leader_node]
direct = 1 / 3
creates_deletes = ops["create"]["Weight"] + ops["delete"]["Weight"]
point_ops = ops["get"]["Weight"] * direct + ops["rev"]["Weight"] * direct + W  # leader applies every write
lists = ops["list"]["Weight"] * direct
calls = direct  # client calls on the leader per global op
H2 = 9 + 6 + 9 + 5  # HEADERS frame + indexed block, DATA frame, gRPC prefix
H2R = 9 + 2 + 9 + 5 + 9 + 3  # reply: HEADERS, DATA, gRPC prefix, trailers
PEER = 16  # peer frame and entry header per forwarded or replicated write (guess)

if a.role == "leader":
    bytes_in = {
        "client requests (1/3 of calls)": direct * (req_w + req_r) + calls * H2,
        "forwarded writes from followers (2/3 of writes)": (2 / 3) * req_w + (2 / 3) * W * PEER,
    }
    bytes_out = {
        "client replies (1/3 of calls)": direct * reply + calls * H2R,
        "replication to 2 followers": 2 * (req_w + W * PEER),
        f"watch events (node {a.leader_node})": watch + watch_events * 40,
    }
else:
    bytes_in = {
        "client requests (1/3 of calls)": direct * (req_w + req_r) + calls * H2,
        "replicated entries from the leader (all writes)": req_w + W * PEER,
    }
    bytes_out = {
        "client replies (1/3 of calls)": direct * reply + calls * H2R,
        "writes forwarded to the leader (1/3 of writes)": (1 / 3) * (req_w + W * PEER),
        f"watch events (node {a.leader_node})": watch + watch_events * 40,
    }
wal = req_w + W * PEER
# Primitive costs (ns).
seal_rec, seal_b = lin("tls13_record_seal", 1024, 16384)
open_rec, open_b = lin("tls13_record_open", 1024, 16384)
crc_b = (p("crc32c", 262144) - p("crc32c", 1024)) / (262144 - 1024)
copy_b = (p("copy", 1048576) - p("copy", 65536)) / (1048576 - 65536)
send_call, send_b = lin("tcp_send", 4096, 65536)
recv_call, recv_b = lin("tcp_receive", 4096, 65536)
disk_b = (p("disk_odirect_prealloc_fdatasync", 1048576) - p("disk_odirect_prealloc_fdatasync", 65536)) / (1048576 - 65536)
disk_b_buf = (p("disk_pwrite_append_nosync", 1048576) - p("disk_pwrite_append_nosync", 65536)) / (1048576 - 65536)
sync_call = p("disk_odirect_prealloc_fdatasync", 65536) - 65536 * disk_b
proto_parse = p("protobuf_request_alias_mixed")  # every request of the mix, alias parser
hpack = p("hpack_floor_compare_and_constant", 6)
h2 = p("h2_grpc_parse", 1024) + p("h2_grpc_build", 1024)
kv_meta = p("protobuf_kv_metadata", 51)
quorum = p("quorum_three_indexes") + 2 * p("peer_ack_encode_parse", 24)

T = a.turn
REC = 16384
# sockets touched per turn: 2 client conns (recv, send), 2 peers (recv, send), the leader's watch conns (send), disk.
watch_conns = 9
rows = []


def row(part, count, unit, ns):
    rows.append((part, count, unit, ns))


sealed = sum(bytes_out.values())
opened = sum(bytes_in.values())
row("TLS open (client + peer bytes in)", opened, "B", opened * open_b + opened / REC * open_rec)
row("TLS seal (all bytes out)", sealed, "B", sealed * seal_b + sealed / REC * seal_rec)
row("h2 frames, HPACK (precomputed), gRPC prefix", calls, "calls", calls * (h2 + hpack))
row("protobuf parse, no copy", direct, "requests", direct * proto_parse + (2 / 3) * 0)  # forwarded writes parsed by follower; leader re-reads keys at apply (below)
row("protobuf encode reply metadata (values not copied)", direct * kvs + watch_events * 2 + direct, "KVs", (direct * kvs + watch_events * 2 + direct) * kv_meta)
# From the index helper (run 42697278993, 1M keys): Go map / flat hash lookup 346 ns;
# google/btree insert 1195, delete 1530; sorted-array seek + 500 successors 1941 ns
# (stands for a B+tree leaf walk: a guess); dependent DRAM miss 148 ns (512 MiB).
LOOKUP, BT_INS, BT_DEL, LIST_WALK, MISS = 346, 1195, 1530, 1941, 148
row("index point lookup (hash, 1M keys)", point_ops, "lookups", point_ops * LOOKUP)
row("ordered index insert (creates) and delete (deletes)", creates_deletes, "ops", ops["create"]["Weight"] * (BT_INS + 183) + ops["delete"]["Weight"] * (BT_DEL + 408))
row("LIST seek + walk of one page", lists, "pages", lists * LIST_WALK)
reads_kv = direct * kvs + watch_events  # values read cold for replies and prev_kv of events
row("cold value reads (1 miss per value)", reads_kv, "values", reads_kv * MISS)
row("watch matching (prefix to watchers)", W, "writes", W * 10)
row("value copy into value store (writes)", durable, "B", durable * copy_b)
row("WAL CRC32C", wal, "B", wal * crc_b)
row("WAL write, O_DIRECT (per byte)", wal, "B", wal * disk_b)
row("fdatasync (per batch)", W / T, "syncs", (W / T) * sync_call)
nsend = (2 + 2 + min(watch_conns, watch_events * T)) / T
nrecv = (2 + 2) / T
row("socket sends (per call, per turn)", nsend, "calls", nsend * send_call)
row("socket send bytes (loopback, per byte)", sealed, "B", sealed * send_b)
row("socket receives (per call, per turn)", nrecv, "calls", nrecv * recv_call)
row("socket receive bytes", opened, "B", opened * recv_b)
row("replication bookkeeping, own (per batch)", 1 / T, "batches", 4 / T)
row("ReadIndex for follower reads (per batch, own: one heartbeat)", 1 / T, "rounds", 0)

print(f"primitives: seal {seal_rec:.0f} ns/record + {seal_b:.3f} ns/B; open {open_rec:.0f} + {open_b:.3f}; crc32c {crc_b:.3f} ns/B; copy {copy_b:.3f} ns/B; "
      f"send {send_call:.0f} ns/call + {send_b:.3f} ns/B; recv {recv_call:.0f} + {recv_b:.3f}; O_DIRECT {disk_b:.3f} ns/B, buffered pwrite {disk_b_buf:.3f} ns/B; fdatasync {sync_call:.0f} ns/call")
print(f"\nBytes per global op ({a.role})")
print(f"| disk | WAL | {wal:.0f} |")
for k, v in bytes_in.items():
    print(f"| in | {k} | {v:.0f} |")
for k, v in bytes_out.items():
    print(f"| out | {k} | {v:.0f} |")
print(f"\nBudget per global op, turn = {T:g} global ops")
tot = 0
for part, c, u, ns in rows:
    tot += ns
    print(f"| {part} | {c:.3g} {u} | {ns/1000:.2f} |")
print(f"| total | | {tot/1000:.2f} |")
print(f"for comparison, etcd raft v3.7.0 per batch of one entry of {T*1040/1024:.0f} KiB: 5.5 us at 1 KiB, 13.9 at 64 KiB, 37 at 512 KiB (helper run)")
