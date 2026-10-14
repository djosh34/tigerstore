#!/usr/bin/env python3
# Question (#134): per bench run, what did the disk and the persister do in
# the measured 10 s window? Usage: analyze_bench.py <out/put or out/txn>
import json, sys, glob, os, statistics as st

d = sys.argv[1]
rows = []
for jf in sorted(glob.glob(f"{d}/*-*-[0-9].json")):
    name = os.path.basename(jf)[:-5]
    layout, binname, rep = name.split("-")
    r = json.load(open(jf))
    t1 = int(open(f"{d}/{name}.t1").read())
    lo, hi = t1 - 10.5e9, t1 - 0.5e9
    samples = []
    for line in open(f"{d}/{name}.csv"):
        f = line.split()
        if len(f) < 18:
            continue
        samples.append([int(x) for x in f])
    win = [s for s in samples if lo <= s[0] <= hi]
    a, b = win[0], win[-1]
    dt = (b[0] - a[0]) / 1e9
    dev = lambda i: b[1 + i] - a[1 + i]
    nd = len(a) - 1 - 3
    ops = r["throughput"] * dt
    cores = [(b[-3 + k] - a[-3 + k]) / 100 / dt for k in range(3)]
    row = dict(name=name, layout=layout, bin=binname, rep=int(rep), tput=r["throughput"], valid=r["valid"],
               p50=r["latency_us"]["p50"] / 1e3, p99=r["latency_us"]["p99"] / 1e3,
               cpu_us=r.get("server_cpu_us_per_op_by_node"), cores=r.get("server_cores_by_node"),
               dev_MiBps=dev(6) * 512 / dt / 2**20, dev_wps=dev(4) / dt, dev_flush_s=dev(15) / dt,
               util=dev(9) / 1e3 / dt, dev_B_per_op=dev(6) * 512 / ops if ops else 0,
               avg_wr_KiB=dev(6) * 512 / max(1, dev(4)) / 1024, cores_win=cores,
               wms_per_s=dev(7) / dt, fms_per_s=dev(16) / dt, inflight=b[1 + 8])
    # log bytes per write, per node
    wal = []
    for i in (1, 2, 3):
        w = json.load(open(f"{d}/{name}.wal-n{i}.json"))
        writes = sum(w["Writes"].values())
        wal.append(dict(file=w["FileBytes"], writes=writes, per=w["FileBytes"] / max(1, writes),
                        entries=w["EntryRecords"], hs=w["HardStateRecords"],
                        wire=sum(w["WireBytes"].values()) / max(1, writes),
                        meta=sum(w["MetaBytes"].values()) / max(1, writes)))
    row["wal"] = wal
    # leader node index from placement
    pl = json.load(open(f"{d}/{name}.placement"))
    li = int(pl["leader"].split(":")[1][3]) if pl["leader"].endswith("0") else 0
    row["leader_node"] = li
    if binname == "instr":
        for i in (1, 2, 3):
            pst = []
            for line in open(f"{d}/{name}.n{i}.log"):
                if line.startswith("PST ") or " PST " in line:
                    f = line[line.index("PST ") + 4:].split()
                    pst.append([int(x) for x in f])
            pw = [p for p in pst if lo <= p[0] <= hi]
            if len(pw) < 2:
                continue
            p, q = pw[0], pw[-1]
            wall = q[0] - p[0]
            g = q[1] - p[1]
            row[f"pst_n{i}"] = dict(groups_s=g / wall * 1e9, jobs_per_g=(q[2] - p[2]) / max(1, g),
                                     entries_per_g=(q[3] - p[3]) / max(1, g), KiB_per_g=(q[4] - p[4]) / max(1, g) / 1024,
                                     wait=(q[5] - p[5]) / wall, append=(q[6] - p[6]) / wall, write=(q[7] - p[7]) / wall,
                                     sync=(q[8] - p[8]) / wall, report=(q[9] - p[9]) / wall,
                                     sync_ms=(q[8] - p[8]) / max(1, g) / 1e6, write_ms=(q[7] - p[7]) / max(1, g) / 1e6)
    rows.append(row)

json.dump(rows, open(f"{d}/analysis.json", "w"), indent=1)
for r in rows:
    print(f"{r['name']:18} {r['tput']:9.0f}/s p50 {r['p50']:6.1f} p99 {r['p99']:6.1f} ms  dev {r['dev_MiBps']:6.1f} MiB/s {r['dev_wps']:6.0f} w/s {r['dev_flush_s']:5.0f} fl/s util {r['util']:.2f} {r['dev_B_per_op']:6.0f} B/op wrKiB {r['avg_wr_KiB']:6.1f}  cores {[round(c,2) for c in r['cores_win']]}  log B/write {[round(w['per'],1) for w in r['wal']]}")
    for i in (1, 2, 3):
        p = r.get(f"pst_n{i}")
        if p:
            print(f"    n{i}{'L' if i == r['leader_node'] else ' '} groups/s {p['groups_s']:6.1f} KiB/g {p['KiB_per_g']:8.1f} jobs/g {p['jobs_per_g']:5.1f} wait {p['wait']:.2f} append {p['append']:.2f} write {p['write']:.2f} sync {p['sync']:.2f} report {p['report']:.2f} sync {p['sync_ms']:.2f} ms write {p['write_ms']:.2f} ms")

def med(xs):
    return st.median(xs) if xs else float("nan")
print()
for lay in ("shared", "alone", "tmpfs"):
    for bn in ("main", "instr"):
        rs = [r for r in rows if r["layout"] == lay and r["bin"] == bn]
        if not rs:
            continue
        print(f"{lay:7}{bn:6} n={len(rs)} tput med {med([r['tput'] for r in rs]):.0f} range {min(r['tput'] for r in rs):.0f}-{max(r['tput'] for r in rs):.0f}  dev MiB/s med {med([r['dev_MiBps'] for r in rs]):.1f}  util med {med([r['util'] for r in rs]):.2f}  devB/op med {med([r['dev_B_per_op'] for r in rs]):.0f}  leader cores med {med([r['cores_win'][0] for r in rs]):.2f} f {med([r['cores_win'][1] for r in rs]):.2f} {med([r['cores_win'][2] for r in rs]):.2f}")
