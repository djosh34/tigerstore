# THROWAWAY PROTOTYPE (#630). Prints a markdown summary of res-*/ dirs.
import glob, json, os, sys

root = sys.argv[1]


def load(p):
    try:
        return json.load(open(p))
    except Exception:
        return None


def mbit(stats):
    if not stats or not stats.get("duration"):
        return None
    return stats["bytes"] * 8 / (stats["duration"] / 1e9) / 1e6


def ms(ns):
    return f"{ns / 1e6:.2f}" if ns is not None else "-"


print("## tailcat between GitHub Actions runners (#630 prototype)\n")
print("| node | label | arch | cores | cpu | mem GB | IMDS location / size | local IPs |")
print("|---|---|---|---|---|---|---|---|")
for d in sorted(glob.glob(f"{root}/res-*")):
    i = load(f"{d}/info.json") or {}
    imds = i.get("imds", {})
    loc = f'{imds.get("location")} / {imds.get("vmSize")}' if "location" in imds else imds.get("error", "-")[:40]
    print(f'| {i.get("node")} | {i.get("label")} | {i.get("arch")} | {i.get("cores")} | {i.get("cpu")} | {i.get("mem_gb")} | {loc} | {" ".join(i.get("local_ips", []))} |')

for d in sorted(glob.glob(f"{root}/res-*")):
    n = d.rsplit("-", 1)[1]
    for t in sorted(glob.glob(f"{d}/to*")):
        j = t.rsplit("to", 1)[1]
        print(f"\n### node{n} → node{j}\n")
        pud = open(f"{t}/ping-until-direct.txt").read() if os.path.exists(f"{t}/ping-until-direct.txt") else "-"
        print("```\n" + pud.strip() + "\n```")
        print("| test | Mbit/s | rtt under load min/avg/max ms | extra |")
        print("|---|---|---|---|")
        for name in ["tcp1", "tcp4", "tcp1rev", "tcp4rev", "udp"]:
            r = load(f"{t}/perf-{name}.json")
            if not r:
                err = open(f"{t}/perf-{name}.err").read().strip().splitlines()[-1:] if os.path.exists(f"{t}/perf-{name}.err") else ["not run"]
                print(f"| perf {name} | - | - | {err[0][:120] if err else ''} |")
                continue
            recv = r.get("serverReceived") or {}
            if r.get("clientReceived"):
                recv = r["clientReceived"]
            rtt = r.get("rtt") or {}
            extra = f'path {"direct " + r["path"].get("endpoint", "") if r["path"]["direct"] else "DERP"}'
            if r.get("pathAfter") and r["pathAfter"]["direct"] != r["path"]["direct"]:
                extra += " (changed)"
            if name == "udp":
                sent = (r.get("clientSent") or {}).get("datagrams", 0)
                got = recv.get("datagrams", 0)
                extra += f', loss {100 * (sent - got) / sent if sent else 0:.2f}%, jitter {ms(recv.get("jitter"))} ms, reordered {recv.get("reordered", 0)}'
            m = mbit(recv)
            print(f'| perf {name} | {m:.0f} | {ms(rtt.get("min"))}/{ms(rtt.get("avg"))}/{ms(rtt.get("max"))} | {extra} |' if m else f"| perf {name} | - | - | {extra} |")
        for name in ["iperf1", "iperf4", "iperf8", "iperf1rev"]:
            r = load(f"{t}/{name}.json")
            if not r or "end" not in r:
                print(f"| {name} (forward) | - | - | {'error' if os.path.exists(f'{t}/{name}.json') else 'not run'} |")
                continue
            e = r["end"]
            print(f'| {name} (forward) | {e["sum_received"]["bits_per_second"] / 1e6:.0f} | - | retransmits {e["sum_sent"].get("retransmits", "-")} |')
        for name in ["echo-64", "echo-4k"]:
            r = load(f"{t}/{name}.json")
            if r:
                print(f'| {name} RTT (ms) | - | p50 {r["p50"]} p90 {r["p90"]} p99 {r["p99"]} p99.9 {r["p999"]} max {r["max"]} | n={r["n"]} |')
        s = load(f"{t}/stab-echo.json")
        if s:
            print(f'\n**Stability ({s.get("secs")} s, 20 Hz 64 B echo, all pairs at once):** n={s.get("n")} p50 {s.get("p50")} p99 {s.get("p99")} p99.9 {s.get("p999")} max {s.get("max")} ms; >10 ms: {s.get("over_10ms")}, >100 ms: {s.get("over_100ms")}, errors: {s.get("errors")}')
            print(f'max per 10 s window: {s.get("max_per_10s")}')
        if os.path.exists(f"{t}/stab-paths.txt"):
            print("\nPath probes every 15 s (fresh client each):\n```\n" + open(f"{t}/stab-paths.txt").read().strip() + "\n```")
