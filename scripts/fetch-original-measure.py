#!/usr/bin/env python3
"""Actions-only original #833 campaign, source overlay and strict artifact adapter.

Never interprets an unqualified/failed test as a performance win. Both refs get
identical syscall counters and cache preparation. Overlay sources, patches, raw
logs, immutable refs and per-RPC samples are retained with the report.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys

SHA = re.compile(r"[0-9a-f]{40}\Z")


def command(*args, cwd=None, **kwargs):
    return subprocess.run(args, cwd=cwd, check=True, text=True, **kwargs)


def replace_once(path, old, new):
    text = path.read_text()
    if text.count(old) != 1:
        raise ValueError(f"{path}: expected one instrumentation anchor, found {text.count(old)}")
    path.write_text(text.replace(old, new))


def prepare(root, checkout, out, side, sync_delay):
    source = root / "bench/fetch-measure"
    for name, target in (("original_test.go.txt", "fetch_measure_original_test.go"),
                         ("io.go.txt", "fetch_measure_io.go")):
        data = (source / name).read_text()
        (checkout / "internal/wl" / target).write_text(data)
        (out / f"{side}-{target}").write_text(data)
    helper = "package wl\nfunc fetchMeasureRouting(n *node) bool { return false }\nfunc fetchMeasureOwned(n *node) int64 { return n.st.fetchedBytes }\n"
    if side == "candidate":
        helper = "package wl\nfunc fetchMeasureRouting(n *node) bool { return n.cursorFetch }\nfunc fetchMeasureOwned(n *node) int64 { b:=n.st.fetchedBytes; if n.fillBook!=nil { b+=n.fillBook.used }; return b }\n"
    (checkout / "internal/wl/fetch_measure_mode.go").write_text(helper)
    (out / f"{side}-mode.go").write_text(helper)
    path = checkout / "internal/wl/sys_os.go"
    replace_once(path, "return unix.Pread(fd, p, off)", "return fetchMeasurePread(fd, p, off)")
    replace_once(path, "return unix.Preadv2(fd, [][]byte{p}, off, unix.RWF_NOWAIT)",
                 "return fetchMeasurePreadNoWait(fd, p, off)")
    if sync_delay:
        # WAL uses O_DSYNC writes: inject at the durable batch, not at the
        # unrelated directory fsync alone. No read delay is injected.
        replace_once(checkout / "internal/wl/wal.go", "target := w.want.Load()\n\t\tt := time.Now()",
                     "target := w.want.Load()\n\t\tt := time.Now()\n\t\ttime.Sleep(5 * time.Millisecond)")
    patch = subprocess.check_output(["git", "diff", "--binary"], cwd=checkout)
    (out / f"{side}.patch").write_bytes(patch)


def adapt(text, ref, side, pair, race, sync_delay, raw_name, raw_sha):
    records = re.findall(r"FETCH_MEASURE (\{[^\n]+\})", text)
    if len(records) != 1 or not re.search(r"^--- PASS: TestFetchOriginalMeasurement", text, re.M):
        raise ValueError("missing unique measurement or successful correctness test")
    row = json.loads(records[0])
    if row.get("schema") != 1 or row.get("correct") is not True or row.get("case") != "original-80-range":
        raise ValueError("invalid measurement identity/correctness")
    if row.get("routing") is not (side == "candidate"):
        raise ValueError("unexpected baseline/candidate routing")
    if (row.get("keys"), row.get("value_bytes"), row.get("ranges"), row.get("memory_budget_bytes")) != (8000, 16388, 80, 96 << 20):
        raise ValueError("original workload/budget changed")
    samples = row["outcomes"]
    if len(samples) != 80 or sorted(r["first"] for r in samples) != list(range(0, 8000, 100)):
        raise ValueError("missing, duplicate or wrong per-range samples")
    for r in samples:
        if not isinstance(r["elapsed_ms"], (float, int)) or isinstance(r["elapsed_ms"], bool) or not math.isfinite(r["elapsed_ms"]) or r["elapsed_ms"] <= 0:
            raise ValueError("invalid per-range duration")
        if type(r["retries"]) is not int or r["retries"] < 0:
            raise ValueError("invalid per-range refusal count")
    times = sorted(r["elapsed_ms"] for r in samples)
    tails = {"p95": times[75], "p99": times[79], "max": times[79]}
    refusals = {"total": sum(r["retries"] for r in samples), "max_per_range": max(r["retries"] for r in samples)}
    if tails != row["latency_ms"] or refusals != row["refusals"]:
        raise ValueError("aggregates differ from raw RPC samples")
    if row["fetch"]["values"] < 4000 or row["fetch"]["jobs"] <= 0 or row["fetch_owned_peak"] > row["fetch_owned_limit"] or row["fetch_owned_limit"] != (96 << 20) // 32:
        raise ValueError("original fetch/share assertion missing")
    if row.get("nowait_refused") is not True or row["io"]["nowait_calls"] <= 0 or row["io"]["blocking_reads"] <= 0 or row["io"]["nowait_bytes"] != 0:
        raise ValueError("refused-NOWAIT preparation unproven")
    if not math.isfinite(row["wall_ms"]) or row["wall_ms"] <= 0 or not math.isclose(row["throughput"], 80000 / row["wall_ms"], rel_tol=1e-9):
        raise ValueError("invalid cohort wall/throughput")
    row.update(ref=ref, side=side, pair=pair, race=race, sync_delay_ms=sync_delay,
               raw_log=raw_name, raw_log_sha256=raw_sha)
    return row


def distribution(values):
    q = statistics.quantiles(values, n=4, method="inclusive")
    return {"median": statistics.median(values), "min": min(values), "max": max(values), "iqr": q[2] - q[0]}


def decide(pairs, race):
    if len(pairs) not in (5, 10):
        return {"status": "INCOMPLETE", "reasons": ["need five pairs or the ten-pair extension"]}
    if any(p["order"] != ("AB" if i % 2 == 0 else "BA") for i, p in enumerate(pairs)):
        raise ValueError("pairs do not alternate AB/BA")
    report = {"pairs": len(pairs), "sides": {}, "paired_ratios": []}
    for side in ("baseline", "candidate"):
        report["sides"][side] = {"throughput": distribution([p[side]["throughput"] for p in pairs]),
            "wall_ms": distribution([p[side]["wall_ms"] for p in pairs]),
            "latency_ms": {t: distribution([p[side]["latency_ms"][t] for p in pairs]) for t in ("p95", "p99", "max")},
            "refusals": {t: [p[side]["refusals"][t] for p in pairs] for t in ("total", "max_per_range")}}
    report["paired_ratios"] = [p["candidate"]["throughput"] / p["baseline"]["throughput"] for p in pairs]
    def ratio(group):
        return statistics.median(p["candidate"]["throughput"] for p in group) / statistics.median(p["baseline"]["throughput"] for p in group)
    report["first_five_ratio"] = first = ratio(pairs[:5])
    report["combined_ratio"] = combined = ratio(pairs)
    reasons = []
    if first < 0.98:
        reasons.append("first-five median successful-throughput ratio below 0.98")
    if len(pairs) == 10 and combined < 1:
        reasons.append("ten-pair combined median successful-throughput ratio below 1.00")
    limit = 3000 if race else 1000
    for p in pairs:
        if p["candidate"]["latency_ms"]["max"] > limit or p["candidate"]["latency_ms"]["p95"] > limit:
            reasons.append(f"candidate pair {p['pair']} exceeds {limit}-ms p95/max target")
    for metric in ("p95", "p99", "max"):
        b = [p["baseline"]["latency_ms"][metric] for p in pairs]
        c = [p["candidate"]["latency_ms"][metric] for p in pairs]
        if statistics.median(c) > statistics.median(b) or metric == "max" and max(c) > max(b):
            reasons.append(f"worsened {metric} tail")
    for metric in ("total", "max_per_range"):
        b = [p["baseline"]["refusals"][metric] for p in pairs]
        c = [p["candidate"]["refusals"][metric] for p in pairs]
        if sum(c) > sum(b) or max(c) > max(b):
            reasons.append(f"worsened refusals {metric}")
    report["status"] = "RULING" if reasons else "MORE_PAIRS" if len(pairs) == 5 and 0.98 <= first <= 1.02 else "PASS"
    report["reasons"] = reasons
    return report


def run(args):
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise ValueError("long campaigns run only on Actions")
    if not SHA.fullmatch(args.baseline) or not SHA.fullmatch(args.candidate) or args.baseline == args.candidate:
        raise ValueError("distinct immutable full refs required")
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    paths = {}
    for side, ref in (("baseline", args.baseline), ("candidate", args.candidate)):
        checkout = Path(os.environ["RUNNER_TEMP"]) / f"fetch-{side}"
        command("git", "worktree", "add", "--detach", str(checkout), ref, cwd=args.root)
        prepare(args.root, checkout, out, side, args.sync_delay_ms)
        binary = out / f"{side}.test"
        command("go", "test", "-c", *( ["-race"] if args.race else []), "-o", str(binary), "./internal/wl", cwd=checkout)
        paths[side] = binary
    pairs = []
    manifest = {"schema": 1, "scope": "original-only", "refs": {"baseline": args.baseline, "candidate": args.candidate},
                "race": args.race, "sync_delay_ms": args.sync_delay_ms, "pairs": pairs,
                "run_url": os.environ["FETCH_MEASURE_RUN_URL"], "status": "INCOMPLETE"}
    def save():
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    save()
    for number in range(1, 11):
        order = "AB" if number % 2 else "BA"
        pair = {"pair": number, "order": order}
        for side in (("baseline", "candidate") if order == "AB" else ("candidate", "baseline")):
            name = f"{number:02}-{side}.log"
            env = dict(os.environ, FETCH_MEASURE_NOWAIT_REFUSED="1", GOMEMLIMIT="1536MiB")
            with (out / name).open("w") as log:
                result = subprocess.run([str(paths[side]), "-test.run", "^TestFetchOriginalMeasurement$", "-test.v", "-test.timeout", "6m"],
                                        stdout=log, stderr=subprocess.STDOUT, env=env, check=False)
            raw = (out / name).read_bytes()
            if result.returncode:
                manifest.update(status="RULING", reasons=[f"{side} correctness/measurement test failed in pair {number}"], failed_log=name)
                save()
                return 1
            try:
                pair[side] = adapt(raw.decode(), manifest["refs"][side], side, number, args.race, args.sync_delay_ms, name, hashlib.sha256(raw).hexdigest())
            except (KeyError, ValueError) as error:
                manifest.update(status="INCOMPLETE", reasons=[str(error)], failed_log=name)
                save()
                return 2
            print(json.dumps({"pair": number, "side": side, "wall_ms": pair[side]["wall_ms"], "latency_ms": pair[side]["latency_ms"], "refusals": pair[side]["refusals"]}), flush=True)
            # A hard latency miss needs a ruling immediately, not ten more
            # process starts. Preserve the partial pair as raw evidence.
            limit = 3000 if args.race else 1000
            if side == "candidate" and pair[side]["latency_ms"]["max"] > limit:
                manifest.update(status="RULING", reasons=[f"candidate p95/max target {limit} ms missed"], partial_pair=pair)
                save()
                return 1
        pairs.append(pair)
        save()
        if number in (5, 10):
            report = decide(pairs, args.race)
            manifest.update(status=report["status"], report=report)
            save()
            if report["status"] == "MORE_PAIRS":
                continue
            return 0 if report["status"] == "PASS" else 1
    raise AssertionError("unreachable")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--baseline", required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--race", action="store_true")
    parser.add_argument("--sync-delay-ms", type=int, choices=(0, 5), default=0)
    args = parser.parse_args()
    try:
        return run(args)
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"measurement blocked: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
