#!/usr/bin/env python3
"""Summarize a saved seed without rerunning its simulator."""
import collections
import json
import pathlib
import re
import sys

for name in sys.argv[1:]:
    directory = pathlib.Path(name)
    h = json.loads((directory / "history.json").read_text())
    ops = h["ops"]
    calls = [o for o in ops if o.get("err", {}).get("message") != "lease expiry"]
    ambiguous = [o for o in calls if o.get("err", {}).get("ambiguous")]
    failed = [o for o in calls if "err" in o and not o["err"].get("ambiguous")]
    points = collections.Counter()
    for o in calls:
        if o["call"] < o["return"]:
            points[o["call"]] += 1
            points[o["return"]] -= 1
    active = peak = 0
    starts = {}
    longest = collections.defaultdict(int)
    for at, delta in sorted(points.items()):
        before = active
        active += delta
        peak = max(peak, active)
        for threshold in (1, 2):
            if before < threshold <= active:
                starts[threshold] = at
            if before >= threshold > active:
                longest[threshold] = max(longest[threshold], at - starts.pop(threshold))
    answers = sorted(o["return"] for o in calls if "resp" in o)
    silence = max((b - a, a, b) for a, b in zip(answers, answers[1:]))
    leaders = set()
    trace = (directory / "trace.txt").read_text()
    for node, leader, term in re.findall(r"node (\d+): leader is (\d+) at term (\d+)", trace):
        if node == leader:
            leaders.add((int(term), int(node)))
    print(json.dumps({
        "directory": name, "ops": len(ops), "calls": len(calls),
        "synthetic_expiries": len(ops) - len(calls),
        "answered": sum("resp" in o for o in calls),
        "ambiguous": len(ambiguous),
        "ambiguous_mutations": sum(any(k in o["req"] for k in ("put", "delete", "txn", "leaseGrant", "leaseRevoke", "compact")) for o in ambiguous),
        "failed_not_ambiguous": len(failed),
        "error_codes": dict(collections.Counter(o["err"]["code"] for o in calls if "err" in o)),
        "error_messages": dict(collections.Counter(o["err"]["message"] for o in calls if "err" in o)),
        "peak_inflight": peak,
        "longest_connected_call_window_s": longest[1] / 1e9,
        "longest_at_least_two_calls_s": longest[2] / 1e9,
        "longest_call_s": max(o["return"] - o["call"] for o in calls) / 1e9,
        "longest_between_answers_s": [n / 1e9 for n in silence],
        "self_elections_term_node": sorted(leaders),
        "committed_log_entries": len(h["log"]), "target": h["target"],
    }, indent=2))
