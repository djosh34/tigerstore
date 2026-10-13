#!/usr/bin/env python3
"""Summarize saved JSONL evidence. This does not rerun any reliability test."""
import collections
import json
import math
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
groups = collections.defaultdict(list)
watches = collections.defaultdict(list)
cases = []


def quantiles(values):
    values = sorted(values)
    return {
        "n": len(values), "min": values[0],
        "p50": values[math.ceil(0.50 * len(values)) - 1],
        "p95": values[math.ceil(0.95 * len(values)) - 1],
        "p99": values[math.ceil(0.99 * len(values)) - 1],
        "max": values[-1], "samples": values,
    }


for path in sorted(root.glob("leader-differential-*/*/*/history.jsonl")):
    store, role = path.parts[-3:-1]
    repetition = path.parts[-4].rsplit("-", 1)[1]
    events = [json.loads(line) for line in path.read_text().splitlines()]
    verdict = [e for e in events if e["event"] == "verdict"]
    assert len(verdict) == 1 and verdict[0]["passed"], path
    loss = next(e for e in events if e["event"] == "leaderless_confirmed")
    assert any(e["event"] == "fault_installed" and e["proc_state"] == "T" for e in events), path
    assert any(e["event"] == "fault_heal" for e in events), path
    cases.append({"store": store, "role": role, "repetition": repetition,
                  "history": str(path.relative_to(root)), "passed": True})
    for event in events:
        if event["event"] == "unary":
            groups[(store, event["label"], event["require_leader"])].append(event)
    for name in ["existing_plain", "existing_required", "leaderless_plain", "leaderless_required", "healed_required"]:
        start = next(e for e in events if e["event"] == "watch_start" and e["name"] == name)
        received = [e for e in events if e["event"] == "watch_recv" and e["name"] == name]
        terminal = [e for e in received if e["code"] != "OK"]
        watches[(store, name)].append({
            "created": any("created:true" in e["response"].replace(" ", "") for e in received),
            "sentinel": any('/leader/sentinel' in e["response"] for e in received),
            "terminal_code": terminal[0]["code"] if terminal else "not recorded",
            "terminal_error": terminal[0]["error"] if terminal else "not recorded",
            "terminal_after_verdict": terminal[0]["elapsed_ms"] >= verdict[0]["elapsed_ms"] if terminal else None,
            "terminal_from_start_ms": terminal[0]["elapsed_ms"] - start["elapsed_ms"] if terminal else None,
            "terminal_from_leaderless_ms": terminal[0]["elapsed_ms"] - loss["elapsed_ms"] if terminal else None,
        })

unary = []
for (store, label, require), events in sorted(groups.items()):
    unary.append({
        "store": store, "label": label, "require_leader": require,
        "duration_ms": quantiles([e["duration_ms"] for e in events]),
        "codes": dict(collections.Counter(e["code"] for e in events)),
        "errors": dict(collections.Counter(e["error"] for e in events)),
    })
watch_summary = []
for (store, name), entries in sorted(watches.items()):
    item = {"store": store, "name": name, "n": len(entries),
            "created": sum(e["created"] for e in entries),
            "sentinel": sum(e["sentinel"] for e in entries),
            "terminal_codes": dict(collections.Counter(e["terminal_code"] for e in entries)),
            "terminal_errors": dict(collections.Counter(e["terminal_error"] for e in entries)),
            "terminal_after_verdict": sum(e["terminal_after_verdict"] is True for e in entries)}
    if "required" in name and name != "healed_required":
        item["terminal_from_start_ms"] = quantiles([e["terminal_from_start_ms"] for e in entries])
        item["terminal_from_leaderless_ms"] = quantiles([e["terminal_from_leaderless_ms"] for e in entries])
    watch_summary.append(item)

print(json.dumps({"quantile_method": "nearest rank, ceil(p*n), integer elapsed milliseconds",
                  "cases": cases, "unary": unary, "watches": watch_summary}, indent=2))
