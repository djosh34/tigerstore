#!/usr/bin/env python3
"""Offline #833 evidence/decision gate. Never starts a server or an Actions run."""
import argparse
import json
import math
from pathlib import Path
import re
import statistics
import sys

SCHEMA = 1
NAMED = tuple(f"{name}-{size}" for name in ("range", "list", "watch", "put", "txn")
              for size in ("1k", "10k")) + tuple(f"k8s/{shape}" for shape in (
                  "small", "medium-crds", "large", "crd-heavy", "many-namespaces"))
CACHE_MODES = ("past-ram-560m", "tmpfs-nowait-refused", "evicted-cache", "resident-3g")
COHORTS = ("get-get", "list-get", "overlapping-list", "failed-cas-get", "overlapping-prev-kv")
DELAYS = (0, 100, 500)
# Endpoints plus an intermediate size; batched probes, not one Actions job per cell.
PROBES = tuple(f"reuse/{cohort}/{delay}ms/{clients}clients/{size}bytes"
               for cohort in COHORTS for delay in DELAYS for clients in (10, 20)
               for size in (64 << 10, 256 << 10, 1 << 20))
RTTS = (0, 2, 10)
MATCHED = ("seed", "member", "offered_load", "cache_preparation", "cohort",
           "go_memory_bytes", "memory_budget_bytes")
TAILS = ("p95", "p99", "max")


def plan():
    return [{"case": case, "cache": cache, "rtt_ms": rtt}
            for case in NAMED + PROBES for cache in CACHE_MODES for rtt in RTTS]


def key(cell):
    return (cell["case"], cell["cache"], cell["rtt_ms"])


def finite(value, name, positive=False, integer=False):
    try:
        valid = not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError(f"{name} must be a finite number")
    if value < 0 or positive and value == 0 or integer and not isinstance(value, int):
        raise ValueError(f"invalid {name}")
    return value


def control(case):
    return case.startswith(("put-", "txn-"))


def ceiling_case(case):
    return control(case) or case.startswith("k8s/")


def qualify(pair, refs):
    """Reject missing/disabled/unmatched/background-only evidence; never call it a win."""
    try:
        cell = key(pair)
        finite(cell[2], "rtt_ms", integer=True)
        if cell[0] not in NAMED + PROBES or cell[1] not in CACHE_MODES or cell[2] not in RTTS:
            raise ValueError("unknown case/cache/RTT")
        if pair["order"] not in ("AB", "BA"):
            raise ValueError("missing AB/BA order")
        base, candidate = pair["baseline"], pair["candidate"]
        for side, run in (("baseline", base), ("candidate", candidate)):
            if not isinstance(run, dict) or any(not isinstance(run.get(field), dict)
                    for field in ("settings", "fetch", "latency_ms", "refusals")):
                raise ValueError("invalid nested run structure")
            if run["ref"] != refs[side]:
                raise ValueError("run ref does not match immutable campaign ref")
            if run["correct"] is not True or run["generator_limited"] is not False or run["ran_out"] is not False:
                raise ValueError("incorrect, generator-limited or exhausted-trace run")
            if not isinstance(run["evidence"], list) or not run["evidence"] or any(
                    not isinstance(x, str) or not x for x in run["evidence"]):
                raise ValueError("missing raw artifact links")
            finite(run["throughput"], "throughput", positive=True)
            for tail in TAILS:
                finite(run["latency_ms"][tail], tail, positive=True)
            if not run["latency_ms"]["p95"] <= run["latency_ms"]["p99"] <= run["latency_ms"]["max"]:
                raise ValueError("inconsistent tail quantiles")
            for count in ("total", "max_per_range"):
                finite(run["refusals"][count], count, integer=True)
            if run["refusals"]["max_per_range"] > run["refusals"]["total"]:
                raise ValueError("inconsistent refusal counters")
            for count in ("jobs", "origin_fulfillment", "same_consumer", "distinct_request"):
                finite(run["fetch"][count], count, integer=True)
            if ceiling_case(cell[0]):
                finite(run["ceiling_share"], "ceiling_share", positive=True)
            for field in ("seed", "member"):
                finite(run["settings"].get(field), field, positive=True, integer=True)
            for field in MATCHED:
                if field not in run["settings"] or run["settings"][field] is None:
                    raise ValueError(f"missing matched setting {field}")
                if field in ("go_memory_bytes", "memory_budget_bytes"):
                    finite(run["settings"][field], field, positive=True, integer=True)
            if run["settings"].get("rtt_ms") != cell[2] or run["settings"].get("cache") != cell[1]:
                raise ValueError("run cache/RTT differs from case")
        for run in (base, candidate):
            prep = run["settings"]["cache_preparation"]
            if not isinstance(prep, dict) or prep.get("mode") != cell[1] or prep.get("confirmed") is not True:
                raise ValueError("cache preparation is unconfirmed")
            if not isinstance(prep.get("dataset_sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", prep["dataset_sha256"]):
                raise ValueError("missing matched dataset fingerprint")
        if cell[0].startswith("reuse/"):
            _, name, delay, clients, size = cell[0].split("/")
            cohort = base["settings"]["cohort"]
            if not isinstance(cohort, dict) or any(cohort.get(k) != v for k, v in (
                    ("name", name), ("delay_ms", int(delay[:-2])),
                    ("clients", int(clients[:-7])), ("value_bytes", int(size[:-5])))):
                raise ValueError("reuse probe settings differ from required cell")
            if cohort.get("same_member") is not True or cohort.get("same_version") is not True:
                raise ValueError("reuse is not same-member/same-version")
        if any(base["settings"][field] != candidate["settings"][field] for field in MATCHED):
            raise ValueError("unmatched seed/member/load/cache preparation/cohort/budget")
        if not isinstance(candidate.get("routing"), dict) or candidate["routing"].get("ranges") is not True or candidate["routing"].get("watches") is not True:
            raise ValueError("candidate routing disabled or unproven")
        if cell[1] == "past-ram-560m" and base["settings"]["go_memory_bytes"] != 560 << 20:
            raise ValueError("past-RAM cohort is not at 560 MiB")
        if cell[1] == "resident-3g" and base["settings"]["go_memory_bytes"] != 3 << 30:
            raise ValueError("resident control is not at 3 GiB")
        if cell[1] != "resident-3g" and not control(cell[0]):
            if base["fetch"]["jobs"] <= 0 or base["fetch"]["distinct_request"] <= 0:
                raise ValueError("no main fetch plus genuine distinct-request reuse in measured cohort")
        ratio = candidate["throughput"] / base["throughput"]
        finite(ratio, "paired ratio", positive=True)
        return None
    except (KeyError, TypeError, ValueError) as error:
        return str(error)


def summary(values):
    return dict(median=statistics.median(values), min=min(values), max=max(values),
                iqr=statistics.quantiles(values, n=4, method="inclusive")[2]
                - statistics.quantiles(values, n=4, method="inclusive")[0])


def decide(pairs):
    """Pairs are chronological. Retain all values, including losses and bad order."""
    ratios = [p["candidate"]["throughput"] / p["baseline"]["throughput"] for p in pairs]
    report = {"valid_pairs": len(pairs), "paired_ratios": ratios}
    if ratios:
        report["throughput"] = summary(ratios) if len(ratios) > 1 else dict(median=ratios[0])
    if any(p["order"] != ("AB" if i % 2 == 0 else "BA") for i, p in enumerate(pairs)):
        return dict(report, status="INCOMPLETE", reasons=["valid pairs do not alternate AB/BA"])
    if len(pairs) < 5 or len(pairs) > 10:
        return dict(report, status="INCOMPLETE", reasons=["need five valid pairs, or the specified ten-pair extension"])
    reasons = []
    first = statistics.median(ratios[:5])
    if first < 0.98:
        reasons.append("first-five throughput median below 0.98: manager ruling required")
    borderline = 0.98 <= first <= 1.02
    pending = None
    if borderline and len(pairs) < 10:
        pending = ("MORE_PAIRS", "0.98–1.02 is diagnostic noise: five more valid pairs required")
    if not borderline and 5 < len(pairs) < 10:
        pending = ("INCOMPLETE", "partial extra-pair campaign")
    if len(pairs) == 10 and statistics.median(ratios) < 1:
        reasons.append("combined throughput median below 1.00: manager ruling required")
    report["tails"] = {}
    for tail in TAILS:
        b = [p["baseline"]["latency_ms"][tail] for p in pairs]
        c = [p["candidate"]["latency_ms"][tail] for p in pairs]
        report["tails"][tail] = {"baseline": summary(b), "candidate": summary(c)}
        if statistics.median(c) > statistics.median(b) or tail == "max" and max(c) > max(b):
            reasons.append(f"worsened {tail} tail: manager ruling required")
    report["refusals"] = {}
    for counter in ("total", "max_per_range"):
        b = [p["baseline"]["refusals"][counter] for p in pairs]
        c = [p["candidate"]["refusals"][counter] for p in pairs]
        report["refusals"][counter] = {"baseline": b, "candidate": c}
        if sum(c) > sum(b) or max(c) > max(b):
            reasons.append(f"worsened refusal {counter}: manager ruling required")
    if ceiling_case(pairs[0]["case"]) and statistics.median(
            p["candidate"]["ceiling_share"] for p in pairs) < 0.8:
        reasons.append("candidate below applicable 80% measured ceiling")
    if reasons:
        return dict(report, status="RULING", reasons=reasons)
    if pending:
        return dict(report, status=pending[0], reasons=[pending[1]])
    return dict(report, status="PASS", reasons=[])


def evaluate(document):
    if not isinstance(document, dict) or document.get("schema") != SCHEMA:
        raise ValueError("unsupported schema")
    refs = document["refs"]
    for side in ("baseline", "candidate"):
        if not isinstance(refs[side], str) or not re.fullmatch(r"[0-9a-f]{40}", refs[side]):
            raise ValueError("immutable full commit SHAs required")
    if refs["baseline"] == refs["candidate"]:
        raise ValueError("A/A calibration does not qualify as main/candidate evidence")
    groups, invalid, failures, seen = {}, [], [], set()
    if not isinstance(document["pairs"], list):
        raise ValueError("pairs must be an array")
    for pair in document["pairs"]:
        if not isinstance(pair, dict):
            raise ValueError("pair must be an object")
        pair_id = pair.get("id")
        if not isinstance(pair_id, str) or not pair_id or pair_id in seen:
            raise ValueError("missing or duplicate pair id")
        seen.add(pair_id)
        if isinstance(pair.get("candidate"), dict) and pair["candidate"].get("correct") is False:
            failures.append(pair_id)
        error = qualify(pair, refs)
        if error:
            invalid.append({"id": pair_id, "reason": error})
        else:
            groups.setdefault(key(pair), []).append(pair)
    cells = []
    for requirement in plan():
        pairs = groups.get(key(requirement), [])
        cells.append(dict(requirement, **decide(pairs)))
    statuses = [c["status"] for c in cells]
    status = "RULING" if failures or "RULING" in statuses else "PASS" if all(s == "PASS" for s in statuses) else "INCOMPLETE"
    return {"schema": SCHEMA, "scope": "named-and-reuse-comparisons-only",
            "original_workload_gate": "NOT_EVALUATED", "refs": refs, "status": status, "invalid_pairs": invalid,
            "candidate_correctness_failures": failures,
            "required_cells": len(cells), "cells": cells}


def reject_constant(value):
    raise ValueError(f"non-finite JSON {value}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", nargs="?", type=Path)
    parser.add_argument("--plan", action="store_true", help="print required cells; run nothing")
    args = parser.parse_args()
    if args.plan:
        print(json.dumps({"schema": SCHEMA, "required_cells": plan()}, indent=2))
        return 0
    if args.manifest is None:
        parser.error("provide a manifest or --plan")
    try:
        with args.manifest.open() as stream:
            document = json.load(stream, parse_constant=reject_constant)
        report = evaluate(document)
    except (KeyError, TypeError, ValueError, OSError) as error:
        print(json.dumps({"status": "INCOMPLETE", "error": str(error)}))
        return 2
    print(json.dumps(report, indent=2, allow_nan=False))
    return {"PASS": 0, "RULING": 1, "INCOMPLETE": 2}[report["status"]]


if __name__ == "__main__":
    sys.exit(main())
