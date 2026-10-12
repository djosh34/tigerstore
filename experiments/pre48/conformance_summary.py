#!/usr/bin/env python3
"""Preserve every selected spec's outcome and runtime, including unrun specs."""
import collections
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
report = root / "ginkgo.json"
summary = {"certified_pass": False, "discovery_only": True}
if report.exists():
    suites = json.loads(report.read_text())
    specs = []
    for suite in suites:
        for spec in suite.get("SpecReports", []):
            name = " ".join((spec.get("ContainerHierarchyTexts") or []) + [spec.get("LeafNodeText", "")])
            if "[Conformance]" not in name:
                continue
            failure = spec.get("Failure") or {}
            specs.append({"name": name, "state": spec["State"],
                          "runtime_seconds": spec.get("RunTime", 0) / 1e9,
                          "failure": failure.get("Message", ""),
                          "failure_location": failure.get("Location"),
                          "source": spec.get("LeafNodeLocation")})
    summary.update({"selected_count": len(specs),
                    "states": dict(collections.Counter(s["state"] for s in specs)),
                    "suite_success": all(s.get("SuiteSucceeded", False) for s in suites),
                    "suite_failure_reasons": [s.get("SpecialSuiteFailureReasons") for s in suites],
                    "specs": sorted(specs, key=lambda s: s["runtime_seconds"], reverse=True)})
    for category, predicate in {
        "failed": lambda s: s["state"] in {"failed", "panicked", "timedout", "interrupted", "aborted"},
        "pending": lambda s: s["state"] not in {"passed", "failed", "panicked", "timedout", "interrupted", "aborted"},
    }.items():
        (root / f"{category}-names.txt").write_text("\n".join(s["name"] for s in specs if predicate(s)) + "\n")
else:
    summary["missing_machine_report"] = True
(root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps({k: v for k, v in summary.items() if k != "specs"}, indent=2))
