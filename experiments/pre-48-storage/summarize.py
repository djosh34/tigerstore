#!/usr/bin/env python3
import collections
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
records = [json.loads(line) for line in (root / "tests.jsonl").read_text().splitlines() if line.startswith("{")]
results = [r for r in records if r.get("Action") in ["pass", "fail", "skip"]]
(root / "test-results.tsv").write_text("package\ttest\tresult\telapsed_seconds\n" + "".join(
    "\t".join([r["Package"], r.get("Test", ""), r["Action"], str(r.get("Elapsed", ""))]) + "\n" for r in results))
summary = {"packages": [r for r in results if "Test" not in r],
           "test_results": dict(collections.Counter(r["Action"] for r in results if "Test" in r)),
           "top_level_results": dict(collections.Counter(r["Action"] for r in results if "Test" in r and "/" not in r["Test"])),
           "failed_tests": [r for r in results if r["Action"] == "fail" and "Test" in r],
           "skipped_tests": [r for r in results if r["Action"] == "skip" and "Test" in r],
           "factory_starts": sum("external endpoint=" in r.get("Output", "") for r in records)}
(root / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary, indent=2))
