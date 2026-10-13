#!/usr/bin/env python3
"""Audit archived round-2 case evidence. Does not replace the Go checkers."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path

CHECKERS = {"Linearizable", "Watches", "Durable", "Revisions"}
PHASES = {"requested", "installed", "exercised", "healed"}


def existing(path):
    if path.exists():
        return path
    compressed = Path(str(path) + ".gz")
    return compressed if compressed.exists() else None


def opener(path):
    return gzip.open(path, "rb") if path.suffix == ".gz" else path.open("rb")


def load(path, default=None):
    path = existing(path)
    if path is None:
        return default
    with opener(path) as stream:
        return json.load(stream)


def audit_case(artifact, verify_hashes=True):
    run = artifact / "run"
    issues = []
    faults = load(run / "faults.json", {})
    result = load(run / "result.json", {})
    oracles = load(run / "oracles.json", [])
    offline = load(run / "offline-results.json", [])
    manifest = load(artifact / "manifest.json", [])
    if not faults:
        issues.append("controller evidence missing")
    if not result.get("controller_pass", False):
        issues.append("controller did not pass")
    for name in faults.get("required", []):
        missing = PHASES - set(faults.get("faults", {}).get(name, {}))
        if missing:
            issues.append(f"fault {name} missing phases: {sorted(missing)}")
    if len(oracles) != 4:
        issues.append(f"expected four disk sets, found {len(oracles)}")
    if not offline:
        issues.append("offline checker execution missing")
    for row in offline:
        if row.get("returncode") != 0:
            issues.append(f"offline {row.get('oracle')}/{row.get('mode')} exit {row.get('returncode')}")
    reports = sorted(set(run.glob("check-*.json")) | set(run.glob("check-*.json.gz")))
    reports = [path for path in reports if ".coverage.json" not in path.name]
    names = set()
    verdicts = []
    for path in reports:
        rows = load(path)
        if not isinstance(rows, list):
            issues.append(f"non-list checker report {path.name}")
            continue
        oracle = path.name.removeprefix("check-").removesuffix(".gz").removesuffix(".json")
        names.add(oracle)
        if len(rows) != 4 or {row.get("name") for row in rows} != CHECKERS:
            issues.append(f"missing or duplicate checker in {path.name}")
        for row in rows:
            verdicts.append({"oracle": oracle, **row})
            if row.get("passed") is not True:
                issues.append(f"{oracle}/{row.get('name')} failed")
            elif row.get("error"):
                issues.append(f"{oracle}/{row.get('name')} passed with an error")
    for oracle in oracles:
        if oracle["name"] not in names:
            issues.append(f"no checker report for {oracle['name']}")
    if existing(run / "workload/history.json") and not reports:
        issues.append("recorded client history has no checker reports")
    if not manifest:
        issues.append("archive manifest missing")
    if verify_hashes:
        for item in manifest:
            path = existing(artifact / item["path"])
            if path is None:
                issues.append(f"archived file missing: {item['path']}")
                continue
            digest, size = hashlib.sha256(), 0
            with opener(path) as stream:
                while block := stream.read(1024 * 1024):
                    digest.update(block)
                    size += len(block)
            if digest.hexdigest() != item["sha256"] or size != item["bytes"]:
                issues.append(f"archive digest/length mismatch: {item['path']}")
    return {
        "artifact": str(artifact),
        "target": faults.get("target"),
        "scenario": faults.get("scenario"),
        "seed": faults.get("seed"),
        "repetition": faults.get("repetition"),
        "utc_origin": faults.get("utc_origin"),
        "controller_started": bool(faults),
        "controller_pass": result.get("controller_pass", False),
        "audit_pass": not issues,
        "issues": issues,
        "oracle_sets": len(oracles),
        "checker_verdicts": len(verdicts),
        "checker_passes": sum(row.get("passed") is True for row in verdicts),
        "checker_timeouts": sum("timed out" in row.get("error", "") or
                                "passed its deadline" in row.get("error", "") for row in verdicts),
        "max_checker_seconds": max((row.get("seconds", 0) for row in verdicts), default=0),
        "required_faults": faults.get("required", []),
        "failures": [event.get("error") for event in faults.get("events", [])
                     if event.get("name") == "controller-failure"],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("roots", nargs="+", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--skip-hashes", action="store_true")
    args = parser.parse_args()
    artifacts = sorted({path.parent for root in args.roots for path in root.rglob("manifest.json")})
    if not artifacts:
        raise SystemExit("no case manifests found")
    cases = [audit_case(path, not args.skip_hashes) for path in artifacts]
    report = {
        "cases": cases,
        "totals": {"cases": len(cases), "passed": sum(case["audit_pass"] for case in cases),
                   "failed": sum(not case["audit_pass"] for case in cases),
                   **{key: sum(case[key] for case in cases) for key in
                      ("oracle_sets", "checker_verdicts", "checker_passes", "checker_timeouts")}},
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report["totals"]))
    return int(any(not case["audit_pass"] for case in cases))


if __name__ == "__main__":
    raise SystemExit(main())
