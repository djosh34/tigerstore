"""Require a successful suite and every expected conformance spec exactly once."""
import collections
import json
from pathlib import Path
import sys
import xml.etree.ElementTree as ET


def summarize(xml, expected, exit_code):
    cases = []
    other_failures = []
    for case in ET.fromstring(xml).iter("testcase"):
        name = case.get("name", "").removeprefix("[It] ")
        state = "passed"
        detail = ""
        for tag in ("skipped", "error", "failure"):
            child = case.find(tag)
            if child is not None:
                state = tag
                detail = child.get("message", "") + "\n" + "".join(child.itertext())
        if "[Conformance]" in name:
            cases.append({"name": name, "state": state, "detail": detail})
        elif state in ("error", "failure"):
            other_failures.append({"name": name, "state": state, "detail": detail})
    observed = collections.Counter(case["name"] for case in cases)
    states = collections.Counter(case["state"] for case in cases)
    missing = sorted(expected - observed.keys())
    unexpected = sorted(observed.keys() - expected)
    duplicates = sorted(name for name, count in observed.items() if count != 1)
    passed = (exit_code == 0 and bool(expected) and not missing and not unexpected
              and not duplicates and not other_failures
              and states.get("passed", 0) == len(expected))
    return {"passed": passed, "exit_code": exit_code, "expected": len(expected),
            "states": dict(states), "missing": missing, "unexpected": unexpected,
            "duplicates": duplicates, "other_failures": other_failures, "specs": cases}


def main():
    out = Path(sys.argv[1])
    expected = set(Path("k8stest/conformance-names-v1.37.0.txt").read_text().splitlines())
    result = {"passed": False}
    try:
        result = summarize((out / "junit_01.xml").read_text(), expected, int(sys.argv[2]))
    finally:
        (out / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "specs"}, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
