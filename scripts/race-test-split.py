#!/usr/bin/env python3
"""Print guarded package/test selections for ci's native race jobs.

The test job owns every untagged package except sim (race-sim), wl (its
existing fast/slow split), and check (the two test-check matrix entries).
Both checker entries enumerate the race-built tests, including fuzz seeds
and examples, so a new test automatically joins the other entry.
"""

import collections
import re
import subprocess
import sys

PREFIX = "github.com/djosh34/tigerstore/"
CHECK = PREFIX + "check"
CHECK_PARTS = ("histories", "other")
# Balance saved histories plus the saved-row rule-off replay and whole-search
# oracle against the recorded rule-off replay and remaining tests in other.
# Keep both partitions below 420 s/package and 630 s/job in CI.
HISTORIES = {
    "TestWeightedSavedMargins",
    "TestSoleGrantSavedRuleOff",
    "TestSoleGrantWholeSearchOracle",
    "TestWitnessHardHistories",
    "TestDueRecordedVerdicts",
    "TestWitnessStartRecordedVerdicts",
}


def go_lines(*args):
    return subprocess.check_output(["go", *args], text=True).splitlines()


def require_partition(expected, selections):
    """Reject both omitted and duplicate assignments, not just a set mismatch."""
    actual = collections.Counter(item for selection in selections for item in selection)
    wanted = collections.Counter(expected)
    if actual != wanted:
        raise ValueError(f"race coverage differs: missing={wanted - actual}, extra={actual - wanted}")
    if any(not selection for selection in selections):
        raise ValueError("empty race selection")


def packages():
    all_packages = go_lines("list", "-race", "./...")
    expected = [p for p in all_packages if p not in (PREFIX + "internal/sim", PREFIX + "internal/wl")]
    main = [p for p in expected if p != CHECK]
    require_partition(expected, [main, [CHECK]])
    return main


def check_pattern(part):
    listed = go_lines("test", "-race", "-list", ".", "./check")
    tests = [name for name in listed if name.startswith(("Test", "Fuzz", "Example"))]
    if not HISTORIES <= set(tests):
        raise ValueError(f"stale checker test names: {HISTORIES - set(tests)}")
    selections = {
        "histories": [name for name in tests if name in HISTORIES],
        "other": [name for name in tests if name not in HISTORIES],
    }
    # Go identifiers have no regexp metacharacters. Anchoring the top-level
    # names runs all their subtests, and cannot match another entry's test.
    patterns = {name: "^(" + "|".join(selections[name]) + ")$" for name in CHECK_PARTS}
    require_partition(tests, [
        [test for test in tests if re.fullmatch(patterns[name], test)]
        for name in CHECK_PARTS
    ])
    return patterns[part]


def main():
    if sys.argv[1:] == ["packages"]:
        print("\n".join(packages()))
    elif len(sys.argv) == 3 and sys.argv[1] == "check-pattern" and sys.argv[2] in CHECK_PARTS:
        print(check_pattern(sys.argv[2]))
    else:
        raise ValueError("usage: race-test-split.py packages | check-pattern {histories,other}")


if __name__ == "__main__":
    main()
