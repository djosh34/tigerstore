"""Checker shard coverage and required heavy-test ownership; no test runs."""
import importlib.util
from pathlib import Path
import re
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location(
    "race_test_split", Path(__file__).with_name("race-test-split.py"))
split = importlib.util.module_from_spec(spec)
spec.loader.exec_module(split)


class CheckerSplitTests(unittest.TestCase):
    def test_heavy_ownership_and_exact_partition(self):
        moved = {"TestSoleGrantSavedRuleOff", "TestSoleGrantWholeSearchOracle"}
        tests = sorted(split.HISTORIES | moved |
                       {"TestSoleGrantRecordedRuleOff", "TestLight", "FuzzLight", "ExampleLight"})
        with mock.patch.object(split, "go_lines", return_value=tests):
            patterns = {part: split.check_pattern(part) for part in split.CHECK_PARTS}
        for name in tests:
            owners = [part for part, pattern in patterns.items()
                      if re.fullmatch(pattern, name)]
            self.assertEqual(len(owners), 1, (name, owners))
            if name in moved:
                self.assertEqual(owners, ["histories"], name)
            if name == "TestSoleGrantRecordedRuleOff":
                self.assertEqual(owners, ["other"], name)

    def test_guard_rejects_missing_or_duplicate_tests(self):
        for selections in [[["a"], ["a", "b"]], [["a"], []]]:
            with self.assertRaises(ValueError):
                split.require_partition(["a", "b"], selections)


if __name__ == "__main__":
    unittest.main()
