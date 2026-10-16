"""Classification regressions. Run in Actions, not on the shared host."""
import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("past_ram", Path(__file__).with_name("past-ram.py"))
past_ram = importlib.util.module_from_spec(spec)
spec.loader.exec_module(past_ram)


class ClassificationTests(unittest.TestCase):
    def result(self, kind="tigerstore", mode="max"):
        return dict(kind=kind, mode=mode, memory=640 << 20, preload_bytes=0,
                    failure="a member was OOM-killed", kills=0,
                    last_acknowledged=dict(phase="preload", objects=2794304,
                                           live_bytes=4076217555),
                    final_cgroups=[dict(events=dict(oom_kill=n)) for n in (0, 1, 0)])

    def test_four_observed_limits(self):
        # Counts and final cgroup events from run 38951406613. The poll loop
        # missed the kill in two cases, but final memory.events recorded it.
        for kind, mode, count, live, driver_failed in [
            ("tigerstore", "max", 2794304, 4076217555, True),
            ("etcd", "max", 463744, 678348675, False),
            ("etcd", "max", 457984, 670027745, False),
            ("etcd", "proof", 463680, 677194644, True),
        ]:
            with self.subTest(kind=kind, mode=mode, count=count):
                result = self.result(kind, mode)
                result["last_acknowledged"].update(objects=count, live_bytes=live)
                progress = []
                if driver_failed:
                    result["failure"] = "driver exited 1; see progress.jsonl and driver.log"
                    progress = [dict(phase="preload-failed", failure="rpc error: code = Unavailable desc = error reading from server: EOF")]
                termination = result["failure"]
                past_ram.classify(result, progress)
                self.assertIsNone(result["failure"])
                self.assertEqual(result["termination"], termination)
                self.assertEqual(result["limit"], "cgroup-oom")
                self.assertEqual(result["outcome"], "capacity-limit" if mode == "max" else "reference-preload-limit")

    def test_failures_stay_failures(self):
        cases = [
            dict(mode="proof"),  # Tigerstore must pass the actual proof.
            dict(mode="cache"),
            dict(kind="etcd", mode="proof", preload_bytes=16 << 20),
            dict(censored=True),
            dict(kills=1),
            dict(mixed_start={}),
            dict(checker_exit=1),
            dict(failure="cluster did not start"),
            dict(failure="driver exited 2; see progress.jsonl and driver.log"),
            dict(final_cgroups=[dict(events=dict(oom_kill=0))] * 3),
            dict(final_cgroups=[]),
            dict(last_acknowledged={}),
        ]
        for change in cases:
            with self.subTest(change=change):
                result = self.result()
                result.update(copy.deepcopy(change))
                before = result["failure"]
                past_ram.classify(result, [])
                self.assertEqual(result["failure"], before)
                self.assertEqual(result["outcome"], "failed")

    def test_unrelated_driver_errors_are_not_limits(self):
        for message in ["value mismatch", "rpc error: code = ResourceExhausted desc = quota"]:
            result = self.result()
            result["failure"] = "driver exited 1; see progress.jsonl and driver.log"
            past_ram.classify(result, [dict(phase="preload-failed", failure=message)])
            self.assertEqual(result["outcome"], "failed")

    def test_later_failure_is_not_preload_limit(self):
        result = self.result("etcd", "proof")
        result["last_acknowledged"]["live_bytes"] = 677194644
        past_ram.classify(result, [dict(phase="preloaded")])
        self.assertEqual(result["outcome"], "failed")

    def test_success_is_not_a_limit(self):
        result = self.result("tigerstore", "proof")
        result["failure"] = None
        past_ram.classify(result, [])
        self.assertEqual(result["outcome"], "passed")
        self.assertNotIn("limit", result)


if __name__ == "__main__":
    unittest.main()
