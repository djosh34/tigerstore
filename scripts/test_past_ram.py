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

    def test_reclaim_limit(self):
        # Run 40075627047 (#419): etcd's three members hit memory.max
        # 7,115 to 11,992 times, none was killed, and a write timed out
        # at 280,256 objects.
        # Run 45844419227 (#888), artifact
        # past-ram-etcd-proof-9223372036854775807-normal: minimal fields
        # from result.json and progress.jsonl, with no OOM kills.
        for mode, count, live, hits, outcome in [
            ("max", 280256, 410807279, (11992, 7115, 10747), "capacity-limit"),
            ("proof", 280960, 411341174, (8949, 11465, 10553), "reference-preload-limit"),
        ]:
            def run(change=None, message="etcdserver: request timed out", phase="preload"):
                result = self.result("etcd", mode)
                result["failure"] = "driver exited 1; see progress.jsonl and driver.log"
                result["last_acknowledged"].update(objects=count, live_bytes=live)
                result["final_cgroups"] = [dict(events=dict(oom_kill=0, max=n)) for n in hits]
                result.update(copy.deepcopy(change or {}))
                past_ram.classify(result, [dict(phase=phase), dict(phase="preload-failed", failure=message)])
                return result

            with self.subTest(mode=mode):
                result = run()
                self.assertEqual(result["outcome"], outcome)
                self.assertEqual(result["limit"], "cgroup-reclaim")
                self.assertIsNone(result["failure"])
                self.assertEqual(result["termination"], "driver exited 1; see progress.jsonl and driver.log")
            for name, result in [
                ("another error", run(message="value mismatch")),
                ("unavailable is not a reclaim timeout", run(message="rpc error: code = Unavailable desc = EOF")),
                ("a member below its limit", run(dict(final_cgroups=[
                    dict(events=dict(oom_kill=0, max=n)) for n in (hits[0], 0, hits[2])]))),
                ("tigerstore", run(dict(kind="tigerstore"))),
                ("driver error", run(dict(failure="driver exited 2; see progress.jsonl and driver.log"))),
                ("checker error", run(dict(checker_exit=1))),
                ("non-preload row", run(phase="mixed")),
                ("preloaded row", run(phase="preloaded")),
                ("mixed started", run(dict(mixed_start={}))),
                ("censored", run(dict(censored=True))),
                ("member killed by harness", run(dict(kills=1))),
                ("cache mode", run(dict(mode="cache"))),
            ]:
                with self.subTest(mode=mode, case=name):
                    self.assertEqual(result["outcome"], "failed")
                    self.assertIsNotNone(result["failure"])
                    self.assertNotIn("limit", result)
            if mode == "proof":
                with self.subTest(mode=mode, case="explicit preload control"):
                    self.assertEqual(run(dict(preload_bytes=16 << 20))["outcome"], "failed")

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
        result["final_cgroups"] = [dict(events=dict(oom_kill=0))] * 3
        past_ram.classify(result, [])
        self.assertEqual(result["outcome"], "passed")
        self.assertNotIn("limit", result)

    def test_final_oom_fails_a_finished_run(self):
        for mode in ["proof", "cache"]:
            with self.subTest(mode=mode):
                result = self.result("tigerstore", mode)
                result["failure"] = None
                result["last_acknowledged"]["phase"] = "mixed"
                past_ram.classify(result, [dict(phase="mixed")])
                self.assertEqual(result["outcome"], "failed")
                self.assertEqual(result["failure"], "a member was OOM-killed")


if __name__ == "__main__":
    unittest.main()
