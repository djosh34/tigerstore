"""Classification regressions. Run in Actions, not on the shared host."""
import argparse
import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

import yaml

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

    def reclaim_samples(self):
        # Run 45844419227's final three samples.jsonl rows, retaining only
        # seconds, current, events.max and io rbytes (one device per member).
        return [dict(seconds=seconds, nodes=[
            dict(current=current, events=dict(max=hits), io=f"8:0 rbytes={reads}\n")
            for current, hits, reads in members]) for seconds, members in [
                (28.238468976000007, [(670990336, 5561, 245018624),
                                      (670879744, 8248, 246472704),
                                      (670957568, 8613, 247226368)]),
                (30.253996838999996, [(670998528, 6727, 552779776),
                                      (671031296, 9121, 560005120),
                                      (665108480, 9793, 559579136)]),
                (32.270915003, [(671064064, 7926, 865955840),
                                (670982144, 10171, 879345664),
                                (671084544, 10551, 758075392)]),
            ]]

    def reclaim_exit(self):
        # The historical artifact predates driver_exit_seconds. Model the
        # harness observing exit on the next (two-second) poll, on its clock.
        return dict(driver_exit=1, driver_exit_seconds=self.reclaim_samples()[-1]["seconds"] + 2)

    def test_proof_rejects_unconfirmed_reclaim(self):
        result = self.result("etcd", "proof")
        result["failure"] = "driver exited 1; see progress.jsonl and driver.log"
        result["last_acknowledged"].update(objects=280960, live_bytes=411341174)
        result["final_cgroups"] = [dict(events=dict(oom_kill=0, max=1)) for _ in range(3)]
        result["last_sample"] = dict(nodes=[dict(current=160 << 20)] * 3)
        progress = [dict(phase="preload-failed", seconds=34.131909638,
                         failure="etcdserver: request timed out")]
        past_ram.classify(result, progress)
        self.assertEqual(result["outcome"], "failed")

    def test_reclaim_uses_only_harness_clock_for_recency(self):
        samples = self.reclaim_samples()
        last_sample = samples[-1]["seconds"]
        for case, elapsed, driver_seconds, outcome in [
            # Driver preload starts one second after the harness clock:
            # the timeout happens 0.2s after the last sample on that clock.
            ("offset clocks", last_sample + 0.2, last_sample - 0.8, "reference-preload-limit"),
            # A startup offset must not make old samples appear recent.
            ("stale samples", last_sample + 30, last_sample + 0.2, "failed"),
        ]:
            with self.subTest(case=case):
                result = self.result("etcd", "proof")
                result.update(failure="driver exited 1; see progress.jsonl and driver.log",
                              driver_exit=1, driver_exit_seconds=elapsed)
                result["last_acknowledged"].update(objects=280960, live_bytes=411341174)
                result["final_cgroups"] = [dict(events=dict(oom_kill=0, max=n))
                                           for n in (8949, 11465, 10553)]
                progress = [dict(phase="preload-failed", seconds=driver_seconds,
                                 failure="etcdserver: request timed out")]
                past_ram.classify(result, progress, samples)
                self.assertEqual(result["outcome"], outcome)

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
                result.update(self.reclaim_exit())
                result.update(copy.deepcopy(change or {}))
                past_ram.classify(result, [dict(phase=phase), dict(phase="preload-failed", failure=message,
                                                              seconds=34.131909638)], self.reclaim_samples())
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
                for change in (dict(preload_bytes=16 << 20), dict(driver_exit=None),
                               dict(driver_exit_seconds=None)):
                    with self.subTest(mode=mode, change=change):
                        self.assertEqual(run(change)["outcome"], "failed")

    def test_proof_requires_recent_reclaim_on_every_member(self):
        for member in range(3):
            for case in ("early max hits only", "low current", "no reads", "small reads",
                         "stale samples", "missing io", "missing member", "too few samples"):
                samples = self.reclaim_samples()
                if case == "early max hits only":
                    for sample in samples:
                        sample["nodes"][member]["events"]["max"] = 1
                elif case == "low current":
                    samples[-1]["nodes"][member]["current"] = 160 << 20
                elif case in ("no reads", "small reads"):
                    growth = 0 if case == "no reads" else (16 << 20) - 1
                    for i, sample in enumerate(samples):
                        sample["nodes"][member]["io"] = f"8:0 rbytes={245018624 + i * growth}\n"
                elif case == "stale samples":
                    for sample in samples:
                        sample["seconds"] -= 10
                elif case == "missing io":
                    samples[-1]["nodes"][member].pop("io")
                elif case == "missing member":
                    samples[-1]["nodes"].pop(member)
                elif case == "too few samples":
                    samples.pop(0)
                for mode in ("proof", "max"):
                    result = self.result("etcd", mode)
                    result["failure"] = "driver exited 1; see progress.jsonl and driver.log"
                    result.update(self.reclaim_exit())
                    result["last_acknowledged"].update(objects=280960, live_bytes=411341174)
                    result["final_cgroups"] = [dict(events=dict(oom_kill=0, max=n))
                                               for n in (8949, 11465, 10553)]
                    progress = [dict(phase="preload-failed", seconds=34.131909638,
                                     failure="etcdserver: request timed out")]
                    past_ram.classify(result, progress, samples)
                    with self.subTest(member=member, case=case, mode=mode):
                        # Max mode deliberately retains its old, weaker rule.
                        self.assertEqual(result["outcome"], "failed" if mode == "proof" else "capacity-limit")

    def test_reference_limit_matches_max_for_confirmed_reclaim_and_oom(self):
        # Proof accepts no smaller measured size than max for the same data.
        for limit in ("cgroup-oom", "cgroup-reclaim"):
            reference = self.result("etcd", "proof")
            reference.update(self.reclaim_exit())
            reference["last_acknowledged"].update(objects=280960, live_bytes=411341174)
            reference["final_cgroups"] = [
                dict(events=dict(oom_kill=int(limit == "cgroup-oom" and i == 1), max=n))
                for i, n in enumerate((8949, 11465, 10553))]
            reference["failure"] = ("a member was OOM-killed" if limit == "cgroup-oom"
                                    else "driver exited 1; see progress.jsonl and driver.log")
            progress = [dict(phase="preload-failed", seconds=34.131909638,
                             failure="etcdserver: request timed out")]
            maximum = copy.deepcopy(reference)
            maximum["mode"] = "max"
            past_ram.classify(maximum, progress, self.reclaim_samples())
            past_ram.classify(reference, progress, self.reclaim_samples())
            with self.subTest(limit=limit):
                self.assertEqual(maximum["outcome"], "capacity-limit")
                self.assertEqual(reference["outcome"], "reference-preload-limit")
                self.assertEqual(reference["limit"], maximum["limit"])
                self.assertEqual(reference["last_acknowledged"], maximum["last_acknowledged"])

    def test_driver_command_arguments(self):
        """Argument regression, not a comparison with an etcd result."""
        for preload in (0, 16 << 20):
            with self.subTest(preload=preload):
                args = argparse.Namespace(driver="pastram", mode="proof", memory=640 << 20,
                                          preload_bytes=preload, seconds=180)
                self.assertEqual(past_ram.driver_command(args, "http://127.0.0.1:20100", "past-ram"),
                                 [str(Path("pastram").resolve()), "-endpoints", "http://127.0.0.1:20100",
                                  "-mode", "proof", "-bytes", str(preload or 2080374784),
                                  "-duration", "180s", "-out", "past-ram"])

    def report_step(self, workflow):
        steps = yaml.safe_load(workflow)["jobs"]["capacity"]["steps"]
        reports = [step for step in steps if step.get("name") == "Report measured outcome"]
        self.assertEqual(len(reports), 1)
        self.assertEqual(reports[0].get("if"), "always()")
        return reports[0]

    def test_workflow_summary_displays_limit_kind(self):
        """Rendering test of the enabled, named workflow reporting step."""
        workflow = Path(__file__).parents[1] / ".github/workflows/past-ram.yml"
        report_step = self.report_step(workflow.read_text())
        for limit in ("cgroup-oom", "cgroup-reclaim"):
            with self.subTest(limit=limit), tempfile.TemporaryDirectory() as directory:
                report = Path(directory) / "summary.json"
                summary = Path(directory) / "job-summary.md"
                report.write_text(json.dumps(dict(outcome="reference-preload-limit", limit=limit)))
                env = dict(os.environ, REPORT=str(report), GITHUB_STEP_SUMMARY=str(summary))
                subprocess.run(["bash", "-e", "-c", report_step["run"]], check=True, env=env)
                rendered = summary.read_text()
                self.assertIn(f'"limit": "{limit}"', rendered)
                self.assertIn("No mixed-load comparison or checker pass is claimed.", rendered)

    def test_rendering_rejects_disabled_reporting_step(self):
        workflow = Path(__file__).parents[1] / ".github/workflows/past-ram.yml"
        disabled = workflow.read_text().replace(
            "      - name: Report measured outcome\n        if: always()",
            "      - name: Report measured outcome\n        if: false")
        read_text = Path.read_text
        with mock.patch.object(Path, "read_text", autospec=True,
                               side_effect=lambda path: disabled if path == workflow else read_text(path)):
            with self.assertRaises(AssertionError):
                self.test_workflow_summary_displays_limit_kind()

    def test_reporting_step_is_selected_by_name(self):
        workflow = Path(__file__).parents[1] / ".github/workflows/past-ram.yml"
        data = yaml.safe_load(workflow.read_text())
        data["jobs"]["capacity"]["steps"].insert(0, dict(
            name="Unrelated report", run="python3 -B - <<'PY'\nprint('not the outcome')\nPY\n"))
        selected = self.report_step(yaml.safe_dump(data))
        self.assertEqual(selected["name"], "Report measured outcome")
        self.assertIn("GITHUB_STEP_SUMMARY", selected["run"])

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
