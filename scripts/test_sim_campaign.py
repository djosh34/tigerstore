"""Planner, runner and completeness regressions; no simulator seeds run."""
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

spec = importlib.util.spec_from_file_location("sim_campaign", Path(__file__).with_name("sim-campaign.py"))
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)


def result(test, seed, status="PASS"):
    # The driver's verdict is immediate. Go's own subtest verdict is
    # buffered until the parent ends, and is deliberately absent here.
    return f"=== RUN   {test}/{seed}\nsimrun: seed {test}/{seed} {status}\n"


class PlannerTests(unittest.TestCase):
    def check_plan(self, test, first, seeds, longs):
        plan = campaign.plan(test, first, seeds, longs)
        seen = []
        for shard, job in enumerate(plan["jobs"]):
            self.assertEqual(job["shard"], shard)
            self.assertLessEqual(len(job["processes"]), 4)
            owned = [seed for process in job["processes"] for span in process
                     for seed in range(span["first"], span["first"] + span["seeds"])]
            for process in job["processes"]:
                self.assertEqual(sum(span["twice"] for span in process),
                                 sum(span["seeds"] for span in process) // 100)
                for span in process:
                    self.assertLessEqual(span["twice"], span["seeds"])
            for seed in longs:
                if seed in owned:
                    self.assertEqual(owned, [seed])
                    self.assertEqual(len(job["processes"]), 1)
            seen += owned
        self.assertEqual(sorted(seen), list(range(first, first + seeds)))
        self.assertLessEqual(len(plan["jobs"]), campaign.MAX_JOBS)
        return plan

    def test_one_long(self):
        plan = self.check_plan("TestCampaign", 1, 2404, [1052])
        self.assertEqual(len(plan["jobs"]), 11)

    def test_two_longs_and_split_queue_replays(self):
        # These allocator outputs are also checked by TestCampaignLongSeeds.
        plan = self.check_plan("TestCampaign", 1, 4808, [1052, 3546])
        split = [p for job in plan["jobs"] for p in job["processes"] if len(p) > 1]
        self.assertIn([83, 38], [[span["seeds"] for span in p] for p in split])
        for process in split:
            self.assertEqual(sum(span["twice"] for span in process), 1)
        self.check_plan("TestCampaign", 1052, 2495, [1052, 3546])

    def test_all_long_tests(self):
        for test in ["TestCampaignShapeSeeds", "TestKubernetesLoadLong"]:
            plan = self.check_plan(test, 1, 4, [1, 2, 3, 4])
            self.assertEqual(len(plan["jobs"]), 4)
            self.check_plan(test, 1, 256, list(range(1, 257)))
        self.check_plan("TestBusySeeds", 1, 4, [])

    def test_no_longs_preserves_process_sizes(self):
        plan = self.check_plan("TestSeeds", 0, 100_000, [])
        self.assertEqual(len(plan["jobs"]), 10)
        for job in plan["jobs"]:
            self.assertEqual([p[0]["seeds"] for p in job["processes"]], [2500] * 4)
        self.check_plan("TestSeeds", 7, 1, [])
        self.check_plan("TestSeeds", 7, 3, [])

    def test_matrix_limit_message_for_truncated_helper_output(self):
        for test in ["TestCampaignShapeSeeds", "TestKubernetesLoadLong"]:
            with self.assertRaisesRegex(ValueError, "256-job matrix"):
                campaign.plan(test, 1, 100_000, list(range(1, 258)))

    def test_matrix_limit_and_invalid_inputs(self):
        for test, first, seeds, longs in [
            ("TestCampaign", 0, 1000, list(range(255))),  # 255 long + ordinary jobs
            ("TestCampaignShapeSeeds", 1, 3, []),
            ("TestKubernetesLoadLong", 1, 3, []),
            ("TestSeeds", 1, 2, [4]),
            ("TestSeeds.*", 1, 1, []),
            ("TestSeeds", -1, 1, []),
            ("TestSeeds", 1, 0, []),
            ("TestSeeds", 1 << 53, 1, []),
        ]:
            with self.subTest(test=test, seeds=seeds):
                with self.assertRaises(ValueError):
                    campaign.plan(test, first, seeds, longs)


class SummaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.artifacts = Path(self.temp.name)
        self.plan = campaign.plan("TestCampaign", 389, 6, [])

    def logs(self):
        for job in self.plan["jobs"]:
            directory = self.artifacts / f"sim-campaign-logs-{job['shard']}"
            directory.mkdir()
            for p, process in enumerate(job["processes"]):
                seeds = [seed for span in process
                         for seed in range(span["first"], span["first"] + span["seeds"])]
                test = self.plan["test"]
                text = f"=== RUN   {test}\n" + "".join(result(test, seed) for seed in seeds)
                # Real -test.v order: parent verdict, then buffered subtests.
                text += f"--- PASS: {test} (1.00s)\n"
                text += "".join(f"    --- PASS: {test}/{seed} (0.01s)\n" for seed in seeds)
                (directory / f"seeds-{job['shard']}-{p}.log").write_text(text)

    def check(self, status):
        out = io.StringIO()
        self.assertEqual(campaign.summary(self.plan, self.artifacts, out), status)
        return out.getvalue()

    def test_missing_seed_and_job_without_artifact(self):
        self.logs()
        # Seed 390 started but never finished. Job 1 uploaded nothing.
        (self.artifacts / "sim-campaign-logs-0/seeds-0-1.log").write_text("=== RUN   TestCampaign/390\n")
        for path in (self.artifacts / "sim-campaign-logs-1").iterdir():
            path.unlink()
        (self.artifacts / "sim-campaign-logs-1").rmdir()
        text = self.check(1)
        self.assertIn("Missing seeds 390: seeds (0), process 1", text)
        self.assertIn("Missing seeds 393: seeds (1), process 0", text)
        self.assertIn("Missing seeds 394: seeds (1), process 1", text)
        self.assertIn("3 of 6 seeds reported; 3 missing", text)

    def test_truncated_multi_seed_log_preserves_completed_verdicts(self):
        self.plan = dict(test="TestCampaign", first=1, seeds=100,
                         jobs=[dict(shard=0, processes=[[dict(first=1, seeds=100)]])])
        directory = self.artifacts / "sim-campaign-logs-0"
        directory.mkdir()
        log = "=== RUN   TestCampaign\n" + "".join(result("TestCampaign", seed) for seed in range(1, 61))
        log += "=== RUN   TestCampaign/61\n"
        self.assertNotIn("--- PASS:", log)
        (directory / "seeds-0-0.log").write_text(log)
        text = self.check(1)
        self.assertIn("60 of 100 seeds reported; 40 missing", text)
        self.assertIn("Missing seeds 61–100: seeds (0), process 0", text)

    def test_known_failure_skip_counts_but_other_skips_do_not(self):
        self.logs()
        path = self.artifacts / "sim-campaign-logs-0/seeds-0-1.log"
        path.write_text(result("TestCampaign", 390, "SKIP"))
        self.check(1)
        path.write_text("=== RUN   TestCampaign/390\n"
                        "known failure: seed 390 of TestCampaign fails because of #123 (known-failures.txt)\n"
                        "    --- SKIP: TestCampaign/390 (0.00s)\n")
        self.check(1)  # The driver record, not the old message, is required.
        path.write_text(path.read_text() + "simrun: seed TestCampaign/390 SKIP known-failure\n")
        self.assertIn("6 of 6 seeds reported", self.check(0))

    def test_aged_image_and_forks_are_not_driver_completion(self):
        self.logs()
        path = self.artifacts / "sim-campaign-logs-0/seeds-0-1.log"
        image = ("=== RUN   TestCampaign/390\n"
                 "    campaign_test.go:132: === RUN   TestCampaign/390\n"
                 "        --- PASS: TestCampaign/390 (1.00s)\n"
                 "simrun: image TestCampaign/390 PASS\n")
        path.write_text(image)
        self.check(1)
        forks = "".join(f"simrun: fork TestCampaign/390/{k} PASS\n" for k in range(4))
        path.write_text(image + forks)
        self.check(1)
        path.write_text(image + forks + "simrun: seed TestCampaign/390 PASS\n")
        self.check(0)

    def test_numeric_subtest_is_not_a_seed(self):
        self.plan = campaign.plan("TestCampaignKindShares", 2404, 1, [])
        directory = self.artifacts / "sim-campaign-logs-0"
        directory.mkdir()
        (directory / "seeds-0-0.log").write_text(
            "=== RUN   TestCampaignKindShares/2404\n"
            "--- PASS: TestCampaignKindShares (0.00s)\n"
            "    --- PASS: TestCampaignKindShares/2404 (0.00s)\n")
        self.assertIn("0 of 1 seeds reported", self.check(1))

    def test_fail_is_reported_and_test_variants_do_not_match(self):
        self.logs()
        path = self.artifacts / "sim-campaign-logs-0/seeds-0-1.log"
        path.write_text(result("TestCampaign", 390, "FAIL"))
        self.check(0)  # The workflow separately propagates seed-job failure.
        for test in ["TestCampaignShapeSeeds", "TestCampaignOther", "TestCampaign/variant"]:
            path.write_text(result(test, 390))
            self.check(1)
        path.write_text("=== RUN   TestCampaign/390\n    --- PASS: TestCampaign/390 (0.00s)\n")
        self.check(1)  # Go's verdict alone cannot stand in for the driver.

    def test_large_missing_range_stays_readable(self):
        self.plan = campaign.plan("TestSeeds", 1, 100_000, [])
        text = self.check(1)
        self.assertIn("Missing seeds 1–2500: seeds (0), process 0", text)
        self.assertIn("0 of 100000 seeds reported; 100000 missing", text)
        self.assertLess(len(text.encode()), 5000)

    def test_summary_cli_exits_nonzero_for_missing_artifacts(self):
        plan = self.artifacts / "plan.json"
        plan.write_text(json.dumps(self.plan))
        command = [sys.executable, str(Path(__file__).with_name("sim-campaign.py")), "summary"]
        checked = subprocess.run(command + ["--plan", str(plan), "--artifacts", str(self.artifacts)],
                                 capture_output=True, text=True)
        self.assertEqual(checked.returncode, 1)
        self.assertIn("0 of 6 seeds reported; 6 missing", checked.stdout)
        # Actions uses job outputs: even a failed artifact download leaves
        # enough ownership information to name all the missing seeds.
        checked = subprocess.run(command + ["--artifacts", str(self.artifacts)],
                                 env=dict(os.environ, TEST="TestCampaign", FIRST="389", SEEDS="6",
                                          MATRIX=json.dumps(dict(include=self.plan["jobs"]))),
                                 capture_output=True, text=True)
        self.assertEqual(checked.returncode, 1)
        self.assertIn("Missing seeds 390: seeds (0), process 1", checked.stdout)

    def test_plan_must_cover_the_requested_range(self):
        self.plan["jobs"][0]["processes"][0][0]["first"] += 1
        with self.assertRaises(ValueError):
            self.check(1)


class RunnerTests(unittest.TestCase):
    def run_stub(self, processes, verdict):
        # Exercise the actual shell with a trivial binary, not a simulator.
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "internal/sim").mkdir(parents=True)
            (root / ".gosim/bin").mkdir(parents=True)
            (root / "scripts").mkdir()
            (root / "out").mkdir()
            binary = root / ".gosim/bin/sim.test"
            binary.write_text('#!/usr/bin/env bash\necho "budget $SIM_SEED $SIM_SEEDS $SIM_TWICE"\n'
                              + ('echo "simrun: seed $TEST/$SIM_SEED PASS"\n' if verdict else ""))
            binary.chmod(0o755)
            replay = root / "scripts/replay.sh"
            replay.write_text("#!/usr/bin/env bash\nexit 0\n")
            replay.chmod(0o755)
            with (root / "runner.log").open("w") as output:
                checked = subprocess.run(["bash", str(Path(__file__).with_name("sim-campaign.sh"))],
                                         env=dict(os.environ, GITHUB_WORKSPACE=str(root), RUNNER_TEMP=str(root / "out"),
                                                  TEST="TestCampaign", SHARD="0", PROCESSES=json.dumps(processes)),
                                         stdout=output, stderr=output, timeout=10)
            return checked.returncode, (root / "out/seeds-0-0.log").read_text(), (root / "runner.log").read_text()

    def test_binary_exiting_zero_without_a_seed_fails_the_job(self):
        process = campaign.plan("TestCampaign", 1, 1, [])["jobs"][0]["processes"]
        status, _, log = self.run_stub(process, False)
        self.assertEqual(status, 1)
        self.assertIn("process 0 reported no seed of TestCampaign", log)

    def test_split_queue_passes_allocated_replay_budget_to_binary(self):
        plan = campaign.plan("TestCampaign", 1, 4808, [1052, 3546])
        process = next(p for job in plan["jobs"] for p in job["processes"]
                       if [span["seeds"] for span in p] == [83, 38])
        status, log, _ = self.run_stub([process], True)
        self.assertEqual(status, 0)
        budgets = [line.split()[1:] for line in log.splitlines() if line.startswith("budget ")]
        self.assertEqual([int(b[1]) for b in budgets], [83, 38])
        self.assertEqual([int(b[2]) for b in budgets], [1, 0])


class RecheckTests(unittest.TestCase):
    def test_single_artifact_flat_layout_uses_shard_filename(self):
        plan = campaign.plan("TestSeeds", 1, 1, [])
        with tempfile.TemporaryDirectory() as temp:
            artifacts = Path(temp)
            (artifacts / "seeds-0-0.log").write_text(result("TestSeeds", 1))
            out = io.StringIO()
            self.assertEqual(campaign.summary(plan, artifacts, out), 0)
            self.assertIn("1 of 1 seeds reported", out.getvalue())

    def test_multiple_artifact_layout_uses_shard_filenames(self):
        plan = campaign.plan("TestSeeds", 1, 5, [])
        with tempfile.TemporaryDirectory() as temp:
            artifacts = Path(temp)
            for job in plan["jobs"]:
                directory = artifacts / f"sim-campaign-logs-{job['shard']}"
                directory.mkdir()
                for p, process in enumerate(job["processes"]):
                    log = "".join(result("TestSeeds", seed) for span in process
                                  for seed in range(span["first"], span["first"] + span["seeds"]))
                    (directory / f"seeds-{job['shard']}-{p}.log").write_text(log)
            out = io.StringIO()
            self.assertEqual(campaign.summary(plan, artifacts, out), 0)
            self.assertIn("5 of 5 seeds reported", out.getvalue())

    def test_only_surviving_artifact_keeps_nonzero_shard_identity(self):
        plan = campaign.plan("TestCampaignShapeSeeds", 7, 2, [7, 8])
        with tempfile.TemporaryDirectory() as temp:
            artifacts = Path(temp)
            (artifacts / "seeds-1-0.log").write_text(result("TestCampaignShapeSeeds", 8))
            out = io.StringIO()
            self.assertEqual(campaign.summary(plan, artifacts, out), 1)
            self.assertIn("1 of 2 seeds reported; 1 missing", out.getvalue())
            self.assertIn("Missing seeds 7: seeds (0), process 0", out.getvalue())
            self.assertNotIn("Missing seeds 8:", out.getvalue())

    def test_failed_download_fails_even_with_complete_extracted_logs(self):
        # Execute the workflow's actual final shell, not a copy of its
        # status logic. Readable results must not forgive a digest error.
        root = Path(__file__).resolve().parent.parent
        workflow = (root / ".github/workflows/sim-campaign.yml").read_text()
        step = workflow.split("      - name: Require a result for every requested seed\n", 1)[1]
        shell = textwrap.dedent(step.split("        run: |\n", 1)[1])
        plan = campaign.plan("TestSeeds", 1, 1, [])
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / "campaign/sim-campaign-logs-0"
            directory.mkdir(parents=True)
            # Both names let this test reproduce the old green result
            # before the independent shard-filename change is applied.
            for name in ["seeds-0.log", "seeds-0-0.log"]:
                (directory / name).write_text(result("TestSeeds", 1))
            for outcome, expected in [("success", 0), ("failure", 1)]:
                checked = subprocess.run(["bash", "-e", "-o", "pipefail", "-c", shell], cwd=root,
                                         env=dict(os.environ, TEST="TestSeeds", FIRST="1", SEEDS="1",
                                                  MATRIX=json.dumps(dict(include=plan["jobs"])),
                                                  PLAN_RESULT="success", SEEDS_RESULT="success",
                                                  DOWNLOAD_RESULT=outcome, RUNNER_TEMP=temp,
                                                  GITHUB_STEP_SUMMARY=str(Path(temp) / "summary.md")),
                                         capture_output=True, text=True, timeout=10)
                self.assertIn("1 of 1 seeds reported", checked.stdout)
                self.assertEqual(checked.returncode, expected, checked.stdout + checked.stderr)
        self.assertIn("id: download", workflow)
        self.assertIn("DOWNLOAD_RESULT: ${{ steps.download.outcome }}", step)


if __name__ == "__main__":
    unittest.main()
