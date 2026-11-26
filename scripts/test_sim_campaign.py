"""Planner and completeness regressions; no simulator seeds are run."""
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("sim_campaign", Path(__file__).with_name("sim-campaign.py"))
campaign = importlib.util.module_from_spec(spec)
spec.loader.exec_module(campaign)


def result(test, seed, status="PASS"):
    return f"=== RUN   {test}/{seed}\n    --- {status}: {test}/{seed} (0.01s)\n"


class PlannerTests(unittest.TestCase):
    def check_plan(self, test, first, seeds, longs):
        plan = campaign.plan(test, first, seeds, longs)
        seen = []
        for shard, job in enumerate(plan["jobs"]):
            self.assertEqual(job["shard"], shard)
            self.assertLessEqual(len(job["processes"]), 4)
            owned = [seed for process in job["processes"] for span in process
                     for seed in range(span["first"], span["first"] + span["seeds"])]
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

    def test_two_longs(self):
        # These allocator outputs are also checked by TestCampaignLongSeeds.
        self.check_plan("TestCampaign", 1, 4808, [1052, 3546])
        self.check_plan("TestCampaign", 1052, 2495, [1052, 3546])

    def test_shape_seeds(self):
        plan = self.check_plan("TestCampaignShapeSeeds", 17, 8, list(range(17, 25)))
        self.assertEqual(len(plan["jobs"]), 8)
        self.check_plan("TestCampaignShapeSeeds", 1, 256, list(range(1, 257)))

    def test_no_longs_preserves_process_sizes(self):
        plan = self.check_plan("TestSeeds", 0, 100_000, [])
        self.assertEqual(len(plan["jobs"]), 10)
        for job in plan["jobs"]:
            self.assertEqual([p[0]["seeds"] for p in job["processes"]], [2500] * 4)
        self.check_plan("TestSeeds", 7, 1, [])
        self.check_plan("TestSeeds", 7, 3, [])

    def test_matrix_limit_and_invalid_inputs(self):
        for test, first, seeds, longs in [
            ("TestCampaignShapeSeeds", 1, 257, list(range(1, 258))),
            ("TestCampaign", 0, 1000, list(range(255))),  # 255 long + ordinary jobs
            ("TestCampaignShapeSeeds", 1, 3, []),
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
            directory = self.artifacts / f"sim-campaign-{job['shard']}"
            directory.mkdir()
            for p, process in enumerate(job["processes"]):
                text = "".join(result(self.plan["test"], seed) for span in process
                               for seed in range(span["first"], span["first"] + span["seeds"]))
                (directory / f"seeds-{p}.log").write_text(text)

    def check(self, status):
        out = io.StringIO()
        self.assertEqual(campaign.summary(self.plan, self.artifacts, out), status)
        return out.getvalue()

    def test_missing_seed_and_job_without_artifact(self):
        self.logs()
        # Seed 390 started but never finished. Job 1 uploaded nothing.
        (self.artifacts / "sim-campaign-0/seeds-1.log").write_text("=== RUN   TestCampaign/390\n")
        for path in (self.artifacts / "sim-campaign-1").iterdir():
            path.unlink()
        (self.artifacts / "sim-campaign-1").rmdir()
        text = self.check(1)
        self.assertIn("Missing seed 390: seeds (0), process 1", text)
        self.assertIn("Missing seed 393: seeds (1), process 0", text)
        self.assertIn("Missing seed 394: seeds (1), process 1", text)
        self.assertIn("3 of 6 seeds reported; 3 missing", text)

    def test_known_failure_skip_counts_but_other_skips_do_not(self):
        self.logs()
        path = self.artifacts / "sim-campaign-0/seeds-1.log"
        skip = result("TestCampaign", 390, "SKIP")
        path.write_text(skip)
        self.check(1)
        path.write_text("=== RUN   TestCampaign/390\n"
                        "known failure: seed 390 of TestCampaign fails because of #123 (known-failures.txt)\n"
                        "    --- SKIP: TestCampaign/390 (0.00s)\n")
        self.assertIn("6 of 6 seeds reported", self.check(0))

    def test_aged_image_and_forks_are_not_driver_completion(self):
        self.logs()
        path = self.artifacts / "sim-campaign-0/seeds-1.log"
        # The driver's t.Log embeds gosim's image and fork results at a
        # deeper indentation. Even four fork passes cannot replace the
        # driver's result (for example a replay/hash check may not finish).
        image = ("=== RUN   TestCampaign/390\n"
                 "    campaign_test.go:132: === RUN   TestCampaign/390\n"
                 "        --- PASS: TestCampaign/390 (1.00s)\n")
        path.write_text(image)
        self.check(1)
        path.write_text(image + "        --- PASS: TestCampaign/390 (1.00s)\n" * 4)
        self.check(1)
        path.write_text(path.read_text() + "    --- PASS: TestCampaign/390 (1.00s)\n")
        self.check(0)

    def test_fail_is_reported_and_test_variants_do_not_match(self):
        self.logs()
        path = self.artifacts / "sim-campaign-0/seeds-1.log"
        path.write_text(result("TestCampaign", 390, "FAIL"))
        self.check(0)  # The workflow separately propagates seed-job failure.
        for test in ["TestCampaignShapeSeeds", "TestCampaignOther", "TestCampaign/variant"]:
            path.write_text(result(test, 390))
            self.check(1)
        path.write_text("    --- PASS: TestCampaign/390 (0.00s)\n")
        self.check(1)  # A result without its driver's RUN is not accepted.

    def test_summary_cli_exits_nonzero_for_missing_artifacts(self):
        plan = self.artifacts / "plan.json"
        plan.write_text(json.dumps(self.plan))
        checked = subprocess.run([sys.executable, str(Path(__file__).with_name("sim-campaign.py")),
                                  "summary", "--plan", str(plan), "--artifacts", str(self.artifacts)],
                                 capture_output=True, text=True)
        self.assertEqual(checked.returncode, 1)
        self.assertIn("0 of 6 seeds reported; 6 missing", checked.stdout)

    def test_plan_must_cover_the_requested_range(self):
        self.plan["jobs"][0]["processes"][0][0]["first"] += 1
        with self.assertRaises(ValueError):
            self.check(1)


if __name__ == "__main__":
    unittest.main()
