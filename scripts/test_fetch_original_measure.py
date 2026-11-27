import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("original", Path(__file__).with_name("fetch-original-measure.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def row(candidate=False):
    return dict(schema=1, case="original-80-range", correct=True, routing=candidate,
                memory_budget_bytes=96 << 20, keys=8000, value_bytes=16388, ranges=80,
                wall_ms=100, throughput=800,
                outcomes=[dict(first=i, elapsed_ms=90, retries=0) for i in range(0, 8000, 100)],
                latency_ms=dict(p95=90, p99=90, max=90),
                refusals=dict(total=0, max_per_range=0),
                fetch=dict(jobs=1, values=8000), fetch_owned_peak=100000,
                fetch_owned_limit=(96 << 20) // 32, nowait_refused=True,
                io=dict(nowait_calls=1, blocking_reads=1, nowait_bytes=0))


def pairs(ratio=1.1, n=5):
    result = []
    for i in range(n):
        base, candidate = row(), row(True)
        candidate["throughput"] *= ratio
        result.append(dict(pair=i+1, order="AB" if i % 2 == 0 else "BA", baseline=base, candidate=candidate))
    return result


class OriginalMeasurements(unittest.TestCase):
    def adapt(self, r=None, side="baseline"):
        text = "    file.go:1: FETCH_MEASURE " + json.dumps(r or row()) + "\n--- PASS: TestFetchOriginalMeasurement (1s)\n"
        return m.adapt(text, "a" * 40, side, 1, False, 0, "raw.log", "b" * 64)

    def test_valid_samples(self):
        self.assertEqual(self.adapt()["ref"], "a" * 40)
        self.adapt(row(True), "candidate")

    def test_missing_sample(self):
        r = row()
        r["outcomes"].pop()
        with self.assertRaises(ValueError): self.adapt(r)

    def test_duplicate_sample(self):
        r = row()
        r["outcomes"][1] = r["outcomes"][0]
        with self.assertRaises(ValueError): self.adapt(r)

    def test_no_disabled_candidate(self):
        with self.assertRaises(ValueError): self.adapt(row(), "candidate")

    def test_no_fabricated_aggregates(self):
        for field in ("latency_ms", "refusals"):
            r = row()
            key = next(iter(r[field]))
            r[field][key] += 1
            with self.assertRaises(ValueError): self.adapt(r)

    def test_share_and_fetch_assertions(self):
        for change in (dict(fetch_owned_peak=1 << 30), dict(fetch=dict(jobs=0, values=8000)), dict(fetch=dict(jobs=1, values=1))):
            r = row()
            r.update(change)
            with self.assertRaises(ValueError): self.adapt(r)

    def test_nonfinite_or_negative_samples(self):
        for value in (float("nan"), float("inf"), -1, 0, True):
            r = row()
            r["outcomes"][0]["elapsed_ms"] = value
            with self.assertRaises(ValueError): self.adapt(r)

    def test_refused_preparation_required(self):
        r = row()
        r["nowait_refused"] = False
        with self.assertRaises(ValueError): self.adapt(r)

    def test_pass(self): self.assertEqual(m.decide(pairs(), False)["status"], "PASS")

    def test_ruling_below_098(self): self.assertEqual(m.decide(pairs(.97), False)["status"], "RULING")

    def test_five_pairs_accept_098_and_borderline_without_extension(self):
        for ratio in (.98, .99, 1.01):
            self.assertEqual(m.decide(pairs(ratio), False)["status"], "PASS")
        with self.assertRaises(ValueError):
            m.decide(pairs(1.01, 10), False)

    def test_each_ratio_not_a_median_decides_the_gate(self):
        p = pairs()
        for pair, b, c in zip(p, [1, 2, 3, 100, 200], [1.1, 2.2, 2.3, 1000, 2000]):
            pair["baseline"]["throughput"], pair["candidate"]["throughput"] = b, c
        report = m.decide(p, False)
        self.assertLess(report["paired_ratios"][2], .98)
        self.assertFalse(report["pairs"][2]["throughput_pass"])
        self.assertEqual(report["status"], "RULING")

    def test_p99_max_and_refusal_checks_remain_information(self):
        p = pairs()
        for pair in p:
            pair["candidate"]["latency_ms"].update(p99=1001, max=1001)
        p[0]["candidate"]["refusals"].update(total=1, max_per_range=1)
        report = m.decide(p, False)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(set(report["worsened_tails"]), {"p99", "max"})
        self.assertEqual(report["sides"]["candidate"]["latency_ms"]["p99"]["max"], 1001)
        self.assertEqual(report["refusal_regressions"], ["total", "max_per_range"])
        self.assertTrue(report["design_figures"][0]["candidate_over"])

    def test_p95_beyond_noise_still_rules_even_below_design_figure(self):
        p = pairs()
        for pair in p: pair["candidate"]["latency_ms"]["p95"] += 1
        report = m.decide(p, False)
        self.assertEqual(report["status"], "RULING")
        self.assertEqual(report["noise_ms"], 0)
        self.assertTrue(all(not info["candidate_over"] for info in report["design_figures"]))

    def test_over_913_design_figures_are_not_a_gate(self):
        for race, limit in ((False, 1000), (True, 3000)):
            p = pairs()
            for pair in p:
                for side in ("baseline", "candidate"):
                    pair[side]["latency_ms"] = dict(p95=limit+100, p99=limit+200, max=limit+200)
            report = m.decide(p, race)
            self.assertEqual(report["status"], "PASS")
            self.assertEqual(report["reasons"], [])
            self.assertEqual(report["baseline_p95_ms"], [limit+100]*5)
            for info in report["design_figures"]:
                self.assertEqual(info["limit_ms"], limit)
                self.assertTrue(info["candidate_over"] and info["main_over"])
                self.assertIn("information: over #913's 1 s / 3 s design figure (main:", info["message"])
                self.assertIn(f"p95 {limit+100} ms", info["message"])

    def test_design_figure_boundary_and_main_only_miss(self):
        for race, limit in ((False, 1000), (True, 3000)):
            p = pairs()[0]
            p["candidate"]["latency_ms"] = dict(p95=limit, p99=limit, max=limit)
            p["baseline"]["latency_ms"] = dict(p95=limit+1, p99=limit+1, max=limit+1)
            info = m.design_figure(p, race)
            self.assertFalse(info["candidate_over"])
            self.assertTrue(info["main_over"])
            p["candidate"]["latency_ms"]["max"] += .001
            self.assertTrue(m.design_figure(p, race)["candidate_over"])

    def test_worse_p95_within_predeclared_noise_passes(self):
        p = pairs()
        for i, pair in enumerate(p):
            base = 1100 + 100*i
            pair["baseline"]["latency_ms"] = dict(p95=base, p99=base+500, max=base+500)
            pair["candidate"]["latency_ms"] = dict(p95=base+100, p99=base+500, max=base+500)
        report = m.decide(p, False)
        self.assertEqual(report["status"], "PASS")
        self.assertAlmostEqual(report["noise_ms"], 148.26)
        self.assertIn("p95", report["worsened_tails"])
        self.assertTrue(all(info["candidate_over"] for info in report["design_figures"]))

    def test_five_pairs_and_order(self):
        self.assertEqual(m.decide(pairs(n=4), False)["status"], "INCOMPLETE")
        p = pairs()
        p[0]["order"] = "BA"
        with self.assertRaises(ValueError): m.decide(p, False)

    def test_overlay_anchor_refuses_drift(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "source"
            path.write_text("old old")
            with self.assertRaises(ValueError): m.replace_once(path, "old", "new")
            self.assertEqual(path.read_text(), "old old")


class SyntheticCampaign(unittest.TestCase):
    """Exercise orchestration with fake binaries only; never run a campaign here."""

    def campaign(self, race=False, target=m, bad_ratio=False, bad_test=False, bad_samples=False):
        original = m if target is m else target.original
        starts = []
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            args = SimpleNamespace(root=root, out=root/"evidence", baseline="a"*40,
                                   candidate="b"*40, race=race, sync_delay_ms=5)

            def fake_binary(argv, stdout, **kwargs):
                if argv[0] == "go":  # The compatibility runner builds directly.
                    return SimpleNamespace(returncode=0)
                side = Path(argv[0]).stem
                starts.append(side)
                self.assertIn("6m", argv)
                self.assertEqual(kwargs["env"]["FETCH_MEASURE_NOWAIT_REFUSED"], "1")
                if bad_test:
                    stdout.write("--- FAIL: TestFetchOriginalMeasurement\n")
                    return SimpleNamespace(returncode=1)
                data = row(side == "candidate")
                elapsed = 4000 if side == "candidate" else 4100
                data["wall_ms"] = (6000 if bad_ratio else 4500) if side == "candidate" else 5000
                data["throughput"] = 80000/data["wall_ms"]
                data["latency_ms"] = dict(p95=elapsed, p99=elapsed, max=elapsed)
                for sample in data["outcomes"]:
                    sample["elapsed_ms"] = elapsed
                if bad_samples:
                    data["outcomes"].pop()
                stdout.write("FETCH_MEASURE "+json.dumps(data)+"\n--- PASS: TestFetchOriginalMeasurement\n")
                return SimpleNamespace(returncode=0)

            with patch.dict(m.os.environ, GITHUB_ACTIONS="true", RUNNER_TEMP=td, FETCH_MEASURE_RUN_URL="synthetic-only"), \
                 patch.object(original, "command"), patch.object(original, "prepare"), \
                 patch.object(target.subprocess, "run", side_effect=fake_binary), patch("builtins.print"):
                result = target.run(args)
            manifest = json.loads((args.out/"manifest.json").read_text())
        return result, manifest, starts

    def test_both_entry_points_finish_all_pairs_above_both_design_figures(self):
        wrapper_spec = importlib.util.spec_from_file_location("relative_compatibility", Path(__file__).with_name("fetch-pr897-measure.py"))
        wrapper = importlib.util.module_from_spec(wrapper_spec)
        wrapper_spec.loader.exec_module(wrapper)
        for target in (m, wrapper):
            for race in (False, True):
                result, manifest, starts = self.campaign(race=race, target=target)
                self.assertEqual(result, 0)
                self.assertEqual(manifest["status"], "PASS")
                self.assertEqual(starts, ["baseline", "candidate", "candidate", "baseline", "baseline", "candidate", "candidate", "baseline", "baseline", "candidate"])
                self.assertEqual(len(manifest["pairs"]), 5)
                self.assertEqual(manifest["report"]["baseline_p95_ms"], [4100]*5)
                self.assertEqual(len(manifest["report"]["information"]), 5)
                for pair, gate_pair, info in zip(manifest["pairs"], manifest["report"]["pairs"], manifest["report"]["design_figures"]):
                    self.assertEqual(pair["design_figure"], info)
                    self.assertEqual(gate_pair["design_figure"], info)
                    self.assertTrue(info["candidate_over"])
                    self.assertEqual(info["limit_ms"], 3000 if race else 1000)

    def test_real_throughput_failure_still_stops_immediately(self):
        result, manifest, starts = self.campaign(bad_ratio=True)
        self.assertEqual(result, 1)
        self.assertEqual(len(starts), 2)
        self.assertEqual(manifest["status"], "RULING")
        self.assertIn("throughput ratio", manifest["report"]["reasons"][0])
        self.assertTrue(manifest["pairs"][0]["design_figure"]["candidate_over"])

    def test_original_correctness_failure_still_stops(self):
        result, manifest, starts = self.campaign(bad_test=True)
        self.assertEqual((result, len(starts)), (1, 1))
        self.assertEqual(manifest["status"], "RULING")
        self.assertIn("correctness/measurement test failed", manifest["reasons"][0])

    def test_unqualified_samples_still_cannot_pass(self):
        result, manifest, starts = self.campaign(bad_samples=True)
        self.assertEqual((result, len(starts)), (2, 1))
        self.assertEqual(manifest["status"], "INCOMPLETE")


if __name__ == "__main__": unittest.main()
