import importlib.util
import json
from pathlib import Path
import unittest

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

    def test_borderline_requires_extra_five(self):
        self.assertEqual(m.decide(pairs(1.01), False)["status"], "MORE_PAIRS")
        self.assertEqual(m.decide(pairs(1.01, 10), False)["status"], "PASS")
        self.assertEqual(m.decide(pairs(.99, 10), False)["status"], "RULING")

    def test_ratio_of_medians_not_median_of_ratios(self):
        p = pairs()
        for pair, b, c in zip(p, [1, 2, 3, 100, 200], [1.1, 2.2, 2.3, 1000, 2000]):
            pair["baseline"]["throughput"], pair["candidate"]["throughput"] = b, c
        report = m.decide(p, False)
        self.assertLess(report["first_five_ratio"], .98)
        self.assertEqual(report["status"], "RULING")

    def test_tail_refusal_and_target_rulings(self):
        p = pairs()
        p[0]["candidate"]["latency_ms"]["max"] = 1001
        self.assertEqual(m.decide(p, False)["status"], "RULING")
        p = pairs()
        p[0]["candidate"]["refusals"]["total"] = 1
        self.assertEqual(m.decide(p, False)["status"], "RULING")
        p = pairs()
        for pair in p: pair["candidate"]["latency_ms"]["p95"] += 1
        self.assertEqual(m.decide(p, False)["status"], "RULING")

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


if __name__ == "__main__": unittest.main()
