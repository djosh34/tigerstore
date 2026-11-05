#!/usr/bin/env python3
import math
import unittest

import analyze as a


def row(seed=1, caught=True, wall_ms=1000, **kwargs):
    return dict(dict(bug='B01a', D=5, seed=seed, phase='survey', caught=caught,
                     other_fail=False, baseline_fail=False, wall_ms=wall_ms,
                     control_wall_ms=1000 if caught else 0, finding='expected' if caught else '',
                     run_id='1', commit='abc'), **kwargs)


class AnalysisTests(unittest.TestCase):
    def test_wilson_edges(self):
        lo, hi = a.wilson(0, 100)
        self.assertAlmostEqual(lo, 0)
        self.assertAlmostEqual(hi, 0.0369948075)
        lo1, hi1 = a.wilson(100, 100)
        self.assertAlmostEqual(lo1, 1-hi)
        self.assertAlmostEqual(hi1, 1)
        with self.assertRaises(ValueError):
            a.wilson(0, 0)

    def test_whole_trials_only(self):
        self.assertEqual(a.distinct(1, .99), 0)
        self.assertEqual(a.distinct(1, 1), 1)
        self.assertAlmostEqual(a.distinct(.5, 2.9), .75)
        self.assertEqual(a.distinct(0, 999), 0)
        self.assertAlmostEqual(a.distinct(1e-12, 100), 1e-10)

    def test_rates_controls_and_missing_denominator(self):
        rows = [row(), row(seed=2, caught=False), row(phase='control', bug='plain', caught=False)]
        result = a.analyze(rows)
        c = result['records'][0]
        self.assertEqual(c['seeds'], 2)
        self.assertEqual(c['catches'], 1)
        self.assertEqual(c['catches_per_h'], 1800)
        self.assertEqual(c['catches_per_h_4_est'], 6120)
        self.assertEqual(c['standalone_validated_catches_per_h'], 1200)
        self.assertAlmostEqual(result['totals'][0]['measured_runner_h_excluding_builds'], 3/3600)
        self.assertEqual(result['totals'][0]['equal_share_catches_per_h'], 1800/29)
        self.assertIn('plain/5', result['missing'])

    def test_completed_seeds_budget(self):
        rows = [row(caught=False, wall_ms=1000), row(seed=2, wall_ms=2000)]
        self.assertEqual(a.observed_within(rows, 2.99), 0)
        self.assertEqual(a.observed_within(rows, 3), 1)
        self.assertEqual(a.observed_within(rows, 3, controls=True), 0)
        self.assertEqual(a.observed_within(rows, 4, controls=True), 1)

    def test_recovery_prefix_sort_and_duplicates(self):
        result = a.analyze([row(seed=2), row(seed=1)])
        self.assertEqual(result['records'][0]['seeds'], 2)
        for rows in [[row(), row()], [row(seed=2)]]:
            with self.assertRaises(ValueError):
                a.analyze(rows)

    def test_invalid_rows(self):
        for r in [row(other_fail=True), row(baseline_fail=True), row(control_wall_ms=0),
                  row(wall_ms=0), row(wall_ms=math.nan), row(bug='bogus')]:
            with self.assertRaises(ValueError):
                a.analyze([r])


if __name__ == '__main__':
    unittest.main()
