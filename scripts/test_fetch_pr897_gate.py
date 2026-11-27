import copy
import importlib.util
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gate = load('pr897_gate', 'fetch-pr897-gate.py')
legacy = load('absolute_gate', 'fetch-original-measure.py')
runner = load('relative_runner', 'fetch-pr897-measure.py')


def pairs():
    result = []
    for i, p95 in enumerate([1500, 1510, 1520, 1530, 1540]):
        base = dict(throughput=100., wall_ms=800., latency_ms=dict(p95=p95, p99=p95+100, max=p95+100),
                    refusals=dict(total=0, max_per_range=0))
        candidate = copy.deepcopy(base)
        candidate['throughput'] = 102.
        candidate['latency_ms']['p95'] += 10
        result.append(dict(pair=i+1, order='AB' if i % 2 == 0 else 'BA', case='original-80-range', baseline=base, candidate=candidate))
    return result


class PR897Gate(unittest.TestCase):
    def test_noise_is_predeclared_baseline_only_mad(self):
        report = gate.assess(pairs())
        self.assertEqual(report['baseline_p95_median_ms'], 1520)
        self.assertEqual(report['baseline_p95_mad_ms'], 10)
        self.assertAlmostEqual(report['noise_ms'], 14.826)
        self.assertEqual(report['status'], 'PASS')

    def test_absolute_target_is_not_the_relative_gate(self):
        # All p95s exceed the untouched absolute target; option A passes.
        data = pairs()
        self.assertEqual(gate.assess(data)['status'], 'PASS')
        for row in data:
            for side in ('baseline', 'candidate'):
                row[side]['latency_ms'] = dict(p95=1500, p99=1600, max=1600)
        old = legacy.decide(data, False)
        self.assertEqual(old['status'], 'RULING')
        self.assertTrue(any('1000-ms' in reason for reason in old['reasons']))

    def test_every_pair_not_the_median_must_clear_098(self):
        data = pairs()
        data[2]['candidate']['throughput'] = 97.9
        report = gate.assess(data)
        self.assertEqual(report['status'], 'RULING')
        self.assertFalse(report['pairs'][2]['throughput_pass'])
        self.assertIn('pair 3 throughput', report['reasons'][0])

    def test_early_throughput_failure_needs_no_five_pair_noise(self):
        data = pairs()[:1]
        data[0]['candidate']['throughput'] = 97
        report = gate.assess(data)
        self.assertEqual(report['status'], 'RULING')
        self.assertIsNone(report['noise_ms'])

    def test_exact_098_is_accepted(self):
        data = pairs()
        for row in data:
            row['candidate']['throughput'] = 98.
        self.assertEqual(gate.assess(data)['status'], 'PASS')

    def test_candidate_variation_cannot_expand_noise(self):
        data = pairs()
        data[4]['candidate']['latency_ms']['p95'] = 2000
        report = gate.assess(data)
        self.assertAlmostEqual(report['noise_ms'], 14.826)
        self.assertEqual(report['status'], 'RULING')
        self.assertFalse(report['pairs'][4]['p95_pass'])

    def test_zero_baseline_noise_has_no_invented_floor(self):
        data = pairs()
        for row in data:
            row['baseline']['latency_ms']['p95'] = 1500
            row['candidate']['latency_ms']['p95'] = 1501
        report = gate.assess(data)
        self.assertEqual(report['noise_ms'], 0)
        self.assertEqual(report['status'], 'RULING')

    def test_less_than_five_cannot_pass(self):
        self.assertEqual(gate.assess(pairs()[:4])['status'], 'INCOMPLETE')
        self.assertEqual(gate.assess([])['status'], 'INCOMPLETE')

    def test_all_raw_tails_and_refusals_remain_visible(self):
        report = gate.assess(pairs())
        for side in ('baseline', 'candidate'):
            self.assertEqual(set(report['sides'][side]['latency_ms']), {'p95', 'p99', 'max'})
            self.assertEqual(report['sides'][side]['refusals']['total'], [0]*5)
            self.assertIn('iqr', report['sides'][side]['throughput'])

    def test_independent_refusal_regression_is_not_lost(self):
        data = pairs()
        data[0]['candidate']['refusals'] = dict(total=1, max_per_range=1)
        report = gate.assess(data)
        self.assertEqual(report['status'], 'RULING')
        self.assertTrue(any('refusal regression' in r for r in report['reasons']))

    def test_order_and_count_are_checked(self):
        data = pairs()
        data[1]['order'] = 'AB'
        with self.assertRaises(ValueError):
            gate.assess(data)
        with self.assertRaises(ValueError):
            gate.assess(pairs()+[pairs()[0]])

    def test_nonfinite_and_nonpositive_data_are_rejected(self):
        for value in (float('nan'), float('inf'), 0, -1, True):
            for field in ('throughput', 'p95'):
                data = pairs()
                if field == 'throughput':
                    data[0]['candidate'][field] = value
                else:
                    data[0]['candidate']['latency_ms'][field] = value
                with self.assertRaises(ValueError):
                    gate.assess(data)

    def test_runner_will_not_execute_campaign_on_host(self):
        with patch.dict(os.environ, GITHUB_ACTIONS='false'):
            with self.assertRaisesRegex(ValueError, 'Actions-only'):
                runner.run(SimpleNamespace())


if __name__ == '__main__':
    unittest.main()
