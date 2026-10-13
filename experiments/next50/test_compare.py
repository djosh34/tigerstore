import csv
from pathlib import Path
import tempfile
import unittest

from compare import compare


class ComparisonTest(unittest.TestCase):
    def evaluate(self, baseline='baseline', candidate='peer', change=None):
        rows = []
        for rep in range(1, 6):
            for variant, rate, cpu in ((baseline, 100, 10), (candidate, 120, 9), ('etcd-control', 20, 50)):
                rows.append(dict(rep=rep, variant=variant, ops_s=rate,
                                 leader_cpu_us_op=cpu, leader_syscw_op=cpu, p99_ms=cpu))
        if change:
            change(rows)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with (root / 'runs.csv').open('w') as stream:
                writer = csv.DictWriter(stream, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)
            return compare(root, baseline)

    def test_paired_changes(self):
        result = self.evaluate()['peer']['changes']
        self.assertAlmostEqual(result['throughput_gain_percent']['median'], 20)
        self.assertAlmostEqual(result['leader_cpu_saved_percent']['median'], 10)
        self.assertEqual(result['leader_cpu_saved_us_put']['median'], 1)

    def test_profile_labels(self):
        self.assertEqual(set(self.evaluate('baseline-profile', 'peer-profile')), {'peer-profile'})

    def test_duplicate_baseline(self):
        with self.assertRaises(ValueError):
            self.evaluate(change=lambda rows: rows.append(rows[0].copy()))

    def test_missing_candidate(self):
        with self.assertRaises(ValueError):
            self.evaluate(change=lambda rows: rows.pop(1))


if __name__ == '__main__':
    unittest.main()
