import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('option_a', Path(__file__).with_name('fetch-option-a-profile.py'))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class OptionAProfiles(unittest.TestCase):
    def test_frozen_pre_correction_refs(self):
        self.assertEqual(m.BASE, '55b32bb9e6ef692cdb9d1b0e3cdb16542811dec4')
        self.assertEqual(m.HEAD, '0c41156a5dc285c1cffc79f80c11b725235031fd')

    def test_three_requested_diagnostic_cells(self):
        self.assertEqual(m.CASES['watch'], dict(bench='watch-1k', seed=94125874, rtt=0))
        self.assertEqual(m.CASES['txn'], dict(bench='txn-1k', seed=94125874, rtt=2))
        self.assertEqual(m.CASES['k8s'], dict(bench='k8s', seed=613206478, rtt=0))
        self.assertNotEqual(m.CASES['k8s']['seed'], 22869534)


if __name__ == '__main__':
    unittest.main()
