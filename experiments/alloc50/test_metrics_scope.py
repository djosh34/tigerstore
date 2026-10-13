#!/usr/bin/env python3
"""Metrics sampling scope regression; no cluster or Go execution."""
import os
from pathlib import Path
import runpy
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent


class MetricsScopeTests(unittest.TestCase):
    def test_only_primary_60000_put_has_runtime_metrics(self):
        with patch.dict(os.environ, {'CLIENTS': '64', 'RUNNER_TEMP': str(HERE)}):
            args = runpy.run_path(str(HERE/'run.py'))['timing_metrics_args']
        self.assertEqual(args('primary', 'put', 60000, 2), [
            '-metrics-url', 'http://127.0.0.1:6062/debug/runtime', '-metrics-kind', 'runtime'])
        for mode, name, total in [
            ('primary', 'seed', 4096), ('primary', 'warmup', 20000),
            ('profile', 'seed', 4096), ('profile', 'warmup', 20000),
            ('profile', 'put', 2000), ('cases', 'put', 60000),
            ('primary', 'put', 2000),
        ]:
            with self.subTest(mode=mode, name=name, total=total):
                self.assertEqual(args(mode, name, total, 0), [])


if __name__ == '__main__':
    unittest.main()
