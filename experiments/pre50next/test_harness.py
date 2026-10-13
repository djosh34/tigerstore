#!/usr/bin/env python3
"""Syntax/source contract checks only. No Go execution or server launch."""
import datetime
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent


def module(name):
    spec = importlib.util.spec_from_file_location(name, HERE / f'{name}.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


runner = module('run')
instrument = module('instrument')


class HarnessTests(unittest.TestCase):
    def test_schedule(self):
        primary = list(runner.schedule([], 'primary', False))
        self.assertEqual(len(primary), 5)
        self.assertEqual([s[0] for s in primary[0][1]], ['baseline', 'etcd'])
        self.assertEqual([s[0] for s in primary[1][1]], ['etcd', 'baseline'])
        profiles = list(runner.schedule([], 'profile', False))
        self.assertEqual(sum(p for _, cohort in profiles for _, _, p in cohort), 5)
        self.assertTrue(all(len(cohort) == 3 for _, cohort in profiles))

    def test_host_restrictions(self):
        host = list(runner.schedule([{'name': 'patch'}], 'primary', True))
        self.assertEqual(len(host), 1)
        self.assertEqual([s[0] for s in host[0][1]], ['baseline', 'etcd'])
        with self.assertRaises(AssertionError):
            list(runner.schedule([], 'profile', True))

    def test_deadline(self):
        with patch.object(runner, 'DEADLINE', datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(seconds=1)):
            with self.assertRaises(RuntimeError):
                runner.run(['must-not-start'])

    def test_instrumentation(self):
        with tempfile.TemporaryDirectory(dir=HERE) as tmp:
            root = Path(tmp)
            (root / 'cmd/tigerstore').mkdir(parents=True)
            (root / 'internal/osfs').mkdir(parents=True)
            main = root / 'cmd/tigerstore/main.go'
            main.write_text('\tt, err := peer.Start(peer.Config{\n')
            instrument.instrument(root)
            self.assertIn('pre50Observe(n)', main.read_text())
            with self.assertRaises(RuntimeError):
                instrument.instrument(root)
        text = (HERE / 'observe.go.txt').read_text()
        self.assertIn('mux.HandleFunc("/debug/pprof/profile", pprof.Profile)', text)
        self.assertIn('http.NewServeMux()', text)
        self.assertNotIn('SetBlockProfileRate', text)
        self.assertNotIn('SetMutexProfileFraction', text)

    def test_generator_contract(self):
        text = (HERE / 'load/main.go').read_text()
        for token in ('const keyspace = 4096', 'make([]byte, 1024)', '"barrier-dir"', '"drained", "release"'):
            self.assertIn(token, text)
        refs = json.loads((HERE / 'refs.json').read_text())
        self.assertEqual(refs['base'], '396bced923ee47540e89e1e2ca1053ecde5673ce')
        self.assertEqual(refs['provenance']['bench68'], '32ceb80fb36fd4229c2e2e7594b40da197fe4eaf')


if __name__ == '__main__':
    unittest.main()
