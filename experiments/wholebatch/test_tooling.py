#!/usr/bin/env python3
"""Synthetic artifact and source-overlay checks. No server or benchmark runs."""
import contextlib
import csv
import io
import json
from pathlib import Path
import tempfile
import unittest

from instrument import instrument
from summarize import summarize


class ToolingTest(unittest.TestCase):
    def test_overlay_is_explicit_and_refuses_duplicates(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as tmp:
            source = Path(tmp)
            (source/'cmd/tigerstore').mkdir(parents=True)
            (source/'internal/osfs').mkdir(parents=True)
            main = source/'cmd/tigerstore/main.go'
            main.write_text('package main\n\tt, err := peer.Start(peer.Config{\n')
            instrument(source)
            self.assertIn('pre50Observe(n)', main.read_text())
            self.assertTrue((source/'internal/osfs/pre50.go').exists())
            with self.assertRaises(RuntimeError):
                instrument(source)

    def test_five_run_summary_and_missing_repetition(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as tmp:
            root = Path(tmp)/'c16'
            root.mkdir()
            names = ('main', 'etcd', 'rpc', 'admit', 'full')
            variants = [dict(name=n, ref=n) for n in names]
            (root/'variants.json').write_text(json.dumps(variants))
            for rep in range(1, 6):
                for name in names:
                    dest = root/f'{rep}-{name}'
                    dest.mkdir()
                    result = dict(valid=True, errors={}, successes=60000, workers=16,
                                  conns=16, keyspace=4096, value_bytes=1024,
                                  cpu_us_op=[10, 20, 30, 4], occupied_cores=[1, 1, 1, .2],
                                  alloc_before=[dict(mallocs=100, bytes=1000)]*3,
                                  alloc_after=[dict(mallocs=60100, bytes=60001000)]*3,
                                  ops_s=10000, mean_ms=1, p50_ms=1, p90_ms=2,
                                  p95_ms=3, p99_ms=4, p999_ms=5, elapsed_s=6)
                    (dest/'put.json').write_text(json.dumps(result))
                    layout = dict(server_go_processors=[2]*3, server_cpus_in_role_order=[0, 1, 2],
                                  generator_cpu=3, generator_connections=16, variant=dict(name=name, ref=name))
                    (dest/'layout.json').write_text(json.dumps(layout))
                    key = 'etcd_disk_wal_fsync_duration_seconds_count' if name == 'etcd' else 'syncs'
                    (dest/'disk.json').write_text(json.dumps(dict(before=[{key: 10}]*3, after=[{key: 110}]*3)))
            with contextlib.redirect_stdout(io.StringIO()):
                summarize(root)
            self.assertIn('1000.0', (root/'summary.csv').read_text())
            self.assertIn('p95_ms', (root/'runs.csv').read_text())
            with (root/'paired.csv').open() as f:
                pairs = list(csv.DictReader(f))
            for candidate, baseline in (('admit', 'rpc'), ('full', 'admit')):
                selected = [p for p in pairs if p['candidate'] == candidate and p['baseline'] == baseline]
                self.assertEqual(len(selected), 5)
                self.assertTrue(all(float(p['ops_s_ratio']) == 1 for p in selected))
                self.assertIn(f'{candidate}/{baseline}', (root/'summary.md').read_text())
            (root/'5-full/put.json').unlink()
            with self.assertRaises(AssertionError), contextlib.redirect_stdout(io.StringIO()):
                summarize(root)


if __name__ == '__main__':
    unittest.main()
