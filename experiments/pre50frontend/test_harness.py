"""Synthetic checks only. Does not launch servers, load, or profiles."""
import json
from pathlib import Path
import tempfile
import unittest

import attribute
import run
import summarize

HERE = Path(__file__).resolve().parent


class HarnessTests(unittest.TestCase):
    def test_orders(self):
        schedule = list(run.orders(['a', 'etcd']))
        self.assertEqual(schedule, [(1, ['a', 'etcd']), (2, ['etcd', 'a']),
                                    (3, ['a', 'etcd']), (4, ['etcd', 'a']),
                                    (5, ['a', 'etcd'])])

    def test_metrics(self):
        m = run.metrics('# HELP ignored\ngo_memstats_mallocs_total 123\nwith_labels{x="y"} 8\n')
        self.assertEqual(m, {'go_memstats_mallocs_total': 123})

    def test_watch(self):
        response = {'Events': [{'kv': {'key': 'L3ByZTUwLXNtb2tl', 'value': 'c2Vjb25k', 'mod_revision': 42}}]}
        self.assertTrue(run.watch_matches(json.dumps(response), 42))
        self.assertFalse(run.watch_matches(json.dumps(response), 43))
        response['Events'][0]['kv']['key'] = 'b3RoZXI='
        self.assertFalse(run.watch_matches(json.dumps(response), 42))
        self.assertFalse(run.watch_matches('incomplete {', 42))
        self.assertFalse(run.watch_matches(json.dumps({'value': 'c2Vjb25k'}), 42))

    def test_pprof_parser_and_weighting(self):
        raw = 'Samples:\n  2 20000000: 1 2\nLocations\n  1: runtime.mallocgc\n  2: internal/core.Process\nMappings\n'
        samples = attribute.parse(raw)
        self.assertEqual(samples[0][0], 20000000)
        a = attribute.attribution(samples+[(80000000, 'crypto/tls.Conn')], 100)
        self.assertEqual(a['sampled_cpu_us_op'], 1000)
        self.assertEqual(a['categories']['core including descendants']['sample_pct'], 20)
        self.assertEqual(a['categories']['TLS including socket descendants']['sample_pct'], 80)
        # One stack can match several inclusive categories. It is counted once per category.
        self.assertEqual(a['categories']['allocation and GC including descendants']['sample_pct'], 20)

    def fixture(self, root, store='a'):
        folder = root/'primary'/'c16'/f'1-{store}'
        folder.mkdir(parents=True)
        j = {'valid': True, 'errors': {}, 'terms': {'2': 60000}, 'workers': 16, 'conns': 16,
             'successes': 60000, 'cpu_us_op': [1, 2, 3, 4], 'occupied_cores': [.1, .2, .3, .4],
             'ops_s': 6000, 'p50_ms': 1, 'p90_ms': 2, 'p99_ms': 3, 'p999_ms': 4, 'mean_ms': 2, 'elapsed_s': 10}
        (folder/'put.json').write_text(json.dumps(j))
        key = 'etcd_disk_wal_fsync_duration_seconds_count' if store == 'etcd' else 'syncs'
        snap = {'before': [{'mallocs': 100, 'total_alloc': 1000, 'disk': {key: 10}}]*3,
                'after': [{'mallocs': 120100, 'total_alloc': 6001000, 'disk': {key: 610}}]*3,
                'cpu_before': [{'pid': 1, 'start': 10, 'ticks': 10}]*3,
                'cpu_after': [{'pid': 1, 'start': 10, 'ticks': 110}]*3, 'hz': 100}
        (folder/'snapshots.json').write_text(json.dumps(snap))
        return folder, snap

    def test_summary(self):
        with tempfile.TemporaryDirectory(dir=HERE) as tmp:
            folder, _ = self.fixture(Path(tmp))
            r = summarize.row_from(folder)
            self.assertEqual(r['leader_allocation_objects_op'], 2)
            self.assertEqual(r['leader_allocation_bytes_op'], 100)
            self.assertEqual(r['leader_puts_per_file_sync'], 100)
            self.assertEqual(r['server_cpu_us_op'], 6)
            self.assertAlmostEqual(r['leader_fenced_cpu_us_op'], 1e6/60000)

    def test_missing_allocations_fail(self):
        for store in ('a', 'etcd'):
            with tempfile.TemporaryDirectory(dir=HERE) as tmp:
                folder, snap = self.fixture(Path(tmp), store)
                snap['after'][0]['mallocs'] = None
                (folder/'snapshots.json').write_text(json.dumps(snap))
                with self.assertRaises(AssertionError):
                    summarize.row_from(folder)

    def test_counter_reset_rejected(self):
        with self.assertRaises(AssertionError):
            summarize.delta({'mallocs': 10}, {'mallocs': 5}, 'mallocs')


if __name__ == '__main__':
    unittest.main()
