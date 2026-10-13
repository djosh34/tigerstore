#!/usr/bin/env python3
"""Small synthetic validity probes; no Go, cluster or benchmark execution."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

HERE = Path(__file__).resolve().parent


class SummaryTests(unittest.TestCase):
    def make_fixture(self, root):
        cell = root/'c64'
        for store in ('base', 'ring', 'arena'):
            for rep in range(1, 6):
                dest = cell/f'{rep}-{store}'
                dest.mkdir(parents=True)
                result = {
                    'valid': True, 'errors': {}, 'successes': 60000, 'workers': 64,
                    'cpu_us_op': [1, 2, 3, 4], 'occupied_cores': [.8, .1, .1, .5],
                    'leader_heap': {'objects_per_put': 10, 'bytes_per_put': 1000,
                                    'heap_bytes_after': 100000, 'gc_cpu_share': .01,
                                    'gc_cpu_us_per_put': .1},
                }
                for key in ('ops_s', 'mean_ms', 'p50_ms', 'p99_ms', 'p999_ms', 'elapsed_s'):
                    result[key] = 1.0
                (dest/'put.json').write_text(json.dumps(result))
                (dest/'retained-after.json').write_text(json.dumps({
                    'forced_gc': True, 'mem_profile_rate': 524288,
                    'heap_alloc_bytes': 80000, 'heap_objects': 500}))
                (dest/'validation.json').write_text('{"valid": true}')
            dest = cell/f'profile-{store}'
            dest.mkdir()
            (dest/'profile-cohort.json').write_text('{"successes": 2000, "mem_profile_rate": 1}')
            (dest/'validation.json').write_text('{"valid": true}')
        for store in ('base', 'ring', 'arena', 'default'):
            dest = cell/f'cases-{store}'
            dest.mkdir()
            result = {'valid': True, 'healthy_successes_while_stalled': 1536,
                      'slow_requests': 4096, 'reply_reads_after_handshake': 0}
            if store in ('ring', 'arena'):
                result.update(allocator_saturation_verified=True, allocator_recovery_verified=True)
                stats = {'mode': {'ring': 1, 'arena': 2}[store], 'slot_budget': 4096,
                         'byte_budget': 64 << 20, 'held_slots': 0, 'held_bytes': 0,
                         'peak_slots': 256, 'peak_bytes': 1024, 'fallback_allocations': 0,
                         'per_connection_slots': 256, 'denied_count': 0}
                for phase in ('before_slow', 'slow_phase_0', 'slow_phase_1', 'slow_phase_2', 'after_slow_close'):
                    current = stats.copy()
                    if phase.startswith('slow_phase'):
                        current.update(held_slots=256, held_bytes=1024, denied_count=3840)
                    result[phase] = {'allocator': current}
            (dest/'cases.json').write_text(json.dumps(result))
            (dest/'validation.json').write_text('{"valid": true}')
        return cell

    def summarize(self, root):
        return subprocess.run(['python3', str(HERE/'summarize.py'), str(root)],
                              capture_output=True, text=True)

    def test_valid_and_corrupt_cases(self):
        with tempfile.TemporaryDirectory(dir=HERE) as temp:
            root = Path(temp)
            cell = self.make_fixture(root)
            self.assertEqual(self.summarize(root).returncode, 0)
            file = cell/'cases-ring'/'cases.json'
            data = json.loads(file.read_text())
            data['allocator_recovery_verified'] = False
            file.write_text(json.dumps(data))
            self.assertNotEqual(self.summarize(root).returncode, 0)

    def test_failed_window_and_missing_repetition(self):
        with tempfile.TemporaryDirectory(dir=HERE) as temp:
            root = Path(temp)
            cell = self.make_fixture(root)
            file = cell/'3-arena'/'put.json'
            data = json.loads(file.read_text())
            data['valid'] = False
            file.write_text(json.dumps(data))
            self.assertNotEqual(self.summarize(root).returncode, 0)
            file.unlink()
            self.assertNotEqual(self.summarize(root).returncode, 0)


if __name__ == '__main__':
    unittest.main()
