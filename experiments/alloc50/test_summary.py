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
            prefix = store == 'arena'
            headers, data_count = (1024, 1023) if prefix else (4096, 4096)
            sender = {'planned_puts': 4096, 'attempted_puts': headers, 'sent_headers': headers,
                      'completed_data': data_count, 'completed_data_bytes': data_count*1047,
                      'message_bytes': 1047, 'finished': True, 'intentional_close': True,
                      'all_data_sent': not prefix, 'error': 'closed network connection' if prefix else '',
                      'error_before_close': False, 'expected_close_error': prefix}
            result = {'valid': True, 'healthy_successes_while_stalled': 1536,
                      'slow_requests': 4096, 'slow_planned_puts': 4096, 'slow_attempted_puts': headers,
                      'slow_sent_headers': headers, 'slow_sent_data': data_count,
                      'slow_data_bytes': data_count*1047, 'slow_sender': sender,
                      'sender_before_close': {'all_data_sent': not prefix}, 'sender_outcome_verified': True,
                      'slow_committed_puts': 32 if prefix else 256,
                      'reply_reads_after_handshake': 0}
            if prefix:
                dump = dest/'cases.json.blocked-prefix.goroutines.txt'
                dump.write_text('synthetic goroutine fixture; not runner evidence\n')
                result['blocked_prefix_goroutines'] = str(dump)
            if store in ('ring', 'arena'):
                result.update(allocator_saturation_verified=True, allocator_recovery_verified=True,
                              quota_observed_elapsed_s=.1, quota_sustained_s=1.1)
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

    def test_async_prefix_validation(self):
        with tempfile.TemporaryDirectory(dir=HERE) as temp:
            root = Path(temp)
            cell = self.make_fixture(root)
            file = cell/'cases-arena'/'cases.json'
            original = json.loads(file.read_text())
            for mutation in ('early_error', 'too_many_commits', 'short_hold', 'late_quota', 'bad_headers'):
                data = json.loads(json.dumps(original))
                if mutation == 'early_error': data['slow_sender']['error_before_close'] = True
                elif mutation == 'too_many_commits': data['slow_committed_puts'] = data['slow_sent_data']+2
                elif mutation == 'short_hold': data['quota_sustained_s'] = .5
                elif mutation == 'late_quota': data['quota_observed_elapsed_s'] = 5.1
                else: data['slow_sender']['sent_headers'] = data['slow_attempted_puts']+1
                file.write_text(json.dumps(data))
                self.assertNotEqual(self.summarize(root).returncode, 0, mutation)
            file.write_text(json.dumps(original))
            (file.parent/'cases.json.blocked-prefix.goroutines.txt').unlink()
            self.assertNotEqual(self.summarize(root).returncode, 0, 'missing pre-close dump')

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
