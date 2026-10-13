import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import patch

os.environ.setdefault('RUNNER_TEMP', '/tmp/shape-offline-test')
spec = importlib.util.spec_from_file_location('shape_runner', Path(__file__).with_name('run.py'))
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


def status(revision):
    return [{'Status': {'raftAppliedIndex': revision + 2, 'raftIndex': revision + 2,
                        'header': {'revision': revision}}} for _ in range(3)]


class RunnerTests(unittest.TestCase):
    def test_native_control_uses_two_status_checks_not_file_proof(self):
        with patch.object(r, 'OBSERVATION_CONTROL', True), \
             patch.object(r, 'status', side_effect=[status(10), status(10)]), \
             patch.object(r, 'http', side_effect=AssertionError('no counter proof')), \
             patch.object(r.time, 'sleep') as sleep:
            st, proof = r.fence(Path('/unused'), 'pristine', [])
        self.assertEqual(st, status(10))
        self.assertTrue(proof['native_applied_only'])
        self.assertNotIn('samples', proof)
        sleep.assert_called_once_with(.3)

    def test_native_control_rechecks_changed_revision(self):
        with patch.object(r, 'OBSERVATION_CONTROL', True), \
             patch.object(r, 'status', side_effect=[status(10), status(11), status(11), status(11)]), \
             patch.object(r.time, 'sleep'):
            st, proof = r.fence(Path('/unused'), 'tigerstore', [])
        self.assertEqual(st, status(11))
        self.assertEqual(proof['first_status'], status(11))

    def test_unobserved_targets_do_not_scrape_debug_server(self):
        with patch.object(r, 'http', side_effect=AssertionError('disabled endpoint')):
            self.assertEqual(r.node_snapshots('pristine', [1, 2, 3]), [])
            self.assertEqual(r.node_snapshots('tigerstore-off', [1, 2, 3]), [])


if __name__ == '__main__':
    unittest.main()
