"""Offline tests only. No processes receive faults and no network rules run."""
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).parent


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / f'{name}.py')
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


class Tools(unittest.TestCase):
    def test_converter_failure_checks_raw_variant(self):
        m = module('run-checks')
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / 'workload').mkdir()
            (out / 'workload/history.json').write_text('{"ops":[]}')
            (out / 'oracles.json').write_text(json.dumps([
                {'name': 'original', 'data_dirs': ['d1', 'd2'], 'stopped_file': 'stopped'}]))
            calls = []
            def fake(command, results, oracle, mode):
                calls.append(command)
                results.append({'oracle': oracle, 'mode': mode, 'returncode': 1})
                if mode == 'check':
                    Path(command[command.index('--out')+1]).write_text(json.dumps([
                        {'name': n, 'passed': False} for n in ['Watches', 'Durable', 'Revisions', 'Linearizable']]))
                return 1
            with patch('sys.argv', ['checks', '--client', 'fake', '--backend', 'etcd', '--clients', '256', '--out', tmp]), patch.object(m, 'run', fake):
                self.assertEqual(m.main(), 1)
            self.assertEqual([c[1] for c in calls], ['log', 'check'])
            self.assertEqual((out / 'history-original.json').read_bytes(), (out / 'workload/history.json').read_bytes())
            self.assertEqual(len(json.loads((out / 'check-original.json').read_text())), 4)

    def test_missing_manifest_still_checks_recorded_history(self):
        m = module('run-checks')
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / 'workload').mkdir()
            (out / 'workload/history.json').write_text('{}')
            calls = []
            def fake(command, results, oracle, mode):
                calls.append(command)
                results.append({'oracle': oracle, 'mode': mode, 'returncode': 1})
                return 1
            with patch('sys.argv', ['checks', '--client', 'fake', '--backend', 'etcd', '--clients', '16', '--out', tmp]), patch.object(m, 'run', fake):
                self.assertEqual(m.main(), 1)
            self.assertEqual([c[1] for c in calls], ['check'])
            self.assertEqual(calls[0][calls[0].index('--history')+1], str(out / 'workload/history.json'))

    def test_archive_includes_attempt_before_controller(self):
        m = module('archive-results')
        with tempfile.TemporaryDirectory() as tmp:
            src, dst = Path(tmp) / 'src', Path(tmp) / 'dst'
            artifact = src / 'correct-round2-faults-build-failure'
            artifact.mkdir(parents=True)
            (artifact / 'attempt.json').write_text('{"job_status":"failure"}')
            (artifact / 'build-client.log').write_text('build failed\n')
            with patch('sys.argv', ['archive', str(src), str(dst)]):
                m.main()
            summary = json.loads((dst / 'summary.json').read_text())
            self.assertFalse(summary[0]['reached_controller'])
            self.assertFalse(summary[0]['passed'])
            self.assertEqual(summary[0]['checker_verdicts'], 0)
            for item in json.loads((dst / artifact.name / 'manifest.json').read_text()):
                data = gzip.decompress((dst / artifact.name / (item['path']+'.gz')).read_bytes())
                self.assertEqual(len(data), item['bytes'])
                self.assertEqual(hashlib.sha256(data).hexdigest(), item['sha256'])


if __name__ == '__main__':
    unittest.main()
