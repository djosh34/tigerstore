import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

HERE = Path(__file__).parent
s = importlib.util.spec_from_file_location('busy', HERE / 'fetch-busy-result.py')
m = importlib.util.module_from_spec(s)
s.loader.exec_module(m)


class BusyOnly(unittest.TestCase):
    def row(self):
        return dict(valid=False, invalid_causes=['request_errors'], unsent=0,
                    errors={'grpc ResourceExhausted: etcdserver: too many requests': 10})

    def test_exact_busy_only_and_known_k8s_wrappers(self):
        for prefix in ('', 'txn: ', 'lease-grant: ', 'range-stream: ', 'watch-progress: '):
            d = self.row(); d['errors'] = {prefix + next(iter(d['errors'])): 10}
            self.assertTrue(m.busy_only(d))

    def test_all_other_causes_counts_and_statuses_fail_closed(self):
        for key, value in [('valid', True), ('invalid_causes', None),
                           ('invalid_causes', ['request_errors', 'server_cpu']),
                           ('unsent', 1), ('errors', {}),
                           ('errors', {'grpc ResourceExhausted: message too large': 1}),
                           ('errors', {'grpc ResourceExhausted: etcdserver: too many requests': True}),
                           ('errors', {'grpc ResourceExhausted: etcdserver: too many requests': -1})]:
            d = self.row(); d[key] = value
            self.assertFalse(m.busy_only(d), (key, value))

    def test_shell_exception_is_opt_in_status_one_and_still_checks_leader(self):
        text = (HERE.parent / 'bench/run.sh').read_text()
        step = text[text.index('step() {'):text.index('\n# once <target>')]
        for enabled, status, busy in [(True, 1, True), (False, 1, True),
                                       (True, 137, True), (True, 1, False),
                                       (True, 2, True)]:
            with tempfile.TemporaryDirectory() as td:
                run = Path(td) / 'run'
                row = self.row()
                if not busy: row['invalid_causes'].insert(0, 'phase')
                run.with_suffix('.json').write_text(json.dumps(row))
                script = step + '\nif step "$1" loadgen bash -c \'echo native-error >&2; exit "$1"\' x "$2"; then touch "$1.leader-check"; fi\n'
                subprocess.run(['bash', '-c', script, 'test', str(run), str(status)],
                               env=dict(os.environ, here=str(HERE.parent / 'bench'),
                                        FETCH_MEASURE_BUSY_REPORT='1' if enabled else ''),
                               check=True, capture_output=True)
                accepted = enabled and status == 1 and busy
                self.assertEqual(run.with_suffix('.busy').exists(), accepted)
                self.assertEqual(run.with_suffix('.failed').exists(), not accepted)
                self.assertEqual(run.with_suffix('.leader-check').exists(), accepted)
                marker = run.with_suffix('.busy' if accepted else '.failed').read_text()
                self.assertIn(f'exited {status}', marker)  # native failure preserved
                self.assertFalse(json.loads(run.with_suffix('.json').read_text())['valid'])


if __name__ == '__main__': unittest.main()
