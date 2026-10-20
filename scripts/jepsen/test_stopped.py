"""wait_stopped from stopped.sh, with a ps that reports the states given."""
import os
import pathlib
import subprocess
import tempfile
import unittest

HERE = pathlib.Path(__file__).resolve().parent


class WaitStoppedTests(unittest.TestCase):
    def wait(self, states):
        """Runs wait_stopped against a ps that prints states in turn, the
        last one from then on, and returns its exit code and log."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = pathlib.Path(tmp)
            (tmp / 'states').write_text('\n'.join(states) + '\n')
            ps = tmp / 'ps'
            ps.write_text('#!/usr/bin/env bash\n'
                          f'f={tmp}/states\n'
                          'head -n1 "$f"\n'
                          '[[ $(wc -l < "$f") -gt 1 ]] && sed -i 1d "$f"\n'
                          'exit 0\n')
            ps.chmod(0o755)
            log = tmp / 'faults.log'
            env = dict(os.environ, PATH=f'{tmp}:{os.environ["PATH"]}')
            rc = subprocess.run(
                ['bash', '-c', f'. {HERE}/stopped.sh; wait_stopped 7 {log}'],
                env=env).returncode
            return rc, log.read_text()

    def test_stop_after_uninterruptible_sleep(self):
        # #303: the process is in D for a while after SIGSTOP.
        rc, log = self.wait([' Dl', ' Dl', ' Tl'])
        self.assertEqual(rc, 0, log)
        self.assertIn('7 Dl', log)
        self.assertIn('7 Tl', log)

    def test_no_stop_fails(self):
        rc, log = self.wait([' Sl'])
        self.assertEqual(rc, 1, log)
        self.assertIn('did not stop within 5s', log)


if __name__ == '__main__':
    unittest.main()
