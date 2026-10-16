"""Run in Actions, with the summary parser's pinned edn_format dependency."""
import pathlib
import tempfile
import unittest

from summarize import summarize


class SummaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.latest = self.root / 'store/latest'
        self.latest.mkdir(parents=True)
        (self.latest / 'results.edn').write_text('{:valid? true :stats {}}')
        (self.latest / 'history.edn').write_text('')
        (self.root / 'command.txt').write_text('--nemesis none')

    def test_missing_result_fails(self):
        (self.latest / 'results.edn').unlink()
        self.assertEqual('execution-failure', summarize(self.root)['status'])

    def test_unknown_is_not_a_pass(self):
        (self.latest / 'results.edn').write_text('{:valid? :unknown :stats {}}')
        self.assertEqual('fail', summarize(self.root)['status'])

    def test_false_is_not_a_pass(self):
        (self.latest / 'results.edn').write_text('{:valid? false :stats {}}')
        self.assertEqual('fail', summarize(self.root)['status'])

    def test_missing_pause_fails(self):
        (self.root / 'command.txt').write_text('--nemesis pause')
        result = summarize(self.root)
        self.assertEqual('fail', result['status'])
        self.assertIn('No completed nonempty pause', result['coverage_errors'])

    def test_lazyfs_needs_cache_clear_log(self):
        (self.root / 'command.txt').write_text('--lazyfs --nemesis kill')
        (self.latest / 'history.edn').write_text(
            '{:process :nemesis, :f :kill, :value {"n1" :done}}\n'
            '{:process :nemesis, :f :start, :value {"n1" :done}}\n')
        (self.root / 'fault-observer-n1.log').write_text('10 S\n20 S\n')
        self.assertEqual('fail', summarize(self.root)['status'])
        node = self.latest / 'n1'
        node.mkdir()
        (node / 'lazyfs.log').write_text('[lazyfs.cmds]: cache is cleared.')
        self.assertEqual('pass', summarize(self.root)['status'])

    def test_failed_observer_fails(self):
        (self.root / 'observer-errors.log').write_text('Observation failed for n1')
        self.assertEqual('fail', summarize(self.root)['status'])


if __name__ == '__main__':
    unittest.main()
