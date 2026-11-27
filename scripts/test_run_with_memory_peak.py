import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('peak', Path(__file__).with_name('run-with-memory-peak.py'))
peak = importlib.util.module_from_spec(spec)
spec.loader.exec_module(peak)


class WholeRunMemory(unittest.TestCase):
    def run_case(self, command, memory=1 << 20, threshold=2):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            group = root / 'group'
            group.mkdir()
            for name, value in [('memory.peak', memory), ('memory.current', memory // 2), ('memory.max', 12 << 30)]:
                (group / name).write_text(str(value))
            out = root / 'report'
            status = peak.measure(command, out, group, threshold, interval=.01)
            report = json.loads((out / 'summary.json').read_text())
            samples = (out / 'samples.jsonl').read_text().splitlines()
            return status, report, samples

    def test_success_records_kernel_peak_not_current(self):
        status, report, samples = self.run_case(['/bin/true'])
        self.assertEqual(status, 0)
        self.assertEqual(report['whole_run_peak_bytes'], 1 << 20)
        self.assertEqual(report['source'], 'cgroup-v2 memory.peak')
        self.assertEqual(json.loads(samples[0])['current_bytes'], 1 << 19)
        self.assertFalse(report['above_threshold'])

    def test_failure_is_preserved_without_classification(self):
        status, report, _ = self.run_case(['/bin/sh', '-c', 'exit 7'])
        self.assertEqual(status, 7)
        self.assertEqual(report['command_exit_code'], 7)
        self.assertFalse(report['above_threshold'])

    def test_threshold_is_strictly_above(self):
        status, report, _ = self.run_case(['/bin/true'], memory=2 << 20)
        self.assertEqual(status, 0)
        self.assertFalse(report['above_threshold'])

    def test_breach_stops_only_owned_command_group(self):
        status, report, _ = self.run_case(['/bin/sh', '-c', 'sleep 1 & wait'], memory=3 << 20)
        self.assertEqual(status, 3)
        self.assertTrue(report['above_threshold'])
        self.assertEqual(report['whole_run_peak_mib'], 3)

    def test_missing_counter_fails_before_start(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            with self.assertRaises(FileNotFoundError):
                peak.measure(['/bin/true'], root / 'out', root, 2)

    def test_invalid_counter_is_not_zero(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'memory.peak'
            path.write_text('max')
            with self.assertRaises(ValueError):
                peak.counter(path)


if __name__ == '__main__':
    unittest.main()
