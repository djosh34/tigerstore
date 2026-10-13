import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile

BASE = Path(__file__).resolve().parent

class ArchiveTests(unittest.TestCase):
    def test_setup_failure_and_symlink_root(self):
        with tempfile.TemporaryDirectory(dir=BASE) as temp:
            root = Path(temp)
            evidence = root / 'evidence'
            evidence.mkdir()
            (evidence / 'attempt.json').write_text('{"controller_outcome":"skipped"}\n')
            (evidence / 'build.log').write_text('intentional setup failure\n')
            source = root / 'source'
            source.mkdir()
            (source / 'clocks-test').symlink_to(evidence, target_is_directory=True)
            out = root / 'out'
            subprocess.run(['python3', str(BASE / 'archive-results.py'), str(source), str(out)], check=True, capture_output=True)
            summary = json.loads((out / 'summary.json').read_text())[0]
            self.assertFalse(summary['reached_controller'])
            self.assertFalse(summary['controller_pass'])
            self.assertEqual(summary['checker_verdicts'], 0)
            self.assertEqual(len(json.loads((out / 'clocks-test/manifest.json').read_text())), 2)

    def test_zip_stream_keeps_full_values_but_not_disks(self):
        with tempfile.TemporaryDirectory(dir=BASE) as temp:
            root = Path(temp)
            value = b'{"full_value":"' + b'x' * 1572864 + b'"}\n'
            zipped = root / 'a.zip'
            with zipfile.ZipFile(zipped, 'w', compression=zipfile.ZIP_DEFLATED) as z:
                z.writestr('attempt.json', '{}')
                z.writestr('run/workload/history.json', value)
                z.writestr('run/data/n1/wal/segment.wal', b'ignored disk')
            out = root / 'out'
            subprocess.run(['python3', str(BASE / 'archive-zip.py'), str(zipped), 'clocks-test', str(out)], check=True)
            manifest = json.loads((out / 'clocks-test/manifest.json').read_text())
            self.assertEqual(len(manifest), 2)
            row = next(x for x in manifest if x['path'] == 'run/workload/history.json')
            self.assertEqual(row['bytes'], len(value))
            self.assertEqual(row['sha256'], hashlib.sha256(value).hexdigest())
            with gzip.open(out / 'clocks-test/run/workload/history.json.gz', 'rb') as f:
                self.assertEqual(f.read(), value)

if __name__ == '__main__':
    unittest.main()
