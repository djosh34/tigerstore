#!/usr/bin/env python3
"""Extract only the eight original register histories and their independent checks."""
import argparse
import hashlib
import json
import pathlib
import tarfile

CASES = {
    '38428716698': ['jepsen-tigerstore-register-kill-1',
                    'jepsen-tigerstore-register-partition-1'],
    '38430056885': ['jepsen-tigerstore-register-kill-rep2-1',
                    'jepsen-tigerstore-register-kill-rep3-1',
                    'jepsen-tigerstore-register-partition-rep2-1',
                    'jepsen-tigerstore-register-partition-rep3-1'],
    '38432262482': ['jepsen-tigerstore-register-kill-rep1-1',
                    'jepsen-tigerstore-register-partition-rep1-1'],
}
parser = argparse.ArgumentParser()
parser.add_argument('evidence', type=pathlib.Path)
parser.add_argument('output', type=pathlib.Path)
args = parser.parse_args()
checksums = dict(line.split()[::-1] for line in (args.evidence / 'SHA256SUMS').read_text().splitlines())
args.output.mkdir(parents=True, exist_ok=True)
manifest = []
for run, cases in CASES.items():
    archive = args.evidence / (run + '.tar.gz')
    with archive.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    assert digest == checksums[archive.name], f'Archive checksum mismatch: {archive}'
    with tarfile.open(archive) as tar:
        for case in cases:
            prefix = case + '/store/latest/'
            root = args.output / (run + '-' + case)
            root.mkdir(parents=True, exist_ok=True)
            members = []
            for member in tar.getmembers():
                if not member.name.startswith(prefix):
                    continue
                rel = pathlib.PurePosixPath(member.name[len(prefix):])
                if str(rel) in ('history.edn', 'results.edn') or (
                    len(rel.parts) == 3 and rel.parts[0] == 'independent'
                    and rel.parts[1].isdigit() and rel.parts[2] in ('history.edn', 'results.edn')):
                    assert member.isfile(), 'Expected regular archive member'
                    dest = root.joinpath(*rel.parts)
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    blob = tar.extractfile(member).read()
                    dest.write_bytes(blob)
                    members.append({'path': str(rel), 'sha256': hashlib.sha256(blob).hexdigest()})
            assert (root / 'history.edn').is_file()
            assert ':unknown' in (root / 'results.edn').read_text(), 'Not an original unknown history'
            keys = sorted(int(p.name) for p in (root / 'independent').iterdir())
            assert keys
            manifest.append({'run': run, 'case': case, 'directory': root.name,
                             'archive_sha256': digest, 'keys': keys, 'members': members})
assert len(manifest) == 8
(args.output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps([{'id': i, 'case': m['case'], 'key_count': len(m['keys'])}
                  for i, m in enumerate(manifest)], indent=2))
