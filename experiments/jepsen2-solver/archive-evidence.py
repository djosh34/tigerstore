#!/usr/bin/env python3
"""Package retained runner artifacts without executing any history checker."""
import gzip
import hashlib
import io
import json
from pathlib import Path
import tarfile

ROOT = Path(__file__).resolve().parent
SPECS = [
    ('old-parser-attempt-38444408855', 'evidence-first',
     '29e1e7fcdc1d7c87acb3f805ef1d120a3c87aa8f',
     ['log-38444408855.txt', 'artifacts-first.json']),
    ('old-success-38444996427', 'evidence-current',
     '89a97a0487a5a904f6b27dc9d9e1b990590cabff',
     ['log-38444996427.txt', 'artifacts-current.json', 'final-old-run.json',
      'old-verdicts.json', 'included-info-audit.json']),
    ('fresh-first-success-38445539009', 'evidence-fresh-first',
     '82989021aabe01bd06a9af26b291e04fe772ed8e',
     ['log-38445539009.txt', 'artifacts-fresh-first.json', 'first-fresh-run.json',
      'fresh-verdicts.json', 'inputs/first-fresh-manifest.json',
      'inputs/first-fresh-38440292743-kill-ops200-rep2.tar.gz']),
]

output = ROOT / 'evidence'
output.mkdir(exist_ok=True)
archives = []
for name, artifact_directory, commit, extras in SPECS:
    files = []
    for path in sorted((ROOT / artifact_directory).rglob('*')):
        if path.is_file():
            files.append((path, 'artifacts/' + str(path.relative_to(ROOT / artifact_directory))))
    for path in sorted((ROOT / 'executed-sources' / commit).rglob('*')):
        if path.is_file():
            files.append((path, 'executed-source/' + str(path.relative_to(ROOT / 'executed-sources' / commit))))
    for rel in extras + ['EVIDENCE.md']:
        path = ROOT / rel
        assert path.is_file()
        files.append((path, rel))
    assert len({arcname for _, arcname in files}) == len(files)
    archive = output / (name + '.tar.gz')
    members = []
    with archive.open('wb') as stream:
        with gzip.GzipFile(filename='', fileobj=stream, mode='wb', compresslevel=1, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode='w|') as tar:
                for path, arcname in files:
                    assert not path.is_symlink()
                    content = path.read_bytes()
                    info = tarfile.TarInfo(arcname)
                    info.size = len(content)
                    info.mode = 0o644
                    info.mtime = 0
                    tar.addfile(info, io.BytesIO(content))
                    members.append({'path': arcname, 'sha256': hashlib.sha256(content).hexdigest(), 'bytes': len(content)})
    manifest = output / (name + '.members.json')
    manifest.write_text(json.dumps({'executed_commit': commit, 'members': members}, indent=2) + '\n')
    record = {'path': archive.name, 'sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
              'bytes': archive.stat().st_size, 'member_count': len(members),
              'members_manifest': manifest.name,
              'members_manifest_sha256': hashlib.sha256(manifest.read_bytes()).hexdigest(),
              'executed_commit': commit}
    archives.append(record)
    print(json.dumps(record), flush=True)
(output / 'manifest.json').write_text(json.dumps({'archives': archives}, indent=2) + '\n')
