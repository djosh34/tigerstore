#!/usr/bin/env python3
"""Append one completed run's permanent evidence without rewriting older archives."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import tarfile

p = argparse.ArgumentParser()
p.add_argument('name')
p.add_argument('artifacts', type=Path)
p.add_argument('commit')
p.add_argument('extras', nargs='*', type=Path)
a = p.parse_args()
root = Path(__file__).resolve().parent
out = root / 'evidence'
out.mkdir(exist_ok=True)
source = root / 'executed-sources' / a.commit
assert (source / 'manifest.json').exists(), 'Exact executed source snapshot required'
manifest_path = out / 'manifest.json'
manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {'archives': []}
filename = a.name + '.tar.gz'
assert not any(x['path'] == filename for x in manifest['archives']), 'Existing evidence is immutable'
files = []
for path in sorted(a.artifacts.rglob('*')):
    if path.is_file():
        files.append((path, 'artifacts/' + str(path.relative_to(a.artifacts))))
for path in sorted(source.rglob('*')):
    if path.is_file():
        files.append((path, 'executed-source/' + str(path.relative_to(source))))
for path in a.extras + [root / 'EVIDENCE.md']:
    assert path.is_file()
    relative = path.resolve().relative_to(root)
    files.append((path, str(relative)))
assert len(files) == len({name for _, name in files})
archive = out / filename
assert not archive.exists()
members = []
with archive.open('wb') as stream:
    with gzip.GzipFile(filename='', fileobj=stream, mode='wb', compresslevel=1, mtime=0) as gz:
        with tarfile.open(fileobj=gz, mode='w|') as tar:
            for path, name in files:
                assert not path.is_symlink()
                content = path.read_bytes()
                info = tarfile.TarInfo(name)
                info.size = len(content)
                info.mode = 0o644
                info.mtime = 0
                tar.addfile(info, io.BytesIO(content))
                members.append({'path': name, 'sha256': hashlib.sha256(content).hexdigest(), 'bytes': len(content)})
sidecar = out / (a.name + '.members.json')
sidecar.write_text(json.dumps({'executed_commit': a.commit, 'members': members}, indent=2) + '\n')
with archive.open('rb') as stream:
    archive_sha = hashlib.file_digest(stream, 'sha256').hexdigest()
record = {'path': archive.name, 'sha256': archive_sha, 'bytes': archive.stat().st_size,
          'member_count': len(members), 'members_manifest': sidecar.name,
          'members_manifest_sha256': hashlib.sha256(sidecar.read_bytes()).hexdigest(),
          'executed_commit': a.commit}
manifest['archives'].append(record)
manifest_path.write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps(record, indent=2))
