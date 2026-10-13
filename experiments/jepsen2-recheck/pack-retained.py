#!/usr/bin/env python3
"""Pack downloaded checker artifacts and their metadata without rerunning tests."""
import argparse
import hashlib
import pathlib
import tarfile

parser = argparse.ArgumentParser()
parser.add_argument('main_artifacts', type=pathlib.Path)
parser.add_argument('long_artifacts', type=pathlib.Path)
parser.add_argument('metadata', type=pathlib.Path)
parser.add_argument('output', type=pathlib.Path)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=True)
main_meta = ['retained-run.json', 'retained-unresolved.json',
             'original-unknown-keys.json', 'cancellation-request.log',
             'published.json', 'saved/manifest.json']
long_meta = ['snapshot5-long-progress.json', 'published-long.json']
archives = []
for name, artifacts, metadata in [
    ('recheck-38441161188.tar.gz', args.main_artifacts, main_meta),
    ('long-wgl-38443204924.tar.gz', args.long_artifacts, long_meta),
]:
    path = args.output / name
    with tarfile.open(path, 'w:gz') as tar:
        for p in sorted(artifacts.rglob('*')):
            if p.is_file():
                tar.add(p, arcname='artifacts/' + str(p.relative_to(artifacts)), recursive=False)
        for name in metadata:
            p = args.metadata / name
            assert p.is_file(), p
            tar.add(p, arcname='metadata/' + name, recursive=False)
    archives.append(path)
lines = []
for p in archives:
    with p.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    lines.append(digest + '  ' + p.name)
(args.output / 'SHA256SUMS').write_text('\n'.join(lines) + '\n')
print('\n'.join(lines))
