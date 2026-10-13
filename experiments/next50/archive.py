#!/usr/bin/env python3
"""Archive downloaded runner evidence, retaining matching profile executables."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import tarfile


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def archive(download, out, run):
    out.mkdir(parents=True, exist_ok=True)
    roots = sorted(download.glob(f'pre50next-*-{run}'))
    if not roots:
        raise ValueError('no downloaded artifacts')
    for root in roots:
        label = root.name.removeprefix('pre50next-').removesuffix(f'-{run}')
        profile = label.startswith('profile-')
        keep_bundle = label == 'primary-c256'
        bundle = root / 'binaries.tar.gz'
        if profile:
            manifest = json.loads((root / 'binaries.json').read_text())
            names = {row['variant'].removesuffix('-profile')
                     for row in json.loads((root / 'profile-manifest.json').read_text())}
            with tarfile.open(bundle) as source:
                for name in sorted(names):
                    symbol = root / f'{name}-symbols.gz'
                    member = source.extractfile(f'./{name}')
                    with member, gzip.open(symbol, 'wb') as dest:
                        shutil.copyfileobj(member, dest)
                    with gzip.open(symbol, 'rb') as stream:
                        assert hashlib.file_digest(stream, 'sha256').hexdigest() == manifest['sha256'][name]
        with tarfile.open(out / f'run-{run}-{label}.tar.gz', 'w:gz') as dest:
            for item in sorted(root.iterdir()):
                if item.name == 'binaries.tar.gz' and not keep_bundle:
                    continue
                dest.add(item, arcname=item.name)
        for name in ('summary.csv', 'runs.csv', 'paired-changes.json', 'attribution.json', 'sampler-bounds.json'):
            source = root / name
            if source.exists():
                shutil.copy2(source, out / f'{label}-{name}')
    manifest = {str(path.relative_to(out)): digest(path)
                for path in sorted(out.iterdir()) if path.is_file() and path.name != 'sha256.json'}
    (out / 'sha256.json').write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('download', type=Path)
    parser.add_argument('out', type=Path)
    parser.add_argument('run')
    args = parser.parse_args()
    archive(args.download, args.out, args.run)
