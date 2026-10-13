#!/usr/bin/env python3
"""Keep one copy of each case, without Docker's duplicated latest symlinks."""
import hashlib
import pathlib
import sys
import tarfile

source = pathlib.Path(sys.argv[1])
destination = pathlib.Path(sys.argv[2])
destination.parent.mkdir(parents=True, exist_ok=True)
with tarfile.open(destination, 'w:gz', compresslevel=6) as archive:
    for artifact in sorted(source.iterdir()):
        if not artifact.is_dir():
            continue
        for item in sorted(artifact.iterdir()):
            if item.is_file():
                archive.add(item, arcname=str(item.relative_to(source)), recursive=False)
        latest = artifact / 'store/latest'
        if latest.exists():
            for item in sorted(latest.rglob('*')):
                if item.is_file() and (item.suffix in {'.edn', '.log', '.jepsen', '.bz2', '.txt'}):
                    archive.add(item, arcname=str(item.relative_to(source)), recursive=False)
print(hashlib.file_digest(destination.open('rb'), 'sha256').hexdigest(), destination.name)
