#!/usr/bin/env python3
"""Check retained evidence archive and every member hash without extraction."""
import hashlib
import json
from pathlib import Path
import sys
import tarfile

root = Path(sys.argv[1])
manifest = json.loads((root / 'manifest.json').read_text())
for archive in manifest['archives']:
    path = root / archive['path']
    with path.open('rb') as stream:
        assert hashlib.file_digest(stream, 'sha256').hexdigest() == archive['sha256']
    sidecar = root / archive['members_manifest']
    assert hashlib.sha256(sidecar.read_bytes()).hexdigest() == archive['members_manifest_sha256']
    expected = {m['path']: m for m in json.loads(sidecar.read_text())['members']}
    seen = set()
    with tarfile.open(path, mode='r|gz') as tar:
        for member in tar:
            assert member.isfile() and member.name in expected and member.name not in seen
            seen.add(member.name)
            record = expected[member.name]
            assert member.size == record['bytes']
            assert hashlib.file_digest(tar.extractfile(member), 'sha256').hexdigest() == record['sha256']
    assert seen == set(expected) and len(seen) == archive['member_count']
    print(archive['path'], len(seen), 'member hashes verified')
