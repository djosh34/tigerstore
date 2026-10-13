#!/usr/bin/env python3
"""Retain exact executed tool source blobs, checking their Git object hashes."""
import base64
import datetime as dt
import hashlib
import json
from pathlib import Path
import subprocess
import sys

DEADLINE = dt.datetime.fromisoformat('2026-10-13T22:00:00+00:00')
ROOT = Path(__file__).resolve().parent
REPO = 'djosh34/tigerstore'
COMMITS = ['29e1e7fcdc1d7c87acb3f805ef1d120a3c87aa8f',
           '89a97a0487a5a904f6b27dc9d9e1b990590cabff',
           '82989021aabe01bd06a9af26b291e04fe772ed8e']


def api(endpoint):
    assert dt.datetime.now(dt.timezone.utc) < DEADLINE
    return json.loads(subprocess.check_output(['gh', 'api', 'repos/' + REPO + '/' + endpoint]))


for commit in (sys.argv[1:] or COMMITS):
    dest = ROOT / 'executed-sources' / commit
    dest.mkdir(parents=True, exist_ok=True)
    metadata = api('git/commits/' + commit)
    (dest / 'commit.json').write_text(json.dumps(metadata, indent=2) + '\n')
    tree = api('git/trees/' + metadata['tree']['sha'] + '?recursive=1')
    assert not tree['truncated']
    (dest / 'tree.json').write_text(json.dumps(tree, indent=2) + '\n')
    records = []
    for entry in tree['tree']:
        path = entry['path']
        selected = path == '.github/workflows/jepsen2-solver.yml' or (
            path.startswith('experiments/jepsen2-solver/') and
            Path(path).suffix in ('.py', '.clj', '.md'))
        if not selected:
            continue
        assert entry['type'] == 'blob'
        blob = api('git/blobs/' + entry['sha'])
        assert blob['encoding'] == 'base64'
        content = base64.b64decode(blob['content'])
        git_sha = hashlib.sha1(b'blob ' + str(len(content)).encode() + b'\0' + content).hexdigest()
        assert git_sha == entry['sha']
        out = dest / path
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(content)
        records.append({'path': path, 'git_blob_sha': git_sha,
                        'sha256': hashlib.sha256(content).hexdigest(), 'bytes': len(content)})
    (dest / 'manifest.json').write_text(json.dumps({'commit': commit, 'files': records}, indent=2) + '\n')
    print(commit, len(records), 'exact source files retained', flush=True)
