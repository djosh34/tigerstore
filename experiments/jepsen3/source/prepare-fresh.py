#!/usr/bin/env python3
"""Extract only authenticated fresh histories and saved checker outputs."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import tarfile

from history_io import isint, read_events
from projections import materialize

p = argparse.ArgumentParser()
p.add_argument('archive', type=Path)
p.add_argument('sha256')
p.add_argument('output', type=Path)
p.add_argument('--case', type=int, help='Generate/check projections only for this selected case')
a = p.parse_args()
assert re.fullmatch('[0-9a-f]{64}', a.sha256), 'Explicit fresh archive SHA-256 required'
with a.archive.open('rb') as stream:
    digest = hashlib.file_digest(stream, 'sha256').hexdigest()
assert digest == a.sha256, 'Fresh archive SHA-256 mismatch'
a.output.mkdir(parents=True, exist_ok=True)
manifest = []
with tarfile.open(a.archive) as tar:
    members = tar.getmembers()
    prefixes = sorted({m.name[:-len('history.edn')] for m in members
                       if m.name.endswith('/store/latest/history.edn')})
    # Also accept compact archives with case/history.edn and independent files.
    if not prefixes:
        prefixes = sorted({m.name[:-len('history.edn')] for m in members
            if m.name.endswith('/history.edn') and '/independent/' not in m.name
            and '/store/current/' not in m.name})
    assert prefixes, 'No full history in fresh archive'
    assert a.case is None or 0 <= a.case < len(prefixes), 'Selected case outside archive'
    for case_id, prefix in enumerate(prefixes):
        parts = PurePosixPath(prefix).parts
        assert '..' not in parts and not PurePosixPath(prefix).is_absolute()
        case = prefix.removesuffix('/store/latest/').rstrip('/')
        root = a.output / f'fresh-{case_id:03}-{PurePosixPath(case).name}'
        root.mkdir()
        extracted = set()
        for member in members:
            if not member.name.startswith(prefix):
                continue
            rel = PurePosixPath(member.name[len(prefix):])
            selected = str(rel) in ('history.edn', 'results.edn') or (
                len(rel.parts) == 3 and rel.parts[0] == 'independent'
                and rel.parts[1].isdigit() and rel.parts[2] in ('history.edn', 'results.edn'))
            if not selected:
                continue
            assert member.isfile() and str(rel) not in extracted
            extracted.add(str(rel))
            dest = root.joinpath(*rel.parts)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(tar.extractfile(member).read())
        assert (root / 'history.edn').exists()
        # Avoid generating every key of every case in each matrix job.
        # Every selected case still receives complete projection/pair checks.
        events = read_events(root / 'history.edn')
        keys = sorted({e['value'][0] for e in events if isint(e['process'])})
        assert keys and all(isint(k) for k in keys)
        projections = materialize(root / 'history.edn', root / 'independent') if (
            a.case is None or case_id == a.case) else []
        if projections:
            assert [r['key'] for r in projections] == keys
        records = []
        for path in sorted(root.rglob('*.edn')):
            records.append({'path': str(path.relative_to(root)),
                            'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                            'retained_from_archive': str(path.relative_to(root)) in extracted})
        manifest.append({'run': 'fresh', 'case': case, 'directory': root.name,
                         'archive_path': str(a.archive), 'archive_sha256': digest,
                         'keys': keys, 'members': records,
                         'projections': projections})
(a.output / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
print(json.dumps([{'id': i, 'case': m['case'], 'key_count': len(m['keys'])}
                  for i, m in enumerate(manifest)], indent=2))
