#!/usr/bin/env python3
"""Read-only module zip/evidence validation; no Go invocation or network."""
from pathlib import Path
import base64, datetime, hashlib, json, zipfile
ROOT = Path('/home/joshazimullah.linux/work_mounts/scratch/bench77-source-audit')
assert datetime.datetime.now(datetime.timezone.utc) < datetime.datetime(2026, 10, 13, 22, tzinfo=datetime.timezone.utc), 'deadline passed; start nothing'
manifest = json.loads((ROOT / 'dependency-source-manifest.json').read_text())
refs_path = ROOT / 'targets.json'
refs = json.loads(refs_path.read_text()) if refs_path.exists() else {k: {'source': 'observed/' + k, 'sha': 'observed-pending-confirmation'} for k in ('main', 'batch', 'wire')}
result = []
for module in sorted({r['module'] for r in manifest}):
    name, version = module.split('@')
    cache = Path('/home/joshazimullah.linux/go/pkg/mod/cache/download') / name / '@v'
    zp = cache / (version + '.zip')
    with zipfile.ZipFile(zp) as z:
        names = sorted(z.namelist())
        # Go x/mod/sumdb/dirhash.Hash1: SHA256 of sorted per-file SHA256/name lines.
        lines = ''.join(hashlib.sha256(z.read(n)).hexdigest() + '  ' + n + '\n' for n in names)
        h1 = 'h1:' + base64.b64encode(hashlib.sha256(lines.encode()).digest()).decode()
        assert h1 == (cache / (version + '.ziphash')).read_text().strip()
        pinned = {}
        for label, ref in refs.items():
            sums = (ROOT / ref['source'] / 'go.sum').read_text().splitlines()
            expected = [s.split()[2] for s in sums if s.startswith(name + ' ' + version + ' ')]
            assert expected == [h1], (label, module, expected, h1)
            pinned[label] = ref['sha']
        evidence = []
        for row in [r for r in manifest if r['module'] == module]:
            relative = str(Path(row['cache_source']).relative_to(Path('/home/joshazimullah.linux/go/pkg/mod') / module))
            payload = z.read(module + '/' + relative)
            assert payload == (ROOT / row['evidence']).read_bytes() == Path(row['cache_source']).read_bytes()
            assert hashlib.sha256(payload).hexdigest() == row['sha256']
            evidence.append(row['evidence'])
    result.append({'module': module, 'module_zip': str(zp), 'computed_h1': h1, 'go_sum_pins': pinned, 'verified_evidence': evidence})
(ROOT / 'dependency-integrity.json').write_text(json.dumps(result, indent=2) + '\n')
print('Verified', len(result), 'module zip h1 hashes against every target go.sum;', len(manifest), 'evidence files match zip and cache exactly.')
