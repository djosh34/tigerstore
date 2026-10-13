#!/usr/bin/env python3
"""No missing, timed-out, OOM, or unknown key can produce a pass."""
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
manifest = json.loads((root / 'input.json').read_text())
results = []
for key in manifest['keys']:
    attempts = []
    verdict = 'unknown'
    for algorithm in ('wgl', 'linear', 'competition'):
        directory = root / str(key)
        status = directory / (algorithm + '.status')
        if not status.exists():
            continue
        rc = status.read_text().strip()
        result = directory / (algorithm + '.edn')
        sidecar = directory / (algorithm + '.edn.verdict')
        v = sidecar.read_text().strip() if sidecar.exists() else ':unknown'
        attempts.append({'algorithm': algorithm, 'exit': rc, 'verdict': v,
                         'result': str(result.relative_to(root)) if result.exists() else None})
        if rc == '0' and v == 'true' and result.is_file():
            verdict = True
            break
        if rc == '1' and v == 'false' and result.is_file():
            verdict = False
            break
    results.append({'key': key, 'valid': verdict, 'attempts': attempts})
valid = False if any(r['valid'] is False for r in results) else (
    True if results and all(r['valid'] is True for r in results) else 'unknown')
summary = {'case': manifest['case'], 'source_run': manifest['run'],
           'archive_sha256': manifest['archive_sha256'], 'valid': valid,
           'expected_key_count': len(manifest['keys']), 'keys': results,
           'scope': 'register workload linearizability only; original non-workload checks unchanged'}
(root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
sys.exit(0 if valid is True else 1)
