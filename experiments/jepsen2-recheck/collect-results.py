#!/usr/bin/env python3
"""Make a cause inventory from downloaded offline recheck artifacts."""
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
results = []
for path in sorted(root.glob('*/case/summary.json')):
    summary = json.loads(path.read_text())
    unresolved = []
    for key in summary['keys']:
        if key['valid'] is True:
            continue
        attempts = []
        for attempt in key['attempts']:
            result = path.parent / attempt['result'] if attempt['result'] else None
            causes = re.findall(r':cause\s+([^,\s}]+)', result.read_text()) if result and result.exists() else []
            cause = 'external-timeout' if attempt['exit'] in ('124', '137') else (
                causes[0].lstrip(':') if causes else 'unknown-or-process-error')
            attempts.append({**attempt, 'cause': cause})
        unresolved.append({'key': key['key'], 'valid': key['valid'], 'attempts': attempts})
    results.append({'original_run': summary['source_run'], 'case': summary['case'],
                    'valid': summary['valid'], 'expected_keys': summary['expected_key_count'],
                    'true_keys': sum(k['valid'] is True for k in summary['keys']),
                    'unresolved': unresolved,
                    'summary': str(path)})
print(json.dumps({'completed_history_jobs': len(results), 'histories': results}, indent=2))
