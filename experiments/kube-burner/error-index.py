#!/usr/bin/env python3
"""Index logged warnings/errors and retain the complete raw logs separately."""
from collections import Counter
import json
from pathlib import Path
import re
import sys

root, out = map(Path, sys.argv[1:])
out.mkdir(parents=True, exist_ok=True)
pattern = re.compile(r'^[WEF]\d{4}\s|level=(?:error|fatal|panic|warning|warn)\b|"level":"(?:error|fatal|panic|warn)"|\berr="[^"\n]+"|panic:|segmentation fault', re.I)
rows = []
with (out / 'log-errors-warnings.jsonl').open('w') as output:
    for result_path in sorted(root.rglob('result.json')):
        result = json.loads(result_path.read_text())
        if not {'store', 'arch', 'rep', 'experiment_ref'} <= result.keys():
            continue
        directory = result_path.parent
        if not (directory / 'workload').exists():
            continue
        counts = Counter()
        for path in sorted(directory.rglob('*')):
            if path.suffix not in {'.stdout', '.stderr', '.log'}:
                continue
            for number, line in enumerate(path.read_text(errors='replace').splitlines(), 1):
                if pattern.search(line):
                    relative = str(path.relative_to(directory))
                    counts[relative] += 1
                    output.write(json.dumps(dict(store=result['store'], arch=result['arch'], rep=result['rep'], file=relative, line=number, text=line)) + '\n')
        metrics = json.loads((directory / 'metrics.json').read_text())
        statuses = {code: values['delta'] for code, values in metrics['apiserver']['status_details']['by_status_code'].items() if code.startswith(('4', '5'))}
        workload = json.loads((directory / 'workload/summary.json').read_text())
        rows.append(dict(store=result['store'], arch=result['arch'], rep=result['rep'],
                         workload_failures=workload.get('workloadFailureCount'),
                         workload_error_records=workload['errorCount'],
                         api_response_errors=statuses, logged_warning_error_lines=sum(counts.values()),
                         files=dict(counts), endpoint_health_exit=int((directory / 'endpoint-health.exit').read_text())))
(out / 'errors.json').write_text(json.dumps(rows, indent=2) + '\n')
print(f'Indexed {len(rows)} runs. Raw logs remain authoritative; this index matches explicit severities and err fields.')
