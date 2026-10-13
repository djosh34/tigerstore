#!/usr/bin/env python3
"""Make input for the parent's independent offline attribution script."""
import json
from pathlib import Path
import sys


def manifest(root):
    records = []
    for path in sorted(root.glob('*/profile-window.json')):
        cohort = path.parent
        rep, variant = cohort.name.split('-', 1)
        placement = json.loads((cohort / 'placement.json').read_text())
        result = json.loads((cohort / 'put.json').read_text())
        window = json.loads(path.read_text())
        assert result['valid'] and result['successes'] == window['operations']
        for role, node in placement['roles'].items():
            raw = cohort / f'n{node}-cpu-raw.txt'
            assert raw.exists(), raw
            records.append(dict(clients=result['workers'], variant=variant, rep=int(rep), role=role,
                                operations=result['successes'], raw=str(raw.relative_to(root))))
    (root / 'profile-manifest.json').write_text(json.dumps(records, indent=2)+'\n')
    return records


if __name__ == '__main__':
    manifest(Path(sys.argv[1]))
