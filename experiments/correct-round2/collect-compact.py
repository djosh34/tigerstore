#!/usr/bin/env python3
"""Merge runner-side compact archives without downloading server disks."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import shutil


def main():
    p = argparse.ArgumentParser()
    p.add_argument('source', type=Path)
    p.add_argument('destination', type=Path)
    a = p.parse_args()
    a.destination.mkdir(parents=True, exist_ok=True)
    summary = a.destination / 'summary.json'
    rows = {row['artifact']: row for row in json.loads(summary.read_text())} if summary.exists() else {}
    for artifact in sorted(a.source.glob('compact-round2-faults-*')):
        for row in json.loads((artifact / 'summary.json').read_text()):
            case = artifact / row['artifact']
            for item in json.loads((case / 'manifest.json').read_text()):
                digest, size = hashlib.sha256(), 0
                with gzip.open(case / (item['path']+'.gz'), 'rb') as f:
                    while block := f.read(1024*1024):
                        digest.update(block)
                        size += len(block)
                if size != item['bytes'] or digest.hexdigest() != item['sha256']:
                    raise RuntimeError(f'archive integrity mismatch: {case}/{item["path"]}')
            dst = a.destination / row['artifact']
            if dst.exists():
                raise RuntimeError(f'duplicate attempt archive: {dst}')
            shutil.move(str(case), dst)
            rows[row['artifact']] = row
    summary.write_text(json.dumps([rows[name] for name in sorted(rows)], indent=2)+'\n')
    print(f'{len(rows)} archived attempts, {sum(row["passed"] for row in rows.values())} passed')


if __name__ == '__main__':
    main()
