#!/usr/bin/env python3
"""Reject accidental edits to the copied, immutable exp/bench-75 harness."""
from pathlib import Path
import subprocess

ORIGIN = '0f82f687e5a21fa6265e508866f07dc3fffeaae9'
files = subprocess.check_output(
    ['git', 'ls-tree', '-r', '--name-only', ORIGIN, 'experiments/bench75'], text=True).splitlines()
assert files
for file in files:
    expected = subprocess.check_output(['git', 'show', f'{ORIGIN}:{file}'])
    assert Path(file).read_bytes() == expected, f'copied harness changed: {file}'
print(f'Verified {len(files)} exact bench75 files from {ORIGIN}')
