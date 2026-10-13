#!/usr/bin/env python3
"""Exact-line assertions against archived final git blobs and module evidence.
Source inspection only; no Go/application executions, benchmarks or tests.
"""
from pathlib import Path
import datetime, hashlib, html, json, subprocess
ROOT = Path('/home/joshazimullah.linux/work_mounts/scratch/bench77-source-audit')
REPO = '/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75'
assert datetime.datetime.now(datetime.timezone.utc) < datetime.datetime(2026, 10, 13, 22, tzinfo=datetime.timezone.utc), 'deadline passed; start nothing'
refs = json.loads((ROOT/'targets.json').read_text())
sites = json.loads((ROOT/'site-specs.json').read_text())
verified_files = {}
for row in sites:
    ref = refs[row['target']]
    path = ROOT/row['file'] if row.get('dependency') else ROOT/ref['source']/row['file']
    content = path.read_bytes()
    key = str(path)
    if key not in verified_files:
        if not row.get('dependency'):
            blob = subprocess.check_output(['git','-C',REPO,'show',ref['sha']+':'+row['file']])
            assert blob == content, 'archived file differs from exact git commit: '+str(path)
        verified_files[key] = hashlib.sha256(content).hexdigest()
    actual = content.decode().splitlines()[row['line']-1]
    assert actual.strip() == row['expected'], f"{row['target']} {row['file']}:{row['line']}: {actual!r} != {row['expected']!r}"
    row['sha'] = ref['sha']
    row['exact_line'] = actual
    row['source_sha256'] = verified_files[key]
(ROOT/'verified-sites.json').write_text(json.dumps(sites,indent=2)+'\n')
md = ['# Verified census sites','', '**Only requester-confirmed rerun measurement targets**, resolved 2026-10-13T08:56:32Z, Actions run 38372738426.','', 'Exact expected source lines asserted by `verify-sites.py`. Every repository evidence file was compared byte-for-byte with `git show <SHA>:<path>` after git-archive extraction. Dependency files are module sources, not files in the git commit; each row is bound to that commit’s dependency pins. `verify-dependencies.py` recomputes the module ZIP h1 hash against every target’s go.sum and compares evidence bytes to ZIP and cache. A call-site and its copying dependency implementation describe ONE stage, never two copies. Alternatives describe one possible receipt, never multiple receipts per event.','']
for label in ('main','batch','wire'):
    md += ['## '+label+' — `'+refs[label]['sha']+'`','', '| File:line | ID / classification | Exact source line (trimmed) |','|---|---|---|']
    for row in [s for s in sites if s['target']==label]:
        escaped = html.escape(row['expected']).replace('|','&#124;')
        md.append(f"| `{row['file']}:{row['line']}` | {row['id']}: {row['classification']} | <code>{escaped}</code> |")
    md += ['']
(ROOT/'verified-sites.md').write_text('\n'.join(md)+'\n')
(ROOT/'source-file-hashes.json').write_text(json.dumps(verified_files,indent=2)+'\n')
print(f'Verified {len(sites)} exact final-target lines across {len(verified_files)} source files.')
