#!/usr/bin/env python3
"""Summarize the three JSON outputs; run after the README's commands."""
import json
import re
import sys
from pathlib import Path

here = Path(__file__).parent
missing = sorted(set(re.findall(r"pkg=([^ ]+) name=([^ \n]+)", Path(sys.argv[1]).read_text())))
sets = {name: json.loads((here / (name + '.json')).read_text()) for name in ('rta', 'vta', 'purego')}
indices = {name: {row['Function']: row for row in rows} for name, rows in sets.items()}
lines = ['# Native-source call-graph experiment results', '',
         'Configuration: Go 1.27.2 linux/arm64, CGO_ENABLED=0, x/tools v0.52.0.',
         'See ../README.md for exact commands, pins and interpretation. This is NOT an observed-hook trace.', '',
         '## Counts', '', '| Analysis | Audit reachable | Projected | Projected bodyless |', '|---|---:|---:|---:|']
for name, rows in sets.items():
    audit = [r for r in rows if r['Audit']]
    frontier = [r for r in audit if not r['OnlyViaNativeOrReplaced']]
    lines.append(f"| {name} | {len(audit)} | {len(frontier)} | {sum(not r['Body'] for r in frontier)} |")
lines += ['', '## All audit symbols', '',
          'Status: frontier = candidate before a native/swapped-package or old-hook cut; cut = only reachable beyond such cuts in this projection; absent = not in this rooted source graph. Go-body = same old audit symbol now has a Go implementation under these tags.', '',
          '| Symbol | RTA | VTA | VTA purego |', '|---|---|---|---|']
def status(name, symbol):
    row = indices[name].get(symbol)
    if not row:
        return 'absent'
    return ('cut' if row['OnlyViaNativeOrReplaced'] else 'frontier') + (' / Go-body' if row['Body'] else '')
for pkg, function in missing:
    symbol = pkg + '.' + function
    lines.append('| `' + symbol + '` | ' + ' | '.join(status(n, symbol) for n in sets) + ' |')
lines += ['', '## One source-level candidate path per reachable audit symbol', '',
          'Prefer a VTA projected path; otherwise show its native-source path, or RTA-only path. Package init is an explicit root. Some interface/callback paths are infeasible: see README cautions.', '']
for pkg, function in missing:
    symbol = pkg + '.' + function
    row = indices['vta'].get(symbol) or indices['rta'].get(symbol)
    if row:
        lines += ['### ' + symbol, '', ' -> '.join(row.get('FrontierPath') or row['Path']), '']
lines += ['## Additional bodyless symbols in the requested namespaces', '',
          'This includes existing working hooks as well as newly moved crypto symbols; bodylessness alone does not mean missing implementation.', '']
for row in sets['vta']:
    if not row['Audit'] and not row['Body']:
        lines += ['### ' + row['Function'], '', ('cut; ' if row['OnlyViaNativeOrReplaced'] else 'frontier; ') + ' -> '.join(row.get('FrontierPath') or row['Path']), '']
lines += ['## Other selected namespace members', '',
          'All other selected functions, with a candidate path each. These are additional namespace reachability, not missing hooks.', '']
for row in sets['vta']:
    if not row['Audit'] and row['Body']:
        prefix = row['Function']
        if prefix.startswith(('encoding/json/jsontext.', 'unique.', 'internal/cpu.', 'crypto/rand.', 'runtime/debug.')):
            lines += ['* `' + prefix + '` [' + ('cut' if row['OnlyViaNativeOrReplaced'] else 'frontier') + ']: ' + ' -> '.join(row.get('FrontierPath') or row['Path'])]
(here / 'results.md').write_text('\n'.join(lines) + '\n')
print('audit symbols:', len(missing))
for line in lines[8:13]:
    print(line)
