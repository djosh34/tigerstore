#!/usr/bin/env python3
"""Materialize complete independent projections, never shorten a history."""
import argparse
import json
from pathlib import Path

from edn_format import Keyword, dumps, loads_all
from history_io import isint, normal, prepare, read_events, sha256, validate_value


def materialize(full_path, root):
    raw = list(loads_all(Path(full_path).read_text()))
    keys, pending = set(), {}
    for event in raw:
        proc = event[Keyword('process')]
        if not isint(proc):
            assert proc == Keyword('nemesis')
            continue
        kind = event[Keyword('type')].name
        f = event[Keyword('f')].name
        k, pair = event[Keyword('value')]
        assert isint(k) and kind in ('invoke', 'ok', 'info', 'fail')
        assert f in ('read', 'write', 'cas')
        validate_value(f, normal(pair))
        keys.add(k)
        if kind == 'invoke':
            assert proc not in pending
            pending[proc] = event
        else:
            inv = pending.pop(proc)
            assert inv[Keyword('f')] == event[Keyword('f')]
            assert inv[Keyword('value')][0] == k
            if kind in ('info', 'fail'):
                assert inv[Keyword('value')] == event[Keyword('value')]
    assert keys
    records = []
    for key in sorted(keys):
        projected = []
        for event in raw:
            if isint(event[Keyword('process')]):
                k, pair = event[Keyword('value')]
                if k != key:
                    continue
                event = dict(event)
                event[Keyword('value')] = pair
            projected.append(event)
        path = root / str(key) / 'history.edn'
        existed = path.exists()
        if existed:
            assert read_events(path) == [normal(e) for e in projected], 'Saved projection mismatch'
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('\n'.join(dumps(e) for e in projected) + '\n')
        # A second full parser checks call pairing and complete projection.
        prepare(full_path, path, key)
        records.append({'key': key, 'path': str(path), 'sha256': sha256(path),
                        'existing_saved_projection': existed,
                        'events': len(projected)})
    existing_keys = {int(p.name) for p in root.iterdir() if p.is_dir() and p.name.isdigit()}
    assert existing_keys == keys, 'Extra independent directories not in full history'
    return records


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('full', type=Path)
    p.add_argument('independent', type=Path)
    p.add_argument('manifest', type=Path)
    a = p.parse_args()
    records = materialize(a.full, a.independent)
    a.manifest.write_text(json.dumps({'full_sha256': sha256(a.full), 'keys': records}, indent=2) + '\n')
    print(' '.join(str(r['key']) for r in records))
