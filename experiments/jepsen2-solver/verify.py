#!/usr/bin/env python3
"""Witness validation without Z3 or solver preparation code."""
import argparse
from fractions import Fraction
import json
from pathlib import Path

from history_io import isint, read_events, sha256


def verify(full_path, saved_path, witness_path):
    w = json.loads(Path(witness_path).read_text())
    assert w['schema'] == 1
    assert w['full_sha256'] == sha256(full_path)
    assert w['independent_sha256'] == sha256(saved_path)
    full = read_events(full_path)
    assert len(full) == w['full_event_count']
    key = w['key']
    projection, expected, pending = [], {}, {}
    for pos, event in enumerate(full):
        proc = event['process']
        if not isint(proc):
            assert proc == 'nemesis'
            projection.append(event)
            continue
        assert event['type'] in ('invoke', 'ok', 'fail', 'info')
        assert event['f'] in ('read', 'write', 'cas')
        k, payload = event['value']
        assert isint(k) and isinstance(payload, list) and len(payload) == 2
        unwrapped = dict(event, value=payload)
        if k == key:
            projection.append(unwrapped)
        if event['type'] == 'invoke':
            assert proc not in pending
            pending[proc] = (pos, k, unwrapped)
        else:
            assert proc in pending
            ipos, ikey, inv = pending.pop(proc)
            assert k == ikey and inv['f'] == event['f']
            if event['type'] in ('fail', 'info'):
                assert inv['value'] == payload
            if k == key:
                expected[str(ipos)] = {'id': str(ipos), 'invoke_pos': ipos,
                    'complete_pos': pos, 'invocation': inv, 'response': unwrapped,
                    'status': event['type'], 'f': inv['f'],
                    'model_value': payload if event['type'] == 'ok' else inv['value']}
    for ipos, k, inv in pending.values():
        if k == key:
            expected[str(ipos)] = {'id': str(ipos), 'invoke_pos': ipos,
                'complete_pos': None, 'invocation': inv, 'response': None,
                'status': 'unfinished', 'f': inv['f'], 'model_value': inv['value']}
    assert projection == read_events(saved_path) == w['events']
    decisions = w['operations']
    assert len(decisions) == len(expected)
    by_id = {o['id']: o for o in decisions}
    assert len(by_id) == len(decisions) and set(by_id) == set(expected)
    for oid, base in expected.items():
        o = by_id[oid]
        assert {k: o[k] for k in base} == base, f'Original operation changed: {oid}'
        choice = o['decision']
        if base['status'] == 'ok':
            assert choice == 'included', f'Success omitted: {oid}'
        elif base['status'] == 'fail':
            assert choice == 'upstream-fail', f'Failure included: {oid}'
        else:
            assert choice in ('included', 'omitted')
        if choice == 'included':
            t = Fraction(o['time'])
            assert t > base['invoke_pos'], f'Before invocation: {oid}'
            if base['status'] == 'ok':
                assert t < base['complete_pos'], f'After success response: {oid}'
        else:
            assert not any(k in o for k in ('time', 'state_after', 'model_version'))
    included = {oid for oid, o in by_id.items() if o['decision'] == 'included'}
    order = w['order']
    assert len(set(order)) == len(order) and set(order) == included
    positions = {oid: i for i, oid in enumerate(order)}
    last_time = Fraction(-1)
    version, value, edges = 0, None, 0
    for oid in order:
        o = by_id[oid]
        t = Fraction(o['time'])
        assert t >= last_time
        last_time = t
        op_version, op_value = o['model_value']
        f = o['f']
        if f == 'read':
            assert op_version is None or op_version == version, f'Read version: {oid}'
            assert op_value is None or op_value == value, f'Read value: {oid}'
        else:
            assert op_version is None or op_version == version + 1, f'Mutation version: {oid}'
            if f == 'cas':
                old, new = op_value
                assert old == value, f'CAS compare: {oid}'
            else:
                assert f == 'write'
                new = op_value
            version += 1
            value = new
        assert o['model_version'] == version
        assert o['state_after'] == {'version': version, 'value': value}
    for a in by_id.values():
        if a['decision'] != 'included' or a['status'] != 'ok':
            continue
        for b in by_id.values():
            if b['decision'] == 'included' and a['complete_pos'] < b['invoke_pos']:
                edges += 1
                assert positions[a['id']] < positions[b['id']], 'Real-time edge reversed'
    return {'valid': True, 'key': key, 'operations': len(decisions),
            'included': len(order), 'omitted': len(decisions) - len(order)
                        - sum(o['decision'] == 'upstream-fail' for o in decisions),
            'upstream_failures': sum(o['decision'] == 'upstream-fail' for o in decisions),
            'included_infos': sum(o['decision'] == 'included' and o['status'] in ('info', 'unfinished') for o in decisions),
            'real_time_edges_checked': edges, 'final_version': version,
            'witness_sha256': sha256(witness_path)}


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('full', type=Path)
    p.add_argument('saved', type=Path)
    p.add_argument('witness', type=Path)
    p.add_argument('out', type=Path)
    a = p.parse_args()
    result = verify(a.full, a.saved, a.witness)
    a.out.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
