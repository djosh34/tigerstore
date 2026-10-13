"""Strict saved-history reader. No solver dependency."""
import hashlib
import json
from pathlib import Path

from edn_format import Keyword, loads_all
from edn_format.immutable_dict import ImmutableDict
from edn_format.immutable_list import ImmutableList


def normal(x):
    if isinstance(x, Keyword):
        return x.name
    if isinstance(x, (dict, ImmutableDict)):
        return {normal(k): normal(v) for k, v in x.items()}
    if isinstance(x, (list, tuple, ImmutableList)):
        return [normal(v) for v in x]
    if x is None or isinstance(x, (str, int, float, bool)):
        return x
    raise ValueError(f'Unsupported EDN object: {x!r}')


def read_events(path):
    ops = [normal(x) for x in loads_all(Path(path).read_text())]
    assert ops and all(isinstance(x, dict) for x in ops)
    return ops


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def isint(x):
    return isinstance(x, int) and not isinstance(x, bool)


def validate_value(f, pair):
    assert isinstance(pair, list) and len(pair) == 2
    version, value = pair
    assert version is None or (isint(version) and version >= 0)
    if f == 'cas':
        assert isinstance(value, list) and len(value) == 2
        assert all(v is None or isint(v) for v in value)
    else:
        assert value is None or isint(value)


def prepare(full_path, independent_path, key):
    full = read_events(full_path)
    pending = {}
    calls = []
    projected = []
    keys = set()
    for pos, raw in enumerate(full):
        p = raw['process']
        if not isint(p):
            assert p == 'nemesis', f'Unexpected process: {raw}'
            projected.append(raw)
            continue
        assert raw['type'] in ('invoke', 'ok', 'fail', 'info')
        assert raw['f'] in ('read', 'write', 'cas')
        kv = raw['value']
        assert isinstance(kv, list) and len(kv) == 2 and isint(kv[0])
        k, pair = kv
        keys.add(k)
        validate_value(raw['f'], pair)
        event = dict(raw, value=pair)
        if k == key:
            projected.append(event)
        if raw['type'] == 'invoke':
            assert p not in pending, 'Concurrent calls on one process'
            pending[p] = (pos, k, event)
            continue
        assert p in pending, 'Completion without invocation'
        ipos, ikey, inv = pending.pop(p)
        assert k == ikey and inv['f'] == event['f']
        if event['type'] in ('fail', 'info'):
            assert inv['value'] == event['value'], 'Unexpected uncertain/failed result payload'
        if k == key:
            calls.append(make_call(ipos, pos, inv, event))
    for ipos, k, inv in pending.values():
        if k == key:
            calls.append(make_call(ipos, None, inv, None))
    assert key in keys
    assert projected == read_events(independent_path), 'Independent projection mismatch'
    return full, projected, sorted(calls, key=lambda o: o['invoke_pos']), sorted(keys)


def make_call(ipos, cpos, inv, response):
    status = response['type'] if response else 'unfinished'
    return {'id': str(ipos), 'invoke_pos': ipos, 'complete_pos': cpos,
            'invocation': inv, 'response': response, 'status': status,
            'f': inv['f'],
            'model_value': response['value'] if status == 'ok' else inv['value']}


def write_edn(x):
    if x is None:
        return 'nil'
    if x is True:
        return 'true'
    if x is False:
        return 'false'
    if isinstance(x, str):
        return json.dumps(x, ensure_ascii=True)
    if isinstance(x, (int, float)):
        return str(x)
    if isinstance(x, list):
        return '[' + ' '.join(write_edn(v) for v in x) + ']'
    if isinstance(x, dict):
        return '{' + ' '.join(':' + k + ' ' + write_edn(v) for k, v in x.items()) + '}'
    raise TypeError(type(x))
