#!/usr/bin/env python3
"""Audit retained certificate accounting and original bytes. Does not solve or replay."""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / 'jepsen2/analysis'))
from analyze import EDN, read_edn


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def audit(artifact, downloads):
    match = re.fullmatch(r'jepsen3-cert-(tigerstore|etcd)-(\d+)-rep([12])', artifact.name)
    assert match
    store, run, repetition = match.groups()
    case = f'jepsen3-{store}-register-rep{repetition}'
    original = downloads / run / case / 'store/latest/history.edn'
    history_sha = digest(original)
    assert digest(artifact / 'case/input/history.edn') == history_sha
    counts = collections.Counter()
    for line in original.read_text().splitlines():
        if not line.strip():
            continue
        event = EDN(line).load()
        if isinstance(event[':process'], int) and event[':type'] == ':invoke':
            counts[event[':value'][0]] += 1
    summary = json.loads((artifact / 'case/summary.json').read_text())
    assert summary['valid'] is True and summary['coverage_complete'] is True
    assert set(summary['expected_keys']) == set(counts)
    assert set(summary['results']) == {str(k) for k in counts}
    assert json.loads((artifact / 'model-source.json').read_text())['record_identical'] is True
    assert json.loads((artifact / 'selftest/summary.json').read_text())['valid'] is True
    if store == 'tigerstore':
        assert (artifact / 'product-commit.txt').read_text().strip() == '499a4c6759d0c1276aa4257af462295216f8ab21'
    keys = []
    for key, expected in sorted(counts.items()):
        result = summary['results'][str(key)]
        folder = artifact / 'case' / str(key)
        python = json.loads((folder / 'python-validation.json').read_text())
        clojure = read_edn(folder / 'clojure-validation.edn')
        assert result['valid'] is True and result['full_sha256'] == history_sha
        assert result['solver_exit'] == result['python_verifier_exit'] == result['clojure_verifier_exit'] == 0
        assert python['valid'] is True and clojure[':valid?'] is True
        assert python == result['validation']
        assert python['operations'] == clojure[':operations'] == expected
        assert python['included'] + python['omitted'] + python['upstream_failures'] == expected
        assert python['witness_sha256'] == digest(folder / 'witness.json')
        assert clojure[':witness-sha256'] == digest(folder / 'witness.edn')
        assert python['included'] == clojure[':included']
        keys.append({'key': key, 'operations': expected, 'included_infos': python['included_infos'],
                     'python_valid': True, 'exact_clojure_model_valid': True})
    return {'case': case, 'store': store, 'workload_run': run, 'register_valid': True,
            'history_sha256': history_sha, 'key_count': len(keys), 'operations': sum(counts.values()),
            'included_infos': sum(k['included_infos'] for k in keys), 'keys': keys}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('downloads', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    rows = [audit(p, args.downloads) for p in sorted(args.downloads.glob('*/jepsen3-cert-*'))]
    assert rows
    args.output.write_text(json.dumps({'all_register_valid': True, 'cases': rows,
        'histories': len(rows), 'keys': sum(r['key_count'] for r in rows),
        'operations': sum(r['operations'] for r in rows)}, indent=2) + '\n')
    print(json.dumps({'histories': len(rows), 'keys': sum(r['key_count'] for r in rows),
                      'operations': sum(r['operations'] for r in rows), 'all_register_valid': True}))
