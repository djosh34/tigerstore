#!/usr/bin/env python3
"""Independently check saved in-fault stream snapshots and overlap bounds."""
import argparse
import base64
import collections
import datetime
import gzip
import json
from pathlib import Path


def load(path):
    with gzip.open(path) as stream:
        return json.load(stream)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    cases = []
    for manifest in sorted(args.root.rglob('manifest.json')):
        run = manifest.parent / 'run'
        faults = load(run / 'faults.json.gz')
        result = load(run / 'result.json.gz')
        assert result['controller_pass'] is True, manifest
        origin = int(datetime.datetime.fromisoformat(faults['utc_origin']).timestamp() * 1e9)
        row = {'case': str(manifest.parent.relative_to(args.root)), 'probes': []}
        assert faults['required']
        for name in faults['required']:
            phases = faults['faults'][name]
            assert {'requested', 'installed', 'exercised', 'healed'} <= set(phases)
            probe = load(run / f'probe-{name}.json.gz')
            assert probe['passed'] is True and probe['list']['passed'] is True
            assert origin + phases['installed']['monotonic_ns'] <= probe['call_utc_ns']
            assert probe['call_utc_ns'] < probe['return_utc_ns'] <= origin + phases['healed']['monotonic_ns']
            chunks = probe['list']['chunks']
            assert len(chunks) >= 2 and len(chunks[0]['kvs']) == 10
            for chunk in chunks[:-1]:
                assert not chunk.get('header') and not chunk.get('count') and not chunk.get('more')
            final = chunks[-1]
            assert final['header']['revision'] == probe['target'] > 1
            assert final['count'] == 32 and not final.get('more')
            kvs = [kv for chunk in chunks for kv in chunk.get('kvs', [])]
            assert len(kvs) == 32
            keys = [base64.b64decode(kv['key']) for kv in kvs]
            assert len(set(keys)) == 32 and keys == sorted(keys)
            unary = probe['list']['historical_unary']
            assert kvs == unary['kvs'] and unary['count'] == 32 and not unary.get('more')
            assert any(w.get('progress') and w['revision'] >= probe['target'] for w in probe['watch_responses'])
            row['probes'].append({'fault': name, 'chunks': len(chunks), 'target': probe['target'], 'passed': True})
        cases.append(row)
    assert cases
    counts = collections.Counter(r['case'].split('/')[0] for r in cases)
    assert all(n == 32 for n in counts.values()), counts
    report = {'scope': 'Snapshot payload, metadata and fault-overlap audit. Watch model checks remain the runner checks.',
              'cases': cases, 'case_counts_by_run': dict(counts),
              'probes': sum(len(r['probes']) for r in cases), 'passed': True}
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k != 'cases'}))


if __name__ == '__main__':
    main()
