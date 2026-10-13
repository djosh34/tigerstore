#!/usr/bin/env python3
"""Audit archived history/certificate accounting, without rerunning solvers."""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import sys
import tarfile


def archive(path):
    with tarfile.open(path) as tar:
        return {m.name.removeprefix('./'): tar.extractfile(m).read()
                for m in tar if m.isfile() and
                m.name.endswith(('history.edn', 'summary.json', 'results.edn', 'product-commit.txt'))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    sys.path.insert(0, str(args.source / 'experiments/jepsen2/analysis'))
    from analyze import EDN
    evidence = args.source / 'experiments/jepsen3/evidence'
    rows = []
    for store, live_run, cert_run in [('tigerstore', '38468196852', '38469322232'),
                                      ('etcd', '38468255858', '38469359753')]:
        live = archive(evidence / f'{live_run}.tar.gz')
        cert = archive(evidence / f'{cert_run}.tar.gz')
        audits = {r['case']: r for r in json.loads((evidence / f'{live_run}-audit.json').read_text())}
        assert len(audits) == 6
        for workload in ['register', 'set', 'append']:
            for rep in [1, 2]:
                case = f'jepsen3-{store}-{workload}-rep{rep}'
                prefix = case + '/store/latest/'
                raw = live[prefix + 'history.edn']
                events = [EDN(line).load() for line in raw.decode().splitlines() if line.strip()]
                calls = [e for e in events if e[':type'] == ':invoke' and isinstance(e[':process'], int)]
                counts = collections.Counter(e[':type'] for e in events if isinstance(e[':process'], int) and e[':type'] != ':invoke')
                digest = hashlib.sha256(raw).hexdigest()
                audit = audits[case]
                assert len(calls) == audit['operations'] == sum(counts.values())
                assert dict(counts) == audit['counts']
                assert audit['faults_valid'] and not audit['fault_errors']
                assert audit['ops_per_key'] == 50 and audit['nemesis'] == 'kill,partition,pause'
                if store == 'tigerstore':
                    assert live[case + '/product-commit.txt'].decode().strip() == '499a4c6759d0c1276aa4257af462295216f8ab21'
                row = {'case': case, 'calls': len(calls), 'history_sha256': digest,
                       'faults_valid': True, 'original_verdict': audit['original_jepsen_verdict']}
                if workload == 'register':
                    cp = f'jepsen3-cert-{store}-{live_run}-rep{rep}/case/'
                    summary = json.loads(cert[cp + 'summary.json'])
                    assert cert[cp + 'input/history.edn'] == raw
                    keys = collections.Counter(e[':value'][0] for e in calls)
                    assert summary['valid'] is True
                    assert set(keys) == set(summary['expected_keys']) == {int(k) for k in summary['results']}
                    for key, result in summary['results'].items():
                        assert result['valid'] is True and result['full_sha256'] == digest
                        assert result['python_verifier_exit'] == result['clojure_verifier_exit'] == 0
                        validation = result['validation']
                        assert validation['valid'] is True and validation['operations'] == keys[int(key)]
                        assert validation['included'] + validation['omitted'] + validation['upstream_failures'] == keys[int(key)]
                    row.update(keys=len(keys), final_verdict='valid certificate',
                               included_unknowns=sum(r['validation']['included_infos'] for r in summary['results'].values()))
                else:
                    result = EDN(live[prefix + 'results.edn'].decode()).load()
                    assert result[':valid?'] is True
                    assert result[':workload'][':valid?'] is True
                    row['final_verdict'] = 'valid upstream checker'
                rows.append(row)
    report = {'scope': 'Archive accounting audit, not a new solver or model check',
              'cases': rows, 'histories': len(rows), 'calls': sum(r['calls'] for r in rows),
              'register_keys': sum(r.get('keys', 0) for r in rows), 'passed': True}
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k != 'cases'}))


if __name__ == '__main__':
    main()
