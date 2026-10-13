#!/usr/bin/env python3
"""Join retained certificates to original inputs. No solve, JVM, or workload."""
import datetime
import hashlib
import json
from pathlib import Path
import sys
import tarfile

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / 'analysis'))
from analyze import EDN


def digest(data):
    return hashlib.sha256(data).hexdigest()


def load(path):
    return json.loads(path.read_text())


def certify(cert, history, archive_sha, count):
    assert cert['valid'] is True
    assert cert['archive_sha256'] == archive_sha
    assert cert['full_sha256'] == digest(history)
    keys, invokes = set(), 0
    for line in history.decode().splitlines():
        if not line.strip():
            continue
        event = EDN(line).load()
        if event[':process'] != ':nemesis' and event[':type'] == ':invoke':
            invokes += 1
            keys.add(event[':value'][0])
    assert invokes == count
    assert keys == {k['key'] for k in cert['keys']}
    assert len(keys) == len(cert['keys']) == cert['key_count']
    assert sum(k['operations'] for k in cert['keys']) == count
    for k in cert['keys']:
        assert k['valid'] is True
        assert k['full_sha256'] == cert['full_sha256']
        assert k['included'] + k['omitted_optional'] + k['upstream_failures'] == k['operations']
    return {'register_valid': True, 'key_count': len(keys),
            'history_sha256': cert['full_sha256'], 'input_archive_sha256': archive_sha,
            'included_infos': sum(k['included_infos'] for k in cert['keys']),
            'certificate_artifact_url': cert['artifact_url']}


def main():
    assert datetime.datetime.now(datetime.timezone.utc) < datetime.datetime.fromisoformat('2026-10-13T22:00:00+00:00')
    rows, archive_hashes = [], {}
    old_stats = {(c['run'], c['case']): c for c in load(ROOT / 'analysis/summary.json')['cases']}
    for cert in load(ROOT / 'evidence/old-verdicts.json')['cases']:
        run, case = cert['original_run'], cert['case']
        raw = old_stats[(run, case)]
        path = ROOT.parent / 'jepsen/evidence' / (run + '.tar.gz')
        if str(path) not in archive_hashes:
            archive_hashes[str(path)] = hashlib.file_digest(path.open('rb'), 'sha256').hexdigest()
        sha = archive_hashes[str(path)]
        with tarfile.open(path) as archive:
            history = archive.extractfile(case + '/store/latest/history.edn').read()
        proof = certify(cert, history, sha, raw['count'])
        rows.append({'scope': 'original', 'store': 'tigerstore',
                     'product_commit': '90870876b6459334c7eddd67c0f9d184267f2eba',
                     'workload_run': run, 'case': case, 'command': raw['command'],
                     'operations': raw['count'],
                     'counts': {t: raw[t[1:]] for t in (':ok', ':fail', ':info')},
                     'original_online_verdict': raw['verdict'],
                     'faults_valid': not raw['coverage_errors_from_saved_summary'],
                     'fault_evidence_source': 'Retained round-1 coverage summary', **proof})
    for scope, run, cert_file, commit in [
        ('first fresh matrix', '38440292743', 'fresh40-verdicts.json', 'b5f2e28ef2edd94393d62952b1568763ebc12fe2'),
        ('latest main', '38448413416', 'latest20-verdicts.json', '2b072ac7e7acb4aeef1a6e794145914201607b8c')]:
        raw_cases = {c['case']: c for c in load(ROOT / 'evidence' / (run + '-summary.json'))}
        cert_cases = load(ROOT / 'evidence' / cert_file)['cases']
        assert set(raw_cases) == {c['case'] for c in cert_cases}
        path = ROOT / 'evidence' / (run + '.tar.gz')
        sha = hashlib.file_digest(path.open('rb'), 'sha256').hexdigest()
        with tarfile.open(path) as archive:
            for cert in cert_cases:
                case = cert['case']
                raw = raw_cases[case]
                history = archive.extractfile(case + '/store/latest/history.edn').read()
                proof = certify(cert, history, sha, raw['operations'])
                if raw['store'] == 'tigerstore':
                    recorded = archive.extractfile(case + '/product-commit.txt').read().decode().strip()
                    assert recorded == commit
                rows.append({'scope': scope, 'store': raw['store'],
                             'product_commit': commit if raw['store'] == 'tigerstore' else '68c065e562994b89e333e77b039ad066f933c586',
                             'workload_run': run, 'case': case, 'command': raw['command'],
                             'operations': raw['operations'], 'counts': raw['counts'],
                             'original_online_verdict': raw['original_jepsen_verdict'],
                             'other_online_checks': raw['original_non_workload_verdicts'],
                             'faults_valid': raw['faults_valid'], 'fault_errors': raw['fault_errors'],
                             'fault_evidence_source': 'summarize-fresh.py on complete retained history and observer logs',
                             **proof})
    result = {'scope': 'Register-model verdicts. Original attempts and fault failures remain unchanged.',
              'case_count': len(rows), 'key_count': sum(r['key_count'] for r in rows),
              'operation_count': sum(r['operations'] for r in rows),
              'all_register_valid': all(r['register_valid'] for r in rows),
              'fault_failures': [r['case'] for r in rows if not r['faults_valid']], 'cases': rows}
    (ROOT / 'evidence/final-verdicts.json').write_text(json.dumps(result, indent=2) + '\n')
    lines = ['# Per-history verdicts', '',
             'A valid register history is separate from a successful requested-fault test. Original online results remain unchanged. Certificate artifacts also have permanent copies linked from [the report](REPORT.md).', '',
             '| Scope | Store/commit | Workload run | Case | Calls | Keys | Original online result | Register certificate | Fault evidence |',
             '| --- | --- | --- | --- | --- | --- | --- | --- | --- |']
    for r in rows:
        product = 'etcd v3.7.2' if r['store'] == 'etcd' else r['product_commit'][:7]
        run = r['workload_run']
        lines.append(f"| {r['scope']} | {product} | [{run}](https://github.com/djosh34/tigerstore/actions/runs/{run}) | {r['case']} | {r['operations']} | {r['key_count']} | {r['original_online_verdict']} | valid | {'observed' if r['faults_valid'] else 'FAILED'} |")
    (ROOT / 'CASES.md').write_text('\n'.join(lines) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k != 'cases'}, indent=2))


if __name__ == '__main__':
    main()
