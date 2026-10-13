#!/usr/bin/env python3
"""Small metadata/probe audit. Does not run a model checker or inspect server disks."""
import argparse
import base64
import collections
import datetime as dt
import gzip
import hashlib
import json
from pathlib import Path


def load(path):
    with gzip.open(path, 'rt') as stream:
        return json.load(stream)


def require(ok, reason):
    if not ok:
        raise AssertionError(reason)


def metadata(kv):
    # Normalize protobuf JSON and the repository's history JSON.
    if isinstance(kv.get('key'), str) and kv['key'].startswith('/correct-'):
        return (kv['key'], kv.get('value', ''), kv.get('createRevision', 0),
                kv.get('modRevision', 0), kv.get('version', 0), kv.get('lease', 0))
    return (base64.b64decode(kv['key']).decode(), base64.b64decode(kv.get('value', '')).decode(),
            kv.get('create_revision', 0), kv.get('mod_revision', 0), kv.get('version', 0), kv.get('lease', 0))


def probe_check(probe, installed, healed):
    require(probe['passed'] and probe['list']['passed'], 'probe verdict not pass')
    require(installed <= probe['call_utc_ns'] < probe['return_utc_ns'] <= healed,
            'probe did not run wholly during installed fault')
    chunks = probe['list']['chunks']
    require(len(chunks) >= 2 and len(chunks[0]['kvs']) == 10, 'missing multi-chunk path')
    for chunk in chunks[:-1]:
        require(not chunk.get('header') and not chunk.get('count') and not chunk.get('more'),
                'nonfinal header/count/more')
    last = chunks[-1]
    target = probe['target']
    require(last['header']['revision'] == target and target > 1, 'wrong pinned target')
    values = [metadata(kv) for chunk in chunks for kv in chunk.get('kvs', [])]
    unary = probe['list']['historical_unary']
    require(len(values) == 32 and last['count'] == unary['count'] == 32 and
            not last.get('more') and not unary.get('more'), 'wrong count/more')
    require(values == [metadata(kv) for kv in unary['kvs']], 'stream/unary full metadata mismatch')
    require([kv[0] for kv in values] == sorted(set(kv[0] for kv in values)), 'unordered or duplicate keys')
    state = {}
    previous_revision = 0
    covered = False
    responses = probe['watch_responses']
    for response in responses:
        require(not response.get('canceled'), 'canceled watch included as success')
        for event in response.get('events', []):
            require(event['type'] == 1, 'unexpected event type')
            value = metadata(event['kv'])
            require(value[3] > previous_revision, 'watch mutation duplicated or reordered')
            previous_revision = value[3]
            if value[3] > target:
                continue
            old = state.get(value[0])
            if old is None:
                require(not event.get('prevKV') and value[4] == 1 and value[2] == value[3], 'create metadata wrong')
            else:
                require(metadata(event['prevKV']) == old and value[4] == old[4] + 1 and value[2] == old[2], 'PrevKV/version chain broken')
            state[value[0]] = value
        covered |= response.get('progress', False) and response['revision'] >= target
    require(covered and [state[key] for key in sorted(state)] == values, 'watch incomplete at independent target')
    return {'chunks': len(chunks), 'watch_responses': len(responses),
            'watch_events': sum(len(row.get('events', [])) for row in responses)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=Path, default=Path('experiments/correct-round3/evidence'))
    parser.add_argument('--out', type=Path, default=Path('experiments/correct-round3/audit.json'))
    args = parser.parse_args()
    cases = []
    for run in sorted(args.root.iterdir()):
        for compact in sorted(run.glob('compact*/summary.json')):
            summaries = json.loads(compact.read_text())
            require(len(summaries) == 1, 'compact artifact contains multiple cases')
            summary = summaries[0]
            folder = compact.parent / summary['artifact']
            attempt = load(folder / 'attempt.json.gz')
            case = attempt['case']
            fault = load(folder / 'run/faults.json.gz')
            origin = int(dt.datetime.fromisoformat(fault['utc_origin']).timestamp()*1e9)
            probes = []
            errors = []
            for name in fault['required']:
                phases = fault['faults'].get(name, {})
                try:
                    require(all(phase in phases for phase in ['requested','installed','exercised','healed']), 'missing fault phase')
                    probe = load(folder / f'run/probe-{name}.json.gz')
                    detail = probe_check(probe, origin + phases['installed']['monotonic_ns'], origin + phases['healed']['monotonic_ns'])
                    probes.append({'fault': name, **detail})
                except (AssertionError, FileNotFoundError, KeyError) as exc:
                    errors.append(f'{name}: {exc}')
            checks = [result for p in sorted((folder/'run').glob('check-*.json.gz'))
                      if not p.name.endswith('.coverage.json.gz') for result in load(p)]
            failed = [result for result in checks if not result['passed']]
            coverage_path = folder / 'run/check-original.json.coverage.json.gz'
            coverage = load(coverage_path) if coverage_path.exists() else {}
            successful_clients = coverage.get('per_client_success', {})
            recorded_lists = sum(successful_clients.get(str(client), 0) for client in (60000, 60001, 60002))
            if run.name != '38468658463':
                require(recorded_lists > 0, 'freshness campaign omitted completed LIST operations')
            manifest = json.loads((folder/'manifest.json').read_text())
            # Rehash small assertion/fault/checker inputs. Large raw histories
            # remain compressed, with their original uncompressed SHA256s.
            verified = 0
            for entry in manifest:
                path = entry['path']
                if (path.startswith('run/probe-') or path in ('attempt.json','case.json','commits.txt','run/faults.json','run/list-watch-overlap.json','run/result.json','run/offline-results.json') or path.startswith('run/check-')):
                    raw = gzip.decompress((folder/(path+'.gz')).read_bytes())
                    require(len(raw) == entry['bytes'] and hashlib.sha256(raw).hexdigest() == entry['sha256'], f'hash mismatch {path}')
                    verified += 1
            require(summary['checker_verdicts'] == len(checks), 'verdict count mismatch')
            passed = summary['passed'] and not errors and len(checks) == 16 and not failed
            cases.append({'run': run.name, 'case': case, 'tooling': attempt['tooling'],
                          'passed': passed, 'controller_pass': summary['controller_pass'],
                          'operations': summary['operations'], 'history_variants': summary['history_variants'],
                          'list_freshness_in_whole_history': run.name != '38468658463',
                          'recorded_linearizable_lists': recorded_lists,
                          'acknowledged_dataset_mutations': successful_clients.get('50001', 0),
                          'verdicts': len(checks), 'passing_verdicts': sum(row['passed'] for row in checks),
                          'timeout_verdicts': summary['checker_timeout_verdicts'],
                          'failed_verdicts': failed, 'fault_probes': probes, 'probe_errors': errors,
                          'small_assertion_files_hash_verified': verified})
    by_run = {}
    for run in sorted({row['run'] for row in cases}):
        rows = [row for row in cases if row['run'] == run]
        by_run[run] = {'cases': len(rows), 'passed': sum(row['passed'] for row in rows),
                       'failed': sum(not row['passed'] for row in rows),
                       'operations': sum(row['operations'] for row in rows),
                       'verdicts': sum(row['verdicts'] for row in rows),
                       'recorded_linearizable_lists': sum(row['recorded_linearizable_lists'] for row in rows),
                       'history_variants': sum(row['history_variants'] for row in rows),
                       'passing_verdicts': sum(row['passing_verdicts'] for row in rows),
                       'timeout_verdicts': sum(row['timeout_verdicts'] for row in rows),
                       'fault_probes': sum(len(row['fault_probes']) for row in rows),
                       'probe_errors': sum(len(row['probe_errors']) for row in rows),
                       'by_target': {target: {'cases': sum(row['case']['target']==target for row in rows),
                                             'passed': sum(row['case']['target']==target and row['passed'] for row in rows)}
                                     for target in ('tigerstore','etcd')}}
    result = {'runs': by_run, 'cases': cases}
    args.out.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(by_run, indent=2))


if __name__ == '__main__':
    main()
