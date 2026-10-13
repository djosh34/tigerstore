#!/usr/bin/env python3
"""Audit retained runner outputs and build an integration-ready summary."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def aggregate(root, run_id, artifacts):
    artifact_map = {a['name']: a for a in artifacts['artifacts']}
    cases = []
    for directory in sorted(root.glob('register-solver-*'), key=lambda p: int(p.name.rsplit('-', 1)[1])):
        case_id = int(directory.name.rsplit('-', 1)[1])
        summary = json.loads((directory / 'case/summary.json').read_text())
        manifest = json.loads((directory / 'manifest.json').read_text())[case_id]
        model = json.loads((directory / 'model-source.json').read_text())
        assert model['record_identical'] is True
        assert model['upstream_commit'] == 'b47cc401f27bfb7c28afd9ac088b5f0dca4501b8'
        if 'model_record_sha256' in model:
            assert model['model_record_sha256'] == '9d4bbb5a3652132be1553cc3f7acb66b5c845b9a82c0922b6d77c9bfe6dd2919'
            assert model['upstream_source_sha256'] == '02d9feff2d36ab137887d760184db6375cf83d5520158b0a322418af3aa58c92'
            assert model['copied_checker_source_sha256'] == '72a57a361462b3ef2095e79b112c98b5aa75695ef6f1d152fcd000a7e60b4668'
        else:
            # Preserve the documented legacy label, without accepting a different source.
            assert model['upstream_source_sha256'] == '72a57a361462b3ef2095e79b112c98b5aa75695ef6f1d152fcd000a7e60b4668'
        source = directory / 'case/input/history.edn'
        members = {m['path']: m['sha256'] for m in manifest['members']}
        retained = {m['path']: m.get('retained_from_archive', True) for m in manifest['members']}
        assert sha(source) == members['history.edn']
        expected_keys = manifest['keys']
        assert summary['coverage_complete'] is True and summary['valid'] is True
        assert sorted(int(k) for k in summary['results']) == expected_keys == summary['expected_keys']
        key_results = []
        for key in expected_keys:
            r = summary['results'][str(key)]
            p = directory / 'case' / str(key)
            assert r['valid'] is True
            assert r['solver_exit'] == r['python_verifier_exit'] == r['clojure_verifier_exit'] == 0
            assert r['solve']['solver_result'] == 'sat'
            py = json.loads((p / 'python-validation.json').read_text())
            assert py['valid'] is True and py['key'] == key
            json_sha, edn_sha = sha(p / 'witness.json'), sha(p / 'witness.edn')
            assert py['witness_sha256'] == json_sha
            clj = (p / 'clojure-validation.edn').read_text()
            assert ':valid? true' in clj
            assert re.search(r':witness-sha256 "' + edn_sha + '"', clj)
            assert sha(directory / f'case/input/independent/{key}/history.edn') == members[f'independent/{key}/history.edn']
            assert r['full_sha256'] == members['history.edn']
            assert r['independent_sha256'] == members[f'independent/{key}/history.edn']
            witness = json.loads((p / 'witness.json').read_text())
            assert len(witness['operations']) == r['solve']['operation_count'] == py['operations']
            outcomes = Counter(o['status'] for o in witness['operations'])
            assert set(outcomes) <= {'ok', 'fail', 'info', 'unfinished'}
            assert outcomes['fail'] == py['upstream_failures']
            assert outcomes['ok'] + py['included_infos'] == py['included']
            assert witness['full_sha256'] == r['full_sha256']
            assert witness['independent_sha256'] == r['independent_sha256']
            key_results.append({'key': key, 'valid': True, 'operations': py['operations'],
                'included': py['included'], 'included_infos': py['included_infos'],
                'original_outcomes': {s: outcomes[s] for s in ('ok', 'fail', 'info', 'unfinished')},
                'omitted_optional': py['omitted'], 'upstream_failures': py['upstream_failures'],
                'real_time_edges_checked': py['real_time_edges_checked'],
                'final_version': py['final_version'], 'phase': r['solve']['phase'],
                'json_witness_sha256': json_sha, 'edn_witness_sha256': edn_sha,
                'full_sha256': r['full_sha256'], 'independent_sha256': r['independent_sha256'],
                'saved_projection_retained_from_archive': retained[f'independent/{key}/history.edn'],
                'artifact_relative_path': f'case/{key}'})
        artifact = artifact_map[directory.name]
        cases.append({'id': case_id, 'case': manifest['case'], 'original_run': manifest['run'],
            'valid': True, 'key_count': len(expected_keys), 'archive_sha256': manifest['archive_sha256'],
            'full_sha256': members['history.edn'], 'model_source': model,
            'artifact_id': artifact['id'],
            'artifact_url': f'https://github.com/djosh34/tigerstore/actions/runs/{run_id}/artifacts/{artifact["id"]}',
            'keys': key_results})
    assert cases
    return {'scope': 'versioned register model only, original fault and other checks unchanged',
        'audit_tool_sha256': sha(Path(__file__)),
        'run_id': run_id, 'run_url': f'https://github.com/djosh34/tigerstore/actions/runs/{run_id}',
        'cases': cases, 'case_count': len(cases), 'key_count': sum(c['key_count'] for c in cases),
        'all_retained_cases_valid': True,
        'operation_count': sum(k['operations'] for c in cases for k in c['keys']),
        'included_info_count': sum(k['included_infos'] for c in cases for k in c['keys']),
        'generated_projection_count': sum(not k['saved_projection_retained_from_archive']
                                          for c in cases for k in c['keys']),
        'original_outcomes': {s: sum(k['original_outcomes'][s] for c in cases for k in c['keys'])
                              for s in ('ok', 'fail', 'info', 'unfinished')}}


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('root', type=Path)
    p.add_argument('run', type=int)
    p.add_argument('artifacts', type=Path)
    p.add_argument('output', type=Path)
    a = p.parse_args()
    result = aggregate(a.root, a.run, json.loads(a.artifacts.read_text()))
    a.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k != 'cases'}, indent=2))
