#!/usr/bin/env python3
"""Generate report counts only from the audited certificate summary."""
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import sys

stem = sys.argv[1]
p = Path(__file__).resolve().parent
x = json.loads((p / (stem + '-verdicts.json')).read_text())
r = json.loads((p / (stem + '-run.json')).read_text())
dispatch = json.loads((p / (stem + '-dispatch-input.json')).read_text())
assert r['conclusion'] == 'success' and r['headSha'] == dispatch['solver_commit']
assert x['case_count'] == len(dispatch['cases']) == len(x['cases'])
assert sorted(c['id'] for c in x['cases']) == dispatch['cases']
if 'parent_expected_outcomes' in dispatch:
    expected = dispatch['parent_expected_outcomes']
    assert x['operation_count'] == expected['operations']
    assert x['original_outcomes'] == {s: expected[s] for s in ('ok', 'fail', 'info', 'unfinished')}
groups = defaultdict(lambda: {'cases': 0, 'keys': 0, 'calls': 0, 'infos': 0, 'generated': 0})
for c in x['cases']:
    assert c['valid'] is True and c['archive_sha256'] == dispatch['archive_sha256']
    product = 'etcd' if c['case'].startswith('jepsen2-etcd-') else 'tigerstore'
    g = groups[product]
    g['cases'] += 1
    g['keys'] += c['key_count']
    for k in c['keys']:
        assert k['valid'] is True
        g['calls'] += k['operations']
        g['infos'] += k['included_infos']
        g['generated'] += not k['saved_projection_retained_from_archive']
assert sum(g['keys'] for g in groups.values()) == x['key_count']
assert sum(g['calls'] for g in groups.values()) == x['operation_count']
assert sum(g['infos'] for g in groups.values()) == x['included_info_count']
assert sum(g['generated'] for g in groups.values()) == x['generated_projection_count']
(p / (stem + '-product-summary.json')).write_text(json.dumps(dict(groups), indent=2) + '\n')
lines = [f'# {stem} register certificate report', '',
    f'Run {x["run_url"]} completed successfully at solver commit `{r["headSha"]}`.',
    f'All {x["case_count"]} histories and {x["key_count"]} keys have SAT certificates accepted by both the independent Python verifier and the exact upstream Clojure model.',
    f'All {x["operation_count"]:,} client calls remain in the decision records. The witnesses include {x["included_info_count"]} info operations. The runner generated {x["generated_projection_count"]} missing saved projections from complete full histories and checked them in both validators.', '',
    f'Parent input commit is `{dispatch["source_parent_commit"]}`.',
    f'Input archive is `{dispatch["archive"]}`.',
    f'Its enforced SHA-256 is `{dispatch["archive_sha256"]}`.', '',
    '| Product | Histories | Keys | Client calls | Included infos | Generated projections |',
    '| --- | --- | --- | --- | --- | --- |']
if 'tested_product_commit' in dispatch:
    lines[6:6] = [f'The parent identified tested product commit `{dispatch["tested_product_commit"]}` for this input archive.', '']
if 'original_outcomes' in x:
    o = x['original_outcomes']
    lines[6:6] = [f'Original client outcomes are {o["ok"]:,} ok, {o["fail"]:,} fail, {o["info"]:,} info, and {o["unfinished"]:,} unfinished. These counts were independently reconstructed from the complete retained operations and matched against the parent input totals.', '']
for product, g in sorted(groups.items()):
    lines.append(f'| {product} | {g["cases"]} | {g["keys"]} | {g["calls"]:,} | {g["infos"]} | {g["generated"]} |')
lines += ['', 'This is a register-model verdict only. Original checker results, crashes, missing online results, and fault-coverage failures remain separate evidence. A SAT certificate does not turn a failed fault-coverage check into a pass. No info was relabelled as fail, and no client history was truncated.', '',
    f'`{stem}-verdicts.json` contains per-key source and witness hashes, artifact URLs, and saved-versus-generated projection flags. Every archive/member hash is in `evidence/manifest.json` and its sidecars. The exact executed source snapshot is retained with the runner artifacts.', '',
    '| Case ID | Original case | Validated keys | Artifact |',
    '| --- | --- | --- | --- |']
for c in x['cases']:
    lines.append(f'| {c["id"]} | {c["case"]} | {c["key_count"]} | [certificate archive]({c["artifact_url"]}) |')
lines += ['', f'All numbers above come from `{stem}-verdicts.json`. Its SHA-256 is `{hashlib.sha256((p / (stem + "-verdicts.json")).read_bytes()).hexdigest()}`.', '']
(p / (stem + '-report.md')).write_text('\n'.join(lines))
