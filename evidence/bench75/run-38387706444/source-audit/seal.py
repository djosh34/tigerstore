#!/usr/bin/env python3
"""Final source-only verification and evidence seal; no Go/toolchain execution."""
from pathlib import Path
import json,datetime,subprocess,hashlib,re
R=Path('/home/joshazimullah.linux/work_mounts/scratch/bench7980-source-audit')
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
expected={'main':'8efd83a173394dd94a3981c72b4c51a1f3d061f1','wire':'936bc5b69ae8a0c59585521734b41222006a0a2c','hpack':'129e49a842b4802e9a36b74c830b3538525f846f','txn':'6179f12c2bbaaffc9960782533ad83c292605962'}
assert json.loads((R/'final-targets.json').read_text())==expected
for script in ['prepare.py','verify-sites.py','build-report.py']:
 subprocess.run(['python3',str(R/script)],cwd=R,check=True)
rows=json.loads((R/'verified-sites.json').read_text())
resolutions=[]
for change in json.loads((R/'relocation-changes.json').read_text()):
 id='alias-apply' if change['id']=='generic-apply-still-exists' and change['target']=='txn' else change['id']
 matches=[x for x in rows if x['target']==change['target'] and x['id']==id]
 assert len(matches)==1,('unresolved changed prior site',change)
 x=matches[0]
 resolutions.append({'target':x['target'],'old_site_id':change['id'],'verified_replacement_id':id,'file':x['file'],'line':x['line'],'expected':x['expected'],'sha':x['sha']})
(R/'relocation-resolution.json').write_text(json.dumps(resolutions,indent=2)+'\n')
allowed={(x['file'],x['line']) for x in rows}
for filename in ['audit.md','dependency-proof.md']:
 text=(R/filename).read_text();assert 'PRELIMINARY ONLY' not in text
 for file,line in re.findall(r'`((?:internal/|dependency-sources/)[^`:]+):(\d+)`',text):assert (file,int(line)) in allowed,(filename,file,line)
for x in rows:assert x['sha']==expected[x['target']]
control=json.loads((R/'tree-control.json').read_text());assert control['main_wire_complete_tree_equal'] and control['main_wire_git_diff_empty']
# Refresh production channel inventory at exact bound sources, not checkout/live branches.
inv=[]
for t in expected:
 for pkg in ['server','node','peer']:
  for p in sorted((R/'sources'/t/'internal'/pkg).glob('*.go')):
   if p.name.endswith('_test.go'):continue
   for i,line in enumerate(p.read_text().splitlines(),1):
    if '<-' in line or 'close(' in line:inv.append(f"{t} {p.relative_to(R/'sources'/t)}:{i}: {line}")
(R/'channel-inventory.txt').write_text('\n'.join(inv)+'\n')
summary={'verified_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'measurement_run':38387706444,'resolver_timestamp':'2026-10-13T11:04:41Z','targets':expected,'verified_exact_sites':len(rows),'verified_repository_files':len(json.loads((R/'source-file-hashes.json').read_text())),'dependency_modules_verified':len(json.loads((R/'dependency-integrity.json').read_text())),'same_code_control':'main == PR77 complete Git tree','put_leader_named_input_materializations':dict.fromkeys(expected,4),'cas_main_leader_named_input_materializations_success_or_compare_failure':6,'cas_pr80_leader_named_input_materializations_success_or_compare_failure':4,'cas_compare_failure_extra_returned_stored_value_encode':1,'normal_fixed_private_channel_sends':0,'normal_fixed_private_channel_receives':0,'normal_explicit_repository_channel_closes':0,'go_executions':0,'subagents':0,'github_writes':0,'repository_writes_by_audit':0,'scope_note':'Named materializations only; peers conditional and physical transport/growth excluded. Actual A/Q/F/O/I/U/Sw/Rw/Ticks and pass/fail fractions belong to runtime evidence.'}
(R/'final-verification.json').write_text(json.dumps(summary,indent=2)+'\n')
manifest=[]
for p in sorted(R.rglob('*')):
 if not p.is_file() or 'object-repo' in p.relative_to(R).parts or p.name=='archive-file-manifest.json':continue
 manifest.append({'file':str(p.relative_to(R)),'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size,'provenance_only':p.relative_to(R).parts[0]=='earlier-preliminary'})
(R/'archive-file-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps(summary,indent=2))
