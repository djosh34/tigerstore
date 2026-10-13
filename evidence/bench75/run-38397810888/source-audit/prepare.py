#!/usr/bin/env python3
"""Read-only selected-file extraction. No Go, network, checkout, or Git writes."""
from pathlib import Path
import subprocess, json, hashlib, datetime
ROOT=Path('/home/joshazimullah.linux/work_mounts/scratch/bench80-lean-source-audit')
REPO=Path('/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75')
OLD=REPO/'evidence/bench75/run-38387706444/source-audit'
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
targets={'main':'549e0c9c9918a80a320eb886ab1b3480ae73fa69','pr80':'67ff792a72f78e83cb6905563dcb6bc0e9bb2764'}
# Parent-supplied immutable binding ONLY; no branch discovery or head substitution.
if (ROOT/'final-binding.json').exists():
    binding=json.loads((ROOT/'final-binding.json').read_text())
    assert binding['status']=='FINAL' and binding.get('parent_confirmed') is True
    targets=binding['targets']; assert set(targets)=={'main','pr80'}
else:
    binding={'status':'PRELIMINARY — NOT MEASUREMENT BINDING','targets':targets,'final_pending':True}
(ROOT/'binding.json').write_text(json.dumps(binding,indent=2)+'\n')
old=json.loads((OLD/'site-specs.json').read_text())
files=sorted({r['file'] for r in old if r['target'] in ('hpack','txn') and not r['file'].startswith('dependency-sources/') }|{'go.mod','go.sum','docs/design.md'})
manifest=[]
for t,sha in targets.items():
    assert len(sha)==40 and all(c in '0123456789abcdef' for c in sha)
    for f in files:
        if f=='internal/core/alias.go' and t=='main':continue
        b=subprocess.check_output(['git','show',sha+':'+f],cwd=REPO)
        p=ROOT/'sources'/t/f;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
        manifest.append({'target':t,'commit':sha,'path':str(p.relative_to(ROOT)),'sha256':hashlib.sha256(b).hexdigest(),'git_blob':subprocess.check_output(['git','rev-parse',sha+':'+f],cwd=REPO,text=True).strip()})
# Retain only cited dependency files. Verify byte identity against prior proved evidence.
deps=sorted({r['file'] for r in old if r['target']=='txn' and r['file'].startswith('dependency-sources/')})
prev=json.loads((OLD/'dependency-source-manifest.json').read_text())
for f in deps:
    b=(OLD/f).read_bytes();digest=hashlib.sha256(b).hexdigest()
    expected={r['file_sha256'] for r in old if r['file']==f}
    assert expected=={digest},f
    p=ROOT/f;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
    manifest.append({'path':f,'sha256':digest,'provenance':str((OLD/f).relative_to(REPO))})
# Preserve proofs by reference and compare exact pins before reusing their conclusions.
pins=[]
for t in targets:
    gm=(ROOT/'sources'/t/'go.mod').read_bytes();gs=(ROOT/'sources'/t/'go.sum').read_bytes()
    assert gm==(OLD/'sources/hpack/go.mod').read_bytes(),(t,'go.mod pin change: re-audit required')
    assert gs==(OLD/'sources/hpack/go.sum').read_bytes(),(t,'go.sum change: re-audit required')
    pins.append({'target':t,'go.mod':'byte-identical to prior verified hpack/go.mod','go.sum':'byte-identical to prior verified hpack/go.sum'})
refs=['source-audit/audit.md','source-audit/dependency-integrity.json','source-audit/dependency-proof.md','source-audit/dependency-source-manifest.json','etcd-source-audit/audit.md','etcd-source-audit/verified-sites.md','etcd-source-audit/verification.json','etcd-source-audit/dependency-integrity.json','etcd-source-audit/client-dependency-integrity.json','etcd-source-audit/release-tag.json','etcd-source-audit/release-tag-object.json','etcd-source-audit/source/server/go.mod','etcd-source-audit/deps/google.golang.org_grpc@v1.83.2/internal/transport/http2_server.go','etcd-buildinfo.txt','etcd-release-sha256.txt']
refrows=[]
for f in refs:
    p=REPO/'evidence/bench75/run-38387706444'/f
    refrows.append({'path':str(p.relative_to(REPO)),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()})
(ROOT/'reference-manifest.json').write_text(json.dumps({'references':refrows,'pins':pins,'notes':'Prior etcd measurements/failure counts and grpc-timeout claims are not this run. Release source and pins reused, no refetch.'},indent=2)+'\n')
(ROOT/'source-file-hashes.json').write_text(json.dumps(manifest,indent=2)+'\n')
print('Extracted',len(manifest),'selected source/dependency files; unchanged dependency pins; '+binding['status'])
