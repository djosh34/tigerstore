#!/usr/bin/env python3
"""Selected-source-only extraction; read-only Git and prior audit inputs."""
from pathlib import Path
import subprocess,json,hashlib,datetime
ROOT=Path('/home/joshazimullah.linux/work_mounts/scratch/bench84-lean-source-audit')
REPO=Path('/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75')
OLD=Path('/home/joshazimullah.linux/work_mounts/scratch/bench80-lean-source-audit')
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
targets={'main':'aa9d9b6eea79c73f12fe0153aefa809841967e88','pr84':'d5681651cd554b8804ed3a77b9ff019ade6815bf'}
binding={'status':'PRELIMINARY — NOT MEASUREMENT BINDING','targets':targets,'candidate_retained_ref':'refs/pull/84/head','note':'Parent final resolve SHAs must override; no automatic mutable-ref resolution or Git fetch.'}
if (ROOT/'final-binding.json').exists():
 binding=json.loads((ROOT/'final-binding.json').read_text());assert binding['status']=='FINAL' and binding['parent_confirmed'] is True
 targets=binding['targets'];assert set(targets)=={'main','pr84'}
(ROOT/'binding.json').write_text(json.dumps(binding,indent=2)+'\n')
# Only Put boundaries and supporting proof sites; no generic CAS/alias evidence copied.
ids='''lib-put-value-field lib-bytes-encode-copy lib-bytes-consume-alias lib-proto2-decode-copy lib-proto2-codec-selection lib-proto2-clone-copy lib-proto-clone-merge follower-logappend unstable-append lib-peer-construction-alias lib-raft-clone lib-raft-propose-clone-call follower-maybeappend lib-entry-data-field lib-entry-proto2 lib-raft-propose-alias async-append-alias async-apply-alias entry-buffer-init entry-value-encode wire-batch-slice pending-owner batch-pending-allocation submitall-loop wire-check wire-size core-encode-call raft-propose apply-dispatch apply-wire-kind apply-wire-parse apply-wire-state apply-wire-reply wire-state-index storage-pointer-append no-watch-fastpath wire-parse-consume wire-parse-clip wire-value-alias putop-alias index-version-append index-value index-key admission-send driver-batch-owner batch-api batch-wrapper batch-inbox-call peer-inbox-send unreachable-send do-reply-create do-reply-receive shutdown-close inbox-receive-outer inbox-receive-drain disk-complete-receive disk-send tick-receive watch-close do-reply-send completion-direct batch-waiter batch-core-submit disk-receive-outer disk-receive-drain wal-append-call wal-sync-call disk-complete-send peer-value-decode peer-value-encode peer-queue-send peer-queue-receive-fast peer-queue-receive-control peer-queue-receive-data opbatch-owner wire-frontend-parse charge-request batch-submit-call completion-cas completion-wake-attempt completion-swap wire-reply-encode release-request charge-reply manual-response-prefix manual-response-append writer-wake-send control-wake-attempt settings-wake-attempt window-wake-attempt credit-wake-attempt hpack-decode hpack-field stream-context-gate stream-deadline timeout-guard stream-channel-guard receive-data-caller unary-nil-signal stream-admit server-stop-close frontend-prealloc frontend-accumulation unary-delivery unary-nil-body-signal out-capacity writer-wake-receive node-done-receive unary-nil-space stream-end-release wal-value-copy wal-value-caller wal-write wal-sync'''.split()
prior=json.loads((OLD/'verified-sites.json').read_text());specs=[r for r in prior if r['target']=='main' and r['id'] in ids]
assert set(ids)=={r['id'] for r in specs},set(ids)-{r['id'] for r in specs}
files=sorted({r['file'] for r in specs if not r['file'].startswith('dependency-sources/')}|{'go.mod','go.sum','docs/design.md'})
manifest=[]
for t,sha in targets.items():
 assert len(sha)==40 and all(c in '0123456789abcdef' for c in sha)
 for f in files:
  b=subprocess.check_output(['git','show',sha+':'+f],cwd=REPO);p=ROOT/'sources'/t/f;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
  manifest.append({'target':t,'commit':sha,'path':str(p.relative_to(ROOT)),'repo_path':f,'sha256':hashlib.sha256(b).hexdigest()})
 for f in ('go.mod','go.sum'):
  assert (ROOT/'sources'/t/f).read_bytes()==(OLD/'sources/main'/f).read_bytes(),(t,f,'pin change: re-audit required')
for f in sorted({r['file'] for r in specs if r['file'].startswith('dependency-sources/')}):
 b=(OLD/f).read_bytes();digest=hashlib.sha256(b).hexdigest();assert {r['file_sha256'] for r in specs if r['file']==f}=={digest}
 p=ROOT/f;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
 manifest.append({'path':f,'sha256':digest,'reference':str(OLD/f)})
(ROOT/'prior-put-specs.json').write_text(json.dumps(specs,indent=2)+'\n')
(ROOT/'source-file-hashes.json').write_text(json.dumps(manifest,indent=2)+'\n')
refs=[OLD/'audit.md',OLD/'verified-sites.json',OLD/'SHA256SUMS',OLD/'lean-etcd-scope.md',OLD/'reference-manifest.json',OLD/'workload-manifest.json',OLD/'final-binding.json',OLD/'generator-binding.json']
refrows=[{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()} for p in refs]
for r in json.loads((OLD/'reference-manifest.json').read_text())['references']:
 p=REPO/r['path'];assert hashlib.sha256(p.read_bytes()).hexdigest()==r['sha256'];refrows.append({'path':str(p),'sha256':r['sha256']})
(ROOT/'reference-manifest.json').write_text(json.dumps({'sealed80_read_only':str(OLD),'references':refrows,'etcd_reused_by_reference':True,'target_go.mod_go.sum':'byte-identical to sealed80 main; dependency proof reused, not module ZIP copies'},indent=2)+'\n')
print('Extracted',len(manifest),'selected files;',len(specs),'Put/support site specs;',binding['status'])
