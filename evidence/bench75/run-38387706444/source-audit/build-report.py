#!/usr/bin/env python3
from pathlib import Path
import json,re,datetime,subprocess
R=Path('/home/joshazimullah.linux/work_mounts/scratch/bench7980-source-audit')
REPO=Path('/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75')
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
final=(R/'final-targets.json').exists();ts=json.loads((R/('final-targets.json' if final else 'preliminary-targets.json')).read_text());rows=json.loads((R/'verified-sites.json').read_text())
for row in rows:assert row['sha']==ts[row['target']]
labels={'main':'M','wire':'W','hpack':'H','txn':'T'}
def site(t,id):
 matches=[x for x in rows if x['target']==t and x['id']==id];assert len(matches)==1,(t,id)
 x=matches[0];return f"`{x['file']}:{x['line']}`"
def dep(id):
 matches=[x for x in rows if x['id']==id];assert len(matches)==4
 assert len({(x['file'],x['line'],x['expected']) for x in matches})==1
 return f"{matches[0]['file']}:{matches[0]['line']}"
def table(title,columns,items):
 out=[title,'| Executed site / cost | '+' | '.join(labels[x] for x in columns)+' |','|---|'+'---|'*len(columns)]
 for text,ids in items:
  out.append('| '+text+' | '+' | '.join(site(t,id) if id else 'not this path' for t,id in zip(columns,ids))+' |')
 return '\n'.join(out)
put=table('### Put input-value boundaries (a codec call and dependency instruction count once)',list(ts),[
 ('Owned DATA accumulation: 1',['frontend-accumulation']*4),
 ('Full request-sized accumulation capacity: no growth copy',['frontend-prealloc']*4),
 ('Owning frontend decode: bypassed for Put ALL targets',[None,None,None,None]),
 ('Wire frontend aliases body: no decode copy',['wire-frontend-parse']*4),
 ('Entry encode/append: 1 each',['entry-value-encode']*4),
 ('Proposal → retained Raft clone: 1 each',['raft-propose']*4),
 ('WAL caller (no extra stage)',['wal-value-caller']*4),
 ('WAL record append: 1 each',['wal-value-copy']*4),
 ('Apply owning decode: bypassed for Put ALL targets',[None,None,None,None]),
 ('Wire apply branch: no value copy',['apply-wire-kind']*4),
 ('Wire apply ParsePut: aliases entry',['apply-wire-parse']*4),
 ('Index Value slice retention: 0',['index-value']*4),
 ('Version-array append: metadata only',['index-version-append']*4),
 ('Put reply encoding: 0 ordinary input-value copies',['wire-reply-encode']*4),
 ('Peer sender Entry.Data encoder: 1/transmitted occurrence',['peer-value-encode']*4),
 ('Peer receiver Entry.Data owning decoder: 1/received occurrence',['peer-value-decode']*4),
 ('Storage append: entry pointers only',['storage-pointer-append']*4),
 ('No-watch fast path: no event value encoding',['no-watch-fastpath']*4),
])
channel=table('### Channel sites: each receive alternative is ONE receipt, never additive for the same message',list(ts),[
 ('A admission sends',['admission-send']*4),
 ('Inbox receives: outer alternative',['inbox-receive-outer']*4),
 ('Inbox receives: drained-input alternative',['inbox-receive-drain']*4),
 ('Completion: direct call, 0 channel ops',['completion-direct']*4),
 ('Completion publish: CAS, 0 channel ops',['completion-cas']*4),
 ('Writer completion collect: atomic Swap, 0 channel ops',['completion-swap']*4),
 ('Q disk queue sends',['disk-send']*4),
 ('Q disk receives: initial array',['disk-receive-outer']*4),
 ('Q disk receives: another drained array',['disk-receive-drain']*4),
 ('F synced-group sends',['disk-complete-send']*4),
 ('F synced-group receives',['disk-complete-receive']*4),
 ('Selected tick receive',['tick-receive']*4),
 ('O peer queue sends',['peer-queue-send']*4),
 ('O peer queue receive: fast control alternative',['peer-queue-receive-fast']*4),
 ('O peer queue receive: control alternative',['peer-queue-receive-control']*4),
 ('O peer queue receive: data alternative',['peer-queue-receive-data']*4),
 ('I inbound peer → inbox sends (same inbox receive sites above)',['peer-inbox-send']*4),
 ('U successful Unreachable → inbox sends (same receive alternatives)',['unreachable-send']*4),
 ('Sw successful writer wake sends',['writer-wake-send']*4),
 ('Rw selected idle writer receives',['writer-wake-receive']*4),
 ('DATA credit wake ATTEMPT, not guaranteed send',['credit-wake-attempt']*4),
 ('Completion wake ATTEMPT, not guaranteed send',['completion-wake-attempt']*4),
 ('Unary body delivery returns before streaming signal',['unary-delivery']*4),
 ('Unary end nil-channel ATTEMPT: no send',['unary-nil-signal']*4),
 ('Unary writer space nil-channel ATTEMPT: no send',['unary-nil-space']*4),
 ('Retained Do reply-channel creation NOT this path',['do-reply-create']*4),
 ('Node shutdown close, NOT normal request',['shutdown-close']*4),
 ('Watch overflow close, NOT no-watch request',['watch-close']*4),
 ('Server Stop close, NOT normal request',['server-stop-close']*4),
])
cas=table('### M/T CAS sites: both compare outcomes follow the full request write path',['main','txn'],[
 ('Frontend fresh protobuf request graph',['frontend-new-op']*2),
 ('Frontend owning vs alias decoder',['frontend-decode','alias-frontend']),
 ('Classify both branches, before compare',['txn-readonly']*2),
 ('Success Put makes write (not read-only)',['txn-write-detect']*2),
 ('Write/read routing',['txn-write-route']*2),
 ('Entry entire-Txn input-value copy',['generic-entry-value-encode','txn-entry-copy']),
 ('Apply owning vs alias decode',['generic-apply-still-exists','alias-apply']),
 ('State → MVCC Txn',['state-txn']*2),
 ('Compute compare path',['compare-path']*2),
 ('Existing key → KeyValue slice-header alias',['compare-kv']*2),
 ('MOD int64 comparison, zero stored-value copy',['compare-mod']*2),
 ('Executed Success Put only',['txn-put']*2),
 ('Executed Failure Range only',['txn-range']*2),
 ('Range builds KeyValue objects, no value bytes copy',['range-kv']*2),
 ('KeyValue.Value retained header: no bytes copy',['kv-value-alias']*2),
 ('Response succeeded flag',['txn-success']*2),
 ('MVCC revision depends on actual mutation',['txn-revision']*2),
 ('CAS generic response-encode caller',['batch-reply-encode']*2),
 ('One full returned-stored-value response encode',['response-proto-encode']*2),
 ('Fresh apply Txn struct remains in both',['new-txn-op']*2),
 ('Alias repeated submessage reconstruction',[None,'alias-nested-list']),
 ('Alias singular/oneof submessage reconstruction',[None,'alias-nested-message']),
 ('Alias bytes slice + cap clip',[None,'alias-value']),
 ('Alias field setter',[None,'alias-set']),
])
submission=table('### Submission and owner fields: zero additional channel operations',list(ts),[
 ('Connection opBatch.reqs descriptor storage',['opbatch-owner']*4),
 ('Reader → Node.SubmitBatch direct call (actual A send is separately listed)',['batch-submit-call']*4),
 ('Node batch.reqs descriptor storage',['driver-batch-owner']*4),
 ('Batch wrapper retains request/tag arrays',['batch-wrapper']*4),
 ('Wrapper → inbox admission via put (not a second send)',['batch-inbox-call']*4),
 ('waiter.to / waiter.tag metadata; no reply channel',['batch-waiter']*4),
 ('Core Submit vs SubmitAll: zero sends/receives/closes',['batch-core-submit']*4),
 ('H/T batch-owned pending array; zero channel operations',[None,None,'batch-pending-allocation','batch-pending-allocation']),
 ('H/T SubmitAll direct per-record calls',[None,None,'submitall-loop','submitall-loop']),
 ('pending.req retains descriptors, no value copies',['pending-owner']*4),
 ('PR80 Request.Wire new alias field',[None,None,None,'request-wire-field']),
])
binding=('**FINAL measurement binding — requester-confirmed; branch heads are NOT substituted.**' if final else '**PRELIMINARY ONLY — awaiting requester final resolved measurement SHAs. Do not present this as the benchmark binding.**')+'\n\n| Label | Role | Exact audited commit | Source directory |\n|---|---|---|---|\n'
for t,sha in ts.items():binding+=f"| {labels[t]} | {t} | `{sha}` | `sources/{t}/` |\n"
if final and (R/'final-binding.md').exists():binding+='\n'+(R/'final-binding.md').read_text()
text=(R/'report-template.md').read_text().replace('{{binding}}',binding).replace('{{put-sites}}',put).replace('{{channel-sites}}',channel).replace('{{cas-sites}}',cas).replace('{{submission-sites}}',submission)
text=re.sub(r'\{\{dep:([^}]+)\}\}',lambda m:dep(m.group(1)),text)
text=re.sub(r'\{\{site:([^:}]+):([^}]+)\}\}',lambda m:site(m.group(1),m.group(2)),text);assert '{{' not in text
(R/'audit.md').write_text(text)
# Dependency proof: exact site identities plus recomputed hashes, not speculative generic semantics.
proof=['# Dependency proof — immutable source binding','',binding,'','All target go.mod files directly pin the versions below; no target replace/exclude/toolchain override is present. The relevant copied dependency go.mod cross-pins do not select a higher version of these cited dependencies. This verifies source pins, not the measured binary: build/module identities belong in runtime evidence. Module ZIP h1 is recomputed without Go as SHA256 of sorted SHA256/name lines (Go x/mod dirhash.Hash1). Each copied evidence file equals its module ZIP member and cache source; every h1 matches every target go.sum and cache .ziphash. See dependency-integrity.json and dependency-source-manifest.json.','', '| Module | Recomputed ZIP h1 |','|---|---|']
for x in json.loads((R/'dependency-integrity.json').read_text()):proof.append(f"| `{x['module']}` | `{x['computed_h1']}` |")
proof+=['','## Exact owning encode/decode, Raft clone and alias reflection sites','| Site | Module-bound file:line | Exact text |','|---|---|---|']
ids=['lib-put-value-field','lib-returned-value-field','lib-proto3-codec-selection','lib-proto3-decode-copy','proto3-bytes-encoder','lib-bytes-encode-copy','lib-bytes-consume-alias','lib-raft-propose-alias','lib-raft-propose-clone-call','lib-raft-clone','lib-entry-proto2','lib-entry-data-field','lib-proto2-codec-selection','lib-proto-clone-merge','lib-proto2-clone-copy','lib-proto2-decode-copy','lib-storage-pointer-append1','lib-storage-pointer-append2','lib-peer-construction-alias','follower-maybeappend','follower-logappend','unstable-append','async-append-alias','async-apply-alias','reflect-set','reflect-scalar-set','reflect-bytes-convert']
for id in ids:
 x=next(x for x in rows if x['target']=='main' and x['id']==id)
 proof.append(f"| {id} | `{dep(id)}` | <code>{x['expected'].replace('<','&lt;').replace('>','&gt;').replace('|','&#124;')}</code> |")
proof+=['','**Critical distinction:** proto2 Entry.Data deep clone uses mergeBytes append(emptyBuf[:], src...). Proto3 Put.Value owning decode uses append(nil, v...). Peer Entry.Data owning decode uses proto2 consumeBytes. AppendBytes copies to a destination; ConsumeBytes returns a subslice. A call plus its dependency instruction is one materialization. None can be substituted for another just because they operate on []byte.','', '**PR80 reflection proof:** repository scalar produces ValueOfBytes(slices.Clip(x)), Set dispatches fi.set, the scalar setter uses rv.Set(conv.GoValueOf(v)), and bytesConverter returns reflect.ValueOf(v.Bytes()).Convert(c.goType). Put.Value is native []byte, so conversion/set preserves its backing store. Mutable and AppendMutable still allocate/build submessage objects; zero value copies is not zero struct construction. The caller-owned input must not change. String/unknown-field materializations are conditional and outside canonical bytes-value scope.','', '**Follower ownership:** MsgApp uses maybeAppend and append of []*Entry references rather than leader proposal appendEntry clone. Peer receiver owns its proto2-decoded Entry.Data. The WAL stages it separately; storage/index alias that receiver-owned entry (for wire/alias apply). Leader successful wire/alias apply aliases its Raft-deep-cloned entry, not frontend or WAL staging bytes.','']
(R/'dependency-proof.md').write_text('\n'.join(proof)+'\n')
# Resolve and record ancestry read-only. Exact hashes are independent of live branch names.
ancestry=[]
for a,b in [('main','wire'),('wire','hpack'),('hpack','txn')]:
 p=subprocess.run(['git','merge-base','--is-ancestor',ts[a],ts[b]],cwd=R/'object-repo')
 ancestry.append({'ancestor':a,'descendant':b,'ancestor_sha':ts[a],'descendant_sha':ts[b],'verified':p.returncode==0})
(R/'ancestry.json').write_text(json.dumps(ancestry,indent=2)+'\n')
trees={t:subprocess.check_output(['git','rev-parse',sha+'^{tree}'],cwd=R/'object-repo').decode().strip() for t,sha in ts.items()}
assert trees['main']==trees['wire'], 'same-code-control claim failed; re-audit required'
assert subprocess.check_output(['git','diff',ts['main'],ts['wire']],cwd=R/'object-repo')==b''
(R/'tree-control.json').write_text(json.dumps({'targets':ts,'trees':trees,'main_wire_complete_tree_equal':True,'main_wire_git_diff_empty':True},indent=2)+'\n')
print(('FINAL' if final else 'PRELIMINARY'),'audit.md and dependency-proof.md written;',len(rows),'verified sites')
