#!/usr/bin/env python3
"""Source-only preparation. All writes confined to ROOT; never invokes Go."""
from pathlib import Path
import json,hashlib,subprocess,datetime,shutil,difflib,base64,zipfile,re
ROOT=Path('/home/joshazimullah.linux/work_mounts/scratch/bench7980-source-audit')
REPO=Path('/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75')
PRIOR=REPO/'evidence/bench75/run-38372738426/source-audit'
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
targets=json.loads((ROOT/('final-targets.json' if (ROOT/'final-targets.json').exists() else 'preliminary-targets.json')).read_text())
manifest=json.loads((PRIOR/'dependency-source-manifest.json').read_text())
for rel in ['internal/impl/convert.go','internal/impl/message_reflect_field.go','internal/impl/message_reflect_gen.go','reflect/protoreflect/value.go','internal/impl/convert_list.go']:
 module='google.golang.org/protobuf@v1.36.11'; src=Path('/home/joshazimullah.linux/go/pkg/mod')/module/rel
 manifest.append(dict(module=module,cache_source=str(src),evidence='dependency-sources/protobuf-v1.36.11/'+rel,sha256=hashlib.sha256(src.read_bytes()).hexdigest()))
module='go.etcd.io/etcd/api/v3@v3.7.2';src=Path('/home/joshazimullah.linux/go/pkg/mod')/module/'mvccpb/kv.pb.go'
manifest.append(dict(module=module,cache_source=str(src),evidence='dependency-sources/etcd-api-v3.7.2/mvccpb/kv.pb.go',sha256=hashlib.sha256(src.read_bytes()).hexdigest()))
for row in manifest:
 dest=ROOT/row['evidence'];dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(row['cache_source'],dest)
(ROOT/'dependency-source-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
integrity=[]
for module in sorted({r['module'] for r in manifest}):
 name,version=module.split('@');cache=Path('/home/joshazimullah.linux/go/pkg/mod/cache/download')/name/'@v';zp=cache/(version+'.zip')
 with zipfile.ZipFile(zp) as z:
  lines=''.join(hashlib.sha256(z.read(n)).hexdigest()+'  '+n+'\n' for n in sorted(z.namelist()))
  h1='h1:'+base64.b64encode(hashlib.sha256(lines.encode()).digest()).decode()
  assert h1==(cache/(version+'.ziphash')).read_text().strip()
  for label,sha in targets.items():
   sums=(ROOT/'sources'/label/'go.sum').read_text().splitlines();assert [s.split()[2] for s in sums if s.startswith(name+' '+version+' ')]==[h1]
   mod=(ROOT/'sources'/label/'go.mod').read_text()
   assert not re.search(r'^\s*(replace|exclude|toolchain)\b',mod,re.M)
   assert re.search(r'^\s*'+re.escape(name)+r'\s+'+re.escape(version)+r'(\s|$)',mod,re.M),(label,module,'not directly pinned')
  for row in [r for r in manifest if r['module']==module]:
   rel=str(Path(row['cache_source']).relative_to(Path('/home/joshazimullah.linux/go/pkg/mod')/module))
   assert z.read(module+'/'+rel)==(ROOT/row['evidence']).read_bytes()==Path(row['cache_source']).read_bytes()
 integrity.append(dict(module=module,computed_h1=h1,targets=targets))
(ROOT/'dependency-integrity.json').write_text(json.dumps(integrity,indent=2)+'\n')
# Relocate exact prior sites by diff context; never carry forward a line number blindly.
old=json.loads((PRIOR/'site-specs.json').read_text()); rows=[]; changes=[]
old_sha={'batch':'29c3f8f95e3c9bcecf5bb39aa1f3bca49fe99c1f','wire':'f2edb7b2b1e81f65d7ca7592638654f09217b933'}
for target in targets:
 base='wire' # Current main includes PR77; old batch main is provenance only.
 for spec in [r for r in old if r['target']==base]:
  file=spec['file'];expected=spec['expected'];path=ROOT/file if file.startswith('dependency-sources/') else ROOT/'sources'/target/file
  if not path.exists():continue
  new=path.read_text().splitlines();candidates=[i for i,s in enumerate(new) if s.strip()==expected]
  if not candidates:
   changes.append(dict(target=target,id=spec['id'],file=file,status='removed or changed'));continue
  if len(candidates)==1:i=candidates[0]
  elif file.startswith('dependency-sources/'):i=spec['line']-1;assert i in candidates
  else:
   orig=subprocess.check_output(['git','show',old_sha[base]+':'+file],cwd=ROOT/'object-repo').decode().splitlines()
   mapped={a+k:b+k for a,b,n in difflib.SequenceMatcher(None,orig,new,autojunk=False).get_matching_blocks() for k in range(n)}
   if spec['line']-1 not in mapped:
    changes.append(dict(target=target,id=spec['id'],file=file,status='ambiguous; manual replacement required',candidates=[x+1 for x in candidates]));continue
   i=mapped[spec['line']-1];assert i in candidates
  # Replace all old-target-specific labels with site semantics in new report.
  rows.append(dict(id=spec['id'],target=target,file=file,line=i+1,expected=expected,classification=spec['classification']))
(ROOT/'relocated-sites.json').write_text(json.dumps(rows,indent=2)+'\n')
(ROOT/'relocation-changes.json').write_text(json.dumps(changes,indent=2)+'\n')
print('Pins and ZIP hashes verified;',len(rows),'prior exact sites relocated;',len(changes),'changed sites require review')
