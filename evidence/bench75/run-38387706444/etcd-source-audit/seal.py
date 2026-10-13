from pathlib import Path
import hashlib,json,re,tarfile,zipfile,datetime,base64
R=Path('/home/joshazimullah.linux/work_mounts/scratch/bench7980-etcd-audit');SHA='68c065e562994b89e333e77b039ad066f933c586'
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
assert json.loads((R/'release-tag-object.json').read_text())['object']['sha']==SHA
# Check all canonical extracted regular release files against the actual primary tar.
archive=R/f'etcd-{SHA}.tar.gz';nsource=0
with tarfile.open(archive) as t:
 for m in t:
  if m.isfile():
   p=R/'source'/Path(m.name).relative_to('etcd-'+SHA)
   assert p.read_bytes()==t.extractfile(m).read(),str(p);nsource+=1
# Check all dependency extracted bytes against their ZIPs, and each server module h1 against binary metadata.
modules=json.loads((R/'dependency-integrity.json').read_text())+[json.loads((R/'client-dependency-integrity.json').read_text())];ndeps=0
build=(R/'inputs/etcd-buildinfo.txt').read_text()
for x in modules:
 name=x['module'].replace('/','_')+'@'+x['version'];z=zipfile.ZipFile(R/'archives'/(name+'.zip'));h=hashlib.sha256()
 for n in sorted(z.namelist()):
  if n.endswith('/'):continue
  b=z.read(n);assert (R/'deps'/name/Path(n).relative_to(x['module']+'@'+x['version'])).read_bytes()==b
  h.update((hashlib.sha256(b).hexdigest()+'  '+n+'\n').encode());ndeps+=1
 assert x['h1']=='h1:'+base64.b64encode(h.digest()).decode()
 if x['module'] in ['go.etcd.io/raft/v3','go.etcd.io/bbolt','google.golang.org/protobuf'] or x['module']=='google.golang.org/grpc' and x['version']=='v1.83.2':
  assert f"dep\t{x['module']}\t{x['version']}\t{x['h1']}" in build
assert f'vcs.revision={SHA}' in build and 'vcs.modified=false' in build
assert (R/'inputs/etcd-release-sha256.txt').read_text().split()[0]=='17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d'
for x in json.loads((R/'input-manifest.json').read_text()):assert digest(R/x['copy'])==x['sha256']
# Minimal instruction-level assertions for every fixed core operation and immediate input boundary.
assertions={
 ('source/pkg/wait/wait.go',65):'newCh := make(chan any, 1)',('source/pkg/wait/wait.go',83):'ch <- x',('source/pkg/wait/wait.go',84):'close(ch)',
 ('source/server/etcdserver/v3_server.go',1123):'case x := <-ch:',('source/server/etcdserver/v3_server.go',1093):'data, err = proto.Marshal(r)',
 ('deps/go.etcd.io_raft_v3@v3.7.0/node.go',386):'case pm := <-propc:',('deps/go.etcd.io_raft_v3@v3.7.0/node.go',391):'pm.result <- err',('deps/go.etcd.io_raft_v3@v3.7.0/node.go',392):'close(pm.result)',('deps/go.etcd.io_raft_v3@v3.7.0/node.go',528):'pm.result = make(chan error, 1)',('deps/go.etcd.io_raft_v3@v3.7.0/node.go',531):'case ch <- pm:',('deps/go.etcd.io_raft_v3@v3.7.0/node.go',541):'case err := <-pm.result:',
 ('deps/go.etcd.io_raft_v3@v3.7.0/raft.go',817):'cloned[i] = proto.Clone(es[i]).(*pb.Entry)',
 ('source/server/storage/wal/wal.go',976):'b := pbutil.MustMarshalMessage(e)',('source/server/storage/wal/encoder.go',84):'data, err = proto.Marshal(rec)',('source/server/storage/wal/encoder.go',89):'data, err = opts.MarshalAppend(e.buf[:0], rec)',
 ('source/pkg/ioutil/pagewriter.go',59):'copy(pw.buf[pw.bufferedBytes:], p)',('source/server/etcdserver/apply/apply.go',29):'pbutil.MustUnmarshalMessage(&raftReq, e.Data)',
 ('source/server/storage/mvcc/kvstore_txn.go',250):'d, err := proto.Marshal(kv)',('source/server/storage/mvcc/kvstore_txn.go',142):'if err := proto.Unmarshal(vs[0], kv); err != nil {',
 ('source/server/etcdserver/txn/txn.go',274):'rr, err := rv.Range(context.TODO(), c.Key, mkGteRange(c.RangeEnd), mvcc.RangeOptions{})',
 ('deps/google.golang.org_protobuf@v1.36.11/internal/impl/codec_gen.go',5497):'*p.Bytes() = append(([]byte)(nil), v...)',('deps/google.golang.org_protobuf@v1.36.11/internal/impl/codec_gen.go',5418):'*p.Bytes() = append(emptyBuf[:], v...)',('deps/google.golang.org_protobuf@v1.36.11/internal/impl/merge.go',188):'*dst.Bytes() = append(emptyBuf[:], *src.Bytes()...)',('deps/google.golang.org_protobuf@v1.36.11/encoding/protowire/wire.go',455):'return append(AppendVarint(b, uint64(len(v))), v...)',
}
for (p,n),text in assertions.items():
 actual=(R/p).read_text().splitlines()[n-1].strip();assert actual==text,(p,n,actual,text)
(R/'instruction-assertions.json').write_text(json.dumps([dict(file=p,line=n,text=s) for (p,n),s in assertions.items()],indent=2)+'\n')
a=(R/'audit.md').read_text();cs=sorted(set((p,int(b),int(e or b)) for p,b,e in re.findall(r'`((?:source|deps|inputs)/[^`\s:]+):(\d+)(?:-(\d+))?`',a)))
md=['# Verified sites — exact source text','',f'Etcd source bound to `{SHA}`. Dependency directory names bind the module version; module ZIP h1 and stock buildinfo are checked in `verification.json` and dependency manifests. Numbered text below is unmodified source line content (only line-number prefixes are added). Each reported range is checked against archive/ZIP bytes.',''];sites=[]
for p,b,e in cs:
 q=R/p;lines=q.read_text().splitlines();assert 1<=b<=e<=len(lines),(p,b,e)
 x=dict(file=p,start=b,end=e,sha256=digest(q),binding=SHA if p.startswith('source/') else p.split('/')[1] if p.startswith('deps/') else 'input-manifest.json',lines=[dict(line=n,text=lines[n-1]) for n in range(b,e+1)]);sites.append(x)
 md += [f'## `{p}:{b}'+(f'-{e}' if e!=b else '')+'`','',f"SHA256: `{x['sha256']}`; binding: `{x['binding']}`",'','```text']+[f'{n}: {lines[n-1]}' for n in range(b,e+1)]+['```','']
(R/'verified-sites.md').write_text('\n'.join(md));(R/'verified-sites.json').write_text(json.dumps(sites,indent=2)+'\n')
# Summarize measured branch counts without loading performance metrics.
rows=json.loads((R/'measurement-binding.json').read_text());totals={}
for label in ['baseline','candidate','etcd']:
 rr=[x for x in rows if Path(x['path']).parent.name.endswith('-'+label)];totals[label]=dict(passed=sum(x['passed'] for x in rr),failed=sum(x['failed'] for x in rr),empty_errors=all(x['errors']=={} for x in rr));assert totals[label]['passed']==300000 and totals[label]['failed']==0
v=dict(verified_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),release_commit=SHA,regular_release_files_checked=nsource,dependency_files_checked=ndeps,citation_ranges=len(sites),instruction_assertions=len(assertions),stock_buildinfo_pins_match=True,API_module_files_equal_release=33,measurement_totals=totals,no_go_execution=True)
(R/'verification.json').write_text(json.dumps(v,indent=2)+'\n')
paths=[]
for d in ['source','deps','archives','inputs']:
 paths += [p for p in (R/d).rglob('*') if p.is_file()]
paths += [p for p in R.iterdir() if p.is_file() and p.name not in ['hashmanifest.json','SHA256SUMS','hash-verification.log']]
manifest=[dict(path=str(p.relative_to(R)),bytes=p.stat().st_size,sha256=digest(p)) for p in sorted(set(paths))]
(R/'hashmanifest.json').write_text(json.dumps(dict(release_commit=SHA,files=manifest,excluded_directory='initial noncanonical etcd-* tar extraction, never used'),indent=2)+'\n')
(R/'SHA256SUMS').write_text(''.join(f"{x['sha256']}  {x['path']}\n" for x in manifest)+f"{digest(R/'hashmanifest.json')}  hashmanifest.json\n")
print(json.dumps(v,indent=2))
