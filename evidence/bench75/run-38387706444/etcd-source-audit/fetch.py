import pathlib,urllib.request,zipfile,concurrent.futures,json,hashlib,base64
ROOT=pathlib.Path('/home/joshazimullah.linux/work_mounts/scratch/bench7980-etcd-audit')
mods={'google.golang.org/protobuf':'v1.36.11','google.golang.org/grpc':'v1.83.2','go.etcd.io/raft/v3':'v3.7.0','go.etcd.io/bbolt':'v1.5.0','go.etcd.io/etcd/api/v3':'v3.7.2','github.com/golang/protobuf':'v1.5.4'}
def fetch(kv):
 m,v=kv;u=f'https://proxy.golang.org/{m}/@v/{v}.zip'; b=urllib.request.urlopen(u,timeout=60).read();name=m.replace('/','_')+'@'+v;(ROOT/'archives').mkdir(exist_ok=True);(ROOT/'archives'/(name+'.zip')).write_bytes(b)
 import io
 z=zipfile.ZipFile(io.BytesIO(b)); hh=hashlib.sha256()
 for n in sorted(z.namelist()):
  if n.endswith('/'):continue
  data=z.read(n);hh.update((hashlib.sha256(data).hexdigest()+'  '+n+'\n').encode()); p=ROOT/'deps'/name/pathlib.Path(n).relative_to(m+'@'+v);p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
 h='h1:'+base64.b64encode(hh.digest()).decode();sums=(ROOT/'source/server/go.sum').read_text();ap=(ROOT/'source/api/go.sum').read_text();return dict(module=m,version=v,url=u,zip_sha256=hashlib.sha256(b).hexdigest(),h1=h,server_go_sum_match=f'{m} {v} {h}' in sums,api_go_sum_match=f'{m} {v} {h}' in ap)
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:r=list(ex.map(fetch,mods.items()))
(ROOT/'dependency-integrity.json').write_text(json.dumps(r,indent=2)+'\n')
u='https://api.github.com/repos/etcd-io/etcd/git/tags/cb9b1aa2b1237b59255d65773c34ab0cd9df3679';(ROOT/'release-tag-object.json').write_bytes(urllib.request.urlopen(u).read())
print(json.dumps(r,indent=2))
