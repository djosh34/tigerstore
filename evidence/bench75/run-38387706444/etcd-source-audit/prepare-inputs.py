from pathlib import Path
import shutil,zipfile,urllib.request,hashlib,base64,json
R=Path('/home/joshazimullah.linux/work_mounts/scratch/bench7980-etcd-audit');repo=Path('/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75');ev=repo/'evidence/bench75/run-38387706444'; files=[(repo/'experiments/bench75/load/main.go','inputs/workload/main.go'),(repo/'experiments/bench75/load/cas.go','inputs/workload/cas.go'),(ev/'cas-c256/versions.txt','inputs/versions.txt'),(Path('/home/joshazimullah.linux/work_mounts/scratch/bench7980-source-audit/audit.md'),'inputs/tigerstore-audit.md')]
files += [(p,'inputs/results/'+p.parent.name+'/cas.json') for p in sorted((ev/'cas-c256').glob('*/cas.json'))]
manifest=[]
for p,q in files:
 t=R/q;t.parent.mkdir(parents=True,exist_ok=True);t.write_bytes(p.read_bytes());manifest.append(dict(original=str(p),copy=q,sha256=hashlib.sha256(t.read_bytes()).hexdigest()))
(R/'input-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
m='google.golang.org/grpc';v='v1.84.0';u=f'https://proxy.golang.org/{m}/@v/{v}.zip';b=urllib.request.urlopen(u).read();name=m.replace('/','_')+'@'+v;(R/'archives'/(name+'.zip')).write_bytes(b)
import io
z=zipfile.ZipFile(io.BytesIO(b));h=hashlib.sha256()
for n in sorted(z.namelist()):
 if n.endswith('/'):continue
 d=z.read(n);h.update((hashlib.sha256(d).hexdigest()+'  '+n+'\n').encode());p=R/'deps'/name/Path(n).relative_to(m+'@'+v);p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(d)
h1='h1:'+base64.b64encode(h.digest()).decode();assert f'{m}\tv1.84.0\t{h1}' in (R/'inputs/versions.txt').read_text()
(R/'client-dependency-integrity.json').write_text(json.dumps(dict(module=m,version=v,url=u,h1=h1,zip_sha256=hashlib.sha256(b).hexdigest(),versions_match=True),indent=2)+'\n')
shutil.rmtree(R/'etcd-68c065e562994b89e333e77b039ad066f933c586')
print('inputs sealed; client grpc pin verified')
