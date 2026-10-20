#!/usr/bin/env python3
"""Fetch only small ZIP members using HTTP ranges (host disk is nearly full).
No Go work. Does not download/extract the multi-GB checksum logs or histories.
"""
import io, json, subprocess, sys, urllib.request, urllib.error, zipfile
from pathlib import Path
artifact, dest = sys.argv[1:3]
base = 'https://api.github.com/repos/djosh34/tigerstore/actions/artifacts/' + artifact
meta = json.loads(subprocess.check_output(['gh', 'api', 'repos/djosh34/tigerstore/actions/artifacts/'+artifact]))
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self, *args): return None
req = urllib.request.Request(base+'/zip', headers={'Authorization':'Bearer '+subprocess.check_output(['gh','auth','token'],text=True).strip(),'Accept':'application/vnd.github+json'})
try:
 urllib.request.build_opener(NoRedirect()).open(req)
 raise RuntimeError('Expected redirect')
except urllib.error.HTTPError as e:
 assert e.code == 302, e
 url = e.headers['Location']
class RangeFile(io.RawIOBase):
 def __init__(self): self.pos=0
 def seekable(self): return True
 def readable(self): return True
 def tell(self): return self.pos
 def seek(self, offset, whence=0):
  self.pos = offset if whence==0 else self.pos+offset if whence==1 else meta['size_in_bytes']+offset
  return self.pos
 def read(self, count=-1):
  if count < 0: count=meta['size_in_bytes']-self.pos
  if count == 0: return b''
  end=min(meta['size_in_bytes'],self.pos+count)-1
  request=urllib.request.Request(url,headers={'Range':f'bytes={self.pos}-{end}'})
  with urllib.request.urlopen(request) as response:
   assert response.status == 206, response.status
   data=response.read(count+1)
  assert len(data)<=count
  self.pos+=len(data)
  return data
out=Path(dest);out.mkdir(parents=True,exist_ok=True)
manifest=[]
with zipfile.ZipFile(RangeFile()) as z:
 for info in z.infolist():
  manifest.append(f'{info.file_size}\t{info.compress_size}\t{info.filename}')
  if info.is_dir() or info.file_size>200_000: continue
  p=out/info.filename
  assert p.resolve().is_relative_to(out.resolve())
  p.parent.mkdir(parents=True,exist_ok=True)
  p.write_bytes(z.read(info))
(out/'artifact-members.tsv').write_text('\n'.join(manifest)+'\n')
print('\n'.join(manifest))
