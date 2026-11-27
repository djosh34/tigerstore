#!/usr/bin/env python3
"""Same-VM main/candidate component profiles on the failed /174 WAL. Off-gate."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import zipfile

REFS=[('main','55b32bb9e6ef692cdb9d1b0e3cdb16542811dec4'),('candidate','2f61154a8a95a8ce56b71e8bbd02775ce4c1ea2f')]

def main():
 p=argparse.ArgumentParser();p.add_argument('--out',type=Path,required=True);a=p.parse_args()
 root=Path.cwd();out=a.out.resolve();out.mkdir(parents=True)
 raw=subprocess.check_output(['gh','api','repos/djosh34/tigerstore/actions/artifacts/15221033025/zip'])
 saved=out/'saved-wal';saved.mkdir()
 hashes={}
 with zipfile.ZipFile(io.BytesIO(raw)) as z:
  for info in z.infolist():
   if '/seed-174/data/node1/wal/' not in info.filename or info.is_dir():continue
   name=Path(info.filename).name;data=z.read(info);(saved/name).write_bytes(data);hashes[name]=hashlib.sha256(data).hexdigest()
 if not any(k.endswith('.seg') for k in hashes):raise RuntimeError('original failed node1 WAL absent')
 (out/'input-sha256.json').write_text(json.dumps(hashes,indent=2)+'\n')
 (out/'diagnostic.json').write_text(json.dumps(dict(diagnostic_only=True,not_a_rolling_reproduction=True,input_artifact=15221033025,input_run=46043002627,refs=REFS,tooling_ref=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()),indent=2)+'\n')
 source_hashes={}
 for label,ref in REFS:
  checkout=out/f'source-{label}';dest=out/label;dest.mkdir()
  subprocess.run(['git','clone','--shared',str(root),str(checkout)],check=True)
  subprocess.run(['git','checkout','--detach',ref],cwd=checkout,check=True)
  text=(checkout/'internal/wl/wal.go').read_text();start=text.index('func scanWAL(');end=text.index('\n}\n',start)+3
  scan=text[start:end];(dest/'scanWAL.go.txt').write_text(scan);source_hashes[label]=hashlib.sha256(scan.encode()).hexdigest()
  (checkout/'internal/wl/saved_wal_diagnostic_test.go').write_bytes((root/'bench/fetch-measure/saved_wal_scan_test.go.txt').read_bytes())
  subprocess.run(['gofmt','-w',str(checkout/'internal/wl/saved_wal_diagnostic_test.go')],check=True)
  env=dict(os.environ,SAVED_WAL_DIR=str(saved),SCAN_OUT=str(dest),SCAN_REF=ref,GOMAXPROCS='3')
  with (dest/'result.log').open('w') as log:
   subprocess.run(['go','test','-race','-count=1','-v','./internal/wl','-run','^TestDiagnosticSavedWALScan$','-timeout','8m'],cwd=checkout,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
  for mode in ('cold','warm'):
   with (dest/f'{mode}-top.txt').open('w') as f:
    subprocess.run(['go','tool','pprof','-top',str(dest/f'{mode}.pprof')],stdout=f,stderr=subprocess.STDOUT,check=True)
  subprocess.run(['rm','-rf',str(checkout)],check=True)
 (out/'scanner-source-sha256.json').write_text(json.dumps(source_hashes,indent=2)+'\n')
 subprocess.run(['rm','-rf',str(saved)],check=True)

if __name__=='__main__':main()
