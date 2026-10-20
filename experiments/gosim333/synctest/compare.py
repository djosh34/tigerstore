#!/usr/bin/env python3
from pathlib import Path
import hashlib, sys
root = Path(sys.argv[1])
for a, b in [('seed-1a','seed-1b'),('seed-1a','seed-2')]:
 aa = sorted((root/a).rglob('*'))
 files = [p.relative_to(root/a) for p in aa if p.is_file()]
 print(a,b,'files',list(map(str,files)))
 for f in files:
  x,y=(root/a/f).read_bytes(), (root/b/f).read_bytes() if (root/b/f).exists() else b''
  print(str(f),'bytes',len(x),len(y),'equal',x==y,'sha256',hashlib.sha256(x).hexdigest(),hashlib.sha256(y).hexdigest())
for n in ['1a','1b','2']:
 data = b''.join(line for line in (root/f'events-{n}.log').read_bytes().splitlines(keepends=True) if b'"msg":"checksummer"' in line)
 (root/f'events-{n}.jsonl').write_bytes(data)
 print('events',n,'bytes',len(data),'sha256',hashlib.sha256(data).hexdigest())
a=(root/'events-1a.jsonl').read_bytes()
print('events same seed byte-identical',a==(root/'events-1b.jsonl').read_bytes())
print('events seed 2 differs',a!=(root/'events-2.jsonl').read_bytes())
