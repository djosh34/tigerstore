#!/usr/bin/env python3
"""Compare every archived file with the original downloaded runner artifacts."""
import datetime
import hashlib
import json
from pathlib import Path
import sys

here=Path(__file__).parent
original,extracted=map(Path,sys.argv[1:3])
records=json.loads((here/'evidence/archives.json').read_text())
excluded={'.downloaded','windows.csv','summary.json','tables.md','summary-errors.json',
          'summary-stdout.txt','summary-stderr.txt'}

def digest(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        while data:=f.read(1<<20):h.update(data)
    return h.hexdigest()

whole=hashlib.sha256();count=0;size=0
for r in records:
    folder=Path(r['folder'])
    expected={p.relative_to(original) for p in (original/folder).rglob('*') if p.is_file() and p.name not in excluded}
    actual={p.relative_to(extracted) for p in (extracted/folder).rglob('*') if p.is_file() and p.name not in excluded}
    assert expected==actual,(r['run_id'],'file set differs',expected-actual,actual-expected)
    for relative in sorted(expected):
        a,b=original/relative,extracted/relative
        h=digest(a)
        assert a.stat().st_size==b.stat().st_size and h==digest(b), relative
        whole.update(str(relative).encode()+b'\0'+h.encode()+b'\n')
        count+=1;size+=a.stat().st_size
    # A restored archive is a complete local input, including retained failures.
    (extracted/folder/'.downloaded').touch()
    print('content matches',r['run_id'],len(expected),flush=True)
result={'verified_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'archives':len(records),'files':count,'uncompressed_file_bytes':size,
        'path_and_content_sha256':whole.hexdigest(),'all_files_match_original_downloads':True}
(here/'evidence/verification.json').write_text(json.dumps(result,indent=2)+'\n')
print(result,flush=True)
