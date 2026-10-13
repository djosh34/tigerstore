#!/usr/bin/env python3
"""Verify archives and extract runner artifacts. Requires Python 3.12+ and zstd."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile

ap=argparse.ArgumentParser()
ap.add_argument('destination',type=Path)
ap.add_argument('--run',type=int,action='append',help='select a run; repeat as needed')
args=ap.parse_args()
source=Path(__file__).parent/'evidence'
records=json.loads((source/'archives.json').read_text())
args.destination.mkdir(parents=True,exist_ok=True)
for record in records:
    if args.run and record['run_id'] not in args.run:continue
    with tempfile.TemporaryFile() as combined:
        full=hashlib.sha256()
        for part in record['parts']:
            h=hashlib.sha256()
            with (source/part['file']).open('rb') as f:
                while data:=f.read(1<<20):
                    h.update(data);full.update(data);combined.write(data)
            if h.hexdigest()!=part['sha256']:raise ValueError(f'bad checksum: {part["file"]}')
        if full.hexdigest()!=record['combined_sha256']:raise ValueError('combined checksum mismatch')
        combined.seek(0)
        decoder=subprocess.Popen(['zstd','-q','-d','-c','--long=27'],stdin=combined,stdout=subprocess.PIPE)
        try:
            with tarfile.open(fileobj=decoder.stdout,mode='r|') as archive:
                archive.extractall(args.destination,filter='data')
        finally:
            decoder.stdout.close()
            code=decoder.wait()
        if code:raise RuntimeError(f'zstd exited {code}')
    (args.destination/record['folder']/'.downloaded').touch()
    print('verified and extracted',record['run_id'])
