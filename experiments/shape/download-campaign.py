#!/usr/bin/env python3
"""Download complete Actions artifacts into distinct, resumable run folders."""
import argparse
import json
from pathlib import Path
import subprocess

ap=argparse.ArgumentParser()
ap.add_argument('root',type=Path)
ap.add_argument('--commit')
args=ap.parse_args()
manifest=json.loads(Path(__file__).with_name('campaign.json').read_text())
for r in manifest['runs']:
    if args.commit and not r['commit'].startswith(args.commit): continue
    dst=args.root/r['folder']
    if (dst/'.downloaded').exists(): continue
    assert r['status']=='completed', r
    print('download',r['id'],dst,flush=True)
    cp=subprocess.run(['gh','run','download',str(r['id']),'-R','djosh34/tigerstore','-D',str(dst)])
    if cp.returncode:raise SystemExit(cp.returncode)
    (dst/'.downloaded').touch()
print('done',flush=True)
