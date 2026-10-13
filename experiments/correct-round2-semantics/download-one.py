#!/usr/bin/env python3
"""For old runs without compact artifacts: one ZIP at a time, never extract WALs."""
import argparse
import importlib.util
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import zipfile

p=argparse.ArgumentParser();p.add_argument('artifact_id');p.add_argument('artifact_name');p.add_argument('destination',type=Path)
a=p.parse_args()
# All temporary files stay in this worktree. No full disks are extracted.
with tempfile.TemporaryDirectory(prefix='one-artifact-',dir=Path.cwd()) as temp:
    root=Path(temp);z=root/'artifact.zip';source=root/'source';artifact=source/a.artifact_name
    with z.open('wb') as stream:
        subprocess.run(['gh','api',f'repos/djosh34/tigerstore/actions/artifacts/{a.artifact_id}/zip'],stdout=stream,check=True)
    with zipfile.ZipFile(z) as archive:
        for info in archive.infolist():
            path=Path(info.filename)
            if path.is_absolute() or '..' in path.parts:raise RuntimeError('unsafe artifact path')
            if path.suffix not in ('.json','.jsonl','.log','.txt'):continue
            # The original round-1 data trees can contain etcd metadata JSON;
            # retain only controller/client evidence and cold receipts.
            if any(part in ('data','original','disk-mount') for part in path.parts):continue
            if any(part.startswith('cold-missing-') for part in path.parts) and path.name not in ('inspect.json','stopped.json'):continue
            dst=artifact/path;dst.parent.mkdir(parents=True,exist_ok=True)
            with archive.open(info) as inp,dst.open('wb') as out:shutil.copyfileobj(inp,out)
    z.unlink()
    subprocess.run([sys.executable,str(Path(__file__).with_name('archive-results.py')),str(source),str(a.destination)],check=True,stdout=subprocess.DEVNULL)
# Each archive helper invocation writes one summary. Aggregate all summaries from
# preserved manifests rather than claiming this one-case summary covers a run.
print(f'archived {a.artifact_name}; temporary ZIP and extracted evidence removed')
