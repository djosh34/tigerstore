#!/usr/bin/env python3
"""Actions-only matched main/candidate rolling174 race diagnosis, not a gate."""
import argparse
import json
import os
from pathlib import Path
import subprocess


def replace(path, old, new):
    text=path.read_text()
    if text.count(old)!=1:
        raise ValueError(f'non-unique diagnostic anchor: {path}: {old!r}')
    path.write_text(text.replace(old,new))


def run(args):
    if os.environ.get('GITHUB_ACTIONS')!='true':
        raise ValueError('Actions only')
    root=Path.cwd()
    out=args.out.resolve();out.mkdir(parents=True,exist_ok=False)
    checkout=Path(os.environ['RUNNER_TEMP'])/'rolling-node-source'
    subprocess.run(['git','worktree','add','--detach',str(checkout),args.ref],check=True)
    (out/'source-ref.txt').write_text(args.ref+'\n')
    (checkout/'cmd/tigerstore/diagnostic_cpu.go').write_bytes((root/'bench/fetch-measure/rolling_profile.go.txt').read_bytes())
    replace(checkout/'cmd/tigerstore/main.go','func main() {','func main() {\n defer diagnosticCPU()()')
    subprocess.run(['gofmt','-w',str(checkout/'cmd/tigerstore/diagnostic_cpu.go'),str(checkout/'cmd/tigerstore/main.go')],check=True)
    (out/'observer.patch').write_bytes(subprocess.check_output(['git','diff','--binary'],cwd=checkout))
    (out/'diagnostic_cpu.go').write_bytes((checkout/'cmd/tigerstore/diagnostic_cpu.go').read_bytes())
    for name,number in [('old',1),('new',2)]:
        original=checkout/'internal/wl/format.go'
        text=original.read_text();anchor='const maxFormat = 1\n'
        if text.count(anchor)!=1:raise ValueError('format overlay drift')
        target=out/f'{name}-format.go';target.write_text(text.replace(anchor,f'const maxFormat = {number}\n'))
        patches={str(original):str(target)}
        if name=='old':
            refusal=checkout/'internal/wl/refusal.go'
            text=refusal.read_text();anchor='const peerRefusals = true\n'
            if text.count(anchor)!=1:raise ValueError('refusal overlay drift')
            target=out/'old-refusal.go';target.write_text(text.replace(anchor,'const peerRefusals = false\n'))
            patches[str(refusal)]=str(target)
        overlay=out/f'{name}.json';overlay.write_text(json.dumps({'Replace':patches}))
        subprocess.run(['go','build','-race','-overlay',str(overlay),'-o',str(out/f'tigerstore-{name}'),'./cmd/tigerstore'],cwd=checkout,check=True)
    results=[]
    for label,pattern in [('isolated174','^TestRollingUpgrade$/^174$'),('original-sequence','^TestRollingUpgrade$')]:
        dest=out/label;dest.mkdir();cpu=dest/'cpu';cpu.mkdir()
        env=dict(os.environ,ROLLING_OLD=str(out/'tigerstore-old'),ROLLING_NEW=str(out/'tigerstore-new'),
                 ROLLING_OUT=str(dest/'rolling'),ROLLING_PROFILE_DIR=str(cpu),ETCD_UNSAFE_LOG_RING_BYTES='536870912')
        with (dest/'result.log').open('w') as log:
            result=subprocess.run(['go','test','-race','-count=1','-timeout','8m','-v','./cmd/chaos','-run',pattern],
                                  env=env,stdout=log,stderr=subprocess.STDOUT,check=False)
        results.append(dict(case=label,pattern=pattern,returncode=result.returncode))
        (out/'diagnostic-results.json').write_text(json.dumps(dict(ref=args.ref,diagnostic_only=True,cases=results),indent=2)+'\n')
    # Closed CPU chunks remain valid if a failing test deliberately SIGQUITs
    # its stuck nodes; incomplete final chunks are retained and labeled.
    for p in out.glob('*/cpu/*.pprof'):
        with p.with_suffix('.top.txt').open('w') as log:
            result=subprocess.run(['go','tool','pprof','-top','-nodefraction=0',str(p)],stdout=log,stderr=subprocess.STDOUT,check=False)
        if result.returncode:
            p.with_suffix('.incomplete.txt').write_text('final chunk interrupted by process termination; not used for CPU conclusions\n')
    return 0


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--ref',required=True)
    p.add_argument('--out',type=Path,required=True)
    run(p.parse_args())
