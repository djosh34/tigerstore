#!/usr/bin/env python3
"""Every recorded history runs all four checkers, even when log conversion fails."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--client',required=True);p.add_argument('--backend',required=True)
    p.add_argument('--out',required=True,type=Path)
    a=p.parse_args();a.out.mkdir(parents=True,exist_ok=True)
    history=a.out/'workload/history.json';manifest=a.out/'oracles.json'
    oracles=json.loads(manifest.read_text()) if manifest.exists() else []
    results=[];failed=len(oracles)!=4
    def run(name,cmd):
        nonlocal failed
        start=time.monotonic()
        try:
            r=subprocess.run(cmd,capture_output=True,text=True,timeout=140)
            code=r.returncode;output=r.stdout+r.stderr
        except (subprocess.TimeoutExpired,OSError) as e:
            code=124 if isinstance(e,subprocess.TimeoutExpired) else 127;output=str(e)
        row={'oracle':name,'mode':cmd[1],'returncode':code,'seconds':time.monotonic()-start,'output':output}
        results.append(row);failed|=code!=0
        print(json.dumps(row),flush=True)
    if not history.exists():
        results.append({'oracle':'none','mode':'missing-history','returncode':1,'output':'FAILED: no recorded history; no checker claims made'})
        failed=True
    else:
        if not oracles:
            oracles=[{'name':'unconverted','data_dirs':[],'stopped_file':''}]
        for oracle in oracles:
            name=oracle['name'];enriched=a.out/f'history-{name}.json'
            run(name,[a.client,'log','--backend',a.backend,'--data-dirs',','.join(oracle['data_dirs']),
                      '--history',str(history),'--out',str(enriched),'--stopped-file',oracle['stopped_file']])
            if not enriched.exists():
                # Raw ops and watches are preserved, not replaced with a guessed log.
                # The repository checkers still execute and must reject the missing oracle.
                shutil.copyfile(history,enriched)
            run(name,[a.client,'check','--history',str(enriched),'--checker-timeout','30s','--out',str(a.out/f'check-{name}.json')])
    (a.out/'offline-results.json').write_text(json.dumps(results,indent=2)+'\n')
    return int(failed)
if __name__=='__main__':sys.exit(main())
