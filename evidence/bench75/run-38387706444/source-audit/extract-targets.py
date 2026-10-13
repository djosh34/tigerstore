#!/usr/bin/env python3
from pathlib import Path
import json,datetime,subprocess,tarfile,io,shutil,sys
R=Path('/home/joshazimullah.linux/work_mounts/scratch/bench7980-source-audit')
REPO=Path('/home/joshazimullah.linux/work_mounts/agent_tools/paseo_host/state/worktrees/1fubxbmj/exp-bench-75')
assert datetime.datetime.now(datetime.timezone.utc)<datetime.datetime(2026,10,13,22,tzinfo=datetime.timezone.utc)
git=R/'object-repo'
if not git.exists():
 subprocess.run(['git','init','--bare',str(git)],check=True,stdout=subprocess.DEVNULL)
 common=Path(subprocess.check_output(['git','rev-parse','--path-format=absolute','--git-common-dir'],cwd=REPO).decode().strip())
 (git/'objects/info/alternates').write_text(str(common/'objects')+'\n')
targets=json.loads((R/(sys.argv[1] if len(sys.argv)>1 else 'preliminary-targets.json')).read_text())
for t,sha in targets.items():
 assert len(sha)==40 and all(x in '0123456789abcdef' for x in sha)
 if subprocess.run(['git','cat-file','-e',sha],cwd=git,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode:
  subprocess.run(['git','fetch','--no-tags','https://github.com/djosh34/tigerstore.git',sha],cwd=git,check=True)
 dest=R/'sources'/t
 if dest.exists():shutil.rmtree(dest)
 dest.mkdir(parents=True)
 data=subprocess.check_output(['git','archive',sha],cwd=git)
 with tarfile.open(fileobj=io.BytesIO(data)) as tar:tar.extractall(dest,filter='data')
 print(t,sha)
