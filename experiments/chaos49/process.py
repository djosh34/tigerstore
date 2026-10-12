#!/usr/bin/env python3
"""Question: can a runner throttle one PID and verify pause/kill timing?
Runner only: sudo python3 experiments/chaos49/process.py "$PWD/results/process"
"""
import json, os, pathlib, signal, statistics, subprocess, sys, time


def read(path): return pathlib.Path(path).read_text().strip()
def run(*args): return subprocess.check_output(args, text=True).strip()
def emit(**kw): print(json.dumps(kw),flush=True)
def wait_file(path):
    end=time.monotonic()+8
    while not path.exists():
        assert time.monotonic()<end, str(path)
        time.sleep(.01)

if len(sys.argv)>1 and sys.argv[1]=='worker':
    root=pathlib.Path(sys.argv[2]); root.mkdir(parents=True,exist_ok=True)
    (root/'pid').write_text(str(os.getpid()))
    while not (root/'go').exists(): time.sleep(.001)
    start=time.monotonic(); cpu=time.process_time(); n=0
    while time.monotonic()-start<3:
        for _ in range(10000): n+=1
    (root/'result.json').write_text(json.dumps({'wall_s':time.monotonic()-start,'cpu_s':time.process_time()-cpu,'n':n}))
    while not (root/'release').exists(): time.sleep(.001)
    sys.exit(0)

root=pathlib.Path(sys.argv[1]).resolve(); root.mkdir(parents=True,exist_ok=True)
script=str(pathlib.Path(__file__).resolve())
emit(kernel=run('uname','-a'), systemd=run('systemd','--version'), cgroup_type=run('stat','-fc','%T','/sys/fs/cgroup'))
assert run('stat','-fc','%T','/sys/fs/cgroup')=='cgroup2fs'

def worker(label, prefix=(), direct=None):
    folder=root/label; folder.mkdir()
    p=subprocess.Popen([*prefix,sys.executable,script,'worker',str(folder)])
    try:
        wait_file(folder/'pid'); pid=int(read(folder/'pid'))
        if direct: (direct/'cgroup.procs').write_text(str(pid))
        cg=pathlib.Path('/sys/fs/cgroup')/read(f'/proc/{pid}/cgroup').split('::')[1].lstrip('/')
        quota=read(cg/'cpu.max'); before=read(cg/'cpu.stat')
        (folder/'go').touch(); wait_file(folder/'result.json')
        result=json.loads(read(folder/'result.json'))
        result.update(label=label,pid=pid,cgroup=str(cg),quota=quota,stat_before=before,stat_after=read(cg/'cpu.stat'))
        (folder/'release').touch()
        p.wait(timeout=5); emit(**result)
        return result
    finally:
        if p.poll() is None: p.kill(); p.wait()

base=worker('baseline')
unit=f'chaos49-{os.getpid()}'
# A service gives all subsequently created Go threads the same quota from exec.
systemd=worker('systemd',('systemd-run','--quiet','--wait','--collect','--unit='+unit,
    '--property=CPUQuota=10%','--property=CPUQuotaPeriodSec=100ms','--property=RuntimeMaxSec=12s'))
assert systemd['quota']=='10000 100000',systemd
assert .12 < systemd['cpu_s'] < .65,systemd
assert 'nr_throttled 0\n' not in systemd['stat_after']+'\n',systemd
assert base['cpu_s']>systemd['cpu_s']*2,(base,systemd)

# Direct cgroupfs child of system.slice, not the runner agent's own cgroup.
direct=pathlib.Path('/sys/fs/cgroup/system.slice')/f'chaos49-direct-{os.getpid()}'
direct.mkdir()
try:
    assert (direct/'cpu.max').exists(), 'cpu controller unavailable in child'
    (direct/'cpu.max').write_text('10000 100000')
    d=worker('direct',direct=direct)
    assert d['quota']=='10000 100000' and .12<d['cpu_s']<.65,d
finally:
    direct.rmdir()

# waitpid WUNTRACED acknowledges actual stop, not just successful kill(2).
stop=[]; resume=[]; killed=[]
for trial in range(10):
    p=subprocess.Popen([sys.executable,'-c','while True: pass'])
    time.sleep(.02)
    start=time.monotonic_ns(); os.kill(p.pid,signal.SIGSTOP)
    pid,status=os.waitpid(p.pid,os.WUNTRACED)
    stop.append((time.monotonic_ns()-start)/1e6)
    assert os.WIFSTOPPED(status) and os.WSTOPSIG(status)==signal.SIGSTOP
    task_states=[read(x/'stat').split(') ')[1][0] for x in pathlib.Path(f'/proc/{pid}/task').iterdir()]
    assert all(s=='T' for s in task_states),task_states
    cpu_before=read(f'/proc/{pid}/stat').split(') ')[1].split()[11:13]
    time.sleep(.1)
    cpu_after=read(f'/proc/{pid}/stat').split(') ')[1].split()[11:13]
    assert cpu_before==cpu_after
    start=time.monotonic_ns(); os.kill(pid,signal.SIGCONT); _,status=os.waitpid(pid,os.WCONTINUED)
    resume.append((time.monotonic_ns()-start)/1e6); assert os.WIFCONTINUED(status)
    time.sleep(.02)
    # Kill a stopped process too. SIGKILL does not require SIGCONT first.
    os.kill(pid,signal.SIGSTOP); os.waitpid(pid,os.WUNTRACED)
    start=time.monotonic_ns(); os.kill(pid,signal.SIGKILL); p.wait(timeout=2)
    killed.append((time.monotonic_ns()-start)/1e6); assert p.returncode==-signal.SIGKILL
emit(stop_ms=stop,continue_ms=resume,kill_wait_ms=killed,
     stop_median_ms=statistics.median(stop),kill_wait_median_ms=statistics.median(killed),
     note='kill_wait includes Python Popen.wait polling; not kernel scheduling latency')
print('PASS: per-process quota, throttle counters, stopped state, frozen CPU and reaped SIGKILL',flush=True)
