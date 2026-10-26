#!/usr/bin/env python3
"""One gosim invocation. Host RSS guard and native-clock phase measurements."""
import base64, json, os, pathlib, queue, signal, subprocess, threading, time

out=pathlib.Path(os.environ['RUNNER_TEMP'])/'data449'
out.mkdir(exist_ok=True)
test=os.environ.get('CELL','TestDataHeavy')
env=dict(os.environ, GOMAXPROCS='2', GOFLAGS='-p=1', GOMEMLIMIT='12GiB', DATA449_PHASES=str(out/'phases.jsonl'))
if os.environ.get('PROFILE')=='1': env['DATA449_HEAP']=str(out/'heap')
p=subprocess.Popen([os.environ['DATA449_BINARY'],'-metatest'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=open(out/'stderr.log','wb'),env=env,start_new_session=True)
p.stdin.write((json.dumps({'Test':test,'Seed':1,'ExtraEnv':[]})+'\n').encode());p.stdin.flush()
q=queue.Queue()
def reader():
    q.put(p.stdout.readline())
threading.Thread(target=reader,daemon=True).start()
start=time.monotonic();peak=0;status='running';reply=None
with open(out/'rss.jsonl','w') as samples:
    while True:
        elapsed=time.monotonic()-start
        try:
            stat=pathlib.Path(f'/proc/{p.pid}/status').read_text()
            rss=int(next(l.split()[1] for l in stat.splitlines() if l.startswith('VmRSS:')))
        except (OSError,StopIteration): rss=0
        peak=max(peak,rss)
        samples.write(json.dumps({'wall_s':elapsed,'rss_kib':rss})+'\n');samples.flush()
        if rss>int(14.5*1024**2): status='memory-guard';break
        if elapsed>19800: status='wall-guard';break
        try: reply=q.get(timeout=1)
        except queue.Empty:
            if p.poll() is not None: status='process-exit';break
            continue
        if not reply: status='empty-result';break
        try:
            result=json.loads(reply)
            log=base64.b64decode(result.get('LogOutput') or '').decode(errors='replace')
            (out/'run.log').write_text(log)
            (out/'result.json').write_text(json.dumps(result))
            status='failed' if result.get('Failed') or result.get('Err') else 'passed'
        except Exception as e:
            (out/'stdout.log').write_bytes(reply)
            status='bad-result:'+repr(e)
        break
try: os.killpg(p.pid,signal.SIGKILL)
except ProcessLookupError: pass
p.wait()
phases=[]
if (out/'phases.jsonl').exists():
    phases=[json.loads(l) for l in (out/'phases.jsonl').read_text().splitlines()]
phase_times={x['Phase']:x['WallNS']/1e9 for x in phases}
splits={}
for name,a,b in [('load','load','scenario'),('scenario_settling','scenario','sim-check'),('sim_check','sim-check','sim-check-done'),('hashes_shutdown','sim-check-done','independent'),('independent','independent','done')]:
    if a in phase_times and b in phase_times: splits[name]=round(phase_times[b]-phase_times[a],3)
summary={'test':test,'outcome':status,'wall_s':round(time.monotonic()-start,3),'peak_rss_gib':round(peak/1024**2,3),'splits_s':splits,'last_phase':phases[-1] if phases else None}
(out/'summary.json').write_text(json.dumps(summary,indent=2))
print(json.dumps(summary,indent=2),flush=True)
if (out/'run.log').exists():
    for l in (out/'run.log').read_text().splitlines():
        if 'DATA449' in l or 'FAIL' in l or 'witness:' in l: print(l[:5000])
if status!='passed': raise SystemExit(1)
