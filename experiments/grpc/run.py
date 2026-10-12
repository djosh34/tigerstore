#!/usr/bin/env python3
"""Bounded local experiments. All generated files belong to --root."""
import argparse
import datetime
import json
import os
import pathlib
import random
import subprocess
import time
import urllib.request

DEADLINE = datetime.datetime.fromisoformat('2026-10-13T22:00:00+00:00')
def check_time():
    if datetime.datetime.now(datetime.timezone.utc) >= DEADLINE:
        raise RuntimeError('Deadline passed. Start nothing.')

def request(url):
    with urllib.request.urlopen(url, timeout=2) as r:
        return json.load(r)

p=argparse.ArgumentParser()
p.add_argument('--root',type=pathlib.Path,required=True)
p.add_argument('--binary',type=pathlib.Path,required=True)
p.add_argument('--suite',choices=['smoke','pilot','main','latency'],default='main')
p.add_argument('--repetitions',type=int,default=5)
p.add_argument('--generator',choices=['grpc','lean'],default='grpc')
p.add_argument('--server-cpus',default='2')
p.add_argument('--server-procs',default='1')
p.add_argument('--client-cpus',default='4-7')
p.add_argument('--client-procs',default='4')
a=p.parse_args()
a.root=a.root.resolve();a.binary=a.binary.resolve();a.root.mkdir(parents=True,exist_ok=True)
certs=a.root/'certs'
check_time()
if not certs.exists():
    subprocess.run([str(a.binary),'-mode','cert','-cert-dir',str(certs)],check=True)
base=[str(a.binary),'-cert-dir',str(certs)]

def start_server(kind,workers=64):
    check_time()
    log=open(a.root/'server.log','w')
    env=dict(os.environ,GOMAXPROCS=a.server_procs,GOGC='100',GOMEMLIMIT='1GiB')
    cmd=['taskset','-c',a.server_cpus]+base+['-mode','server','-kind',kind,'-workers',str(workers)]
    s=subprocess.Popen(cmd,env=env,stdout=log,stderr=log)
    for _ in range(100):
        if s.poll() is not None:raise RuntimeError('server failed')
        try:
            request('http://127.0.0.1:22380/stats')
            return s,cmd,log
        except Exception:time.sleep(.05)
    s.kill();raise RuntimeError('server startup timeout')

def stop_server(s,log):
    s.terminate()
    try:s.wait(timeout=5)
    except subprocess.TimeoutExpired:s.kill();s.wait()
    log.close()

if a.suite=='smoke':
    for kind in ['grpc-default','grpc-tuned','handler','raw']:
        s,cmd,log=start_server(kind)
        try:
            check_time();subprocess.run(base+['-mode','smoke'],check=True,timeout=20)
            for cli in [str(a.root.parent/'etcd-v3.7.2-linux-arm64/etcdctl'),'/usr/local/bin/etcdctl']:
                cli_base=[cli,'--endpoints=https://127.0.0.1:22379','--cacert='+str(certs/'cert.pem'),'--cert='+str(certs/'cert.pem'),'--key='+str(certs/'key.pem')]
                check_time();subprocess.run(cli_base+['put','cli-key','before'],check=True,timeout=5)
                check_time();r=subprocess.run(cli_base+['get','cli-key'],check=True,timeout=5,text=True,capture_output=True)
                if r.stdout!='cli-key\nbefore\n':raise RuntimeError('bad etcdctl Range '+r.stdout)
                check_time();w=subprocess.Popen(cli_base+['watch','cli-key','--prev-kv','--write-out=json'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                try:
                    time.sleep(.4)
                    check_time();subprocess.run(cli_base+['put','cli-key','after'],check=True,timeout=5)
                    time.sleep(.3);w.terminate();out,err=w.communicate(timeout=5)
                    if not out:raise RuntimeError('no etcdctl watch event '+err)
                    event=json.loads(out)['Events'][0]
                    if event['kv']['value']!='YWZ0ZXI=' or event['prev_kv']['value']!='YmVmb3Jl':raise RuntimeError('bad watch '+out)
                    print(kind,cli,'Put Range Watch passed:',out.strip())
                finally:
                    if w.poll() is None:w.kill();w.wait()
        finally:stop_server(s,log)
else:
    configs=[('grpc-default',64),('grpc-shared',64),('grpc-workers',64),('grpc-tuned',64),('handler',64),('raw',64)]
    if a.suite=='pilot':configs=[('grpc-default',64),('grpc-unshared',64)]+[('grpc-workers',n) for n in [1,8,64,256]]+[('grpc-tuned',64),('handler',64),('raw',64)]
    rng=random.Random(47)
    path=a.root/('results-'+a.suite+'.jsonl')
    with open(path,'w') as dest:
        for rep in range(a.repetitions):
            order=configs.copy();rng.shuffle(order)
            for kind,workers in order:
                s,server_cmd,log=start_server(kind,workers)
                try:
                    streams='1' if a.suite=='latency' else '128'
                    conns='1' if a.suite=='latency' else '2'
                    env=dict(os.environ,GOMAXPROCS=a.client_procs,GOGC='100',GOMEMLIMIT='1GiB')
                    client_cmd=['taskset','-c',a.client_cpus]+base+['-mode','load','-generator',a.generator,'-kind',kind,'-connections',conns,'-streams',streams,'-warmup','1s','-duration','5s']
                    check_time();r=subprocess.run(client_cmd,env=env,check=True,text=True,capture_output=True,timeout=45)
                    row=json.loads(r.stdout);row.update(repetition=rep,workers=workers,server_cpus=a.server_cpus,server_procs=a.server_procs,client_cpus=a.client_cpus,client_procs=a.client_procs,server_cmd=server_cmd,client_cmd=client_cmd,timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat())
                    dest.write(json.dumps(row)+'\n');dest.flush();print(json.dumps(row),flush=True)
                finally:stop_server(s,log)
