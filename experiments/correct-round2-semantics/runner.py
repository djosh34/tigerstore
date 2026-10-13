#!/usr/bin/env python3
"""Runner-only lease, compaction and concurrent-watch campaign. Reuses round 1."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback

base_path = Path(__file__).resolve().parents[1] / 'correct-round1/runner.py'
spec = importlib.util.spec_from_file_location('round1', base_path)
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)

class Controller(base.Controller):
    def __init__(self, args):
        super().__init__(args)
        self.required = ['peer-blackhole', 'stop-leader', 'kill-leader']
        self.partition = False
        self.persist()

    def wait_file(self, path, timeout):
        end = time.monotonic() + timeout
        while not path.exists():
            base.require(time.monotonic() < end, f'missing workload marker {path}')
            self.sleep(.1, require_workload=True)

    def blackhole(self):
        name = 'peer-blackhole'
        self.phase(name, 'requested', selector='all TCP peer port 2380, both directions', hold_seconds=2)
        self.cmd(['iptables', '-N', 'SEMANTICS'])
        self.cmd(['iptables', '-A', 'SEMANTICS', '-p', 'tcp', '--dport', '2380', '-j', 'DROP'])
        self.cmd(['iptables', '-A', 'SEMANTICS', '-p', 'tcp', '--sport', '2380', '-j', 'DROP'])
        self.cmd(['iptables', '-I', 'OUTPUT', '1', '-j', 'SEMANTICS'])
        self.partition = True
        installed = self.cmd(['iptables-save', '-c']).stdout
        base.require('--dport 2380 -j DROP' in installed and '--sport 2380 -j DROP' in installed,
                     'peer selectors not installed')
        self.phase(name, 'installed', iptables=installed)
        self.sleep(2, require_workload=True)
        state = self.cmd(['iptables', '-L', 'SEMANTICS', '-nvx']).stdout
        packets = sum(int(line.split()[0]) for line in state.splitlines() if ' DROP ' in line)
        base.require(packets > 0, f'no actual TCP peer traffic hit the fault: {state}')
        # Client ports are not selected. A bounded read fails without a quorum.
        read = self.cmd(self.client_args('status') + ['--request-timeout', '1s'], check=False, timeout=5)
        base.require(read.returncode != 0, 'all members passed linearizable health while peer quorum was blackholed')
        self.phase(name, 'exercised', actual_peer_packets=packets, counters=state,
                   quorum_read_returncode=read.returncode, quorum_read_output=read.stdout+read.stderr)
        self.heal_partition()
        status = self.ready(timeout=25)
        self.phase(name, 'healed', iptables=self.cmd(['iptables-save', '-c']).stdout, status=status)

    def heal_partition(self):
        if self.partition:
            self.cmd(['iptables', '-D', 'OUTPUT', '-j', 'SEMANTICS'], starting=False)
            self.cmd(['iptables', '-F', 'SEMANTICS'], starting=False)
            self.cmd(['iptables', '-X', 'SEMANTICS'], starting=False)
            self.partition = False

    def pause_leader(self):
        name = 'stop-leader'
        i, selection = self.role_node('leader')
        node = self.nodes[i]
        p = node['p']
        old = next(x['member_id'] for x in selection['nodes'] if x['endpoint'] == self.endpoint(i))
        pair = [n for n in self.nodes if n != i]
        self.phase(name, 'requested', node=i, pid=p.pid, status=selection, hold_seconds=8)
        os.kill(p.pid, signal.SIGSTOP)
        proof = self.wait_status(p, os.WUNTRACED, lambda s: os.WIFSTOPPED(s) and os.WSTOPSIG(s) == signal.SIGSTOP)
        first = base.proc_snapshot(p.pid)
        base.require(first['tasks'] and all('\tT ' in v for v in first['tasks'].values()), 'not all tasks stopped')
        self.phase(name, 'installed', waitpid=proof, proc=first)
        base.dump(self.out / 'workload/fault-live.json', {'healthy': pair, 'paused': i})
        status = self.ready(pair, timeout=20)
        base.require(all(x['leader_id'] != old for x in status['nodes']), 'leader did not change while paused')
        self.sleep(8, require_workload=True)
        second = base.proc_snapshot(p.pid)
        base.require(all('\tT ' in v for v in second['tasks'].values()) and
                     (first['utime'], first['stime']) == (second['utime'], second['stime']),
                     'paused leader ran during fault')
        if self.a.scenario == 'compaction':
            # Workload receipt alone is not proof of fault overlap.
            self.wait_file(self.out / 'workload/compacted-during-fault', 8)
        self.phase(name, 'exercised', proc_first=first, proc_last=second, new_leader_status=status,
                   compaction_overlap=self.a.scenario == 'compaction')
        os.kill(p.pid, signal.SIGCONT)
        continued = self.wait_status(p, os.WCONTINUED, os.WIFCONTINUED)
        status = self.ready(timeout=25)
        self.phase(name, 'healed', waitpid=continued, status=status)

    def run(self):
        self.guard()
        self.event('environment', uname=self.cmd(['uname', '-a']).stdout,
                   runner=os.environ.get('RUNNER_NAME'), image=os.environ.get('ImageVersion'),
                   commit=self.cmd(['git', 'rev-parse', 'HEAD']).stdout.strip(),
                   product_commit='5d964228952d6983ba273bc2e6813770aad796c6',
                   netns=os.readlink('/proc/self/ns/net'), init_netns=os.readlink('/proc/1/ns/net'),
                   server_sha256=hashlib.sha256(Path(self.a.server).read_bytes()).hexdigest())
        self.cmd(['ip', 'link', 'set', 'lo', 'up'])
        if self.a.target == 'etcd':
            base.require('etcd Version: 3.7.2' in self.cmd([self.a.server, '--version']).stdout,
                         'baseline is not stock etcd v3.7.2')
        self.certs()
        for i in range(1, 4): self.start_node(i)
        self.ready(timeout=25)
        argv = self.client_args('run') + ['--scenario', self.a.scenario, '--seed', str(self.a.seed),
                '--watchers', str(self.a.watchers), '--out', str(self.out / 'workload')]
        with (self.out / 'workload.log').open('ab', buffering=0) as log:
            self.workload = subprocess.Popen(argv, stdout=log, stderr=log, start_new_session=True)
        self.event('workload-start', pid=self.workload.pid, argv=argv)
        self.wait_file(self.out / 'workload/prepared', 30)
        self.blackhole()
        self.pause_leader()
        self.signal_fault('kill', 'leader')
        (self.out / 'workload/faults-healed').write_text('all faults verified and healed\n')
        code = self.workload.wait(timeout=120)
        self.event('workload-finish', returncode=code)
        base.require(code == 0, 'semantic workload failed; see workload.log')
        self.inspect([1, 2, 3], self.out / 'pre-kill-inspect.json')
        evidence = self.reap_all()
        self.preserve_original(evidence)
        self.cold_recovery()
        missing = [f'{n}/{p}' for n in self.required for p in ('requested','installed','exercised','healed')
                   if p not in self.faults.get(n, {})]
        base.require(not missing, f'missing fault evidence {missing}')
        base.require(len(self.oracles) == 4, 'not all cold recovery sets completed')
        self.event('controller-pass', note='all four offline repository checkers still required')

    def cleanup(self):
        self.heal_partition()
        return super().cleanup()

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--target', choices=['tigerstore','etcd'], required=True)
    p.add_argument('--scenario', choices=['lease','compaction','watchers'], required=True)
    p.add_argument('--server', required=True)
    p.add_argument('--client', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--seed', type=int, default=4901)
    p.add_argument('--repetition', type=int, default=1)
    p.add_argument('--watchers', type=int, default=96)
    p.add_argument('--in-namespace', action='store_true')
    a = p.parse_args()
    base.require(os.environ.get('GITHUB_ACTIONS') == 'true' and
                 os.environ.get('RUNNER_ENVIRONMENT') == 'github-hosted', 'faults only on free GitHub runners')
    base.require(os.geteuid() == 0 and base.deadline_seconds() > 0, 'requires sudo and time before deadline')
    a.server = str(Path(a.server).resolve());a.client = str(Path(a.client).resolve())
    a.pause_seconds = 8;a.kill_seconds = 1.2
    if not a.in_namespace:
        os.execvp('unshare',['unshare','--net','--mount','--propagation','private',sys.executable,
                 str(Path(__file__).resolve()),*sys.argv[1:],'--in-namespace'])
    base.require(os.readlink('/proc/self/ns/net') != os.readlink('/proc/1/ns/net'), 'refusing host namespace')
    ctl = Controller(a)
    def alarm(_s,_f): raise TimeoutError('controller budget or absolute deadline reached')
    signal.signal(signal.SIGALRM,alarm)
    signal.setitimer(signal.ITIMER_REAL,min(400,max(.01,base.deadline_seconds()-90)))
    code = 1
    try:
        ctl.run();code=0
    except BaseException as e:
        ctl.event('controller-failure',error=str(e),traceback=traceback.format_exc())
        print(f'FAILED: {e}',file=sys.stderr,flush=True)
    finally:
        signal.setitimer(signal.ITIMER_REAL,0)
        try:
            if ctl.cleanup(): code=1
        except BaseException as e:
            code=1
            print(f'FAILED cleanup: {e}',file=sys.stderr,flush=True)
    base.dump(ctl.out/'result.json',{'controller_pass':code==0,'offline_checkers_required':True})
    return code

if __name__ == '__main__':sys.exit(main())
