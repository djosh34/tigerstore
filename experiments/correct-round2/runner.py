#!/usr/bin/env python3
"""Round 2 schedules. All process/network/disk operations run on Actions only."""
import json
import os
from pathlib import Path
import signal
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'correct-round1'))
import runner as base


class Controller(base.Controller):
    def __init__(self, args):
        super().__init__(args)
        self.required = {
            'partition-kill': ['partition-old-leader', 'kill-new-leader'],
            'election-pause': ['kill-old-leader', 'pause-election'],
            'heavy-disk': ['disk-enospc'],
            'one-way': ['one-way-partition'],
            'flapping': [f'flap-{i}' for i in range(6)],
        }[args.scenario]
        self.disk_mounts = {}
        self.cuts = {}
        self.fenced = {}
        self.persist()

    def start_node(self, i):
        # Distinct uid identifies packet origin, including sockets whose source is
        # 127.0.0.1. Ports keep client, status and watch traffic outside the cut.
        self.data[i].mkdir(parents=True, exist_ok=True)
        self.cmd(['chown', '-R', str(41000 + i), self.data[i]])
        # Actions has private ancestor directories and sudo may keep umask 077.
        # osfs syncs every ancestor, which needs read as well as search access.
        for parent in [self.data[i].parent, *self.data[i].parent.parents, self.cert]:
            parent.chmod((parent.stat().st_mode & 0o7777) | 0o555)
        for pem in self.cert.glob('*.pem'):
            pem.chmod(0o644)
        self.event('node-permissions', node=i, uid=41000+i,
                   data_mode=oct(self.data[i].stat().st_mode & 0o777),
                   cert_mode=oct(self.cert.stat().st_mode & 0o777))
        original = self.server_args
        self.server_args = lambda n: ['setpriv', '--reuid', str(41000 + n),
                                      '--regid', '0', '--clear-groups', *original(n)]
        try:
            super().start_node(i)
        finally:
            self.server_args = original

    def observe_exits(self):
        # Known watchdog fences are designed behavior. Keep the exit evidence and
        # restart only after the fault heals. Other exits remain failures.
        for i, node in list(self.nodes.items()):
            p = node['p']
            if p.returncode is not None:
                proof = node.get('exit_evidence') or self.record_exit(i, None)
            else:
                pid, status = os.waitpid(p.pid, os.WNOHANG)
                if not pid:
                    continue
                p.returncode = os.waitstatus_to_exitcode(status)
                proof = self.record_exit(i, status)
            if proof['classification'] != 'designed-self-fence':
                raise RuntimeError(f'unexpected node exit: {proof}')
            self.fenced[i] = proof
            del self.nodes[i]
            self.event('designed-fence-retained', node=i, proof=proof)

    def restart_fenced(self):
        for i in list(self.fenced):
            self.start_node(i)
            del self.fenced[i]

    def iptables(self, *argv, **kw):
        return self.cmd(['iptables', '-w', '3', *argv], **kw)

    def cut_rule(self, i):
        return ['-p', 'tcp', '-m', 'owner', '--uid-owner', str(41000 + i),
                '-m', 'multiport', '--ports', str(base.PEER_PORT),
                '-m', 'comment', '--comment', f'round2-node-{i}', '-j', 'DROP']

    def cut_state(self, i):
        text = self.iptables('-L', 'OUTPUT', '-v', '-x', '-n').stdout
        lines = [x for x in text.splitlines() if f'round2-node-{i}' in x]
        base.require(len(lines) == 1, f'missing or repeated packet rule: {text}')
        return {'iptables': text, 'packets': int(lines[0].split()[0]),
                'bytes': int(lines[0].split()[1])}

    def cut(self, name, i):
        self.phase(name, 'requested', node=i, uid=41000+i,
                   semantics='drop only packets originated by this node on peer TCP connections; reverse packets and client RPCs are not selected')
        self.iptables('-A', 'OUTPUT', *self.cut_rule(i))
        self.cuts[i] = name
        state = self.cut_state(i)
        self.phase(name, 'installed', rule=self.cut_rule(i), state=state,
                   process=base.proc_snapshot(self.nodes[i]['p'].pid))

    def prove_cut(self, name, i, before=0):
        end = time.monotonic() + 3
        while time.monotonic() < end:
            state = self.cut_state(i)
            if state['packets'] > before:
                self.phase(name, 'exercised', actual_peer_tcp_drops=state)
                return
            self.sleep(.1, require_workload=True)
        raise RuntimeError(f'{name}: no actual peer TCP packet drop')

    def heal_cut(self, name, i, ready=True):
        state = self.cut_state(i)
        self.iptables('-D', 'OUTPUT', *self.cut_rule(i))
        del self.cuts[i]
        after = self.iptables('-S', 'OUTPUT').stdout
        base.require(f'round2-node-{i}' not in after, 'packet cut remains after healing')
        self.restart_fenced()
        status = self.ready() if ready else None
        self.phase(name, 'healed', final_state=state, after=after, status=status)

    def raw_status(self, members):
        p = self.cmd(self.client_args('status', members) +
                     ['--status-only', '--request-timeout', '200ms'], timeout=2, check=False)
        return json.loads(p.stdout)['nodes']

    def leader_index(self, state):
        leaders = {n['leader_id'] for n in state['nodes']}
        base.require(len(leaders) == 1 and '0' not in leaders, f'no common leader: {state}')
        leader = next(iter(leaders))
        return next(i for i in self.nodes for n in state['nodes']
                    if n['endpoint'] == self.endpoint(i) and n['member_id'] == leader)

    def kill_named(self, name, i, **evidence):
        node = self.nodes[i]
        self.phase(name, 'requested', node=i, pid=node['p'].pid,
                   generation=node['generation'], **evidence)
        self.phase(name, 'installed', signal=int(signal.SIGKILL),
                   proc=base.proc_snapshot(node['p'].pid))
        proof = self.kill_node(i)
        base.require(not proof.get('already_exited'), 'kill target exited before SIGKILL')
        self.phase(name, 'exercised', waitpid=proof)

    def partition_kill(self):
        old, state = self.role_node('leader')
        pair = [i for i in self.nodes if i != old]
        self.cut('partition-old-leader', old)
        self.prove_cut('partition-old-leader', old)
        elected = self.ready(pair, timeout=12)
        new = self.leader_index(elected)
        base.require(new != old, 'replacement leader equals isolated leader')
        self.kill_named('kill-new-leader', new, old_leader=old,
                        replacement_status=elected, partition_still_installed=self.cut_state(old))
        self.sleep(2, require_workload=True)
        self.start_node(new)
        self.heal_cut('partition-old-leader', old)
        self.phase('kill-new-leader', 'healed', status=self.ready(),
                   new_pid=self.nodes[new]['p'].pid, generation=self.generations[new])

    def election_pause(self):
        old, state = self.role_node('leader')
        pair = [i for i in self.nodes if i != old]
        self.kill_named('kill-old-leader', old, status=state)
        # Catch the leaderless interval using Status only. A linearizable read
        # would wait for the election and erase the timing evidence.
        until = time.monotonic() + 8
        leaderless = None
        while time.monotonic() < until:
            nodes = self.raw_status(pair)
            if all(n['leader_id'] == '0' and n['member_id'] != '0' for n in nodes):
                leaderless = nodes
                break
            self.sleep(.01, require_workload=True)
        base.require(leaderless is not None, 'no observed leaderless election interval; missing fault hit')
        i = self.rng.choice(pair)
        p = self.nodes[i]['p']
        self.phase('pause-election', 'requested', node=i, pid=p.pid,
                   leaderless_status=leaderless, old_leader_dead=True,
                   semantics='SIGSTOP during an observed leaderless election interval, not a named Raft vote instruction')
        os.kill(p.pid, signal.SIGSTOP)
        self.phase('pause-election', 'installed', signal=int(signal.SIGSTOP))
        proof = self.wait_status(p, os.WUNTRACED,
            lambda s: os.WIFSTOPPED(s) and os.WSTOPSIG(s) == signal.SIGSTOP)
        first = base.proc_snapshot(p.pid)
        self.sleep(3, require_workload=True)
        last = base.proc_snapshot(p.pid)
        base.require(first['tasks'] and last['tasks'] and
                     all('\tT ' in s for s in first['tasks'].values()) and
                     all('\tT ' in s for s in last['tasks'].values()) and
                     (first['utime'], first['stime']) == (last['utime'], last['stime']),
                     f'pause did not hold all tasks: {first}, {last}')
        self.phase('pause-election', 'exercised', waitpid=proof, first=first, last=last,
                   surviving_status=self.raw_status([n for n in pair if n != i]))
        os.kill(p.pid, signal.SIGCONT)
        continued = self.wait_status(p, os.WCONTINUED, os.WIFCONTINUED)
        self.start_node(old)
        status = self.ready()
        self.phase('pause-election', 'healed', continued=continued, status=status)
        self.phase('kill-old-leader', 'healed', new_pid=self.nodes[old]['p'].pid, status=status)

    def setup_disk(self):
        # Give every member its own bounded filesystem so the elected leader can
        # be selected after startup, without a forced transfer or shared failure.
        for i in range(1, 4):
            mount = self.out / f'disk-mount-n{i}'
            mount.mkdir()
            image = self.out / f'disk-n{i}.ext4'
            with image.open('wb') as f:
                f.truncate(self.a.disk_mib * 1024 * 1024)
            loop = self.cmd(['losetup', '--find', '--show', image]).stdout.strip()
            self.disk_mounts[i] = {'mount': mount, 'loop': loop, 'mounted': False}
            self.cmd(['mkfs.ext4', '-F', '-m', '0', loop], timeout=20)
            self.cmd(['mount', '-t', 'ext4', '-o', 'rw,noatime,barrier=1', loop, mount])
            self.disk_mounts[i]['mounted'] = True
            self.data[i] = mount / f'n{i}'
            actual = json.loads(self.cmd(['findmnt', '--json', '--target', mount]).stdout)
            base.require(actual['filesystems'][0]['fstype'] == 'ext4', 'wrong disk filesystem')
            self.event('disk-setup', node=i, loop=loop, mount=actual,
                       bytes=self.a.disk_mib * 1024 * 1024)

    def workload_extra_args(self):
        if self.a.scenario != 'heavy-disk':
            return []
        return ['--burst-arm-file', str(self.out / 'burst.arm'),
                '--burst-release-file', str(self.out / 'burst.release'),
                '--burst-ready-file', str(self.out / 'burst-ready.json'),
                '--burst-writes', '8']

    def arm_burst(self):
        (self.out / 'burst.arm').write_text('arm all 256 clients\n')
        end = time.monotonic() + 6
        while time.monotonic() < end:
            marker = self.out / 'burst-ready.json'
            if marker.exists():
                ready = json.loads(marker.read_text())
                base.require(ready['clients_ready'] == 256 and ready['writes_per_client'] == 8,
                             f'incomplete heavy-load barrier: {ready}')
                self.event('burst-ready', marker=ready)
                return
            self.sleep(.01, require_workload=True)
        raise RuntimeError('256-client burst barrier did not become ready in six seconds')

    def disk_installed(self):
        if self.a.scenario == 'heavy-disk':
            (self.out / 'burst.release').write_text('ENOSPC installed, release 256 writers\n')
            self.event('burst-released', clients=256, sequential_puts_per_client=8,
                       total_unpaced_puts=2048, installed_fault=self.faults['disk-enospc']['installed'])

    def run_faults(self):
        self.event('round2-workload', clients=self.a.clients, per_client_rate=self.a.rate,
                   keys=self.a.keys, member_client_counts=[len(range(i, self.a.clients, 3)) for i in range(3)],
                   max_scheduled_ops=self.a.clients*self.a.rate*self.a.duration,
                   watches=3, prev_kv=True)
        if self.a.scenario == 'partition-kill':
            self.partition_kill()
        elif self.a.scenario == 'election-pause':
            self.election_pause()
        elif self.a.scenario == 'heavy-disk':
            self.disk_node, state = self.role_node('leader')
            self.disk_mount = self.disk_mounts[self.disk_node]['mount']
            self.event('disk-leader-selected', node=self.disk_node, status=state)
            self.arm_burst()
            self.disk_fault()
        elif self.a.scenario == 'one-way':
            i, _ = self.role_node('leader')
            name = 'one-way-partition'
            self.cut(name, i)
            self.prove_cut(name, i)
            self.sleep(5, require_workload=True)
            self.heal_cut(name, i)
        elif self.a.scenario == 'flapping':
            i, _ = self.role_node('leader')
            for flap in range(6):
                name = f'flap-{flap}'
                self.cut(name, i)
                self.prove_cut(name, i)
                self.sleep(.4 if flap % 2 == 0 else 1.2, require_workload=True)
                self.heal_cut(name, i, ready=False)
                self.sleep(.3, require_workload=True)
            self.ready()

    def cleanup(self):
        errors = []
        for i in list(self.cuts):
            try:
                self.iptables('-D', 'OUTPUT', *self.cut_rule(i), starting=False)
                del self.cuts[i]
            except Exception as exc:
                errors.append(f'packet rule cleanup: {exc}')
        errors += super().cleanup()
        return errors

    def cleanup_extra(self):
        errors = []
        for disk in self.disk_mounts.values():
            try:
                if disk['mounted']:
                    self.cmd(['umount', disk['mount']], starting=False)
                    disk['mounted'] = False
                self.cmd(['losetup', '-d', disk['loop']], starting=False)
            except Exception as exc:
                errors.append(f'disk cleanup: {exc}')
        if errors:
            base.dump(self.out / 'round2-cleanup-errors.json', errors)
        return errors


if __name__ == '__main__':
    base.Controller = Controller
    sys.exit(base.main())
