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
            'election-pause': ['election-peer-cut', 'kill-old-leader', 'pause-election'],
            'heavy-disk': ['disk-enospc'],
            'heavy-read-disk': ['disk-enospc'],
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
        i = self.rng.choice(pair)
        self.cut('election-peer-cut', i)
        self.prove_cut('election-peer-cut', i)
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
        self.heal_cut('election-peer-cut', i, ready=False)
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
        if self.a.scenario not in ('heavy-disk', 'heavy-read-disk'):
            return []
        args = ['--burst-arm-file', str(self.out / 'burst.arm'),
                '--burst-release-file', str(self.out / 'burst.release'),
                '--burst-ready-file', str(self.out / 'burst-ready.json'),
                '--burst-writes', '1', '--interval', '3s',
                '--burst-healed-file', str(self.out / 'disk.healed')]
        if self.a.scenario == 'heavy-read-disk':
            args += ['--read-arm-file', str(self.out / 'read.arm'),
                     '--read-release-file', str(self.out / 'read.release'),
                     '--read-ready-file', str(self.out / 'read-ready.json'),
                     '--trigger-file', str(self.out / 'trigger.request.json')]
        return args

    def arm_burst(self):
        (self.out / 'burst.arm').write_text('arm all 256 clients\n')
        end = time.monotonic() + 6
        while time.monotonic() < end:
            marker = self.out / 'burst-ready.json'
            if marker.exists():
                ready = json.loads(marker.read_text())
                base.require(ready['clients_ready'] == 256 and ready['writes_per_client'] == 1,
                             f'incomplete heavy-load barrier: {ready}')
                self.event('burst-ready', marker=ready)
                return
            self.sleep(.01, require_workload=True)
        raise RuntimeError('256-client burst barrier did not become ready in six seconds')

    def wait_marker(self, path, expected, timeout=8):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if path.exists():
                marker = json.loads(path.read_text())
                base.require(all(marker.get(k) == v for k, v in expected.items()),
                             f'bad barrier marker: {path}: {marker}')
                return marker
            self.sleep(.01, require_workload=True)
        raise RuntimeError(f'barrier marker missing: {path}')

    def prepare_read_fault(self):
        (self.out / 'burst.release').write_text('release pre-fault 256-Put burst\n')
        complete = self.wait_marker(self.out / 'burst-ready.json.complete.json',
                                   {'clients_complete': 256, 'failed_calls': 0})
        self.event('prefault-write-burst-complete', marker=complete, clients=256)
        (self.out / 'read.arm').write_text('park all clients for 256 concurrent reads under ENOSPC\n')
        ready = self.wait_marker(self.out / 'read-ready.json',
                                {'clients_ready': 256, 'writes_per_client': 0})
        self.event('read-burst-ready', marker=ready)

    def disk_installed(self):
        if self.a.scenario == 'heavy-read-disk':
            (self.out / 'read.release').write_text('ENOSPC installed; release reads\n')
            base.dump(self.out / 'trigger.request.json', {'endpoint': self.endpoint(self.disk_node)})
            self.event('read-burst-and-trigger-released', read_clients=256, trigger_puts=1,
                       trigger_value_bytes=1024*1024, leader=self.disk_node,
                       installed_fault=self.faults['disk-enospc']['installed'])
        elif self.a.scenario == 'heavy-disk':
            (self.out / 'burst.release').write_text('ENOSPC installed, release 256 writers\n')
            self.event('burst-released', clients=256, sequential_puts_per_client=1,
                       total_unpaced_puts=256, installed_fault=self.faults['disk-enospc']['installed'])

    def disk_exercised(self):
        if self.a.scenario == 'heavy-read-disk':
            self.sleep(1, require_workload=True)
            self.read_window_end_ns = self.now()
            space = os.statvfs(self.disk_mount)
            self.event('enospc-read-window-finished', blocks_available=space.f_bavail,
                       no_space_filler_still_present=(self.disk_mount / 'filler').exists())

    def run_faults(self):
        heavy = self.a.scenario in ('heavy-disk', 'heavy-read-disk')
        self.event('round2-workload', clients=self.a.clients, per_client_rate=1/3 if heavy else self.a.rate,
                   keys=self.a.keys, member_client_counts=[len(range(i, self.a.clients, 3)) for i in range(3)],
                   paced_op_budget=self.a.clients*(1 + self.a.duration//3) if heavy else self.a.clients*(1 + self.a.rate*self.a.duration),
                   watches=3, prev_kv=True)
        if self.a.scenario == 'partition-kill':
            self.partition_kill()
        elif self.a.scenario == 'election-pause':
            self.election_pause()
        elif self.a.scenario in ('heavy-disk', 'heavy-read-disk'):
            self.disk_node, state = self.role_node('leader')
            self.disk_mount = self.disk_mounts[self.disk_node]['mount']
            self.event('disk-leader-selected', node=self.disk_node, status=state)
            self.arm_burst()
            if self.a.scenario == 'heavy-read-disk':
                self.prepare_read_fault()
            self.disk_fault()
            (self.out / 'disk.healed').write_text('server ENOSPC healed and all nodes ready\n')
            self.event('disk-healed-marker')
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

    def workload_finished(self):
        traffic = json.loads((self.out / 'workload/traffic.json').read_text())
        history = json.loads((self.out / 'workload/history.json').read_text())
        if self.a.scenario == 'heavy-read-disk':
            installed = self.faults['disk-enospc']['installed']['monotonic_ns']
            controller_ns = int(base.dt.datetime.fromisoformat(self.utc_origin).timestamp()*1e9)
            installed_client_ns = controller_ns + installed - traffic['utc_origin_ns']
            release_client_ns = controller_ns + self.read_window_end_ns - traffic['utc_origin_ns']
            puts = [op for op in history['ops'] if '-burst' in op.get('req', {}).get('put', {}).get('value', '')]
            reads = json.loads((self.out / 'workload/read-burst.json').read_text())
            trigger = [op for op in history['ops'] if op['client'] == 40000]
            base.require(len(puts) == 256 and all('resp' in op and op['return'] < installed_client_ns for op in puts),
                         'pre-fault write burst was not 256 acknowledged puts before ENOSPC')
            base.require(len(reads) == 256 and len({op['client'] for op in reads}) == 256 and
                         all(installed_client_ns <= op['call'] <= release_client_ns for op in reads),
                         'not all 256 read-burst calls started under installed ENOSPC')
            base.require(len(trigger) == 1 and installed_client_ns <= trigger[0]['call'] <= release_client_ns,
                         'missing or duplicated recorded ENOSPC trigger Put')
            self.event('read-fault-traffic-verified', prefault_acked_puts=256,
                       under_enospc_reads=256, trigger_puts=1, trigger_ambiguous='err' in trigger[0], traffic=traffic)
        healed = max(phases['healed']['monotonic_ns'] for phases in self.faults.values())
        controller_utc_ns = int(base.dt.datetime.fromisoformat(self.utc_origin).timestamp()*1e9)
        bound = controller_utc_ns + healed + 10_000_000_000 - traffic['utc_origin_ns']
        late = [op for op in history['ops'] if op['client'] < self.a.clients and op['call'] >= bound]
        successful = {op['client'] for op in late if 'resp' in op}
        errors = [op for op in late if 'err' in op]
        self.event('post-heal-liveness', grace_seconds=10, request_timeout_seconds=2,
                   late_operations=len(late), successful_clients=len(successful),
                   missing_clients=sorted(set(range(self.a.clients))-successful),
                   failed_late_operations=len(errors), traffic=traffic)
        base.require(len(successful) == self.a.clients and not errors,
                     f'post-heal liveness failed: {len(successful)}/{self.a.clients} clients progressed, {len(errors)} late errors')

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
