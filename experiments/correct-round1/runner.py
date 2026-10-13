#!/usr/bin/env python3
"""Throwaway, runner-only cluster chaos. No product instrumentation."""
import argparse
import datetime as dt
import errno
import glob
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import signal
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time
import traceback

DEADLINE = dt.datetime(2026, 10, 13, 22, tzinfo=dt.timezone.utc)
PEER_PORT = 2380
CLIENT_PORT = 2379


def deadline_seconds():
    return (DEADLINE - dt.datetime.now(dt.timezone.utc)).total_seconds()


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def dump(path, value):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, indent=2) + '\n')
    tmp.replace(path)


def proc_snapshot(pid):
    tasks = {}
    for path in glob.glob(f'/proc/{pid}/task/*/status'):
        lines = Path(path).read_text().splitlines()
        tasks[Path(path).parent.name] = next(x for x in lines if x.startswith('State:'))
    stat = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    return {'tasks': tasks, 'utime': int(stat[11]), 'stime': int(stat[12]),
            'starttime': int(stat[19])}


class Controller:
    def __init__(self, args):
        self.a = args
        self.out = Path(args.out).resolve()
        self.out.mkdir(parents=True, exist_ok=False)
        self.origin = time.monotonic_ns()
        self.utc_origin = dt.datetime.now(dt.timezone.utc).isoformat()
        self.end = time.monotonic() + min(540, deadline_seconds())
        self.commands = (self.out / 'commands.jsonl').open('a', buffering=1)
        self.events = []
        self.faults = {}
        self.oracles = []
        self.nodes = {}
        self.generations = {i: 0 for i in range(1, 4)}
        self.workload = None
        self.loop = None
        self.mounted = False
        self.netem = False
        self.cold_mode = False
        self.rng = random.Random((args.seed << 16) + args.repetition)
        self.data = {i: self.out / 'data' / f'n{i}' for i in range(1, 4)}
        self.required = []
        dump(self.out / 'result.json', {'controller_pass': False,
             'offline_checkers_required': True, 'reason': 'run has not completed'})
        if args.scenario in ('signals', 'mixed'):
            self.required += ['kill-leader', 'kill-follower', 'stop-leader', 'stop-follower']
        if args.scenario in ('netem', 'mixed'):
            self.required += ['netem-loss', 'netem-delay', 'netem-reorder']
        if args.scenario in ('disk', 'mixed'):
            self.required += ['disk-enospc']
        self.persist()

    def now(self):
        return time.monotonic_ns() - self.origin

    def guard(self):
        require(deadline_seconds() > 0, 'UTC deadline reached; starting nothing')
        require(time.monotonic() < self.end, '540-second run budget exhausted')

    def persist(self):
        dump(self.out / 'faults.json', {'target': self.a.target, 'scenario': self.a.scenario,
             'seed': self.a.seed, 'repetition': self.a.repetition,
             'utc_origin': self.utc_origin,
             'monotonic_origin_ns': self.origin, 'required': self.required,
             'faults': self.faults, 'events': self.events})
        dump(self.out / 'oracles.json', self.oracles)

    def event(self, name, **evidence):
        self.events.append({'name': name, 'monotonic_ns': self.now(), **evidence})
        self.persist()

    def phase(self, name, phase, **evidence):
        record = self.faults.setdefault(name, {})
        require(phase not in record, f'duplicate phase {name}/{phase}')
        record[phase] = {'monotonic_ns': self.now(), **evidence}
        self.persist()

    def cmd(self, argv, timeout=15, check=True, starting=True):
        if starting:
            self.guard()
        start = self.now()
        try:
            p = subprocess.run([str(x) for x in argv], capture_output=True, text=True,
                               timeout=min(timeout, max(.1, self.end - time.monotonic())))
            self.commands.write(json.dumps({'argv': [str(x) for x in argv],
                'start_ns': start, 'end_ns': self.now(), 'returncode': p.returncode,
                'stdout': p.stdout, 'stderr': p.stderr}) + '\n')
            if check and p.returncode:
                raise RuntimeError(f'command failed {argv}: {p.returncode}: {p.stderr[-4000:]}')
            return p
        except subprocess.TimeoutExpired as exc:
            self.commands.write(json.dumps({'argv': [str(x) for x in argv],
                'start_ns': start, 'end_ns': self.now(), 'timeout': timeout}) + '\n')
            raise RuntimeError(f'command timed out: {argv}') from exc

    def sleep(self, seconds, require_workload=False):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            self.guard()
            if require_workload:
                require(self.workload is not None and self.workload.poll() is None,
                        'workload exited before required fault completed')
            time.sleep(min(.1, max(0, until - time.monotonic())))

    def certs(self):
        self.cert = self.out / 'certs'
        self.cert.mkdir()
        self.cmd(['openssl', 'ecparam', '-name', 'prime256v1', '-genkey', '-noout',
                  '-out', self.cert / 'ca-key.pem'])
        self.cmd(['openssl', 'req', '-x509', '-new', '-key', self.cert / 'ca-key.pem',
                  '-days', '2', '-subj', '/CN=correct-round1 CA', '-out', self.cert / 'ca.pem'])
        sans = ','.join(['DNS:localhost', 'IP:127.0.0.1'] +
                       [f'IP:127.{prefix}.0.{i}' for prefix in (77, 78) for i in range(1, 4)])
        ext = self.cert / 'cert.ext'
        ext.write_text(f'subjectAltName={sans}\nextendedKeyUsage=serverAuth,clientAuth\n')
        for name in ['n1', 'n2', 'n3', 'client']:
            key = self.cert / f'{name}-key.pem'
            csr = self.cert / f'{name}.csr'
            self.cmd(['openssl', 'ecparam', '-name', 'prime256v1', '-genkey', '-noout', '-out', key])
            self.cmd(['openssl', 'req', '-new', '-key', key, '-subj', f'/CN={name}', '-out', csr])
            self.cmd(['openssl', 'x509', '-req', '-in', csr, '-CA', self.cert / 'ca.pem',
                      '-CAkey', self.cert / 'ca-key.pem', '-CAcreateserial', '-days', '2',
                      '-extfile', ext, '-out', self.cert / f'{name}.pem'])
        self.event('certificates', sans=sans)

    @staticmethod
    def endpoint(i):
        return f'https://127.78.0.{i}:{CLIENT_PORT}'

    def client_args(self, mode, members=None):
        return [self.a.client, mode, '--endpoints', ','.join(self.endpoint(i) for i in
            (members if members is not None else sorted(self.nodes))),
            '--cacert', str(self.cert / 'ca.pem'), '--cert', str(self.cert / 'client.pem'),
            '--key', str(self.cert / 'client-key.pem')]

    def server_args(self, i):
        peer = f'https://127.77.0.{i}:{PEER_PORT}'
        cert = self.cert / f'n{i}.pem'
        key = self.cert / f'n{i}-key.pem'
        ca = self.cert / 'ca.pem'
        argv = [self.a.server, '--name', f'n{i}', '--data-dir', str(self.data[i]),
            '--initial-cluster', ','.join(f'n{n}=https://127.77.0.{n}:{PEER_PORT}' for n in range(1, 4)),
            '--initial-cluster-token', f'correct-round1-{self.a.seed}-{self.a.repetition}',
            '--listen-peer-urls', peer, '--listen-client-urls', self.endpoint(i),
            '--advertise-client-urls', self.endpoint(i), '--cert-file', str(cert),
            '--key-file', str(key), '--trusted-ca-file', str(ca), '--client-cert-auth',
            '--peer-cert-file', str(cert), '--peer-key-file', str(key),
            '--peer-trusted-ca-file', str(ca)]
        if self.a.target == 'etcd':
            argv += ['--initial-advertise-peer-urls', peer, '--peer-client-cert-auth',
                     '--snapshot-count', '1000000']
        return argv

    def start_node(self, i):
        self.guard()
        require(i not in self.nodes, f'node {i} already managed')
        self.data[i].mkdir(parents=True, exist_ok=True)
        self.generations[i] += 1
        log = self.out / f'n{i}-{self.generations[i]}.log'
        argv = self.server_args(i)
        with log.open('ab', buffering=0) as stream:
            p = subprocess.Popen(argv, stdout=stream, stderr=stream, start_new_session=True,
                                 env={**os.environ, 'GOMAXPROCS': '2', 'GOMEMLIMIT': '2GiB'})
        self.nodes[i] = {'p': p, 'log': log, 'generation': self.generations[i]}
        self.event('node-start', node=i, pid=p.pid, generation=self.generations[i],
                   argv=argv, data_dir=str(self.data[i]), log=str(log))

    def wait_status(self, p, flags, predicate, timeout=5, cleanup=False):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if not cleanup:
                self.guard()
            pid, status = os.waitpid(p.pid, flags | os.WNOHANG)
            if pid:
                if os.WIFEXITED(status) or os.WIFSIGNALED(status):
                    p.returncode = os.waitstatus_to_exitcode(status)
                require(predicate(status), f'unexpected waitpid status {status} for PID {p.pid}')
                return {'pid': pid, 'raw_status': status,
                        'wifsignaled': os.WIFSIGNALED(status),
                        'termsig': os.WTERMSIG(status) if os.WIFSIGNALED(status) else None,
                        'wifstopped': os.WIFSTOPPED(status),
                        'stopsig': os.WSTOPSIG(status) if os.WIFSTOPPED(status) else None,
                        'wifcontinued': os.WIFCONTINUED(status),
                        'returncode': p.returncode, 'monotonic_ns': self.now()}
            time.sleep(.02)
        raise RuntimeError(f'waitpid evidence timeout for PID {p.pid}')

    def kill_node(self, i, cleanup=False):
        node = self.nodes[i]
        p = node['p']
        # Read exit status before sending the signal. Never claim a prior death as SIGKILL.
        if p.returncode is not None:
            pid, status = p.pid, None
        else:
            pid, status = os.waitpid(p.pid, os.WNOHANG)
        if pid:
            if status is not None:
                p.returncode = os.waitstatus_to_exitcode(status)
            evidence = {'pid': pid, 'raw_status': status, 'returncode': p.returncode,
                        'already_exited': True, 'monotonic_ns': self.now()}
        else:
            os.kill(p.pid, signal.SIGKILL)
            evidence = self.wait_status(p, 0,
                lambda s: os.WIFSIGNALED(s) and os.WTERMSIG(s) == signal.SIGKILL,
                cleanup=cleanup)
        del self.nodes[i]
        self.event('node-reaped', node=i, generation=node['generation'], **evidence)
        return evidence

    def ready(self, members=None, timeout=18):
        members = members if members is not None else sorted(self.nodes)
        end = time.monotonic() + timeout
        last = None
        while time.monotonic() < end:
            self.guard()
            result = self.cmd(self.client_args('status', members), timeout=10, check=False)
            if result.returncode == 0:
                state = json.loads(result.stdout)
                nodes = state['nodes']
                leaders = {str(x['leader_id']) for x in nodes}
                if len(nodes) == len(members) and len(leaders) == 1 and '0' not in leaders:
                    self.event('ready', members=members, status=state)
                    return state
            last = result.stdout + result.stderr
            self.sleep(.25)
        raise RuntimeError(f'cluster not ready within {timeout}s: {last}')

    def role_node(self, role):
        state = self.ready()
        leader = next(iter({str(x['leader_id']) for x in state['nodes']}))
        choices = [i for i in sorted(self.nodes) for n in state['nodes']
                   if n['endpoint'] == self.endpoint(i) and
                   ((str(n['member_id']) == leader) == (role == 'leader'))]
        require(bool(choices), f'cannot find {role}: {state}')
        return self.rng.choice(choices), state

    def signal_fault(self, kind, role):
        self.guard()
        self.sleep(.25, require_workload=True)
        i, selection = self.role_node(role)
        node = self.nodes[i]
        p = node['p']
        name = f'{kind}-{role}'
        self.phase(name, 'requested', node=i, pid=p.pid, generation=node['generation'],
                   role=role, status=selection)
        before = proc_snapshot(p.pid)
        sig = signal.SIGKILL if kind == 'kill' else signal.SIGSTOP
        os.kill(p.pid, sig)
        self.phase(name, 'installed', signal=int(sig), proc_before=before)
        if kind == 'kill':
            proof = self.wait_status(p, 0,
                lambda s: os.WIFSIGNALED(s) and os.WTERMSIG(s) == signal.SIGKILL)
            del self.nodes[i]
            self.phase(name, 'exercised', waitpid=proof)
            self.sleep(1.2, require_workload=True)
            self.start_node(i)
        else:
            proof = self.wait_status(p, os.WUNTRACED,
                lambda s: os.WIFSTOPPED(s) and os.WSTOPSIG(s) == signal.SIGSTOP)
            first = proc_snapshot(p.pid)
            require(first['tasks'] and all('\tT ' in s for s in first['tasks'].values()),
                    f'not all tasks stopped: {first}')
            self.sleep(1.5, require_workload=True)
            second = proc_snapshot(p.pid)
            require(second['tasks'] and all('\tT ' in s for s in second['tasks'].values()),
                    f'tasks left stopped state: {second}')
            require((first['utime'], first['stime']) == (second['utime'], second['stime']),
                    f'stopped process consumed CPU: {first}, {second}')
            self.phase(name, 'exercised', waitpid=proof, proc_first=first, proc_last=second)
            os.kill(p.pid, signal.SIGCONT)
            continued = self.wait_status(p, os.WCONTINUED, os.WIFCONTINUED)
            resumed = proc_snapshot(p.pid)
            require(resumed['tasks'] and all('\tT ' not in s for s in resumed['tasks'].values()),
                    f'process still stopped after SIGCONT: {resumed}')
            self.event('signal-continued', node=i, waitpid=continued, proc=resumed)
        status = self.ready()
        self.phase(name, 'healed', new_pid=self.nodes[i]['p'].pid,
                   generation=self.nodes[i]['generation'], status=status)

    def setup_disk(self):
        mount = self.out / 'disk-mount'
        mount.mkdir()
        image = self.out / 'disk.ext4'
        with image.open('wb') as stream:
            stream.truncate(self.a.disk_mib * 1024 * 1024)
        self.loop = self.cmd(['losetup', '--find', '--show', image]).stdout.strip()
        self.cmd(['mkfs.ext4', '-F', '-m', '0', self.loop], timeout=20)
        self.cmd(['mount', '-t', 'ext4', '-o', 'rw,noatime', self.loop, mount])
        self.mounted = True
        self.disk_mount = mount
        self.disk_node = self.rng.choice([1, 2, 3])
        self.data[self.disk_node] = mount / f'n{self.disk_node}'
        actual = json.loads(self.cmd(['findmnt', '--json', '--target', mount]).stdout)
        require(actual['filesystems'][0]['fstype'] == 'ext4', f'wrong filesystem: {actual}')
        self.event('disk-setup', node=self.disk_node, loop=self.loop, image=str(image),
                   bytes=self.a.disk_mib * 1024 * 1024, mount=actual)

    def disk_fault(self):
        self.guard()
        self.sleep(.25, require_workload=True)
        name = 'disk-enospc'
        i = self.disk_node
        node = self.nodes[i]
        self.phase(name, 'requested', node=i, pid=node['p'].pid,
                   generation=node['generation'], data_dir=str(self.data[i]))
        filler = self.disk_mount / 'filler'
        written = 0
        caught = None
        with filler.open('wb', buffering=0) as stream:
            while True:
                self.guard()
                try:
                    written += stream.write(b'F' * (1024 * 1024))
                except OSError as exc:
                    require(exc.errno == errno.ENOSPC, f'filler failed without ENOSPC: {exc}')
                    caught = exc.errno
                    break
        space = os.statvfs(self.disk_mount)
        probe = self.disk_mount / 'no-space-probe'
        try:
            with probe.open('wb', buffering=0) as stream:
                stream.write(b'P' * 4096)
                os.fsync(stream.fileno())
        except OSError as exc:
            require(exc.errno == errno.ENOSPC, f'probe failed without ENOSPC: {exc}')
            probe_errno = exc.errno
        else:
            raise RuntimeError('filled ext4 still accepted 4096-byte probe')
        self.phase(name, 'installed', filler_bytes=written, filler_errno=caught,
                   probe_errno=probe_errno, blocks_available=space.f_bavail,
                   filesystem=self.cmd(['findmnt', '--json', '--target', self.disk_mount]).stdout)
        # Controller ENOSPC is not proof of a server fault. Require the stock node to
        # exit with its own disk error while the recorded client keeps writing.
        exit_proof = self.wait_status(node['p'], 0, os.WIFEXITED, timeout=12)
        log = node['log'].read_text(errors='replace')
        require(node['p'].returncode != 0 and
                ('no space left on device' in log.lower() or 'enospc' in log.lower()),
                f'node did not report disk ENOSPC: {exit_proof}, {log[-4000:]}')
        del self.nodes[i]
        self.phase(name, 'exercised', waitpid=exit_proof, server_log=str(node['log']),
                   server_log_tail=log[-6000:])
        filler.unlink()
        probe.unlink(missing_ok=True)
        with (self.disk_mount / 'freed-probe').open('wb', buffering=0) as stream:
            require(stream.write(b'works-after-free') == 16, 'short healed write')
            os.fsync(stream.fileno())
        self.start_node(i)
        status = self.ready()
        self.phase(name, 'healed', available_bytes=os.statvfs(self.disk_mount).f_bavail * space.f_frsize,
                   new_pid=self.nodes[i]['p'].pid, write_fsync=True, status=status)

    def tc(self, *args, **kw):
        return self.cmd(['tc', *args], **kw)

    def tc_state(self):
        return {'qdisc': json.loads(self.tc('-j', '-s', 'qdisc', 'show', 'dev', 'lo').stdout),
                'filters': json.loads(self.tc('-j', '-s', 'filter', 'show', 'dev', 'lo',
                                             'parent', '1:').stdout)}

    @staticmethod
    def netstat(state):
        return next(x for x in state['qdisc'] if x['kind'] == 'netem')

    @staticmethod
    def udp_packets(state):
        rules = [x for x in state['filters'] if x.get('options', {}).get('keys', {}).get('ip_proto') == 'udp']
        require(len(rules) == 2, f'expected two UDP canary rules: {rules}')
        return sum(a['stats']['packets'] for rule in rules for a in rule['options']['actions'])

    def install_netem(self, i, options):
        self.tc('qdisc', 'add', 'dev', 'lo', 'root', 'handle', '1:', 'prio', 'bands', '3',
                'priomap', *(['0'] * 16))
        self.netem = True
        self.tc('qdisc', 'add', 'dev', 'lo', 'parent', '1:3', 'handle', '30:', 'netem',
                'limit', '10000', *options)
        expected = []
        pref = 1
        for proto in ('tcp', 'udp'):
            # Current transport may dial from 127.0.0.1, not its listening IP.
            # Select the destination for inbound, and the peer listener source for replies.
            for ip_key, port_key in [('dst_ip', 'dst_port'), ('src_ip', 'src_port')]:
                self.tc('filter', 'add', 'dev', 'lo', 'parent', '1:', 'protocol', 'ip',
                    'pref', str(pref), 'flower', 'ip_proto', proto, ip_key, f'127.77.0.{i}',
                    port_key, str(PEER_PORT), 'classid', '1:3', 'action', 'pass')
                expected.append({'eth_type': 'ipv4', 'ip_proto': proto,
                                 ip_key: f'127.77.0.{i}', port_key: PEER_PORT})
                pref += 1
        state = self.tc_state()
        rules = [x for x in state['filters'] if 'options' in x]
        require(len(rules) == 4, f'wrong rule count: {state}')
        require([x['options']['keys'] for x in rules] == expected, f'wrong selectors: {state}')
        require(all(x['options']['classid'] == '1:3' for x in rules), f'wrong class: {state}')
        installed = self.netstat(state)['options']
        require(installed['limit'] == 10000, f'wrong netem limit: {state}')
        if 'delay' in options:
            require(abs(installed['delay']['delay'] - float(options[options.index('delay') + 1][:-2]) / 1000) < .00001,
                    f'wrong delay: {state}')
        if 'loss' in options:
            require(abs(installed['loss-random']['loss'] - .25) < .001, f'wrong loss: {state}')
        if 'reorder' in options:
            require(installed['reorder']['reorder'] == 1 and installed['gap'] == 2,
                    f'wrong reorder: {state}')
        return state

    def burst(self, src, sport, dst, dport, count=240):
        """UDP is only a packet canary; stock peer traffic remains TCP/TLS."""
        arrivals = []
        sent = {}
        stop = threading.Event()
        receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
        receiver.bind((dst, dport))
        receiver.settimeout(.1)
        sender.bind((src, sport))
        def receive():
            while not stop.is_set():
                try:
                    data, _ = receiver.recvfrom(2048)
                    arrivals.append((time.monotonic_ns(), data))
                except socket.timeout:
                    continue
        thread = threading.Thread(target=receive, daemon=True)
        thread.start()
        try:
            for n in range(count):
                self.guard()
                sent[n] = time.monotonic_ns()
                sender.sendto(struct.pack('!I', n) + bytes([n % 251]) * 996, (dst, dport))
                time.sleep(.002)
            self.sleep(.15, require_workload=True)
        finally:
            stop.set()
            thread.join(timeout=1)
            sender.close()
            receiver.close()
        require(not thread.is_alive(), 'canary receiver failed to finish')
        last = -1
        reordered = 0
        latency = []
        seen = set()
        for at, data in arrivals:
            n = struct.unpack('!I', data[:4])[0]
            require(n in sent and data == struct.pack('!I', n) + bytes([n % 251]) * 996,
                    'invalid packet canary payload')
            require(n not in seen, 'duplicate packet canary')
            seen.add(n)
            reordered += n < last
            last = max(last, n)
            latency.append((at - sent[n]) / 1e6)
        require(latency, 'no packet canary received')
        return {'source': [src, sport], 'destination': [dst, dport], 'sent': count,
                'received': len(arrivals), 'missing': count - len(arrivals),
                'out_of_order': reordered, 'median_ms': statistics.median(latency)}

    def positive_canaries(self, i):
        return [self.burst('127.0.0.1', 0, f'127.77.0.{i}', PEER_PORT),
                self.burst(f'127.77.0.{i}', PEER_PORT, '127.79.0.1', 29000)]

    def negative_canaries(self, i):
        return [self.burst('127.0.0.1', 0, f'127.77.0.{i}', CLIENT_PORT, count=60),
                self.burst('127.0.0.1', 0, f'127.78.0.{i}', PEER_PORT, count=60)]

    def netem_fault(self, kind):
        self.guard()
        self.sleep(.25, require_workload=True)
        i, selection = self.role_node('follower')
        name = f'netem-{kind}'
        options = {'loss': ['loss', '25%'], 'delay': ['delay', '5ms'],
                   'reorder': ['delay', '10ms', 'reorder', '100%', 'gap', '2']}[kind]
        self.phase(name, 'requested', node=i, options=options, status=selection,
                   delay_semantics='5ms or 10ms per selected packet direction, not calibrated network RTT')
        before = self.positive_canaries(i)
        require(all(x['missing'] == 0 for x in before), f'baseline canary loss: {before}')
        state = self.install_netem(i, options)
        self.phase(name, 'installed', tc=state)
        hit = self.positive_canaries(i)
        if kind == 'loss':
            require(all(15 < x['missing'] < 115 for x in hit), f'loss did not exercise both selectors: {hit}')
        elif kind == 'delay':
            require(all(x['median_ms'] >= 4 and x['median_ms'] > b['median_ms'] + 3
                        for x, b in zip(hit, before)), f'delay did not exercise: {hit}')
        else:
            require(all(x['out_of_order'] > 20 for x in hit), f'reorder did not exercise: {hit}')
        pre_negative = self.tc_state()
        negative = self.negative_canaries(i)
        post_negative = self.tc_state()
        require(self.udp_packets(pre_negative) == self.udp_packets(post_negative),
                f'negative controls entered peer netem: {pre_negative}, {post_negative}')
        require(all(x['missing'] == 0 and x['out_of_order'] == 0 and x['median_ms'] < 3
                    for x in negative), f'negative controls affected: {negative}')
        stats = self.netstat(post_negative)
        require(stats['packets'] + stats['drops'] > 300, f'netem packet stats did not advance: {stats}')
        tcp_rules = [x for x in post_negative['filters'] if
                     x.get('options', {}).get('keys', {}).get('ip_proto') == 'tcp']
        require(all(sum(a['stats']['packets'] for a in x['options']['actions']) > 0
                    for x in tcp_rules), f'actual TCP peer selectors were not exercised: {tcp_rules}')
        self.sleep(1.5, require_workload=True)
        self.phase(name, 'exercised', baseline=before, positive=hit, negative=negative,
                   tc_before_negative=pre_negative, tc_after_negative=post_negative)
        self.tc('qdisc', 'del', 'dev', 'lo', 'root')
        self.netem = False
        healed_state = json.loads(self.tc('-j', '-s', 'qdisc', 'show', 'dev', 'lo').stdout)
        require(all(x['kind'] != 'netem' for x in healed_state), f'netem remained: {healed_state}')
        healed = self.positive_canaries(i)
        require(all(x['missing'] == 0 and x['out_of_order'] == 0 and x['median_ms'] < 3
                    for x in healed), f'canaries did not heal: {healed}')
        status = self.ready()
        self.phase(name, 'healed', tc=healed_state, canaries=healed, status=status)

    def inspect(self, members, path):
        self.cmd(self.client_args('inspect', members) + ['--receipt',
            str(self.out / 'workload' / 'receipt.json'), '--out', str(path)], timeout=20)
        self.event('receipt-verified', members=members, report=str(path))

    def reap_all(self, cleanup=False):
        evidence = []
        for i in list(self.nodes):
            evidence.append({'node': i, **self.kill_node(i, cleanup=cleanup)})
        return evidence

    def preserve_original(self, evidence):
        if any(x['name'] == 'original' for x in self.oracles):
            return
        original = self.out / 'original'
        original.mkdir(exist_ok=True)
        copied = []
        for i in range(1, 4):
            dst = original / f'n{i}'
            if self.data[i].exists() and not dst.exists():
                shutil.copytree(self.data[i], dst)
                copied.append(str(dst))
            elif dst.exists():
                copied.append(str(dst))
        marker = self.out / 'stopped.json'
        dump(marker, {'monotonic_ns': self.now(), 'reaped': evidence,
                      'live_nodes': [], 'directories': copied})
        if len(copied) == 3:
            self.oracles.append({'name': 'original', 'data_dirs': copied,
                                 'stopped_file': str(marker)})
        self.event('original-preserved', directories=copied, stopped_file=str(marker))

    def cold_recovery(self):
        require(not self.nodes, 'cold recovery requires no live original nodes')
        self.cold_mode = True
        for missing in range(1, 4):
            self.guard()
            pair = [i for i in range(1, 4) if i != missing]
            case = self.out / f'cold-missing-n{missing}'
            case.mkdir()
            self.event('cold-requested', missing=missing, surviving=pair)
            self.data = {}
            for i in pair:
                self.data[i] = case / f'n{i}'
                shutil.copytree(self.out / 'original' / f'n{i}', self.data[i])
            try:
                for i in pair:
                    self.start_node(i)
                require(set(self.nodes) == set(pair), 'cold recovery started missing member')
                self.ready(pair, timeout=25)
                self.inspect(pair, case / 'inspect.json')
                self.event('cold-verified', missing=missing, surviving=pair)
            finally:
                evidence = self.reap_all(cleanup=True)
                marker = case / 'stopped.json'
                dump(marker, {'monotonic_ns': self.now(), 'reaped': evidence, 'live_nodes': []})
                self.oracles.append({'name': f'cold-missing-n{missing}',
                    'data_dirs': [str(self.data[i]) for i in pair], 'stopped_file': str(marker)})
                self.persist()

    def run(self):
        self.guard()
        self.event('environment', uname=self.cmd(['uname', '-a']).stdout,
                   runner=os.environ.get('RUNNER_NAME'), image=os.environ.get('ImageVersion'),
                   commit=self.cmd(['git', 'rev-parse', 'HEAD']).stdout.strip(),
                   netns=os.readlink('/proc/self/ns/net'),
                   init_netns=os.readlink('/proc/1/ns/net'),
                   server_sha256=hashlib.sha256(Path(self.a.server).read_bytes()).hexdigest())
        self.cmd(['ip', 'link', 'set', 'lo', 'up'])
        if self.a.target == 'etcd':
            version = self.cmd([self.a.server, '--version']).stdout
            require('etcd Version: 3.7.2' in version, f'etcd is not stock 3.7.2: {version}')
        self.certs()
        if self.a.scenario in ('disk', 'mixed'):
            self.setup_disk()
        for i in range(1, 4):
            self.start_node(i)
        self.ready(timeout=25)
        argv = self.client_args('run') + ['--duration', f'{self.a.duration}s',
            '--seed', str(self.a.seed), '--clients', '3', '--rate', '5',
            '--value-size', str(self.a.value_size),
            '--out', str(self.out / 'workload'), '--stop-file', str(self.out / 'workload.stop')]
        workload_start = time.monotonic()
        with (self.out / 'workload.log').open('ab', buffering=0) as log:
            self.workload = subprocess.Popen(argv, stdout=log, stderr=log, start_new_session=True)
        self.event('workload-start', pid=self.workload.pid, argv=argv,
                   duration_seconds=self.a.duration, value_size=self.a.value_size)
        self.sleep(2, require_workload=True)
        if self.a.scenario in ('signals', 'mixed'):
            for kind, role in [('kill', 'leader'), ('kill', 'follower'), ('stop', 'leader'), ('stop', 'follower')]:
                self.signal_fault(kind, role)
        if self.a.scenario in ('netem', 'mixed'):
            for kind in ['loss', 'delay', 'reorder']:
                self.netem_fault(kind)
        if self.a.scenario in ('disk', 'mixed'):
            self.disk_fault()
        # A too-short duration is a failed schedule, not a reason to skip faults.
        self.sleep(4, require_workload=True)
        elapsed = time.monotonic() - workload_start
        require(elapsed < self.a.duration, 'duration did not allow four seconds of post-heal writes')
        code = self.workload.wait(timeout=min(self.a.duration + 40 - elapsed,
                                             max(.1, self.end - time.monotonic())))
        self.event('workload-finish', returncode=code)
        require(code == 0, f'workload failed: see {self.out / "workload.log"}')
        self.inspect([1, 2, 3], self.out / 'pre-kill-inspect.json')
        evidence = self.reap_all()
        self.preserve_original(evidence)
        self.cold_recovery()
        missing = [f'{name}/{phase}' for name in self.required for phase in
                   ['requested', 'installed', 'exercised', 'healed'] if phase not in self.faults.get(name, {})]
        require(not missing, f'missing required fault evidence: {missing}')
        require(len(self.oracles) == 4, 'not all cold recovery sets completed')
        self.event('controller-pass', note='four offline checkers must still run for every oracles.json set')

    def cleanup(self):
        errors = []
        if self.workload is not None and self.workload.poll() is None:
            (self.out / 'workload.stop').write_text('controller cleanup\n')
            try:
                self.workload.wait(timeout=40)
            except subprocess.TimeoutExpired:
                os.killpg(self.workload.pid, signal.SIGKILL)
                self.workload.wait(timeout=5)
                errors.append('workload required SIGKILL; history may be incomplete')
        if self.netem:
            try:
                self.tc('qdisc', 'del', 'dev', 'lo', 'root', starting=False)
                self.netem = False
            except Exception as exc:
                errors.append(f'netem cleanup: {exc}')
        try:
            evidence = self.reap_all(cleanup=True)
            if not self.cold_mode:
                self.preserve_original(evidence)
        except Exception as exc:
            errors.append(f'node cleanup/preservation: {exc}')
        if self.mounted:
            try:
                self.cmd(['umount', self.disk_mount], starting=False)
                self.mounted = False
            except Exception as exc:
                errors.append(f'unmount: {exc}')
        if self.loop and not self.mounted:
            try:
                self.cmd(['losetup', '-d', self.loop], starting=False)
                self.loop = None
            except Exception as exc:
                errors.append(f'loop detach: {exc}')
        self.event('cleanup', errors=errors)
        self.commands.close()
        return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--target', choices=['tigerstore', 'etcd'], required=True)
    parser.add_argument('--scenario', choices=['signals', 'netem', 'disk', 'mixed'], required=True)
    parser.add_argument('--server', required=True)
    parser.add_argument('--client', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--seed', type=int, default=49)
    parser.add_argument('--repetition', type=int, default=1)
    parser.add_argument('--duration', type=int, default=90)
    parser.add_argument('--disk-mib', type=int, default=128)
    parser.add_argument('--value-size', type=int, default=128)
    parser.add_argument('--in-namespace', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    require(os.environ.get('GITHUB_ACTIONS') == 'true' and
            os.environ.get('RUNNER_ENVIRONMENT') == 'github-hosted',
            'ALL fault runs require a GitHub-hosted Actions runner; local runs forbidden')
    require(os.geteuid() == 0, 'run with sudo -E')
    require(deadline_seconds() > 0, 'UTC deadline reached; starting nothing')
    require(20 <= args.duration <= 180, '--duration must be 20..180 seconds')
    require(96 <= args.disk_mib <= 256, '--disk-mib must be 96..256')
    require(1 <= args.value_size <= 1024 * 1024, '--value-size must be 1..1048576 bytes')
    args.server = str(Path(args.server).resolve())
    args.client = str(Path(args.client).resolve())
    args.out = str(Path(args.out).resolve())
    for path in [args.server, args.client]:
        require(Path(path).is_file() and os.access(path, os.X_OK), f'not executable: {path}')
    if not args.in_namespace:
        # Private mount namespace prevents loop mounts leaking into the host namespace.
        os.execvp('unshare', ['unshare', '--net', '--mount', '--propagation', 'private',
                             sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], '--in-namespace'])
    require(os.readlink('/proc/self/ns/net') != os.readlink('/proc/1/ns/net'),
            'refusing faults in host network namespace')
    controller = Controller(args)
    def timed_out(_signum, _frame):
        raise TimeoutError('run wall/UTC deadline exceeded')
    signal.signal(signal.SIGALRM, timed_out)
    # A second timer bounds cleanup too. Server sessions cannot escape the hard cap.
    hard_done = threading.Event()
    def hard_timeout():
        if hard_done.wait(max(.01, controller.end - time.monotonic())):
            return
        for node in list(controller.nodes.values()):
            try:
                os.killpg(node['p'].pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        if controller.workload is not None:
            try:
                os.killpg(controller.workload.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        os._exit(124)
    threading.Thread(target=hard_timeout, daemon=True).start()
    signal.setitimer(signal.ITIMER_REAL, min(450, max(.01, deadline_seconds() - 90)))
    code = 1
    try:
        controller.run()
        code = 0
    except BaseException as exc:
        controller.event('controller-failure', error=str(exc), traceback=traceback.format_exc(),
                         classification='tooling-or-product-undetermined; do not file a bug automatically')
        print(f'FAIL: {exc}', file=sys.stderr, flush=True)
    finally:
        # Cleanup is bounded separately and is the only operation allowed after deadline.
        signal.setitimer(signal.ITIMER_REAL, 0)
        if controller.cleanup():
            code = 1
    dump(controller.out / 'result.json', {'controller_pass': code == 0,
         'offline_checkers_required': True, 'out': str(controller.out)})
    hard_done.set()
    return code


if __name__ == '__main__':
    sys.exit(main())
