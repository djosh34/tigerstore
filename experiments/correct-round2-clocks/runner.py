#!/usr/bin/env python3
"""Runner-only quota and verified single-member native Go realtime jumps."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

BASE = Path(__file__).resolve().parents[1] / 'correct-round1' / 'runner.py'
spec = importlib.util.spec_from_file_location('base', BASE)
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)
require = base.require

class Controller(base.Controller):
    def __init__(self, args):
        super().__init__(args)
        self.canaries = []
        self.cgroup = None
        self.offsets = {}

    def certs(self):
        super().certs()
        if self.a.scenario not in ('clock', 'clock-control'):
            return
        for i in range(1, 4):
            self.offsets[i] = self.out / f'offset-n{i}.txt'
            self.offsets[i].write_text('0\n')
            log = self.out / f'canary-n{i}.jsonl'
            with log.open('ab', buffering=0) as stream:
                p = subprocess.Popen([self.a.clockwrap, str(self.offsets[i]),
                    str(self.out / f'canary-trace-n{i}.jsonl'), self.a.canary],
                    stdout=stream, stderr=stream, start_new_session=True)
            self.canaries.append(p)
            self.event('canary-start', node=i, tracer_pid=p.pid, log=str(log))

    def server_args(self, i):
        argv = super().server_args(i)
        if self.a.scenario in ('clock', 'clock-control'):
            argv = [self.a.clockwrap, str(self.offsets[i]),
                    str(self.out / f'clock-trace-n{i}-{self.generations[i]}.jsonl'), *argv]
        return argv

    def traffic_ready(self):
        if self.a.near_limit:
            end = time.monotonic() + 20
            while not (self.out / 'workload' / 'limit.json').exists():
                require(time.monotonic() < end, 'limit probes did not finish within 20s')
                self.sleep(.1, require_workload=True)

    @staticmethod
    def cpu_stat(path):
        return {k: int(v) for k, v in (line.split() for line in (path / 'cpu.stat').read_text().splitlines())}

    def cpu_fault(self):
        self.traffic_ready()
        i, selection = self.role_node(self.a.fault_role)
        p = self.nodes[i]['p']
        group = Path('/sys/fs/cgroup/system.slice') / f'correct-clocks-{p.pid}'
        group.mkdir()
        self.cgroup = group
        original = Path(f'/proc/{p.pid}/cgroup').read_text().strip().split('::')[1]
        self.phase('cpu-quota', 'requested', node=i, pid=p.pid, role=self.a.fault_role,
                   quota_percent=self.a.quota_percent, status=selection)
        quota = f'{self.a.quota_percent * 1000} 100000'
        (group / 'cpu.max').write_text(quota)
        (group / 'cgroup.procs').write_text(str(p.pid))
        membership = Path(f'/proc/{p.pid}/cgroup').read_text()
        require(group.name in membership, 'actual node did not join cgroup')
        require((group / 'cpu.max').read_text().strip() == quota, 'quota differs')
        first = self.cpu_stat(group)
        before = base.proc_snapshot(p.pid)
        start = time.monotonic()
        self.phase('cpu-quota', 'installed', cgroup=membership, cpu_max=quota,
                   cpu_stat=first, process=before)
        self.sleep(8, require_workload=True)
        last = self.cpu_stat(group)
        after = base.proc_snapshot(p.pid)
        seconds = time.monotonic() - start
        require(last['nr_throttled'] > first['nr_throttled'], 'quota installed but no actual throttle hit')
        require(last['throttled_usec'] > first['throttled_usec'], 'no actual throttled duration')
        require(after['utime'] + after['stime'] > before['utime'] + before['stime'], 'gray node did no CPU work')
        self.phase('cpu-quota', 'exercised', cpu_stat=last, process=after,
                   wall_seconds=seconds, cpu_seconds=(last['usage_usec']-first['usage_usec'])/1e6)
        (Path('/sys/fs/cgroup') / original.lstrip('/') / 'cgroup.procs').write_text(str(p.pid))
        healed = Path(f'/proc/{p.pid}/cgroup').read_text()
        require(group.name not in healed, 'node quota was not removed')
        self.phase('cpu-quota', 'healed', membership=healed, status=self.ready())
        group.rmdir()
        self.cgroup = None

    def traces(self, i):
        p = self.out / f'clock-trace-n{i}-{self.generations[i]}.jsonl'
        return [json.loads(x) for x in p.read_text().splitlines()]

    def canary(self, i):
        lines = (self.out / f'canary-n{i}.jsonl').read_text().splitlines()
        require(lines, f'no canary samples for n{i}')
        return json.loads(lines[-1])

    def clock_fault(self):
        self.traffic_ready()
        i, selection = self.role_node(self.a.fault_role)
        self.sleep(.5, require_workload=True)
        for n in range(1, 4):
            rows = self.traces(n)
            require(any(x['event'] == 'auxv' and x['replacement'] == 0 for x in rows), 'vDSO not disabled')
            require(any(x['event'] == 'realtime' and x['offset_sec'] == 0 for x in rows), 'no actual Go server realtime syscall')
        schedule = [('clock-forward', 3600), ('clock-backward', -3600)] if self.a.scenario == 'clock' else [('clock-zero-control', 0)]
        for name, delta in schedule:
            self.phase(name, 'requested', node=i, tracer_pid=self.nodes[i]['p'].pid,
                       role=self.a.fault_role, offset_seconds=delta, status=selection)
            before = {n: self.canary(n) for n in range(1, 4)}
            counts = {n: len(self.traces(n)) for n in range(1, 4)}
            self.offsets[i].write_text(f'{delta}\n')
            self.phase(name, 'installed', offsets={n: p.read_text() for n, p in self.offsets.items()})
            self.sleep(3, require_workload=True)
            after = {n: self.canary(n) for n in range(1, 4)}
            proof = {}
            for n in range(1, 4):
                elapsed = (after[n]['monotonic_elapsed_ns']-before[n]['monotonic_elapsed_ns'])/1e9
                wall = (after[n]['realtime_ns']-before[n]['realtime_ns'])/1e9
                expected = delta if n == i else 0
                require(2 < elapsed < 5, f'monotonic affected n{n}: {elapsed}')
                require(abs(wall-elapsed-expected) < .2, f'clock effect differs n{n}: wall={wall}, mono={elapsed}, expected={expected}')
                hits = [x for x in self.traces(n)[counts[n]:] if x['event'] == 'realtime']
                require(hits and all(x['offset_sec'] == expected and x['returned_sec']-x['native_sec'] == expected for x in hits), f'actual server clock effect absent or wrong n{n}')
                proof[n] = {'canary_before': before[n], 'canary_after': after[n],
                    'wall_seconds': wall, 'monotonic_seconds': elapsed,
                    'server_clock_hits': len(hits), 'server_first_hit': hits[0], 'server_last_hit': hits[-1]}
            self.phase(name, 'exercised', nodes=proof)
            self.offsets[i].write_text('0\n')
            self.sleep(.6, require_workload=True)
            require(self.traces(i)[-1].get('offset_sec') == 0, 'server clock did not heal')
            self.phase(name, 'healed', actual_server_clock=self.traces(i)[-1], status=self.ready())

    def cleanup(self):
        for p in self.offsets.values():
            p.write_text('0\n')
        for p in self.canaries:
            if p.poll() is None:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait(timeout=5)
        errors = super().cleanup()
        if self.cgroup:
            try:
                self.cgroup.rmdir()
            except OSError as exc:
                errors.append(str(exc))
        return errors

base.Controller = Controller
if __name__ == '__main__':
    sys.exit(base.main())
