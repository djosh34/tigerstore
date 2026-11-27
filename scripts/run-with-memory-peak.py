#!/usr/bin/env python3
"""Measure an entire command tree with its dedicated cgroup-v2 memory.peak.

The caller supplies a fresh systemd scope. The native driver, translated gosim
process, checkers, and reports stay in that scope. This is not sampled child RSS.
A manager threshold breach terminates only the process group we started.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def cgroup_path():
    for line in Path('/proc/self/cgroup').read_text().splitlines():
        if line.startswith('0::'):
            return Path('/sys/fs/cgroup') / line[3:].lstrip('/')
    raise ValueError('cgroup v2 is required for whole-run memory accounting')


def counter(path):
    value = path.read_text().strip()
    if not value.isdecimal():
        raise ValueError(f'non-numeric kernel counter: {path}')
    return int(value)


def measure(command, out, group, threshold_mib, interval=1):
    out.mkdir(parents=True, exist_ok=False)
    peak_file = group / 'memory.peak'
    peak = counter(peak_file)  # Fail before dispatch if accounting is absent.
    limit = (group / 'memory.max').read_text().strip()
    threshold = threshold_mib << 20
    process = subprocess.Popen(command, start_new_session=True)
    interrupted = []
    def forward(sig, _frame):
        interrupted.append(sig)
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass
    original = {sig: signal.signal(sig, forward) for sig in (signal.SIGINT, signal.SIGTERM)}
    exceeded = False
    started = time.monotonic()
    try:
        with (out / 'samples.jsonl').open('w') as samples:
            while True:
                peak = max(peak, counter(peak_file))
                sample = dict(elapsed_seconds=time.monotonic()-started,
                              current_bytes=counter(group / 'memory.current'), peak_bytes=peak)
                samples.write(json.dumps(sample)+'\n')
                samples.flush()
                if peak > threshold:
                    exceeded = True
                    print(f'MANAGER_RULING: whole-run peak {peak} bytes ({peak/(1<<20):.3f} MiB) exceeds {threshold_mib} MiB', flush=True)
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                    break
                try:
                    process.wait(timeout=interval)
                    break
                except subprocess.TimeoutExpired:
                    continue
            peak = max(peak, counter(peak_file))
        report = dict(schema=1, source='cgroup-v2 memory.peak', cgroup=str(group),
                      command=command, command_exit_code=process.returncode,
                      whole_run_peak_bytes=peak, whole_run_peak_mib=peak/(1<<20),
                      threshold_mib=threshold_mib, above_threshold=peak > threshold,
                      memory_max=limit, interrupted=interrupted,
                      elapsed_seconds=time.monotonic()-started)
        (out / 'summary.json').write_text(json.dumps(report, indent=2)+'\n')
        print('WHOLE_RUN_MEMORY '+json.dumps(report), flush=True)
        if exceeded or report['above_threshold']:
            return 3
        return process.returncode if process.returncode >= 0 else 128-process.returncode
    finally:
        for sig, handler in original.items():
            signal.signal(sig, handler)
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--threshold-mib', type=int, default=11264)
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ['--'] else args.command
    if not command or args.threshold_mib <= 0:
        parser.error('a command and a positive threshold are required')
    return measure(command, args.out, cgroup_path(), args.threshold_mib)


if __name__ == '__main__':
    sys.exit(main())
