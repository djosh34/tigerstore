#!/usr/bin/env python3
"""Run a separate Go worker and require both installation and firing evidence."""
import errno
import json
import pathlib
import socket
import subprocess
import sys
import time

binary, release, work = sys.argv[1:]
work = pathlib.Path(work).resolve()
work.mkdir(parents=True, exist_ok=True)
data = work / 'wal.bin'
sock = work / 'ctl.sock'
if sock.exists():
    raise RuntimeError('refusing a stale control socket')


def call(**q):
    with socket.socket(socket.AF_UNIX) as c:
        c.settimeout(3)
        c.connect(str(sock))
        c.sendall(json.dumps(q).encode() + b'\n')
        with c.makefile('rb') as f:
            return json.loads(f.readline())


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def status():
    return call(cmd='status')


def fired(ident):
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        events = status().get('events', [])
        matches = [e for e in events if e['id'] == ident and e['phase'] == 'fired']
        if matches:
            require(len(matches) == 1, 'duplicate firing')
            print(json.dumps(matches[0]), flush=True)
            return matches[0]
        time.sleep(.01)
    raise RuntimeError('requested fault did not fire: ' + ident)


def install(ident, op, action, nth=1, cut=0):
    r = call(cmd='install', id=ident, path=str(data), op=op, action=action, nth=nth, cut=cut)
    require(r['ok'], str(r))
    events = status()['events']
    e = [e for e in events if e['id'] == ident and e['phase'] == 'installed']
    require(len(e) == 1, 'missing install acknowledgment')
    print(json.dumps(e[0]), flush=True)


def io(op, **kw):
    return call(cmd='io', path=str(data), op=op, **kw)


# The normal executable must refuse a controller, even if the flag is supplied.
r = subprocess.run([release, '--data', str(work / 'release.bin'), '--control', str(sock)],
                   capture_output=True, text=True, timeout=3)
require(r.returncode != 0 and 'requires a -tags=faultfs build' in r.stderr, 'release accepted control')
p = subprocess.Popen([binary, '--data', str(data), '--control', str(sock)],
                     stdout=subprocess.DEVNULL)
try:
    deadline = time.monotonic() + 3
    while not sock.exists():
        require(p.poll() is None and time.monotonic() < deadline, 'worker failed to start')
        time.sleep(.01)
    print(json.dumps({'worker_pid': p.pid, 'controller_pid': __import__('os').getpid(),
                      'incarnation': status()['incarnation']}), flush=True)
    require(io('write', data='BASELINE', offset=0)['n'] == 8, 'baseline write failed')
    require(io('sync')['ok'], 'baseline real fsync failed')

    # Bad scope must fail installation. An installed but unmatched rule is not coverage.
    require(not call(cmd='install', id='bad', path='wrong', op='sync', action='error', nth=1)['ok'],
            'wrong path accepted')
    install('sync-eio', 'sync', 'error', nth=2)
    require(io('sync')['ok'], 'Nth selection fired early')
    require(not any(e['phase'] == 'fired' for e in status()['events']), 'false firing evidence')
    r = io('sync')
    require(not r['ok'] and r['errno'] == errno.EIO, 'missing EIO')
    fired('sync-eio')
    require(io('sync')['ok'], 'one-shot fault was not consumed')
    require(not call(cmd='install', id='sync-eio', path=str(data), op='sync', action='error', nth=1)['ok'],
            'rule ID reused')

    install('sync-stall', 'sync', 'stall')
    with socket.socket(socket.AF_UNIX) as held:
        held.settimeout(.15)
        held.connect(str(sock))
        held.sendall(json.dumps(dict(cmd='io', path=str(data), op='sync')).encode() + b'\n')
        fired('sync-stall')
        try:
            premature = held.recv(4096)
            raise RuntimeError('stalled operation returned: ' + repr(premature))
        except TimeoutError:
            pass
        require(call(cmd='release', id='sync-stall')['ok'], 'could not release blocked worker')
        held.settimeout(3)
        with held.makefile('rb') as f:
            require(json.loads(f.readline())['ok'], 'real fsync after release failed')
    require(any(e['id'] == 'sync-stall' and e['phase'] == 'completed' for e in status()['events']),
            'released sync never completed')

    install('partial-enospc', 'write', 'enospc', cut=3)
    r = io('write', data='DISKFULL', offset=8)
    require(r['n'] == 3 and r['errno'] == errno.ENOSPC, 'missing partial ENOSPC')
    require(data.read_bytes() == b'BASELINEDIS', 'ENOSPC prefix not written to real file')
    fired('partial-enospc')

    install('torn-record', 'write', 'tear', cut=5)
    r = io('write', data='TORN-WRITE-RECORD', offset=11)
    require(r['n'] == 5 and not r['ok'], 'not a partial real write')
    fired('torn-record')
    require(data.read_bytes() == b'BASELINEDISTORN-', 'wrong real partial bytes')

    # Kill a worker blocked in Sync. There must be no completed event or success reply.
    install('kill-stall', 'sync', 'stall')
    with socket.socket(socket.AF_UNIX) as held:
        held.settimeout(3)
        held.connect(str(sock))
        held.sendall(json.dumps(dict(cmd='io', path=str(data), op='sync')).encode() + b'\n')
        fired('kill-stall')
        evidence = status()
        require(not any(e['id'] == 'kill-stall' and e['phase'] == 'completed' for e in evidence['events']),
                'stalled sync completed')
        (work / 'events.json').write_text(json.dumps(evidence, indent=2) + '\n')
        p.kill()
        p.wait(timeout=3)
        require(held.recv(4096) == b'', 'killed sync returned a response')
    require(data.read_bytes() == b'BASELINEDISTORN-', 'SIGKILL changed the observed file image')
    print('PASS: install + fired acknowledgments, real partial bytes, fsync EIO, live control during stall, release, SIGKILL')
    print('NOT PROVEN: power-loss persistence, real kernel EIO/ENOSPC, or tigerstore integration')
finally:
    if p.poll() is None:
        p.kill()
        p.wait(timeout=3)
    sock.unlink(missing_ok=True)
