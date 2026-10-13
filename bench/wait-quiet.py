#!/usr/bin/env python3
"""Wait for five consecutive one-second samples without another heavy process."""
import glob
import time

def snapshot():
    procs = {}
    for path in glob.glob('/proc/[0-9]*/stat'):
        try:
            text = open(path).read()
            fields = text.rsplit(')', 1)[1].split()
            procs[path] = int(fields[11]) + int(fields[12])
        except (OSError, ValueError, IndexError):
            pass
    fields = list(map(int, open('/proc/stat').readline().split()[1:9]))
    return procs, sum(fields[i] for i in [0, 1, 2, 5, 6])

before, busy = snapshot()
quiet = 0
while quiet < 5:
    if time.time() >= 1791928800:
        raise SystemExit('Deadline reached')
    time.sleep(1)
    after, nextbusy = snapshot()
    heavy = max((after[p] - before.get(p, after[p]) for p in after), default=0)
    # HZ=100. Background must average less than 0.8 core in total,
    # with no individual process consuming more than 0.4 core.
    quiet = quiet + 1 if nextbusy - busy < 80 and heavy < 40 else 0
    before, busy = after, nextbusy
print('Host quiet for 5 s', time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), flush=True)
