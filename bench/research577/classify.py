"""Split a Go CPU profile (go tool pprof -raw) into the parts of the #577 budget.

Each sample's stack (leaf first) is matched by the first rule whose test hits.
Usage: python3 -I classify.py <raw> <leader us/op>
"""
import re
import sys

raw, usop = sys.argv[1], float(sys.argv[2])
lines = open(raw).read().split("\n")
i = lines.index("Locations")
locs = {}
cur = None
for l in lines[i + 1:]:
    if l.startswith("Mappings"):
        break
    m = re.match(r"\s+(\d+): 0x[0-9a-f]+ M=\d+ (\S+)", l)
    if m:
        cur = int(m.group(1))
        locs[cur] = [m.group(2)]
        continue
    m = re.match(r"\s+(\S+) /", l)
    if m and cur is not None:
        locs[cur].append(m.group(1))  # inlined callers, leaf first
samples = []
for l in lines[lines.index("Samples:") + 2:i]:
    m = re.match(r"\s*(\d+)\s+(\d+): (.*)", l)
    if not m:
        continue
    stack = []
    for x in m.group(3).split():
        stack += locs[int(x)]
    samples.append((int(m.group(1)), stack))


def has(stack, *pats):
    return any(any(p in f for p in pats) for f in stack)


def leaf(stack, *pats):
    return any(p in stack[0] for p in pats)


# (part, test) in order; first hit wins.
GC = ("runtime.gcBgMarkWorker", "runtime.gcDrain", "runtime.bgsweep", "runtime.bgscavenge", "runtime.markroot", "runtime.scanobject", "runtime.scanObject", "runtime.gcAssistAlloc", "runtime.sweepone", "runtime.(*sweepLocked)", "runtime.wbBufFlush", "runtime.greyobject", "runtime.findObject", "runtime.tryDeferToSpanScan", "runtime.(*gcWork)", "runtime.gcMarkDone", "runtime.gcStart", "runtime.(*spanSet)", "runtime.(*mspan).moveInlineMarks", "runtime.scanSpan")
ALLOC = ("runtime.mallocgc", "runtime.newobject", "runtime.makeslice", "runtime.growslice", "runtime.(*mcache)", "runtime.(*mcentral)", "runtime.(*mheap)", "runtime.memclrNoHeapPointers", "runtime.rawstring", "runtime.slicebytetostring", "runtime.concatstring", "runtime.makemap")
rules = [
    ("syscall: fdatasync / disk write", lambda s: leaf(s, "Syscall") and has(s, "Fdatasync", "Fsync", "Pwrite", "pwrite", "fdatasync", "internal/wal", "internal/osfs")),
    ("syscall: peer send", lambda s: leaf(s, "Syscall") and has(s, "internal/peer.") and has(s, "syscall.write", "syscall.Write", "writev")),
    ("syscall: peer receive", lambda s: leaf(s, "Syscall") and has(s, "internal/peer.") and has(s, "syscall.read", "syscall.Read")),
    ("syscall: client send", lambda s: leaf(s, "Syscall") and has(s, "internal/server.") and has(s, "syscall.write", "syscall.Write", "writev")),
    ("syscall: client receive", lambda s: leaf(s, "Syscall") and has(s, "internal/server.") and has(s, "syscall.read", "syscall.Read")),
    ("syscall: scheduler (epoll, futex, sleep)", lambda s: leaf(s, "Syscall", "runtime.futex", "runtime.usleep", "runtime.epollwait", "runtime.netpoll", "runtime.osyield", "runtime.nanotime", "runtime.madvise") ),
    ("GC", lambda s: has(s, *GC)),
    ("allocation", lambda s: has(s, *ALLOC)),
    ("scheduler (goroutine handoff)", lambda s: has(s, "runtime.schedule", "runtime.findRunnable", "runtime.park_m", "runtime.goready", "runtime.ready", "runtime.selectgo", "runtime.chansend", "runtime.chanrecv", "runtime.mcall", "runtime.gopark", "runtime.lock2", "runtime.unlock2", "sync.(*Mutex)", "runtime.wakep", "runtime.notewakeup", "runtime.netpollblock", "runtime.mPark", "runtime.stopm", "runtime.startm", "runtime.goschedIfBusy", "runtime.morestack", "runtime.newproc")),
    ("TLS AES-GCM (client)", lambda s: has(s, "crypto/internal/fips140/aes", "ghash", "gcmAes") and has(s, "internal/server.")),
    ("TLS AES-GCM (peer)", lambda s: has(s, "crypto/internal/fips140/aes", "ghash", "gcmAes")),
    ("WAL crc32", lambda s: has(s, "crc32")),
    ("index: B-tree / key table", lambda s: has(s, "google/btree", "mvcc.(*keyIndex)", "mvcc.(*keyTable)", "mvcc.(*Store).key", "cmpbody") ),
    ("value cache (map)", lambda s: has(s, "core.(*cache)", "runtime.mapaccess", "runtime.mapassign", "internal/runtime/maps", "runtime.mapdelete")),
    ("protobuf (generated)", lambda s: has(s, "google.golang.org/protobuf")),
    ("raft library", lambda s: has(s, "go.etcd.io/raft")),
    ("memmove (copies)", lambda s: leaf(s, "runtime.memmove")),
    ("h2 / hpack / gRPC framing", lambda s: has(s, "hpack", "internal/server.(*conn)", "http2")),
    ("other tigerstore", lambda s: has(s, "djosh34/tigerstore")),
    ("other", lambda s: True),
]
tot = sum(c for c, _ in samples)
parts = {}
for c, s in samples:
    for name, test in rules:
        if test(s):
            parts[name] = parts.get(name, 0) + c
            break
print(f"samples {tot}, leader {usop} us/op")
for name, _ in rules:
    c = parts.get(name, 0)
    print(f"| {name} | {100*c/tot:.1f}% | {usop*c/tot:.2f} |")

# memmove by caller, for the copies row
if len(sys.argv) > 3:
    callers = {}
    for c, s in samples:
        if leaf(s, "runtime.memmove") and len(s) > 1:
            k = next((f for f in s[1:] if "runtime." not in f), s[1])
            callers[k] = callers.get(k, 0) + c
    for k, c in sorted(callers.items(), key=lambda x: -x[1])[:15]:
        print(f"memmove from {k}: {usop*c/tot:.2f} us/op")
    sysc = {}
    for c, s in samples:
        if leaf(s, "Syscall"):
            k = " < ".join(f for f in s[1:8] if "syscall" not in f.lower() and "internal/poll" not in f and not f.startswith("net."))[:160]
            sysc[k] = sysc.get(k, 0) + c
    for k, c in sorted(sysc.items(), key=lambda x: -x[1])[:15]:
        print(f"syscall {usop*c/tot:.2f} us/op: {k}")
