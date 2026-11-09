"""concept.py cpu.traces us_per_op: exclusive split of CPU into the owner's parts.

Each sample goes to exactly one part, by the first rule that matches (order
matters), so the parts add up to 100%."""
import sys
from collections import defaultdict
sys.path.insert(0, "" + __import__("os").path.dirname(__import__("os").path.abspath(__file__)) + "")
from traces import parse
from classify import has

GCBG = ("runtime.gcBgMarkWorker", "runtime.bgsweep", "runtime.bgscavenge")
SYS = ("internal/runtime/syscall", "syscall.Syscall", "syscall.RawSyscall")

def part(f):
    if has(f, *GCBG) or has(f, "runtime.gcAssistAlloc", "runtime.wbBufFlush", "runtime.bulkBarrierPreWrite", "gcWriteBarrier"):
        return "GC (background, assists, write barriers)"
    if has(f, "runtime.mallocgc", "runtime.newobject", "runtime.makeslice", "runtime.growslice", "runtime.rawbyteslice", "runtime.slicebytetostring", "runtime.memclrNoHeapPointers", "runtime.memclrHasPointers"):
        # growslice also copies; counted here as allocation work
        return "allocation (mallocgc, growslice, zeroing)"
    peer = has(f, "peer.(*sender)", "peer.(*TCP)")
    wal = has(f, "node.(*Node).syncGroups", "node.(*Node).persist", "wal.(*WAL)")
    if has(f, *SYS):
        if wal:
            return "syscalls: WAL write + fdatasync"
        if peer:
            return "syscalls: peer sockets"
        if has(f, "server.", "crypto/tls"):
            return "syscalls: client sockets"
        return "syscalls: scheduler, futex, epoll, other"
    if has(f, "crypto/tls", "crypto/internal/fips140/aes", "crypto/cipher"):
        return "TLS record layer + AES-GCM (" + ("peer" if peer else "client") + ")"
    if has(f, "golang.org/x/net/http2", "hpack", "server.(*conn).readLoop", "server.(*conn).onFrame", "server.(*conn).writeLoop", "server.(*conn).newStream", "server.(*stream).push", "server.(*stream).receive", "bufconn.", "bufio."):
        if not has(f, "google.golang.org/protobuf", "server.(*conn).admit", "server.(*conn).complete"):
            return "HTTP/2 framing, HPACK, buffering (client)" if not peer else "peer framing, buffering"
    if has(f, "go.etcd.io/raft"):
        return "Raft library (etcd raft)"
    if has(f, "google.golang.org/protobuf", "proto.Size", "Marshal", "Unmarshal"):
        if peer:
            return "protobuf (peer messages)"
        return "protobuf (client requests/replies, LIST encoding)"
    if has(f, "hash/crc32"):
        if has(f, "mvcc.check", "mvcc.(*Store).value"):
            return "CRC32 of every value read"
        return "CRC32 (WAL, other)"
    if has(f, "core.(*cache)", "core.(*values)", "core.(*Node).fillCache"):
        return "value cache (map, LRU, insert/evict)"
    if has(f, "node.(*Node).readValues", "core.(*ValueRead)"):
        return "cold value reads (mmap copy, faults)"
    if has(f, "core.(*State).track"):
        return "lease key tracking (per-lease B-tree)"
    if has(f, "mvcc."):
        return "index: B-tree, key table, revisions"
    if has(f, "server."):
        return "client request decode, reply encode (own code)"
    if has(f, "peer."):
        return "peer send/receive (own code)"
    if has(f, "core.", "node."):
        return "committer: other own code (apply, submit, watches, loop)"
    if has(f, "runtime.mcall", "runtime.schedule", "runtime.findRunnable", "runtime.park_m"):
        return "Go scheduler"
    return "other"

if __name__ == "__main__":
    for path, us in zip(sys.argv[1::2], sys.argv[2::2]):
        s = parse(path)
        tot = sum(v for v, _ in s)
        c = defaultdict(float)
        for v, f in s:
            c[part(f)] += v
        print(f"### {path}  ({float(us):.1f} us/op)")
        for k, v in sorted(c.items(), key=lambda x: -x[1]):
            print(f"| {k} | {100*v/tot:.1f}% | {float(us)*v/tot:.2f} |")
