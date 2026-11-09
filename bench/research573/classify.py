"""classify.py traces [us_per_op]: matrix of layer x kind for a CPU or alloc profile.

Layer: where in the node the work is done (by stack, outermost rule wins in the
order of LAYERS). Kind: what the leaf work is (first matching frame from the
leaf upward, in the order of KINDS)."""
import sys
from collections import defaultdict
sys.path.insert(0, "" + __import__("os").path.dirname(__import__("os").path.abspath(__file__)) + "")
from traces import parse

T = "github.com/djosh34/tigerstore/internal/"

def has(f, *subs):
    return any(any(s in x for s in subs) for x in f)

# Layer rules: evaluated in order; first that matches wins.
def layer(f):
    root = " ".join(f[-4:])
    if has(f, "runtime.gcBgMarkWorker", "runtime.bgsweep", "runtime.bgscavenge"):
        return "GC background (mark, sweep, scavenge)"
    if has(f, "runtime.mcall", "runtime.schedule", "runtime.findRunnable") and not has(f, T):
        return "Go scheduler (idle, park)"
    # client connection
    if has(f, "server.(*conn).readLoop"):
        if has(f, "crypto/tls.(*Conn).Read", "crypto/tls.(*Conn).readRecord"):
            return "client: TLS read + decrypt"
        if has(f, "server.(*conn).admit", "server.(*conn).flush"):
            return "client: request decode, admit, batch"
        return "client: HTTP/2 frames, HPACK, request body"
    if has(f, "server.(*conn).writeLoop"):
        if has(f, "server.(*conn).completeWaiting", "server.(*conn).complete"):
            return "client: reply encode (unary)"
        return "client: TLS encrypt + socket write"
    if has(f, "server.(*Server).rangeStream"):
        if has(f, "node.(*Node).RangeChunk", "node.(*Node).do", "server.(*Server).countRead"):
            return "LIST stream: index walk + values (RangeChunk)"
        return "LIST stream: protobuf encode, gRPC framing"
    if has(f, "server.(*Server).watch", "server.sendFragments"):
        return "watch streams"
    if has(f, "server.(*conn).serve", "server.(*Server).serveConn", "server.(*conn).runStream"):
        return "client: other conn work"
    # committer goroutine
    if has(f, "node.(*Node).commit", "node.(*Node).run.func"):
        if has(f, "node.(*Node).syncGroups", "node.(*Node).persist"):
            return "log: WAL append, write, fsync"
        if has(f, "node.(*Node).readValues"):
            return "cold value reads (disk)"
        if has(f, "core.(*Node).serveRead"):
            if has(f, "mvcc.(*Store).fillValues", "mvcc.(*Store).fill"):
                return "committer: reads - value fetch + CRC"
            if has(f, "mvcc.(*Store).ascend", "mvcc.(*Store).walk", "mvcc.(*Store).get"):
                return "committer: reads - index lookup/walk"
            return "committer: reads - reply build, other"
        if has(f, "core.(*Node).applyEntry", "core.(*Node).apply"):
            if has(f, "core.(*State).track"):
                return "committer: apply - lease key tracking"
            if has(f, "mvcc.(*Store).Txn", "mvcc.(*Store).Put", "mvcc.(*writer)"):
                return "committer: apply - index update"
            return "committer: apply - decode, watches, other"
        if has(f, "core.(*Node).fillCache", "core.(*cache)"):
            return "committer: value cache insert/evict"
        if has(f, "go.etcd.io/raft"):
            return "committer: Raft"
        if has(f, "core.(*Node).Submit", "core.(*Node).SubmitAll", "core.(*Node).submit"):
            return "committer: submit checks, queue"
        if has(f, "peer.(*TCP).Send", "core.(*Node).send"):
            return "committer: hand to peers"
        return "committer: loop, completions, other"
    if has(f, "node.(*Node).syncGroups", "node.(*Node).persist", "wal."):
        return "log: WAL append, write, fsync"
    if has(f, "node.(*Node).readValues"):
        return "cold value reads (disk)"
    if has(f, "peer.(*sender)"):
        if has(f, "crypto/tls", "bufconn.", "bufio.(*Writer).Flush"):
            return "peer send: TLS encrypt + socket write"
        return "peer send: protobuf encode, framing"
    if has(f, "peer.(*TCP).receive", "peer.(*TCP).frames", "peer.(*TCP).accept"):
        if has(f, "crypto/tls", "io.ReadFull", "io.ReadAtLeast"):
            return "peer receive: TLS read + decrypt"
        return "peer receive: protobuf decode, deliver"
    if has(f, "runtime.gcAssist", "runtime.mallocgc", "runtime.gcDrain"):
        return "runtime: other alloc/GC"
    return "other: " + f[-1][:60]

KINDS = [
    ("GC assist", ("runtime.gcAssistAlloc",)),
    ("GC mark/sweep", ("runtime.gcDrain", "runtime.scanobject", "runtime.scanObject", "runtime.greyobject", "runtime.findObject", "runtime.markroot", "runtime.sweepone", "runtime.(*sweepLocked)", "runtime.tryDeferToSpanScan", "runtime.bgscavenge", "runtime.gcBgMarkWorker", "runtime.(*gcWork)", "runtime.scanblock", "runtime.bgsweep")),
    ("write barrier", ("runtime.wbBufFlush", "runtime.bulkBarrierPreWrite", "gcWriteBarrier")),
    ("allocation", ("runtime.mallocgc", "runtime.newobject", "runtime.makeslice", "runtime.(*mcache)", "runtime.(*mcentral)", "runtime.(*mheap)", "runtime.memclrNoHeapPointers", "runtime.memclrHasPointers", "runtime.rawstring", "runtime.rawbyteslice", "runtime.slicebytetostring")),
    ("syscall (incl. kernel time)", ("Syscall6", "syscall.Syscall", "syscall.RawSyscall", "internal/runtime/syscall", "runtime.futex", "runtime.write1", "runtime.read", "runtime.usleep", "runtime.osyield", "runtime.epollwait", "runtime.netpoll", "runtime.nanotime1", "runtime.walltime")),
    ("AES-GCM (TLS crypto)", ("crypto/internal/fips140/aes", "gcmAes", "crypto/aes", "crypto/cipher")),
    ("CRC32", ("hash/crc32",)),
    ("memmove (copy)", ("runtime.memmove", "runtime.typedmemmove", "runtime.typedslicecopy")),
    ("Go map", ("runtime.map", "internal/runtime/maps", "runtime.aeshash", "aeshashbody", "runtime.memhash", "runtime.strhash")),
    ("B-tree / key compare / index", ("github.com/google/btree", "cmpbody", "memeqbody", "runtime.memequal", "bytes.Compare", "mvcc.(*keyTable)", "mvcc.(*keyIndex)", "mvcc.(*Store).key", "mvcc.(*writer).compare")),
    ("protobuf", ("google.golang.org/protobuf", "Marshal", "Unmarshal", "proto.Size", "ProtoReflect", "golang/protobuf")),
    ("HTTP/2, HPACK", ("golang.org/x/net/http2", "hpack")),
    ("chan/select/lock/sched", ("runtime.selectgo", "runtime.chansend", "runtime.chanrecv", "runtime.lock", "runtime.unlock", "sync.(*Mutex)", "runtime.schedule", "runtime.park_m", "runtime.ready", "runtime.goready", "runtime.runqput", "runtime.mcall", "runtime.gopark", "runtime.wakep", "runtime.findRunnable", "runtime.(*waitq)", "sync.(*Cond)", "runtime.semrelease", "runtime.semacquire", "runtime.notewakeup", "runtime.stopm", "runtime.startm", "runtime.exitsyscall", "runtime.entersyscall", "runtime.reentersyscall")),
]

def kind(f):
    for frame in f:  # leaf first
        for name, subs in KINDS:
            if any(s in frame for s in subs):
                return name
    return "other Go code"

if __name__ == "__main__":
    s = parse(sys.argv[1])
    scale = float(sys.argv[2]) if len(sys.argv) > 2 else None
    tot = sum(v for v, _ in s)
    mat = defaultdict(lambda: defaultdict(float))
    lt = defaultdict(float)
    kt = defaultdict(float)
    for v, f in s:
        l, k = layer(f), kind(f)
        mat[l][k] += v
        lt[l] += v
        kt[k] += v
    unit = (lambda v: f"{scale * v / tot:6.2f}") if scale else (lambda v: f"{v:10.0f}")
    print(f"total {tot:.0f}")
    print("\n## by layer")
    for l, v in sorted(lt.items(), key=lambda x: -x[1]):
        top = sorted(mat[l].items(), key=lambda x: -x[1])[:4]
        print(f"{100*v/tot:6.2f}% {unit(v)}  {l}   [" + ", ".join(f"{k} {100*x/v:.0f}%" for k, x in top) + "]")
    print("\n## by kind")
    for k, v in sorted(kt.items(), key=lambda x: -x[1]):
        print(f"{100*v/tot:6.2f}% {unit(v)}  {k}")
