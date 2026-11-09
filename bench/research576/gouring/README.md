# #576: Go netpoller vs a bounded, single-owner io_uring loop

Standalone Linux/arm64 research executable; **not a production transport**. No tigerstore imports, no cgo. Uses x/sys/unix and explicit kernel ABI structures. Ordinary `uring` is a copy-send control; `uringzc` is `SEND_ZC` with a registered fixed buffer and usage reporting. Calling a buffer registered does **not** make ordinary SEND use the fixed-buffer optimization.

## Back-of-envelope (before measuring)

Given 36 us CPU/op, 21% in system calls is 7.56 us. Even magically eliminating all that work leaves **28.44 us**, not 10 us. Network syscall CPU (7.4% client writes + 5.0% peer writes + 3.5% reads) is 5.724 us; WAL pwrite+fdatasync is another 1.836 us. io_uring does not eliminate TCP, encryption, persistence, copies on ordinary SEND, or kernel request processing: it batches entry/exit and completion handling. **Guess, not a measurement:** a useful residual networking win after existing 64/256 KiB batching could be low-single-digit us/op, not the required 26 us. Do not double-count the net-to-batch win below: tigerstore already batches.

## Shape

- Server CPU 0; load generator CPU 3. Explicit GOMAXPROCS=2 for the main matrix, GOMAXPROCS=1 comparison for 6 connections/256 B.
- Request 100 B, depth 32 per connection, 6 or 64 connections. Replies 256 B, 4 KiB, 16 KiB.
- `net`: one goroutine per connection, `io.ReadFull(100)`, one Write per reply.
- `batch`: same goroutine model; a Read parses all complete requests, then emits up to 256 KiB per Write, retaining partial framing. All four servers share a single immutable 256 KiB reply region (within each process), avoiding an artificial per-connection payload cache-footprint difference.
- `uring`: one application goroutine locked to its OS thread; SINGLE_ISSUER|DEFER_TASKRUN; multishot accept and multishot recv with a provided buffer ring. After draining CQEs, at most one send per connection is outstanding; short sends are resubmitted. No per-request Go object, channel, map, timer, or goroutine. Shared *immutable* reply bytes mean SEND_ZC notification handling does not need a per-send ownership pool.
- 256 x 16 KiB receive buffers, one 256 KiB immutable transmit region, 4 KiB provided buffer descriptors, 1024 SQEs/2048 CQEs, 128 fixed connection slots, all allocated before accepting. mmap keeps async kernel-visible buffers outside the Go heap. Fixed send region registered once. Kernel/socket memory still allocates dynamically: this is not a claim about kernel allocations.
- Zero-copy sends request REPORT_USAGE and record notification/fallback counts. A successful SEND_ZC does **not** prove no copy; inspect those counters. The buffer may not be mutated until its notification CQE; this prototype never mutates it at all.
- Client opens all connections, warms for 500 ms, drains; start marker snapshots server runtime.MemStats and getrusage; run 2 s, drain; stop marker snapshots again. Count client completed requests and assert they equal server requests. Setup allocations are excluded. Output JSON encoding happens after snapshots.
- `getrusage(RUSAGE_SELF)` sums **all server threads'** user+system CPU. It does not measure client CPU, remote kernel workers, or whole-machine CPU. Wall throughput may be client-limited. No latency/SLO claim.
- Three untraced trials per matrix cell. Separate 500 ms strace -f -c run with warmup disabled, so its request denominator excludes no uncounted warmup workload. Strace includes startup/shutdown syscalls and profoundly perturbs throughput: use its counts, not its timing.

## Reproduce

```sh
cd bench/research576/gouring
GOFLAGS=-p=2 go build -o ../../../gouring576 .
cd ../../..
bash bench/research576/gouring/run.sh
python3 bench/research576/gouring/summarize.py results
```

Actions:

```sh
git push origin research/576-gouring
gh workflow run bench.yml --ref research/576-gouring
# Query no more than once per 2 minutes; never --watch.
gh run view RUN_ID --json status,conclusion,url,jobs
gh run download RUN_ID -n research576gouring -D /tmp/576-results
python3 bench/research576/gouring/summarize.py /tmp/576-results
```

Individual untraced test (wait for READY before client):

```sh
taskset -c 0 env GOMAXPROCS=2 ./gouring576 -mode=uringzc -reply=4096 >server.json &
taskset -c 3 env GOMAXPROCS=1 ./gouring576 -mode=client -reply=4096 -connections=64 -depth=32 -duration=2s
```

## What Go actually does (go1.27.2 primary source)

- [`src/runtime/netpoll_epoll.go:49-54,99-119`](https://github.com/golang/go/blob/go1.27.2/src/runtime/netpoll_epoll.go#L49-L54): edge-triggered epoll readiness, not submission of reads/writes. [`src/internal/poll/fd_unix.go:141-172,360-397`](https://github.com/golang/go/blob/go1.27.2/src/internal/poll/fd_unix.go#L141-L172): read/write syscall; park in poller on EAGAIN; retry after readiness. One successful I/O syscall is still required, plus unsuccessful EAGAIN attempts and epoll/scheduler work.
- [`src/syscall/syscall_linux.go:73-99`](https://github.com/golang/go/blob/go1.27.2/src/syscall/syscall_linux.go#L73-L99): Syscall/Syscall6 call **entersyscall**, not entersyscallblock. x/sys/unix's Linux ARM64 assembly forwards to these. Our blocking `io_uring_enter(GETEVENTS,min_complete=1)` uses this normal runtime-aware wrapper. It makes the G a syscall G and its P `_Psyscall`, preserving the old P for cheap reacquisition. Do not describe this prototype as doing an unconditional P handoff on every enter.
- [`src/runtime/proc.go:4636-4653,4663-4670`](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L4636-L4670): normal syscall disassociates M from P and leaves P available for retaking. [`proc.go:4724-4778`](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L4724-L4778): explicit entersyscallblock instead immediately runs `handoffp(releasep())`. It is a runtime internal, not a supported new transport API.
- [`proc.go:4801-4923,4965-5015`](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L4801-L4923): exitsyscall first tries the old P, then another idle P, otherwise scheduler slow path; a locked G can park its M until rescheduled. [`proc.go:6392-6458`](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L6392-L6458): sysmon retakes syscall Ps (comment says >one tick, at least 20 us), but can leave an otherwise unneeded P there until 10 ms. Thus a blocking enter can cost scheduling/handoff, but not every enter does. No standalone nanosecond cost was measured here.
- LockOSThread preserves **kernel task identity**, exactly what SINGLE_ISSUER/DEFER_TASKRUN needs. It does **not** preserve exclusive ownership of a P, eliminate runtime background threads, or make a blocking enter nonblocking. Waiting when idle is necessary; the goal is no blocking *individual* request or synchronous disk/TLS operation in the event loop.
- Never use RawSyscall6 for an unbounded blocking enter to avoid the runtime transitions: it hides the wait from scheduler/GC and can stall Go, especially at GOMAXPROCS=1. A custom runtime-aware/nonblocking integration could reduce transition overhead, but is not measured.
- [`src/runtime/cgocall.go:143-198`](https://github.com/golang/go/blob/go1.27.2/src/runtime/cgocall.go#L143-L198): cgo also calls entersyscall/exitsyscall, additionally switches through asmcgocall, performs cgo accounting, and imposes pointer/lifetime rules. liburing via cgo is not a free escape from Go scheduling; if used, cross once per batch, not once per SQE/CQE. Numeric cgo overhead is unmeasured here.
- [`src/runtime/debug.go:58-61`](https://github.com/golang/go/blob/go1.27.2/src/runtime/debug.go#L58-L61): default GOMAXPROCS is not forced to >=2 when the **affinity mask itself is one CPU**. It can be >=2 for a one-CPU cgroup quota on a wider affinity mask, or explicit configuration. The supplied node configuration GOMAXPROCS>=2 on one pinned CPU oversubscribes runnable M threads without adding CPU bandwidth; our main matrix explicitly recreates it. Compare the GOMAXPROCS=1 subset, not a generic assertion that 2 always loses.
- [Go issue #31908](https://github.com/golang/go/issues/31908), especially [Ian Lance Taylor on transparency vs app-directed batching](https://github.com/golang/go/issues/31908#issuecomment-2287038308) and [ring coordination](https://github.com/golang/go/issues/31908#issuecomment-490516338): a bounded application can own the ring and its concurrency; transparent arbitrary net.Conn use cannot simply manufacture application batches. The issue is open, and Go 1.27.2's Linux netpoller remains epoll in the cited source.

## Kernel contracts (primary sources)

- [io_uring_setup(2)](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html): SINGLE_ISSUER requires one submitting task; DEFER_TASKRUN requires it and processes deferred task work on entering with GETEVENTS. A locked owner fits. It must keep driving the ring. SQPOLL creates a kernel polling thread: it consumes a spare core or competes on the only node core; not a free reduction of node CPU, and not tested.
- [multishot receive](https://man7.org/linux/man-pages/man3/io_uring_prep_recv_multishot.3.html): zero length, provided buffer group + buffer selection; buffer ID in CQE; MORE missing means rearm; ENOBUFS is handled. [SEND_ZC](https://man7.org/linux/man-pages/man3/io_uring_prep_send_zc.3.html): registered-buffer address range must match; completion and safe-to-reuse notification are distinct; report fallback usage.

## Library source audit (not a formal security audit)

GitHub repository API `pushed_at` fetched 2026-11-09; source pinned below. Do not mistake star/`updated_at` activity for maintenance.

| Library | Last push | Feature/source findings | Trust decision for this experiment |
|---|---|---|---|
| pawelgaczynski/giouring @ `69588b89acb97d043c6846af55515de322a6246b` | 2023-09-24 | Broad liburing-shaped API: SINGLE_ISSUER/DEFER_TASKRUN in `include.go:119-120`, multishot accept/recv, buffer rings, SEND_ZC_FIXED. But [`prepare.go:402-410`](https://github.com/pawelgaczynski/giouring/blob/69588b89acb97d043c6846af55515de322a6246b/prepare.go#L402-L410) passes `unsafe.Pointer(&buf)` (slice **header**, not backing array) for SEND_ZC; fixed helper inherits it. | **Not unmodified.** Closest API coverage, but the exact primitive requested has a source-level pointer bug. Audit/fork and correctness tests before trust. |
| godzie44/go-uring @ `d16a9e5976394f3d0fe256df75c7cb3506fe48ee` | 2025-05-01 | [`uring/operation.go`](https://github.com/godzie44/go-uring/blob/d16a9e5976394f3d0fe256df75c7cb3506fe48ee/uring/operation.go): basic accept/send/recv, older PROVIDE_BUFFERS; no multishot/pbuf-ring/SEND_ZC/defer-taskrun helpers found. Syscall vs RawSyscall selection in `uring/syscall.go:49-80`. | More recent, but missing this experiment's feature surface. Would require extension/audit. |
| iceber/iouring-go @ `002cfd2e2a9021c13192f3bd4d9023374e38c93f` | 2023-11-20 | SEND_ZC opcode constant exists; no corresponding high-level helper/multishot/pbuf-ring/defer-taskrun support found. [`iouring.go:47-88,152-186`](https://github.com/iceber/iouring-go/blob/002cfd2e2a9021c13192f3bd4d9023374e38c93f/iouring.go#L47-L88), [`poller.go`](https://github.com/iceber/iouring-go/blob/002cfd2e2a9021c13192f3bd4d9023374e38c93f/poller.go): maps, channels, background completion goroutine and shared epoll/eventfd poller. | Wrong shape for the requested single-owner allocation-free loop, even though useful for a general async API. |

**I would trust none of these unmodified for tigerstore's transport.** For this small feature set, a tightly scoped raw wrapper with layout tests, kernel matrix CI, fuzzed framing, lifecycle/cancellation tests and ownership assertions is more auditable; this prototype is only the first measurement, not that production wrapper.

## Integration cost and exclusions

This benchmark intentionally has no HTTP/2, TLS, Raft, WAL, compression or real reply construction. A static repeated plaintext payload is a favorable transport-only case, not a mixed-k8s simulation. No TLS measurements were made.

Go crypto/tls exposes a blocking net.Conn-style API, not a public record-sealing-into-caller-buffer function. [`crypto/tls/conn.go:975-1040`](https://github.com/golang/go/blob/go1.27.2/src/crypto/tls/conn.go#L975-L1040) gets internal record scratch from a pool, encrypts into it and calls the underlying Write. Merely replacing the underlying Conn does not let the event loop seal into a registered buffer: either copy ciphertext into an owned registered region (extra copy and keep TLS goroutines), or change/fork the TLS record layer and explicitly own handshake/key-update/sequence-number/AEAD state and notification lifetimes. A Go TLS/net.Conn facade over a ring can remain a useful incremental step, but cannot claim TigerStyle scheduling or zero copies. Forking this security-sensitive layer is a separate decision, not a weekend syscall substitution.

Production requirements omitted here: nonconstant replies and a bounded per-connection/per-send slab retained until zero-copy notification; async TLS and H2 parsing/flow control/handshakes; all connection limits and overload behavior; cancellation and fd-reuse generations; asynchronous close; timeout wheel; ENOBUFS/backpressure and CQ overflow recovery; ring teardown; capability checks/fallback; disk durability semantics. The framing parser retains partial opcodes as well as byte counts. No dynamic clients are admitted after 128 lifetime accepts. Buffer IDs are recycled only after synchronous parsing, before the next kernel entry. Registered transmit bytes are never changed. The measured successful paths allocate no per-request Go objects; kernel allocations and runtime threads are not eliminated.
