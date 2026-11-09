# #576 primitive research — report to parent, not an issue comment

## Short answer and back-of-the-envelope

The supplied leader profile puts 21% of 36 µs/op in the named syscall paths:
client writes 2.664 µs, peer writes 1.800 µs, reads 1.260 µs, pwrite 1.008 µs,
fdatasync 0.828 µs; sum 7.560 µs/op. Even deleting **all** that work leaves
28.44 µs/op, not 10. In reality io_uring eliminates neither TCP nor copying nor
AES nor ext4 nor persistence work; a getpid boundary itself costs ~200 ns.
That arithmetic is an optimistic bound based on the supplied profile, **not a
measured application speedup**.

Measured: ordinary io_uring SEND batched across 32 connections saves about
0.20/0.27/0.43/0.24/0.45 µs sender CPU per message versus write at
256 B/1/4/16/64 KiB. Batch-1 io_uring is usually slower. Combining 32 messages
into a large write/writev saves much more, but it is **same-connection
aggregation**, not 32 ready replies on 32 sockets; availability/latency is a real
constraint. The bulk test is an upper bound on that opportunity. For an
illustrative three small sends per mixed op, the measured 0.2–0.4 µs/send is
only ~0.6–1.2 µs/op (**guess**, not a mixed-path measurement). The parent must
multiply deltas by its measured calls, bytes, and group-commit sizes.

MSG_ZEROCOPY and SEND_ZC lose on loopback: the notifications report copying.
SEND_ZC fixed buffers modestly reduce SEND_ZC overhead, not enough to beat
ordinary sends. Software kTLS works but is not a faster send pipeline here;
software kTLS rejects MSG_ZEROCOPY/SEND_ZC. sendfile saves CPU for large plain
TCP payloads, but its advantage disappears under software kTLS. True kTLS TX
zero-copy via TLS_TX_ZEROCOPY_RO is a **NIC/device-offload-only** feature, not
what this loopback runner exercises. Setting that option successfully is not
proof of offload.

Disk io_uring cuts issuer-thread CPU by moving buffered write/fsync work to
io-wq, but measured **process CPU including those workers increases**. This
must not be called a CPU saving. Linked write+fsync uses one enter vs two sync
syscalls; O_DIRECT and registered buffers help at 256 KiB, not small buffered
WAL groups. Latencies have substantial cloud-device variation.

## Provenance

- First repaired matrix (diagnostic, obsolete disk wait/zero-copy scheduling):
  https://github.com/djosh34/tigerstore/actions/runs/42696208705 — commit
  `76594b4d`; AWS speed tool target was not built because BUILD_LIBSSL=OFF.
- Main matrix used below:
  https://github.com/djosh34/tigerstore/actions/runs/42697256143 — commit
  `3bdba139`, branch `research/576-primitives`.
- Follow-up crypto/config and TLS_TX_ZEROCOPY_RO experiment:
  https://github.com/djosh34/tigerstore/actions/runs/42698811772 — commit
  `4fc7d736` on the same branch.
- AF_ALG explicit module-load measurement:
  https://github.com/djosh34/tigerstore/actions/runs/42699545613 — `8c53321f`.
- Final disk, including explicit CPU-0 io-wq affinity:
  https://github.com/djosh34/tigerstore/actions/runs/42700129694 — `9fc597db`.
- Each microbench runs pinned, three repetitions. Tables report median
  **[minimum–maximum]** across those three samples, not confidence intervals.
- C CPU measurements use CLOCK_PROCESS_CPUTIME_ID, independently for sender
  and receiver. Issuer-only columns use CLOCK_THREAD_CPUTIME_ID. io-wq/SQPOLL
  use CLONE_THREAD ([kernel v7.0 fork.c, lines 2589–2602](https://github.com/torvalds/linux/blob/v7.0/kernel/fork.c#L2589-L2602)),
  so their CPU is included in process totals. Unrelated kernel threads/interrupt
  work outside the thread group is not. Network tables report logical sent
  messages, not TCP packets or CQEs.
- Sender CPU 0, receiver process CPU 1; SQPOLL CPU 2. TCP_NODELAY, 32
  connections, immutable hot buffers. Loopback runs much of the receiver's TCP
  path in **sender context**; do not transplant these CPU numbers to a real NIC.
  Sender wall time ends at enqueue, not delivery; receiver CPU runs through
  full drain and EOF. Report both sides rather than claiming enqueue GB/s.
- Disk: RUNNER_TEMP on root ext4, `mount -o remount,barrier=1 /`;
  `data=writeback,journal_async_commit,commit=30` also present. Preallocated
  64 MiB file, 128 sequential groups, one group-sized write then DATASYNC;
  aligned O_DIRECT cases. No persistence-failure test or latency percentiles.
- See `README.md` for modes, timer boundaries, buffering caveats, and exact
  commands. See workflow for build commands and pinned dependencies.

## Why the original attempt died

Not SQ size. Its log has `completion e=0 res=759 expected=1024`, and another
case `res=3584 expected=16384`. Those are legal short TCP sends. The original
[`complete()` at e6769800:io.c:27](https://github.com/djosh34/tigerstore/blob/e6769800/bench/research576/io.c#L27)
wrongly required full length. A 512-entry SQ is enough for its batch of 32.
The original `... | tee` without pipefail hid the exit, so the workflow passed
while repetitions aborted. The repaired harness retries short completions,
labels failures explicitly, and runs each case in a separate process.

## Kernel and library facts

Measured runner: 4 x Neoverse-N2, `7.0.0-1017-azure`, Go 1.27.2,
OpenSSL 3.0.13. `/proc/sys/kernel/io_uring_disabled=0`, seccomp=0,
memlock soft limit 8192 KiB. io_uring init succeeded unprivileged, SQ=512,
CQ=1024, features=`0x3ffff`: all feature bits 0–17 in this liburing revision,
including FAST_POLL, SQPOLL_NONFIXED, NATIVE_WORKERS, RSRC_TAGS,
RECVSEND_BUNDLE, RW_ATTR and NO_IOWAIT. Probe reports opcodes 0–64 supported.
liburing is source version 2.16 at
`c63f4aa6f930bed36589b4c738a9da5a6cd564bd`; installed and statically linked by
the workflow. A container's seccomp/memlock policy may differ (unmeasured).

`/proc/crypto` advertises `gcm(aes)` as `gcm-aes-ce`, priority 300,
synchronous AEAD, module `ghash_ce`. `/proc/net/tls_stat` counts software TX/RX
sessions and zero device-offload sessions, zero decrypt errors. Dummy TLS 1.3
AES-128-GCM keys are installed on both endpoints, no handshake timed.

Ordinary IORING_OP_SEND + IORING_RECVSEND_FIXED_BUF returns **-EINVAL** on this
kernel. That is expected: its SEND preparation accepts a flag mask without
FIXED_BUF ([v7.0 io_uring/net.c:416–432](https://github.com/torvalds/linux/blob/v7.0/io_uring/net.c#L416-L432)).
SEND_ZC_FIXED is the measured registered-buffer send variant. Software kTLS
MSG_ZEROCOPY/SEND_ZC return **EOPNOTSUPP/-EOPNOTSUPP**, matching the explicit
allowed sendmsg flag mask ([tls_sw.c:1278–1287](https://github.com/torvalds/linux/blob/v7.0/net/tls/tls_sw.c#L1278-L1287)).
Unsupported rows are absent, never reported as zero cost.

## Small-operation floor

| Primitive | CPU ns/op, median [min–max] | Data-path syscalls/op |
|---|---:|---:|
| getpid syscall | 199.56 [199.39–199.66] | 1 |
| io_uring NOP, batch 1 | 308.62 [294.17–308.81] | 1 |
| io_uring NOP, batch 8 | 69.22 [67.91–71.08] | 0.125 |
| io_uring NOP, batch 32 | 43.03 [42.65–43.61] | 0.03125 |

## Full data and interpretation

- [TABLES.md](TABLES.md): all main-matrix network sender/receiver CPU, receiver
  variants, taskrun/SQPOLL, TLS, all original disk modes, per-side syscall counts,
  crypto CPU and throughput; every cell has a three-run spread.
- [FOLLOWUP.md](FOLLOWUP.md): default and patched eight-way AWS-LC CPU, speed-tool
  checks, kTLS sendfile with/without the device-only zero-copy option, and AF_ALG.
- [DISK_ONE_CORE.md](DISK_ONE_CORE.md): final one-core disk CPU, issuer CPU,
  latency and spread, all 32 size/mode/direct combinations. Buffered 4 KiB
  pwrite+fdatasync is 16.21 [15.88–18.39] µs process CPU/group vs linked ring
  20.73 [20.62–22.35]; issuer-only 16.19 vs 2.62 µs is not a total saving.
  O_DIRECT+registered 256 KiB is 19.92 [19.76–21.83] µs vs direct pwrite
  27.85 [26.85–30.23], but direct pwrite already beats buffered pwrite's
  59.98 [58.93–64.05]. Registering files adds no consistent CPU benefit.
- The final one-core disk follow-up is separate: the main matrix's taskset pins
  the issuer but **kernel io-wq defaults to the cpuset mask**, not the issuer's
  affinity. The final disk harness explicitly registers CPU-0 io-wq affinity;
  use that result for one-core comparisons. Kernel source:
  [io-wq.c:1267–1270](https://github.com/torvalds/linux/blob/v7.0/io_uring/io-wq.c#L1267-L1270)
  and [io-wq.c:787–791](https://github.com/torvalds/linux/blob/v7.0/io_uring/io-wq.c#L787-L791).

The 1 KiB main-matrix traces validate data-path syscall counts independently of
the C counters (1024 logical messages; **traced timings are not used**):

| Sender path | Relevant actual syscalls | Calls/message |
|---|---:|---:|
| write | 1024 writes | 1 |
| writev32 | 32 writev | 0.03125 |
| io_uring SEND batch1 | 1024 enter | 1 |
| io_uring SEND batch32 | 32 enter | 0.03125 |
| SEND_ZC batch32 | 86 enter, including notification waits | 0.08398 |
| MSG_ZEROCOPY | 1024 sendto + 277 recvmsg | 1.27051 |
| SQPOLL batch32 | 33 enter, mostly completion waits | 0.03223 |

Setup/teardown/log writes are excluded. The trace's notification coalescing
changes under strace, hence MSG_ZEROCOPY differs from untraced counts. RX
read cases have 1024 data reads + 32 EOF reads and poll calls; multishot uses
zero data reads and 18 total enter calls (including initial registration of the
32 recv requests) in the default trace, 40 with deferred taskrun. The original
trace filter did not include arm64 ppoll, so use the C poll counters for that
component; the script now includes ppoll. SQPOLL `tx_calls` in the raw C logs
counts submit **API invocations**, not only enters, so its printed 0.0625 is an
upper bound, not its actual syscall rate. The separate trace above fixes that.

At 1 KiB, RX CPU is 3140 [3133–3148] ns/message for poll+read versus 3412
[3392–3468] for multishot, despite the latter's large syscall reduction. At
64 KiB, multishot helps modestly: 9858 [9852–9874] versus read 10346
[10000–10356] ns/message. Buffers are still copied into application memory;
this is multishot completion/provided-buffer optimization, not zero-copy RX.
Single/defer/coop flags do not consistently improve TX here. SQPOLL at 1 KiB
reduces issuer-thread CPU to 87 [86–95] ns/message but costs 3480 [3457–3481]
ns **including its polling thread**, versus 3141 [3131–3143] without SQPOLL;
it requires a spare CPU and is not a free single-core speedup.

All MSG_ZEROCOPY completion IDs reported COPIED, and every SEND_ZC notification
reported IORING_NOTIF_USAGE_ZC_COPIED. Registered SEND_ZC cuts some pin/accounting
cost but still loses to ordinary SEND. A hot 64 KiB sendfile reduces sender CPU
from write's 11273 to 8126 ns (~28%); splice is 8416 ns. At 1 KiB both lose.
This omits the cost of originally placing/producing the data in the file. Under
kTLS, 64 KiB sendfile is 43290 ns versus kTLS write's 41760 ns: no saving.

kTLS TX CPU per plaintext byte, **including the full TCP/TLS send path**, is
16.992/4.647/1.544/0.721/0.637 ns/B at 256 B/1/4/16/64 KiB. See the per-message
spreads in TABLES.md. Compared with the OpenSSL EVP+plain-send harness, kTLS is
slower at every size tested. That EVP receiver does not decrypt; only sender
CPU is a fair comparison. It is also a simple GCM loop, not full TLS framing;
at 64 KiB kTLS must split records whereas that harness does not.

Go copy throughput provides a sanity check on achievable copy savings:
1 KiB copy 21.67 [21.67–21.69] ns, 16 KiB 311.1 [311.1–312.0] ns,
64 KiB 1242 [1241–1244] ns; these are single-core Go benchmark wall ns/op,
not C process-clock samples. A copy of one small reply is tens of ns, not µs.
A per-message syscall/TCP/TLS-record reduction is a different, larger saving.

Limits: this is steady-state primitive CPU, not an application integration.
Ring/buffer/file registration, socket setup and TLS handshakes are outside the
timed loops; their one-time cost was not measured. Disk has one durable group
in flight, not a high-queue-depth storage engine. Remote NIC/offload behavior,
container-specific seccomp, application buffering latency, Go cgo/GC/pinning
costs and actual mixed-workload speedups remain unmeasured. No end-to-end
36→10 µs claim follows from these tests.

## Primary-source constraints and implementation costs

1. MSG_ZEROCOPY exchanges copying for page accounting and completion work and
   is generally effective only above ~10 KiB; the buffer must stay immutable
   until notification. Local looped packets incur a deferred copy, including
   local/veth test arrangements:
   [kernel v7.0 msg_zerocopy.rst:18–30](https://github.com/torvalds/linux/blob/v7.0/Documentation/networking/msg_zerocopy.rst#L18-L30),
   [208–246](https://github.com/torvalds/linux/blob/v7.0/Documentation/networking/msg_zerocopy.rst#L208-L246).
   Real remote NIC behavior was **not measured**; these negative results apply
   to this mixed benchmark's local path, not a blanket rejection for networks.
2. SEND_ZC normally produces send CQE plus a later lifetime notification CQE;
   F_MORE/F_NOTIF and REPORT_USAGE must be handled, including short sends:
   [pinned liburing io_uring_prep_send_zc.3](https://github.com/axboe/liburing/blob/c63f4aa6f930bed36589b4c738a9da5a6cd564bd/man/io_uring_prep_send_zc.3).
   It costs completion state, buffer retention, and backpressure logic, not just
   replacing one syscall spelling.
3. Registered buffers pin memory and charge RLIMIT_MEMLOCK:
   [io_uring_register.2:44–75](https://github.com/axboe/liburing/blob/c63f4aa6f930bed36589b4c738a9da5a6cd564bd/man/io_uring_register.2#L44-L75).
   Fixed files add a registered descriptor table. The N2 runner allows these,
   but security/memlock/container policy still needs a deployment-specific test.
4. Multishot requires provided buffers and F_MORE/BUFFER handling; buffer-ring
   ownership and rearming/ENOBUFS are additional state:
   [io_uring_prep_recv_multishot.3](https://github.com/axboe/liburing/blob/c63f4aa6f930bed36589b4c738a9da5a6cd564bd/man/io_uring_prep_recv_multishot.3),
   [io_uring_setup_buf_ring.3](https://github.com/axboe/liburing/blob/c63f4aa6f930bed36589b4c738a9da5a6cd564bd/man/io_uring_setup_buf_ring.3).
5. SINGLE_ISSUER and DEFER_TASKRUN impose same-thread submission/enter and
   periodic taskwork progress; SQPOLL creates a polling thread:
   [io_uring_setup.2](https://github.com/axboe/liburing/blob/c63f4aa6f930bed36589b4c738a9da5a6cd564bd/man/io_uring_setup.2).
   These are not automatic drop-ins for a migrating goroutine/netpoller.
   Go integration itself is the parent's separate research, not benchmarked here.
6. kTLS encrypts after TLS_TX setup, creates a record per send unless MSG_MORE
   or a max-size record closes it, and supports sendfile from plaintext files:
   [tls.rst:65–100](https://github.com/torvalds/linux/blob/v7.0/Documentation/networking/tls.rst#L65-L100).
   TLS_TX_ZEROCOPY_RO's true zero-copy is **device-only**:
   [254–268](https://github.com/torvalds/linux/blob/v7.0/Documentation/networking/tls.rst#L254-L268).
   User-space TLS cannot simply sendfile plaintext around encryption. In
   contrast, MSG_ZEROCOPY of **already-encrypted user-space TLS buffers** is an
   architectural possibility without kTLS, if their lifetime is retained;
   that combination/Go integration was **not measured** here.
7. Kernel gcm-aes-ce is AES/PMULL-accelerated, not scalar fallback:
   [ghash-ce-glue.c:285–333](https://github.com/torvalds/linux/blob/v7.0/arch/arm64/crypto/ghash-ce-glue.c#L285-L333),
   [476–495](https://github.com/torvalds/linux/blob/v7.0/arch/arm64/crypto/ghash-ce-glue.c#L476-L495).
   Go already uses AES/PMULL too:
   [Go 1.27.2 gcm_arm64.s](https://github.com/golang/go/blob/go1.27.2/src/crypto/internal/fips140/aes/gcm/gcm_arm64.s).
   Moving AES into the kernel does not introduce hardware acceleration that Go
   lacked. See FOLLOWUP.md for the modest best-measured eight-way advantage.

## Exact commands / reproduction

The workflow is the executable build recipe, pinned to dependency commits; no
local heavy benchmark runs were performed. Actual dispatches:

```bash
gh workflow run bench.yml --ref research/576-primitives
# Main matrix: 42697256143, source 3bdba139
gh workflow run bench.yml --ref research/576-primitives -f followup=true
# AWS eight-way + sendfile opt-in: 42698811772, source 4fc7d736
gh workflow run bench.yml --ref research/576-primitives -f followup=true -f kernel_only=true
# AF_ALG explicit load: 42699545613, source 8c53321f
gh workflow run bench.yml --ref research/576-primitives -f followup=true -f kernel_only=true -f disk_only=true
# Final constrained disk: 42700129694, source 9fc597db
# Check status manually, no faster than every 2 minutes; never --watch.
gh run download 42697256143 --dir /tmp/research576-final
gh run download 42698811772 --dir /tmp/research576-followup
gh run download 42699545613 --dir /tmp/research576-afalg
gh run download 42700129694 --dir /tmp/research576-disk-onecore
python3 bench/research576/summarize.py /tmp/research576-final/research576 > /tmp/summary576-final.csv
```

On runner, whole matrix `bash bench/research576/run.sh`. Individual examples:

```bash
taskset -c 0 bin/io576 net 1024 0 1 0 0 0 unused    # write
taskset -c 0 bin/io576 net 1024 1 1 0 0 0 unused    # writev32
taskset -c 0 bin/io576 net 1024 3 32 0 0 0 unused   # 32 SEND SQEs
taskset -c 0 bin/io576 net 1024 6 32 0 0 0 unused   # fixed SEND_ZC
taskset -c 0 bin/io576 net 1024 3 32 0 0 2 unused   # multishot receiver
taskset -c 0 bin/io576 net 16384 8 1 0 2 0 unused   # kTLS sendfile + RO option
taskset -c 0 bin/io576 disk "$RUNNER_TEMP/io576" 4096 3 0
taskset -c 0 bin/aes576 evp
taskset -c 0 bin/aes576 afalg
taskset -c 0 bin/crypto576.test -test.run '^$' -test.bench . -test.benchtime 500ms -test.count 3 -test.cpu 1
taskset -c 0 openssl speed -seconds 1 -bytes 16384 -evp aes-128-gcm -aead
taskset -c 0 "$RUNNER_TEMP/aws-lc/build/tool/bssl" speed -filter EVP-AES-128-GCM,EVP-AES-256-GCM -chunks 1024,16384 -timeout 1
```

The shell matrix and workflow repeat the relevant commands three times and
include all other sizes/modes. No issue/PR comments, PR creation, or main/work
branch pushes were performed; all pushes are to research/576-primitives.

