# Research #576: primitive harness (not production code)

Run on Actions, not on the shared development host. The workflow replaces the
normal bench workflow with a single ARM research job. It deliberately does not
run `mixed.sh` (the parent researcher owns mixed-workload measurements).

```
gh workflow run bench.yml --ref research/576-primitives
# Do not poll faster than once per 2 minutes; do not use --watch.
gh run view RUN_ID
gh run download RUN_ID --dir /tmp/research576-results
python3 bench/research576/summarize.py /tmp/research576-results/research576 > /tmp/summary576.csv
```

## Exact measurement entry points

The build, source revisions, and crypto commands are in `.github/workflows/bench.yml`.
`bash bench/research576/run.sh` executes the entire C matrix three times, with each
invocation pinned by `taskset -c 0`. Each case is an independent process, so an
unsupported primitive does not kill unrelated measurements. A 45-second alarm
bounds a stalled case. `FAILED` is a real failed/unsupported case, **not a zero**.

```
taskset -c 0 bin/io576 config
taskset -c 0 bin/io576 net SIZE MODE BATCH FLAGS TLS RX unused
taskset -c 0 bin/io576 disk "$RUNNER_TEMP/io576" SIZE MODE DIRECT
taskset -c 0 bin/aes576 evp
taskset -c 0 bin/aes576 afalg
```

Network: 32 distinct loopback TCP connections, TCP_NODELAY, sender CPU 0,
receiver process CPU 1. `BATCH=1` rotates through all 32 sockets; `BATCH=32`
submits one message on each socket. Only writev32/bigwrite32 combine 32 messages
**on the same socket**. These are distinct optimizations; combining 32 replies
requires them to be available on the same connection and is a throughput
upper-bound, not a claim that a request/reply service can do it at no latency cost.

- Network modes: 0 write; 1 writev(32 messages); 2 write(32 messages);
  3 io_uring SEND; 4 SEND with IORING_RECVSEND_FIXED_BUF (explicitly test whether
  the kernel permits it); 5 SEND_ZC; 6 SEND_ZC with a registered buffer;
  7 MSG_ZEROCOPY; 8 sendfile (hot memfd/page cache, no storage read);
  9 splice(file->pipe->TCP), including pipe population syscalls;
  10 OpenSSL EVP AES-128-GCM + write (tag included; **receiver does not decrypt**,
  so compare sender CPU, not receiver CPU, against kTLS).
- Flags: 0 default; 1 SINGLE_ISSUER|DEFER_TASKRUN|COOP_TASKRUN;
  2 SQPOLL|SQ_AFF, poller pinned CPU 2 (a spare core is required).
- TLS: 1 sets both TX and RX to TLS 1.3 AES-128-GCM, dummy all-zero keys and
  initial sequences. No handshake is timed. kTLS itself performs record framing.
  EVP+write encrypts an entire logical message, not TLS wire framing, so it is
  not a drop-in user-space TLS implementation and its 64 KiB record is not a
  valid single TLS record. kTLS splits at its record limit.
- RX: 0 poll + read capped to message size; 1 poll + read up to 64 KiB;
  2 provided-buffer ring (256 x 64 KiB) plus multishot recv on all 32 sockets.
  TCP read boundaries need not match logical messages. Report CPU / sent logical
  message and CQEs / message; there is no claim of preserved application framing.
- Count: at least 16 MiB / case, at least 4096 logical messages (65536 at 256 B).
  The same immutable source buffer is used throughout. Short sends are retried.
  SEND_ZC notification completion is pipelined, not waited after each batch;
  every notification is drained before source lifetime ends. MSG_ZEROCOPY reads
  the error queue, accounts completion ranges and COPIED flags, and includes
  that cost in sender CPU and call counts.

Network `tx_ns` and `rx_ns` use CLOCK_PROCESS_CPUTIME_ID independently in the
sender/receiver processes. `issuer_ns` additionally uses CLOCK_THREAD_CPUTIME_ID.
Thus io-wq and SQPOLL CPU in the same thread group is not hidden: io-wq/SQPOLL
workers use create_io_thread with CLONE_THREAD in kernel v7.0. The sender's
loopback TCP call also executes a substantial part of the peer's network stack
in sender context. **These are not NIC transmission CPU measurements.**

Network `wall_ns` is send/enqueue time, not end-to-end delivery time. Sender CPU
stops before close; receiver CPU continues until all bytes and EOF have been
read. Especially for large write batching, the sender may finish before the
receiver drains. Use both per-side CPU columns and their sum when comparing
network-wide CPU. Do not infer delivered GB/s from sender wall time.

Calls count data-path send/read/poll/submit/wait APIs, not setup/teardown.
Non-SQPOLL sends have one submit syscall per submission plus any completion
wait syscalls. SQPOLL's `tx_calls` counts submit API invocations even if they do
not enter the kernel: **use the separate strace run to get its actual syscall
count**. Trace runs have SHORT=1 (1024 logical messages), are not used for CPU
or latency measurements, and record per-process syscall logs plus process IDs.
On arm64, libc poll becomes ppoll. Poll/read overhead is part of RX CPU.

Disk: root ext4 in RUNNER_TEMP, barrier=1 remount, preallocated 64 MiB file,
128 sequential groups per case (no wrap), one size-byte write per group then
DATASYNC. Modes: 0 pwrite+fdatasync; 1 linked io_uring WRITE+FSYNC(DATASYNC);
2 registered WRITE_FIXED + linked FSYNC; 3 same plus registered file descriptor.
A single submit_and_wait(2) submits and waits for the linked pair. DIRECT adds
O_DIRECT to each mode (4 KiB alignment). `cpu_ns` is process CPU, `issuer_ns`
only the main thread, `wall_ns` average wall latency per group; no percentile
latencies or hardware power-loss durability tests are claimed. All CPU figures
include syscall execution, but kernel work outside the process/thread group is
not charged to it.

AES: aes.c measures process CPU with a reused EVP context/key, per-message nonce,
13-byte AAD and a tag; AF_ALG does the same through the kernel's gcm(aes) with
sendmsg+read (2 syscalls). Go benchmarks have nil AAD, no allocations and reuse
key/context/nonce. Speed tools are also run; these have their own harnesses and
wall/CPU conventions. Treat library speed-tool numbers as throughputs, not as
CLOCK_PROCESS_CPUTIME_ID samples.

AWS-LC default dispatch and an **experimental patched 8x dispatch** are compared.
`OPENSSL_armcap='|0x1000'` was first tried but AWS-LC correctly rejected adding
an undetected V1 capability, so its empty output files are not measurements.
The follow-up instead patches the GCM-only 8x dispatch predicate to accept this
SHA3-capable N2, rebuilds, and archives the exact source patch. No hardware
capabilities are overridden. This is not AWS-LC's default on N2 and not a
production configuration recommendation. aes.c authenticates a round trip at
each size outside the timed loop. See cpucap/internal.h and modes/gcm.c at the
pinned revision. `gh workflow run bench.yml --ref research/576-primitives -f
followup=true` runs just AF_ALG/config diagnostics, kTLS sendfile with/without
TLS_TX_ZEROCOPY_RO, and default/patched AWS-LC crypto. An additional `-f kernel_only=true` skips
AWS-LC and explicitly loads algif_aead with modprobe --ignore-install on this
disposable runner; the distro blocks that user-space crypto test API by default
with an install /bin/false directive. This does not describe production policy
or a requirement for kTLS (which uses kernel-internal AEAD and already works).
