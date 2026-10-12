## Pre-work for #49

Recommendation: use scoped netem for packet faults, a transparent seeded byte proxy for stream faults, systemd CPU quotas, and a test-only wrapper around the real disk adapter. Add a bounded ext4 filesystem for genuine ENOSPC. FUSE and device-mapper targets work on the free runners, but neither is needed for the first file-fault implementation.

[Throwaway branch](https://github.com/djosh34/tigerstore/tree/exp/49-real-process-chaos/experiments/chaos49). Nothing is intended for merge. I read #46, #1, #49, the linked testing decisions and `docs/design.md` on main. Inspected main was `91c4f974a6661ac02c2e7747781ea215353c643b`, which had a skeleton executable. These are mechanism tests, not tests of a working tigerstore cluster.

### Runner evidence and versions

[Final runner run](https://github.com/djosh34/tigerstore/actions/runs/38316542472), commit `6d1bf2344d3d961c83c87ca0d7f1fa38065659da`, passed all 13 jobs. It ran network, process, FUSE, DM, tmpfs and loop-ext4 probes on both `ubuntu-24.04-arm` and `ubuntu-24.04`, plus the tagged Go file wrapper on ARM. Jobs have four- or five-minute limits. [Raw output is committed](https://github.com/djosh34/tigerstore/tree/exp/49-real-process-chaos/experiments/chaos49/results/runner-38316542472), not left only in expiring artifacts.

Both architectures used Linux `6.17.0-1022-azure`. ARM image `20261004.142.1`; AMD64 image `20261004.327.1`. Packages were iproute2 `6.1.0-1ubuntu6.4`, Python `3.12.3-0ubuntu2.1`, systemd `255.4-1ubuntu8.17`, fuse3/libfuse3-dev `3.14.0-5build1`, dmsetup `2:1.02.185-3ubuntu3.2`, util-linux `2.39.3-9ubuntu6.6`, and e2fsprogs `1.47.0-2.4~exp1ubuntu4.1`. The wrapper used Go `1.27.2`.

### 1. Scoped loopback netem

Proven on both architectures, inside `sudo unshare --net`. Bring up that namespace's loopback; distinct `127/8` addresses work without adding addresses. Bind outgoing peer sockets to their node address. A listener address alone does not select the outgoing source address.

The tested configuration puts unmatched traffic in `1:1` and only peer traffic in netem class `1:3`:

```sh
tc qdisc add dev lo root handle 1: prio bands 3 \
  priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
tc qdisc add dev lo parent 1:3 handle 30: netem limit 10000 delay 5ms
tc filter add dev lo parent 1: protocol ip pref 1 flower \
  ip_proto tcp src_ip 127.77.0.1 dst_ip 127.77.0.2 \
  dst_port 2380 classid 1:3
```

Add the reverse address pair with `src_port 2380` to delay replies. The probe adds equivalent UDP filters. For a full mesh where either node initiates a connection, cover both peer source-port and destination-port cases for each directed address pair. Use explicit IPv4 endpoints or add and verify IPv6 filters. Keep client/control addresses and ports outside these selectors. Run the whole local cluster/controller inside the private namespace; do not put an unfiltered netem root on the host loopback.

Each case sent 300 UDP packets of 1,000 bytes, spaced by 2 ms. TCP RTT uses 20 echo exchanges. Final-run observations:

| Fault | ARM64 | AMD64 |
| --- | --- | --- |
| Baseline peer TCP median RTT | 0.024 ms | 0.023 ms |
| `delay 5ms`, one direction | 5.035 ms RTT | 5.072 ms RTT |
| `delay 5ms`, both directions | 10.048 ms RTT | 10.112 ms RTT |
| `loss 25%` | 74 packets missing | 94 packets missing |
| `delay 10ms reorder 100% gap 2` | 150 out-of-order arrivals | 150 out-of-order arrivals |
| `corrupt 10%` | 27 damaged payloads, 299 received | 26 damaged payloads, 300 received |

Client-control median RTT stayed between 0.022 and 0.029 ms, and control traffic added zero packets to netem's counter. The corruption canary alone sets Linux `SO_NO_CHECK` so damaged UDP reaches its payload checker. Production TCP/TLS keeps its integrity checks. TCP can retransmit damaged packets; encrypted-stream corruption is a different fault. The cause of the one missing corruption-canary packet was not localized.

A job must check effective `tc -j -s qdisc` parameters, exact filter selectors, affected counters and a traffic canary, plus a negative control. The prototype does all four. A successful `tc` command is insufficient. A seed does not reproduce kernel scheduling. The upstream [netem manual](https://github.com/iproute2/iproute2/blob/v6.14.0/man/man8/tc-netem.8#L252-L266) also warns that realistic TCP performance tests need receiver-ingress placement because of TCP Small Queues. Loopback chaos is not a calibrated physical-network throughput model.

Run with `sudo unshare --net python3 experiments/chaos49/network.py`. Declare whether the 1 to 10 ms test setting means one-way delay or RTT. A symmetric 1 ms RTT needs 0.5 ms per direction.

### 2. CPU quotas and process signals

Both transient systemd services and direct cgroupfs worked. Tested service properties were `CPUQuota=10%`, `CPUQuotaPeriodSec=100ms`, and `RuntimeMaxSec=12s`:

```sh
sudo systemd-run --wait --collect --unit=chaos49-node \
  -p CPUQuota=10% -p CPUQuotaPeriodSec=100ms -p RuntimeMaxSec=12s \
  /path/to/node
```

The actual `cpu.max` was `10000 100000`. Ten percent means 10% of one CPU, aggregated across the group's threads, not 10% of the runner. Direct cgroupfs created a fresh child under `/sys/fs/cgroup/system.slice`, wrote `10000 100000` to `cpu.max`, then put only the gated child PID into `cgroup.procs`. Prefer systemd for the real job so the quota applies from exec. Do not move the controller or runner service into the node's group.

| Measurement | ARM64 | AMD64 |
| --- | --- | --- |
| Baseline CPU in approximately 3 wall seconds | 3.000 s | 3.000 s |
| systemd quota, CPU / wall | 0.303 / 3.086 s | 0.302 / 3.084 s |
| Direct quota, CPU / wall | 0.311 / 3.017 s | 0.311 / 3.051 s |
| SIGSTOP to stopped wait status, median of 10 | 0.026 ms | 0.017 ms |
| SIGKILL to reaped exit, median of 10 | 0.438 ms | 0.585 ms |

Both throttled cases increased `nr_throttled` by 31. Verification reads `/proc/PID/cgroup`, `cpu.max`, `cpu.stat` and process CPU time. A stopped process must produce `WIFSTOPPED`, all observed task states must be `T`, and CPU counters must remain unchanged over the 100 ms pause. The probe checks SIGCONT and kills a stopped process without resuming it first. SIGKILL success requires `waitpid` to report signal 9, not merely `kill` returning zero. These timings include controller overhead and are observations, not scheduling guarantees. A task stuck in kernel I/O can behave differently.

Run with `sudo python3 experiments/chaos49/process.py "$PWD/results/process"`. Keep PID/incarnation evidence and bounded waits. For recovery/snapshot timing, arm named before/after-I/O hooks and kill after a hit acknowledgment; random sleeps do not prove the desired crash boundary. SIGKILL does not discard the kernel page cache.

Sources: [cgroup v2 CPU fields](https://github.com/torvalds/linux/blob/v6.17/Documentation/admin-guide/cgroup-v2.rst#L1122-L1184), [systemd quota semantics](https://github.com/systemd/systemd/blob/v255/man/systemd.resource-control.xml#L249-L290).

### 3. File faults

Use the existing storage interfaces, but wrap the real `os.File` adapter rather than substitute the simulator's memory disk. At inspected main no adapter existed. Unmerged storage commit `cbcdb7b6f7fb7db02c06cb22b4497b68235d8290` has [wal.FS/wal.File](https://github.com/djosh34/tigerstore/blob/cbcdb7b6f7fb7db02c06cb22b4497b68235d8290/internal/wal/wal.go#L30-L59). That is a candidate integration point, not a promise about the eventual merged interfaces. Its files use `O_APPEND`, so wrap `Write`; Go rejects `WriteAt` on such files.

| Choice | Evidence and decision |
| --- | --- |
| Test-only real-file wrapper | Passed on this host and the ARM runner. A normal build rejects the control flag. Tagged build independently acknowledges installed and fired rules; returns fsync EIO; stalls sync for at least the controller's 150 ms timeout while control stays live; releases to a real fsync; writes a three-byte ENOSPC prefix and a five-byte torn prefix; leaves the expected bytes for controller inspection after SIGKILL during a second stall. Best first implementation because it can select exact file, operation and Nth call. |
| FUSE | Actual unprivileged mount, write/read and kernel-visible fsync EIO passed on both runners after installing fuse3 and libfuse3-dev. The tiny filesystem used direct I/O and daemon memory. It proves availability, not a complete passthrough layer. A production test layer still needs directory, rename, truncate, handle and cache semantics. |
| dm-flakey / dm-delay | Both targets worked over a private 16 MiB loop image. `error_writes` produced real direct-write EIO while reads worked. A delay table with read/write/flush delays of 0/200/300 ms gave write-plus-fsync times of 0.5159 s ARM and 0.5149 s AMD64. Flush-only timing was not independently measured. Useful additional block/filesystem tests, but they cannot name a WAL operation and can affect filesystem metadata first. |
| 1 MiB tmpfs | Real ENOSPC after 1,048,576 bytes on both runners; writes worked after removing the fill file. Fast errno test, not durability evidence. |
| 32 MiB ext4 loop | Real ENOSPC after 26,542,080 bytes on both runners, with write/fsync recovery after freeing space. Recommended independent ENOSPC job. `mkfs.ext4 -m 0` removes reserved blocks. |

Exact scripts are in `experiments/chaos49/disk`. Run `bash run-demo.sh` there, or the runner-only `probes/probe-fuse.sh`, `probes/probe-dm.sh`, and `probes/probe-enospc.sh tmpfs|loop`. The DM tables are `0 32768 flakey LOOP 0 0 60 1 error_writes` and `0 32768 delay LOOP 0 0 LOOP 0 200 LOOP 0 300`. The scripts read back tables/mount options and demand actual I/O outcomes. Missing support exits nonzero; no required fault silently skips.

Create the control socket before recovery and allow startup fault installation before the first WAL read. Cover directory sync, truncate, remove, snapshot/root installation and any direct syscall or mmap path, not just append. Keep controller state outside the failing mount and stalls outside controller locks. Keep original damaged files before recovery repairs them.

The wrapper injects errors before syscall completion; it does not reproduce a kernel writeback failure or a task blocked inside fsync. `dm-flakey drop_writes` silently lies about writes and must not replace `error_writes` in the truthful-storage gate. Power loss and loss of unsynced page-cache contents remain simulator/device-model work. [Detailed comparison and primary sources](https://github.com/djosh34/tigerstore/blob/exp/49-real-process-chaos/experiments/chaos49/reports/disk.md).

### 4. Seeded TLS stream proxy

The standalone Go sketch transparently relays encrypted TCP bytes. It does not terminate TLS or gain plaintext access. Policy decisions use seed, connection ordinal, direction and fixed 4 KiB source-stream offsets. Ordinary reads are split at those offsets; the proxy never waits for a whole 4 KiB block before forwarding a small TLS record. It records actual offsets, directions, actions and monotonic timestamps. A seed reproduces policy, not connection order or kernel timing.

All 78 cases passed across seeds 49 and 50, with three repetitions of 13 modes. Modes cover pass-through, one-byte drop, 1 ms short delay, 25 ms long delay, switching delay, stall, reset, corrupt, duplicate, one-way cut, both-way cut and 100 MB/s versus 1 MB/s links. Drop/corrupt/duplicate produced TLS authentication errors, reset produced `ECONNRESET`, and an independent reverse-direction TLS record passed during one-way cut and stall. A saturation test confirmed stall backpressure. Unit and race tests passed. Canonical policy hashes matched across each seed's three repetitions.

A stream proxy's drop deletes bytes already accepted by its TCP socket. TCP cannot retransmit those deleted stream bytes. That differs from netem packet loss. TLS ciphertext mutation tests rejection or connection failure, not silent modification of an authenticated Raft message. Test decoded message corruption separately if required. Keep production TLS end-to-end; TLS termination would change the system under test.

Measured here with Go `1.27.2`, Linux `5.14.0-611.26.1.el9_7.aarch64`, Apple-reported 8-core ARM VM, TLS 1.3 `TLS_AES_128_GCM_SHA256`. Endpoint and separate proxy processes both used `GOMAXPROCS=1`, pinned to CPU 6. Five alternating direct/proxy repetitions used 2,000 sequential 256-byte echo RTTs and 256,016,384 bytes of 32 KiB bulk requests plus equal echo bytes per repetition. TLS handshakes were outside the timed regions. Server certificate verification was enabled; mutual TLS was not exercised. Other workers were active, including a simulation process observed on CPU 6. This was not an isolated host benchmark.

| Median across five repetitions | Direct | Transparent proxy |
| --- | --- | --- |
| Per-run p50 RTT | 7.292 us | 21.458 us |
| Per-run p99 RTT | 13.084 us | 46.167 us |
| Endpoint CPU ms per decimal MB | 0.662 | 0.731 |
| Proxy-only CPU ms per decimal MB | 0 | 0.527 |
| Total CPU ms per decimal MB | 0.662 | 1.271 |
| Echo request MB/s | 753.710 | 397.134 |

CPU denominators count request plus response plaintext bytes, 512,032,768 bytes per bulk repetition. Throughput counts request bytes only. Proxy-only CPU ranged from 0.527 to 0.547 ms/MB, sampled from `/proc/PID/stat` at 100 ticks/s. Accounting quantization contributes less than 0.0391 ms/MB uncertainty. The median paired p50 increment was 14.168 us; the median paired total CPU increment was 0.604 ms/MB. This cost belongs in the chaos CPU budget. It is not an etcd-versus-tigerstore benchmark, and active-fault tracing costs were not benchmarked.

Run `bash experiments/chaos49/proxy/run.sh` before the task deadline. [Source, raw measurements and full report](https://github.com/djosh34/tigerstore/tree/exp/49-real-process-chaos/experiments/chaos49/proxy). The sketch is not a production relay. Delay accumulates once per 4 KiB boundary rather than modelling fixed propagation latency; the crude rate limiter permits idle credit; traces are unbounded; connection lifetime is capped at 30 seconds. The clean implementation needs declared delay semantics, bounded queues/traces, stable link identities, cancellation and reconnect handling. Keep packet reordering in netem rather than rearranging bytes inside a valid TCP stream.

### 5. What to copy from etcd robustness

Audited etcd v3.7.2 at `68c065e562994b89e333e77b039ad066f933c586`, module `go.etcd.io/etcd/tests/v3`, with gofail v0.2.0 and Porcupine v1.1.0. This was a source audit, not an etcd chaos run. [Full audit with pinned line-level citations](https://github.com/djosh34/tigerstore/blob/exp/49-real-process-chaos/experiments/chaos49/etcd/REPORT.md).

- The suite separately registers binary gofail hooks and a list of candidate faults. It filters availability, chooses a fault, checks health, arms through HTTP or startup environment, triggers the required condition, waits for death or recovery, and checks health again. Snapshot triggers deliberately isolate a follower until catch-up needs a snapshot. Copy targeted triggers and instrument tigerstore's own persistence/recovery boundaries.
- Traffic, injection and per-member watch collection run concurrently against one monotonic origin. There is roughly one second of traffic before and after the fault, with jitter. Profiles have six member-pinned and two cluster clients, limits of 100, 200 or 1,000 QPS, 200 ms request timeouts and three concurrent nonunique requests. Copy hot-key traffic, Kubernetes CAS/list/watch shapes, unique mutation identities and a successful terminal write. Add explicit numeric post-heal progress bounds; upstream only gates pre-fault QPS.
- Keep raw client histories. A timeout is an unknown outcome, not an abort. Upstream starts a new sequential stream after an ambiguous failed mutation and extends its checker return bound to `MaxInt64`. Preserve that distinction. Do not omit interfering writers or blindly copy WAL/watch-assisted history patching as the independent oracle.
- Copy watch order, uniqueness, resumability and progress checks, but require completeness through an independently obtained target revision. Require requested PrevKV on updates/deletes, not creates. Upstream relaxes PrevKV under compaction and can miss a silent watch.
- Do not copy permissive coverage handling. Some sleep failpoints lack hit verification despite gofail's hit-count endpoint. A network blackhole warns about an alternate peer pipeline. Empty persisted requests skip watch/historical validation, and the zero-value validation status returns no error. Require explicit success for every required checker and installed-plus-hit evidence for every fault.
- Replace the etcd WAL reader. It may repair inspected WALs, and persisted-entry presence is not itself commitment. Preserve original disks, retain full value bytes and MVCC metadata, and add receipt-ledger plus erase-one-disk cold-recovery checks. The upstream model hashes larger values to 32 bits and is not a full MVCC specification.
- Copy artifact retention and offline validator reruns. `TestDataReports` revalidates saved evidence; it does not replay processes. No automatic execution shrinker appeared in the audited source. Save the realized fault schedule, then reduce it into named regressions at the layer that reproduces the bug.

### Pitfalls and remaining work

The [first run](https://github.com/djosh34/tigerstore/actions/runs/38315923559) was falsely green. `tc -j filter` emits header entries as well as actual rules; our first assertion counted both. Implicit Actions `bash -e` piped through `tee` hid the assertion failure. We fixed rule parsing, set explicit `shell: bash`, tested a failing pipeline, captured stderr and required a final PASS marker. That first network run is not evidence of success.

The host file-wrapper prototype also hit virtiofs rejecting chmod on a Unix socket and a Go/Python error-string case mismatch. It now sets creation umask and checks numeric errno. These were prototype defects, not tigerstore bugs.

For #49, make each fault record distinguish requested, installed, exercised and healed, with target identity, parameters and monotonic times. Fail on a missing hit or an unevaluated checker. Run linearizability, complete-watch and durability checks on actual cluster histories, then add gray-node/client liveness bounds, startup/recovery/snapshot hooks and regressions. None of those product guarantees follows from these mechanism probes. No host tc, cgroup, mount or other root-level configuration was changed.
