# #484: put/txn persistence, CPU and RTT gap

Branch: `exp/484-ceiling-gap`. No issue comment, PR or main push.

## Provenance

- Base: `bd1ade319a4d0bfc92082a8bf8d335aa01199428` (`origin/main` when fetched).
- Instrumentation/harness: `f7937e28`; workflow registration: `7bdd85fff0ac907ecbf2de5429a624b07553aebc`.
- Full matrix: https://github.com/djosh34/tigerstore/actions/runs/42019718823 — success, 240 fresh-cluster runs, 120 on each of two VMs, three repetitions/configuration/VM.
- Disk follow-up: `da0731181c8cf917cda6c6d6cacd70fdcfbebbd4`; https://github.com/djosh34/tigerstore/actions/runs/42027385931 — success, 60 runs, 30 on each of two new VMs.
- Local-pipeline follow-up: `a71634b2940dac942674c5f30713d218c5f204ca`; https://github.com/djosh34/tigerstore/actions/runs/42030854796 — success, 48 runs, 24 on each of two new VMs. Total: **348 validated fresh-cluster runs on six independent VMs**, all configurations repeated three times per VM.
- Registration-only push run https://github.com/djosh34/tigerstore/actions/runs/42019675324 was cancelled immediately in favor of the dispatch run; do not use its partial measurements. GitHub initially returned 404 for dispatch of a workflow absent from main; a branch-only push trigger registered it, then the trigger was removed. No default-branch change.
- Artifact names in each successful run: `ceiling-gap-1`, `ceiling-gap-2`. Raw CPU profiles, symbolized raw/top views, block/mutex/goroutine profiles, loadgen JSON, full metrics snapshots, Raft progress, diskstats, per-process I/O, TCP state and logs are retained for 30 days. Detail artifacts also contain syscall histograms, block traces and FIEMAP/journal extents.
- Local raw copies: `/tmp/r484/gap-art/ceiling-gap-{1,2}`, `/tmp/r484/gap-detail-art/ceiling-gap-{1,2}`, `/tmp/r484/gap-pipeline-art/ceiling-gap-{1,2}`.

**Hardware qualification:** all measured VMs have 4 Neoverse-N2 cores, 16,715,923,456 bytes RAM, ext4 `/dev/sda1`, Go 1.27.2, but **kernel 7.0.0-1017-azure**, not the earlier bench's 6.17.0-1022-azure. `/` was remounted `barrier=1`; ext4 reports `journal_async_commit,commit=30,data=writeback`. These are new measurements of main, not a rerun of commit 255ae6e1 on an identical kernel. Do not splice these rates or component latencies into the earlier run as if hardware/software were identical.

## Exact method

```sh
gh workflow run ceiling-gap.yml --ref exp/484-ceiling-gap -R djosh34/tigerstore
gh workflow run ceiling-gap.yml --ref exp/484-ceiling-gap -f phase=detail -R djosh34/tigerstore
gh workflow run ceiling-gap.yml --ref exp/484-ceiling-gap -f phase=pipeline -R djosh34/tigerstore
```

- `.github/workflows/ceiling-gap.yml:11-96`: two independently provisioned `ubuntu-24.04-arm` VMs per dispatch; pinned generator/leader/followers = CPUs 3/0/1/2; same client/peer TLS certificates and same netem script as bench.
- `bench/ceiling-gap.sh:10-75`: fresh cluster per point, 10,000 keys, 1,024-byte values, six connections, default 256 streams each, 3 s warmup + 14 s measurement, independent seed per repetition. Rate mode offers 2,500/s. CPU profiles of leader and first pinned follower cover t=5..13 s from generator launch; measurement subwindow covers the same middle eight seconds, with per-node timestamps. Transaction preload precedes its measured phase.
- Full matrix: put/txn × disk/tmpfs × RTT 0/2/10 × max/rate, plus RTT-10 `streams512` (3,072 slots), `streams1024` (6,144 slots), `wide` (only Raft MaxInflightMsgs 256→1,024), and RTT-0 `pure` controls built directly from base with no experiment code. Three repetitions each, per VM.
- Pipeline: disk RTT 0/10, put/txn, alternating same-VM max versus `depth8` (only `const ownDepth = 2`→`8`), three repetitions each, per VM. No BPF or block trace in this phase.
- Detail: disk RTT 0, put/txn, max / alone (followers on tmpfs, forced leader node 1) / unsafe (`--unsafe-no-fsync`) / streams1024 / purerate (base binary at 2,500/s), three repetitions each, per VM. BPF enabled; default max points additionally have `blktrace -d /dev/sda -w 8`, `blkparse -f '%a %S %n %d\n'`, `debugfs -R 'stat <8>' /dev/sda1`, and 1 Hz `filefrag -v` of open regular FDs.
- Instrumentation: `internal/node/ceiling_gap.go:13-49`, `internal/node/persist_timing.go:14`: aggregate existing persistence timestamps; no per-group logging. `/debug/vars` adds cloned Raft progress inflight counts/full flags. Go block sampling = one sample per blocked millisecond, mutex = original pprof default one in five events. Pure controls omit experiment code.
- `bench/ceiling-gap-report.py:16-48`: fsync/s and mean fsync from delta of WAL histogram count/sum; WAL B/fsync from byte counter/count; WAL B/op and ops/fsync from the **same node's revision delta**, not a different throughput window; disk MiB/s, write await and busy fraction from diskstats; process CPU from loadgen's `/proc/PID/stat` sampler. Every transaction here makes exactly one successful write/revision. For `unsafe`, the histogram still counts logical WAL sync attempts, but they are no-ops: do not read its fsync/s as actual kernel fsyncs. Safe WAL timing includes the sync path's bookkeeping and occasional directory/vote syncs, not just a bare syscall. Final tables recompute revision-based byte ratios from raw snapshots; original job-summary byte-ratio derivation used the wider loadgen window.
- `bench/ceiling-gap-profiles.py`: exclusive stack classification matching `bench/profile.sh` categories. CPU us/op = sampled share × full-window process CPU us/op. This is CPU time, not request wall latency. Profiles remain authoritative; applying middle-window shares to full-window averages introduces sampling uncertainty.
- `bench/ceiling-gap-blocks.py`: count **D** (issue) write sectors once, not every block lifecycle event. Journal bytes use inode 8's physical extents plus partition offset. Metadata-flag bytes are separate from ordinary data. Open-file WAL/checkpoint attribution is a lower bound because closed files and short-lived opens can escape 1 Hz snapshots. Trace B/op uses measured applied-op rate × eight seconds, so it is approximate and not exactly the 14 s loadgen denominator.
- `bench/ceiling-gap-tables.py`: runner-1 median / runner-2 median [min,max across six repetitions]. `484-ceiling-gap-tables.md` contains all analyzed configuration/node counters; `484-ceiling-gap-profiles.md` contains the top six CPU steps for both sampled nodes; `484-ceiling-gap-blocks.md` contains every block trace's byte accounting. Raw artifacts include all unabridged counters and samples.

## Findings

### 1. The earlier CPU labels need correcting

`cmd/loadgen/main.go:640-649` sets `server_cpu_us_per_op` to **the sum over all nodes**. Leader-only is index 0 of `server_cpu_us_per_op_by_node`. Thus the earlier 15.0 us/op disk / 13.1 us/op tmpfs numbers were not leader-only. On this run, full-matrix median leader cost at disk RTT 0 is put 9.75 us/op, txn 12.74 us/op; tmpfs is put 8.59, txn 11.92. The earlier 19.1 txn figure likewise sums server nodes.

### 2. At 0/2 ms, the safe shared-disk layout is persistence/batching limited; tmpfs is leader-CPU limited

Full-matrix per-runner medians, ops/s:

| workload | RTT ms | disk runner 1 / 2 | tmpfs runner 1 / 2 | measured tmpfs/disk speedup runner 1 / 2 |
|---|---:|---:|---:|---:|
| put | 0 | 78,600 / 77,083 | 118,931 / 111,726 | 1.513 / 1.449 |
| put | 2 | 82,558 / 79,535 | 118,310 / 111,565 | 1.433 / 1.403 |
| put | 10 | 70,926 / 68,856 | 90,965 / 87,349 | 1.283 / 1.269 |
| txn | 0 | 65,104 / 61,946 | 86,198 / 81,906 | 1.324 / 1.322 |
| txn | 2 | 63,975 / 61,942 | 84,196 / 79,679 | 1.316 / 1.286 |
| txn | 10 | 57,505 / 55,593 | 71,972 / 68,747 | 1.252 / 1.237 |

- Tmpfs RTT 0/2: leader ~0.994–1.000 cores, followers ~0.38–0.40; generator ~0.21–0.29. The constrained CPU is the leader's single pinned CPU, not total machine CPU and not generator CPU.
- Disk RTT 0: leader ~0.75–0.81 cores; put device ~263–271 MiB/s, txn ~227–243 MiB/s, not a fully utilized 339–343 MiB/s sequential device. Diskstats busy fraction is only ~50–65%; this is not evidence of an idle durability path: fsync queues and multiple WAL streams do not keep the raw sequential benchmark busy continuously.
- Same-VM detail controls: moving only followers to tmpfs raises put 79,218→103,857 and 78,013→100,935 (+31.1%/+29.4%); txn 61,517→73,757 and 61,683→74,806 (+19.9%/+21.3%). Leader fsync mean drops from ~5.2 ms to ~2.3 ms put / ~2.7 ms txn. This measures the cost of the three independent WALs sharing one device.
- Removing all fsyncs (unsafe diagnostic, **not an acceptable durability mode**) raises put to 116,176/114,292 (+46.7%/+46.5%), txn to 83,402/83,221 (+35.6%/+34.9%), leader ~1.00 core. Page-cache writeback lags and device B/op changes, so these short unsafe runs are not a safe-storage or steady-state ceiling.
- Raising client slots 4× even at RTT 0 improves safe disk put only +5.5%/+6.1%, txn +12.5%/+10.6%; larger groups do help, but removing fsync waits helps substantially more.
- Contemporary `diskbench -dir "$RUNNER_TEMP"`: max 338.54 and 343.26 MiB/s. At 256 KiB groups: 245.72/329.44 MiB/s; at 1 MiB: 338.54/335.67; at 4 MiB: 336.81/343.26. Consequently a disk ceiling is group-shape dependent, not a single invariant 345 MiB/s. Dividing peak by ~3,510 B/put gives ~101–102k/s, above observed safe throughput. It is an optimistic bandwidth bound, not an explanation of the whole gap.
- At disk RTT 0, all-six-run median leader WAL fsync/s is put 150.1, txn 115.7; mean fsync 4.95/5.30 ms; applied ops/fsync 517.7/536.7; WAL bytes/fsync 564,724/648,646. Followers sync ~123–157 times/s with ~492–508 ops/fsync, at ~4.35–4.41 ms each. Full node/RTT spreads are in the tables.
- Persistence means at disk RTT 0: put group total ~9.22 ms (queue 1.10, append .13, write .85, sync queue 1.87, sync 4.38, report .87); txn ~10.53 ms. These groups overlap; they are not per-request sums or independent ceiling calculations.
- Block profiles show committer select waits (`internal/node/node.go:1087`), writer/sync channels and reply/peer queues. WAL fsync sleep is measured by the WAL histogram, not by Go's channel/mutex blocker profile. Total sampled mutex-contention estimates across the main max profiles are only .001–.093 seconds per process/profile since startup, versus ~13 seconds elapsed; lock contention is not the missing quarter-core.

### 3. At 10 ms, the benchmark's client in-flight limit changes the answer

| workload/layout | 1,536 slots runner 1 / 2 | 3,072 slots runner 1 / 2 | 6,144 slots runner 1 / 2 | 6,144/1,536 speedup runner 1 / 2 |
|---|---:|---:|---:|---:|
| put/disk | 70,926 / 68,856 | 84,268 / 82,030 | 88,596 / 85,817 | 1.249 / 1.246 |
| put/tmpfs | 90,965 / 87,349 | 118,771 / 112,078 | 126,080 / 118,993 | 1.386 / 1.362 |
| txn/disk | 57,505 / 55,593 | 66,508 / 63,969 | 70,840 / 68,385 | 1.232 / 1.230 |
| txn/tmpfs | 71,972 / 68,747 | 84,252 / 78,836 | 85,793 / 80,304 | 1.192 / 1.168 |

- Default max's throughput × mean latency is ~1,519–1,527 operations, close to the 1,536 permitted slots. Generator CPU is only ~0.15–0.24 core; it is **slot/latency limited, not generator-compute limited**. More slots also make larger, cheaper durability batches; this is not simply the formula slots / bare network RTT.
- With 6,144 slots, tmpfs leader returns to ~0.99 core; put exceeds its 0 ms/default-window CPU reference because larger batching lowers CPU/op. Txn recovers approximately the 0 ms CPU rate. Default-window 10 ms results must not be called a fundamental network or Raft ceiling.
- Increasing **only** Raft MaxInflightMsgs 256→1,024 does not recover throughput: put disk 71,295/68,734; txn disk 55,359/54,073; tmpfs put 90,769/88,390; txn 70,675/67,950. The changes are noise or regressions, not the +17–39% from client slots.
- At 10 ms default max, sampled Raft inflight counts reach only ~5–13 messages, none full. The 32 MiB byte window and 4 MiB message cap are much larger than the default ~1.5 MiB client payload window. One **2 ms txn** sample has a lagging follower's byte window full at nine messages; the other follower keeps committing. It is not the sustained 10 ms bottleneck.
- `ss -tinp` records the active data senders as `app_limited`, with multi-MiB advertised windows and congestion windows comfortably above the ~0.7–1 MiB BDP; the example put/disk/10/max/1 has ~5.47 MiB `snd_wnd`, cwnd 112/177 segments × 65,483 bytes, delivery rates ~5.5 Gbit/s, versus ~70–80 MiB/s application payload per follower. Some receive-window-limited time includes startup; no TCP-window perturbation was made. **Inference, not a separate experiment:** TCP is not the dominant sustained cap in these samples.
- Peer send code flushes when its queue empties (`internal/peer/peer.go:1179-1220`); no batching timer exists there. CPU profiles charge peer send ~1.6–2.1 us/op at max; no evidence of a timer explaining the 10 ms drop.

### 3b. Local persistence depth: small gain, not the missing large ceiling

The core independently allows only two appends of the leader's own writes awaiting durability (`internal/core/node.go:311-313,1762-1791`), while the writer allows four written groups (`internal/node/node.go:1396-1397`). Changing only ownDepth 2→8 gives:

| workload | RTT ms | baseline runner 1 / 2 | depth8 runner 1 / 2 | ratio runner 1 / 2 |
|---|---:|---:|---:|---:|
| put | 0 | 77,981 / 67,968 | 80,750 / 71,934 | 1.036 / 1.058 |
| put | 10 | 70,621 / 64,704 | 71,844 / 67,581 | 1.017 / 1.044 |
| txn | 0 | 61,692 / 54,932 | 64,437 / 57,721 | 1.045 / 1.051 |
| txn | 10 | 56,131 / 52,793 | 57,462 / 53,178 | 1.024 / 1.007 |

Leader CPU rises, but so does mean WAL sync time (put RTT 0 ~5.21→5.70 ms on runner 1, ~6.13→6.24 on runner 2). This experiment does **not** recover the 17–39% obtained by increasing client slots, nor the disk→tmpfs gap. Gains are small and spreads overlap; no production tuning recommendation follows. The slower second VM emphasizes why controls must be compared on the same VM. Its rates must not be pooled with the earlier VMs as if all runs shared hardware speed.

### 4. WAL/device amplification

- Put keys are 45 bytes (`cmd/loadgen/keys.go:35-50`), not zero length. A 1,024-byte value produces a 1,074-byte Put protobuf (45-byte key + five protobuf framing bytes + value); Raft batch metadata adds origin, boot, request ID, kind and length (`internal/core/codec.go:65-136`).
- Measured WAL bytes/applied put: ~1,090 per node, ~3,272 across replicas. Txn is ~1,208 per node, ~3,624 across replicas. This is the dominant part of ~3,510–3,520 device B/put or ~3,875 B/txn.
- WAL framing is **9-byte record header + 17-byte entry header**, amortized over the writes packed into each Raft entry (`internal/wal/wal.go:133-136,888-900,946-1006`). WAL writes are packed `WriteAt(buf,size)`, **not per-operation 4 KiB padded records**. Block-device page rounding can still rewrite the tail page at fsync boundaries.
- All 12 detail block traces dropped zero events. Put trace journal bytes are ~126–131 B/op; total metadata-flag bytes ~126–131 B/op. Txn is ~125–132 B/op. Journal writes are measured with inode 8's exact extent range, not assumed from residual bytes.
- Trace-window accounting below uses medians [min,max] across six runs, B/applied operation; the block trace denominator is approximate. The paired `/proc/PID/io` write_bytes counters positively locate the ~150–163 B/op non-WAL amplification in application-file dirtying, rather than assigning it all to the journal:

  | workload | summed WAL bytes/op | summed process write_bytes/op | process minus WAL | journal block bytes/op | total block issue bytes/op |
  |---|---:|---:|---:|---:|---:|
  | put | 3271.2 [3268.2,3276.7] | 3421.8 [3416.3,3425.9] | 149.0 [146.6,155.2] | 127.7 [125.8,130.8] | 3551.2 [3544.3,3667.6] |
  | txn | 3626.1 [3621.4,3629.2] | 3789.4 [3782.5,3794.2] | 163.3 [157.9,167.8] | 129.2 [124.8,131.8] | 3918.6 [3911.8,3927.9] |

  Put is therefore approximately **3271 WAL + 149 application-file/page amplification + 128 journal = 3548 device bytes/op**, consistent with the ~3520 full-window device figure within timing/writeback differences. Do **not** claim an exact checkpoint-vs-tail-rewrite split of the 149.
- FIEMAP positively attributes checkpoint traffic: ~0–75 B/put and ~19–86 B/txn in individual traces are mapped checkpoint writes. These are **lower bounds**, not total checkpoint overhead. Checkpoints are real files (e.g. a ~16,066,010-byte checkpoint per replica after a put run); writer code syncs checkpoint data each MiB (`internal/node/node.go:1952-2004`).
- Per-FD BPF raw maps are provided, but FD reuse and incomplete snapshot coverage prevent a trustworthy exact per-file split. Use WAL counters for WAL bytes and block/journal extents for device classification. Exact remaining checkpoint-vs-tail-rewrite attribution is **unmeasured**, not guessed as all metadata or all padding.

### 5. Low-load latency floor: measured components, not an invented additive trace

Medians pooled across the six full-matrix repetitions:

| workload | RTT ms | disk p50 ms | tmpfs p50 ms | disk minus tmpfs p50 ms | leader mean WAL fsync ms | leader persistence group total ms |
|---|---:|---:|---:|---:|---:|---:|
| put | 0 | 1.540 | .707 | .834 | .375 | .528 |
| put | 2 | 3.441 | 2.941 | .500 | .344 | .484 |
| put | 10 | 11.436 | 10.912 | .524 | .339 | .488 |
| txn | 0 | 1.581 | .745 | .836 | .361 | .509 |
| txn | 2 | 3.473 | 2.966 | .508 | .328 | .469 |
| txn | 10 | 11.469 | 10.945 | .524 | .333 | .475 |

- The earlier 1.24 ms p50 was **not reproduced** on current main/new kernel. Base-binary/no-experiment-code rate controls on detail VMs give put medians 1.716/1.577 ms, txn 1.716/1.626 ms, confirming that the current floor is not just the added Go block/timing instrumentation.
- Network contribution is directly imposed: one peer RTT, not client RTT. At 2/10 ms, tmpfs p50 minus imposed RTT is ~.94/.91 ms put, ~.97/.94 ms txn. At 0, remaining software/scheduling/TCP loopback floor is ~.71/.75 ms. These are measured counterfactual floors, not pure CPU numbers.
- Disk low-load leader group stages (put): queue ~.03–.04 ms, append .002, flush .05–.06, sync queue .10–.12, group-weighted sync .25–.28, reporting .04; total .48–.53 ms. Fsync histogram means are higher (.34–.38 ms) because it weights syscalls, not groups, and includes the sync path's bookkeeping. Followers' low-load fsync means are ~.28–.35 ms. Table file has every node/configuration.
- CPU/op at low rate is much larger than max-rate batched CPU/op; syscall/wakeup costs dominate rate profiles (example leader profile 58% flat Syscall6). CPU on parallel nodes overlaps. Summing their CPU averages or persistence-group stage means into a p50 request timeline would be invalid.
- No batching timer is present in server admission (`internal/server/batch.go:131-151`) or peer output. Tick is not on the ordinary low-load per-write critical path. **Unmeasured:** exact request-by-request division of the residual into kernel scheduling, client wakeup, peer scheduling and apply/report waits; existing group timing is not a distributed critical-path trace. No precise additive decomposition of the old 1.24 ms is claimed.

## Validation and cautions

- Passed locally: `go test -short ./cmd/loadgen ./cmd/benchreport ./internal/core ./internal/peer -count=1`; `GAP_TIMING=1 GAP_BLOCK=1 go test ./internal/node ./internal/server -run 'TestPersist|TestEtcdMetrics' -count=1`; shell/Python syntax checks.
- An earlier broad local test command failed because the shared development host filled `/`; loadgen integration tests also require `LOADGEN_ETCD` unless `-short`. Those environment failures are not benchmark results. No development-host cluster benchmark was used.
- Full matrix pure controls put max differ by roughly 1–3% from instrumented medians on the same VMs; txn controls vary around zero. Use the recorded spreads, not sub-percent differences.
- A few non-baseline experiment runs have a badly lagging follower, reflected in low follower CPU and lower device B/op. Leader/term stayed stable and loadgen validation passed, but these are not clean three-replica-storage bandwidth samples. Raw runs are retained rather than silently discarded. Main max matrices are stable; the isolated 2 ms follower byte-window event is noted above.
- Block-profiling delay sums across concurrent goroutines are not elapsed idle CPU; the pprof handler itself contributes an 8 s sleep. Do not sum all blocker rows as a latency budget.

## Source paths read (base bd1ade319a4d0bfc92082a8bf8d335aa01199428 unless experimental)

- `.github/workflows/bench.yml:58-199`: ARM setup, pinning, remount, RTTs, layouts, report.
- `bench/run.sh:1-96,112-139,173-220,225-267`: workloads, concurrency, duration, layouts and rate semantics.
- `bench/cluster.sh:74-105,145-204,218-250`: leadership, CPU placement, node flags, TLS, tmpfs, checks and stop.
- `bench/netem.sh:24-42`: peer-port qdiscs and half-RTT each direction.
- `bench/profile.sh:14-78`: existing tmpfs-only profile and exclusive step categories.
- `cmd/loadgen/main.go:48-66,116-180,269-286,596-658`: defaults, output semantics, txn preload, CPU and throughput denominators.
- `cmd/loadgen/cpu.go:16-46,59-80`: process ticks and diskstats sectors.
- `cmd/loadgen/workload.go:165-255`: CAS per-key serialization and validated one-put successful transactions.
- `cmd/loadgen/worker.go:55-143`: offered arrivals, closed-loop slots, drain and wakeup.
- `cmd/loadgen/keys.go:35-50`: generated key shape/length.
- `cmd/benchreport/main.go:347-400`: existing bandwidth/bytes-op ceiling and tmpfs reference calculation.
- `cmd/diskbench/main.go:46-70,98-125`: O_DIRECT allocated-space group writes + fdatasync.
- `internal/core/node.go:311-313,802-805,1672-1676,1724-1728,1762-1791,1840-1949`: own-append depth, Raft limits, persistence acknowledgment, self-clocked batching and apply/send.
- `internal/core/status.go:120-153`: current status/progress sampling.
- `internal/core/codec.go:65-136`: application batch encoding.
- `internal/node/node.go:608-617,1034-1156,1396-1400,1437-1609,1619-1702,1952-2004`: unsafe wrapper, committer/persist separation, four-group disk pipeline, sync coalescing and checkpoints.
- `internal/node/persist_timing.go:5-34`: existing stage timestamps and slow-group-only logging.
- `internal/server/debug.go:12-25`, `internal/server/metrics.go:382-413`: pprof rates and debug vars.
- `internal/server/batch.go:24-28,131-151`: buffer-driven batching without timers.
- `internal/peer/peer.go:917-951,1179-1220`: queues, output flushing and frame encoding.
- `internal/peer/metrics.go:9-35`: peer byte/failure/RTT counters.
- `internal/server/tracing.go:20-119`: existing tracing is server/RPC spans, not a detailed per-entry distributed critical-path trace.
- `internal/wal/wal.go:133-136,888-900,946-1016`: record framing and packed writes.
- raft dependency `go.etcd.io/raft/v3@v3.7.0/tracker/inflights.go:126-140`: Full checks message count or bytes; Count is exposed for experiment snapshots.
