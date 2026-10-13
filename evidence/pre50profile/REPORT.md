# Three-node Put and Range profiles

Read #46, #1, #50 and `docs/design.md` on main. Nothing merged. Experiment branch: https://github.com/djosh34/tigerstore/tree/exp/pre-50-profile.

## Runs and versions

- Main engine: `08d620707e25182219d8bd23830b11fed14f6c6a`, the #60 merge. The paired measurement used `1186010d5f43007e4fc0d8d51779541e3a018b7d`. Its only server changes are a loopback HTTP pprof listener and an atomic leader-ID query. Core, Raft, peer transport and storage are unchanged. CPU capture, mutex sampling and block sampling are off in the comparison windows; Go's default allocation sampling remains unchanged. This is main with observability, not the byte-identical main executable.
- Paired run: https://github.com/djosh34/tigerstore/actions/runs/38323686555, passed, 7m03s. Five fresh clusters per store. Each repetition runs both stores back to back on the same VM; odd repetitions run tigerstore first, even repetitions run etcd first. Separate fresh tigerstore clusters then provide Put and Range profiles.
- Disk-timing follow-up: `bea9ee34347f57abb852a1c6063f8a29661f9612`, https://github.com/djosh34/tigerstore/actions/runs/38324504695, passed, 1m42s. It adds timed file Write/Sync wrappers and profiles both followers. It does not replace the paired numbers. It changes no persistence ordering or batching logic.
- Stock release etcd v3.7.2, upstream `68c065e562994b89e333e77b039ad066f933c586`, built with Go 1.26.8. Binary SHA-256 `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d` was checked before running.
- Tigerstore and generator: Go 1.27.2, linux/arm64, etcd API 3.7.2, Raft 3.7.0, grpc-go 1.84.0, protobuf 1.36.11, x/net 0.58.0, Google B-tree 1.1.3. Each run's `versions.txt` records full module/build information and all executable hashes. The paired tigerstore SHA-256 is `3116f2b0f2b337927a6d5d4a15809ed589098d4ec6983250d4d0cd90ca04ccd2`; generator is `5846c134267e766bb82c5c608dc230b6df02f7c60ac024189caf333ab4872dbc`.

Both free `ubuntu-24.04-arm` jobs had four Neoverse-N2 cores, image `ubuntu24-arm64 20261004.142.1`, kernel `6.17.0-1022-azure`, about 16.72 GB RAM, and ext4 `/dev/sda1` on one 150 GiB virtual disk. The first job had 114.29 GB available. Both remounted `/` with `barrier=1` and rejected `nobarrier` or `barrier=0` afterward. All data directories were on that mount. Guest barriers and successful fsync do not prove host power-loss protection. These are three processes sharing one disk and failure domain.

The isolated layout matches the pre-work: leader CPU 0, followers CPUs 1 and 2, generator CPU 3, all `GOMAXPROCS=1`. The elected leader can be any node ID. The script discovers it, pins all existing threads to the role's core with `taskset -apc`, and checks every server thread's affinity after the load. No proxy or added network delay. Client and peer links use certificate-authenticated TLS 1.3. Etcd uses its safe default persistence and an 8 GiB quota. Neither store has watches, compaction, auth policy or defrag in this workload.

## Workload and measurement

The same direct protobuf/grpc-go client drives both stores: 16 connections, 256 closed-loop workers, leader endpoint only. There are 4,096 keys shaped as `/registry/pods/ns-%03d/pod-%08d`, across 64 namespaces. The deterministic multiplicative permutation visits every key once per 4,096 requests. Values are 1,024-byte SHA-256-derived buffers, different for each key and reused on overwrite. These are synthetic values, not Kubernetes objects. No compression is enabled.

Each cluster gets 4,096 seed Puts, 20,000 warmup Puts, a one-second pause, 60,000 measured Puts, 10,000 warmup Ranges and 100,000 measured Ranges. Range means a present-key, current-revision, linearizable point read, returning and validating all 1 KiB of the value. It is not a prefix LIST, historical or serializable read. Every new client connection completes a probe before measurement. Connection establishment is outside the comparison timer.

CPU is process user plus system ticks from `/proc/PID/stat`, parsed after the final `)`, with process-start identity checks and `CLK_TCK=100`. This excludes I/O wait and kernel-worker, interrupt or hypervisor work not charged to the process. There is no follower catch-up barrier at the CPU boundary, so trailing asynchronous work can fall outside it. Sampling brackets the worker interval, through drain, before result sorting/output. One tick is 0.1667 CPU us per measured Put or 0.1 us per measured Range. Throughput uses successful completions divided by elapsed time through drain. Latencies cover every successful call, including its gRPC context setup and completion. No timeout or error is discarded from validity checks.

All 20 comparison windows passed: no RPC errors, exact expected success counts, Put revision deltas equal to successful calls, Range revision deltas zero, one response term per window, valid point-read contents, unchanged before/after leaders and verified affinities. This is not a full linearizability or durability test. Constant overwrite values cannot distinguish old from new content. Closed-loop clients suppress arrivals while stalled, so these are fixed-concurrency latencies, not open-loop service-level measurements.

## Paired results

These are medians of five per-run statistics, not pooled latency percentiles. `summary.csv` and the raw JSONs retain every repetition.

| Store / operation | Operations/s | Mean ms | p50 ms | p99 ms | p99.9 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Tigerstore Put | 4,693.7 | 54.514 | 54.464 | 111.177 | 158.174 |
| Etcd Put | 5,051.1 | 50.621 | 48.770 | 85.034 | 134.287 |
| Tigerstore Range | 27,081.0 | 9.442 | 9.219 | 22.478 | 35.221 |
| Etcd Range | 8,591.3 | 29.777 | 28.689 | 47.814 | 55.399 |

| Store / operation | Leader CPU us/op | Follower 1 | Follower 2 | All servers | Generator |
| --- | ---: | ---: | ---: | ---: | ---: |
| Tigerstore Put | 75.667 | 12.333 | 12.167 | 100.000 | 26.500 |
| Etcd Put | 161.667 | 28.333 | 27.667 | 217.833 | 48.833 |
| Tigerstore Range | 36.500 | 1.900 | 13.100 | 51.500 | 21.600 |
| Etcd Range | 116.400 | 1.200 | 1.200 | 118.800 | 42.500 |

Each column is an independent median; the medians of individual nodes need not sum to the median of their per-run totals. Tigerstore's Put rates ranged from 4,355.9 to 5,831.7/s; etcd's ranged from 4,753.7 to 5,158.9/s. Tigerstore is not a demonstrated Put throughput winner. The ratio of median rates is 0.929, with 54.1% less aggregate CPU/op. Its Range ratio is 3.152, with 56.6% less aggregate CPU/op.

Median occupied leader cores were 0.337 and 0.825 for tigerstore and etcd Put; 0.983 and 1.000 for Range. Generator occupancy was 0.124/0.247 for Put and 0.582/0.365 for Range. The generator did not consume its entire core, though this does not prove the fixed concurrency is optimal.

## Profiles and costs per request

The first job captured 30-second CPU, mutex, block and allocation-delta profiles of the leader and the follower on CPU 1. Each profile interval contains a 25-second arrival window plus connection setup, drain and idle margins. Profiling runs are separate from the throughput comparison. Put completed 138,762 operations at 5,549.1/s, with leader/follower process CPU of 76.46/13.62 us/op. Range completed 656,192 operations at 26,242.3/s, with 37.57/2.00 us/op. These are profile-run numbers, not extra benchmark repetitions. Profiled Range uses the seed and Put warmup state at revision 24,097, without a Range warmup; the paired Range windows use revision 84,097 after the measured Puts. Thus history depth and warmup differ. The short paired tigerstore Range windows lasted about 3.7 seconds, so they do not establish long-run behavior.

The table divides sampled CPU nanoseconds by the completed operations. Categories match stack ancestry with `attribute.py`. They are inclusive and overlap: TLS includes socket descendants, gRPC includes codecs, committer includes Raft/index/allocation, and WAL includes write/fsync. Do not sum them. A zero means no matching CPU samples, not proof of zero cost.

| Inclusive CPU us/op | Put leader | Put follower | Range leader | Range follower |
| --- | ---: | ---: | ---: | ---: |
| Total sampled CPU | 75.96 | 12.61 | 37.52 | 1.89 |
| gRPC | 26.59 | 0 | 20.24 | 0 |
| TLS, including socket work | 11.75 | 2.31 | 12.76 | 0.52 |
| Cryptographic implementations | 1.44 | 0.36 | 0.62 | 0 |
| Protobuf | 5.98 | 2.02 | 2.24 | 0.40 |
| Committer | 10.74 | 3.75 | 5.03 | 0.29 |
| Raft | 3.68 | 1.08 | 1.55 | 0.14 |
| Entry encode/decode | 4.04 | 2.09 | 0 | 0 |
| WAL total | 21.33 | 2.81 | 0 | 0 |
| WAL Flush/write path | 6.63 | 1.08 | 0 | 0 |
| File fsync path | 14.34 | 1.59 | 0 | 0 |
| MVCC/index | 1.44 | 1.51 | 1.07 | 0 |
| Allocation and GC | 8.00 | 2.45 | 4.56 | 0.34 |
| Peer transport | 11.10 | 5.40 | 10.23 | 1.43 |

Top flat Put-leader cost was `internal/runtime/syscall/linux.Syscall6`, 38.71% of sampled CPU. Its callers include file fsync/write and socket operations. After that, individual leaves were small: `runtime.memmove` 2.75%, `runtime.nanotime` 2.66%, and AES-GCM encryption 1.14%. The largest useful groups are gRPC and WAL, not encryption arithmetic or B-tree lookup. Put allocation stacks include protobuf value decoding, marshal buffers, protobuf merge copies, grpc-go headers and Raft messages. The leader's commit loop also re-encodes batches and every replica decodes them again at apply. Values and history remain in memory without compaction on this main commit.

Mutex profiling used fraction 1 and block profiling used a 1 ms sampling rate. Total measured mutex delay over each 30-second interval was only 6.704 us for the Put leader, 2.808 us for its follower, 24.435 us for the Range leader and 20.464 us for its follower. These samples came from runtime scheduling locks; no large application-mutex contention appeared with one Go execution core per process. This does not establish the same result with several cores.

Block profiles mainly show channel/select waits. Leader `Node.Do` accumulated about 2,026.68 seconds during Put, about 14.605 ms per completed request, and 1,583.04 seconds during Range, about 2.412 ms/request. Follower waits were mostly the idle committer and peer sender. grpc-go keepalive and the pprof handlers themselves also accumulate idle waits. These are overlapping goroutine waits, not CPU, fsync duration or an additive latency decomposition. Block profiles do not time filesystem syscalls.

The allocation profiles are archived as supporting stack evidence, not exact bytes/op. Go's delayed heap-profile accounting can carry warmup allocation records into the delta; the Range follower profile contains prior write-decode allocations despite no writes in the Range window.

The follow-up profiles both followers. During Range it measured 37.03, 2.14 and 13.68 process CPU us/op. The high-cost follower on CPU 2 had 12.71 sampled CPU us/op, including 9.19 in peer transport, 7.84 in syscall stacks and 7.42 in TLS-with-socket stacks. Those categories overlap. Cryptographic implementation cost was only 0.26 us/op. This identifies where that follower executes, not why its socket/scheduling cost differs from the other follower. No cause for the asymmetry was isolated.

## Disk follow-up and the upper-bound gap

The follow-up used the same workload and core layout, on a new runner, with optional atomic counters around existing file Write/Sync calls. It performed 128,005 profiled Puts in 25.035 seconds. Counter snapshots bracket the generator subprocess, including connection probes and drain; directory syncs are excluded. Segment-header file writes and syncs are included. No file errors occurred.

| Node | File sync calls | Completed Puts / file sync | Mean file write bytes | Mean write wall us | Mean file sync wall us | Sum file sync wall seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Leader | 70,610 | 1.813 | 1,999.9 | 15.51 | 273.12 | 19.285 |
| Follower 1 | 3,570 | 35.856 | 39,550.5 | 126.53 | 1,212.92 | 4.330 |
| Follower 2 | 5,046 | 25.368 | 27,982.6 | 77.08 | 992.71 | 5.009 |

The leader spends roughly 77% of that wall interval inside file fsync. Its accumulated file-sync wall cost is 150.66 us per completed Put; write calls add 8.56 us/op on the same disk goroutine. That is not 150.66 us of CPU or a claim about each request's critical path. In the Range follow-up, all three nodes recorded zero file writes and zero file syncs.

The largest demonstrated mismatch with the benchmark pre-work is durable batching:

- The pre-work's conditional 64 KiB model assumed 60 writes per fsync, amortizing 16.337 CPU us to 0.2723 us/write. Here the leader completed only 1.813 Puts per file sync, including metadata and rotation calls. That is about 33 times as many file syncs per operation as the model assumes. Its first profile attributed 14.34 CPU us/op to fsync, about 53 times that conditional amortized term. Different VMs, growing files and rotation prevent treating the primitive's per-call time as a controlled baseline.
- The main driver invokes `Process` after arrivals and disk completions; it does not hold new proposals until the previous append completes. The disk worker drains the jobs then queued. The observed small effective leader groups show that this does not achieve the design's assumed append-clocked grouping here. Testing append-clocked batching and hard-state/write coalescing is the first recommendation. A speedup from changing them remains unmeasured.
- Leader Put CPU of 75.67 us/op gives a CPU-only budget of about 13.2k/s, but measured throughput is 4.69k/s with only 0.337 occupied leader cores at the median. Saving instructions alone will not close that gap. Fsync/grouping and the resulting waiting deserve attention first.
- The complete no-op RPC reference was 19.037 server CPU us/op, while this profile has 26.59 us/op under grpc-go, plus asynchronous committer, disk and peer work. Those are different loads and definitions, not an exact subtraction. The current compiler/client library and concurrency also differ from the primitive. The integrated RPC path remains worth reducing, especially for Range, where gRPC accounts for about 54% of sampled leader CPU and the leader is nearly fully occupied.
- The memory-backed TLS primitive's roughly 0.45/0.50 us per KiB encrypt/decrypt is not comparable with the entire 11.75 us TLS stack, which contains sockets and fanout. Actual cryptographic routines cost only 1.44 us per leader Put here. Likewise, `MemoryStorage.Append` at 0.0368 us is not the same work as the 3.68 us real Raft path. Neither comparison justifies blaming encryption or consensus algorithms.
- MVCC/index is about 1.44 us per Put and 1.07 us per point Range on this 4,096-key dataset. It is not the leading cost here. That says nothing about prefix counts or a many-million-key index. Allocation/GC at about 8.00 us per leader Put remains a secondary target, including repeated protobuf/value copies.

The pre-work's 19.644 us partial-work budget, about 50,906 writes/s/core, excludes real peer replication, Raft, apply, index and watches. This run is about 10.8 times below that rate, but it does not prove a 10.8x achievable gain. No integrated upper bound has been established. The older etcd baseline also used 65,536 binary keys, longer windows and a different client, so its 184.2 leader CPU us/op cannot be compared with this run's 161.67 as a controlled improvement.

## Reproduction and evidence

On a disposable free ARM runner before the deadline:

```sh
git checkout 1186010d5f43007e4fc0d8d51779541e3a018b7d
# actions/setup-go installs Go 1.27.2; RUNNER_TEMP is provided by Actions.
timeout --kill-after=5s 800s bash experiments/pre50profile/run.sh

# Separate diagnostic job, not a replacement baseline.
git checkout bea9ee34347f57abb852a1c6063f8a29661f9612
PRE50_DETAIL=1 timeout --kill-after=5s 240s bash experiments/pre50profile/run.sh
```

The scripts execute and verify:

```sh
sudo mount -o remount,barrier=1 /
findmnt -T "$RUNNER_TEMP/pre50profile" -o TARGET,SOURCE,FSTYPE,OPTIONS
# Server processes: GOMAXPROCS=1 taskset -c <role-core> <binary> <flags>
# run.py records every complete server and client argv in commands.json.
GOMAXPROCS=1 taskset -c 3 "$RUNNER_TEMP/pre50profile/bin/load" \
  -endpoint 127.0.0.1:<leader-client-port> -certdir <cert-directory> \
  -pids <leader-pid>,<follower1-pid>,<follower2-pid> -hz 100 \
  -op put -total 60000 -workers 256 -out <result.json>
# Range uses -op range -total 100000. The default is 16 connections.
# Profile load uses -total 0 -duration 25s.
curl 'http://127.0.0.1:<debug-port>/debug/rates?on=1'
curl 'http://127.0.0.1:<debug-port>/debug/pprof/profile?seconds=30' > cpu.pb.gz
# Concurrent requests also collect mutex, block and allocs with seconds=30.
go tool pprof -top <matching-tigerstore-binary> cpu.pb.gz
```

`raw/` and `detail/` preserve JSONs, CPU/mutex/block/allocation profiles, pprof top output, complete commands, mount evidence, affinities, logs and executable identity. The matching symbol binaries are gzip-compressed with no certificate material. `summary.csv`, `attribution.json` and `detail-attribution.json` reproduce the tables. The generator saved latency aggregates, not individual samples or histograms, so the archived output cannot independently recompute its percentiles. Run `python3 experiments/pre50profile/summarize.py` and `python3 experiments/pre50profile/attribute.py` from the branch root. Use `attribute.py evidence/pre50profile/detail` for the follow-up.

Go recorded `vcs.modified=true` because the scripts created untracked evidence files before building. They did not rewrite tracked source. The follow-up additionally records `git status --short`, showing only untracked evidence, and checks `git diff --exit-code` before compilation. The first run did not archive that clean-diff check. All source changes are in the named experiment commits. Gofmt/gofumpt, go vet, golangci-lint and short tests passed for the changed Go packages before both code pushes. The generator package has no dedicated unit tests; the real-server result checks are the exercised validation here.

Proven here: the stated fixed-workload results on one paired runner, sampled CPU costs, small effective leader disk groups in a separate instrumented run, and no measured write/fsync calls during point Range. Unproven: maximum throughput, production Kubernetes behavior, crash durability, capacity, high-key-count index costs, multi-host performance, or any proposed optimization's gain. Do not close #50 from this pre-work. No tags, PRs or merges were created. Both Sol helpers were deleted after their source and result audits; their reports are archived here.
