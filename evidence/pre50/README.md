## Pre-work for #50 and the paused load generator

Measured on free `ubuntu-24.04-arm` runners. Nothing merged. Read #46, #1, #26, #28, #54, this issue and `docs/design.md` at main `91c4f97`.

- [Baseline experiment branch](https://github.com/djosh34/tigerstore/tree/exp/pre-50-bench), measured code [`e6a8ddc`](https://github.com/djosh34/tigerstore/tree/e6a8ddce19d6f78981a3019e5d723893dcea08f8/experiments/pre50), successful [run 38316625259](https://github.com/djosh34/tigerstore/actions/runs/38316625259).
- [Primitive experiment branch](https://github.com/djosh34/tigerstore/tree/exp/pre50-primitives), measured code [`28edcb7`](https://github.com/djosh34/tigerstore/tree/28edcb7fe4d96394e814331d4987961f79857087/experiments/pre50-primitives), successful [run 38316850699](https://github.com/djosh34/tigerstore/actions/runs/38316850699). [Detailed methods, audit and archived raw results](https://github.com/djosh34/tigerstore/tree/cdac73a913de2f9131362f9f39de2464aebbffa0/docs/experiments/pre50-primitives).
- [Baseline raw results, JSON summaries and load-generator probes](https://github.com/djosh34/tigerstore/tree/exp/pre-50-bench/evidence/pre50). Archives preserve the evidence beyond Actions artifact expiry.

### Runner and barriers

All three successful jobs exposed four Neoverse-N2 cores, one thread per core, AES/PMULL instructions, 128 MiB shared L3, and 16,722,010,112 bytes RAM. Image `ubuntu24-arm64 20261004.142.1`, Linux `6.17.0-1022-azure`. Disk was one 150 GiB virtual `sda`, with ext4 `/dev/sda1` mounted at `/`. Filesystem capacity was 154,894,188,544 bytes and initial available space about 114.58 GB. `ROTA=1` describes the virtual device, not proof of physical spinning media. These are observed allocations, not a promise about future runners.

#26 left `nobarrier` enabled. #28 remounted the existing root filesystem, rather than creating a loop device. I repeated that setup. Exact commands, only on the disposable runner:

```sh
lscpu
free -b
lsblk -b -o NAME,SIZE,TYPE,ROTA,MODEL,MOUNTPOINTS
df -B1 "$PWD"
findmnt -T "$PWD"
sudo mount -o remount,barrier=1 /
findmnt -T "$PWD" -o TARGET,SOURCE,FSTYPE,OPTIONS
test "$(findmnt -T "$PWD" -n -o FSTYPE)" = ext4
! findmnt -T "$PWD" -n -o OPTIONS | grep -Eq 'nobarrier|barrier=0'
```

The resulting options were `rw,relatime,discard,journal_async_commit,errors=remount-ro,commit=30,data=writeback`. Default-enabled barriers need not appear as `barrier=1`. All data files were on this mount. No loop filesystem was needed or measured. Successful fsync and guest barriers do not prove physical power-loss protection or the hypervisor's persistence contract. Three replicas here share one failure domain.

Five repetitions per disk case, one pinned writer, initialized 64 MiB files, cyclic overwrites, two-second windows. `File.Sync` means fsync, whereas etcd normally uses fdatasync. Values below are medians of per-run statistics, in microseconds per batch.

| Batch | Write+fsync mean wall | Write+fsync CPU | Fsync-only mean wall | Fsync-only CPU | Write+fsync p50 / p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 4 KiB | 80.020 | 17.040 | 75.073 | 12.762 | 67.922 / 192.070 |
| 64 KiB | 186.556 | 27.812 | 174.442 | 16.337 | 90.123 / 4,298.948 |
| 1 MiB | 2,940.971 | 128.839 | 2,850.996 | 41.696 | 482.711 / 9,158.978 |

Completed write+sync rates were 12,496.8, 5,360.3 and 340.0 batches/s. The probe-pair overhead was 0.852 us and was not subtracted. These tests exclude file growth and segment installation. Do not substitute the 1 MiB p50 for its much larger mean when estimating throughput. The difference from #28's fsync numbers is not a controlled speedup; initialization, VM and workload differ.

### Etcd's own benchmark, five repetitions

Unmodified release etcd **v3.7.2**, upstream commit `68c065e562994b89e333e77b039ad066f933c586`, built with Go **1.26.8**. Etcd SHA-256 `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d`. Upstream `tools/benchmark` came from that same source commit and was built with Go **1.27.2**. Its SHA-256 was `62d144d8899b4e4d80438a9d2416ac528842c1acc7db427e2abb275f14e3ccca` in both jobs.

Two candidate layouts for #54:

| Layout | Leader n0 | Follower n1 | Follower n2 | Generator |
| --- | --- | --- | --- | --- |
| isolated | CPU 0, GOMAXPROCS=1 | CPU 1, GOMAXPROCS=1 | CPU 2, GOMAXPROCS=1 | CPU 3, GOMAXPROCS=1 |
| leader2 | CPUs 0,1, GOMAXPROCS=2 | CPU 2, GOMAXPROCS=1 | CPU 2, GOMAXPROCS=1 | CPU 3, GOMAXPROCS=1 |

Processes launch with `taskset -c <list>` and the stated `GOMAXPROCS`. Every observed server thread's affinity was checked. The driver transfers leadership to n0 before warmup and checks the leader afterward. No proxy or injected delay. The two layouts ran in separate jobs, not as a paired layout comparison on one allocated VM.

Both client and peer links use certificate-authenticated TLS, with `--tls-min-version=TLS1.3 --tls-max-version=TLS1.3`. Nodes use separate data directories, an 8 GiB quota, default safe WAL/backend persistence, and default heartbeat, election, snapshot and backend batching settings. No auth policy, watches, compaction or defrag. Five fresh clusters per layout; each gets 20,000 warmup Puts, a one-second pause, then 300,000 measured Puts. The measured generator opens fresh connections, so connection setup is included.

Exact measured command, with the test certificate paths abbreviated:

```sh
GOMAXPROCS=1 taskset -c 3 ./benchmark \
  --endpoints=https://127.0.0.1:2379 \
  --cacert=cert.pem --cert=cert.pem --key=key.pem \
  --conns=16 --clients=256 --precise \
  put --key-size=64 --val-size=1024 \
  --key-space-size=65536 --total=300000
```

This is closed-loop load against the leader, with random selection from 65,536 binary keys. It is not the Kubernetes-shaped, open-loop workload that the later generator must supply. It is a fixed concurrency measurement, not a demonstrated maximum. All ten windows had zero reported RPC errors, exactly 300,000 leader revision increments and zero leader-change counter increments.

| Layout | Rep | Put/s | Mean ms | p50 ms | p99 ms | p99.9 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| isolated | 1 | 4,499.0 | 56.833 | 51.495 | 115.148 | 250.037 |
| isolated | 2 | 4,610.6 | 55.443 | 50.815 | 102.892 | 117.531 |
| isolated | 3 | 4,707.9 | 54.319 | 49.782 | 102.231 | 118.316 |
| isolated | 4 | 4,698.8 | 54.403 | 49.538 | 102.384 | 124.546 |
| isolated | 5 | 4,722.7 | 54.148 | 49.429 | 101.927 | 116.686 |
| leader2 | 1 | 8,278.0 | 30.857 | 25.493 | 90.390 | 313.841 |
| leader2 | 2 | 9,041.3 | 28.222 | 24.899 | 56.518 | 68.510 |
| leader2 | 3 | 8,969.0 | 28.458 | 25.092 | 56.591 | 67.994 |
| leader2 | 4 | 8,997.7 | 28.372 | 24.940 | 57.440 | 70.950 |
| leader2 | 5 | 9,010.8 | 28.330 | 24.713 | 58.278 | 70.073 |

Median rates are **4,698.8** and **8,997.7 Put/s**. These do not contradict #26's 27k/s. That experiment had different placement, concurrency, 256-byte values, plaintext links and `nobarrier`. This experiment does not isolate which change caused the difference.

### Request costs and conditional one-core budgets

All primitive cases ran five times with one pinned CPU per process and `GOMAXPROCS=1`. Go **1.27.2**, etcd API **3.7.2**, Raft **3.7.0**, grpc-go **1.83.2**, protobuf **1.36.11**, x/net **0.58.0**. The primitive runner was a separate VM from the baseline jobs. Costs are process user+system CPU, not elapsed I/O wait.

The last column is `1,000,000 / CPU_us`, for one invocation per write. It is an implementation budget if that step were the only CPU cost, not a database throughput claim. For fsync, `B` is writes per batch. Overlapping rows must not be added.

| Step | Median CPU us | Wall measurement | CPU-only writes/s/core |
| --- | ---: | --- | ---: |
| TLS 1.3 record decrypt, 1 KiB | 0.49799 | 0.49794 us | 2.008M |
| TLS 1.3 record encrypt, 1 KiB | 0.44795 | 0.44824 us | 2.232M |
| HTTP/2 DATA frame parse, 1 KiB | 0.12073 | 0.12073 us | 8.283M |
| gRPC envelope + PutRequest decode | 0.53246 | 0.53254 us | 1.878M |
| gRPC envelope + PutRequest encode, client-side reference | 0.51119 | 0.51122 us | 1.956M |
| PutResponse envelope + encode, 11-byte fixture | 0.17009 | 0.17011 us | 5.879M |
| TLS record encrypt, same small reply | 0.19178 | 0.19183 us | 5.214M |
| Stock raft.MemoryStorage.Append | 0.03680 | 0.03680 us | 27.17M |
| Custom entry metadata + 1 KiB copy | 0.03611 | 0.03612 us | 27.69M |
| Entry protobuf + framed buffered file append, no sync | 0.29794 | 0.29792 us | 3.356M |
| Fsync of a 64 KiB batch | 16.337 per batch | 174.442 us mean per batch | 61,211 × B |
| Cached memcpy, 1 KiB | 0.02469 | 0.02469 us | 40.50M |
| Three-node TLS exchange, memory copies only, leader CPU | 39.727 | Quorum p50 36.889 us, p99 56.266 us | 25,172 |
| Three-node TLS exchange, fsync before ack, leader CPU | 102.418 | Quorum p50 347.547 us, p99 779.520 us | 9,764 |
| Complete no-op TLS gRPC Put, 64 workers, server CPU | 19.037 | RPC p50 1,097.850 us, p99 3,152.718 us | 52,529 |
| Actual etcd isolated leader, median | 184.200 | End-to-end table above | 5,429 |
| Actual etcd leader2 leader, median | 192.800 | End-to-end table above | 5,187 per core |

TLS measurements use actual `crypto/tls` record processing with `TLS_AES_128_GCM_SHA256`, but memory-backed connections, not sockets. Pure AES-GCM seal/open costs were 0.31844/0.32498 us per KiB. HTTP/2 parsing excludes HPACK, flow control and dispatch. Request codecs allocate fresh messages/buffers; their fixture has a 32-byte key and 1 KiB value, 1,061 encoded bytes. An ordinary server encodes a small PutResponse, not another 1 KiB PutRequest.

`MemoryStorage.Append` retains prebuilt immutable entry pointers without copying the value. It is not `RawNode.Propose`, consensus or durable append. The buffered-file test includes ordinary 64 KiB flushes into page cache, but no fsync, WAL checksum, rotation or recovery format. The final partial-buffer flush is recorded separately. Cached memcpy is not DRAM bandwidth.

The quorum prototype overlaps the leader's local append with sends to both followers. Quorum timing stops after local completion plus the first follower ack. It drains the other follower before the next entry. Measured all-drained rates were 19,191.2/s without fsync and 2,380.5/s with fsync. Follower CPU costs were 22.465/21.509 us and 98.244/82.076 us respectively. These are three-process exchanges, not a Raft implementation or pipelined quorum capacity.

The complete no-op RPC reached 46,779/s with 64 workers, server CPU 19.037 us and client CPU 18.788 us. One worker reached 9,495/s and cost 58.892 server CPU us/op. Scheduling and batching change CPU/op. The complete RPC already includes TLS, HTTP/2 and codecs; do not add those rows again.

For a declared model of 1,088 bytes per stored write, a 64 KiB batch holds 60 writes. Fsync CPU amortizes to `16.337/60 = 0.2723 us/write`. Combining complete no-op RPC, stock memory append, buffered append and this fsync term gives `19.037 + 0.03680 + 0.29794 + 0.2723 = 19.644 us`, or about **50,906 writes/s per leader core before peer replication, real Raft, apply, index and watches**. This is a conditional partial-work budget, not proof that tigerstore can reach it. A full integrated upper bound remains unproven.

At etcd's observed CPU costs, the isolated leader's CPU-only budget is 5,429 Put/s. Two leader cores give 10,373 Put/s at the leader2 cost. Actual throughput was lower because CPU execution is not the only constraint. These are not immutable hardware ceilings, and no CPU profile here attributes etcd's remaining cost to individual subsystems.

### Measuring server CPU per operation

Read `/proc/<pid>/stat` before and after the same successful-operation window. Parse after the final `)` because the command name may contain spaces or parentheses. Fields 14 and 15 are user and system ticks; field 22 is start time. Use `getconf CLK_TCK`, which was 100 here. Reject a missing process or changed start time.

```python
fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
ticks = int(fields[11]) + int(fields[12])
starttime = int(fields[19])
# For matching before/after samples and N successful operations:
cpu_us_per_op = 1e6 * (after_ticks - before_ticks) / CLK_TCK / N
occupied_cores = (after_ticks - before_ticks) / CLK_TCK / elapsed_seconds
```

The process value already includes its threads. Do not sum thread CPU again or add child times. Sum the three process costs only when reporting total server CPU/op, and report the generator separately. It includes process runtime/GC and charged system CPU, but not I/O wait or all asynchronous kernel-worker/IRQ work.

| Layout / rep | Leader CPU us/op | Follower1 | Follower2 | All servers | Generator |
| --- | ---: | ---: | ---: | ---: | ---: |
| isolated / 1 | 184.567 | 39.600 | 37.267 | 261.433 | 54.167 |
| isolated / 2 | 184.200 | 39.367 | 37.100 | 260.667 | 54.400 |
| isolated / 3 | 182.967 | 39.433 | 36.867 | 259.267 | 54.500 |
| isolated / 4 | 183.233 | 39.133 | 37.000 | 259.367 | 54.367 |
| isolated / 5 | 184.500 | 39.733 | 37.367 | 261.600 | 55.000 |
| leader2 / 1 | 192.800 | 34.533 | 35.067 | 262.400 | 49.433 |
| leader2 / 2 | 191.933 | 34.667 | 34.267 | 260.867 | 49.033 |
| leader2 / 3 | 192.967 | 35.033 | 34.233 | 262.233 | 49.933 |
| leader2 / 4 | 192.533 | 35.100 | 34.433 | 262.067 | 49.367 |
| leader2 / 5 | 194.533 | 34.500 | 35.167 | 264.200 | 49.700 |

Median aggregate server CPU was 260.667 and 262.233 us/op. Median occupied server cores were 0.860/0.184/0.173 and 1.730/0.313/0.309. Etcd's `process_cpu_seconds_total` deltas agreed with `/proc` within 0.2 us/op; scrapes have different boundaries. A tick over 300,000 operations is 0.0333 us/op. The CPU window surrounds the entire benchmark subprocess, including its setup, drain and output, while the tool's throughput uses its own internal timer. The raw evidence records both intervals. Generator CPU uses `/usr/bin/time` to avoid an exit-time `/proc` race.

### Paused load generator and pitfalls

Remote `work/50-loadgen` did not exist when checked. The local branch did exist at `91c4f97` with uncommitted files. I copied it read-only for inspection and probes; the worker's checkout is untouched. [Snapshot, tests and full findings](https://github.com/djosh34/tigerstore/tree/exp/pre-50-bench/evidence/pre50/loadgen-review).

The later worker should fix these before publishing comparisons:

- Client and peer traffic are still plaintext. The scripts neither enable nor verify barriers, configure delay, nor record the actual layout and binary identity.
- Latencies include only completions inside the window. A probe with 200 ms responses and a 100 ms window drained six successes but reported zero latency samples and zero unsent. Keep completion-window throughput separate from arrival-cohort latency through drain, and report censored/failed requests.
- CPU-sampling failure can leave `errors={}` and zero CPU/op in the written result. The reporter also accepts failed runs. Add explicit validity and reject invalid rows.
- Empty or duplicate unary responses count as successful operations. Require exactly one validated response.
- Stale repetition files change the chosen offered rate. Failed cluster startup leaves sibling processes running. Use fresh run directories and cleanup traps.
- Adjacent 10 KiB values share 6,141 bytes. A probe compressed their batch to 42.25% with gzip, versus 100.01% for non-overlapping random data. This proves input correlation, not a server compression gain.

The linked report also covers early closed-loop starts, pre-write service timestamps and accepted NaN/infinite rates. Short tests, vet and race probes passed; they assert the defects, not fixes. Etcd integration tests were not run against this snapshot.

My first baseline run, [38315892754](https://github.com/djosh34/tigerstore/actions/runs/38315892754), completed all ten measurement windows but failed afterward deleting read-only Go module-cache directories. The fix makes the owned tree writable before deletion. The successful rerun above is reported separately; the first run's raw data remains archived. No explanation based only on a green rerun is needed here: the failing cleanup command and permissions are in the original log.

Reproduce on a fresh disposable runner with `LAYOUT=isolated bash experiments/pre50/run.sh`, or `LAYOUT=leader2`, from the baseline branch. The primitive branch runs `timeout 660 bash experiments/pre50-primitives/run.sh` after the remount. Both workflows trigger only on their experiment branches and have 15-minute job limits. Full certificate generation, node flags, commands, hashes, mount checks and raw counters are committed or archived at the links above.

No tigerstore speedup, real crash durability, capacity maximum, delayed-network result or Kubernetes result is established by this pre-work. Both Sol helpers were deleted. Owned caches, temporary files and build output were cleaned. The experiment branches remain for the later worker; nothing should be merged from them.
