# Pre-50 measurement audit

Audited on 2026-10-12. No benchmarks, subagents, repository writes, pushes, or GitHub comments. Checks used source inspection, JSON arithmetic, binary hashes, and local `go tool pprof -raw` analysis.

## Scope and verdict

The original measurements under `repo/evidence/pre50profile/raw` belong to commit `1186010d5f43007e4fc0d8d51779541e3a018b7d`. The completed follow-up under `repo/evidence/pre50profile/detail` belongs to HEAD `bea9ee34347f57abb852a1c6063f8a29661f9612`. I inspected both after the follow-up became available.

The CPU/op calculation is correct. The original paired results support a narrow comparison of these binaries on one four-core ARM VM, with one core per server, one generator core, 256 closed-loop workers, and 16 connections. They do not establish unloaded latency, latency at equal offered throughput, whole-machine CPU cost, production durability, or a causal breakdown of the difference between stores.

The main reporting risks are treating closed-loop latency as general latency, interpreting CPU samples as disk wait time, summing overlapping profile categories, and mixing the original results with the instrumented follow-up. The follow-up resolves the original missing high-CPU follower profile. Allocation deltas still cannot support precise allocation/op claims.

All source references below are relative to `repo`. Script line numbers refer to HEAD. `load/main.go` is unchanged from the original measured commit.

## Checks that passed

- All 56 original load JSONs have `valid=true`, no errors, and exactly one response Raft term. Term counts equal successful operation counts. The six follow-up load JSONs pass the same checks.
- Every CPU/op value reproduces from the saved tick delta, `clock_hz=100`, and successful operation count. Original occupied-core values also reproduce from tick deltas and elapsed time. The `/proc/PID/stat` offsets in `experiments/pre50profile/load/main.go:34-53` correctly select user time, system time, and process start time.
- Original measured Puts have 60,000 successes and revision `24097` to `84097`. Measured Ranges have 100,000 successes and unchanged revision `84097`. Seeds have 4,096 successes and warmups have 20,000. Follow-up profile revision deltas also match their operation counts.
- Saved PID/start-time pairs do not change within loads. Server PID order matches `placement.json`, so CPU arrays mean leader, first follower, second follower, generator. Saved thread affinities match cores 0, 1, and 2 in that role order.
- Original etcd status snapshots preserve leader and term 2. Afterward, all three nodes report applied index equal to Raft index `84104`. Tigerstore snapshots preserve their selected leader IDs.
- The original retained binary SHA-256 is `3116f2b0f2b337927a6d5d4a15809ed589098d4ec6983250d4d0cd90ca04ccd2`, matching `raw/versions.txt`. The follow-up retained binary hash is `7025b7598fac2445e5b6c340aae7294920ddaeb11542ff568b41d324b00c41bf`, matching `detail/versions.txt`.

### Original medians

CPU columns are microseconds per successful client operation. Total server CPU is the median of each run's three-server sum, not the sum of component medians. Latency columns are milliseconds.

| Store | Operation | Ops/s | Mean | p99 | p99.9 | Leader CPU | Total server CPU | Generator CPU |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Tigerstore | Put | 4693.66 | 54.514 | 111.177 | 158.174 | 75.667 | 100.000 | 26.500 |
| etcd | Put | 5051.08 | 50.621 | 85.034 | 134.287 | 161.667 | 217.833 | 48.833 |
| Tigerstore | Range | 27081.02 | 9.442 | 22.478 | 35.221 | 36.500 | 51.500 | 21.600 |
| etcd | Range | 8591.29 | 29.777 | 47.814 | 55.399 | 116.400 | 118.800 | 42.500 |

These reproduce `experiments/pre50profile/summarize.py:10-20`. The script computes separate column medians. Its median row is not one observed run, and its median percentiles are not pooled percentiles.

## Findings

### 1. High reporting risk: latency compares different achieved loads

`experiments/pre50profile/load/main.go:147-188` issues the next RPC only after the preceding RPC returns. There is no external arrival schedule. Each store gets the same concurrency, not the same arrival rate. Ranges run at about 27,081 ops/s on Tigerstore and 8,591 ops/s on etcd. Both leaders occupy about one core. Mean latency largely follows concurrency divided by throughput.

The recorded duration includes application queueing, gRPC/TLS work, server work, response delivery, validation, and cancellation between `time.Now()` and `time.Since()`. It excludes the wait before a worker starts its next operation. The client stops arrivals during slow responses. It therefore does not measure the backlog or latency an independent arrival stream would experience during a stall.

Report these as client-observed latencies at 256 closed-loop workers. Do not claim equivalent-load latency, unloaded service time, or an open-loop p99 improvement. A fixed-rate comparison would require another experiment, which this audit did not run.

### 2. Medium reporting risk: short runs and unstable tails limit Put claims

Original Tigerstore Range runs last only 3.65 to 3.75 seconds, versus 11.27 to 11.90 seconds for etcd, because both stop after 100,000 successes. Put runs last roughly 10 to 14 seconds. Five alternating pairs on one VM do not estimate host-to-host variability or long-run disk behavior.

Tigerstore Put throughput spans about 4,356 to 5,832 ops/s. It exceeds its paired etcd run only in repetition 4. Tigerstore Put p99 is worse in every pair. Its p99.9 is better in two pairs and worse in three. A blanket assertion that Tigerstore Puts are faster or have uniformly better tails contradicts the evidence.

The percentile formula in `load/main.go:233-234` correctly implements nearest rank. However, only aggregate percentiles remain in JSON. Without latency samples, histograms, or a time series, the audit cannot independently reproduce percentiles or locate stalls. `raw/1-etcd/n1.log` contains 101 slow-request warnings during the measured Put window. Later shutdown warnings and leader step-downs occur after measurements and should not be presented as measurement failures.

### 3. Medium scope limitation: process CPU/op is not whole-system or steady-state CPU/op

`load/main.go:193-214,241-244` measures each process's user plus system ticks during the load and divides by successes. This correctly measures process CPU, including background work within that interval. It does not measure disk wait, kernel worker CPU, interrupts outside process accounting, hypervisor steal, or work deferred until after the last response.

Samples occur sequentially before release and after `wg.Wait()`. Their intervals slightly exceed the elapsed interval and differ by process. At 100 ticks/s, one tick is 0.167 microseconds/op for a 60,000-operation Put run and 0.100 microseconds/op for a 100,000-operation Range run. Four decimal places from `summarize.py` should not imply that accuracy. Occupancy slightly above one in some etcd Range rows is consistent with tick resolution and mismatched boundaries, not evidence of broken affinity.

There is no idle-baseline subtraction or explicit follower catch-up barrier at CPU boundaries. Slower runs accrue more time-based heartbeat/background cost per operation. Short finite runs can omit trailing replication or storage work. Generator CPU excludes connection setup, key/value generation, final probes, sorting, and JSON output. Describe it as generator CPU during the measured load, not total generator cost.

### 4. High reporting risk: profile numerator and operation denominator cover different intervals

`experiments/pre50profile/run.py:119-135` starts 30-second profiles, sleeps one second, and then runs a 25-second arrival interval plus drain. Profiles also cover client initialization, TLS probes, pre/post probes, idle activity, and profile collection. There is no start barrier that proves the workload lies inside each profile interval.

For the original Put leader, CPU profiling begins at `23:50:07.166589086Z`, whereas the measured load begins at `23:50:08.236572997Z` and lasts `25.006317842` seconds. Range profiling begins at `23:50:44.050901065Z`, whereas its load begins at `23:50:45.120178139Z` and lasts `25.005083040` seconds. Actual original loads fit inside the profiles, but roughly five profile seconds are outside the measured interval.

`attribute.py:67-72` correctly converts sampled CPU nanoseconds to microseconds and divides by the profile load's successes. The resulting CPU/op is an approximate attribution for that profiled load, not an exact attribution of the original unprofiled median. It should retain the word "sampled" and the window caveat.

Do not multiply sampled CPU/op by 30/25 or 25/30 as an automatic correction. CPU time is already a measured numerator; removing extra-window work requires that work's CPU, not a duration ratio. Conversely, `pprof`'s samples divided by its 30-second duration describe profile-window occupancy, not load-window occupancy. Original Range leader samples give about 0.82 core over the profile window, versus 0.986 core in the load JSON.

Original sampled versus tick CPU/op provides a useful cross-check:

| Operation and role | Sampled CPU/op | Tick CPU/op |
| --- | ---: | ---: |
| Put leader | 75.957 | 76.462 |
| Put first follower | 12.612 | 13.620 |
| Range leader | 37.520 | 37.565 |
| Range first follower | 1.890 | 1.996 |

These differences are compatible with sampling and interval differences. They do not justify exact attribution percentages for the baseline.

### 5. Original profile coverage gap is resolved by the separate follow-up

Original `run.py` profiled only `order[:2]`. In `raw/profile-range`, role order is `[1,0,2]`. The unprofiled second follower, node 2, costs `13.075` CPU microseconds/op, versus `1.996` for the profiled first follower. It contributes about 25% of total server CPU. Doubling the low follower's profile would badly undercount follower work.

HEAD profiles all three nodes when `PRE50_DETAIL=1`, and the completed follow-up contains all 24 profile files. Its role order is `[2,0,1]`, so node 2 is now the leader and node 1 is the high-CPU Range follower. Compare roles through `placement.json`, not filenames.

In `detail/profile-range`, high follower node 1 has sampled CPU `12.714` microseconds/op and tick CPU `13.677`. Inclusive peer transport accounts for `9.193` sampled microseconds/op, TLS for `7.418`, and syscalls for `7.839`. Those categories overlap. This supports the user's narrower statement that the high follower does peer/socket work. It does not prove that all its extra CPU has one cause.

`attribute.py:63-64` silently skips absent CPU files. Consumers should inspect completeness before treating output as three-node attribution. That weakness did not hide a missing file in the completed follow-up.

### 6. High reporting risk: CPU, block delay, mutex delay, and disk wall time are different quantities

CPU samples on `os.File.Sync` or `syscall.Fsync` measure on-CPU work in that call path, not the elapsed time waiting for storage. A small fsync CPU share cannot rule out a disk-limited workload. A syscall-heavy CPU profile does not identify physical I/O latency by itself.

Block profiles sum sampled goroutine blocking durations. They are not an additive breakdown of client latency or elapsed time, and idle channel waits are not necessarily contention. `raw/profile-put/n1-block.pb.gz.top.txt` reports 2,660.77 seconds of delay in a 30-second profile. That is valid concurrent wait accounting, not 2,660 seconds of service time. In `raw/profile-range/n0-block.pb.gz.top.txt`, 60 of 118.81 seconds come from the profiler's own HTTP handlers. Report relevant stacks rather than total block seconds/op as application latency. Mutex delay also needs its own units and contention interpretation.

The follow-up disk counters are wall timers around `osfs.file.Write` and `Sync` in `internal/osfs/pre50.go:20-40`. They exclude directory syncs, do not count physical disk requests, and cannot prove power-loss behavior. Their snapshots surround the whole load command, not exactly the CPU interval. They include scheduling delay within calls and omit later work after the final snapshot.

I independently reproduced the leader Put follow-up values from `detail/profile-put/disk.json`, whose arrays follow role order `[2,0,1]`:

- 128,005 successful Puts and 70,610 file Sync calls give 1.812845 Puts per file Sync.
- 141,210,732 written bytes and 70,610 writes give 1,999.868744 bytes per Write.
- 19,284,766,746 accumulated Sync nanoseconds give 273.116651 microseconds per Sync.
- Accumulated leader Sync wall time is 19.284767 seconds, versus 25.035253 seconds of measured load.

These numbers support poor leader batching and substantial Sync wall time in this follow-up. They are not 273 microseconds of client latency per Put, nor proof that fsync alone explains the whole latency. Followers sync concurrently, so adding node wall times does not give elapsed time or a critical-path decomposition. The Range follow-up reports zero Write and Sync deltas on all nodes.

### 7. Medium reporting risk: attribution rows overlap and there are no etcd profiles

`attribute.py` counts a CPU sample in every category whose regex matches any frame. This correctly avoids counting the same sample twice within one category, but categories are inclusive and overlap. TLS includes socket descendants, gRPC includes protobuf/TLS/socket descendants, and WAL includes write/fsync paths. Do not sum category CPU/op values, or call the TLS row pure encryption CPU. The separate crypto-implementation row is closer to that question, but still depends on stack classification.

The parser assumes the CPU raw format contains count followed by nanoseconds. That assumption matches the inspected Go 1.27.2 CPU profiles. It is not a generic parser for four-column allocation profiles or other sample types.

No etcd CPU, allocation, mutex, or block profiles exist in either evidence set. Tigerstore profiles identify candidate optimization work within Tigerstore. They cannot establish which component causes the measured cross-store CPU difference.

### 8. Medium comparability limitation: profile runs have different history and instrumentation

Original measured Ranges follow seed, Put warmup, 60,000 measured Puts, and 10,000 Range warmups. Their revision is `84097`. Both original and follow-up Range profiles instead follow seed and Put warmup only, at revision `24097`, without a Range warmup. They therefore differ in version history, retained memory, and workload warmup.

CPU, full-rate mutex, sampled block, and allocation collection run together. HEAD also adds file wrappers and enables disk timing for detail runs. The follow-up is one profiled run per operation on another VM instance, not five new paired unprofiled runs. Keep `raw` medians and `detail` diagnostics separate. Do not substitute the follow-up throughput or tails into the original comparison.

Allocation profiles are genuine 30-second deltas. In the measured Go 1.27.2, `net/http/pprof` honors `seconds=30` for allocs, block, and mutex by subtracting two snapshots. Calling these lifetime profiles would be wrong. However, runtime memory profiles can lag by up to two GC cycles and use a 524,288-byte sampling period. Warmup allocations can enter the delta, and final load allocations can remain unreported. An `inuse_space` delta is a change in sampled retained space, not allocated bytes or total heap size. The user's decision not to claim precise allocation/op is correct.

### 9. Status and correctness assertions are useful, but weaker than a linearizability or durability test

`load/main.go:170-180,246-247` verifies one key/value per successful Range, a nonnil positive-revision header, one term among successes, no errors, exact fixed-count success totals, and a global revision delta matching successful Puts or unchanged for Ranges. Invalid measurements write JSON and exit nonzero. These are meaningful checks and pass in the evidence.

They do not assert:

- A positive Raft term, expected cluster/member IDs, or agreement between response terms and placement/status terms.
- Distinct Put response revisions, correct per-key versions, or each Put's persisted value.
- Current value visibility under overlapping writes, because every overwrite restores the same bytes.
- Follower catch-up at each CPU boundary, equal data on all replicas, or successful crash/restart recovery.

`run.py:141-146` checks Tigerstore leader IDs only. Its debug status has no term or applied index. The response-term map detects a term change within an individual load, but not one between loads or a return to the same leader with a higher term. etcd's before/after leader and term checks are stronger, but they are snapshots rather than continuous monitoring.

Do not call `valid=true` proof of linearizability, durable acknowledgment under failure, or full replica convergence. The saved etcd applied indices offer stronger end-of-cluster evidence, but the script does not assert their equality.

## Verified workload

`experiments/pre50profile/load/main.go:103-112,150-172` uses exactly 4,096 keys. Every key is 34 bytes, formatted `/registry/pods/ns-%03d/pod-%08d`, across 64 namespaces. Each value is exactly 1,024 bytes made from 32 SHA-256 outputs tied to the key index and chunk index. Values differ by key but never change across overwrites or loads.

The multiplier `2654435761` is odd and therefore permutes the 4,096-key space modulo 4,096. Each full cycle visits every key once. This is a deterministic repeated access order, not independent random key selection. Concurrent completion order can differ. The seed covers the whole keyspace by construction.

Measured operations are single-key Puts and single-key Ranges through direct etcd protobuf RPCs to the selected leader. Range requests leave `Serializable` false, so they request linearizable reads. Range validation checks count, returned key, and all value bytes. There are no prefix scans, Kubernetes object decoding, watches, transactions, leases, changing-value workloads, or follower-client traffic. Kubernetes-shaped keys alone do not make this a Kubernetes workload.

The same client binary and flags serve both stores. Both use client mutual TLS with TLS 1.3. Pinning, one-thread Go scheduling, key/value dimensions, and operation counts match. The binaries do not use the same toolchain: Tigerstore and the driver use Go 1.27.2, whereas released etcd 3.7.2 uses Go 1.26.8. This is a valid released-binary comparison, not an isolated source-code or compiler-controlled comparison.

## Environment and provenance limits

`raw/environment.txt` records the filesystem before remount, including `nobarrier`. `raw/mount.txt` records the post-remount options without `nobarrier`. `run.sh:20-23` checks ext4 and rejects disabled barriers after remount. It would be incorrect to reject the run solely from the earlier environment snapshot. Post-remount options still include `journal_async_commit`, `commit=30`, and `data=writeback`, on an Azure virtual disk. Neither these checks nor successful fsync calls prove end-to-end physical durability.

The original build records `vcs.modified=true`, plausibly because evidence was already untracked. It lacks the new clean-diff guard and saved build status. The follow-up records `?? evidence/`, consistent with that explanation. Revision metadata and matching retained binary hashes anchor the artifacts, but the original artifacts alone do not prove the absence of tracked source edits at build time. I found no evidence that such edits occurred.

## Reporting recommendation

Publish the original paired rows and medians with the stated workload and core limits. Say Tigerstore used less measured server process CPU per successful operation in these runs, delivered higher closed-loop Range throughput, and had worse paired Put p99. Describe profiles and disk counters as separate diagnostic measurements, with overlapping categories and distinct time units. The completed follow-up covers both followers and supports peer/socket work on the high-CPU Range follower and small leader Put batches. Do not claim precise allocation/op or causal cross-store profile differences.
