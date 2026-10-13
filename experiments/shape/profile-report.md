# Runner CPU and trace diagnostics

## Evidence and limits

CPU run [38415945699](https://github.com/djosh34/tigerstore/actions/runs/38415945699) contains nine valid Tigerstore cohorts and 27 CPU profiles. All nine `load.json` and `audit.json` files report valid, with no request, sampling or invariant errors. Retain these profiles as Tigerstore diagnostics, not a complete passing paired comparison. The balanced 1 KiB, 4096-concurrency job failed on etcd: 515 `too many requests` errors and 1847 ambiguous timeouts, 45,752 successes of 48,114 requests, and revision delta 47,599 rather than 48,114. No failed etcd window is included below.

Trace run [38415950650](https://github.com/djosh34/tigerstore/actions/runs/38415950650) passed. Its three Tigerstore cohorts and audits are valid. CPU and trace jobs use different VMs and durations, so their differences are not estimates of profiler overhead or repeatable performance changes.

The recorded source is `99cd39439b62ccfb80792f36f51d4a5e98c23797`. Every downloaded matching Tigerstore binary has SHA-256 `eec2f3e8fda3b27dc1ded4d468c6bf141dda7a6f6b2944b7d635451196089dcf`, with CPU profile build ID `43615ee7092c14270ffdbeab9bdd56ea6c9a3aa0`. The runner records only untracked `bin/` and `results/`, and build metadata says `vcs.modified=true`. This is the observed experiment executable, not byte-identical uninstrumented main. These artifacts contain no disabled-versus-enabled observation control.

Free `ubuntu-24.04-arm`, four Neoverse-N2 CPUs, Go 1.27.2. Leader CPU0, followers CPU1/2, generator and sampler CPU3. Servers have GOMAXPROCS=2 and the generator has one. Client and peer TLS remain enabled. All replicas share the runner's ext4 virtual disk, mounted with `data=writeback,journal_async_commit,commit=30` and no `nobarrier` flag. No injected regional delay. Each cell has one diagnostic repetition, 18 client connections and fully drained seed/warmup before the measured cohort. Put and point Range use 4096 keys; LIST returns all 1000 seeded 1 KiB values.

Artifacts are under `results/cpu/shape-<cell>/<cell>/tigerstore-1/` and `results/trace/shape-<cell>/<cell>/tigerstore-1/` in the parent experiment directory. `n0` is the leader slot from `placement.json`, not necessarily Raft member 1. Matching executables are under each artifact's `binaries/`. All analysis was offline. No executable under test, benchmark, profiler capture or runner was started locally.

## CPU per successful cohort operation

CPU captures last six seconds around five-second arrival cohorts, with about 0.2 seconds of lead and 0.8 seconds of tail. The tail includes drain and observation/fence work. Every number below is sampled CPU nanoseconds divided by `1000 * load.shape.success`, including those capture margins. These are estimates normalized to that cohort, not exact CPU consumed exclusively inside its arrival window. One 10 ms CPU sample represents about 0.04 us/Put in the 1 KiB leader-256 cell, but about 2.55 us/LIST in leader LIST.

### Disjoint leader contexts

Units are sampled CPU microseconds per successful operation. Each full sample stack belongs to exactly one column. The owner includes frontend completion callbacks it invokes; they are not also charged to frontend writers. Other includes runtime-only stacks, debug HTTP, observation and unknown contexts. The columns sum to Total before rounding.

| Cell | Successes | Reader | Writer | Owner | WAL worker | Peer writer | Peer reader | Other | Total |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Put 1 KiB, leader, 256 | 226069 | 5.574 | 2.787 | 2.079 | 1.371 | 1.991 | 0.265 | 1.106 | 15.172 |
| Put 1 KiB, leader, 4096 | 406092 | 3.595 | 0.197 | 1.921 | 0.443 | 1.354 | 0.000 | 0.492 | 8.003 |
| Put 10 KiB, leader, 256 | 36002 | 9.444 | 3.055 | 7.777 | 4.444 | 14.999 | 0.278 | 2.778 | 42.775 |
| Put 1 KiB, balanced, 256 | 182157 | 3.019 | 1.208 | 4.392 | 1.482 | 3.788 | 3.404 | 1.812 | 19.104 |
| Put 1 KiB, balanced, 4096 | 494908 | 1.293 | 0.343 | 2.162 | 0.303 | 2.081 | 0.828 | 0.566 | 7.577 |
| Point Range, leader, 1024 | 587824 | 2.875 | 2.688 | 1.565 | 0.000 | 0.476 | 0.102 | 0.783 | 8.489 |
| Point Range, balanced, 1024 | 1024337 | 1.289 | 1.025 | 0.996 | 0.000 | 0.693 | 0.537 | 0.264 | 4.803 |
| LIST, leader, 64 | 3929 | 12.726 | 1018.071 | 152.711 | 0.000 | 15.271 | 5.090 | 81.446 | 1285.314 |
| LIST, balanced, 64 | 5831 | 6.860 | 277.825 | 41.159 | 0.000 | 30.869 | 13.720 | 120.048 | 490.482 |

Balanced numbers use all successful cluster operations as the denominator, not just requests landing on the leader. They do not mean each individual operation executed each leader frontend context. Zero means no matching CPU sample, not zero execution cost.

Sampled follower1/follower2 CPU per cohort success is 4.954/4.645 us for leader Put-1K-256, 2.438/2.389 for leader Put-1K-4096, 17.221/16.944 for leader Put-10K-256, and 6.284/6.021 for balanced Put-1K-4096. Balanced LIST costs 459.612/464.757 us on the followers, while leader-only LIST costs 7.636/15.271. Distributing LIST moves frontend/index work to the followers; it does not remove the response bytes or establish a scaling ceiling.

### Major inclusive mechanisms

These columns overlap with each other and with the context table. Do not add them. TLS includes socket descendants, HTTP/2 includes its reads, core includes Raft and MVCC, and allocation/GC work may occur under any of them.

| Put cell | TLS | Socket calls | HTTP/2 + HPACK | Core | Raft | MVCC | WAL | File fsync | Allocation + GC |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB, leader, 256 | 6.281 | 4.689 | 3.937 | 1.327 | 0.310 | 0.442 | 1.371 | 0.840 | 1.371 |
| 1 KiB, leader, 4096 | 2.241 | 1.330 | 1.995 | 1.650 | 0.197 | 0.763 | 0.443 | 0.098 | 1.207 |
| 10 KiB, leader, 256 | 20.832 | 8.611 | 6.944 | 7.222 | 3.055 | 3.055 | 4.444 | 1.944 | 2.778 |
| 1 KiB, balanced, 256 | 6.643 | 4.557 | 2.361 | 3.733 | 1.976 | 0.878 | 1.427 | 0.769 | 2.361 |
| 1 KiB, balanced, 4096 | 2.950 | 1.697 | 0.869 | 2.122 | 0.525 | 0.950 | 0.303 | 0.101 | 1.132 |

For leader LIST, inclusive TLS is 674.472 us/LIST, socket calls 363.960, MVCC 104.352, allocation/GC 185.798 and `runtime.memmove` 213.795. Its dominant sampled context is the response writer, not Raft. For leader point Range, reader plus writer is 5.563 us/op, versus 1.565 under the owner and only 0.068 inclusive under Raft. All four read cells have zero fenced file-write/fsync deltas.

At 10 KiB the peer writer is the largest disjoint leader context. Inclusive TLS costs 20.832 us/Put and `runtime.memmove` 7.500. Inspect byte movement and peer preparation; these stacks do not isolate encryption arithmetic as the bottleneck. At 1 KiB, higher closed-loop concurrency amortizes frontend, peer and WAL costs, while sampled owner cost per operation changes much less. Neither contrast certifies an optimization or a sustainable open-loop rate.

## Trace waits, correctly attributed

The trace cohorts are four seconds, not five. The leader captures are only about two seconds, starting 0.500 to 0.502 seconds after cohort start. Their totals cannot be divided by full-cohort successes and called per-request wait. The following values are seconds of goroutine wall state during that middle capture, not CPU seconds or additive request latency.

Standard `go tool trace -pprof=sched` stacks describe the event that made a goroutine runnable. They often name its waker, not the goroutine that waited to run. For example, the 1 KiB-256 scheduler top attributes 5.915 seconds to stacks containing the owner, but that is not the owner's own runnable delay. Its raw scheduler profile also contains 5.197 seconds with no location stack, which should not be silently discarded. The subject-goroutine state analysis below uses each StateTransition's `GoID` and `TransitionStack`, not the event's waker `G` and `Stack`.

| Leader cell | Capture s | Owner runnable | Owner select waiting | WAL runnable | WAL syscall state | Frontend readers runnable, aggregate | Frontend writers runnable, aggregate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Put 1 KiB, 256 | 2.001 | 0.358 | 1.262 | 0.052 | 1.744 | 4.964 | 9.436 |
| Put 1 KiB, 4096 | 2.001 | 0.495 | 0.904 | 0.138 | 1.368 | 5.902 | 8.096 |
| Put 10 KiB, 256 | 2.002 | 0.089 | 1.763 | 0.025 | 1.872 | 1.159 | 3.067 |

Frontend columns sum 18 distinct readers or writers. Their totals can exceed capture duration. Waiting in a writer's select or a reader's network poll often means there is no ready work; it does not by itself establish flow-control or socket congestion. A Go goroutine marked Running can also suffer OS-thread descheduling, so these trace states are not an OS CPU accounting substitute.

Completed observed-file leader syscall spans, excluding directory syncs, show:

| Cell | File fsync calls in capture | Fsync span sum s | Fsync median ms | Fsync p99 ms | File-write span sum s | Largest file-write span ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB, 256 | 1293 | 1.479 | 0.798 | 9.173 | 0.265 | 9.354 |
| 1 KiB, 4096 | 141 | 1.203 | 6.368 | 27.304 | 0.158 | 9.346 |
| 10 KiB, 256 | 151 | 1.545 | 8.830 | 50.553 | 0.324 | 255.469 |

Audit correction: the initial table included directory syncs through `osfs.syncPath` and `Dir.SyncDir` during WAL segment creation. This table excludes them by stack. The excluded counts for these cells are respectively 1, 3 and 2, with duration sums of 768, 62,016 and 258,880 ns. No sync is unclassified. The file-only observation counters below already excluded directory syncs and are unchanged. Total WAL syscall state above still includes them. None of the 27 CPU profiles contains a directory-sync sample, so the CPU file-fsync column is unchanged.

These are elapsed intervals in the Go Syscall state, including kernel execution and any descheduling, not pure off-CPU time or physical-device latency. Syscalls crossing a capture boundary are omitted from this completed-span table. There is a 255 ms file-write span in the 10 KiB trace; its cause is not isolated. Fsync is important, but it is not the only long WAL syscall observed.

The disk helper occupies the Syscall state for about 87%, 68% and 94% of the respective captures. The owner spends much of the same captures waiting in its event select. This is consistent with append-clocked progress rather than an owner that continuously consumes the assigned CPU. The trace does not prove that all owner select waits are caused by disk, or that removing a particular wait would improve throughput.

### Wider fenced disk counters

These totals bracket the whole four-second trace cohort plus external completion fences, not just the trace capture. The counter definitions include WAL metadata and segment headers, and exclude directory syncs.

| Cell | Successes | Leader file fsyncs | Successes/fsync | Mean file-write KiB | Fsync wall sum s | File-write wall sum s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 KiB, 256 | 182384 | 2663 | 68.488 | 72.940 | 3.031 | 0.526 |
| 1 KiB, 4096 | 339462 | 261 | 1300.621 | 1383.222 | 2.391 | 0.591 |
| 10 KiB, 256 | 26560 | 318 | 83.522 | 840.636 | 3.340 | 0.416 |

All file write/fsync error deltas are zero. Fsync wall sum per success falls from 16.619 us at 256 clients to 7.044 us at 4096, despite slower individual fsyncs. Fewer calls and larger groups amortize that wall cost. This is grouping evidence from these diagnostics, not a new batching-policy change or an exact per-request critical path.

The 100 ms CPU-run samples show a further qualification. Balanced Put-1K-4096 has 16 follower observations with `inflight_full=true`, all at `inflight_count=256`, among 49 leader sampling polls. Its sampled persist-channel maximum is 51 batches. Leader-only Put-1K-4096 never samples Full and reaches at most four inflight slots. No CPU cell samples Full at a count below 256. Thus the count ceiling is reached in the balanced cell, but the hook cannot distinguish count-only from simultaneous byte saturation at count 256. It also cannot exclude limits between samples. These diagnostics contain no balanced 10 KiB CPU/trace cell.

## Offline method

The [analysis script and reproduction instructions](profile-analysis/README.md), [generated tables](profile-analysis/archive/computed-tables.md), [integer calculations and attribution records](profile-analysis/archive/computed.json.gz), and [input hashes and commands](profile-analysis/archive/input-manifest.json) are committed alongside this report. `profile-analysis/archive/raw/` preserves all 27 CPU and 12 leader trace-profile raw pprof outputs, without the top report's node-dropping threshold. The machine archive preserves each completed classified syscall's timestamps, GoID and full entry stack, plus CPU sample attribution and per-subject Go-state totals.

```sh
python3 profile-analysis/analyze.py \
  --results-root /home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/results \
  --output-dir /tmp/tigerstore-profile-analysis
```

This performs offline decoding only. Each Go subprocess uses `GOFLAGS=-p=2 GOMEMLIMIT=2GiB`.

Disjoint priority is observation functions, `node.(*Node).persist`, `node.(*Node).commit`, frontend `readLoop`, frontend `writeLoop`, peer sender `run`, peer `receive`, debug HTTP, runtime-only, then other. Inclusive mechanisms count a sample once if any frame matches the named package or operation; inline frames are included. Socket calls require `net.(*netFD).Read/Write`. File fsync requires the `osfs.observedFile.Sync` wrapper, with directory-helper stacks excluded first; other unidentified syncs remain separate. File writes similarly require the observed-file wrapper. Allocation/GC matches `runtime.mallocgc*`, `gc*`, `newobject`, `growslice` and `makeslice`.

For subject trace states, classify dedicated goroutines from their `TransitionStack` roots. Accumulate each target `GoID`'s intervals after its first observed state, through subsequent transitions, and close its final state at the last trace event. Completed syscall spans use the entry stack to separate file Write from Fsync. Trace p99 is the sorted completed span at zero-based index `floor(0.99 * count)`. These analyses establish where CPU samples and Go-state waits occurred. They do not establish causal CPU savings, profiler overhead, regional performance, arbitrary-history linearizability or crash durability.
