# Audit of the 256-client group-commit clock

Source audit completed 2026-10-13, before the 2026-10-13T22:00:00Z deadline. No code changes, benchmarks, runner jobs, subagents, pushes or issue comments. This report is for the parent to consolidate.

## Finding

The strongest specific explanation is that the disk goroutine's ordinary Linux fsync retains the sole Go P. The kernel can receive network bytes during that syscall, but Go cannot decode them, run a gRPC handler or put its request in the committer's inbox until that P becomes available. The prototype batches admitted requests, not bytes waiting in sockets.

The source establishes this mechanism. It does not establish how often it occurs in the untraced slow run. The parent's new same-binary P1/P2 result strongly supports a scheduling cause, but leader-only and follower-only P2 isolation are still needed. Native tracing and the event recorder both move P1 into a much faster mode. Neither is a faithful observation of the original slow execution.

The intended clock is "requests admitted during the previous append." The plausible actual clock at P1 is "one or a few handlers run after syscall exit, followed by another immediate append." Repeated short fsyncs can starve the intended admission window. Followers consume bursts of many already-created leader entries, so their roughly 41 Puts/fsync does not mean the leader created 41-Put entries.

## Sources and scope

The audit checkout is at `79bd08f`, on `exp/pre-47-batch256-audit`. The measured prototype source is `fad17fe3fe64c73075da484c773f4859bc4e8e18`. The behavior change is in `internal/node/node.go`; the core, Raft library and peer transport are unchanged from the prototype's baseline.

Read the required `../issue46.md`, `../issue1.md`, `../design-main.md` and the unslop skill. Inspected the core, driver, codec, storage wrapper, WAL, peer transport, generator, measurement scripts, existing driver tests, original archived evidence and the parent's diagnostic evidence. The pinned Raft source is v3.7.0. The installed Go source is Go 1.27.2, matching the candidate and generator build identities in the original runner evidence. Stock etcd's archived binary was built with Go 1.26.8.

Useful source links:

- [Driver admission, completion and blanket Process gate, lines 226 to 322](https://github.com/djosh34/tigerstore/blob/fad17fe3fe64c73075da484c773f4859bc4e8e18/internal/node/node.go#L226-L322).
- [Disk worker and final-hard-state coalescing, lines 358 to 391](https://github.com/djosh34/tigerstore/blob/fad17fe3fe64c73075da484c773f4859bc4e8e18/internal/node/node.go#L358-L391).
- [Core Submit and byte-cap proposal, lines 269 to 329](https://github.com/djosh34/tigerstore/blob/fad17fe3fe64c73075da484c773f4859bc4e8e18/internal/core/node.go#L269-L329).
- [Core Persisted, Process and Ready handling, lines 394 to 485](https://github.com/djosh34/tigerstore/blob/fad17fe3fe64c73075da484c773f4859bc4e8e18/internal/core/node.go#L394-L485).
- [Peer writer and queue-empty flush, lines 385 onward](https://github.com/djosh34/tigerstore/blob/fad17fe3fe64c73075da484c773f4859bc4e8e18/internal/peer/peer.go#L385-L431).

The local dependency paths used were `/home/joshazimullah.linux/go/pkg/mod/go.etcd.io/raft/v3@v3.7.0` and `/home/joshazimullah.linux/.local/opt/go1.27.2/src`.

## Exact Go syscall mechanism

The Linux ARM64 call chain is:

1. [`os.(*File).Sync`, file_posix.go:163](https://github.com/golang/go/blob/go1.27.2/src/os/file_posix.go#L163-L171), calls the internal poll FD's Fsync.
2. [`internal/poll.(*FD).Fsync`, fd_fsync_posix.go:12](https://github.com/golang/go/blob/go1.27.2/src/internal/poll/fd_fsync_posix.go#L12-L20), calls `syscall.Fsync` through an EINTR retry wrapper. Regular-file fsync is not asynchronous network-poller I/O.
3. [`syscall.Fsync`, zsyscall_linux_arm64.go:462](https://github.com/golang/go/blob/go1.27.2/src/syscall/zsyscall_linux_arm64.go#L462-L468), calls `Syscall(SYS_FSYNC, ...)`.
4. [`syscall.Syscall`, syscall_linux.go:73](https://github.com/golang/go/blob/go1.27.2/src/syscall/syscall_linux.go#L73-L92), calls `runtime_entersyscall`, executes `RawSyscall6`, then calls `runtime_exitsyscall`. It does not call `entersyscallblock`.
5. [`runtime.entersyscall`, proc.go:4776](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L4776-L4783), calls [`reentersyscall`, proc.go:4642](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L4642-L4755). That saves the old P, records `syscalltick`, and changes the G to `_Gsyscall`. It does not normally release the P immediately. A pending stop-the-world operation is an exception.

Go 1.27's implementation matters. Do not describe it using an older `_Psyscall` transition. The P normally remains `_Prunning`, attached to a G in `_Gsyscall`. [`setBlockOnExitSyscall`, proc.go:6801](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L6801-L6846), verifies those links before retake. [`syscallingThread.takeP` and `releaseP`, proc.go:6861](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L6861-L6883), actually detach it. The ordinary syscall-exit fast path can continue on the retained P and increments its syscall tick, in [proc.go:4968 to 5040](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L4968-L5040).

By contrast, [`runtime.entersyscallblock`, proc.go:4830](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L4830-L4871), explicitly calls `handoffp(releasep())` before the syscall. [`handoffp`, proc.go:3146](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L3146-L3221), starts another M for runnable work or the network poller. This is the behavior ordinary `File.Sync` lacks.

### Retake is not a fixed 10 ms fsync timeout

[`sysmon`, proc.go:6537 onward](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L6537-L6656), starts with 20 microsecond sleeps and backs off as far as 10 ms. It also polls the network when the last poll is more than 10 ms old.

[`retake`, proc.go:6681](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L6681-L6776), normally needs more than one sysmon observation of the same syscall tick. A newly observed tick resets `syscallwhen` and defers retake. The check at lines 6754 to 6757 also avoids retake for a while when the local queue is empty and another P or spinning M exists. A long unchanged scheduling tick can force retake through the separate preemption path.

A sequence of roughly 0.2 ms fsyncs can finish and change `syscalltick` between sysmon observations. This can repeatedly postpone retake despite pending client traffic. It is not a guarantee that every fsync holds the P for its entire duration. Retake, GC and other scheduling work can break the cycle.

## How the prototype amplifies that clock

`inflight` becomes true as soon as `Process` produces a persist group, before the disk worker receives it. The owner continues to accept peer messages and ticks, but holds client requests outside `Submit` and skips every subsequent `Process` until disk completion.

On completion, the owner calls `Persisted` on the original jobs, submits its deferred requests, snapshots and drains the currently queued inbox, and immediately calls `Process` again. The snapshot fixes the old inbox-drain omission, but does not give unscheduled gRPC readers or handlers an admission opportunity. An empty inbox can coexist with bytes waiting in a socket or goroutines waiting to run.

The completion channel can wake the owner promptly. Once the owner has one admitted request, `Process` proposes it without a timer. The disk worker then receives another tiny group and can enter fsync before the other client work runs. The count cap, byte cap and 256-client population do not force a larger group.

Peer transport provides a different arrival pattern:

- The leader sends `MsgApp` before local persistence is complete under async Raft's contract. Its own durable append acknowledgement waits for persistence.
- The peer writer buffers frames and flushes when its queues become empty, or when its buffer fills. It can combine many small leader appends in one flush.
- A follower receives ordered frames, steps them while its local append is held, and persists the accumulated unstable suffix in a later Ready.
- Raft also collects pending entries when replication flow control resumes. Its limits here are 256 messages and 32 MiB, not one entry in flight.

See Raft [`send` and `maybeSendAppend`, raft.go:514 to 662](https://github.com/etcd-io/raft/blob/v3.7.0/raft.go#L514-L662), and [acknowledgement handling and resumed sends, raft.go:1525 to 1570](https://github.com/etcd-io/raft/blob/v3.7.0/raft.go#L1525-L1570). Transport buffering and shared-disk timing remain secondary possibilities, not separately proven causes.

## What the archived numbers constrain

The original 256-client measurements used three nodes and a generator on four distinct pinned CPUs, with `GOMAXPROCS=1` in every process. The clients used 16 established gRPC connections against the leader. The default entry cap was 4 MiB; all 256 outstanding 1 KiB Puts together are far below it. The server's concurrent-stream limit is `1 << 20`, so a 100-stream server limit is not the explanation.

For original prototype repetition 3:

| Quantity | Leader | Follower 1 | Follower 2 |
| --- | ---: | ---: | ---: |
| File syncs | 44,203 | 1,457 | 1,446 |
| Puts per file sync | 1.357 | 41.181 | 41.494 |
| Mean measured file-Sync duration, microseconds | 186.7 | 470.0 | 415.5 |
| Client-window duration divided by sync count, milliseconds | 0.265 | 8.040 | 8.101 |
| Written bytes in fenced counter window | 65,941,348 | 65,937,223 | 65,937,223 |

The interval estimate is not a measured inter-sync distribution. Disk counters bracket a wider window than client timing. It nevertheless shows why a follower's 41 cannot be explained simply as arrival rate times its roughly 0.45 ms fsync. Burst admission and aggregation matter.

The byte counts also argue against hard-state-only syncs being the dominant explanation for the leader's density. Each hard-state record is 33 bytes. All replicas receive the same entry bytes, and these windows cross one 20-byte segment-header write each. The leader/follower byte difference is exactly 125 hard-state records.

A conservative format bound gives the same conclusion. Each of the 60,000 Puts contributes at most 1,080 bytes before the per-entry WAL header and batch-count field. Each leader group writes at most one hard-state tuple. Follower 1 therefore writes at most 1,457 tuples, and the leader at most 1,582. Subtracting those maxima and the segment header from the leader's bytes leaves at least 38,898 entries, even allowing two bytes for every batch count. With only leader-origin client requests and no term change, each Process can propose at most one entry for this workload. Thus at least about 88% of the leader file syncs are entry-bearing groups, and the average Put count per entry is at most about 1.54 under these assumptions. Direct group classification is still preferable to this indirect bound.

### Parent's new diagnostic result

Read-only evidence is under `../repo/evidence/pre47batch256/diagnose/c256`, from run [38334185473](https://github.com/djosh34/tigerstore/actions/runs/38334185473). These are individual 12,000-Put windows, not another five-repetition median:

| Mode | Put/s | Leader Puts/fsync |
| --- | ---: | ---: |
| Uninstrumented P1 | 5,991.7 | 1.724 |
| Same binary P2 | 29,104.7 | 63.492 |
| Event-traced P1 | 25,779.6 | 64.171 |
| Native-runtime-traced P1 | 29,639.5 | 61.224 |
| Partial-window event-traced P1 | 15,682.8 | 8.163 |
| Event-traced P1 with 256 connections | 6,388.4 | 2.627 |

P2 improves the individual untraced window about 4.86 times and its sync density about 36.8 times. This supports scheduling sensitivity. It does not yet identify which node needs the spare P, nor prove an exclusive syscall-retake cause. Increasing connections does not reliably rescue the traced slow mode.

### Independent event summary of the partial bad modes

Parsed the supplied gzip JSON without modifying it. Both files report zero dropped events. Sorted by `ns`, counted `propose.a` between successive `group` events, and counted `arrival` timestamps strictly between each `fsync_begin` and `fsync_end` pair. The proposed-operation total is exactly 12,000 in each file.

| Event summary | `1-trace-window-p1/n1-trace.json.gz` | `1-trace-conns256-p1/n1-trace.json.gz` |
| --- | ---: | ---: |
| Groups | 1,470 | 4,568 |
| One-Put groups | 1,305 | 4,380 |
| Zero-Put groups | 29 | 48 |
| Median and p90 group Put count | 1 and 1 | 1 and 1 |
| Fsync intervals with any admitted arrival | 130 | 145 |
| Arrivals inside those intervals | 9,835 | 7,356 |
| Mean recorded fsync interval, microseconds | 386.5 | 272.6 |

Only about 8.8% and 3.2% of recorded fsync intervals have any arrival, despite large aggregate admission bursts in those few intervals. This fits an alternation between tiny-group and burst-admission modes. It does not prove the P state inside the empty intervals. `fsync_begin/end` bracket the user-level wrapper, not exact kernel entry and exit, and the event recorder itself uses `time.Now` and a shared mutex.

## Why native tracing can change the mode

There is no tracing-specific replacement of `entersyscall` with `entersyscallblock` in the inspected source. Tracing adds other scheduling changes:

- `reentersyscall` and syscall exit execute extra `systemstack` work and record events. [`traceAcquireEnabled`, traceruntime.go:202](https://github.com/golang/go/blob/go1.27.2/src/runtime/traceruntime.go#L202-L244), disables preemption through `acquirem`, updates tracing state and may flush buffers. [`GoSysCall` and `GoSysExit`, traceruntime.go:500](https://github.com/golang/go/blob/go1.27.2/src/runtime/traceruntime.go#L500-L525), record syscall events and stack information. This changes the time between short syscalls and the scheduler observations around them.
- [`StartTrace`, trace.go:420 to 452](https://github.com/golang/go/blob/go1.27.2/src/runtime/trace.go#L420-L452), stops and restarts the world, starts a CPU-sample reader and a trace advancer. [`traceStartReadCPU`, tracecpu.go:37](https://github.com/golang/go/blob/go1.27.2/src/runtime/tracecpu.go#L37-L71), adds a goroutine with periodic sleeps. It does not by itself establish that CPU profiling was enabled.
- [`handoffp`, proc.go:3156](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L3156-L3160), treats available trace-reader work as reason to start an M. The scheduler also explicitly runs the trace reader in [proc.go:3433 onward](https://github.com/golang/go/blob/go1.27.2/src/runtime/proc.go#L3433-L3448).
- The reader, HTTP trace response and trace buffer traffic add runnable work, allocations and I/O. Any of these can alter retake, network polling or the completion-to-next-append order.

These are source-supported ways tracing changes execution, not a measured attribution of the fivefold speedup. The parsed native files confirm P1 and P2 through their initial `/sched/gomaxprocs:threads` events. Their initial records also show the trace-start stop-the-world operation and CPU-reader goroutine creation. I did not perform a full analysis of the 156 MiB and 171 MiB parsed traces.

### What a representative native trace could distinguish

Identify the disk goroutine using its `os.(*File).Sync`/`syscall.Fsync` stack. Follow its `Running -> Syscall` transition, the P's state and any P-steal/start events, then its syscall exit. The native writer uses `GoSyscallEnd` when the P was retained and `GoSyscallEndBlocked` when it was lost. The high-level parsed trace represents the resulting G and P transitions.

Evidence of starvation would be a retained sole P during fsync, already-runnable gRPC reader or handler goroutines that do not start, then those goroutines running after return or P retake. At P2, look for the spare P running the reader/handler and owner while the disk G remains in syscall.

If readers are merely Waiting on network throughout the interval and no client work is Runnable, the native trace alone cannot distinguish "the kernel has no bytes" from "bytes arrived but Go has not polled them." A network wait is a Go observation, not a socket receive-queue measurement. That requires independent client-send timing or a sparse kernel observer of socket readiness/receive-queue state. Do not claim starvation from an empty inbox alone.

For the existing native P1 trace, analyze only the execution it actually records. Since that window is already fast, it cannot establish the untraced bad path. Even a short trace-start intervention may reset the mode.

## Minimal next ablations and observer

The parent owns runner execution. Keep every OS thread pinned to the same physical CPU and verify affinity after P changes, including newly created threads. GOMAXPROCS=2 changes Go execution permits, not the physical core budget. Keep the generator at P1.

1. Compare original candidate with the new diagnostic binary while all instrumentation is inactive. This controls for callback branches, stats handlers and observer-related code changes.
2. Run untraced P1 everywhere, leader-only P2, follower-only P2, and P2 everywhere. Set the debug endpoint before warmup and do not poll it during load. Repeat or reverse order because these runs show mode changes. A separate P1-to-P1 endpoint call controls for the endpoint invocation itself.
3. Try disk-worker-only `runtime.Gosched()` immediately after successful `WAL.Sync`, before posting `persistDone`. Keep durability and completion order unchanged. This lets runnable helpers admit work while the gate is still held, but does not release the P during fsync. A pre-Sync Gosched is a separate admission-window experiment and has the same limitation. Neither is equivalent to `entersyscallblock`, and one yield does not guarantee network polling.
4. Only if those leave ambiguity, isolate peer buffering with a bounded frame-count/byte flush policy. Preserve data ordering and control priority. Do not combine this change with a P change in the same causal comparison.

There is no supported public Go function that turns only `File.Sync` into `entersyscallblock`. `LockOSThread`, a plain cgo call and Gosched before the syscall do not guarantee that handoff. A linkname/raw-syscall wrapper is an unsupported runtime experiment, not a clean production solution. A native worker outside Go or an asynchronous disk facility with a parked Go waiter could provide true overlap, but is a larger change than the P2 control.

### Lowest-overhead observer

Start with owner-local and disk-worker-local counters, dumped only after the cohort is drained and fenced. No per-event clocks, mutex, JSON, log, allocations, extra timer, live debug scrape or native tracing during the window.

Count groups by admitted Put count, entry count, encoded bytes and whether they are entry-bearing, hard-state-only or response-only. Also count the inbox/deferred sizes at completion and at next-group formation. Use fixed-size histograms. The core can supply deterministic counts through an effect or job field; do not add wall-clock calls to the core.

In a separate observer build, place one atomic `inSync` flag around the actual file Sync wrapper. At handler admission, perform one atomic load and count admissions that observe it true. Handler admission precedes inbox insertion; also count owner-handled requests during that flag if needed. Disk-only and owner-only counters can remain private. Shared handler counters need atomic updates. Prefer a sampled check keyed by an existing request ID if full-rate checks perturb the mode. The flag interval includes a small wrapper boundary, so call it "Sync overlap," not exact kernel overlap.

Validate each observer against the unobserved same binary or inactive mode. If it converts P1 to large groups, its overlap statistics do not represent the bad path. The current mutex-based event recorder fails this test. Sparse externally collected syscall/scheduler data may be less intrusive, but is not automatically nonperturbing.

If timestamps remain necessary, sample a small fixed fraction of groups into a preallocated disk-writer-only ring. Record append submission, Sync return and completion acknowledgement without a shared logging lock. Require the slow-mode density to remain present before interpreting those samples.

## Durability contract for the real fix

These requirements are independent of whether P2 fixes performance.

1. **The majority must contain durable value bytes.** Socket writes and volatile replication are not acknowledgements of durability. Do not send a follower's append acceptance, count the leader's self-append acceptance, or publish a vote before the corresponding persistence dependency succeeds.
2. **Use async Raft's original response dependencies.** [Raft Config.AsyncStorageWrites, raft.go:153 to 187](https://github.com/etcd-io/raft/blob/v3.7.0/raft.go#L153-L187), requires messages to each local storage target to be processed reliably in order. Writes associated with append messages carrying responses must be durable before those responses are delivered. Messages with no responses do not necessarily require an immediate physical sync. A future optimization must still order them before dependent responses.
3. **Local stability gates application.** [`RawNode.applyUnstableEntries`, rawnode.go:443](https://github.com/etcd-io/raft/blob/v3.7.0/rawnode.go#L440-L445), returns false with async writes. [`raftLog.maxAppliableIndex`, log.go:267](https://github.com/etcd-io/raft/blob/v3.7.0/log.go#L263-L273), caps application at both the committed index and the local stable frontier. Remote-majority commit cannot bypass this frontier. Replies and watch events for these writes wait for local persistence and committed application.
4. **Do not invent or collapse logical completions.** The physical group may have one fsync, but retain every original PersistJob and its responses. After successful group persistence, install each original job in MemoryStorage and deliver its original responses in order. `core.Persisted` already does this. Preserve Raft's self-`MsgAppResp` before `MsgStorageAppendResp` order. See [`newStorageAppendMsg`, rawnode.go:223 to 260](https://github.com/etcd-io/raft/blob/v3.7.0/rawnode.go#L223-L260).
5. **Preserve full hard-state tuples and suffix order.** Coalesce to the last non-nil term/vote/commit tuple, including vote zero. Do not take fieldwise maxima. Persist its named entries first. Repeated entry indexes replace suffixes, so entry append order is part of the durable state. A later term can supersede an earlier vote before the original responses are released, but no response may escape an unsynced group.
6. **Keep completion term and entry identity.** Raft's storage response includes the term in which append work was issued. Lower-term completions must not stabilize a replacement suffix. See [raft.go:1166 to 1179](https://github.com/etcd-io/raft/blob/v3.7.0/raft.go#L1166-L1179) and [`newStorageAppendRespMsg`, rawnode.go:266 onward](https://github.com/etcd-io/raft/blob/v3.7.0/rawnode.go#L266-L362). Preserve entry-free responses after term changes too. Index/log-term matching alone does not cover the documented ABA case.
7. **Fence on persistence failure.** A Write, Sync, segment-header or directory-sync failure produces no successful group completion. Stop the node; do not retry a failed fsync on an unknown log state. Segment installation and replacement durability are part of the completion, not post-ack cleanup. File adapters must report short writes as errors or the WAL must reject them.
8. **Keep buffers and boot identity alive.** Entry and message bytes must remain immutable while any disk, transport or application consumer owns them. A completion from a prior node incarnation must not reach the new core. Never reuse request IDs within a boot, including timed-out requests.

One nuance matters for instrumentation. A client reply can leave the core while a *later commit-only hard-state* group is in flight. The write may already be committed and locally stable. Do not assert "no reply while any fsync is outstanding." Assert that each reply's own entry has passed its durability dependency and committed application.

The original runner's fences and barriers are useful measurement checks, not a power-loss proof. Three processes on one virtual disk are not independent failure domains. Prove acked-value survival by erasing each node's disk in turn and cold-restarting the remaining two without live-memory repair.

## Blanket Ready-gating contract and defects

Ready gating is a prototype scheduling policy, not a Raft durability requirement.

[`RawNode.Ready`, rawnode.go:131 to 135](https://github.com/etcd-io/raft/blob/v3.7.0/rawnode.go#L131-L135), accepts returned work. [`acceptReady`, rawnode.go:400 onward](https://github.com/etcd-io/raft/blob/v3.7.0/rawnode.go#L400-L438), clears outgoing messages and marks entries/apply work in progress. Never call Ready merely to inspect it and discard its work. With async storage, do not call Advance; original local storage responses replace it.

The current driver's gate suppresses all Process work during an append. Even though Step and Tick may execute, the gate delays extraction of heartbeats, election traffic, ReadIndex messages, newly available committed application, replies, watch output and leader observations. Tick-created timeout replies also wait in effects. A sufficiently long disk stall can therefore delay heartbeats and cause elections.

Additional limits:

- Deferred request deadlines begin at Submit, not at initial driver admission. Disk-stall time is excluded from that logical timeout.
- The 8,192 deferred-request cap is a count cap, roughly 12 GiB at the maximum request size, before another inbox and RPC buffers. Multiple capped entries may share one physical group; there is no physical group-byte cap.
- Once the shared inbox is backpressured, peers lose admission along with clients. The control queue in the outbound peer writer does not fix an inbound shared-inbox stall.
- Forwarded follower proposals arrive through Step, not local Submit. A policy that only defers local requests is not a complete proposal admission bound.
- The existing simulator executes `core.Node` directly, not this driver's changed gate. Its passing seeds do not prove the new scheduler's crash or liveness behavior.

The future driver should keep one ordered physical append stream and defer proposal flushing as needed, but continue safe Raft/control processing. Split those operations rather than use blanket Process suppression. If a Ready contains the next append while disk is busy, retain that job in a bounded ordered queue; never lose it. Drain permitted sends, local application and replies according to Raft's original dependencies. `Submit` itself can propose at the byte cap, so merely skipping `Process.propose` is insufficient.

## Short regression design for the future worker

Use a shared scheduling component in the real driver and simulator. These tests do not depend on wall-clock sleeps:

- Hold a fake append completion, admit 256 valid requests through the normal input boundary, then complete it. Assert the next bounded group contains every admitted request exactly once. Cross the entry cap in a second case. Keep requests outside Submit until the policy permits proposal flushing.
- While that completion is held, deliver enough follower durable acknowledgements to commit the leader's entries. Assert no application, reply or watch event for those entries until the leader's local completion. Then require exactly one application and reply. Repeat for a follower-origin write.
- Hold disk completion through many virtual ticks. Require safe heartbeats and ReadIndex/control processing to continue within the specified bound, while append-dependent responses remain held. Fill the data admission budget and verify control progress and bounded bytes. This test should fail against blanket Process gating.
- Change term and replace a suffix with old work pending. Complete the old job, then the new term's entry-free and replacement jobs. Assert the stable index/term, final recovered tuple including vote zero, and no stale or duplicated reply. Inject Write/Sync/rotation errors and assert fencing with no successful completion.

These deterministic held-completion tests release the Go P through channel waits. They prove scheduler policy and durability, not the real-fsync P-retention hypothesis. Keep a separate small subprocess integration diagnostic for actual File.Sync and pinned P1/P2, outside a flaky unit throughput assertion. The production regression should test the chosen admission/control contract, not hard-code the runner's incidental 1.36 or 63.49 density.

## Files for the parent

Report: `/home/joshazimullah.linux/work_mounts/scratch/pre-47-batch256/audit/batch256-audit.md`.

Original evidence: `/home/joshazimullah.linux/work_mounts/scratch/pre-47-batch256/audit/evidence/pre47groupcommit`.

Parent's new evidence, read-only: `/home/joshazimullah.linux/work_mounts/scratch/pre-47-batch256/repo/evidence/pre47batch256/diagnose/c256`.

Native parsed traces, read-only: `/home/joshazimullah.linux/work_mounts/scratch/pre-47-batch256/runtime-p1.txt` and `/home/joshazimullah.linux/work_mounts/scratch/pre-47-batch256/runtime-p2.txt`.
