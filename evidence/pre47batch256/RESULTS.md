# Why 256 clients still made tiny groups

The limiting mechanism is the leader's Go scheduler around blocking fsync with `GOMAXPROCS=1`. The prototype waits for arrivals during an append, but the ordinary fsync syscall can retain the only Go execution processor, or P. Other goroutines then cannot decode requests, admit them, send peer messages or release replies until the syscall returns or the runtime takes its P. A nearly empty inbox does not mean the clients supplied no work.

The fix tested here is `GOMAXPROCS=2`, still with every server thread pinned to the same single physical CPU. The committer remains one goroutine. This changes neither the core nor the WAL and adds no timer. At 256 clients, the same uninstrumented prototype binary improved from 5,297 to 29,657 Put/s and from 1.249 to 52.678 Puts per leader file fsync.

A separate causal control kept one P and changed only the syscall entry's blocking hint. It also restored batching. Do not copy that control's unsupported runtime linknames into the real change.

Nothing merged. This is pre-work for #47, not a completed correctness or production performance gate. Read #46, #1 and `docs/design.md` on main at `7f4e030b4155dd7332afda22a2e00041db904570`. Both Sol helpers and their worktrees were deleted. No Astra or Claude helpers were started.

## Commits and runs

- Branch: https://github.com/djosh34/tigerstore/tree/exp/pre-47-batch256
- Uninstrumented prototype checkout: `79bd08f326ec8c02e06c9fbc1e8f8373ec5234d7`. Its executable source is unchanged from the previous measured prototype, `fad17fe3fe64c73075da484c773f4859bc4e8e18`. The later commit adds evidence and a test, not server behavior. The original base was main `b8bac8b371c94f6d17136ad14aaa04354bbff01e` plus pre50 observation code.
- First diagnostic source: `758a9c235f23f18056778dcb295c25f194b0b674`, [run 38334185473](https://github.com/djosh34/tigerstore/actions/runs/38334185473). One 92-second job. Its artifacts are in `first-diagnose/`.
- Paired measurements and HTTP/2 diagnostic source: `103ba4f6a607fdac8afeb487303e84e722cd0a05`, [run 38334811483](https://github.com/djosh34/tigerstore/actions/runs/38334811483). Five jobs, all successful, 47 seconds to 6 minutes 24 seconds. Artifacts are in `paired/`, `diagnose/` and `isolate/`.
- No-clock counters and syscall-handoff controls: `fc51d109fd7717b8b62223267f5beff3691b87d4`, [run 38337490212](https://github.com/djosh34/tigerstore/actions/runs/38337490212). Three successful jobs, 2 minutes 38 seconds to 3 minutes 32 seconds. Artifacts are in `counter/`, `handoff/` and `isolate-long/`.

The main comparison uses exactly the same `prototype` executable for P1 and P2. Only the environment changes. Instrumented executables are separate diagnostic controls. Every binary hash and module version is in the relevant `versions.txt`.

## Runner and unchanged workload

Free `ubuntu-24.04-arm`, four Neoverse-N2 CPUs, about 16.72 GB RAM, image `ubuntu24-arm64 20261004.142.1`, Linux `6.17.0-1022-azure`. Leader CPU 0, followers CPUs 1 and 2, generator CPU 3. The harness checks every server thread's affinity after the run. P2 does not add a physical core. The generator always uses `GOMAXPROCS=1`.

Three real server processes, certificate-authenticated client and peer TLS, TLS 1.3, one shared runner disk. Both stores use the same ext4 mount after `sudo mount -o remount,barrier=1 /`; the script rejects `nobarrier` and `barrier=0`. Guest barriers do not prove physical power-loss protection, and the three directories are not independent disk failure domains.

The unchanged pre50 generator uses 16 established gRPC connections, leader-only closed-loop Puts, 4,096 registry-shaped keys and 1,024-byte SHA-256-derived values. Each cluster gets 4,096 seed Puts, 20,000 warmup Puts, a one-second pause and 60,000 measured Puts. No compression, injected delay, watches or compaction. P1/P2/etcd run sequentially on the same VM for each client-count cell, five fresh clusters per store. Even repetitions reverse the order. Different client counts use different VMs.

Stock etcd is v3.7.2, upstream `68c065e562994b89e333e77b039ad066f933c586`, built with Go 1.26.8. Its ARM64 binary SHA-256 is `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d`. It retains safe persistence defaults, an 8 GiB quota and the previous harness's `GOMAXPROCS=1`. The etcd column is that reference configuration, not a claim about an independently tuned P2 etcd. Tigerstore and the client use Go 1.27.2.

All 45 main comparison windows passed zero-error, exact success/revision, unchanged leader/term, replica completion fence and CPU-affinity checks. The diagnostic windows also passed their checks. Initial diagnostics and short isolation runs used 12,000 measured Puts; the final counter, handoff and long-isolation runs used 60,000. Their configurations and results remain separate.

## Five-repetition numbers

Medians of each run's statistic, not pooled percentiles. Puts/fsync is 60,000 divided by the fenced counter delta. Tigerstore counts file fsyncs, including segment headers but excluding directory syncs. Etcd counts its stock WAL sync histogram, primarily fdatasync. Its separate backend commit counts remain in the raw metrics.

| Clients | Store | Put/s | Mean ms | p50 ms | p99 ms | p99.9 ms | Leader Puts/sync | Follower 1 | Follower 2 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | Prototype P1 | 6,654.5 | 2.404 | 2.372 | 3.892 | 9.582 | 3.840 | 3.885 | 3.890 |
| 16 | Same binary P2 | 7,366.0 | 2.172 | 2.063 | 3.920 | 9.193 | 4.694 | 4.859 | 4.855 |
| 16 | Etcd P1 | 2,695.8 | 5.934 | 5.777 | 12.163 | 16.169 | 1.848 | 3.461 | 3.414 |
| 64 | Prototype P1 | 6,948.5 | 9.206 | 4.496 | 27.683 | 34.361 | 2.476 | 13.257 | 13.333 |
| 64 | Same binary P2 | 16,737.2 | 3.821 | 3.645 | 8.981 | 13.538 | 19.531 | 18.756 | 19.392 |
| 64 | Etcd P1 | 3,676.5 | 17.399 | 16.947 | 29.065 | 33.225 | 3.309 | 11.338 | 11.244 |
| 256 | Prototype P1 | 5,297.3 | 48.296 | 51.281 | 85.097 | 93.936 | 1.249 | 42.313 | 42.463 |
| 256 | Same binary P2 | 29,656.6 | 8.593 | 7.933 | 19.262 | 26.538 | 52.678 | 60.545 | 59.583 |
| 256 | Etcd P1 | 5,598.6 | 45.678 | 44.348 | 75.617 | 89.721 | 6.397 | 45.872 | 44.944 |

P2's five rates ranged from 7,235.7 to 7,512.8 at 16 clients, 16,527.4 to 17,230.8 at 64, and 29,455.5 to 30,257.4 at 256. P1 at 256 ranged from 5,136.3 to 6,426.6. P1 is phase-sensitive: several short diagnostic windows ran fast without any fix. A single short fast run is not evidence that the defect disappeared.

CPU microseconds per Put, from process user plus system ticks. These exclude later follower catch-up and work charged outside the measured processes. Column medians need not sum.

| Clients | Store | Leader | Follower 1 | Follower 2 | Generator |
| ---: | --- | ---: | ---: | ---: | ---: |
| 16 | Prototype P1 | 113.167 | 49.500 | 40.667 | 54.000 |
| 16 | Same binary P2 | 114.167 | 45.333 | 38.333 | 54.333 |
| 16 | Etcd P1 | 275.333 | 102.000 | 88.833 | 85.333 |
| 64 | Prototype P1 | 77.833 | 20.667 | 17.833 | 33.833 |
| 64 | Same binary P2 | 56.500 | 15.667 | 13.833 | 33.167 |
| 64 | Etcd P1 | 212.333 | 51.167 | 47.667 | 66.833 |
| 256 | Prototype P1 | 69.833 | 11.000 | 9.833 | 23.833 |
| 256 | Same binary P2 | 33.167 | 6.333 | 5.833 | 23.833 |
| 256 | Etcd P1 | 151.167 | 27.000 | 26.000 | 46.167 |

## Evidence for the cause

### Empty admission windows, not a full inbox

The no-clock observer uses fixed histograms and atomic counters, with no per-event timestamp, mutex, allocation, timer or live scrape. It counts new `Node.Do` arrivals across `WAL.Sync`, including its file write, fsync and scheduling before return. Counter snapshots bracket the same replica-fenced measurement window.

In `counter/c256/2-counter-p1`, 41,311 physical groups carried 60,000 Puts. Of these, 40,714 were entry-bearing. Thus extra hard-state-only rounds were about 1.45% of groups, not the explanation for the tiny density. Only 10,653 of 60,019 admitted operations arrived inside append spans. The extra 19 operations are probes and the replica fence. **99.64% of append spans admitted no request.** Completion inbox and deferred depths were both zero at p50 and p99.

The slower first P1 repetition is more extreme: 48,551 groups, 47,842 entry-bearing, only 204 arrivals inside append spans, and 99.992% of spans with no arrival. This observer retained the bad mode. It did not manufacture large batches as the full tracer often did.

For the paired counter P2 cases, the median empty-admission-span fraction fell to 14.79%, median deferred depth rose to 29 at p50 and 231 at p99, and the median leader density reached 58.083 Puts/fsync. The inbox still usually stayed empty because the owner drained arrivals into the deferred queue.

### The leader needs the scheduling change

Three 60,000-Put repetitions per variant in `isolate-long/`, same physical layout. Instrumentation is inactive. The debug endpoint changes P counts before seed/warmup, never during measured load.

| Go processors | Median Put/s | Leader Puts/fsync |
| --- | ---: | ---: |
| P1 everywhere, diagnostic binary inactive | 5,396.2 | 1.578 |
| Leader P2, followers P1 | 28,902.0 | 59.055 |
| Leader P1, followers P2 | 4,803.4 | 1.323 |
| P2 everywhere, uninstrumented binary | 28,136.6 | 58.480 |

This isolates the useful change to the leader. It does not require faster follower fsyncs or more physical CPU.

### Explicitly handing off the P during fsync also fixes it

Go 1.27.2's Linux ARM64 `os.File.Sync` reaches `syscall.Fsync`, which uses ordinary `runtime.entersyscall`, not `entersyscallblock`. The ordinary path can retain its P through the syscall. The runtime's retake depends on scheduling/syscall observations; it is not a fixed 10 ms fsync timer. Exact source links are in `batch256-audit.md`.

The `handoff/` experiment keeps P1 everywhere. Both variants bypass the same `os.File` poll-FD lock on the WAL's single-owner descriptor and make the same kernel `SYS_FSYNC` call. The raw control uses ordinary syscall entry. The blocking variant calls `entersyscallblock`, executes `RawSyscall`, then calls `exitsyscall`. It releases no persistence completion early and skips no fsync. This unsupported wrapper exists only to isolate the runtime handoff.

| P1 syscall control | Median Put/s | Leader Puts/fsync |
| --- | ---: | ---: |
| Ordinary raw fsync | 6,175.3 | 1.810 |
| Immediate blocking handoff | 31,616.4 | 64.795 |
| Ordinary raw fsync, no-clock counters | 5,546.0 | 1.596 |
| Immediate handoff, no-clock counters | 30,946.8 | 62.048 |

Each is three 60,000-Put repetitions. One ordinary-raw repetition ran at 20,023 Put/s and 11.34 Puts/fsync; the other two returned to the small-group mode. All three blocking-handoff repetitions reached 62.1 to 65.8 Puts/fsync without counters. With counters, the median fraction of append spans without arrivals fell from 99.48% to 12.5%. The median arrivals inside append spans rose from 15,004 to 59,882 of 60,019.

Together, the same-binary P2 comparison, leader/follower isolation, nonperturbing bad-mode counters and syscall-entry ablation establish the scheduling mechanism. The exact relative contributions of retained-P duration, retake timing, runnable-queue ordering and netpoll scheduling in an untouched slow run remain unmeasured.

## What the timestamped traces show

The branch records arrivals, Submit, Process begin/end, Ready with observed commit index, physical group membership, persist and file-fsync begin/end, completion, peer receipt/Step, send enqueue/write/flush, apply, reply-channel release and handler return. Group events carry inbox/deferred/waiter depths. Core timestamps come through an optional adapter callback; the deterministic core still imports no clock.

Raw events are `n*-trace.json.gz`; reconstructed per-group timelines are `n*-groups.jsonl.gz`. Request IDs join applied entries back to their physical groups. A Ready commit timestamp is when the adapter observes that commit, not an assertion about the earlier instant inside Raft's Step. HTTP/2 frame timestamps are TLS Read/Write completion observations, not packet timestamps.

Tracing perturbs this workload. In the first job, uninstrumented P1 gave 5,992 Put/s and 1.724 Puts/fsync, while full event tracing gave 25,780 and 64.171. Native Go tracing gave 29,640 and 61.224. Those fast P1 traces cannot represent the original slow run. The raw native traces remain archived, but are not the causal proof.

Two event captures retained long small-group phases. `first-diagnose/c256/1-trace-conns256-p1/n1-trace.json.gz` recorded 4,380 one-Put groups out of 4,568, with only 48 zero-Put groups. Only 145 file-fsync intervals admitted any request. The 256-connection setting is a diagnostic ablation, not the primary 16-connection workload.

That capture also separates other possible clocks:

- Of 4,520 entry-bearing groups with complete joins, 4,448 started the next group before the first follower append acknowledgement for the current group. The same count started the next group before its first apply. The leader was not waiting for remote commit or apply before forming its next group.
- Leader send enqueue to peer-writer execution took 7.66 ms at p50 and 35.26 ms at p99. The corresponding next-group fsync kept running meanwhile. Peer send scheduling delayed the follower acknowledgements; their lateness does not establish an acknowledgement-gated batching policy.
- All 135 apply spans together took about 25 ms for 12,000 Puts, about 2.08 microseconds per Put. Apply computation was not the dominant wall-time cost in that capture. Reply-channel release to handler resumption had a 1.15 ms median and 16.80 ms p99. Reply scheduling is affected too, and closed-loop arrivals necessarily depend on replies.

The static-window capture retained 1,305 one-Put groups out of 1,470. Its next group preceded the current group's first follower acknowledgement in 1,348 of 1,441 joined entry-bearing groups. These are traced diagnostic schedules, not untraced percentile claims.

## gRPC is not a 256-client admission cap

The source sets `MaxConcurrentStreams(1 << 20)`, per connection. The HTTP/2 tracer saw that exact SETTINGS value, and the stats handler saw 256 simultaneous server RPCs and 16 on one connection. Each Put uses 1,068 DATA bytes. Sixteen outstanding Puts on one connection use 17,088 bytes, below the initial 65,535-byte connection and stream windows.

In both complete HTTP/2 captures, reconstructed connection credit never reached zero: the minimum request-direction credit was 32,427 bytes and the minimum response-direction credit was 64,043. No larger dynamic window was advertised in those captures. `n*-h2-summary.json` retains every connection's counters. The tracer stores frame sizes and selected SETTINGS/WINDOW_UPDATE fields, never request contents or TLS material.

These HTTP/2 captures run in the perturbed fast mode, so their credits are not a trace of the untouched slow execution. They combine with the source rules and the ablations to reject a fixed stream/window cap as the explanation. Static 16 MiB receive windows did not reliably eliminate small groups. Increasing connections to 256 still produced the small-group trace described above. Neither transport change is the recommended fix.

## Durability and the real change

The measured P2 fix changes scheduling, not durability. Keep all of these rules from `docs/design.md` and the pinned async-Raft contract:

- No leader reply, local apply or new watch event for an entry before that node's successful local persistence and committed application. Remote-majority commit does not bypass local stability.
- No follower append acknowledgement or vote escapes its persistence dependency. Socket writes are not durable acknowledgements.
- Retain every original append job and its ordered Responses, including empty and hard-state-only jobs. Install the job in MemoryStorage before delivering those responses. Do not use Advance with async writes or invent a combined storage response.
- Preserve suffix replacement order, immutable buffers and term/incarnation identity. Coalesce the final complete hard-state tuple, including vote zero, not fieldwise maxima.
- Fence on write, fsync, rotation or directory-sync failure. Never turn EIO into success or retry a failed fsync. The ordinary EINTR handling is not such a retry policy.

A reply may safely occur while a later commit-only group is syncing. The dependency is the reply's own entry, not the absence of all outstanding disk work.

The prototype's blanket Ready gate is still wrong for production. Step and Tick may run, but suppression of Process delays heartbeats, votes, ReadIndex messages/results, ready apply work, timeout replies, watches and leader observations. A stalled disk can therefore break control-path liveness and trigger elections. Deferred deadlines start too late, the count-only queues can hold huge byte volumes, and a full shared inbox blocks peer admission.

The real change should separate proposal admission/flush from Ready/control processing. Continue taking and preserving Ready work, keep one bounded ordered append stream, and release only messages and effects whose original Raft dependencies permit it. Never call Ready and discard it. Merely skipping `Process.propose` is insufficient because Submit can propose at the byte cap. Bound queued bytes and preserve control progress, including follower-forwarded proposals. Keep enough runtime execution capacity for helper goroutines while real file syscalls block; one committer does not require one Go P.

Do not replace these rules with the unsupported linkname control. A supported P2 configuration is the tested small fix for this runner. A different disk backend that parks its Go caller may also solve the handoff problem, but was not tested.

Focused driver, core, WAL-adapter, peer and server checks passed. The integrated server/driver race checks passed, and the final changed packages passed vet and lint. Initial formatting/errorlint findings were corrected. The current simulator does not execute this driver gate; its old passing seeds do not certify it. All 118 new measurement windows passed their recorded client and fence checks. No new crash campaign, delayed-network benchmark, open-loop test, follower-client workload or Kubernetes acceptance test ran here.

## Reproduction

Run only on a fresh disposable ARM runner, before the deadline. The scripts enforce `2026-10-13T22:00:00Z`, remount that runner's root filesystem with barriers, and clean their owned temporary directory.

```sh
git clone --branch exp/pre-47-batch256 https://github.com/djosh34/tigerstore.git
cd tigerstore
git checkout 103ba4f6a607fdac8afeb487303e84e722cd0a05
CLIENTS=16 MODE=paired timeout --kill-after=5s 820s bash experiments/pre47batch256/run.sh
# Separate identical jobs use CLIENTS=64 and CLIENTS=256.

git checkout fc51d109fd7717b8b62223267f5beff3691b87d4
CLIENTS=256 MODE=counter timeout --kill-after=5s 820s bash experiments/pre47batch256/run.sh
CLIENTS=256 MODE=handoff timeout --kill-after=5s 820s bash experiments/pre47batch256/run.sh
CLIENTS=256 MODE=isolate-long timeout --kill-after=5s 820s bash experiments/pre47batch256/run.sh
```

The main P1/P2 server command is the same `taskset -c <0|1|2> .../bin/prototype` argv, with `GOMAXPROCS=1` or `GOMAXPROCS=2`. The generator command is unchanged:

```sh
GOMAXPROCS=1 taskset -c 3 "$RUNNER_TEMP/pre47batch256/bin/load" \
  -endpoint 127.0.0.1:<leader-port> -certdir <cert-directory> \
  -pids <leader-pid>,<follower1-pid>,<follower2-pid> -hz 100 \
  -op put -total 60000 -workers <16-or-64-or-256> -out <result.json>
```

Every unabridged argv is in its cell's `commands.json`, and each node's selected environment is in `n*-env.json`. Counter fences retain the previous harness's applied-revision check and etcd's post-WAL.Save committed-index fence followed by a fresh metrics scrape. CPU accounting keeps the narrower client window.

From the evidence checkout, regenerate summaries without rerunning measurements:

```sh
python3 experiments/pre47batch256/summarize.py
python3 experiments/pre47batch256/counter_summary.py
python3 experiments/pre47batch256/trace_summary.py
python3 experiments/pre47batch256/h2_summary.py
```

`runs.csv` preserves every window, including fast P1 controls rather than discarding them. `clock-runs.csv` preserves each observer repetition. The helper audits preserve their source findings and earlier hypotheses; this report supersedes their pre-measurement guesses and old local artifact paths.
