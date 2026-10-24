# Simulator size and duration experiment (#401)

Nothing here is intended for main. No PR was opened. All Go builds and simulations ran in GitHub Actions, not on the shared research host.

## Method

Primary matrix: [40113707142](https://github.com/djosh34/tigerstore/actions/runs/40113707142), harness `95fe1fc8516e0f8e633c2058e24ec775c4736404`.

One seed (1), three real nodes, their simulated disks and peer TLS network. Preload 1,000 / 10,000 / 100,000 objects. Then churn for 600 / 3,600 / 14,400 virtual seconds, followed by the simulator's healing and settling. Values follow `internal/k8ssize` (mean 1,458 B). The request mix is 25% get, 10% bounded list, 40% update, 15% create, 10% delete. Creates and updates are simple puts into a fixed key pool, not the soak's CAS transactions. Event puts use 120-second leases reused for a minute. Watches resume after the preload revision, as `cmd/soak/main.go:308` does. The ordinary simulator watchers and the unread gray-client fault remain enabled. Compaction is requested every five minutes, one interval behind, with retries through faults; an already-compacted reply confirms an ambiguous prior completion.

Sixteen closed-loop clients wait 40 ms after each call: **400 calls/s is a ceiling, not achieved throughput under these faults**. The object count is the initial population, not a guaranteed constant live-key count. Preload values are not leased. This is a simplified Kubernetes-shaped capacity workload, not the complete soak, apiserver, gRPC or correctness-checker workload.

`DefaultConfig(1)` supplies the usual crash, network, slow disk, disk error/full/shutdown, pause, clock drift, gray/slow watcher, recovery, snapshot, corruption and hold-down faults. The changes needed for size are 4 MiB segments and a disk budget of `max(16 MiB, objects*8 KiB)` per node, with capacity twice that budget. The ordinary tiny Raft/checkpoint limits remain. There is no value-cache memory budget. Full histories and the online invariant hooks remain; end-of-run witness/Porcupine/watch/cold-restart checking and artifact hashing are **not** run.

The workflow builds through `scripts/sim.sh ./internal/sim '^$'`, as the campaign does, then runs the translated binary directly. This streams progress rather than buffering it in `metatesting.RunQuiet`. The build job first validates 1,000 objects for 60 virtual seconds. Each matrix cell gets its own `ubuntu-24.04-arm` VM. `/usr/bin/time -v` measures the child binary. The host monitor samples RSS every five wall seconds; the harness logs virtual time every ten virtual seconds. Native `GODEBUG=gctrace=1` and a SIGQUIT stack dump at the memory guard give additional diagnostics.

A 12 GiB RSS guard leaves room on the 16 GB VM. Sampling and a stack dump can overshoot it. A guard exit is **not a measured kernel OOM**. Primary cells also have a 1,200-second wall cap; the job's outer timeout is 360 minutes. No build time is included in the table. Rates are the last logged total virtual elapsed time (preload + churn + healing/settling) divided by GNU time's actual wall time. For killed runs they are lower-bound progress rates, and can mostly describe preload rather than useful churn. Earlier monitor summaries rounded wall time to five-second polls; `scripts/sim-size-report.py` always reads the precise GNU time result instead.

## Primary results

| Initial objects | Churn target | Wall s | Peak RSS GiB | Virtual s / wall s | Outcome |
|---:|---:|---:|---:|---:|---|
| 1,000 | 10 min | 17.87 | 1.156 | 35.54 | finished |
| 1,000 | 1 h | 55.74 | 1.943 | 65.21 | finished |
| 1,000 | 4 h | 192.25 | 5.556 | 75.08 | finished |
| 10,000 | 10 min | 114.62 | 3.538 | 5.82 | finished |
| 10,000 | 1 h | 222.18 | 4.679 | 16.50 | finished |
| 10,000 | 4 h | 722.75 | 8.887 | 20.02 | finished |
| 100,000 | 10 min | 80.57 | 12.997 | 4.24 | 12 GiB memory guard, after preload |
| 100,000 | 1 h | 80.57 | 12.571 | 4.24 | 12 GiB memory guard, after preload |
| 100,000 | 4 h | 75.63 | 13.769 | 4.51 | 12 GiB memory guard, after preload |

All 100,000 objects were preloaded in 341.359 virtual seconds. No subsequent ten-second churn progress tick arrived before those cells were killed. Their rates mostly measure preload, **not sustained churn**. The 1,000 / 10,000 object four-hour cells made 1,071,721 / 1,268,904 calls; only 55,624 / 69,191 succeeded (3.9 / 4.8 successful calls per virtual second). They confirmed 38 / 41 compactions and counted 2,580 / 3,047 kills. Thus this is a very fault-heavy run, not four hours of 400 successful calls/s. All measurement jobs can be green even when a simulation hit its guard; the JSON outcome, not job color, says whether it completed.

## Controls

### Smaller values

[40112893509](https://github.com/djosh34/tigerstore/actions/runs/40112893509), harness `2cb5d66332eb7e657d58e456b51fa820df2f0ea5`. Same seed and stock adversarial watch policy as its baseline [40112891835](https://github.com/djosh34/tigerstore/actions/runs/40112891835). Divide histogram value lengths by ten, floor at 64 B: mean 146.7 B. The 1,000 / 10,000 / 100,000 object, ten-minute cells finished in 12.92 / 71.76 / 616.25 s, at peaks 0.534 / 1.819 / 6.471 GiB and rates 48.41 / 9.00 / 1.40 virtual s/wall s. The matched full-value baseline peaks for the first two were 0.988 / 4.924 GiB, and wall times 17.04 / 102.91 s. Thus approximately ten times smaller values bought 1.85x / 2.71x less peak RSS, not ten times less total process memory. At 100,000 objects it changed the outcome from a startup memory guard to a completed ten-minute churn. Different value sizes change simulated timing and fault interleavings, so this is not a same-operation-stream A/B.

### Compact before starting the gray-client fault

[40114211095](https://github.com/djosh34/tigerstore/actions/runs/40114211095), harness `90ca284e5213419344fbd1a5ad88af38791214c2`. Full-sized values, normal watches after preload, all normal faults still enabled. Compact to the observed preload revision before starting faults. This is a leader-acknowledged compaction, not an all-followers-applied barrier. It can let the server cancel gray clients that request revision 1, rather than replaying every preloaded object for them.

| Initial objects | Churn target | Wall s | Peak RSS GiB | Virtual s / wall s | Outcome |
|---:|---:|---:|---:|---:|---|
| 100,000 | 10 min | 80.56 | 12.332 | 4.35 | memory guard; last logged churn 8.767 s |
| 100,000 | 1 h | 1,163.46 | 7.875 | 3.40 | finished |
| 100,000 | 4 h | 75.54 | 12.069 | 4.64 | memory guard; last logged churn 8.767 s |

The completed one-hour cell made 376,496 calls, 21,193 successful calls, ten confirmed compactions and 981 kills. Its measured tail rate was 3.08 virtual s/wall s, and its tail endpoint RSS slope was 27.8 MiB per virtual minute. This proves a full-value 100,000-object hour can fit; it does **not** make startup compaction a reliable cure. Changing duration changes fault scheduling/random draws, so a one-hour cell is not a prefix of the four-hour cell, and passing the longer target does not contradict failing a shorter one. The failed cells' stacks include cold value reads and materialization after recovery. An all-nodes compaction barrier, changes to gray-client replay and incremental catch-up were not measured.

## What limits a run

- **Watch catch-up can be a startup cliff, not a bytes-per-live-object ceiling.** The gray-client fault opens ten whole-keyspace watches, never reads them, reconnects every 50–300 ms, and starts each at revision 1 one quarter of the time: [`internal/sim/gray.go:147-189`](https://github.com/djosh34/tigerstore/blob/95fe1fc8516e0f8e633c2058e24ec775c4736404/internal/sim/gray.go#L147-L189). [`mvcc.Store.History:571-587`](https://github.com/djosh34/tigerstore/blob/95fe1fc8516e0f8e633c2058e24ec775c4736404/internal/mvcc/store.go#L571-L587) materializes and sorts every matching change; [`core/watch.go:305-344`](https://github.com/djosh34/tigerstore/blob/95fe1fc8516e0f8e633c2058e24ec775c4736404/internal/core/watch.go#L305-L344) constructs the catch-up events, at 1,000 revisions per response. The simulator stream queue is only eight responses (`internal/sim/sim.go`, `WatchQueue: 8`). SIGQUIT stacks in the 100,000-object cells show `createWatch`, `History`, event/value materialization and recovery disk copies. Moving normal watches past preload did not remove this fault and did not fix that cliff.
- **The simulator itself retains state.** [`client.go:180-205`](https://github.com/djosh34/tigerstore/blob/95fe1fc8516e0f8e633c2058e24ec775c4736404/internal/sim/client.go#L180-L205) keeps every request and response, including failed requests and their values. [`storage.go:34-73`](https://github.com/djosh34/tigerstore/blob/95fe1fc8516e0f8e633c2058e24ec775c4736404/internal/sim/storage.go#L34-L73) keeps every committed entry separately from the nodes. [`watch.go:202-254`](https://github.com/djosh34/tigerstore/blob/95fe1fc8516e0f8e633c2058e24ec775c4736404/internal/sim/watch.go#L202-L254) retains watch responses and events. These are strong references; fixing weak pointers alone cannot reclaim them. In contrast trace and disk-call logs are bounded at 3,000 / 4,000 lines (`sim.go:937-959`).
- **Disks and recovery copies cost RAM and CPU.** [`simfs.go:195-231`](https://github.com/djosh34/tigerstore/blob/95fe1fc8516e0f8e633c2058e24ec775c4736404/internal/simfs/simfs.go#L195-L231) rebuilds a crash image with cloned bytes. [`simfs.go:316-344`](https://github.com/djosh34/tigerstore/blob/95fe1fc8516e0f8e633c2058e24ec775c4736404/internal/simfs/simfs.go#L316-L344) clones disk data, durable bytes and pending writes. `sim.checkRecovery` opens `n.disk.Copy()` (`sim.go:1685-1705`). Recovery/copy stacks also appear in the measurements. The tiny settings are a 16-byte window, 256-byte checkpoint threshold and 32-byte checkpoint chunks, not production sizing: [`storage.go:19-31`](https://github.com/djosh34/tigerstore/blob/95fe1fc8516e0f8e633c2058e24ec775c4736404/internal/sim/storage.go#L19-L31).
- **The port retains some things, but not every allocation is immortal.** The pinned fork makes weak pointers strong: [`gosim ad69b41b1d28, internal/hooks/go127/weak.go:5-14`](https://github.com/djosh34/gosim/blob/ad69b41b1d28/internal/hooks/go127/weak.go#L5-L14). Pool cleanup is a no-op (`internal/hooks/go127/sync.go:23-25`); simulated `FreeOSMemory`, GC percentage and memory-limit settings do not run/control a simulated collector ([`runtime_debug.go:23-68`](https://github.com/djosh34/gosim/blob/ad69b41b1d28/internal/hooks/go127/runtime_debug.go#L23-L68)). But the **native** Go collector still runs. The GC diagnostic run below collected a 2,403 MB heap to 1,294 MB during the simulation, and the primary samples also fall after GC. The issue's statement that memory is never returned inside a run is too broad. RSS alone cannot assign a fraction of memory to weak-pointer retention.
- **One seed has one execution lane.** [`gosimruntime/runtime.go:242-307`](https://github.com/djosh34/gosim/blob/ad69b41b1d28/gosimruntime/runtime.go#L242-L307) chooses one runnable coroutine, calls `pick.step()`, then chooses again. GNU time reports 110–145% process CPU in the cited cells: one execution lane plus substantial native GC overhead, not four CPUs executing the workload. Parallel CI matrix cells and separate seeds use more CPUs; adding CPUs does not make one seed four times faster.
- **The one-hour helper limit is adjustable.** [`simrun/gosim.go:19-26`](https://github.com/djosh34/tigerstore/blob/95fe1fc8516e0f8e633c2058e24ec775c4736404/internal/simrun/gosim.go#L19-L26) sets a one-hour *virtual* timeout in `Bubble`. The probe sets it to 24 hours, so four virtual hours are not a gosim hard limit. GitHub documents [4 CPUs / 16 GB for this public ARM runner](https://docs.github.com/en/actions/reference/runners/github-hosted-runners#standard-github-hosted-runners-for-public-repositories) and a [six-hour wall-clock job limit](https://docs.github.com/en/actions/reference/limits#existing-system-limits).

## Levers and estimates

**Confirmed envelope:** 10,000 initially populated, full-sized objects for four virtual hours completed in 12 min 02.75 s at 8.887 GiB. At 1,000 objects, four virtual hours completed in 3 min 12.25 s at 5.556 GiB. The usual gray-client fault makes 100,000 full-sized objects unsafe under the 12 GiB guard almost immediately after preload. Smaller values let 100,000 objects finish ten minutes at 6.471 GiB. Preload compaction let one full-value 100,000-object hour finish in 19 min 23.46 s at 7.875 GiB, but its other duration cells still hit the guard. There is no single size-only gosim ceiling.

From one to four virtual hours, peak RSS rose by 1.204 GiB per virtual hour at 1,000 objects, and 1.403 GiB per virtual hour at 10,000 objects. A straight-line extension reaches the 12 GiB guard at approximately 9.35 / 6.22 virtual hours. These are **extrapolations**, not measured maximum durations: GC peaks, fault interleavings and retained histories can change the slope. The measured tail RSS endpoint slopes were about 21.5 / 26.0 MiB per virtual minute. In this region memory is likely to matter well before six wall hours. At 10,000 objects/four hours use one process per runner: two measured peaks sum to 17.77 GiB, already above this runner's memory before the OS.

A similarly crude line between the two four-hour peak-RSS points reaches 12 GiB at about 18,400 initial objects. This is only a next test point, not a safe capacity promise: 100,000 objects have a qualitatively different watch-replay startup cliff. The 100,000-object guard cells were growing by roughly 0.3 GiB per wall second near the stop, while virtual progress stalled. Another four GiB would buy only roughly 13 seconds at that trend; GC could change it, so this is not an observed OOM threshold.

Keep a nonzero scaled disk budget. Zero disables ordinary cleaning, not just NOSPACE protection: [`internal/node/clean.go:49-50,134-160`](https://github.com/djosh34/tigerstore/blob/95fe1fc8516e0f8e633c2058e24ec775c4736404/internal/node/clean.go#L134-L160). The unbudgeted pilot's 1,000-object, one-hour cell took 620.52 s and peaked at 8.263 GiB. The later budgeted/compacting stock-watch cell took 61.22 s and peaked at 1.960 GiB. Both cleaning and compaction behavior changed, along with fault interleavings; do not attribute this whole gain to the cleaner alone.

Other unmeasured levers: stream/digest old checker history instead of retaining every value; incremental/backpressured watch catch-up; realistic checkpoint/window sizes; deterministic weak/pool reclamation in the port; more RAM or a faster single core. Native `GOMEMLIMIT` might reduce transient heap headroom, but cannot collect reachable histories. None has a measured multiplier here. Removing gray clients or other faults changes the test rather than merely optimizing it.

## What was not measured

No kernel OOM was intentionally caused. No cell reached the six-hour GitHub limit or its explicit wall cap. Early guards and the explicit 20-minute wall cap are not proof of a hard 16 GB/six-hour maximum. No 600,000-object cell, extra seeds, open-loop sustained 400 successful calls/s, full end-of-run checker cost, port-reclamation patch, heap allocation profile, tuned checkpoint/window experiment, or native-GC memory-limit control was measured. Do not call a passing capacity run a proof of long-run correctness. A general maximum requires more seeds and profiles; these are measured operating points and workload-specific failures.

## CI audit

Every run used for conclusions or harness validation:

- [40111203732](https://github.com/djosh34/tigerstore/actions/runs/40111203732): initial 1,000-object / 60-second smoke, `0a88ea43`.
- [40111479819](https://github.com/djosh34/tigerstore/actions/runs/40111479819): unbudgeted pilot, `eeeeb2ba`; remaining long cells canceled after discovering that zero budget disables the cleaner. Completed measurements are retained in artifacts.
- [40112008897](https://github.com/djosh34/tigerstore/actions/runs/40112008897): unbudgeted smaller-value pilot, `6087659e`; 100,000 objects reached 498.4 churn seconds before the memory guard at 885.79 wall seconds / 12.919 GiB.
- [40112037401](https://github.com/djosh34/tigerstore/actions/runs/40112037401): unbudgeted native-GC/SIGQUIT diagnostic, `6087659e`; 100,000 objects, guard at 90.61 s / 13.415 GiB, stack in `CrashImage` cloning bytes.
- [40112891835](https://github.com/djosh34/tigerstore/actions/runs/40112891835): scaled budgets and stock adversarial ordinary watches, `2cb5d663`; two remaining long cells canceled once their superseding harness fixed ambiguous compaction completion.
- [40112893509](https://github.com/djosh34/tigerstore/actions/runs/40112893509): matched smaller-value control, `2cb5d663`.
- [40113707142](https://github.com/djosh34/tigerstore/actions/runs/40113707142): primary nine-cell matrix, `95fe1fc8`; ordinary watches start after preload, gray-client fault unchanged, ambiguous compactions recognized.
- [40114211095](https://github.com/djosh34/tigerstore/actions/runs/40114211095): early-compaction control, `90ca284e`.

Registration/build-only attempts not used as measurements: [40111196567](https://github.com/djosh34/tigerstore/actions/runs/40111196567), [40112720850](https://github.com/djosh34/tigerstore/actions/runs/40112720850), [40112741312](https://github.com/djosh34/tigerstore/actions/runs/40112741312), [40113598555](https://github.com/djosh34/tigerstore/actions/runs/40113598555). These were canceled/superseded. The workflow needed an initial branch-only push trigger to register before `workflow_dispatch` could address it; no default-branch edit was made.

Artifacts contain the raw log, GNU time output, five-second RSS/progress samples and JSON result. Recompute exact wall times/rates without compiling or running Go:

```sh
python3 scripts/sim-size-report.py <downloaded-artifact-dir> [...]
```
