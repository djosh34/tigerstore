Measured on `ubuntu-24.04-arm`, on a throwaway branch. Nothing was merged.

**10,000 objects × four virtual hours finished in 12m03s at 8.89 GiB.** A 100,000-object hour also finished after startup compaction, but that change did not prevent memory guards in its other duration cells. These are operating points for seed 1, not a universal gosim maximum.

Harness: [`95fe1fc8516e0f8e633c2058e24ec775c4736404`](https://github.com/djosh34/tigerstore/commit/95fe1fc8516e0f8e633c2058e24ec775c4736404). [Primary CI run](https://github.com/djosh34/tigerstore/actions/runs/40113707142).

The workload preloads Kubernetes-sized values, then runs puts, bounded lists, reads, deletes, 120-second leases, watches and compactions with the usual simulator faults. It uses 16 closed-loop clients, at most 400 calls/s. The faults reduce successful calls to about 4–6/s. Disk budgets scale with object count. Histories and online invariants remain, but full end-of-run checkers do not run.

| Initial objects | Virtual churn | Wall s | Peak RSS GiB | Sim s / wall s | Outcome |
|---:|---:|---:|---:|---:|---|
| 1,000 | 10 min | 17.87 | 1.16 | 35.54 | finished |
| 1,000 | 1 h | 55.74 | 1.94 | 65.21 | finished |
| 1,000 | 4 h | 192.25 | 5.56 | 75.08 | finished |
| 10,000 | 10 min | 114.62 | 3.54 | 5.82 | finished |
| 10,000 | 1 h | 222.18 | 4.68 | 16.50 | finished |
| 10,000 | 4 h | 722.75 | 8.89 | 20.02 | finished |
| 100,000 | 10 min | 80.57 | 13.00 | 4.24 | memory guard after preload |
| 100,000 | 1 h | 80.57 | 12.57 | 4.24 | memory guard after preload |
| 100,000 | 4 h | 75.63 | 13.77 | 4.51 | memory guard after preload |

Wall time and RSS come from GNU time and five-second RSS samples. Rates include preload, healing and settling. The 100,000-object rates mostly describe preload: all objects loaded, but no subsequent ten-second churn progress tick arrived. They are not sustained churn rates. The monitor stops at 12 GiB; sampling can overshoot. These were deliberate stops, **not measured kernel OOMs**. No cell reached the 20-minute inner wall cap or six-hour job limit.

### Controls

[Smaller-value CI](https://github.com/djosh34/tigerstore/actions/runs/40112893509), harness `2cb5d663`, used the stock ordinary-watch policy. Values averaged 147 B instead of 1,458 B. All ten-minute cells finished.

| Objects | Virtual churn | Wall s | Peak GiB | Sim s / wall s | Outcome |
|---:|---:|---:|---:|---:|---|
| 1,000 | 10 min | 12.92 | 0.53 | 48.41 | finished |
| 10,000 | 10 min | 71.76 | 1.82 | 9.00 | finished |
| 100,000 | 10 min | 616.25 | 6.47 | 1.40 | finished |

Against its [matched full-value baseline](https://github.com/djosh34/tigerstore/actions/runs/40112891835), the first two peaks fell by 1.85× and 2.71×. Ten times smaller values do not mean ten times smaller process memory.

[Preload-compaction CI](https://github.com/djosh34/tigerstore/actions/runs/40114211095), harness [`90ca284e5213419344fbd1a5ad88af38791214c2`](https://github.com/djosh34/tigerstore/commit/90ca284e5213419344fbd1a5ad88af38791214c2), kept full-sized values and all faults. It compacted on the leader before starting faults, without an all-followers-applied barrier.

| Objects | Virtual churn | Wall s | Peak GiB | Sim s / wall s | Outcome |
|---:|---:|---:|---:|---:|---|
| 100,000 | 10 min | 80.56 | 12.33 | 4.35 | guard; last logged churn 8.8 s |
| 100,000 | 1 h | 1,163.46 | 7.87 | 3.40 | finished |
| 100,000 | 4 h | 75.54 | 12.07 | 4.64 | guard; last logged churn 8.8 s |

Duration changes fault scheduling and random draws. The one-hour cell is not a prefix of the four-hour cell. Startup compaction helped one cell, but is not a reliable fix by itself.

### Limits and levers

- **Retained state and copies matter.** The simulator keeps [every call and response (`client.go:180–205`)](https://github.com/djosh34/tigerstore/blob/95fe1fc8/internal/sim/client.go#L180-L205) and [committed entry bytes (`storage.go:34–73`)](https://github.com/djosh34/tigerstore/blob/95fe1fc8/internal/sim/storage.go#L34-L73). [Recovery clones disk contents (`simfs.go:316–344`)](https://github.com/djosh34/tigerstore/blob/95fe1fc8/internal/simfs/simfs.go#L316-L344). Streaming/digesting old checker history and reducing copies could raise the limit. The existing tiny checkpoint/window settings also merit a separate sizing experiment.
- **Gray clients make large replay bursts.** [Ten unread watches reconnect every 50–300 ms, with revision 1 chosen one quarter of the time (`gray.go:147–189`)](https://github.com/djosh34/tigerstore/blob/95fe1fc8/internal/sim/gray.go#L147-L189). [History materializes and sorts matching changes (`store.go:571–587`)](https://github.com/djosh34/tigerstore/blob/95fe1fc8/internal/mvcc/store.go#L571-L587). Abort stacks show watch catch-up, cold value reads and recovery copies. Starting ordinary watches after preload did not remove the gray-client fault. Bound/backpressure catch-up before assuming more RAM alone solves it.
- **The port retains some objects, not every allocation.** [Weak pointers stay strong (`weak.go:5–14`)](https://github.com/djosh34/gosim/blob/ad69b41b1d28/internal/hooks/go127/weak.go#L5-L14). But native GC collected memory during the run; “no memory is ever returned within a run” is too broad. A port reclamation fix cannot free histories that the simulator still explicitly owns. Its benefit was not measured.
- **One seed has one execution lane.** [The scheduler runs one selected coroutine at a time (`runtime.go:242–307`)](https://github.com/djosh34/gosim/blob/ad69b41b1d28/gosimruntime/runtime.go#L242-L307). Measured process CPU was 110–145%, including native GC, not 400%. Parallelize across jobs. Two 10,000-object/four-hour peaks would exceed this runner's RAM.
- **Configuration matters.** [Zero disk budget disables ordinary cleaning (`clean.go:49–50,134–160`)](https://github.com/djosh34/tigerstore/blob/95fe1fc8/internal/node/clean.go#L134-L160). Keep a scaled nonzero budget. The ordinary one-hour virtual timeout in `simrun.Bubble` was raised to 24 hours for this probe.

From one to four virtual hours, peak RSS grew by about 1.2 GiB/virtual hour at 1,000 objects and 1.4 GiB/hour at 10,000. A straight-line extension reaches the 12 GiB guard near nine and six virtual hours. **Those are estimates, not measured maximum durations.** Memory is likely to bind before six wall hours in this profile.

Other CI evidence used: [initial smoke](https://github.com/djosh34/tigerstore/actions/runs/40111203732), [unbudgeted pilot](https://github.com/djosh34/tigerstore/actions/runs/40111479819), [unbudgeted smaller-value pilot](https://github.com/djosh34/tigerstore/actions/runs/40112008897), and [native-GC/abort-stack diagnostic](https://github.com/djosh34/tigerstore/actions/runs/40112037401). The throwaway branch's `docs/research/sim-size-401.md` records the methodology, canceled superseded runs and source details.

Not measured: a kernel OOM or six-hour wall limit; 600,000 objects; more seeds; sustained 400 successful calls/s; full checker costs; a port-reclamation patch; or a reliable full-value 100,000-object/four-hour run. These capacity runs are not long-run correctness proofs.
