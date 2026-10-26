# #448: memory as the limit

Research only. Branch `exp/448-memory`. Nothing merged. No issue comment posted.

## Evidence and method

- **M1:** commit `53875e5b`, Actions run **40283731158**. `scripts/sim.sh ./internal/sim '^$'`, then `TestMemory448` natively and under gosim. Cases: DefaultConfig, Kubernetes short, Kubernetes long shortened to 10 minutes, and that 10-minute profile with 50,000 object slots. Artifacts: `memory448-{default,short,long10,data10}`. This run succeeded.
- **M2:** commit `7f40b5fc`, Actions run **40284798964**. Same build. Cases: default, short, 30-minute long, 50,000-slot/10-minute long. Each ran natively, in gosim with heap profiles, and in gosim without forced collections. Artifacts: `memory448-{default,short,long30,data10}`. All twelve test processes returned zero. The workflow itself failed because its last shell glob test returned false when raw mode deliberately had no profiles. The final branch fixes that reporting bug. It was not a test failure.
- `TestMemory448` calls the existing `run` and `checkers` with the existing configurations. DefaultConfig is TestSeeds's scenario and checks, without its campaign-level coverage assertions. Kubernetes configurations are TestKubernetesLoad's. `EXP441_MINUTES` is #441's shortening mechanism. Native execution bypasses the normal race-only gate for research; it uses synctest, not gosim. Native and gosim run the same code/configuration, **not the same interleaving or history**.
- Profile mode collects real `runtime.MemStats`, forces a real GC at the labelled snapshots, and writes real heap profiles through #441's escape hatch. Raw mode does neither forced GC nor the extra compression pass. Its RSS is the sum of `translated*` and `sim.test`, sampled every 100 ms. Profiles use Go's default sampling rate, so profile attribution is approximate. MemStats and explicit slice counts are exact at their snapshots.
- M1's GNU time RSS for gosim is only the driver. Do not use it as the scenario's RSS. M2 fixes this with #441's child-process sampling technique.
- **R1:** #441 commit `85ed39c2`, run **40282688114**, seed `202610260009002`, 60 virtual minutes. Read with `gh run view 40282688114 --log` and downloaded artifact `exp441-202610260009002`.
- **R2:** same commit, run **40282690495**, same seed, 120 virtual minutes. Its completed job is **121439272590**. Read with `gh api repos/djosh34/tigerstore/actions/jobs/121439272590/logs`; downloaded the same named artifact. The whole workflow was still running other seeds when read.
- **R4:** #441 commit `cffcc24c`, run **40282629536**, seed `202610260012001`, four hours. Completed job **121439090456**. Read its log through the jobs API. Other seeds of that workflow were still running. No long seed was repeated for this investigation.

All new local runs were short, with `GOMAXPROCS=2 GOFLAGS=-p=1`, after checking at least 3 GiB was available. All gosim builds/runs were on Actions. No owned run remains active.

## Hardware

M2's `nproc; lscpu; free -b` measured four cores and approximately **16,722,000,000 physical bytes = 15.57 GiB** per `ubuntu-24.04-arm` runner. M1 measured the same. The available memory before builds was about **15.27 billion bytes = 14.22 GiB**. There is also swap; none of the capacity arithmetic below relies on it.

Use **12 GiB per runner for scenarios** below. That is an explicit planning allowance, not measured usable capacity. It leaves approximately 3.57 GiB of physical RAM for the OS, driver, tools and uncertainty. `GOMEMLIMIT=12GiB` in `scripts/kube-sim.sh` is per process and is a soft Go-runtime limit, not a runner-wide reservation or a hard RSS cap. Go accounts approximately `MemStats.Sys - HeapReleased`; the executable and external mappings are additional. Four processes each configured with that limit can still exhaust a runner.

## What a scenario holds now

M2, seed 1. Heap means after full GC at load end. RSS means the separate raw gosim run, without forced GC. Native RSS is the profile-mode native run. MiB are binary.

| Configuration | Native live heap MiB | Gosim live heap MiB | Native peak RSS MiB | Raw gosim peak RSS MiB | Gosim calls |
|---|---:|---:|---:|---:|---:|
| TestSeeds default | 19.3 | 95.7 | 79.9 | 198.0 | 9,646 |
| Kubernetes short, 1,000 slots | 30.4 | 130.3 | 129.5 | 289.9 | 2,284 |
| Kubernetes long, 10,000 slots, 30 minutes | 254.9 | 546.6 | 848.6 | 1,564.2 | 35,718 |
| Kubernetes long, 50,000 slots, 10 minutes | 635.9 | 499.0 | 1,916.8 | 1,452.1 | 9,763 |

The Kubernetes load end includes its fixed drain: fault duration + 10 client seconds + 242 lease-expiry seconds. Thus the two-minute short case reaches virtual second 372, the ten-minute case 852, and the thirty-minute case 2052. Cold-restart checking runs afterwards. These tails matter when extrapolating small profiles.

**Slots are not live objects.** The initialization advances past a batch even when its Txn fails. M2 measured only **220**, **2,843**, and **6,326 live keys** in the short, 30-minute, and 50,000-slot cases. The last row is not proof that 50,000 full-size live objects fit. Native histories differed; its last row had 9,802 live keys, explaining why subtracting native RSS from gosim RSS is not a translation-overhead measurement.

### History: there is no universal bytes-per-operation constant

`unsafe.Sizeof` on M2's ARM runner measured:

- simulator `Record`: **88 B**, plus its request, response/error and pointed-to contents;
- checker `history.Op`: **104 B**, plus its request/response structures;
- simulator `WatchRecord`: **16 B**, plus the protobuf response;
- checker `history.Event`: **80 B** inline; an allocated previous `history.KeyValue` adds **64 B**.

`GOMAXPROCS=2 GOFLAGS=-p=1 go run ./cmd/exp448sizes` on this host's Go 1.27.2 measured protobuf Event **64 B**, KeyValue **120 B**, WatchResponse **120 B**, RangeRequest **152 B**, RangeResponse **88 B**, TxnRequest **112 B**, TxnResponse **80 B**, PutRequest **104 B**, Compare **112 B**. This is a local 64-bit size measurement, not a new ARM run.

A protobuf watch event therefore needs at least **192 B** for an Event, one KV and its event-slice pointer; an update/delete with PrevKV needs **312 B**, before keys, values, response/header and allocator rounding. The second checker representation adds its own event metadata. Interning shares equal values but not all these objects.

For the M2 30-minute case, explicit counters measured **35,718 Records**, **32,974 watch responses**, **14,851 watch events**, **46,504,519 value bytes** retained by `values.bytes`, and **20,453,130 raw committed-entry bytes** retained by `s.log`. Shallow Records alone are `35,718 * 88 = 3,143,184 B`. Request/response protobuf *wire sizes* total **84,332,715 B**; watch wire sizes total **31,260,662 B**. Those are serialization sizes, not heap sizes, and must not be added to the heap as independent allocations.

Converting the result to checker history raised full-GC heap from **554.1 to 624.8 MiB**, or **70.7 MiB** (M2 `MEM448.result`/`MEM448.checkable`). This is an incremental whole-conversion measurement, not a universal per-op cost. The profile attributes about **40.1 million bytes** to `history/frometcd.putRequest`, chiefly the additional string representation of values. `values.strings` retains these strings even after a checker invocation ends.

### Disks and copies dominate this seed

M2's explicit `Disk.Memory` counter counts unique files reachable through each disk directory, including durable/pending directory state. It measures byte-slice capacities, excluding maps, file structs, allocator rounding and unlinked open handles.

| Configuration | Three current disks, logical bytes | Current disk byte-slice capacities | Last-crash images | Saved cold-restart disk copies |
|---|---:|---:|---:|---:|
| default | 39,541 | 89,872 | 14,884 | 113,296 |
| short | 3,578,876 | 8,135,184 | 918,570 | 1,841,648 |
| 30-minute long | 49,988,144 | 104,333,840 | 46,053,293 | 188,405,328 |
| 50,000-slot/10-minute | 54,311,153 | 111,215,120 | 53,527,092 | 98,342,624 |

Current simulated disks cost **2.05–2.27 bytes of slice capacity per logical file byte**, before the extra saved images. This follows from separate page-cache and durable byte arrays. Saved checkpoints also clone both arrays (`simfs.Durable`), while last-crash images clone another set (`Disk.Files`). The 30-minute case holds **338,792,461 B = 323.1 MiB** across these disk/image arrays, approximately **59%** of its 546.6 MiB live heap. Its three saved cluster checkpoints contain 71 files; the current disks contain 27 files. File counts include metadata/checkpoints/spares, not only WAL segments. Segment-kind counts were not measured.

The same 30-minute disks had accepted **95,185,179 written bytes**. Current disk capacity/written-byte ratio is **1.10**; including the saved arrays it is **3.56**. This ratio is time/configuration dependent, not an allocation cost per write. Cleaning means current disk size need not grow with every byte ever written.

`kubeConfig` uses 4 MiB segments. Per node it sets `DiskBudget=max(16MiB,8192*slots)` and `DiskCapacity=2*DiskBudget`. Therefore budget/capacity are **16/32 MiB**, **78.125/156.25 MiB**, and **390.625/781.25 MiB** for 1,000, 10,000 and 50,000 slots. These are limits, not allocated buffers. The default scenario uses **32/128 KiB**.

### Tigerstore, Raft, model, gosim, GC

M2's 30-minute frozen active nodes each report approximately **1.267 million bytes of MVCC index**, **2,843 live keys**, and **0–28 entry-data bytes** in the core's Raft/storage window. The core Node struct itself is **1,880 B**. The 50,000-slot case reports **3,197,567 index bytes per active node**, **6,326 live keys**, and **0–27 entry bytes**. These index numbers use the store's existing `Bytes()` estimator, not a whole-node retained-heap traversal. Raft protocol metadata, channels and driver buffers are additional; their complete per-node retained size was not isolated.

The simulator's uncompacted replay model reports **1,665,917 index bytes** for the 30-minute case. Its creation raised live heap by approximately **10.6 MiB**; that delta also includes decoded-log and auxiliary allocations. The other checkers independently build past-version maps and witness/search structures (`check/replay.go`, `check/witness.go`). Their models are not just the live current KV set.

The profiles consistently attribute approximately **67,862,528 B = 64.7 MiB** to gosim's `allocGlobals`. This is a substantial fixed translation/runtime cost before the scenario grows. M2's 30-minute load-end profile additionally samples approximately **17.5 million bytes** in two major gosim channel-allocation types. Map wrappers, scheduling and networking add more. These are allocation-stack attributions, not disjoint ownership totals.

Memory *can* be collected and returned within a run. The port's weak pointers are strong, but it does not retain every allocation. In M2, 30-minute raw load-end HeapAlloc was **954.7 MiB**, versus **546.6 MiB** after forced GC in the same deterministic scenario. Real HeapReleased was nonzero and changed throughout. The old blanket statement “memory is never returned” is too strong. A weak-pointer fix alone cannot release histories and disk copies that the harness explicitly owns.

The raw 30-minute peak was **1,564.2 MiB**, well above its live heap. Default GOGC normally targets approximately twice live heap; fragmentation, stacks, runtime metadata and the driver add headroom. At these sizes the 12 GiB Go limit does not bind. Live heap, HeapSys/Sys and RSS are not interchangeable.

## Growth per hour and per object

- **Within one seed:** M2 30-minute profile, full-GC snapshots at seconds 600 and 1800: **243.91 -> 434.32 MiB**, or `(434.32-243.91)*3 = 571.2 MiB/live-heap per virtual hour`. In that interval the rate was **69,717 calls/hour**, **58,917 watch responses/hour**, **15,654 watch events/hour**, **63,380,043 new interned-value bytes/hour**, and **26,115,783 retained committed-entry bytes/hour**. This excludes its final drain/checker peak.
- **Current complete scenarios:** R1/R2, same seed and object count: peak **1,878 -> 3,252 MiB** for one -> two hours. The empirical increment is **1,374 MiB/hour**. Length changes scheduling, so these are not exact prefixes. R4's other four-hour seed peaked at **5,317 MiB**, consistent with approximately 5 GiB production observations, but not an identical-seed fit.
- **Object-slot sensitivity:** M1's 10-minute full-GC load-end heaps were **231.69 MiB at 10,000 slots** and **499.10 MiB at 50,000 slots**. Difference/40,000 is approximately **7.0 KiB per additional configured slot**. This is a cross-configuration empirical slope, not bytes per live object and not a guaranteed extrapolation. The fault/RNG schedule and live object count change too.
- #401's 8.9 GiB four-hour run was a different earlier harness without full end checks. Its documented 1.2–1.4 GiB/hour slope supports history retention as a cause, but is not a matched current baseline. Faults farther apart increase successful work and memory; no universal bytes/hour number follows from a virtual clock alone.

## Compact history and a conditional floor

There is a concrete compact representation already available: gzip of the **complete checker History**, including Ops, Log, Watches and Target. #441 ran the native checkers successfully on these saved inputs.

| Source | Checker ops | Compressed bytes | B/checker op, all history included |
|---|---:|---:|---:|
| R1, one hour | 59,487 | 8,468,765 | 142.36 |
| R2, two hours | 125,998 | 17,996,453 | 142.83 |
| R4, four hours | 238,012 | 32,528,302 | 136.67 |

These are measured archive sizes, **not an information-theoretic lower bound or the RAM used by today's decoders/checkers**. R1's native checker invocation peaked at **1,259,984 KiB**, and R2's at **2,601,172 KiB**, from the existing GNU time results. Merely moving today's checkers out of gosim does not yield the compressed-size RAM floor. Values in this simulator are highly compressible deterministic strings; do not transfer the archive ratio to arbitrary etcd user values.

A floor-oriented design would retain:

1. Each node's durable live disk image once, plus dirty/pending writes needed by the crash model. Use immutable/COW pages, not full page-cache + durable + crash + checkpoint clones.
2. Per-node current MVCC/Raft indexes and bounded in-flight entries; an independent model with value identities or regenerated deterministic values.
3. Compact observations and the unresolved checker frontier. Spill old history to disk or retain the compressed archive and replay it when needed.

It can check completed revision/watch prefixes and cold-restart snapshots promptly, then discard their full protobufs, old expected versions, trace strings and redundant images. The watermark must cover every active historical read, watch and unresolved ambiguous call. **FromRevisions's ambiguous-write search has no proven bounded frontier here.** Keeping only the current KV state or trusting the server log would weaken the present independent checks. An unconditional constant-RAM streaming checker has not been demonstrated.

For explicit capacity arithmetic, use this **illustrative optimized envelope, not a measured implementation**:

`F(N,h) = 128 MiB + 5,029.45*N bytes + 10 MiB*h`.

Here N means **fully live objects**, not configured slots. From `internal/k8ssize`, weighted mean value size is `9,253,583/6,345 = 1,458.41 B`; the generated mean key length is **35.56 B**. The store's `Bytes()` formula gives one retained version approximately `64+keyBytes+64 = 163.56 B` per key. Three value copies plus four such indexes are `3*1458.41 + 4*163.56 = 5,029.45 B/live object`. The model can regenerate deterministic expected values; storing another full expected-value copy would add **1,458.41 B/object**.

The **128 MiB fixed allowance is an assumption** for runtime, bounded pending data, temporary buffers and working indexes. It is not a proved bound on historical versions, GC or ambiguous search. **10 MiB/hour is a padded estimate** from the measured approximately 8–9 MB/hour compact archives. It assumes their approximately 60–70 thousand calls/hour mix. At 400 calls/s, simply applying 144 B/op instead gives `400*3600*144 = 207,360,000 B/hour`; rates and retained frontier must be specified. Fully streaming/spilled completed history could remove the archive's RAM growth, but not the disk-history growth or an unbounded unresolved frontier.

## Seeds per runner, and largest sizes

Use the explicit 12 GiB scenario budget above. Memory-only fits are `floor(12288MiB / measured_or_estimated_seed_MiB)`. This is additive-peak planning, not a measured parallel packing test. Productive seed concurrency is additionally capped by the runner's four cores.

| Kind | Now: memory-only seeds | Optimized envelope: memory-only seeds | Core-limited now / envelope |
|---|---:|---:|---:|
| default short | 62, from M2 198.0 MiB | 95, about 128 MiB | 4 / 4 |
| Kubernetes short | 42, from M2 289.9 MiB | 92, 133.1 MiB at 1,000 fully live objects | 4 / 4 |
| 30-minute, 10,000 slots | 7, from M2 1,564.2 MiB | 67, 181.0 MiB at 10,000 fully live objects | 4 / 4 |
| four-hour, 10,000 slots | 2, from R4 5,317 MiB | 56, 216.0 MiB at 10,000 fully live objects | 2 / 4 |
| 50,000-slot/10-minute | 8, from M2 1,452.1 MiB, only 6,326 live keys | 33, 369.5 MiB for 50,000 fully live objects | 4 / 4 |

The now/envelope data counts deliberately differ where today's configured slots were not all live. Do not advertise that last row as a matched 50,000-live-object speedup.

- **Longest now, estimated:** R1/R2 fit `peak MiB = 504 + 1374*h`. At the 12 GiB allowance, `(12288-504)/1374 = 8.58 virtual hours` for one 10,000-slot seed. Four at once would allow approximately **1.87 hours each** under that fit. Neither is a measured maximum, and the job's wall-clock limit/checker search may bind sooner.
- **Largest now in fully live objects:** not established. A deliberately crude fixed-profile extrapolation of M2's 50,000-slot run gives `50000*12288/1452.06 = 423,123 slots`, or `6326*12288/1452.06 = 53,534 live keys` if occupancy and costs stayed identical. They will not necessarily do so. This is an arithmetic illustration, not a supported capacity claim.
- **Envelope, one 10,000-live-object seed:** `(12288-128-47.96)/10 = 1,211 hours` from memory alone. Four at once: approximately **290 hours each**. These are conditional engineering estimates, not reachable Actions job durations or a streaming-checker proof.
- **Envelope, four-hour seed:** `(12288-128-40)*2^20 / 5029.45 = 2.53 million fully live objects` for one seed, approximately **605,000 each** for four seeds. This ignores any growing ambiguous frontier and assumes efficient disk/version retention and the stated working-memory allowance. It is a hardware-envelope calculation, not a tested configuration.

The practical result is less speculative: short scenarios already fit four cores comfortably. Current four-hour seeds are memory-limited to roughly two per runner under this allowance. Redundant simulated disk images, full protobuf histories, second value strings, checker past-state/search, and GC headroom explain the gap to a compact representation. Raft's tiny frozen entry window is not the bulk of the retained bytes.

## Not measured

No maximum-size/OOM search. No fully preloaded 50,000/100,000-live-object current case. No matched-schedule native/gosim overhead factor. No exact retained heap per node or Raft-metadata root. No WAL-versus-pack/checkpoint file count. No implemented compact or streaming checker, bounded ambiguous frontier, COW disk patch, or proof of the 128 MiB working allowance. No simultaneous packing run beyond #441's existing work. No new multi-hour scenario.

The #441 comments API was read before finishing; it still returned no comments at that check. Completed #441 jobs were used directly rather than waiting for its other long seeds.
