# Research #449, Q3: data capacity with every simulator checker

Throwaway branch `exp/449-data-size`. No PR, merge or issue comment.

## Measured answer

On one `ubuntu-24.04-arm` runner, seed 1, this probe passed every checker at **200,000 Kubernetes-sized objects**, **200 objects of 1 MiB**, and **200,000 individual overwrite revisions**. The next tested points (300,000 objects, 400 objects of 1 MiB, 400,000 overwrite revisions) hit the 14.5 GiB RSS guard. These are brackets for this profile and this seed, not universal maxima or safe campaign sizes.

Main measurement commit: [`e3a9516f78a50377fc28fe44ec7b088cd9066817`](https://github.com/djosh34/tigerstore/commit/e3a9516f78a50377fc28fe44ec7b088cd9066817).
[Actions run 40285653104](https://github.com/djosh34/tigerstore/actions/runs/40285653104).
The workflow is red because four cells deliberately reached the guard; eight passed. No kernel OOM and no six-hour limit was measured. The longest cell lasted 249 seconds, excluding build and queue time.

### Wall time and sampled RSS

All times below are measured native wall seconds. `scenario` includes the 120 simulated seconds with faults, 10 seconds of healing, 242 seconds of lease/watch settling, and any existing end-settling extension. `own` is `Sim.check`; `independent` is all five independent checks, including witness in both modes. Hashing/shutdown is reported separately. Dashes mean that phase did not finish before the guard.

| Bootstrap (objects; value size; overwrites/key) | Result | Wall s | Load s | Scenario s | Own s | Independent s | Hash/shutdown s | Peak RSS GiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| [10k; k8ssize; 0](https://github.com/djosh34/tigerstore/actions/runs/40285653104/job/121450211351) | pass | 8.949 | 0.690 | 4.687 | 0.397 | 2.192 | 0.890 | 1.136 |
| [30k; k8ssize; 0](https://github.com/djosh34/tigerstore/actions/runs/40285653104/job/121450211303) | pass | 22.799 | 2.197 | 8.678 | 1.350 | 8.280 | 2.127 | 2.998 |
| [100k; k8ssize; 0](https://github.com/djosh34/tigerstore/actions/runs/40285653104/job/121450211195) | pass | 79.855 | 8.966 | 31.205 | 4.945 | 27.189 | 7.166 | 9.837 |
| [200k; k8ssize; 0](https://github.com/djosh34/tigerstore/actions/runs/40285653104/job/121450211213) | pass | 192.450 | 17.650 | 54.401 | 10.782 | 94.420 | 14.643 | 11.990 |
| [300k; k8ssize; 0](https://github.com/djosh34/tigerstore/actions/runs/40285653104/job/121450211477) | guard entering own checks | 171.802 | 28.120 | 124.486 | — | — | — | 14.569 |
| [1k; 64 KiB; 0](https://github.com/djosh34/tigerstore/actions/runs/40285653104/job/121450211196) | pass | 24.910 | 1.169 | 15.079 | 2.629 | 0.917 | 4.917 | 4.572 |
| [200; 1 MiB; 0](https://github.com/djosh34/tigerstore/actions/runs/40285653104/job/121450211385) | pass | 99.530 | 3.976 | 38.117 | 15.981 | 4.755 | 36.159 | 13.097 |
| [400; 1 MiB; 0](https://github.com/djosh34/tigerstore/actions/runs/40285653104/job/121450211242) | guard entering own checks | 95.689 | 8.040 | 79.270 | — | — | — | 14.636 |
| [10k; k8ssize; 20](https://github.com/djosh34/tigerstore/actions/runs/40285653104/job/121450211321) | pass | 125.132 | 64.316 | 34.182 | 4.457 | 13.684 | 7.928 | 11.987 |
| [1k; k8ssize; 200](https://github.com/djosh34/tigerstore/actions/runs/40285653104/job/121450211198) | pass | 182.495 | 67.383 | 81.895 | 7.195 | 11.268 | 14.170 | 11.993 |
| [10k; k8ssize; 40](https://github.com/djosh34/tigerstore/actions/runs/40285653104/job/121450211272) | guard during scenario | 248.837 | 137.173 | — | — | — | — | 14.524 |
| [1k; k8ssize; 400](https://github.com/djosh34/tigerstore/actions/runs/40285653104/job/121450211388) | guard during scenario | 180.828 | 137.756 | — | — | — | — | 14.736 |

Times do not sum exactly to wall time because interpreter startup and result delivery are outside the phase marks. The monitor samples the translated simulation process once a second; it does not measure a kernel high-water mark, the compiler, runner services or the small Python monitor. Sampling can miss spikes or overshoot the guard. `GOMEMLIMIT=12GiB` is a soft Go runtime limit, not the RSS guard. Every job uses `GOMAXPROCS=2 GOFLAGS=-p=1`; the runner has four CPUs and 16 GB RAM. One cell per job, 19,800-second inner wall guard, 350-minute job timeout.

### Final frozen stores, before checks

These are **measured**, not input sizes. Live value bytes come from the latest live keys' MVCC reference lengths, not serialized JSON and not empty placeholders from cold reads. Disk bytes are `simfs.Disk.Bytes()`: visible file lengths, including WAL/checkpoint/old-version data, not allocation capacity and not its second durable-memory copy. Disks can include a deliberately fenced node with stale data; they need not have equal sizes. Every passing row subsequently passed all checks.

| Bootstrap | Final live keys | Live value bytes | Revision | Node 1 disk B | Node 2 disk B | Node 3 disk B |
|---|---:|---:|---:|---:|---:|---:|
| 10k k8ssize | 9,845 | 14,457,568 | 1,191 | 17,354,717 | 17,354,354 | 16,467,788 |
| 30k k8ssize | 29,925 | 43,551,432 | 752 | 47,566,986 | 46,620,638 | 47,567,019 |
| 100k k8ssize | 99,925 | 145,205,066 | 1,267 | 156,292,391 | 156,292,325 | 155,539,968 |
| 200k k8ssize | 199,947 | 289,895,060 | 2,020 | 310,266,394 | 311,345,625 | 311,344,155 |
| 1k × 64 KiB | 945 | 61,931,520 | 482 | 90,586,558 | 68,209,903 | 90,586,558 |
| 200 × 1 MiB | 126 | 132,120,576 | 642 | 497,055,998 | 95,434,924 | 497,055,666 |
| 10k × 20 overwrites | 9,910 | 14,179,560 | 200,745 | 18,872,029 | 18,878,166 | 332,209,531 |
| 1k × 200 overwrites | 956 | 1,360,479 | 200,327 | 23,747,236 | 23,747,665 | 337,439,011 |

For killed cells there is no completed/checkable result and no final-store/disk measurement. The native phase file proves their entire bootstrap completed. Gosim buffers its detailed test log until returning the result, so the guard loses those detailed lines. The 400 × 1 MiB bootstrap is exactly 419,430,400 payload bytes by construction; the 300k k8ssize payload was not recovered and must not be presented as a measured byte count.

Measured payloads before faults: 10k = 14,544,536 B; 30k = 43,643,428 B; 100k = 145,283,847 B; 200k = 289,974,713 B. The 10k × 20 case made 210,000 bootstrap values totaling 302,604,506 B; 1k × 200 made 201,000 totaling 308,935,332 B. Each overwrite is a separate transaction/revision. Initial bulk transactions change up to 128 keys at one revision, so object counts and revisions are separate dimensions.

## What the prototype does, and what it changed

[`data_heavy_test.go`](https://github.com/djosh34/tigerstore/blob/e3a9516f78a50377fc28fe44ec7b088cd9066817/internal/sim/data_heavy_test.go) compiles each cell's numbers into a test name. Harness and scheduler seed are both 1. It starts the real three-node simulator, does a fault-free bootstrap through `Sim.call`/`Node.Do`, retains every request/response and `s.log`/record as usual, then starts the Kubernetes mix with all the `DefaultConfig` fault switches and unspread default rates. Overwrites happen before faults. No check is omitted or relaxed.

Initial transactions use at most 128 operations and 1.5 MiB minus 64 KiB of envelope headroom. Every value is unique. The initial objects use the Kubernetes keys/resource histogram; fixed-size cells keep that fixed size during churn too. Bootstrap Events are persistent (no lease); churn subsequently puts Events on the usual 120-second leases. This is a data/retained-version stress case, not a faithful aged Kubernetes store.

The 16 clients all use `Node.Do`, not clientv3, avoiding known gosim/gRPC hang #440 as the existing long kube profile does. Reads, paginated lists, CAS transactions, creates/deletes, Event leases, watches with progress and compaction requests reuse `kube.go`. Compaction is attempted every 20 simulated seconds, as the existing short kube profile, rather than production's five minutes. Faults sometimes prevent any compaction from being acknowledged. All faults are configured, but a two-minute seed does not necessarily exercise every kind. In particular the existing rot routine looks for the small `k0`…`k7` keys, not kube keys, and its count is zero in these runs; this probe does not fix that.

`tinyLimits` would reject/drop large proposals and make almost every bulk write trigger checkpoint/snapshot work. The probe instead uses window 4/8 MiB (was 16/32 B), unapplied 64 MiB (was 16 KiB), checkpoint threshold 4 MiB (was 256 B), chunk 32 KiB (was 32 B). It retains tinyLimits' checkpoint ratio 2, SnapshotTicks 10 and `Record=true`. Segments stay the kube profile's 4 MiB. Disk budget is `max(32 MiB, objects × max(value-size, 2048) × (overwrites+1) × 3)` and capacity twice that, so old bootstrap versions do not prematurely trip a toy NOSPACE limit. Cold restarts use the same raised core limits. Production's windows and checkpoint threshold are still much larger (64/128 MiB and 256 MiB).

The main sweep also raises the simulated connection buffer from 2 KiB to **256 KiB**, including cold restarts. It changes no peer deadline or checker settling bound. A tiny transport can be a capacity limit before RAM: see the control below.

`runInspect` calls `Sim.check`, including replay without compaction, historical reads/writes, real-time/log/watch/lease/liveness checks, cold erase-one-disk restarts and recovery. Then [`independent`](https://github.com/djosh34/tigerstore/blob/e3a9516f78a50377fc28fe44ec7b088cd9066817/internal/sim/sim_test.go#L515) runs durability, revisions, watches, witness-from-log and witness-from-revisions, with the original 50-million-step witness bound. Hashes still cover the trace, history and disks. Porcupine is not part of `independent` and is not run on these large histories.

Native-clock timestamps and runtime heap counters are written into `phases.jsonl` by `internal/dataprobe` using explicit `//gosim:notranslate` standard-library imports. None feeds back into simulated decisions. Heap files use native pprof without explicit GC. `scripts/data-heavy-449.py` runs one translated binary via gosim's metatest protocol and records RSS, logs and result JSON. Artifacts are `data449-<test-name>`, containing `summary.json`, `phases.jsonl`, `rss.jsonl`, `run.log` when returned, and the 30k heap profiles.

Replay after building with `GOMAXPROCS=2 GOFLAGS=-p=1 scripts/sim.sh ./internal/sim '^$'`: run `DATA449_BINARY=<translated.test> RUNNER_TEMP=<output-dir> CELL=TestDataHeavy_100000_0_0 GOMAXPROCS=2 GOFLAGS=-p=1 python3 scripts/data-heavy-449.py`. The test name is the complete parameter set. Replay-twice equality was not measured here.

## Measured tiny-transport control

[`9033e041c08b3e7cf1e0c1552e494fda1decf714`](https://github.com/djosh34/tigerstore/commit/9033e041c08b3e7cf1e0c1552e494fda1decf714), [run 40284612563](https://github.com/djosh34/tigerstore/actions/runs/40284612563), already had the raised core/disk limits but retained the stock 2 KiB socket buffer. Five cells passed all checkers:

| Bootstrap | Wall s | Peak GiB | Final revision |
|---|---:|---:|---:|
| 10k k8ssize | 6.731 | 1.026 | 451 |
| 30k k8ssize | 17.580 | 2.335 | 567 |
| 1k × 64 KiB | 17.978 | 4.012 | 288 |
| 10k × 20 overwrites | 130.323 | 11.979 | 200,681 |
| 1k × 200 overwrites | 163.804 | 11.992 | 200,495 |

100k objects failed cold restart convergence after 36.410 wall s at 6.766 GiB. Node 2 was still applied only through entry 31 while node 3 had entry 797; the follower was fetching a full snapshot. 200 × 1 MiB failed the same way after 52.947 s at 11.372 GiB (entry 12 versus 210). These were checker failures, **not memory failures and not passing full-checker cells**. The current cold check allows six seconds, then one more second for convergence. The 2 KiB buffer serializes large snapshots into many simulated link delays. Raising it to 256 KiB produced the passing 100k/200k and 200 MiB cells above, with unchanged assertions/time bounds. Changing scheduling also changes RNG draws and fault outcomes; this is not a matched exact-prefix benchmark.

Earlier smoke runs [40283882299](https://github.com/djosh34/tigerstore/actions/runs/40283882299) at `6362e8e2` and [40284085517](https://github.com/djosh34/tigerstore/actions/runs/40284085517) at `e2be5fc9` failed independent history validation because sequential bootstrap calls had equal return/call timestamps. They are invalid capacity evidence. The fix is one simulated nanosecond between calls.

## Measured heap, and source-backed interpretation

The main 30k job's `heap-done.pprof` samples **1,429,475,091 B in-use heap**, versus 2.998 GiB peak RSS. It is an allocation-site profile at the latest completed GC, not a precise census of final object reachability; no explicit GC was injected. Native `HeapAlloc` at the end was 1,519,230,336 B.

Disjoint prominent allocation paths in that end profile:

- `newCheckpoint → Disk.Durable`: **569,598,646 B**, 39.85%.
- simulated file syncs: **298,672,412 B**, 20.89%.
- `Disk.Files` crash images: about **141 MB**, 9.86%.
- canonical `kubePut` value allocations: **53,791,000 B** approximately, 3.76%.
- protobuf `mergeBytes` (encoded proposal/log bytes): about **47.8 MB**, 3.34%.
- checker `frometcd.putRequest` strings: about **59.6 MB**, 4.17%.
- cloned checker `[]history.KeyValue` state vectors: about **79.0 MB**, 5.53%.

The disk-copy paths dominate this profile; percentages/paths must not be added to their own cumulative parents. The live store contained only 43.55 MB of value bytes. Its three visible disks totaled 141.75 MB. Its two retained checkpoints contained 283,989,540 logical disk bytes; `Disk.Durable` creates *two* full cloned byte slices per file, explaining roughly 568 MB of checkpoint buffers by direct arithmetic and matching the profile.

Sources, read rather than experimentally modified:

- [`simfs.go:326`](https://github.com/djosh34/tigerstore/blob/e3a9516f78a50377fc28fe44ec7b088cd9066817/internal/simfs/simfs.go#L326): `Durable` clones durable bytes into both `data` and `durable`. `Copy` also clones current/durable/pending bytes; `Files` clones current file bytes for crash diagnostics. Ordinary simulated files retain page-cache and durable bytes too.
- [`faults.go:513–550`](https://github.com/djosh34/tigerstore/blob/e3a9516f78a50377fc28fe44ec7b088cd9066817/internal/sim/faults.go#L513): each checkpoint copies all three disks. Checks make another end checkpoint, then cold restarts clone the surviving disks again. Retained checkpoints live across the scenario.
- [`storage.go`](https://github.com/djosh34/tigerstore/blob/e3a9516f78a50377fc28fe44ec7b088cd9066817/internal/sim/storage.go): `s.log` retains encoded committed entry data independently of current server state.
- [`check.go:521`](https://github.com/djosh34/tigerstore/blob/e3a9516f78a50377fc28fe44ec7b088cd9066817/internal/sim/check.go#L521): the own checker replays every committed version without compaction. Its MVCC model retains version/index metadata. `mvcc.Heap.Place` retains existing value slices rather than copying them.
- [`values.go`](https://github.com/djosh34/tigerstore/blob/e3a9516f78a50377fc28fe44ec7b088cd9066817/internal/sim/values.go): requests, responses, watch events and decoded checker log share equal canonical byte values. Checker history shares equal strings separately. Thus it is wrong to charge a fresh full value copy for every checker/model/watch response. All generated values, including failed attempts, are retained.
- [`history.go:120–160`](https://github.com/djosh34/tigerstore/blob/e3a9516f78a50377fc28fe44ec7b088cd9066817/internal/sim/history.go#L120): committed/unique-operation lookups temporarily use full serialized-request strings as map keys, making additional payload-sized copies. `putValues` also constructs full value strings for log checking. Witnesses retain past versions and clone key vectors; these add metadata, allocations and CPU as object/revision counts grow.

The 1k × 200 case is particularly instructive: only **1,360,479 live value bytes**, but 308,935,332 bootstrap version-value bytes, **2,726,469,905 logical bytes in three retained checkpoints**, and roughly 1.012 GB of last-crash images. The checkpoint buffers alone occupy about **5.45 GB** before allocation slack. Current disks, old snapshots, raw log, canonical values, checker strings/models and history explain why a small live store can still need about 12 GiB RSS.

## Cheap changes: estimates, not measured patches

1. **Make retained checkpoint disks truly read-only with one buffer per file.** Only cold-restart working copies need independent mutable `data` and `durable` buffers. This preserves all checker assertions and snapshots. Expected retained-byte saving: **284 MB** at 30k, **2.73 GB (2.54 GiB)** in the 1k × 200 cell, before transient/slack effects. This is the most direct small patch suggested by the measurements. It does not justify aliasing mutable page cache and durable bytes in ordinary disks.
2. **Use copy-on-write immutable sealed-file sharing for snapshots/crash images/cold restarts.** Sharing across snapshots could save most of their payload duplicates: potentially another **2–3 GB** in the 200k-revision cells beyond one-buffer checkpoints, plus up to roughly **1 GB** of crash-image duplication. It needs correct COW for corruptions, truncations, syncs and torn writes. A 1.5–2× revision/data capacity gain is plausible, not demonstrated.
3. **For a compacted-state probe, compact, checkpoint and clean after the overwrite preload, before copying disks.** In 1k × 200, historical values occupy about 309 MB although latest live values are about 1.5 MB before churn. Eliminating those old on-server bytes before snapshotting could save several GB of disk copies. A rough **4–6 GB** saving is plausible for that cell. The own checker would still replay all old versions from its retained log. This changes the state being tested and must be a separate compacted profile, not a replacement for the un-compacted revision stress case.
4. **Use digests plus equality-on-collision for checker lookup keys instead of full serialized-request strings, and avoid full value strings in `putValues`.** At ~303–309 MB of historical payload, two serialized-key copies alone are of order **0.6 GB**, plus transient encoding buffers. The retained raw log/canonical values remain necessary. Estimated benefit is hundreds of MB, smaller than the multi-GB disk-copy lever. No digest-only weakening of value correctness is proposed.

The 256 KiB transport change was measured, not merely estimated: it removes an artificial cold-restart size barrier, but does not remove the RAM barrier. Port weak-reference reclamation, alternative model layouts, checkpoint COW, digest keys, pre-fault compaction, other seeds, replay-twice equality and the exact largest fitting sizes were **not measured**. The memory estimates above are source/profile arithmetic, not benchmark results.
