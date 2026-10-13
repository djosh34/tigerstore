# Pre-#50 measurements: lean follow-up and two cheap patches

Nothing merged. The current-main lean generator removes the observed generator CPU saturation. Neither cheap patch establishes a repeatable CPU or throughput win. Peer buffering reliably reduces write calls, but that is not the same as reducing CPU.

At 256 clients, the first current-main primary job measured 63,361 Put/s, 12.17 leader µs/Put and 5.50 generator µs/Put. Median occupied cores were 0.77 leader and 0.34 generator. Thus this is not evidence of a fully saturated leader CPU either. Disk waits, network waits and scheduling were not isolated.

The earlier grpc-go peer-profile run had a real measured regression: paired median throughput −8.99%, p99 +73.59%, and leader CPU +0.288 µs/Put. Its cause is unproven. It is retained below rather than discarded in favour of the initial positive screen.

## Sources and runs

The first pass selected the highest open PR, #80, at `396bced923ee47540e89e1e2ca1053ecde5673ce`. The architect then requested current main with the lean client. That follow-up pins `aa9d9b6eea79c73f12fe0153aefa809841967e88`: PRs 75–79 plus test-only PR85. Its production code is identical to #79's `549e0c9c9918a80a320eb886ab1b3480ae73fa69`. PR80 was still open at selection, and at the 13:07 UTC check on 2026-10-13; it is not part of this main cohort.

| Cohort | Harness commit | Actions run |
| --- | --- | --- |
| PR80 grpc-go baseline/profiles | `dd08f1ad3313356772075606c3da97f343a4dd74` | [38387195114](https://github.com/djosh34/tigerstore/actions/runs/38387195114) |
| PR80 grpc-go patch screen | `568da4d98fb77b06dbb5032d56c9fe65f055cbf9` | [38389143652](https://github.com/djosh34/tigerstore/actions/runs/38389143652) |
| PR80 grpc-go longer confirmation | `4abf26eb4a28e60b037d2d7aee6ea6badbb1ec3b` | [38390703857](https://github.com/djosh34/tigerstore/actions/runs/38390703857) |
| Main lean baseline/profiles | `c3cc3d91456269af520ee5cdbe9fa51199368a2a` | [38395499741](https://github.com/djosh34/tigerstore/actions/runs/38395499741) |
| Main lean patch screen/profiles | `b6d1492affff134b81917e99f33379b6e0002f73` | [38397622813](https://github.com/djosh34/tigerstore/actions/runs/38397622813) |

The lean source is adapted from `exp/pre-47-grpc`, `d2408301966b99471bfed06d4c2dc1cd1e5af462`. It retains reader/owner connections, pre-encoded requests, warmed HPACK and coalesced writes. The port adds barriers, exact totals, response validation, flow control and bounded I/O. It uses current dependencies. Each invocation has fresh connections, and batches HEADERS before DATA rather than interleaving them. It omits grpc-go's per-Put context and `grpc-timeout`. These are different client cohorts, not a controlled attribution of the entire improvement to generator CPU alone. See [client details](../../experiments/pre50next/leanload/README.md).

## Method

Five repetitions per runner cohort, with etcd v3.7.2 back-to-back on the same VM and reversed order on even repetitions. Each cell starts a fresh three-node cluster, checks TLS/health, seeds 4,096 keys, warms up with 8,000 Puts, and waits one second. Values are independently SHA-derived 1 KiB bytes. There are 16 connections and 64 or 256 closed-loop outstanding Puts. All calls target the leader.

Free ARM runner layout: leader CPU0, followers CPU1/2, generator CPU3. Every server has GOMAXPROCS=2 while pinned to one core; generator GOMAXPROCS=1. Ext4 barriers are explicitly enabled. No persistence bypass, peer plaintext, codec shortcut, open-loop arrivals, fake acknowledgements or protocol replacement is used. Three replicas share a VM and disk; guest barriers do not prove physical power-loss persistence.

Primary grpc-go screens use 30,000 measured Puts; main lean screens use 60,000. Longer confirmation and profile cohorts use a nominal 12-second arrival window through drain. Profiles cover 15 seconds, including about 0.5 seconds of lead and the remaining idle/drain tail. All three replicas are profiled, five times per selected source. Actual sampler timestamps in each pprof protobuf enclose the worker window.

CPU is `/proc/PID/stat` delta over workers through drain. The lean interval includes a final PING round trip; the per-Put latency samples do not. At 60,000 Puts, one 100 Hz tick is 0.167 µs/Put. Allocation and I/O deltas cover a wider, all-replica-fenced interval. Connection cleanup can enter those wider counters. PING catches already-observed duplicate replies; it is not an application durability fence or proof that no malformed frame could arrive later.

Results require no RPC errors, exact success counts, contiguous response revisions, exact before/after revision deltas, stable terms and converged replica fences. These checks do not independently read back every stored value or prove crash durability. Profiles are operation-weighted sample totals; throughput/CPU tables use independent medians. Inclusive profile mechanisms overlap and must not be added.

## Main with lean load

First primary-job medians:

| Clients | Store | Put/s | p99 ms | Leader µs/Put | Follower1 | Follower2 | Generator µs/Put |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | Tigerstore | 33,371 | 3.979 | 24.167 | 13.833 | 12.000 | 9.667 |
| 64 | etcd | 5,202 | 24.271 | 188.500 | 45.833 | 43.833 | 42.833 |
| 256 | Tigerstore | 63,361 | 7.488 | 12.167 | 5.333 | 4.833 | 5.500 |
| 256 | etcd | 6,222 | 72.866 | 158.000 | 29.167 | 28.000 | 37.500 |

Generator occupied cores were 0.317/0.343 for Tigerstore at 64/256, and 0.224/0.235 for etcd. Leader occupied cores were 0.796/0.771 for Tigerstore and 0.983/0.987 for etcd. No generator CPU saturation was observed. This does not establish an absolute server capacity limit.

Tigerstore leader allocation was 16.43/11.10 objects per Put at 64/256. Puts per file sync were 18.77/73.08; write calls per Put were 0.5256/0.2342. `/proc` write calls include file and socket calls, not just TLS writes.

### Remaining CPU costs

Selected disjoint execution contexts from the initial main profiles, µs/Put:

| Leader context | 64 clients | 256 clients |
| --- | ---: | ---: |
| Frontend reader | 4.824 | 3.296 |
| Frontend reply/writer | 3.962 | 1.578 |
| Peer send | 4.403 | 2.397 |
| Peer receive | 2.395 | 0.409 |
| WAL fsync | 4.233 | 0.733 |
| Other WAL | 1.248 | 0.547 |
| Apply | 1.872 | 1.066 |
| Raft | 0.882 | 0.417 |
| Entry encode | 0.285 | 0.213 |
| Sampled total, including other contexts | 28.480 | 12.350 |

At 256, frontend reader/writer is 4.87 µs/Put, peer send 2.40, total WAL 1.28 and apply 1.07. Raft plus entry encoding is only 0.63 µs/Put. This argues against replacing Raft or rewriting the entry encoder as the next cheap CPU experiment.

The separate, overlapping mechanism view has socket syscall samples 3.09, TLS excluding socket syscall samples 1.49, HTTP/2/HPACK excluding TLS descendants 0.63, and allocation/GC 1.71 µs/Put. Crypto implementation samples are 1.02, already included elsewhere. An inclusive HTTP/2 stack containing TLS reads is not framing-only work. Fsync CPU is not fsync wall time.

| Follower context at 256 | Follower1 µs/Put | Follower2 µs/Put |
| --- | ---: | ---: |
| Peer receive | 1.505 | 1.379 |
| Peer send | 0.749 | 0.584 |
| WAL fsync | 0.735 | 0.692 |
| Other WAL | 0.679 | 0.555 |
| Apply | 1.145 | 1.169 |
| Raft | 0.159 | 0.124 |
| Sampled total, including other contexts | 5.588 | 5.047 |

At 64, follower totals are 14.18/14.27 µs/Put; fsync alone is 4.00/4.13. Those profile jobs ran on different VMs from primary jobs. For example, the 64-client profile job's unprofiled Tigerstore control was only 24,653 Put/s, versus 33,371 in the primary job. Do not interpret that difference as profiling overhead or a source-code change.

## Two cheap patches

1. **Conditional credit wake.** Signal the frontend writer only when `creditLocked` actually appends a control frame. The connection lock still protects the decision; batched control completion still signals separately. Regression tests cover the threshold and existing-control cases.
2. **Peer ciphertext coalescing.** Add a bounded 256 KiB writer below outbound TLS, preserving Go's TLS records. Flush both plaintext and ciphertext layers before waiting. Handshake traffic remains unbuffered with its original deadline. Closing the raw connection interrupts a blocked flush. Authentication, encryption, wire encoding and Raft messages are unchanged.

Go already increases ordinary TLS records to 16 KiB after initial traffic. Disabling dynamic record sizing was therefore not treated as a promising steady-state experiment. No TLS record-size change was measured.

| Patch | PR80 source | Current-main source |
| --- | --- | --- |
| wake | `30544e072dafd562ca89a101f05a9d2c706aaa77` | `66625cd9502e64526f0cf3bbd339e5bde9c68564` |
| peer | `fe6fadb747743a77ebeafe575eb4a8877e356e56` | `8963bb646e98642772971e542f0156aa80e7403b` |
| both | `d937be176f8712ec9c3209d6850955c3786a50a5` | Not measured |

Current-main branches are `exp/pre-50-next-main-wake` and `exp/pre-50-next-main-peer`. These include the same test-only PR85 change as baseline. Parent patches before that test update are `50cbde1b678c32ea27839e6a38ad2d75529dd3c1` and `db77e544ad405439b7627ea9e19a04563a04e266`.

### Main lean screen

Paired changes against baseline in the same repetition. Brackets are minimum to maximum, not confidence intervals. Positive CPU savings mean less CPU.

| Clients | Patch | Put/s change % | Leader CPU saved µs/Put | Write calls saved % |
| --- | --- | ---: | ---: | ---: |
| 64 | wake | +1.08 [−15.98, +50.95] | +1.000 [−0.167, +1.667] | +1.01 [−1.94, +3.76] |
| 64 | peer | +2.71 [−13.59, +51.13] | +0.167 [−1.333, +0.667] | +19.79 [+16.90, +20.63] |
| 256 | wake | −0.04 [−3.79, +9.65] | 0.000 [−0.167, +0.167] | +0.23 [−0.50, +0.45] |
| 256 | peer | +0.41 [−3.43, +12.48] | +0.167 [−0.333, +0.333] | +52.77 [+52.23, +53.30] |

At 256, baseline/wake/peer medians were 64,562/64,885/66,155 Put/s and 13.667/13.667/13.333 leader µs/Put. Separate medians differ from the median paired change. Peer p99 was 8.180 ms versus baseline 7.715 ms; paired median p99 worsened 3.46%.

The 64-client job had large throughput/tail excursions, including baseline. These cells passed validity checks, but five short windows on this VM do not support a throughput claim.

The longer matched 256-client profile job did not rescue the CPU result. Peer paired throughput was +0.92% [−4.83, +5.96], leader CPU savings −0.175 µs/Put [−0.370, +0.532], and p99 reduction −4.15% [−28.54, +1.01]. Write calls fell 53.13% [53.01, 53.30]. Weighted sampled leader CPU was essentially unchanged, 12.047 to 12.022 µs/Put. Peer-send samples fell 2.334 to 2.006 and socket samples 2.947 to 2.720, but other work offset the sampled reduction.

**Established:** bounded buffering reduces write-call count substantially. **Not established:** a repeatable CPU, throughput or tail-latency benefit. Neither patch should be promoted as a performance win from this evidence. The next investigation should separate server off-CPU waits and byte-processing costs before assuming that fewer calls buy faster Puts.

## Earlier grpc-go cohort

These are retained as a separate historical cohort, not combined with lean results.

Initial pinned-PR80 baseline medians were 26,909/38,696 Put/s at 64/256 clients, leader CPU 32.333/22.667 µs/Put and p99 5.534/13.625 ms. Etcd was 5,137/5,619 Put/s. At 256, sampled leader CPU was 21.38 µs/Put: frontend reader/writer 8.76, peer send 3.21, WAL 2.63, Raft 0.76 and entry encoding 0.29.

The 30,000-Put screen initially favoured peer buffering at 256: 40,740 baseline versus 42,968 peer Put/s, about +5.5% paired throughput and 28.6% fewer write calls. Both patches together gave 42,160 Put/s. Wake alone was inconsistent and had a 55.869 ms p99 outlier.

The 12-second unprofiled confirmation weakened that result:

| Clients | Baseline Put/s | Peer Put/s | Paired throughput % | Paired CPU saved µs/Put | Write calls saved % |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64 | 24,826 | 24,795 | +0.98 [−2.03, +12.28] | +0.234 [−0.210, +0.491] | 10.22 [9.95, 10.87] |
| 256 | 41,260 | 42,381 | +2.38 [−2.70, +8.07] | +0.153 [−0.834, +0.482] | 28.20 [27.79, 28.96] |

In the matched 256-client profiles, baseline/peer medians were 46,881/43,001 Put/s and p99 11.899/20.293 ms. Peer throughput by repetition was 47,423; 45,406; 43,001; 40,617; 28,947, while baseline stayed near 47k. Paired CPU increased in all five repetitions. All windows remained valid. The cause was not diagnosed; this is not discarded as noise. The initial screen alone would have overstated the patch.

## Host comparison

Separate Apple ARM VM, existing `/tmp` XFS on `/dev/vda3`, no remount, other host work present. Leader CPUs0/1, followers2/3 and4/5, generator6; all GOMAXPROCS=2. One baseline/etcd window per client count. No local Go build: binaries were downloaded from the matching primary-c256 job and hashes verified.

The whole benchmark tree ran under a hard 1 GiB cgroup memory limit with swap disabled. The required GOFLAGS/GOMEMLIMIT remained set. The lean run's `memory.events` showed no max/oom/oom_kill events. Systemd printed an impossible 1.8 MiB peak despite a 75.8 MB final `memory.current`; that printed peak is not used. The kernel `memory.max` was verified inside the unit. See the run script/log and preflight under `audits/`.

| Client cohort | Clients | Store | Put/s | p99 ms | Leader µs/Put | Generator µs/Put |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Main lean, 60k Puts | 64 | Tigerstore | 19,103 | 8.942 | 19.667 | 7.500 |
| Main lean, 60k Puts | 64 | etcd | 9,690 | 20.499 | 118.500 | 31.667 |
| Main lean, 60k Puts | 256 | Tigerstore | 48,393 | 18.174 | 9.667 | 4.667 |
| Main lean, 60k Puts | 256 | etcd | 14,242 | 37.012 | 98.667 | 25.500 |
| PR80 grpc-go, 30k Puts | 64 | Tigerstore | 18,813 | 6.663 | 27.667 | See tables |
| PR80 grpc-go, 30k Puts | 64 | etcd | 9,616 | 16.083 | 122.333 | See tables |
| PR80 grpc-go, 30k Puts | 256 | Tigerstore | 45,614 | 11.718 | 17.667 | See tables |
| PR80 grpc-go, 30k Puts | 256 | etcd | 14,332 | 35.599 | 101.667 | See tables |

These are not isolated two-core scaling results and should not be pooled with runner numbers.

## Checks, evidence and reproduction

Focused local vet/lint/short tests passed under hard memory limits, with `GOFLAGS=-p=2 GOMEMLIMIT=2GiB` and `-parallel 2`. ARM race checks passed for both main patches over peer/server/node/core/WAL/lint, and for the lean client. The earlier combined patch also passed ARM race checks. Tests cover credit wake thresholds, record-buffer flush/close/stall handling, TLS authentication, and existing node/quorum/WAL behaviour. This is not a full chaos, power-cut, rolling-upgrade or compatibility acceptance run. No core, WAL format, quorum or acknowledgement ordering was changed by the two patches.

[Archive validation](validation.json) rechecks 250 runner windows, eight host windows and 120 CPU profiles: exact revision accounting, no errors, stable terms/leadership, process CPU arithmetic, all-thread affinity, complete schedules and actual sampler coverage. It also checks that all recorded Actions jobs succeeded.

[Generated tables](tables.md) contain every cohort, all three replicas' CPU/allocation/I/O/retained figures, paired ranges and complete profile partitions. `initial/`, `screen/`, `confirm/`, `lean-initial/` and `lean-screen/` contain source/run/job metadata, raw evidence archives and SHA256 manifests. `host/` and `host-lean/` contain the separate host observations. Each raw artifact includes argv, affinities, fences, binary hashes and observation source overlays. Matching profile executable files are retained as `*-symbols.gz`. Duplicate executable bundles are omitted from most archives; primary-c256 bundles retained in initial and lean stages permit host reproduction. Earlier confirm/screen bundles were omitted.

On a disposable free ARM runner, check out the exact harness commit from the table above. Before the campaign deadline, representative commands are:

```sh
GENERATOR=lean CLIENTS=64 MODE=primary TOTAL_PUTS=60000 \
  timeout --kill-after=5s 780s bash experiments/pre50next/run.sh
GENERATOR=lean CLIENTS=256 MODE=profile PROFILE_VARIANT=peer \
  timeout --kill-after=5s 780s bash experiments/pre50next/run.sh
```

Use `GENERATOR=grpc` and omit `TOTAL_PUTS` for the older default 30k screen; its older harness predates the generator selector. The old longer confirmation uses `WINDOW_SECONDS=12`. Exact workflow commands, commits and refs for each run are archived. Do not run the remounting wrapper on a non-disposable host.

The lean host used controller commit `b6d1492affff134b81917e99f33379b6e0002f73`, binaries from `c3cc3d91456269af520ee5cdbe9fa51199368a2a`, and:

```sh
systemd-run --user --wait --pipe --unit=pre50next-host-lean \
  -p MemoryMax=1073741824 -p MemorySwapMax=0 -p RuntimeMaxSec=240 \
  --working-directory="$ROOT/repo" /usr/bin/env "PATH=$PATH" \
  /usr/bin/bash "$ROOT/notes/host-lean-run.sh"
```

The archived [script](audits/host-lean-run.sh) contains exact environment, paths, deadline checks and both client counts. It invokes `run.py` directly, not `run.sh`.

Offline after extracting an archive:

```sh
python3 experiments/pre50next/summarize.py ROOT
python3 experiments/pre50next/profile_manifest.py ROOT
python3 experiments/next50/check_profiles.py ROOT
python3 experiments/next50/attribute.py ROOT/profile-manifest.json
python3 experiments/next50/compare.py ROOT                    # primary patch roots
python3 experiments/next50/compare.py ROOT --baseline baseline-profile
python3 experiments/next50/tables.py evidence/pre50next > evidence/pre50next/tables.md
python3 experiments/next50/verify.py evidence/pre50next
PYTHONDONTWRITEBYTECODE=1 python3 experiments/pre50next/test_harness.py
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s experiments/next50
```

Profile/paired commands apply only to roots containing those cohorts. The campaign deadline is `2026-10-13T22:00:00Z`; do not restart runner or host work after it. All results are throwaway pre-work for #50, not production performance promises.
