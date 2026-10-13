# Next speed experiments

Throwaway code only. Nothing here is intended to merge. The source checkout contains the experiment tools; `refs.json`, not that checkout's engine code, selects the server sources built from `git archive`.

The architect requested a lean-generator follow-up after the first measurements. The current baseline is main `aa9d9b6eea79c73f12fe0153aefa809841967e88`, including PRs 75 to 79 and the test-only PR 85. PR 80 was still open when this baseline was selected at 12:10 UTC. The earlier cohort pinned its then-open head `396bced923ee47540e89e1e2ca1053ecde5673ce`.

## Cohorts

| Harness commit | Generator and purpose |
| --- | --- |
| `dd08f1ad3313356772075606c3da97f343a4dd74` | grpc-go baseline and three-node profiles of pinned PR80. |
| `568da4d98fb77b06dbb5032d56c9fe65f055cbf9` | grpc-go screen of conditional writer wakes, peer ciphertext buffering and their combination. |
| `4abf26eb4a28e60b037d2d7aee6ea6badbb1ec3b` | Longer grpc-go peer-buffer comparisons and matched profiles. |
| `c3cc3d9` | Current-main baseline with the lean generator. Later commits select the patched sources in `refs.json`. |

The grpc-go client is copied unchanged from frontend `b66573f`, including its controller barriers. The lean client adapts `experiments/grpc/lean.go` from `exp/pre-47-grpc` at `d2408301966b99471bfed06d4c2dc1cd1e5af462`. Its README lists changes and limits. It retains the reader/owner model, warmed HPACK blocks and pre-encoded requests. It adds the existing controller barriers, exact finite totals, strict response/revision checks and bounded I/O. It uses this repository's dependencies, including x/net v0.58.0 rather than the source experiment's v0.61.0.

The lean client sends no per-Put `grpc-timeout` metadata. The grpc-go client creates a timeout context for every Put. Arrival batching, headers, client implementation and source commit differ between cohorts. Do not attribute their difference solely to freeing generator CPU.

## Workload and placement

Free `ubuntu-24.04-arm` only for `run.sh`. Each job verifies ext4 barriers after remounting the disposable runner. Leader CPU0, followers CPU1/2, generator and controller CPU3. Each server has GOMAXPROCS=2, including etcd. The generator has one. Client and peer certificate-authenticated TLS remain enabled. Etcd v3.7.2 uses its safe persistence defaults and an 8 GiB quota.

Each fresh cluster receives 4,096 seed Puts and 8,000 warmup Puts, then a one-second pause. Primary lean measurements use 60,000 Puts. Earlier grpc-go screens used 30,000. `TOTAL_PUTS` selects either count. `WINDOW_SECONDS=12` selects a duration cohort instead. Every job runs five repetitions with the stores sequentially on the same VM and reverses the order on even repetitions.

Workload is leader-only, closed-loop Put over 4,096 Kubernetes-shaped keys and independently SHA-derived 1 KiB values. There are 16 connections and 64 or 256 outstanding calls in total. No live health traffic, injected delay, compression, watches or compaction runs during the measured window. HTTP/1.1 and HTTP/2 health/version smoke checks precede seed and warmup.

The generator parks before and after the worker interval. The controller fences all replicas before reading allocation, disk and I/O counters. Etcd additionally waits for the post-WAL.Save committed gauge before taking a fresh scrape. CPU uses the narrower worker interval through drain. Generator CPU is always reported separately.

## Profiles

Default profile jobs alternate baseline-control, etcd-control and baseline-profile for five repetitions. Each uses a 12-second arrival window through drain. Profiled baseline clusters capture the leader and both followers for 15 seconds, with about half a second of lead and the remaining time for drain and idle tail.

`PROFILE_VARIANT=peer` instead selects baseline-profile, etcd-control and peer-profile in each repetition. Both Tigerstore sources get the same capture. Raw/top pprof output uses the matching executable for each profile. There is no mutex, block or allocation profiler alongside CPU capture. Ordinary runtime heap sampling remains enabled.

`profile-window.json` records HTTP bounds. `experiments/next50/check_profiles.py` checks the actual timestamps in each pprof protobuf against the generator's start and elapsed time. A profile must enclose the entire worker interval. Attribution divides sampled CPU nanoseconds by completed Puts. Inclusive categories overlap; the separate ordered execution-context partition sums to total CPU. Profiles contain fixed observation overhead and idle margins.

## Commands

Only on a disposable ARM runner before `2026-10-13T22:00:00Z`:

```sh
GENERATOR=lean CLIENTS=64 MODE=primary TOTAL_PUTS=60000 \
  timeout --kill-after=5s 780s bash experiments/pre50next/run.sh
GENERATOR=lean CLIENTS=256 MODE=profile \
  timeout --kill-after=5s 780s bash experiments/pre50next/run.sh
```

The script exports `GOFLAGS=-p=2 GOMEMLIMIT=2GiB`. Tests use `-parallel 2`. Workflow jobs reserve their full time limit before the absolute deadline. Commands and HTTP requests also check the deadline. The workflow only runs on `exp/pre-50-next`, with no tags, PRs or merge steps.

Offline, for an extracted artifact root:

```sh
python3 experiments/pre50next/summarize.py ROOT
python3 experiments/pre50next/profile_manifest.py ROOT
python3 experiments/next50/check_profiles.py ROOT > ROOT/sampler-bounds.json
python3 experiments/next50/attribute.py ROOT/profile-manifest.json > ROOT/attribution.json
python3 experiments/next50/compare.py ROOT > ROOT/paired-changes.json
```

The last three commands apply only to their relevant profile or candidate-comparison roots. Summaries reject incomplete scheduled cohorts. Paired changes compare the same repetition on the same VM, not separate runner medians.

## Host runs

`HOST=1` calls `run.py` directly with downloaded ARM binaries. It never builds or remounts. The parent checks memory first and puts the entire benchmark process tree under a 1 GiB `memory.max` limit with swap disabled, leaving room for the agent within the task's 2 GiB budget. GOFLAGS and GOMEMLIMIT still have the required values. All local tests use `-parallel 2` and separate hard memory limits.

Leader gets CPUs0/1, followers2/3 and4/5, generator CPU6. All processes receive GOMAXPROCS=2. One baseline/etcd repetition runs at each client count. `BIN_DIR` contains the downloaded binaries and `binaries.json`; hashes and baseline source must match. `WORK_DIR` can select an owned data directory on the host's existing XFS mount. Certificates and cluster data are deleted afterward. Results are labelled host results, not runner results or an isolated scaling test.

## Evidence and limits

Runner artifacts contain setup commands, server/client argv, source refs, complete observation overlays, instrumented source archives, binary hashes, raw JSON/Prometheus counters, affinities, replica fences, logs and profiles. All selected executable hashes are checked before use. Public evidence archives may omit redundant primary executable bundles. Matching profile executables and the host's reusable binary bundle remain archived.

Allocation, disk and `/proc/PID/io` snapshots span a wider interval than CPU. `syscr/syscw` include observation traffic and background work. They do not include fsync or represent physical I/O. Tigerstore counters count completed file Write/Sync calls and exclude directory syncs. Etcd counts WAL histogram observations, including rotation-related directory syncs, and records backend commits separately. Forced-GC retained heap is available only for Tigerstore. Retained observations run after eight quiet seconds, outside the measured and profiled intervals.

The observation overlay adds a private debug listener and counting wrappers only. Each wrapper calls the original file operation once and preserves its result. All sources retain real persistence, peer TLS, ordered Raft storage responses and committed-apply acknowledgements. Three runner replicas share one disk and failure domain. Guest barriers do not prove physical power-loss protection. These Put checks do not prove crash durability, capacity, Kubernetes compatibility, open-loop latency or production performance.
