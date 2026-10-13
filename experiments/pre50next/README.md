# PR80 baseline and profiles

Throwaway experiment only. Nothing here is intended to merge. Parent owns patch selection, attribution and the final report.

`refs.json` pins PR80 at `396bced923ee47540e89e1e2ca1053ecde5673ce`. It also records bench68 `32ceb80`, frontend `b66573f`, wholebatch `c7c39ac` and profile `1d90dce` provenance. The generator is copied unchanged from frontend `b66573f`, including its controller barriers. Replica application and etcd post-WAL.Save fences come from bench68/frontend. Warmup size, measured cohort and retained observations follow the latest wholebatch experiment.

## Runner launch after review

Push the reviewed commit to `exp/pre-50-next` or `exp/pre-50-next-harness`. The push-only workflow creates four free `ubuntu-24.04-arm` jobs, primary/profile at 64/256 clients. Each has a 14-minute limit and a 780-second script timeout. It checks the absolute deadline before starting commands and HTTP requests. Clusters need at least 90 seconds left before the deadline. No manual dispatch, tags, PRs or merge steps.

Primary jobs alternate baseline/stock etcd v3.7.2 order over five repetitions. Fresh cluster per window, leader CPU0, followers CPU1/2, generator CPU3. Servers have GOMAXPROCS=2; generator has one. Both stores use client and peer certificate-authenticated TLS, safe persistence, ext4 barriers and the same data mount. Etcd has an 8 GiB quota. Each cluster gets 4,096 seed Puts, 8,000 warmup Puts, then 30,000 measured Puts. Workload is leader-only closed-loop Put, 4,096 Kubernetes-shaped keys, independent deterministic 1 KiB values, 16 grpc-go connections. No live HTTP health traffic, delay, compression, watches or compaction in the measured workload. HTTP/1.1 and HTTP/2 health/version checks run before seed/warmup.

Profile jobs run five repetitions of baseline control, etcd control, and baseline profile on the same VM, reversing the order every second repetition. All use separate 12-second arrival workloads through drain. Each profiled baseline captures the leader and both followers concurrently for 15 seconds. No stock-etcd CPU profiles or candidate profiles in this first round. No mutex, block or allocation profiler runs alongside CPU capture. Normal runtime allocation sampling remains unchanged. Debug HTTP has a dedicated ServeMux and listener, so the new custom client frontend does not route profile requests.

The profile denominator is completed Puts. The 15-second capture includes about 0.5 seconds of lead, drain, debug/fence requests and idle tail around the 12-second arrival window. `profile-window.json` records each node's HTTP bounds, the load bounds and operation count. Reject capture if drain exceeds a profile's response bound. These HTTP bounds approximate sampler bounds, not exact profiler start times. Before attribution, `experiments/next50/check_profiles.py` verifies that the actual pprof sampler timestamps enclose the generator window.

## Artifacts

Each artifact is named `pre50next-{primary|profile}-c{64|256}-<run-id>` and contains:

- `binaries.tar.gz`, reusable ARM binaries plus a SHA-256 manifest and matching symbol binaries.
- Full instrumented source archives, source refs, overlay templates/diff, versions, binary hashes, mount evidence and exact build/download argv.
- `commands.json`, exact server/generator argv and relevant process environment, plus debug HTTP requests.
- Per-window Put/warmup/seed metrics, status/term/affinity checks, replica fences, allocation/sync snapshots, `/proc/PID/io` and RSS observations.
- Allocation objects/bytes, GC CPU for Tigerstore, and retained heap/RSS after eight quiet seconds. Tigerstore retained heap forces GC; stock etcd retained heap does not, so they are not equal-GC comparisons.

Profiles use `profile/c64/1-baseline-profile/n<node>-cpu.pb.gz`, with `n<node>-cpu-raw.txt`, `n<node>-cpu-top-flat.txt` and `n<node>-cpu-top-cumulative.txt`. Node IDs are zero-based. `placement.json` maps them to leader/follower roles. `profile-manifest.json` is a JSON list with clients, variant, rep, role, operations and raw path relative to that artifact root, for the parent's independent `experiments/next50/attribute.py`.

CPU uses the generator's worker interval through drain. Allocation, disk and process-I/O snapshots bracket the wider all-replica-fenced interval, so they include fixed observation overhead and trailing follower work. Tigerstore counts completed file Write/Sync calls and excludes directory syncs. Etcd counts WAL sync observations, mostly fdatasync, including rotation-related directory syncs. It archives backend commits separately. `/proc/PID/io` syscr/syscw are syscall counts, not proof of physical I/O. Replicas share one disk and failure domain. Guest barriers do not prove physical power-loss protection.

Offline summaries need only Python:

```sh
python3 experiments/pre50next/summarize.py <artifact-root>
python3 experiments/pre50next/profile_manifest.py <artifact-root>
```

## Host launch, parent gated

Do not use `run.sh` on the host. Download a primary runner artifact, extract `binaries.tar.gz` into a dedicated directory, and keep `binaries.json` beside the executables. No local Go command is needed. The parent first gates host memory and removes other heavy work. GOMEMLIMIT is a per-process soft limit, not an aggregate 2 GiB host limit.

From this checkout, before the deadline:

```sh
mkdir -p evidence/pre50next/host-bin
# Download/extract the reviewed primary artifact first.
tar xzf <artifact-root>/binaries.tar.gz -C evidence/pre50next/host-bin
HOST=1 CLIENTS=64 MODE=primary \
  BIN_DIR="$PWD/evidence/pre50next/host-bin" \
  python3 experiments/pre50next/run.py
HOST=1 CLIENTS=256 MODE=primary \
  BIN_DIR="$PWD/evidence/pre50next/host-bin" \
  python3 experiments/pre50next/run.py
python3 experiments/pre50next/summarize.py evidence/pre50next/host/c64
python3 experiments/pre50next/summarize.py evidence/pre50next/host/c256
```

HOST verifies every executable checksum and the pinned base SHA, runs only one baseline/etcd repetition per client count, and refuses profiles. Leader gets CPUs0/1, followers2/3 and4/5, generator CPU6. All processes receive GOFLAGS=-p=2, GOMEMLIMIT=2GiB, GOMAXPROCS=2. It does not remount or build. It records the existing mount, generates fresh dev certificates and deletes only its fresh cluster data. Results are labelled host results and cannot be substituted for runner results. Fresh output directories are required for reruns.

## Later patch comparisons

Leave `base` pinned unless the parent explicitly changes it. Add up to three candidate entries such as `{"name":"peer-buffer","ref":"<40-character-commit-sha>"}` to `refs.json` and push the experiment branch. Primary jobs build each immutable source with the same observation-only overlay and run it sequentially with baseline/etcd for five repetitions. Profile jobs remain baseline-only. No experimental selector environment is inherited by servers. Later matrix changes belong to the parent.

The overlay inserts one debug startup call and file-call counters only. Core, Raft, WAL ordering, transport and acknowledgement behavior remain unchanged. No new crash-durability proof or Kubernetes compatibility claim follows from these workload checks.
