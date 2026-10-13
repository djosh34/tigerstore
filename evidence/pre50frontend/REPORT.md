# Main ServeHTTP baseline and leader CPU costs

The architect stopped the native variants before any measured Put window completed. This report follows the revised assignment: main as it is against etcd, with leader CPU profiles for work outside the frontend. No custom-frontend or native speedup is established. Nothing merged, and no PR opened.

## What ran

- Main server source `eb8a20eda7b1436e3319b924753469f2d8c0ee99`.
- Unprofiled harness `465ac357a968e3085401e678403bcd683b1f6b3d`, [run 38365055048](https://github.com/djosh34/tigerstore/actions/runs/38365055048). All three jobs passed, in 3m35s to 4m51s.
- Profile harness `8fc8ec20ff3c502af1e1578723bb2e152edc343a`, [run 38365649009](https://github.com/djosh34/tigerstore/actions/runs/38365649009). All three jobs passed, in 4m08s to 4m46s.
- Stock etcd v3.7.2, upstream `68c065e562994b89e333e77b039ad066f933c586`, built with Go 1.26.8. Main and generator use Go 1.27.2, grpc-go v1.84.0.
- The exact same observed main executable ran in all six jobs. SHA-256 `bddcfea3bc86d5ee4763c905bcba4bb2631a81df17d8b6316851152b9b9bf417`. Stock etcd SHA-256 `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d`.

The measurement runner archives the main SHA. It does not build the native code visible at the experiment branch's HEAD. Main has only observational file counters and debug endpoints added. `sources.json`, `versions.txt`, `instrumentation-sha256.txt` and the committed observation files identify the exact build inputs.

[All numbers](tables.md), [raw primary runs](primary), [raw profile runs](profile), [corrected outside-frontend attribution](outside-frontend.json), [method and scripts](../../experiments/pre50frontend/README.md).

## Matched configuration

Free `ubuntu-24.04-arm`, four Neoverse-N2 CPUs, 16.72 GB RAM, image `ubuntu24-arm64 20261004.142.1`, Linux `6.17.0-1022-azure`. Leader CPU0, followers CPU1/2, generator and controller CPU3. All servers have two Go processors, including etcd; generator has one. Thread affinity checks passed. Each client count has its own VM. Both stores run sequentially on that VM, five fresh clusters each, reversing order every second repetition.

Client and peer TLS stay enabled. The runner enables ext4 barriers and rejects disabled barriers. Etcd has its safe persistence defaults and an 8 GiB quota. The directories share one VM disk, not independent disk failure domains. Guest barriers cannot prove physical power-loss protection.

The generator is the direct generated protobuf KV client over grpc-go from `exp/pre-50-profile`, not the custom lean framer. The process and disk fences come from `exp/bench-68`. It runs leader-only closed-loop 1 KiB Puts over 16 connections and 4,096 keys, with no injected delay, compression, watches or compaction during measurement. Each unprofiled cluster gets 4,096 seed Puts, 20,000 warmup Puts and 60,000 measured Puts.

All 30 primary and 30 profile/control windows passed zero-error, exact revision/success, leader/term, process identity, replica completion and affinity checks. These are workload checks, not a new crash or durability campaign.

## Baseline result

At 16, 64 and 256 clients, main's median rates were 5,387.2, 8,959.7 and 9,433.2 Put/s. Etcd's were 3,501.9, 5,205.3 and 6,005.1. Main's median leader CPU was 167.167, 108.167 and 105.833 microseconds per Put. Median leader occupancy was 0.911, 0.965 and 0.998 CPU-seconds per second.

Main's five rates ranged from 5,240.4 to 5,579.6 at 16 clients, 8,578.0 to 8,986.3 at 64, and 9,390.2 to 9,608.7 at 256. No repetition was dropped. At 256, leader Puts/file-fsync fell to 9.176 from 16.004 at 64. This is an observation, not proof of why the batch shape changed.

The tables report every node's CPU, allocated objects/bytes and Puts/file-sync, plus generator CPU, throughput and mean/p50/p90/p99/p99.9 latency. They are medians of per-run statistics, not pooled percentiles. Independent column medians need not sum.

CPU comes from process user plus system ticks in the generator's window. Allocation and file counters use a slightly broader replica-fenced window. Main uses runtime.MemStats deltas; etcd uses its stock cumulative Go metrics. Fences and observation requests add fixed overhead, not subtracted. TLS connection establishment is outside the counter window. Extra fenced CPU columns remain in each CSV. Main counts file fsyncs, including segment headers; etcd counts its WAL sync histogram, predominantly fdatasync, with backend commits separately retained. Neither count includes directory syncs.

## Remaining CPU outside the frontend

Five main-leader CPU profiles per client count contain 249,964, 519,217 and 592,362 Puts. Each capture is 15 seconds around 12 seconds of arrivals plus drain. It includes about 0.500 seconds of idle lead and 2.481 to 2.502 seconds of tail with fence/debug work. There is no simultaneous block, mutex or allocation profiling. The same job runs stock etcd for five unprofiled 12-second controls. Those controls remain separate from headline performance.

At 256 clients, sampled leader CPU is 95.077 microseconds per Put. The partition finds 21.794 outside frontend stacks and 73.283 on frontend stacks. Thus 77.08% of these samples have HTTP/gRPC/frontend frames. The owner loop is 5.098, WAL 4.963, peers 3.545, outside-frontend allocation/GC 2.954, and outside-frontend scheduler paths 6.229 microseconds per Put. Within the owner, entry encode/decode is 2.296, Raft 1.080 and MVCC/index 0.827. Outside-frontend protobuf is 2.009. These inclusive costs overlap and must not be added.

At 16 clients, outside costs are larger: owner 15.202, WAL 21.283, peers 19.123 and scheduler 15.402 microseconds per Put. The tables retain all three concurrency levels.

The inherited attribution expression missed Go 1.27's `net/http/internal/http2` package, including its independent frame-writer goroutine. `outside_frontend.py` corrects that offline using the saved raw profile text. Use `outside-frontend.json` for HTTP/frontend attribution, not the original runner `attribution.json` HTTP rows. Other original rows, raw profiles, flat tops and cumulative tops remain unchanged. The exclusion covers net/http, its bundled HTTP/2, x/net HTTP/2, grpc-go and internal/server. Peer TLS stays outside.

Background GC and scheduling cannot be causally assigned to the requests that created their work. A custom frontend can change those costs, batching and downstream work. The 21.794 figure is a stack partition, not a hard CPU floor or a predicted custom-frontend throughput. CPU in File.Sync stacks is not blocked fsync wall time.

Profile results also show variability. The first 256-client main profile ran at 7,749.9 Put/s with p99 246.793 ms and p99.9 551.777 ms; it remains included. The profile cohort median was 10,391.4 Put/s. Profile jobs use different VMs and durations from the primary jobs, so their difference does not isolate profiler overhead. The operation-weighted process leader CPU is 206.590, 104.041 and 95.837 microseconds per Put across the 16/64/256 profiles, close to sampled totals of 198.029, 102.096 and 95.077.

## Compatibility failures retained

`etcdctl endpoint health` does not pass on pinned main. It exits 1 with `Unable to fetch the alarm list`. The read succeeds, but `/etcdserverpb.Maintenance/Alarm` returns Unimplemented. Etcdctl v3.7.2 explicitly asks for AlarmList after its health read. The separate `alarm list` command confirms that response. No prototype patch hides it. Main's TLS Put/Get/Watch and curl HTTP/1.1 and HTTP/2 `/health` and `/version` passed in all three baseline smoke clusters.

Before the scope changed, [run 38363285818](https://github.com/djosh34/tigerstore/actions/runs/38363285818) stopped at endpoint health. [Run 38363954643](https://github.com/djosh34/tigerstore/actions/runs/38363954643) recorded the same Alarm failure on main and native b, then found a native HTTP/2 curl `/version` failure while main passed. Go's HTTP/2 test and the host's older curl passed on that prototype. Its runner-specific interoperability cause was not isolated. Neither run reached a measured Put window. They are not performance data or passing compatibility evidence.

Abandoned native source b is `2d4052e96939b12fd68f0252dfd1154127cdf7c6`, tuned c is `78db0c56fe2b051b651bbfa3f1aed03ca3724b9c`. They stay on `exp/pre-50-frontend-native` and `exp/pre-50-frontend-tuned`. No recommendation to use them follows. Etcd v3.7.2 itself uses ServeHTTP for its secure shared client port; its cmux-native shared-port path is insecure. The original premise did not describe its TLS path.

## Guarantees and limits

Main's core, node, WAL and peer behavior is unchanged. Observation wraps file writes/syncs, calls the original operation once, preserves its result and counts afterward. There is no earlier acknowledgement, application, storage response or vote release, no bypass of a sync, and no change to async-Raft dependency order. Source comparison and the identical main binary establish what changed. The measurement does not independently re-prove the design's crash guarantees.

No open-loop, follower-ingress, delayed-network, mixed transaction, slow-client or Kubernetes performance claim is made. This work does not measure a custom frontend. Allocation counts do not establish that allocation removal is the next limiting optimization. Only the Opus worker writes the real change.

Both Sol helpers were deleted with their worktrees. Local work was limited to small correctness/build checks with `GOFLAGS=-p=2 GOMEMLIMIT=2GiB` and tests at `-parallel 2`. Native lint had six style/deprecation findings and is not claimed clean. No local benchmark ran.

## Reproduction

Before the deadline, only on a disposable ARM runner:

```sh
git checkout 465ac357a968e3085401e678403bcd683b1f6b3d
CLIENTS=16 MODE=primary timeout --kill-after=5s 800s bash experiments/pre50frontend/run.sh
# Separate jobs use CLIENTS=64 and CLIENTS=256.

git checkout 8fc8ec20ff3c502af1e1578723bb2e152edc343a
CLIENTS=16 MODE=profile SELECTED_VARIANT=a timeout --kill-after=5s 800s bash experiments/pre50frontend/run.sh
# Separate jobs use CLIENTS=64 and CLIENTS=256.
```

The script exports the Go memory/build limits. It requires `GITHUB_ACTIONS=true`, uses `RUNNER_TEMP`, verifies the filesystem, and rejects work after `2026-10-13T22:00:00Z`. Each cell's `commands.json` retains complete server and generator commands. The measured generator command is:

```sh
GOMAXPROCS=1 taskset -c 3 "$RUNNER_TEMP/pre50frontend/bin/load" \
  -endpoint 127.0.0.1:<leader-port> -certdir <cert-directory> \
  -pids <leader-pid>,<follower1-pid>,<follower2-pid> -hz 100 \
  -op put -total 60000 -workers <16-or-64-or-256> -conns 16 \
  -out <put.json> -barrier-dir <barrier-directory>
```

Profiles replace `-total 60000` with `-total 0 -duration 12s`. Offline analysis needs no new measurement:

```sh
python3 experiments/pre50frontend/summarize.py evidence/pre50frontend/primary/c16
python3 evidence/pre50frontend/outside_frontend.py > evidence/pre50frontend/outside-frontend.json
python3 evidence/pre50frontend/make_tables.py > evidence/pre50frontend/tables.md
```
