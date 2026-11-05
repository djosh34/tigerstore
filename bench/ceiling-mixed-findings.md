# Research #484: mixed-k8s

Branch: `exp/484-ceiling-mixed`. Server under test: **bd1ade319a4d0bfc92082a8bf8d335aa01199428**, built separately from the research generator. No production server code changed.

## Runs and provenance

- https://github.com/djosh34/tigerstore/actions/runs/42019370875 — success, measurement commit `37276e37e98d01e2f97114884495e222b95f9b1c`; two ARM jobs, each 3 fresh-cluster repetitions of RTT/streams = 0/48, 2/48, 10/48, 10/96. Each repetition preloads 1M objects, warms for 3 s and measures 10 s. Leader/follower/generator CPU profiles on repetition 1 at 0 and 10 ms, with 48 streams/connection.
- https://github.com/djosh34/tigerstore/actions/runs/42019785137 — success, `956b3fa54c7cccbb69d09cfa5d5f90d76683e49d`; two ARM jobs, three repeats of raw primitives and production reply/watch encoders. No cluster results in this run: `primitives_only=true`.
- https://github.com/djosh34/tigerstore/actions/runs/42023145903 — success, `335807a51500f4bfc4334152a3ab288fde29a902`; two NEW ARM jobs, one repetition per RTT 0/2/10, 48 streams, per-node client-wire counters and process/device IO, plus profiles at 0/10. No new primitive benchmarks.
- https://github.com/djosh34/tigerstore/actions/runs/42019357686 — cancelled push-trigger registration run, not measurement evidence.

Earlier analysis-only commit: `fd44dd36fc17f17657ca8f06322d9d4ccd28c287`. Its watch-category regex had an unbalanced parenthesis; fixed before relying on the analysis. The resumed experiment also selects RETURN counters explicitly, so diagnostic no-target per-port counters cannot be mistaken for aggregate counters.

The logs, result JSON, summary.jsonl and diskbench.jsonl (the exact data printed into job summaries), profiles, placement, and qdisc probes were checked. All 30 cluster results are valid, with no request/validation errors and successful post-run leader/term checks. Detail operation counts equal result operation counts. Summary rows agree with raw results. Artifacts downloaded were only ~3 MB in total and can be deleted after producing the tables; remote raw artifacts remain in the runs.

**Hardware caveat:** these jobs actually report Linux **7.0.0-1017-azure**, not the 6.17 kernel of the older benchmark in the brief. CPU is still 4 x Neoverse-N2, memory ~16.7 GB decimal, Go 1.27.2; client and peer mutual TLS, generator CPU 3, leader CPU 0, followers CPUs 1/2, GOMEMLIMIT=3GiB per server, ext4 /dev/sda1 with barrier=1 remount. Do not call these a controlled same-kernel comparison to the earlier 9,880/9,488/7,896 ops/s results. Runner spread is substantial: new diagnostic 0-ms results were 9,471.9 and 10,643.2 ops/s, versus 7,870.4–8,799.5 in the earlier two jobs.

## Complete tables and reproduction

`bench/ceiling-mixed-tables.md` contains every principal measurement, all RTTs and stream settings, runner-to-runner throughput spreads, per-operation request/reply sizes and latency, per-node cores/CPU/IO, all exclusive profile categories, every microbenchmark, diskbench, and the exact dump statistics by resource. Numbers are min / median / max, not pooled percentiles. Microbenchmarks have three observations PER runner; normal cluster configurations have three per runner; diagnostic configurations have one per runner.

```sh
# Download each run into its own new empty directory; retain subdirectories
# ceiling-mixed-1 and ceiling-mixed-2.
gh run download 42019370875 -R djosh34/tigerstore -D /tmp/mixed-old
gh run download 42019785137 -R djosh34/tigerstore -D /tmp/mixed-primitives
gh run download 42023145903 -R djosh34/tigerstore -D /tmp/mixed-diagnostics
python3 bench/ceiling-mixed-analyze.py /tmp/mixed-old
python3 bench/ceiling-mixed-analyze.py /tmp/mixed-diagnostics
python3 bench/ceiling-mixed-report.py /tmp/mixed-old /tmp/mixed-diagnostics /tmp/mixed-primitives
```

Exact cluster command and counter/profiler sampling are `bench/ceiling-mixed.sh:23-65`; setup/build/remount/matrix are `.github/workflows/ceiling-mixed.yml:16-73`. In particular the load command is:

```sh
R484_DIR="$run" R484_PROFILE="$profile" taskset -c 3 bin/loadgen-research \
 -endpoints "$(<"$dir/endpoints")" -cacert certs/ca.pem \
 -cert certs/client.pem -key certs/client-key.pem \
 -pids "$(<"$dir/pids")" -disk sda1 \
 -workload mixed -keys 1000000 -conns 6 -streams "$streams" \
 -warmup 3s -duration 10s -seed "$rep" -out "$run/result.json"
```

Primitive commands:

```sh
taskset -c 3 bin/loadgen.test -test.run '^$' -test.bench '^BenchmarkResearch484$' -test.benchtime 1s -test.count 3 -test.cpu 1
taskset -c 3 bin/server.test -test.run '^$' -test.bench '^BenchmarkResearch484Encoding$' -test.benchtime 1s -test.count 3 -test.cpu 1
taskset -c 3 bin/diskbench -dir "$RUNNER_TEMP"
```

CPU profile reads: TLS-authenticated `/debug/pprof/profile?seconds=9` on the leader and first nonleader; generator `runtime/pprof.StartCPUProfile` during the 10 s window. Categories are exclusive, first-match attribution over complete protobuf profile stacks, NOT sums of overlapping pprof cumulative rows. Server category shares from the 9 s sample are scaled by the measured full-window CPU/op. Background GC is separate; GC assists remain with the calling operation. TLS includes record processing and IO, not just AES.

CPU is process user+system CPU; server CPU/op is the **sum of all three nodes**, not leader CPU/op (`cmd/loadgen/main.go:625-648`, `cmd/loadgen/cpu.go:13-49`). The device write counter includes filesystem activity and other writers; /proc/pid/io write_bytes is separate charged storage IO. wchar is not WAL bytes: it also counts sockets. IP counters count loopback OUTPUT bytes once, including IP/TCP headers and ACKs, not physical Ethernet bytes. External counters poll markers every 50 ms, so they have small boundary skew relative to the internal CPU/device sample.

## Exact workload

Research source references below use `335807a5`; production core/server code is unchanged from `bd1ade31`.

- Mix get/list/update/create/delete = 25/10/40/15/10, alongside watches (`cmd/loadgen/mixed.go:20-29`). Operation selection is random; realized measured proportions vary slightly.
- Six unary TLS/HTTP2 connections, two per endpoint, each with 48 slots. Connections stay on their selected endpoint; this is not leader-only routing (`cmd/loadgen/mixed.go:847-857`). At max there is no offered-rate throttle. The worker can fill up to the configured stream/window limits (`cmd/loadgen/worker.go:52-105`).
- One million keys split by connection ownership. Each owner has floor(1024/6)=170 namespaces/name groups PER resource: 1,020 groups per resource across owners, 27,540 total groups (`cmd/loadgen/mixed.go:119-166`). Names are `/registry/<resource>/bench-<owner>-<namespace>/object-<id>`; cluster-scoped groups use a hyphen rather than slash before the object part (`:99-116`).
- Histogram: 6,345 objects, 9,253,583 value bytes, exact mean **1,458.405516 B**, median **750 B**, nearest-rank p99 **10,834 B**, min **63 B**, max **33,365 B** (`internal/k8ssize/k8ssize.go:15-21,442-470`). A key's resource is drawn by object count. Each new value is independently drawn conditional on that resource (`mixed.go:141-166`). Contents are windows of a seeded random pool, NOT actual Kubernetes protobuf/JSON objects (`cmd/loadgen/keys.go:55-91`).
- Preload is Txns of <=128 puts and <=60 KiB, constrained by the peer's initial stream window; <=8 preload streams per connection; offered 300 Txns/s aggregate (`mixed.go:512-557,859`, `cmd/loadgen/main.go:60`). About **1.458 GB values per replica** in expectation, before keys/index/log overhead; preload is outside the measured window.
- GET is a single-key Range. About 10% of GETs target a known deleted key once absent keys exist, so not all GET replies have one KV (`mixed.go:242-246,275-280`).
- LIST is a linearizable first-page Range with limit 500 over ONE resource/owner/namespace prefix, not all one million keys and not necessarily 500 returned KVs. It does not page further. The group is held still for exact validation, with no active operation on that group while LIST begins (`mixed.go:195-208,262-305,398-414`). Measured mean returned keys ~307–309 at 48 streams, mean body ~290–291 kB. Events are ~61.4% of initial keys, so their ~602-key groups usually hit the limit; most other resource groups do not. Increasing stream count slightly biases which groups are available for LIST, so the 96-stream control has ~294 returned KVs and a ~281 kB mean page.
- GET and LIST omit Serializable, so both use ReadIndex; no MVCC revision is requested. A follower obtains the read index via the leader, then serves from its own local applied state (`mixed.go:275-280`, `internal/core/node.go:1198-1203,1677-1686,1980-2050`).
- Update/create/delete are each a Txn with one Compare and one successful mutation, no failure branch. Update/delete compare ModRevision; create compares CreateRevision=0. Writes entering followers are forwarded as Raft proposals; all three replicas persist/apply them (`mixed.go:169-193`; `internal/core/node.go:1655-1675`; `internal/core/codec.go:62-132`; `internal/wal/wal.go:943-978`).
- There are **27 watches**, one per resource, EACH on a separate connection, round-robin across the three endpoints. PrevKv and progress are requested; watch start is revision after preload; load begins only after all watch-created responses (`mixed.go:880-929`, `cmd/loadgen/wire.go:269-285`). One matching watch per mutation, NOT 27 or 3 events per write. Update event carries new+previous KV, create new only, delete tombstone+previous KV. All replicas apply the write, but only the replica hosting that resource watch sends its event.
- Watch endpoint load is intentionally uneven by resource: endpoint nodes 1/2/3 own **5.122% / 19.905% / 74.972%** of expected event counts. Their value-byte contributions per global resource draw are **35.388 / 547.920 / 875.097 B**. Leadership is elected, not tied to this resource order.
- Typical synthetic single-event WatchResponse protobuf sizes with 1,458 B value and an events-shaped key are **3,072 B update / 1,546 B create / 1,608 B delete**, measured by the encoder benchmark. Actual workload sizes differ with resource/key/header and event batching: observed watch traffic is ~1,615 B per global op, ~0.648 event/op, hence ~2,493 B/event. Per-type exact workload watch byte means were not separately instrumented; these per-type numbers are a synthetic model, not an exact measurement of actual events.
- Logical request-body replication floor: roughly `3*(.4*1595 + .15*1559 + .1*120.5) = 2,652 B/op`, excluding origin/boot/request IDs, batch/Raft/WAL headers, hard state and filesystem/journal write amplification. Reads have zero WAL writes, but can miss the value cache. Peer request-body model is `(2 + 2/3)` copies of the write request, ~2,357 B/op, before message/framing/header costs. Actual physical disk writes and peer IP traffic are measured, not derived from that model.

## What limits the present implementation

1. **LIST/read work and GC dominate, not Raft or watch fan-out.** At 0 ms, leader read/index/checksum/value-cache work is 26.0–29.5% (~29.5–32.2 us/global op), encoding 10.7–13.3% (~11.7–15.0 us/op), background GC 28.4–28.6% (~31.2–32.1 us/op). Together ~65–72%. TLS+IO is 12.4–12.7%; Raft ~1.1%; watch fan-out/encode ~0.5–0.6%. New diagnostic profiles confirm the same ordering. `RangeKVs` includes cached value lookups and CRC checking, not only tree traversal (`internal/mvcc/store.go:213-240`, `internal/core/cache.go:64-82`). The 0-ms leader's CRC32 flat samples are 0.44 s / 8.19 s; 0.42 s of those were in range-value verification.
2. **The generator is not saturated at today's rates.** Original runs used ~0.58 core at 0 ms, ~0.51 at 10 ms; diagnostics ~0.62–0.68 and ~0.51–0.52. About 44.7–48.2% of generator samples are GET/LIST validation, ~27–33% TLS+IO, not simply issuing writes. Full value validation, field scanning and LIST validation matter (`mixed.go:312-370,372-414`); watches canonicalize and hash events (`:733-812`).
3. **But this generator cannot be treated as an unlimited source.** `1e6 / measured client_cpu_us_per_op` gives 13.49–15.00k ops/s at 0 ms, 12.79–14.64k at 2 ms, 13.20–14.20k at 10 ms in the original jobs; the 576-slot run projects 14.90–15.80k. New 0-ms runners project 15.25–15.63k. These are constant-cost ONE-CORE PROJECTIONS, not a measured immediate-response fake-server maximum. The brief explicitly permits this method. A claimed 30k or 100k mixed improvement cannot be validated by this fixed one-core generator without changing it; a hard universal 15k cap is NOT established because per-op cost varies with concurrency and page sizes.
4. **288 in flight is already materially restrictive at 10 ms.** Occupancy is ~282/288 at 10 ms. Increasing to 576 raises each runner's median throughput by **20.96% and 21.70%** (7,388 -> 8,937 and 6,793 -> 8,268 ops/s); leader utilization rises from ~0.79 to ~0.88. It is not a pure 288/RTT wall: observed mean request latency is ~40 ms, not 10 ms, and pages become ~3% smaller on the larger-window control. Under an idealized balanced endpoint routing model, one leader RTT plus two follower RTTs gives mean `5*RTT/3`, so the latency-only bounds are **86,400 ops/s at 2 ms** and **17,280 at 10 ms**, before CPU/storage/queueing. That is a MODEL, not a measured no-work server latency. The actual generator projection is lower still.
5. **The disk is nowhere near its write-bandwidth ceiling in this mix.** Measured original writes 3,586–3,743 B/op at 0 ms, 3,751–3,830 at 2 ms, 4,097–4,341 at 10 ms. Diskbench maxima were 342.53–344.44 MiB/s over four jobs. At 343 MiB/s, dividing by measured write traffic gives median write-only projections **98.0k / 94.6k / 84.3k ops/s**. These are optimistic, not attainable cluster limits: ignore read traffic, small-group persistence latency, and other bottlenecks. Diagnostic device reads add ~195–390 B/op; process-charged writes sum ~2.8–2.9 kB/op, less than whole-device filesystem writes.
6. **Raw AES is faster than the unmeasured 1–2 GB/s guess.** AES-128-GCM at 16 KiB was 3.936–3.939 us Seal and 3.917–3.919 us Open (~4.16–4.18 GB/s/core), but full TLS CPU includes more than AES. A hot resource-weighted production LIST encoder took ~103–107 us for an average ~302 kB synthetic page; general protobuf marshal/unmarshal benchmarks are NOT the production fast-path encoder and NOT the loadgen's field-validation path.

## Floor model and things NOT established

The tables include a transparent partial primitive-work model per operation and per server role. It assumes balanced client AND watch work, raw AEAD at 4.17 GB/s, hot encoder costs and scaling the LIST page model to the observed page size. It includes reply/watch encoding and client/peer crypto only. It gives leader/follower mix ~**6.85 / 6.57 us/op**, hence ~146k ops/s for the leader core or 150k summed-three-core, and ~136k ops/s if disk wrote only request payload. At RTT 2/10 the idealized in-flight bound lowers these to 86.4k/17.28k before considering the generator. These are **optimistic subset calculations with guesses marked**, NOT a measured or proven full theoretical ceiling. Actual read/index/CRC/memory/cache/GC/HTTP2/syscall/Raft/storage costs are missing, and endpoint/watch work is not perfectly balanced.

Measured same-implementation constant-cost CPU projections are much tighter: median leader-core **9.30k / 9.21k / 8.89k**, versus summed-three-core **11.33k / 11.64k / 11.01k** at 0/2/10. The smaller leader-core number matters: three server cores are not fungible when the leader is the hottest one. New diagnostic runners show a faster 0-ms leader projection (10.21–12.21k), another reason not to pretend a single exact hardware limit was measured.

No full optimized MVCC CPU floor, real zero-work fake-server throughput, actual per-type physical filesystem byte attribution, or controlled old-kernel/current-kernel comparison was measured. The physical disk byte/op depends on batching; it cannot be assigned exactly to GET/LIST/update/create/delete from the aggregate /proc/diskstats counter. No claim of a proven 100k+ mixed capability follows from the optimistic primitive model.

## Source inventory

Read `.github/workflows/bench.yml:53-123,138-182`, `bench/run.sh:89-113,158-199`, `bench/cluster.sh:88-108,142-204`, `bench/netem.sh:25-62`, `bench/profile.sh:19-77`, `cmd/loadgen/mixed.go` (entire file), `cmd/loadgen/main.go:48-75,625-648`, `cmd/loadgen/cpu.go:13-106`, `cmd/loadgen/worker.go:52-160`, `cmd/loadgen/keys.go:55-91`, `cmd/loadgen/wire.go:269-285`, `internal/k8ssize/k8ssize.go` (entire file), `cmd/benchreport/main.go:282-327,351-417`, `cmd/diskbench/main.go:44-71,99-124`, `internal/core/node.go:1198-1203,1655-1686,1980-2050`, `internal/core/codec.go:62-132`, `internal/core/watch.go:368-419`, `internal/mvcc/store.go:213-240`, `internal/core/cache.go:64-82`, `internal/server/batch.go:467-567`, `internal/wal/wal.go:943-1017`. Research measurements: `cmd/loadgen/research484.go:22-76`, `cmd/loadgen/research484_test.go:15-98`, `internal/server/research484_test.go:14-109`, `bench/ceiling-mixed.sh:23-65`.

Validation: `go test -short ./cmd/loadgen ./internal/server` passes; shell syntax and Python analysis/report generation pass; `git diff --check` passes. Initial non-short loadgen tests require LOADGEN_ETCD pointing at etcd v3.7.2 and failed only for that missing dependency; full internal/server tests passed. No issue/PR comments, no PR, no main push, no subagents.
