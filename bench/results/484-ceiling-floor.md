# #484: CPU primitives, real front end, generator

## Identity and runs

- Branch: `exp/484-ceiling-floor`, from `origin/main` = `bd1ade319a4d0bfc92082a8bf8d335aa01199428`.
- Experiment introduction: `4f35af77e97ed9c76577ec0b85b5b27cba7000f9`.
- First measured commit: `e9df49fce25494cc3003f20e1678e931fa7cf3f4`.
- Final measured commit: `826113a986b6577fc9a56615dea2cbb9ec63e576`.
- Registration-only, skipped: https://github.com/djosh34/tigerstore/actions/runs/42019821998
- Initial successful 2-runner run: https://github.com/djosh34/tigerstore/actions/runs/42019938286
- Second registration-only, skipped: https://github.com/djosh34/tigerstore/actions/runs/42023071219 (not a measurement).
- Final successful 2-runner run: https://github.com/djosh34/tigerstore/actions/runs/42023069905
- Baseline artifacts checked: https://github.com/djosh34/tigerstore/actions/runs/40157852634, candidate `255ae6e11883c9e5e08f7cfa0567f264068b42a6`.
- Initial run is superseded: its ParsePut benchmark accidentally boxed a value in an interface, and its null Txn nested response had unnecessarily full headers. Final run fixes both, adds log-write and reply benchmarks, exact client sizes and peer-frame byte counts.
- Both final jobs: `ubuntu-24.04-arm`, 4 Neoverse-N2 CPUs, 1 thread/core, 16 GB, ext4 `/dev/sda1`, Go 1.27.2. `barrier=1` remount and 0-ms peer netem installed. **Kernel now 7.0.0-1017-azure**, not baseline's 6.17.0-1022-azure. No cluster throughput measurements ran on the development host.
- Final jobs are concurrent independent workers: runner 1 Worker ID `{8e5b589d-5dd7-46c5-8938-f2f54cbefebb}`, runner 2 `{57ac664d-26b2-44f0-a797-4171ef9b82d0}`. Their image hostname happens to be identical; provisioning Worker IDs differ.
- No issues/PR comments, PRs, main pushes or subagents.

## Complete measurements and reproducibility

- Every timing and null-server throughput/CPU measurement, with 3-repetition min/median/max for each runner: [484-floor-final.md](484-floor-final.md).
- Initial/superseded run's complete table: [484-floor-initial.md](484-floor-initial.md).
- All underlying parsed timing samples, batch ns, allocations, and complete loadgen result objects: `484-floor-final.json`, `484-floor-initial.json`.
- Raw runner descriptions, stdout, size checks, null-server logs and loadgen JSON are uploaded as `ceiling-floor-1` and `ceiling-floor-2` in both run URLs, 30-day retention.

Exact timing command:

```sh
GOMAXPROCS=1 taskset -c 0 go test -p 1 \
  ./internal/floor ./internal/core ./internal/wal ./internal/mvcc ./internal/server \
  -run '^$' -bench '^BenchmarkFloor' -cpu 1 -benchtime=400ms -count=3 -benchmem
```

Exact frontend/generator invocation: `bench/ceiling-floor.sh:6-38`. Each process starts fresh; warmup 2 s, duration 6 s, 10k keys, 1 KiB values, 6 connections x 256 streams, mTLS. Generator CPU 3. Real tigerstore server CPU 0, then CPUs 0-2 to give the generator headroom. The latter is intentionally NOT the production placement; it isolates the generator's ceiling. Same server binary, HTTP2 reader/writer, frame/HPACK parsing, request decode, metrics, reply encode, TLS and socket path. Only `SubmitBatch` and everything behind it are replaced by immediate completions (`internal/server/batch.go:177-182`, `internal/server/floor_null.go:15-36`). A single real node exists for server metadata/readiness; no request goes through Raft or disk. CAS is **not validated** by the null backend, intentionally. Loadgen still checks unique revisions and one successful Put response per Txn. Null-completion allocation and revision numbering remain in the measured frontend cost.

Reproduce collection:

```sh
gh run download 42023069905 -R djosh34/tigerstore -D /tmp/floor-final
python3 bench/ceiling-floor-collect.py /tmp/floor-final /tmp/floor-final-collected
python3 bench/ceiling-floor-model.py /tmp/floor-final-collected.json
```

Primitive implementations at measured commit:

| Component | Benchmark source | What is timed |
|---|---|---|
| AES-128-GCM seal/open | `internal/floor/floor_test.go:20-53` | preallocated Go AEAD, 80 / 1,103 / 1,221 / 16,384 byte plaintext; zero-allocation; TLS1.3's 5-byte AAD shape |
| Full TLS record seal+open | `internal/floor/tls_test.go:36-78` | actual crypto/tls TLS1.3 AES128-GCM Write + Read over buffers, no syscalls; handshake outside timer; **both ends' CPU** |
| H2 DATA frame parse | `internal/floor/floor_test.go:55-67` | x/net/http2 Framer of one 1,100-byte DATA frame; not HPACK/full request dispatch |
| Decode | `internal/core/floor_bench_test.go:17-53` | generated Google protobuf Unmarshal vs real ParsePut/DecodeAlias; exact loadgen requests, including padded 9-byte revision varint |
| Raft entry encode | `internal/core/floor_bench_test.go:55-85` | real encodeBatch, retaining input wire form; new batch allocation included |
| Raft bookkeeping | `internal/core/floor_bench_test.go:91-138` | bare raft RawNode Propose/Ready/Advance/local memory append/two MsgAppResp; no crypto/network/disk/apply; synchronous Ready API, **not** tigerstore's AsyncStorageWrites driver |
| WAL record encode | `internal/wal/floor_bench_test.go:11-34` | real appendRecord, header/copy/chained CRC32C; reused output capacity |
| Peer message encode/ack decode | `internal/floor/floor_test.go:69-103` | Size + MarshalAppend of actual raftpb.Message/Entry; one follower, reused capacity; protobuf Unmarshal of ack separately |
| Syscall-only | `internal/floor/floor_test.go:105-117` | unix.Write to /dev/null; not a log/durability result |
| Log CPU/kernel-copy reference | `internal/floor/logwrite_test.go:12-40` | prewritten tmpfs overwrite via Pwrite + Fdatasync, per batch; **not durable media/ext4 fsync** |
| MVCC apply | `internal/mvcc/floor_bench_test.go:16-65` | real PutKV or successful CAS Txn on 10k keys, CRC/revisions/version arrays/response allocation included; fake Values.Place names existing bytes without retaining them; rebuild every 65,536 timed operations outside timer |
| Minimal current B-tree lookup/revision update | `internal/mvcc/floor_bench_test.go:67-81` | get existing key and update revision in place; excludes MVCC history, checksum, transaction semantics |
| Reply encode | `internal/server/floor_bench_test.go:14-35` | real put/txn gRPC reply encode, allocation included |

64, 256, 1,536 are batch-size sensitivity points, **not measured production batch histograms**. Node's 4 MiB entry limit permits all three. HTTP connection batches cap at 256, and committer coalescing can produce larger entries.

## Correct the supplied CPU baseline

`server_cpu_us_per_op` is **sum over all three nodes**, not leader CPU (`cmd/loadgen/main.go:635-644`). Use `server_cpu_us_per_op_by_node[0]` for the leader. Multiplying 131,770/s by 13.1 us gives 1.73 cores, already proving it cannot mean a one-core leader.

Baseline tmpfs candidate, min / median / max over five repetitions:

| Workload | ops/s | Leader us/op | Sum of all nodes us/op |
|---|---:|---:|---:|
| put | 129653 / 131770 / 134531 | 7.426 / 7.574 / 7.704 | 12.755 / 13.099 / 13.351 |
| txn | 92994 / 93470 / 93779 | 10.653 / 10.699 / 10.753 | 19.098 / 19.116 / 19.153 |

Baseline 0-ms disk max leader medians are 8.417 us/put and 12.093 us/txn, not 15 and 21.6. Baseline device bytes/op medians: put 3520.491, txn 3906.642, all three nodes on the shared disk.

## Frontend and generator results

These are medians of three runs **per runner**, not pooled results; full ranges in the complete tables.

| CPUs (server / generator) | Workload | Runner 1 ops/s | Runner 2 ops/s | Server us/op R1 / R2 | Generator us/op R1 / R2 | Server cores R1 / R2 | Generator cores R1 / R2 |
|---|---|---:|---:|---:|---:|---:|---:|
| 0 / 3 | put | 296082 | 311678 | 3.377 / 3.203 | 2.200 / 2.116 | 1.000 / 0.998 | 0.652 / 0.659 |
| 0 / 3 | txn | 217167 | 235654 | 4.597 / 4.244 | 2.350 / 2.256 | 1.000 / 1.000 | 0.509 / 0.528 |
| 0-2 / 3 | put | 466409 | 485383 | 4.360 / 4.272 | 2.121 / 2.026 | 2.033 / 2.073 | 0.985 / 0.984 |
| 0-2 / 3 | txn | 435747 | 447505 | 5.642 / 5.560 | 2.234 / 2.156 | 2.451 / 2.488 | 0.970 / 0.965 |

- At normal one-core server placement, server saturates while generator uses only 0.51-0.66 cores. Generator does not explain the present store's throughput.
- With server given 3 cores, generator reaches 0.965-0.985 cores. Measured practical generator envelope: 466-485k puts/s, 436-448k txns/s. This is an approximate bench-generator ceiling, not a formal bound: server scheduling/transport still contributes and per-core server occupancy was not captured.
- Current protocol/TLS implementation costs ~3.20-3.38 us/put, ~4.24-4.60 us/txn. Relative to corrected old leader times, this is ~42-45% / ~40-43%. It is **not** an irreducible tax every conceivable store must pay: it includes tigerstore's stream bookkeeping/metrics/allocation and a tiny null completion.

## Request semantics and encoded bytes

`cmd/loadgen/keys.go:35-53`: key 45 bytes, `/registry/pods/bench-NNN/web-<10 chars>-<5 chars>`; value 1,024 random bytes. `cmd/loadgen/wire.go:251-268`, `cmd/loadgen/workload.go:180-239`: txn is one MOD-revision equality compare, success one Put of the same key, failure one Range. The request repeats the key three times and pads compare revision to 9 varint bytes. All measured CASs succeed. Failure Range is sent/decoded but **not executed**; no old value read is needed for a MOD comparison. Both put and successful txn make one MVCC revision and one replicated write. Txn has **no additional consensus RTT** over put.

Deterministic encoded fixture counts verified identically on both runners:

| Quantity | put | txn |
|---|---:|---:|
| Request protobuf | 1074 | 1192 |
| Request gRPC incl 5-byte prefix | 1079 | 1197 |
| Warm HPACK request block | 6 | 6 |
| Client request HTTP2 incl HEADERS+DATA headers | 1103 | 1221 |
| Reply protobuf (fixture IDs, revision 1M) | 28 | 40 |
| Reply gRPC | 33 | 45 |
| Response HPACK headers / trailers | 19 / 13 | 19 / 13 |
| Reply HTTP2 incl 3 frame headers | 92 | 104 |

Reply fixture uses two 9-byte IDs. Real bench member IDs are **1,2,3** (`cmd/tigerstore/listen.go:27-58`), not random 64-bit IDs: replies are 8 bytes shorter from member ID alone; FNV cluster ID may be 9 or 10 varint bytes. At revision 1M, usual real HTTP2 reply sizes are thus 84-85 put / 96-97 txn. These are **code-derived counts**, not a packet capture; cluster ID/term/revision widths can change them.

| Fixture | Batch | Raft entry data | WAL entry record | Each follower's peer frame | Each follower ACK frame |
|---|---:|---:|---:|---:|---:|
| put | 64 | 69763 | 69789 | 69817 | 32 |
| put | 256 | 279044 | 279070 | 279098 | 32 |
| put | 1536 | 1674244 | 1674270 | 1674298 | 32 |
| txn | 64 | 77315 | 77341 | 77369 | 32 |
| txn | 256 | 309252 | 309278 | 309306 | 32 |
| txn | 1536 | 1855492 | 1855518 | 1855546 | 32 |

Fixture uses 9-byte origin, 1-byte boot, 3-byte request ID. Actual bench uses 1-byte origin, random 64-bit boot (`internal/node/node.go:724`), and usually 3-byte request ID. For boot width 9/10, actual entry is fixture +0/+batch bytes. General per-op overhead is `uvarint(origin)+uvarint(boot)+uvarint(requestID)+1(kind)+uvarint(request size)`. Entry adds format bytes and uvarint batch count. WAL entry adds 26 bytes per entry (9-byte record frame, 17-byte term/index/type); hardstate records add 33 bytes per record separately. Peer fixture frame adds 54 bytes; short actual From/To IDs reduce that by 16 bytes, subject to optional-field presence/index widths. ACK fixture similarly shrinks from 32 to 16 bytes with the same field set. Every follower gets the full data, not half. All three nodes write one copy, before ext4/device/checkpoint overhead.

TLS1.3 adds 22 bytes per record (5 header + 1 content type + 16 tag), **not per operation**; records can coalesce operations and split at 16 KiB. TCP/IP/ACK/flow-control bytes, actual peer aggregation and actual record counts/op were **not measured** here; do not treat fixture bytes as observed NIC traffic. Model 16 KiB coalescing only as an explicit optimistic assumption.

## CPU component sum: explicitly a model/guess, not a universal maximum

See [484-floor-model.md](484-floor-model.md) for every batch/runner row. Formula implemented exactly in `bench/ceiling-floor-model.py`:

`null frontend + one additional apply decode + entry encode + raw Raft + WAL record + two peer protobuf encodes + actual MVCC apply + tmpfs Pwrite/Fdatasync + two peer AES-GCM seals (16 KiB-coalesced byte rate) + two ack decodes/AEAD opens per batch`.

At batch 1,536:

| Workload | Runner | Frontend us/op | Backend primitives us/op | Peer AEAD+ACK us/op | Sum us/op | Reciprocal ops/s | Corrected old leader minus sum us/op |
|---|---|---:|---:|---:|---:|---:|---:|
| put | 1 | 3.377 | 1.521 | 0.577 | 5.475 | 182651 | 2.099 |
| put | 2 | 3.203 | 1.251 | 0.525 | 4.980 | 200819 | 2.594 |
| txn | 1 | 4.597 | 3.538 | 0.639 | 8.774 | 113971 | 1.925 |
| txn | 2 | 4.244 | 2.947 | 0.582 | 7.772 | 128659 | 2.926 |

For batches 64/256 the modeled totals span put 5.084-5.651 us (177-197k/s), txn 7.885-8.896 us (112-127k/s). Thus these particular isolated components suggest an optimistic **~180-200k put/s / ~110-129k txn/s CPU reference** at 0-ms RTT, versus existing tmpfs ~132k/~93k. This is a constructive budget assembled from separately timed primitives, **not demonstrated end-to-end attainable throughput** and not a theorem about other store designs.

Why this is a guess/model:

- Actual production batch-size distribution not captured; 1,536 per batch is intentionally optimistic.
- Peer TLS record bookkeeping, socket payload copies/reads/writes, scheduling, batching queues, auth/quota/size validation, timeouts, waiter maps, watch matching, placement bookkeeping, cache maintenance, background checkpoints and cross-component GC/cache interactions are not fully captured. Null frontend does include its own scheduling/TLS/socket work, but peer side is only AEAD+protobuf.
- Raw Raft reference uses the supported synchronous Ready API, not the production asynchronous-storage driver; txn model reuses put-sized Raft/log-write timing. These are optimistic approximations, not measured current Raft/txn driver costs.
- MVCC benchmark keeps only ~6.5 recent timed versions per key between rebuilds, unlike the growing production history; it does measure real insertion/CRC/CAS/response code. This is a low-history floor reference, not production apply throughput.
- Protocol/TLS 3.20-4.60 us is a current-code reference, not a mandatory minimum for all implementations. A purpose-built decoder/index/transport may eliminate allocations or duplicate decodes, including much of the 12-allocation txn decode.
- Kernel differs from baseline. Subtracting the model from baseline is an **unattributed difference**, not measured profiling attribution. Rough residual ~2-3 us/leader op could contain the omitted work above; no exact split is claimed.
- For a store independent of today's implementation, these experiments cannot establish a unique rigorous CPU floor/max. No honest measurement can prove all possible implementations need the sum of these particular routines.

## End-to-end code map read (at final measured commit)

- Bench contract/placement/netem/profiling: `.github/workflows/bench.yml:1-211`, `bench/run.sh:75-215`, `bench/cluster.sh:79-198`, `bench/netem.sh:24-64`, `bench/profile.sh:13-78`, `cmd/diskbench/main.go:96-123`, `cmd/benchreport/main.go:320-387`.
- Client generation/wire/CAS validation: `cmd/loadgen/main.go:49-87,230-332,635-647`, `cmd/loadgen/cpu.go:12-49,85-106`, `cmd/loadgen/keys.go:8-53`, `cmd/loadgen/wire.go:140-214,220-268`, `cmd/loadgen/conn.go:235-300`, `cmd/loadgen/workload.go:180-239`.
- Server TLS handoff: `internal/server/server.go:245-309`; HTTP2 frame/HPACK/read dispatch: `internal/server/h2.go:282-339,493-537,571-620,726-801`; gRPC framing/body copy: `internal/server/stream.go:131-177`; decode/batch: `internal/server/rpc.go:104-108`, `internal/server/batch.go:24-32,51-103,146-207`.
- Driver batch and completion: `internal/node/node.go:335-360,1036-1185,1301-1345`; persistence grouping/write/sync: `internal/node/node.go:1437-1612,1619-1703`; random boot: `internal/node/node.go:724`.
- Core validate/propose: `internal/core/node.go:1099-1258,1594-1672`; entry codec/put parser/txn alias codec: `internal/core/codec.go:12-24,63-137,179-211`, `internal/core/wire.go:49-101`, `internal/core/txnwire.go:73-149,186-235`.
- Raft persist/apply ordering: `internal/core/node.go:1691-1818,1851-1938,2183-2350`; actual Raft config uses AsyncStorageWrites: `internal/core/node.go:794-809`.
- WAL record: `internal/wal/wal.go:133-136,888-904,955-973,997-1020`; packs/cleaned values also read in `internal/wal/pack.go:112-178`, not part of hot-log primitive timing.
- Peer receive/TLS/protobuf/framing and send: `internal/peer/peer.go:591-665,1058-1098,1179-1219`.
- Apply/index/history/checksum/txn compare: `internal/mvcc/store.go:1-7,28-51,139-174,353-369`, `internal/mvcc/ops.go:396-409,451-541,581-620,644-730,782-807,848-890`.
- Reply build/header/gRPC and TLS writer: `internal/server/batch.go:339-400,442-500`, `internal/server/txnreply.go:19-35,140-207`, `internal/server/stream.go:352-409`, `internal/server/write.go:32-90,125-190`.
