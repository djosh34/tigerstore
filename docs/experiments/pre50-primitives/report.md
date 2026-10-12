# Request costs on the ARM runner

Successful [run 38316850699](https://github.com/djosh34/tigerstore/actions/runs/38316850699), [job 115035419286](https://github.com/djosh34/tigerstore/actions/runs/38316850699/job/115035419286), measured code [`28edcb7fe4d96394e814331d4987961f79857087`](https://github.com/djosh34/tigerstore/tree/28edcb7fe4d96394e814331d4987961f79857087/experiments/pre50-primitives). Branch `exp/pre50-primitives` only, nothing merged and no issue comments. The complete job took about 3 minutes 20 seconds, below its 15-minute limit. No further experiments are needed.

Free `ubuntu-24.04-arm`, image `20261004.142.1`, four Neoverse-N2 CPUs with AES/PMULL instructions, 16,722,010,112 bytes RAM, Linux `6.17.0-1022-azure`. Five repetitions per case. CPU0 ran local cases and quorum leader, CPU1 follower1 or no-op RPC server, CPU2 follower2, CPU3 RPC client. Every measured process used `GOMAXPROCS=1`. All 94 recorded start/end affinity snapshots had singleton CPU masks for every observed thread; follower/server result snapshots also passed. Process pinning does not isolate a CPU from runner background work.

The disposable VM initially mounted ext4 `/dev/sda1` with `nobarrier`. The workflow ran `sudo mount -o remount,barrier=1 /` and rejected `nobarrier` and `barrier=0` before and after measurement. Other options remained `discard,journal_async_commit,commit=30,data=writeback`. No host sudo or settings changes occurred. Barriers and successful fsync do not independently prove hypervisor or physical power-loss durability. The three files share one disk failure domain.

Compiler is Go `gc` 1.27.2, linux/arm64, GOARM64=v8.0. Exact versions are etcd API v3.7.2, Raft v3.7.0, grpc-go v1.83.2, protobuf v1.36.11, x/net v0.58.0, x/sys v0.47.0 and x/text v0.41.0. Full direct/transitive versions and checksums are in the evidence and module lockfiles. Executable SHA-256 is `c3398f68d29d81e51f2183d2bc065e8cc22d8daccd09b286991a92331ab9550b`. Build metadata says `vcs.modified=true` because `.run` generated untracked files. All ten uploaded source/lock/workflow files match the measured commit byte for byte.

## Local costs

These are median wall and process CPU nanoseconds per operation. CPU is user plus system `getrusage` time, including runtime/GC work during timing. Five repetitions, no overhead subtraction. The complete summary retains each cost's CPU range.

| Operation | Wall ns/op | CPU ns/op |
| --- | ---: | ---: |
| AES128-GCM seal, 1KiB plus TLS inner type | 318.47 | 318.44 |
| AES128-GCM open, same | 324.99 | 324.98 |
| Actual TLS1.3 record encrypt, 1KiB | 448.24 | 447.95 |
| Actual TLS1.3 record decrypt, 1KiB | 497.94 | 497.99 |
| Actual TLS record encrypt, 11B PutResponse | 191.83 | 191.78 |
| Actual TLS record decrypt, 11B PutResponse | 157.09 | 156.98 |
| HTTP/2 DATA parse, 1KiB | 120.73 | 120.73 |
| gRPC envelope plus PutRequest decode | 532.54 | 532.46 |
| gRPC envelope plus PutRequest encode | 511.22 | 511.19 |
| gRPC envelope plus PutResponse decode | 232.82 | 232.77 |
| gRPC envelope plus PutResponse encode | 170.11 | 170.09 |
| Custom memory entry metadata plus 1KiB copy | 36.12 | 36.11 |
| Stock raft.MemoryStorage.Append, prebuilt pointer | 36.80 | 36.80 |
| Entry protobuf plus buffered file append, no sync | 297.92 | 297.94 |
| Cached memcpy, 1KiB | 24.69 | 24.69 |

PutRequest has a 32B key and 1,024B value, 1,061B encoded. PutResponse is 11B and does not echo the value. Real TLS record tests negotiate `TLS_AES_128_GCM_SHA256`, then use a memory connection adapter. Encrypt includes the library's record path and writes to a discard sink, not a socket. Decrypt consumes genuine prebuilt records once in sequence and compares plaintext after timing. Wire records are 1,046B and 33B. The HTTP/2 case includes Framer allocation/copy and reader reset, but no HPACK, flow control or dispatch. Fresh protobuf objects/buffers include allocation; decode does not call Size inside timing. Memcpy consumes its destination after timing.

MemoryStorage.Append retains prebuilt immutable Entry pointers sharing a payload. It does not deep-copy the value or measure RawNode.Propose. The custom memory-copy row is separate. Buffered file append encodes and frames a stock Entry through a 64KiB bufio.Writer into the kernel page cache. No fsync or recoverable WAL. Its separately charged final partial flush took 11.048 to 16.961 us wall across repeats. The loop control measured 1.90 ns/op CPU.

## Write and fsync

One writer, two-second windows, cyclic overwrite of fully initialized 64MiB files. File and directory sync before timing. Five repeats with reversed size order on even repeats. Units are microseconds per batch. Fsync-only measurements include probe overhead and are not device service times.

| Batch | Write+sync wall | Write+sync CPU | Fsync-only wall | Fsync-only CPU | Write+sync p50 | Write+sync p99 | Batches/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4KiB | 80.020 | 17.040 | 75.073 | 12.762 | 67.922 | 192.070 | 12,496.8 |
| 64KiB | 186.556 | 27.812 | 174.442 | 16.337 | 90.123 | 4,298.948 | 5,360.3 |
| 1MiB | 2,940.971 | 128.839 | 2,850.996 | 41.696 | 482.711 | 9,158.978 | 340.0 |

The fsync probe-pair control measured 0.852 us CPU and wall. It runs the two getrusage calls and time probes inserted around each Sync. We do not subtract it. The 1MiB mean wall cost is much larger than p50, so the median latency alone does not represent its completed-batch rate. These overwrites exclude extent growth and segment installation.

## Three-node TLS loopback

One leader and two followers, each on a distinct CPU, mutual TLS1.3 AES128-GCM. The leader marshals a 1KiB Entry, sends both followers concurrently and appends locally. Followers decode/validate the Entry before appending. Quorum timing waits for local completion and the first follower acknowledgment. Both followers drain before the next proposal. Twenty warmups precede 10,000 nondurable or 3,000 durable measured entries per repeat.

| Mode | Quorum p50 us | Quorum p99 us | All-drained op/s | Leader CPU us/op | Follower1 CPU us/op | Follower2 CPU us/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Nondurable memory rings | 36.889 | 56.266 | 19,191.2 | 39.727 | 22.465 | 21.509 |
| Durable-interface file sync | 347.547 | 779.520 | 2,380.5 | 102.418 | 98.244 | 82.076 |

Durable mode writes growing files and fsyncs every entry at all nodes before that node's completion. File creation and directory sync precede warmup. This tests a flush-qualified quorum exchange, not actual Raft agreement, recovery or a database. Throughput is all-followers-drained and single-entry closed-loop, not pipelined quorum throughput. The artifact records each process's CPU and elapsed window separately.

## Complete no-op gRPC Put over TLS

Separate pinned server/client, one connection, mutual TLS1.3 AES128-GCM. Actual PutRequest/PutResponse, grpc-go unary dispatch, 500 warmup calls and two-second closed-loop windows. The handler validates the 1KiB value and returns the small header. It does no consensus or storage. All measured client/server counts match.

| Workers | RPC/s | p50 us | p99 us | Client CPU us/op | Server CPU us/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 9,495.2 | 102.203 | 207.135 | 52.612 | 58.892 |
| 64 | 46,779.0 | 1,097.850 | 3,152.718 | 18.788 | 19.037 |

This is the whole no-op RPC transport/dispatch cost, not isolated handler dispatch. Do not add the local TLS/framing/codec rows again. Server CPU uses control RPC boundaries and has small additional boundary overhead.

## Reproduction and evidence

```sh
# The workflow enables barriers on its disposable runner first.
timeout 660 bash experiments/pre50-primitives/run.sh
# Retrieve the exact successful run:
gh run download 38316850699 -R djosh34/tigerstore \
  -n pre50-primitives-arm64 -D ./downloaded
python3 experiments/pre50-primitives/summarize.py ./downloaded/results
```

[Artifact 11777411625](https://github.com/djosh34/tigerstore/actions/runs/38316850699/artifacts/11777411625), 79,245 bytes, digest `sha256:83733ecf0531395b3f8c75f743b4aba24819f95740c7d68a26afb42b33f7e3a9`, expires 2027-01-10. The branch also preserves the raw results archive and full summary under `docs/experiments/pre50-primitives/`. Tests, vet, module verification and summary count checks passed. All 52 stderr files are empty. Workflow Actions use exact commit pins. Runner build outputs/caches were deleted at exit; host-owned build caches and temporary outputs were removed after report preparation.

No reciprocal here is whole-database speed. No percentile sums or sampled CPU-to-wall conversion. Short repeats on one VM are not independent-host confidence intervals. There is no full consensus/apply, MVCC, index, watch fanout, overload, recovery or crash test. The parent's independently measured TLS/barrier-enabled etcd baseline remains a separate experiment. Only the parent posts the consolidated #50 comment.
