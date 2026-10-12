# ARM request costs

Five repetitions, one CPU per process, GOMAXPROCS=1. CPU is process user plus system time. Wall includes measurement-loop overhead. No overhead subtraction. These are not database rates.

| Test | Median wall ns/op | Median CPU ns/op | CPU range ns/op |
| --- | ---: | ---: | --- |
| fsync-probe-pair-control | 851.79 | 851.68 | 838.231 to 858.674 |
| loop-control | 1.91 | 1.90 | 1.793 to 1.904 |
| memcpy-1KiB-cached | 24.69 | 24.69 | 24.629 to 24.718 |
| AES128GCM-TLS-inner-1024B-seal | 318.47 | 318.44 | 317.606 to 318.679 |
| AES128GCM-TLS-inner-1024B-open | 324.99 | 324.98 | 324.286 to 325.081 |
| crypto-tls-record-1024B-encrypt-discard | 448.24 | 447.95 | 447.500 to 451.790 |
| crypto-tls-record-1024B-decrypt-memory | 497.94 | 497.99 | 493.910 to 503.540 |
| AES128GCM-TLS-inner-11B-seal | 78.13 | 78.13 | 78.016 to 78.205 |
| AES128GCM-TLS-inner-11B-open | 82.82 | 82.79 | 82.775 to 85.964 |
| crypto-tls-record-11B-encrypt-discard | 191.83 | 191.78 | 187.190 to 192.350 |
| crypto-tls-record-11B-decrypt-memory | 157.09 | 156.98 | 156.810 to 160.790 |
| HTTP2-DATA-parse-1KiB | 120.73 | 120.73 | 117.723 to 121.901 |
| grpc-envelope-PutRequest-decode-fresh | 532.54 | 532.46 | 517.813 to 535.786 |
| grpc-envelope-PutRequest-encode-fresh | 511.22 | 511.19 | 497.386 to 511.934 |
| grpc-envelope-PutResponse-decode-fresh | 232.82 | 232.77 | 231.330 to 233.138 |
| grpc-envelope-PutResponse-encode-fresh | 170.11 | 170.09 | 169.348 to 170.727 |
| raft-entry-memory-metadata-and-copy-ring | 36.12 | 36.11 | 35.802 to 36.444 |
| raft-MemoryStorage-Append-prebuilt-pointer-no-data-copy | 36.80 | 36.80 | 35.920 to 37.269 |
| raft-entry-protobuf-framed-buffered-file-no-sync | 297.92 | 297.94 | 295.825 to 301.390 |

## Write and fsync

Cyclic overwrite of initialized 64MiB files. File and directory sync before timing. Two-second windows. Quantiles are medians of per-run quantiles, not pooled quantiles.

| Batch | Wall us/batch | CPU us/batch | Fsync-only wall us | Fsync-only CPU us | Write+sync p50 us | Write+sync p99 us | Batches/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4096 B | 80.020 | 17.040 | 75.073 | 12.762 | 67.922 | 192.070 | 12496.8 |
| 65536 B | 186.556 | 27.812 | 174.442 | 16.337 | 90.123 | 4298.948 | 5360.3 |
| 1048576 B | 2940.971 | 128.839 | 2850.996 | 41.696 | 482.711 | 9158.978 | 340.0 |

## Three-process TLS loopback

One leader and two followers on distinct CPUs. Wait for local completion plus first follower for quorum latency. Drain both followers before the next entry. The reported rate is all-followers-drained, not a pipelined quorum rate.

| Mode | Quorum p50 us | Quorum p99 us | All-drained op/s | Leader CPU us/op | Follower1 CPU us/op | Follower2 CPU us/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| nondurable | 36.889 | 56.266 | 19191.2 | 39.727 | 22.465 | 21.509 |
| durable | 347.547 | 779.520 | 2380.5 | 102.418 | 98.244 | 82.076 |

## Complete no-op gRPC Put over TLS

Actual grpc-go unary dispatch, actual etcd PutRequest and PutResponse. One connection, separate CPU for client and server. Closed-loop workers. No Raft, storage, apply, auth or watches.

| Workers | RPC/s | RPC p50 us | RPC p99 us | Client CPU us/op | Server CPU us/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 9495.2 | 102.203 | 207.135 | 52.612 | 58.892 |
| 64 | 46779.0 | 1097.850 | 3152.718 | 18.788 | 19.037 |

Raw JSONL, compiler/build info, full module versions, mount checks and thread affinity are in the artifact. See README.md in the exact experiment commit for timing and durability semantics.

