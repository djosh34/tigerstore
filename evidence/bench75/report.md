## Paired Put benchmark for PRs #75 and #76

[Run 38368919077](https://github.com/djosh34/tigerstore/actions/runs/38368919077) passed. Two free `ubuntu-24.04-arm` jobs, 64 and 256 clients, each ran five repetitions of main, PR 75, PR 76 and stock etcd v3.7.2 back to back. Odd repetitions used that order; even repetitions reversed it. All 40 measured windows passed the error, success-count, revision, term, leader and affinity checks.

The workflow resolved the newest branch heads at 08:23:29Z, six seconds before both measurement jobs started:

| Store | Measured commit |
| --- | --- |
| Main | `e1fd889c345bc3c06d8100eb6d635c88ad6967cd` |
| PR 75, `work/50-frontend` | `8fcdd021aea23eca9b9d012f56faee13be39f1fc` |
| PR 76, `work/50-batch`, stacked on PR 75 | `b3e6f6237c6d6a7fb1ad6018efffe8cee6bf4c56` |
| Etcd v3.7.2 release | `68c065e562994b89e333e77b039ad066f933c586` |

The runner image was `ubuntu24-arm64 20261004.142.1`, Linux `6.17.0-1022-azure`, four Neoverse-N2 cores and 16.72 GB RAM. Runner names were `GitHub Actions 1000007749` for 64 clients and `GitHub Actions 1000007748` for 256. Each store used leader CPU 0, followers CPUs 1 and 2, generator CPU 3. All server threads' affinities were checked. Tigerstore kept its existing minimum of two Go processors per server on one pinned CPU; etcd and the generator used one.

Tigerstore and the generator used Go 1.27.2; the stock etcd release used Go 1.26.8. The generator was the same direct protobuf/grpc-go v1.84.0 closed-loop client adapted from `exp/bench-68`, 16 established TLS 1.3 connections with client certificates, leader endpoint only. Values were 1 KiB, with 4,096 Kubernetes-shaped keys and independent deterministic values reused on overwrite. Each fresh cluster received 4,096 seed, 20,000 warmup and 60,000 measured Puts. Peer traffic also used certificate-authenticated TLS. There were no watches, leases, compaction or injected network delay. Etcd used safe persistence defaults and an 8 GiB quota. Both jobs remounted their data filesystem with `barrier=1` and verified that `nobarrier` and `barrier=0` were absent.

### Medians of five repetitions

CPU is user plus system time from `/proc/PID/stat`, with process identity checks and `CLK_TCK=100`. Total CPU sums the three server processes, not their threads. Latency columns are medians of per-run percentiles, not pooled percentiles.

| Clients | Store | Put/s | p50 ms | p99 ms | Leader CPU us/Put | Total server CPU us/Put |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 64 | Main | 8,733.4 | 6.924 | 16.532 | 112.17 | 147.50 |
| 64 | PR 75 | 21,026.6 | 2.871 | 7.387 | 44.33 | 78.50 |
| 64 | PR 76 | 23,886.1 | 2.501 | 6.547 | 36.33 | 70.83 |
| 64 | Etcd | 3,802.4 | 16.286 | 28.318 | 200.17 | 297.33 |
| 256 | Main | 9,565.0 | 25.282 | 56.519 | 104.33 | 123.00 |
| 256 | PR 75 | 31,612.9 | 7.375 | 19.182 | 31.00 | 46.33 |
| 256 | PR 76 | 41,552.5 | 5.483 | 14.316 | 22.50 | 39.33 |
| 256 | Etcd | 4,744.8 | 53.255 | 85.703 | 173.33 | 232.50 |

PR 75's median rate was 2.41x main and 5.53x etcd at 64 clients, 3.31x main and 6.66x etcd at 256. PR 76 added 13.6% and 31.4% over PR 75. These are ratios of median rates for this workload, not maximum or sustained capacity claims.

### Leader allocations and GC

Allocation counts are cumulative counter deltas over the window divided by 60,000 successful Puts. Tigerstore uses `runtime/metrics`; etcd uses its stock `go_memstats_mallocs_total` and `go_memstats_alloc_bytes_total`. They include the whole leader process, including Raft, peers, runtime and observation overhead.

| Clients | Store | Objects/Put | Bytes/Put | GC CPU share | Heap object MiB after window |
| ---: | --- | ---: | ---: | ---: | ---: |
| 64 | Main | 139.75 | 21,898 | 1.38% | 248.9 |
| 64 | PR 75 | 43.35 | 14,002 | 1.81% | 244.6 |
| 64 | PR 76 | 38.98 | 13,650 | 2.28% | 240.6 |
| 64 | Etcd | 277.81 | 32,811 | Unavailable | 29.0 |
| 256 | Main | 140.54 | 21,768 | 1.65% | 231.6 |
| 256 | PR 75 | 39.51 | 14,034 | 2.71% | 226.1 |
| 256 | PR 76 | 34.59 | 13,802 | 3.98% | 264.4 |
| 256 | Etcd | 264.85 | 33,389 | Unavailable | 43.2 |

GC share is the runtime's estimated GC CPU delta divided by its estimated non-idle CPU delta, using the CPU-class metrics. It is not GC pause time or a fraction of the independent `/proc` CPU measurement. Etcd's stock endpoint has no cumulative GC CPU counter. Heap bytes are the observed object bytes without a forced GC, not a minimum live set. The allocation snapshots immediately bracket the CPU window and are slightly wider.

### Channels and value copies, counted from code

These are source counts for the exact tigerstore commits above, not runtime measurements. One successful send and one receive count separately. Failed nonblocking sends and unselected select arms do not count. A value copy means all 1 KiB reaches another backing store, not a slice header or pointer copy.

| Leader cost per Put | Main | PR 75 | PR 76 |
| --- | --- | --- | --- |
| Fixed application request/reply channel operations | 4, two sends and two receives | 4, two sends and two receives | 0 on the batch Put path |
| Fresh reply channels created | 1 | 1 | 0 on the batch Put path |
| Admission channel operations | Included above | Included above | 2 per actual admission batch, divided by Puts |
| Grouped completion handoff | Included above | Included above | Direct call, no channel operation |
| Known leader full-value materialization stages before peer sends | 5 | 6 | 6 |

The channel figures are not total channel operations per Put. Disk and peer work remain batch-dependent; writer wakes are conditional and coalesced. Every counted site and the conditional ledger appear in the [source audit](https://github.com/djosh34/tigerstore/blob/b39be74241c0c8d100c845fec6901c5fe5178e8b/evidence/bench75/source-audit/audit.md). The main and PR 75 request exchange is `internal/node/node.go:122`, `:264` or `:269`, `:301`, `:217`. PR 76 admission is `internal/node/node.go:123`, `:311` or `:316`; its completion calls `Complete` at `internal/node/node.go:400`.

Other leader channel sites are:

| Work | Main and PR 75 sites | PR 76 sites |
| --- | --- | --- |
| Disk queue send/receive and completion send/receive | `internal/node/node.go:282`, `:369` or `:376`, `:390`, `:275` | `internal/node/node.go:329`, `:453` or `:460`, `:474`, `:322` |
| Outbound peer send/receive | `internal/peer/peer.go:294`, `:305` or `:311` or `:314` | Same |
| Inbound peer send/receive | `internal/node/node.go:197`, `:264` or `:269` | `internal/node/node.go:242`, `:311` or `:316` |
| Writer wake send/receive | Main is inside excluded HTTP/gRPC libraries. PR 75 uses `internal/server/h2.go:175`, `internal/server/write.go:50` | `internal/server/h2.go:188`, `internal/server/write.go:58` |
| Inflight-space send/receive | None | `internal/server/batch.go:122`, `:101` |
| Background ticker receive | `internal/node/node.go:286` | `internal/node/node.go:333` |

For N fully accounted Puts, A admission batches, Q disk-queue handoffs, F disk-completion handoffs, O outbound and I inbound peer messages, T ticker receives, W actual writer-wake sends plus receives, and R actual inflight-space sends plus receives, the repository-level channel counts per Put are `4 + (2Q+2F+2O+2I+T)/N` on main, that plus `W/N` on PR 75, and `(2A+2Q+2F+2O+2I+T+W+R)/N` on PR 76. These populations were not instrumented, so no exact total or assumed batch size is reported. There is no explicit application channel close per normal Put. Library/context lifecycle channels are outside this census.

The named value materializations are:

| Stage | Source site | Copies/Put |
| --- | --- | ---: |
| Custom frontend accumulates the message | PR 75 `internal/server/stream.go:100`; PR 76 `:103` | 1 on each candidate |
| Frontend protobuf owns PutRequest.Value | Main grpc-go v1.84.0 `encoding/proto/proto.go:96`; PR 75 `internal/server/rpc.go:59`; PR 76 `:62`. Protobuf v1.36.11 copies the bytes at `internal/impl/codec_gen.go:5497` | 1 on each store |
| Encode the value into the Raft batch | `internal/core/codec.go:64`, all commits | 1 |
| Raft deep-clones entry data | `internal/core/node.go:522` reaches raft v3.7.0 `raft.go:817`; protobuf `internal/impl/merge.go:188` | 1 |
| Copy entry data into the WAL buffer | `internal/wal/wal.go:351`, reached at `:368`, all commits | 1 |
| Apply decodes another owned value | `internal/core/codec.go:114`, all commits | 1 |
| Serialize to each entry-bearing peer recipient | `internal/peer/peer.go:416`, all commits | 1 per transmitted occurrence per recipient |

The leader subtotal is therefore 5/6/6 plus peer-send multiplicity and conditional buffer-growth or transport copies. It is not an exact physical memcpy total. Each normally delivered follower also copies at peer decode `internal/peer/peer.go:238`, WAL `internal/wal/wal.go:351`, and apply `internal/core/codec.go:114`. Main's HTTP/gRPC buffering and all stores' TLS, bufio, socket/kernel and growth copies are excluded, not zero. The index stores the decoded value slice at `internal/mvcc/ops.go:235`, not a log offset; that assignment adds no payload copy. Neither PR implements stored-once wire values or in-place apply yet.

### Limits and reuse

The three replicas share a VM and disk. Guest barriers and successful syncs do not establish physical power-loss protection. CPU/allocation windows include drain but exclude later follower persistence fences. At 256 clients PR 76's generator occupied a median 0.922 of its one core; generator headroom is small. Its median measured window was only 1.44 seconds. Closed-loop latency suppresses arrivals during stalls. No crash durability, open-loop latency, capacity maximum or Kubernetes result is established here.

[Raw evidence, per-run CSVs and source audit](https://github.com/djosh34/tigerstore/tree/b39be74241c0c8d100c845fec6901c5fe5178e8b/evidence/bench75) preserve the results. [Reusable workflow and scripts](https://github.com/djosh34/tigerstore/tree/fa86c2a730ec53277898c7c7c711e724e2410556/experiments/bench75) live on `exp/bench-75`, with `candidate_ref`, optional `second_candidate_ref` and `baseline_ref` inputs. Only that branch triggers the workflow. Every tigerstore archive receives the same observation-only loopback endpoint, so these are instrumented versions of the named commits. Core, frontend, Raft, storage and peer code are unchanged. Nothing ran locally except the small generator tests and vet with the host memory limits. Nothing merged.
