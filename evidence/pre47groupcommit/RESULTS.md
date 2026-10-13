# Results

Final measured source: `fad17fe3fe64c73075da484c773f4859bc4e8e18`.

Successful run: https://github.com/djosh34/tigerstore/actions/runs/38330842168

All 45 measured windows passed. Each completed 60,000 Puts with no errors, exactly 60,000 new revisions, unchanged leader and term, correct all-replica fences and verified CPU affinities. Five fresh clusters per store per concurrency. These are medians of per-run statistics, not pooled latency percentiles.

## Put throughput and latency

| Clients | Store | Put/s | Mean ms | p50 ms | p99 ms | p99.9 ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 16 | Baseline | 5,275.5 | 3.032 | 2.753 | 10.713 | 15.289 |
| 16 | Prototype | 5,172.1 | 3.093 | 2.981 | 4.871 | 10.223 |
| 16 | Etcd | 2,180.2 | 7.338 | 7.267 | 13.656 | 18.309 |
| 64 | Baseline | 3,863.1 | 16.563 | 17.371 | 33.494 | 82.143 |
| 64 | Prototype | 11,232.2 | 5.696 | 3.648 | 25.191 | 42.238 |
| 64 | Etcd | 4,084.6 | 15.664 | 15.001 | 27.487 | 62.582 |
| 256 | Baseline | 4,474.5 | 57.134 | 59.467 | 99.033 | 114.749 |
| 256 | Prototype | 4,922.4 | 51.941 | 54.100 | 99.875 | 111.957 |
| 256 | Etcd | 4,745.8 | 53.872 | 53.227 | 84.605 | 99.748 |

## Process CPU microseconds per Put

| Clients | Store | Leader | Follower 1 | Follower 2 | All servers | Generator |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 16 | Baseline | 150.333 | 74.667 | 84.167 | 309.167 | 71.833 |
| 16 | Prototype | 137.667 | 59.833 | 59.667 | 259.833 | 67.500 |
| 16 | Etcd | 317.000 | 123.167 | 123.000 | 563.667 | 104.667 |
| 64 | Baseline | 103.667 | 26.333 | 23.667 | 152.333 | 30.667 |
| 64 | Prototype | 55.667 | 17.333 | 14.500 | 87.500 | 28.833 |
| 64 | Etcd | 188.500 | 47.500 | 42.000 | 277.833 | 59.333 |
| 256 | Baseline | 86.833 | 13.500 | 12.000 | 112.667 | 29.333 |
| 256 | Prototype | 80.667 | 12.500 | 11.167 | 104.333 | 28.833 |
| 256 | Etcd | 175.500 | 30.667 | 29.167 | 235.333 | 55.167 |

CPU ends at client drain, before follower catch-up fences. Independent column medians need not sum. These numbers include runtime, allocation and system CPU charged to each process, but not I/O wait.

## Completed Puts per sync

| Clients | Store | Leader | Follower 1 | Follower 2 |
| ---: | --- | ---: | ---: | ---: |
| 16 | Baseline | 4.122 | 3.446 | 3.311 |
| 16 | Prototype | 3.998 | 4.135 | 4.134 |
| 16 | Etcd WAL | 1.799 | 3.385 | 3.399 |
| 64 | Baseline | 1.170 | 10.838 | 9.908 |
| 64 | Prototype | 5.616 | 14.071 | 14.265 |
| 64 | Etcd WAL | 3.316 | 11.431 | 11.255 |
| 256 | Baseline | 1.251 | 39.761 | 38.290 |
| 256 | Prototype | 1.357 | 41.181 | 41.494 |
| 256 | Etcd WAL | 6.193 | 45.386 | 46.368 |

Tigerstore counts file fsync calls, including hard state and segment headers, excluding directories. Etcd uses its WAL sync histogram, mostly fdatasync, not all process fsync calls. Backend commit counts are separate in the raw files. Counters follow all-replica persistence fences and cover a wider interval than the CPU timer. This is gross sync density, not the number of Puts in each logical Raft entry.

## What the experiment answers

The deterministic driver test proves the intended scheduling rule for supplied arrivals. Holding one append, submitting 16, 64 or 256 requests, and releasing it produces one next group containing exactly 16, 64 or 256 unique requests. The test crosses the small test entry cap, so several Raft entries can share that group. `audit/queued-load-tests.txt` records the result and 50 race repetitions. This does not prove that closed-loop gRPC clients supply those arrivals during a real append.

The real workload does not show reliably large leader groups or monotonic group growth with client count. In the final run, leader sync density is 3.998, 5.616 and 1.357 as concurrency rises. Followers reach about 41 Puts per sync at 256 clients, while the leader remains near one. The prototype batches the actual queued arrivals; increasing closed-loop client count is not the same as increasing the leader's arrival rate.

At 64 clients, the final prototype is 2.91 times its paired baseline and 2.75 times etcd by the ratio of median rates. At 16 clients it is 2% slower than baseline, though its p99 improves. At 256 it is only 10% faster than baseline and 3.7% faster than etcd, with a worse p99 than etcd. Aggregate CPU per Put is 53.9%, 68.5% and 55.7% below etcd at the three concurrencies. These results do not establish a general throughput or latency win.

Repetition spread is large at 64 clients. Prototype rates range from 5,969.4 to 16,566.2 Put/s; baseline ranges from 3,276.2 to 7,578.8. At 256, prototype rates range from 4,481.4 to 5,269.0. Every valid repetition remains in `runs.csv`.

The first complete run used `ae9abb82394e47799003fc62afae2b68fde558a5`, before the inbox-drain correction, and also passed all 45 windows. Run https://github.com/djosh34/tigerstore/actions/runs/38329983954 is archived in `first-run/`. Its prototype medians were 6,618.6, 6,007.6 and 8,631.0 Put/s at 16, 64 and 256 clients, with leader sync densities 3.766, 2.419 and 2.523. The final run uses new VMs. It is not a controlled comparison of the two prototype versions, so neither the 64-client increase nor the 256-client decrease can be attributed to the inbox correction.

The remaining cause of small high-concurrency leader groups is not established. Scheduler timing, reply-driven arrivals and shared-disk timing are candidates to measure, not explanations proved here. No ablation isolates hard-state coalescing from Ready gating. No maximum throughput, 1 to 10 ms network-delay result, follower-client workload, production Kubernetes result, physical power-loss durability or full changed-driver crash campaign was measured.

The later worker should take the persistence ordering and explicit deferred-Submit rule from these facts, not treat this driver-only patch as the finished design. It needs a real byte bound and tests of control-message progress during long disk stalls. The changed scheduler must run in the deterministic simulator. The full fault matrix is in `audit/contracts.md`.
