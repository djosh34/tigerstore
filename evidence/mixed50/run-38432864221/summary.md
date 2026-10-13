| Benchmark | Store | Mode | Ops/s | p50 ms | p90 ms | p99 ms | p99.9 ms | Mean ms | Max ms | Server CPU us/op | Generator CPU us/op | Runs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mixed-k8s | etcd | rate | 502 / 502 / 505 | 1.73 / 1.77 / 1.87 | 3.99 / 4.06 / 4.93 | 18.94 / 20.25 / 24.97 | 26.67 / 30.87 / 43.12 | 2.52 / 2.63 / 3.18 | 31.66 / 43.33 / 53.60 | 1601.7 / 1657.0 / 1701.8 | 154.0 / 167.6 / 168.5 | 5 |
| mixed-k8s | baseline | rate | 502 / 502 / 505 | 1.19 / 1.22 / 1.26 | 1.88 / 1.92 / 1.97 | 2.58 / 2.68 / 7.36 | 3.15 / 6.05 / 21.43 | 1.24 / 1.30 / 1.42 | 3.67 / 15.99 / 28.39 | 813.9 / 842.5 / 887.4 | 131.1 / 138.1 / 153.1 | 5 |
| mixed-k8s | candidate | rate | 501 / 502 / 505 | 1.17 / 1.21 / 1.25 | 1.83 / 1.89 / 1.99 | 2.55 / 2.88 / 9.99 | 2.96 / 17.50 / 25.89 | 1.24 / 1.31 / 1.52 | 3.53 / 21.22 / 38.17 | 790.2 / 833.7 / 889.7 | 134.0 / 146.6 / 153.1 | 5 |

Statistics are min / median / max over the repetitions, not pooled percentiles. Runs is the repetition count. At max every stream is busy. Open loop uses half of etcd's median max rate, or a fixed rate for rate-only mixed-k8s. Latency starts at the scheduled arrival time.

| Watch benchmark | Store | Mode | Events/s | p50 ms | p90 ms | p99 ms | p99.9 ms | Mean ms | Max ms | Server CPU us/event | Runs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| mixed-k8s | etcd | rate | 326 / 327 / 329 | 1.70 / 1.81 / 1.87 | 3.51 / 3.81 / 4.21 | 18.94 / 20.12 / 22.22 | 25.62 / 28.51 / 35.00 | 2.52 / 2.69 / 2.85 | 29.23 / 39.50 / 43.83 | 2467.1 / 2528.9 / 2612.3 | 5 |
| mixed-k8s | baseline | rate | 326 / 327 / 329 | 1.27 / 1.35 / 1.38 | 1.81 / 1.88 / 1.94 | 2.35 / 2.60 / 7.85 | 2.79 / 13.07 / 19.60 | 1.32 / 1.41 / 1.55 | 3.68 / 16.37 / 24.46 | 1254.2 / 1288.2 / 1363.5 | 5 |
| mixed-k8s | candidate | rate | 326 / 327 / 329 | 1.23 / 1.33 / 1.41 | 1.75 / 1.88 / 1.94 | 2.35 / 2.68 / 11.89 | 2.91 / 17.50 / 25.23 | 1.36 / 1.45 / 1.66 | 3.29 / 20.59 / 25.90 | 1216.3 / 1284.7 / 1365.6 | 5 |

Event latency runs from the mutation's scheduled send time to the event's arrival at the watcher.

| Benchmark | Store | Mode | Server cores per node | Offered ops/s | Unsent | Runs |
|---|---|---|---|---|---|---|
| mixed-k8s | etcd | rate | 0.29 / 0.30 / 0.31; 0.26 / 0.27 / 0.28; 0.24 / 0.26 / 0.27 | 500 / 500 / 500 | 0 / 0 / 0 | 5 |
| mixed-k8s | baseline | rate | 0.15 / 0.16 / 0.17; 0.14 / 0.14 / 0.15; 0.12 / 0.13 / 0.13 | 500 / 500 / 500 | 0 / 0 / 0 | 5 |
| mixed-k8s | candidate | rate | 0.15 / 0.15 / 0.19; 0.14 / 0.14 / 0.15; 0.11 / 0.12 / 0.13 | 500 / 500 / 500 | 0 / 0 / 0 | 5 |
