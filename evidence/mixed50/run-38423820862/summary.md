| Benchmark | Store | Mode | Ops/s | p50 ms | p90 ms | p99 ms | p99.9 ms | Mean ms | Max ms | Server CPU us/op | Generator CPU us/op | Runs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| mixed-k8s | etcd | rate | 502 / 503 / 505 | 1.63 / 1.75 / 1.81 | 3.71 / 4.31 / 8.49 | 16.42 / 26.80 / 42.60 | 23.92 / 38.93 / 109.84 | 2.35 / 3.23 / 3.73 | 29.43 / 52.77 / 131.60 | 1404.0 / 1543.5 / 1551.9 | 147.5 / 165.7 / 167.8 | 5 |
| mixed-k8s | baseline | rate | 502 / 503 / 505 | 1.18 / 1.20 / 1.26 | 1.86 / 2.03 / 2.14 | 2.65 / 6.77 / 65.67 | 5.88 / 17.76 / 144.18 | 1.25 / 1.47 / 2.82 | 9.32 / 24.40 / 152.39 | 764.5 / 809.4 / 952.2 | 132.7 / 143.9 / 151.4 | 5 |
| mixed-k8s | candidate | rate | 502 / 503 / 505 | 1.14 / 1.19 / 1.20 | 1.79 / 1.84 / 1.92 | 2.53 / 2.60 / 9.47 | 3.25 / 8.24 / 55.97 | 1.22 / 1.25 / 1.52 | 4.32 / 12.00 / 70.34 | 748.7 / 787.9 / 801.4 | 129.9 / 130.5 / 143.5 | 5 |

Statistics are min / median / max over the repetitions, not pooled percentiles. Runs is the repetition count. At max every stream is busy. Open loop uses half of etcd's median max rate, or a fixed rate for rate-only mixed-k8s. Latency starts at the scheduled arrival time.

| Watch benchmark | Store | Mode | Events/s | p50 ms | p90 ms | p99 ms | p99.9 ms | Mean ms | Max ms | Server CPU us/event | Runs |
|---|---|---|---|---|---|---|---|---|---|---|---|
| mixed-k8s | etcd | rate | 324 / 328 / 329 | 1.65 / 1.82 / 1.90 | 2.83 / 4.57 / 10.85 | 15.63 / 29.56 / 45.22 | 23.13 / 35.52 / 111.41 | 2.18 / 3.50 / 3.87 | 28.11 / 45.62 / 132.72 | 2185.6 / 2364.6 / 2385.3 | 5 |
| mixed-k8s | baseline | rate | 324 / 328 / 329 | 1.33 / 1.37 / 1.41 | 1.86 / 2.02 / 2.32 | 2.47 / 7.29 / 77.33 | 7.95 / 18.94 / 147.32 | 1.38 / 1.63 / 3.74 | 9.39 / 24.34 / 151.17 | 1190.3 / 1238.9 / 1471.2 | 5 |
| mixed-k8s | candidate | rate | 324 / 328 / 329 | 1.27 / 1.29 / 1.33 | 1.78 / 1.81 / 1.86 | 2.40 / 2.48 / 11.04 | 2.89 / 9.01 / 66.45 | 1.34 / 1.37 / 1.74 | 3.69 / 11.88 / 71.69 | 1165.2 / 1208.8 / 1226.7 | 5 |

Event latency runs from the mutation's scheduled send time to the event's arrival at the watcher.

| Benchmark | Store | Mode | Server cores per node | Offered ops/s | Unsent | Runs |
|---|---|---|---|---|---|---|
| mixed-k8s | etcd | rate | 0.23 / 0.28 / 0.29; 0.25 / 0.25 / 0.26; 0.20 / 0.24 / 0.25 | 500 / 500 / 500 | 0 / 0 / 0 | 5 |
| mixed-k8s | baseline | rate | 0.14 / 0.15 / 0.22; 0.13 / 0.14 / 0.14; 0.11 / 0.12 / 0.12 | 500 / 500 / 500 | 0 / 0 / 0 | 5 |
| mixed-k8s | candidate | rate | 0.14 / 0.15 / 0.16; 0.12 / 0.13 / 0.13; 0.11 / 0.11 / 0.12 | 500 / 500 / 500 | 0 / 0 / 0 | 5 |
