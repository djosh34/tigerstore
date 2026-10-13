## Unprofiled baseline

| Clients | Store | Put/s | Mean ms | p50 ms | p90 ms | p99 ms | p99.9 ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | Main | 5387.2 | 2.969 | 2.813 | 3.582 | 5.485 | 11.828 |
| 16 | Etcd | 3501.9 | 4.568 | 4.292 | 6.020 | 10.967 | 15.814 |
| 64 | Main | 8959.7 | 7.139 | 6.781 | 9.235 | 15.447 | 23.925 |
| 64 | Etcd | 5205.3 | 12.290 | 11.538 | 17.446 | 24.013 | 31.379 |
| 256 | Main | 9433.2 | 27.085 | 25.581 | 38.723 | 56.960 | 74.708 |
| 256 | Etcd | 6005.1 | 42.585 | 40.694 | 57.901 | 75.681 | 98.478 |

All triples below are leader / follower 1 / follower 2.

| Clients | Store | Server CPU us/Put by node | Generator CPU us/Put | Puts/file sync by node |
| ---: | --- | --- | ---: | --- |
| 16 | Main | 167.167 / 46.167 / 38.833 | 71.167 | 4.405 / 3.975 / 4.018 |
| 16 | Etcd | 265.167 / 85.667 / 75.833 | 78.833 | 4.570 / 4.746 / 4.635 |
| 64 | Main | 108.167 / 17.000 / 15.667 | 52.000 | 16.004 / 14.444 / 14.444 |
| 64 | Etcd | 185.833 / 43.667 / 41.500 | 62.500 | 10.502 / 15.970 / 15.621 |
| 256 | Main | 105.833 / 9.833 / 9.000 | 48.000 | 9.176 / 41.068 / 40.761 |
| 256 | Etcd | 163.167 / 30.000 / 29.167 | 54.667 | 15.500 / 46.765 / 47.356 |

| Clients | Store | Allocated objects/Put by node | Allocated bytes/Put by node |
| ---: | --- | --- | --- |
| 16 | Main | 159.82 / 18.02 / 17.97 | 21473.0 / 3194.8 / 3193.2 |
| 16 | Etcd | 298.68 / 105.18 / 105.77 | 36079.2 / 16333.3 / 16276.1 |
| 64 | Main | 143.69 / 10.13 / 10.13 | 21941.8 / 2940.1 / 2942.3 |
| 64 | Etcd | 274.97 / 90.21 / 90.89 | 35446.2 / 15635.7 / 15729.1 |
| 256 | Main | 147.62 / 11.09 / 11.10 | 21748.2 / 2961.1 / 2961.6 |
| 256 | Etcd | 264.16 / 81.35 / 81.50 | 34685.8 / 14739.6 / 14671.4 |

## Leader CPU profiles

| Inclusive stack cost, CPU us/Put | 16 clients | 64 clients | 256 clients |
| --- | ---: | ---: | ---: |
| Owner loop | 15.202 | 5.913 | 5.098 |
| Core under owner | 12.162 | 4.410 | 4.170 |
| Entry encode/decode | 6.321 | 2.369 | 2.296 |
| Protobuf outside frontend | 5.321 | 2.022 | 2.009 |
| Raft | 3.681 | 0.867 | 1.080 |
| WAL | 21.283 | 4.199 | 4.963 |
| File Sync under WAL | 16.202 | 2.504 | 3.359 |
| MVCC/index | 3.120 | 1.021 | 0.827 |
| Allocation and GC outside frontend | 6.121 | 3.313 | 2.954 |
| Peers including peer TLS | 19.123 | 4.834 | 3.545 |
| Scheduler outside frontend | 15.402 | 7.665 | 6.229 |

These rows overlap. Entry codec, Raft and index costs are inside the owner loop. File Sync is inside WAL. Allocators, protobuf and syscalls overlap those paths. Do not add them.

| Profile cohort | 16 clients | 64 clients | 256 clients |
| --- | ---: | ---: | ---: |
| Puts across five profiles | 249,964 | 519,217 | 592,362 |
| Sampled leader CPU us/Put | 198.029 | 102.096 | 95.077 |
| CPU us/Put outside frontend stacks | 74.891 | 24.884 | 21.794 |
| Outside frontend sample percent | 37.82 | 24.37 | 22.92 |

## Separate profile controls

Do not use these as headline performance. Main is CPU-profiled; stock etcd is not. These jobs use 12-second arrival windows and different VMs from the baseline. They do not isolate profiler overhead.

| Clients | Store | Put/s | Mean ms | p50 ms | p90 ms | p99 ms | p99.9 ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 16 | Main | 4216.6 | 3.792 | 3.686 | 4.646 | 6.975 | 12.610 |
| 16 | Etcd | 2957.9 | 5.407 | 5.191 | 7.063 | 12.270 | 16.544 |
| 64 | Main | 9277.2 | 6.897 | 6.542 | 8.862 | 15.418 | 27.363 |
| 64 | Etcd | 5350.4 | 11.957 | 11.189 | 17.044 | 23.443 | 27.992 |
| 256 | Main | 10391.4 | 24.619 | 23.271 | 35.134 | 52.465 | 70.530 |
| 256 | Etcd | 6586.0 | 38.842 | 37.764 | 53.190 | 67.830 | 81.456 |

All triples below are leader / follower 1 / follower 2.

| Clients | Store | Server CPU us/Put by node | Generator CPU us/Put | Puts/file sync by node |
| ---: | --- | --- | ---: | --- |
| 16 | Main | 204.635 / 65.137 / 63.984 | 83.775 | 4.450 / 4.004 / 4.054 |
| 16 | Etcd | 304.722 / 105.056 / 105.357 | 90.754 | 4.537 / 4.687 / 4.663 |
| 64 | Main | 103.567 / 16.918 / 15.625 | 49.501 | 16.208 / 14.551 / 14.582 |
| 64 | Etcd | 180.598 / 44.682 / 42.347 | 60.138 | 10.857 / 16.060 / 15.682 |
| 256 | Main | 95.835 / 8.889 / 7.840 | 42.266 | 11.503 / 41.888 / 41.720 |
| 256 | Etcd | 151.353 / 29.035 / 26.763 | 48.347 | 15.420 / 46.461 / 46.956 |

| Clients | Store | Allocated objects/Put by node | Allocated bytes/Put by node |
| ---: | --- | --- | --- |
| 16 | Main | 159.52 / 17.91 / 17.86 | 21516.3 / 3208.8 / 3206.7 |
| 16 | Etcd | 298.29 / 104.32 / 104.60 | 35822.6 / 15994.6 / 16029.9 |
| 64 | Main | 143.54 / 10.10 / 10.10 | 21852.3 / 2836.2 / 2835.4 |
| 64 | Etcd | 274.65 / 90.68 / 90.41 | 35426.3 / 15673.2 / 15588.8 |
| 256 | Main | 145.40 / 10.34 / 10.34 | 21762.3 / 2946.2 / 2943.6 |
| 256 | Etcd | 264.13 / 81.17 / 81.32 | 34552.0 / 14541.1 / 14560.9 |
