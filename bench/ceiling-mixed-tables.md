# Mixed #484 numerical evidence

All spreads are min / median / max; not pooled latency percentiles. Cluster cells have 6 observations (3 per runner); diagnostics 2 (1 per runner); microbenchmarks 3 per runner. CPU projections are extrapolations, NOT measured maximum throughput. IP counters include headers/ACKs; loopback OUTPUT counts each transmitted packet once.

## Netem verification
Each file contains the median of 20 TCP connects: delayed peer port 20021, undelayed client port 20020.

| run | runner | RTT-streams | peer median ms | client median ms |
|---|---|---|---|---|
| 42019370875 | ceiling-mixed-1 | 0-48 | 0.020 | 0.019 |
| 42019370875 | ceiling-mixed-1 | 10-48 | 10.070 | 0.018 |
| 42019370875 | ceiling-mixed-1 | 10-96 | 10.069 | 0.019 |
| 42019370875 | ceiling-mixed-1 | 2-48 | 2.053 | 0.019 |
| 42019370875 | ceiling-mixed-2 | 0-48 | 0.019 | 0.018 |
| 42019370875 | ceiling-mixed-2 | 10-48 | 10.099 | 0.019 |
| 42019370875 | ceiling-mixed-2 | 10-96 | 10.076 | 0.018 |
| 42019370875 | ceiling-mixed-2 | 2-48 | 2.061 | 0.020 |
| 42023145903 | ceiling-mixed-1 | 0-48 | 0.020 | 0.018 |
| 42023145903 | ceiling-mixed-1 | 10-48 | 10.073 | 0.019 |
| 42023145903 | ceiling-mixed-1 | 2-48 | 2.053 | 0.019 |
| 42023145903 | ceiling-mixed-2 | 0-48 | 0.018 | 0.017 |
| 42023145903 | ceiling-mixed-2 | 10-48 | 10.063 | 0.018 |
| 42023145903 | ceiling-mixed-2 | 2-48 | 2.055 | 0.018 |

## Cluster measurements, run 42019370875

| metric | RTT 0, 48 streams/conn | RTT 2, 48 streams/conn | RTT 10, 48 streams/conn | RTT 10, 96 streams/conn |
|---|---|---|---|---|
| throughput ops/s | 7870 / 8411 / 8800 | 7097 / 8332 / 8714 | 6306 / 7037 / 7399 | 7952 / 8706 / 9188 |
| leader cores | 0.8779 / 0.9062 / 0.932 | 0.8609 / 0.9043 / 0.921 | 0.7779 / 0.7924 / 0.8229 | 0.846 / 0.879 / 0.8899 |
| follower1 cores | 0.579 / 0.6394 / 0.6722 | 0.5888 / 0.616 / 0.649 | 0.5369 / 0.5525 / 0.576 | 0.534 / 0.565 / 0.601 |
| follower2 cores | 0.6379 / 0.657 / 0.702 | 0.587 / 0.6329 / 0.6811 | 0.526 / 0.5815 / 0.617 | 0.541 / 0.602 / 0.62 |
| generator cores | 0.5763 / 0.5851 / 0.6074 | 0.5549 / 0.5756 / 0.6039 | 0.4778 / 0.5052 / 0.538 | 0.5339 / 0.5596 / 0.5817 |
| leader CPU us/op | 102.6 / 107.5 / 115.2 | 99.04 / 108.7 / 127.7 | 105.8 / 112.6 / 130.5 | 94.37 / 101.4 / 110.9 |
| follower1 CPU us/op | 65.8 / 76.8 / 84.09 | 69.16 / 73.66 / 86.51 | 72.58 / 79.22 / 86.74 | 59.75 / 66.53 / 70.85 |
| follower2 CPU us/op | 72.7 / 80.02 / 84.37 | 69.28 / 77.95 / 85.52 | 75.62 / 82.32 / 90.82 | 60.54 / 69.31 / 76.59 |
| server sum CPU us/op | 245.6 / 265 / 282.1 | 244.9 / 257.9 / 299.7 | 257.4 / 272.6 / 300.7 | 219.8 / 236.8 / 256.4 |
| generator CPU us/op | 66.68 / 69.63 / 74.13 | 68.3 / 69.27 / 78.2 | 70.43 / 72.24 / 75.76 | 63.31 / 64.36 / 67.14 |
| disk writes B/op | 3586 / 3670 / 3743 | 3751 / 3801 / 3830 | 4097 / 4269 / 4341 | 3254 / 3339 / 3460 |
| peer IP B/op | 2330 / 2358 / 2369 | 2316 / 2330 / 2349 | 2270 / 2290 / 2320 | 2242 / 2291 / 2323 |
| client IP B/op | 3.197e+04 / 3.269e+04 / 3.289e+04 | 3.225e+04 / 3.267e+04 / 3.299e+04 | 3.236e+04 / 3.263e+04 / 3.306e+04 | 3.128e+04 / 3.161e+04 / 3.198e+04 |
| unary request protobuf B/op | 898.7 / 902.9 / 906.8 | 894.1 / 902.8 / 906.7 | 886.3 / 898.2 / 903.6 | 887.6 / 896.7 / 903.7 |
| unary reply protobuf B/op | 2.94e+04 / 2.988e+04 / 3.011e+04 | 2.952e+04 / 2.976e+04 / 3.006e+04 | 2.964e+04 / 2.974e+04 / 3.02e+04 | 2.842e+04 / 2.88e+04 / 2.919e+04 |
| watch protobuf B/op | 1610 / 1615 / 1621 | 1595 / 1617 / 1635 | 1595 / 1612 / 1627 | 1593 / 1613 / 1619 |
| watch events/op | 0.647 / 0.6478 / 0.6492 | 0.6471 / 0.6483 / 0.6503 | 0.6469 / 0.6485 / 0.6491 | 0.6456 / 0.6478 / 0.6555 |
| mean latency ms | 31.69 / 33.2 / 35.51 | 32.09 / 33.62 / 39.5 | 38.03 / 40.01 / 44.81 | 60.99 / 64.48 / 70.36 |
| occupancy (Little law) | 278.9 / 279.3 / 280.3 | 279 / 279.9 / 280.4 | 281.3 / 281.7 / 282.6 | 559.5 / 560.3 / 561.8 |
| generator one-core projection ops/s | 1.349e+04 / 1.436e+04 / 1.5e+04 | 1.279e+04 / 1.444e+04 / 1.464e+04 | 1.32e+04 / 1.384e+04 / 1.42e+04 | 1.49e+04 / 1.554e+04 / 1.58e+04 |
| leader one-core projection ops/s | 8677 / 9303 / 9751 | 7834 / 9206 / 1.01e+04 | 7663 / 8889 / 9448 | 9016 / 9868 / 1.06e+04 |
| three-server-core projection ops/s | 1.064e+04 / 1.133e+04 / 1.222e+04 | 1.001e+04 / 1.164e+04 / 1.225e+04 | 9978 / 1.101e+04 / 1.165e+04 | 1.17e+04 / 1.268e+04 / 1.365e+04 |
| write-only disk projection at 343 MiB/s | 9.609e+04 / 9.801e+04 / 1.003e+05 | 9.39e+04 / 9.462e+04 / 9.588e+04 | 8.284e+04 / 8.426e+04 / 8.778e+04 | 1.039e+05 / 1.077e+05 / 1.105e+05 |

| runner | RTT | streams/conn | throughput ops/s |
|---|---|---|---|
| ceiling-mixed-1 | 0 | 48 | 8403 / 8776 / 8800 |
| ceiling-mixed-1 | 2 | 48 | 8516 / 8694 / 8714 |
| ceiling-mixed-1 | 10 | 48 | 7104 / 7388 / 7399 |
| ceiling-mixed-1 | 10 | 96 | 8930 / 8937 / 9188 |
| ceiling-mixed-2 | 0 | 48 | 7870 / 7992 / 8419 |
| ceiling-mixed-2 | 2 | 48 | 7097 / 8029 / 8147 |
| ceiling-mixed-2 | 10 | 48 | 6306 / 6793 / 6969 |
| ceiling-mixed-2 | 10 | 96 | 7952 / 8268 / 8482 |

## Per-operation measurements, run 42019370875
Protobuf body bytes, excluding gRPC/HTTP2/TLS/TCP/IP. LIST keys are returned keys, not Count before the 500-key limit.

| RTT | streams | op | request B | reply B | LIST keys | mean latency ms |
|---|---|---|---|---|---|---|
| 0 | 48 | get | 54.52 / 54.58 / 54.58 | 1374 / 1407 / 1417 | 0 / 0 / 0 | 29.17 / 29.99 / 32.38 |
| 0 | 48 | list | 74.32 / 74.36 / 74.41 | 2.902e+05 / 2.912e+05 / 2.922e+05 | 307.8 / 309.1 / 312.4 | 32.28 / 33.55 / 36.28 |
| 0 | 48 | update | 1584 / 1595 / 1607 | 33 / 33 / 33 | 0 / 0 / 0 | 32.47 / 34.43 / 36.28 |
| 0 | 48 | create | 1525 / 1559 / 1582 | 33 / 33 / 33 | 0 / 0 / 0 | 32.6 / 34.41 / 36.34 |
| 0 | 48 | delete | 120.4 / 120.5 / 120.6 | 35 / 35 / 35 | 0 / 0 / 0 | 32.25 / 34.38 / 36.42 |
| 2 | 48 | get | 54.54 / 54.56 / 54.59 | 1377 / 1388 / 1412 | 0 / 0 / 0 | 29.35 / 31.08 / 36.36 |
| 2 | 48 | list | 74.31 / 74.36 / 74.4 | 2.902e+05 / 2.911e+05 / 2.914e+05 | 307.2 / 308.7 / 311.6 | 32.99 / 34.59 / 40.69 |
| 2 | 48 | update | 1574 / 1597 / 1610 | 33 / 33 / 33 | 0 / 0 / 0 | 32.91 / 34.47 / 40.46 |
| 2 | 48 | create | 1540 / 1552 / 1567 | 33 / 33 / 33 | 0 / 0 / 0 | 33.01 / 34.32 / 40.61 |
| 2 | 48 | delete | 120.5 / 120.6 / 120.6 | 35 / 35 / 35 | 0 / 0 / 0 | 32.76 / 34.45 / 40.38 |
| 10 | 48 | get | 54.54 / 54.56 / 54.58 | 1392 / 1401 / 1412 | 0 / 0 / 0 | 35.62 / 37.61 / 42.03 |
| 10 | 48 | list | 74.3 / 74.38 / 74.42 | 2.876e+05 / 2.895e+05 / 2.931e+05 | 305.6 / 307.1 / 310.2 | 38.96 / 41.33 / 46.14 |
| 10 | 48 | update | 1565 / 1582 / 1606 | 33 / 33 / 33 | 0 / 0 / 0 | 38.86 / 40.63 / 45.73 |
| 10 | 48 | create | 1532 / 1555 / 1575 | 33 / 33 / 33 | 0 / 0 / 0 | 38.52 / 40.7 / 45.77 |
| 10 | 48 | delete | 120.5 / 120.6 / 120.6 | 35 / 35 / 35 | 0 / 0 / 0 | 38.43 / 40.66 / 45.9 |
| 10 | 96 | get | 54.56 / 54.58 / 54.59 | 1383 / 1397 / 1424 | 0 / 0 / 0 | 57.03 / 61.13 / 66.41 |
| 10 | 96 | list | 74.52 / 74.58 / 74.68 | 2.798e+05 / 2.814e+05 / 2.836e+05 | 292.2 / 294.2 / 296.8 | 62.12 / 65.98 / 72.09 |
| 10 | 96 | update | 1567 / 1576 / 1593 | 33 / 33 / 33 | 0 / 0 / 0 | 62.57 / 65.78 / 72.3 |
| 10 | 96 | create | 1541 / 1566 / 1582 | 33 / 33 / 33 | 0 / 0 / 0 | 62.24 / 65.5 / 72.12 |
| 10 | 96 | delete | 120.5 / 120.5 / 120.6 | 35 / 35 / 35 | 0 / 0 / 0 | 62.2 / 65.54 / 71.67 |

## Diagnostic run 42023145903
New runners, not additional repetitions on the original runners. External IO boundaries follow markers with up to ~50 ms polling delay. /proc/pid/io write_bytes is storage IO charged to a process; wchar includes sockets and is deliberately NOT described as WAL bytes.

| metric | RTT 0 | RTT 2 | RTT 10 |
|---|---|---|---|
| throughput ops/s | 9472 / 1.006e+04 / 1.064e+04 | 8672 / 9238 / 9806 | 6987 / 7111 / 7234 |
| leader cores | 0.8724 / 0.9002 / 0.928 | 0.8709 / 0.89 / 0.9091 | 0.775 / 0.792 / 0.8091 |
| follower1 cores | 0.651 / 0.6652 / 0.6793 | 0.6089 / 0.6165 / 0.6241 | 0.532 / 0.5485 / 0.565 |
| follower2 cores | 0.65 / 0.6782 / 0.7063 | 0.6379 / 0.639 / 0.6401 | 0.493 / 0.5355 / 0.578 |
| generator cores | 0.6212 / 0.6513 / 0.6814 | 0.5821 / 0.6123 / 0.6425 | 0.5144 / 0.5156 / 0.5167 |
| leader CPU us/op | 81.93 / 89.95 / 97.97 | 92.7 / 96.57 / 100.4 | 110.9 / 111.4 / 111.8 |
| generator CPU us/op | 63.99 / 64.79 / 65.58 | 65.51 / 66.33 / 67.14 | 71.11 / 72.53 / 73.95 |
| disk writes B/op | 3589 / 3655 / 3720 | 3688 / 3732 / 3776 | 4304 / 4328 / 4352 |
| peer IP B/op | 2322 / 2328 / 2334 | 2315 / 2318 / 2320 | 2297 / 2302 / 2306 |
| client IP B/op | 3.198e+04 / 3.206e+04 / 3.214e+04 | 3.226e+04 / 3.231e+04 / 3.235e+04 | 3.235e+04 / 3.235e+04 / 3.235e+04 |
| device read B/op | 292.2 / 340.9 / 389.6 | 266.8 / 279.7 / 292.6 | 194.9 / 220.7 / 246.5 |
| leader client IP B/op | 1.292e+04 / 1.326e+04 / 1.36e+04 | 1.361e+04 / 1.38e+04 / 1.399e+04 | 1.445e+04 / 1.461e+04 / 1.477e+04 |
| follower1 client IP B/op | 8747 / 9140 / 9534 | 8840 / 8843 / 8847 | 8233 / 8436 / 8638 |
| follower2 client IP B/op | 9532 / 9665 / 9797 | 9520 / 9669 / 9817 | 9260 / 9302 / 9345 |
| leader charged storage write B/op | 954 / 955.5 / 957.1 | 955.1 / 962.3 / 969.4 | 989.7 / 990 / 990.3 |
| follower1 charged storage write B/op | 927.8 / 928.4 / 929 | 931.8 / 937.1 / 942.5 | 962.2 / 963.1 / 964.1 |
| follower2 charged storage write B/op | 926.3 / 928.3 / 930.2 | 930.8 / 935.7 / 940.6 | 960 / 962.2 / 964.4 |

## Exclusive profile attribution, run 42019370875
Each sample is assigned to the first matching category in ceiling-mixed-analyze.py. GC means background GC only; assists stay charged to callers. TLS includes record handling and IO, not just AES. Shares from 9 s server profiles multiply the full 10 s window CPU/op.

### leader

| category | RTT0 % | RTT0 us/op | RTT10 % | RTT10 us/op |
|---|---|---|---|---|
| GC background | 28.38 / 28.47 / 28.57 | 31.18 / 31.66 / 32.13 | 28.05 / 29.06 / 30.07 | 31.83 / 34.22 / 36.61 |
| HTTP2 read/write | 2.442 / 2.708 / 2.974 | 2.665 / 3.016 / 3.368 | 1.277 / 1.578 / 1.879 | 1.351 / 1.902 / 2.452 |
| Raft | 1.099 / 1.107 / 1.115 | 1.199 / 1.231 / 1.263 | 0.7092 / 1.093 / 1.477 | 0.7507 / 1.339 / 1.927 |
| TLS incl IO | 12.39 / 12.54 / 12.7 | 13.86 / 13.94 / 14.03 | 14.23 / 14.99 / 15.74 | 16.66 / 17.62 / 18.57 |
| WAL persist | 2.107 / 2.335 / 2.564 | 2.385 / 2.592 / 2.798 | 2.953 / 4.384 / 5.816 | 3.854 / 5.005 / 6.155 |
| apply | 6.444 / 6.457 / 6.471 | 7.062 / 7.179 / 7.297 | 5.106 / 5.439 / 5.772 | 5.405 / 6.469 / 7.532 |
| client admit/decode | 0.2442 / 0.5558 / 0.8674 | 0.2665 / 0.6244 / 0.9823 | 0.4027 / 0.485 / 0.5674 | 0.5255 / 0.563 / 0.6005 |
| other | 3.907 / 3.936 / 3.965 | 4.264 / 4.377 / 4.49 | 4.255 / 5.483 / 6.711 | 4.504 / 6.631 / 8.759 |
| other reply encode | 0.1221 / 0.3708 / 0.6196 | 0.1332 / 0.4174 / 0.7016 | 0.5369 / 0.6231 / 0.7092 | 0.7007 / 0.7257 / 0.7507 |
| peer transport | 0.9768 / 1.17 / 1.363 | 1.066 / 1.305 / 1.544 | 1.135 / 1.171 / 1.208 | 1.201 / 1.389 / 1.577 |
| range/list encode | 10.74 / 12 / 13.26 | 11.73 / 13.37 / 15.01 | 8.591 / 9.402 / 10.21 | 10.81 / 11.01 / 11.21 |
| range/list index/read | 26.02 / 27.79 / 29.55 | 29.47 / 30.86 / 32.24 | 24.11 / 25.41 / 26.71 | 25.52 / 30.19 / 34.86 |
| watch fanout/encode | 0.4957 / 0.5531 / 0.6105 | 0.5613 / 0.6138 / 0.6662 | 0.2837 / 0.8801 / 1.477 | 0.3003 / 1.114 / 1.927 |

### follower

| category | RTT0 % | RTT0 us/op | RTT10 % | RTT10 us/op |
|---|---|---|---|---|
| GC background | 25.22 / 27.83 / 30.44 | 19.89 / 22.75 / 25.6 | 28.98 / 29.75 / 30.53 | 23.06 / 24.1 / 25.14 |
| HTTP2 read/write | 2.041 / 2.575 / 3.109 | 1.716 / 2.084 / 2.453 | 1.633 / 2.29 / 2.947 | 1.416 / 1.821 / 2.226 |
| Raft | 0.3454 / 0.5128 / 0.6803 | 0.2725 / 0.4223 / 0.572 | 0.6122 / 0.7272 / 0.8421 | 0.5311 / 0.5835 / 0.636 |
| TLS incl IO | 13.44 / 13.89 / 14.34 | 11.3 / 11.3 / 11.31 | 13.89 / 14.29 / 14.69 | 10.49 / 11.62 / 12.75 |
| WAL persist | 1.19 / 1.632 / 2.073 | 1.001 / 1.318 / 1.635 | 3.061 / 3.11 / 3.158 | 2.385 / 2.52 / 2.655 |
| apply | 8.981 / 9.167 / 9.354 | 7.086 / 7.476 / 7.865 | 8.98 / 10.38 / 11.79 | 7.789 / 8.346 / 8.904 |
| client admit/decode | 0.3401 / 0.4291 / 0.5181 | 0.286 / 0.3474 / 0.4088 | 0.2105 / 0.4114 / 0.6122 | 0.159 / 0.345 / 0.5311 |
| other | 4.422 / 5.838 / 7.254 | 3.718 / 4.721 / 5.723 | 3.061 / 4.373 / 5.684 | 2.655 / 3.474 / 4.293 |
| other reply encode | 0.3401 / 0.3428 / 0.3454 | 0.2725 / 0.2793 / 0.286 | 0.2105 / 0.3093 / 0.4082 | 0.159 / 0.2565 / 0.354 |
| peer transport | 0.6803 / 0.8583 / 1.036 | 0.572 / 0.6948 / 0.8176 | 0.8421 / 1.135 / 1.429 | 0.636 / 0.9376 / 1.239 |
| range/list encode | 10.54 / 11.22 / 11.9 | 8.312 / 9.161 / 10.01 | 8.211 / 8.799 / 9.388 | 6.201 / 7.172 / 8.143 |
| range/list index/read | 24.83 / 24.94 / 25.04 | 19.76 / 20.32 / 20.88 | 20.84 / 23.58 / 26.33 | 15.74 / 19.29 / 22.84 |
| watch fanout/encode | 0.3401 / 0.7746 / 1.209 | 0.286 / 0.6199 / 0.9539 | 0.8163 / 0.8292 / 0.8421 | 0.636 / 0.672 / 0.7081 |

### generator

| category | RTT0 % | RTT0 us/op | RTT10 % | RTT10 us/op |
|---|---|---|---|---|
| GC background | 5.458 / 6.608 / 7.759 | 3.934 / 4.676 / 5.418 | 6.624 / 7.479 / 8.333 | 5.019 / 5.543 / 6.067 |
| GET/LIST KV validation | 29.48 / 30.32 / 31.16 | 20.59 / 21.53 / 22.46 | 27.35 / 28.92 / 30.49 | 20.72 / 21.46 / 22.2 |
| HTTP2/control | 6.866 / 6.881 / 6.897 | 4.816 / 4.883 / 4.95 | 7.265 / 7.61 / 7.955 | 5.504 / 5.648 / 5.791 |
| LIST sort / other unary validation | 11.97 / 12.8 / 13.62 | 8.63 / 9.071 / 9.511 | 13.83 / 15.67 / 17.52 | 10.07 / 11.67 / 13.27 |
| TLS incl IO | 30 / 30.23 / 30.46 | 20.95 / 21.45 / 21.96 | 26.89 / 28.94 / 30.98 | 19.58 / 21.53 / 23.47 |
| choose/build requests | 4.93 / 4.965 / 5 | 3.491 / 3.522 / 3.554 | 4.545 / 4.623 / 4.701 | 3.309 / 3.435 / 3.562 |
| other | 1.034 / 1.398 / 1.761 | 0.7224 / 0.9957 / 1.269 | 1.282 / 1.493 / 1.705 | 0.9713 / 1.106 / 1.241 |
| watch validation/hash | 6.207 / 6.801 / 7.394 | 4.334 / 4.832 / 5.33 | 4.274 / 5.262 / 6.25 | 3.238 / 3.894 / 4.55 |

## Exclusive profile attribution, run 42023145903
Each sample is assigned to the first matching category in ceiling-mixed-analyze.py. GC means background GC only; assists stay charged to callers. TLS includes record handling and IO, not just AES. Shares from 9 s server profiles multiply the full 10 s window CPU/op.

### leader

| category | RTT0 % | RTT0 us/op | RTT10 % | RTT10 us/op |
|---|---|---|---|---|
| GC background | 27.7 / 28.8 / 29.9 | 22.69 / 26 / 29.3 | 26.76 / 28.15 / 29.55 | 29.93 / 31.35 / 32.77 |
| HTTP2 read/write | 2.179 / 2.233 / 2.287 | 1.874 / 2.004 / 2.135 | 2.254 / 2.618 / 2.983 | 2.52 / 2.914 / 3.309 |
| Raft | 0.7624 / 0.9865 / 1.211 | 0.6246 / 0.9054 / 1.186 | 0.2817 / 0.3539 / 0.4261 | 0.315 / 0.3938 / 0.4727 |
| TLS incl IO | 13.6 / 13.88 / 14.16 | 11.14 / 12.51 / 13.88 | 15.77 / 15.91 / 16.06 | 17.49 / 17.72 / 17.96 |
| WAL persist | 2.3 / 2.548 / 2.795 | 2.254 / 2.272 / 2.29 | 2.415 / 3.109 / 3.803 | 2.679 / 3.466 / 4.253 |
| apply | 6.053 / 6.839 / 7.624 | 5.931 / 6.088 / 6.246 | 5.966 / 6.293 / 6.62 | 6.618 / 7.01 / 7.403 |
| client admit/decode | 0.5083 / 0.6173 / 0.7264 | 0.4164 / 0.564 / 0.7117 | 0.2817 / 0.2829 / 0.2841 | 0.315 / 0.3151 / 0.3151 |
| other | 3.511 / 3.725 / 3.939 | 3.227 / 3.334 / 3.44 | 5.398 / 5.797 / 6.197 | 5.987 / 6.459 / 6.93 |
| other reply encode | 0.3812 / 0.4327 / 0.4843 | 0.3123 / 0.3934 / 0.4745 | 0.7102 / 1.059 / 1.408 | 0.7878 / 1.181 / 1.575 |
| peer transport | 0.9685 / 1.183 / 1.398 | 0.9489 / 1.047 / 1.145 | 1.408 / 1.628 / 1.847 | 1.575 / 1.812 / 2.048 |
| range/list encode | 11.18 / 11.58 / 11.99 | 9.161 / 10.45 / 11.74 | 7.955 / 8.343 / 8.732 | 8.823 / 9.294 / 9.765 |
| range/list index/read | 25.91 / 26.55 / 27.19 | 22.28 / 23.83 / 25.38 | 25.07 / 25.46 / 25.85 | 28.04 / 28.36 / 28.68 |
| watch fanout/encode | 0.6053 / 0.6203 / 0.6353 | 0.5205 / 0.5568 / 0.5931 | 0.8523 / 0.9895 / 1.127 | 0.9454 / 1.103 / 1.26 |

### follower

| category | RTT0 % | RTT0 us/op | RTT10 % | RTT10 us/op |
|---|---|---|---|---|
| GC background | 24.27 / 26.11 / 27.94 | 16.68 / 17.25 / 17.83 | 26.27 / 27.62 / 28.98 | 19.32 / 21.38 / 23.43 |
| HTTP2 read/write | 2.19 / 2.239 / 2.288 | 1.459 / 1.482 / 1.505 | 1.525 / 1.756 / 1.987 | 1.233 / 1.347 / 1.461 |
| Raft | 0.4902 / 0.5188 / 0.5474 | 0.3127 / 0.3445 / 0.3763 | 0.6623 / 0.9847 / 1.307 | 0.487 / 0.772 / 1.057 |
| TLS incl IO | 16.83 / 17.36 / 17.88 | 10.74 / 11.51 / 12.29 | 15.03 / 16.24 / 17.44 | 12.16 / 12.49 / 12.82 |
| WAL persist | 1.797 / 2.359 / 2.92 | 1.147 / 1.577 / 2.007 | 2.208 / 2.956 / 3.704 | 1.623 / 2.309 / 2.995 |
| apply | 8.759 / 9.2 / 9.641 | 6.02 / 6.085 / 6.15 | 10.46 / 10.75 / 11.04 | 8.117 / 8.287 / 8.457 |
| client admit/decode | 0 / 0.09124 / 0.1825 | 0 / 0.06271 / 0.1254 | 0.4357 / 0.8801 / 1.325 | 0.3524 / 0.6632 / 0.974 |
| other | 3.65 / 4.521 / 5.392 | 2.508 / 2.974 / 3.44 | 5.011 / 5.044 / 5.077 | 3.734 / 3.893 / 4.052 |
| other reply encode | 0.1825 / 0.2546 / 0.3268 | 0.1254 / 0.167 / 0.2085 | 0.2179 / 0.2193 / 0.2208 | 0.1623 / 0.1693 / 0.1762 |
| peer transport | 0.9804 / 1.129 / 1.277 | 0.6255 / 0.7517 / 0.8779 | 0.6536 / 1.099 / 1.545 | 0.5285 / 0.8324 / 1.136 |
| range/list encode | 7.664 / 7.754 / 7.843 | 5.004 / 5.136 / 5.268 | 9.051 / 9.863 / 10.68 | 6.656 / 7.644 / 8.633 |
| range/list index/read | 25.82 / 27.6 / 29.38 | 16.47 / 18.33 / 20.19 | 21.35 / 21.71 / 22.08 | 16.23 / 16.75 / 17.27 |
| watch fanout/encode | 0.6536 / 0.8742 / 1.095 | 0.417 / 0.5847 / 0.7525 | 0.6536 / 0.8787 / 1.104 | 0.5285 / 0.6701 / 0.8117 |

### generator

| category | RTT0 % | RTT0 us/op | RTT10 % | RTT10 us/op |
|---|---|---|---|---|
| GC background | 6.37 / 6.611 / 6.852 | 4.077 / 4.285 / 4.493 | 6.312 / 6.926 / 7.54 | 4.667 / 5.014 / 5.361 |
| GET/LIST KV validation | 32.59 / 32.85 / 33.12 | 20.86 / 21.29 / 21.72 | 25.64 / 28.4 / 31.15 | 18.96 / 20.56 / 22.15 |
| HTTP2/control | 6.074 / 6.708 / 7.341 | 3.887 / 4.351 / 4.814 | 7.341 / 7.418 / 7.495 | 5.22 / 5.381 / 5.542 |
| LIST sort / other unary validation | 9.625 / 11.11 / 12.59 | 6.312 / 7.185 / 8.059 | 12.7 / 13.15 / 13.61 | 9.029 / 9.547 / 10.06 |
| TLS incl IO | 29.78 / 31.2 / 32.63 | 19.06 / 20.23 / 21.4 | 29.17 / 30.36 / 31.56 | 20.74 / 22.04 / 23.34 |
| choose/build requests | 3.259 / 3.506 / 3.752 | 2.086 / 2.273 / 2.461 | 4.96 / 5.537 / 6.114 | 3.527 / 4.024 / 4.521 |
| other | 1.305 / 1.467 / 1.63 | 0.8558 / 0.9494 / 1.043 | 0.9921 / 1.186 / 1.381 | 0.7054 / 0.8632 / 1.021 |
| watch validation/hash | 5.383 / 6.544 / 7.704 | 3.53 / 4.23 / 4.93 | 6.151 / 7.02 / 7.89 | 4.374 / 5.104 / 5.834 |

## Microbenchmarks, run 42019785137
Nanoseconds per operation, three repeats, pinned CPU 3, -test.cpu 1. AES uses AES-128-GCM. General protobuf and production encoders are different APIs. Weighted LIST is a hot synthetic page model, not an exact replay of all measured pages.

| benchmark ns/op | runner1 | runner2 |
|---|---|---|
| BenchmarkResearch484Encoding/LISTWeighted | 1.028e+05 / 1.031e+05 / 1.044e+05 | 1.034e+05 / 1.063e+05 / 1.066e+05 |
| BenchmarkResearch484Encoding/Reply/get | 1022 / 1027 / 1036 | 999 / 1009 / 1012 |
| BenchmarkResearch484Encoding/Reply/update | 157.4 / 158 / 159.1 | 154.9 / 155.3 / 155.5 |
| BenchmarkResearch484Encoding/Watch/update | 1525 / 1539 / 1554 | 1510 / 1517 / 1517 |
| BenchmarkResearch484Encoding/Reply/create | 159.4 / 159.8 / 159.8 | 155.2 / 155.4 / 156.2 |
| BenchmarkResearch484Encoding/Watch/create | 1073 / 1083 / 1088 | 1060 / 1063 / 1066 |
| BenchmarkResearch484Encoding/Reply/delete | 176.6 / 177.1 / 178.1 | 173.3 / 173.6 / 173.6 |
| BenchmarkResearch484Encoding/Watch/delete | 1146 / 1146 / 1154 | 1141 / 1143 / 1146 |
| BenchmarkResearch484/AESGCMSeal/64 | 77.41 / 77.41 / 77.58 | 77.27 / 77.34 / 77.43 |
| BenchmarkResearch484/AESGCMOpen/64 | 78.65 / 78.67 / 78.68 | 78.64 / 78.65 / 78.66 |
| BenchmarkResearch484/AESGCMSeal/1460 | 412.8 / 413 / 413.2 | 412.6 / 412.8 / 412.8 |
| BenchmarkResearch484/AESGCMOpen/1460 | 415.7 / 415.7 / 415.7 | 415.6 / 415.7 / 415.8 |
| BenchmarkResearch484/AESGCMSeal/16384 | 3936 / 3937 / 3937 | 3937 / 3937 / 3939 |
| BenchmarkResearch484/AESGCMOpen/16384 | 3919 / 3919 / 3919 | 3917 / 3918 / 3918 |
| BenchmarkResearch484/AESGCMSeal/33365 | 8003 / 8005 / 8051 | 8006 / 8006 / 8011 |
| BenchmarkResearch484/AESGCMOpen/33365 | 7959 / 7959 / 7960 | 7956 / 7958 / 7958 |
| BenchmarkResearch484/ProtoMarshalRange/1 | 240.9 / 241 / 241.1 | 240.7 / 240.7 / 240.8 |
| BenchmarkResearch484/ProtoUnmarshalRange/1 | 1040 / 1046 / 1049 | 1030 / 1039 / 1041 |
| BenchmarkResearch484/ProtoMarshalRange/500 | 7.612e+04 / 7.622e+04 / 7.628e+04 | 7.722e+04 / 7.732e+04 / 7.736e+04 |
| BenchmarkResearch484/ProtoUnmarshalRange/500 | 3.473e+05 / 3.55e+05 / 3.58e+05 | 3.46e+05 / 3.472e+05 / 3.476e+05 |
| BenchmarkResearch484/DrawResource | 403.4 / 403.4 / 403.5 | 403.3 / 403.6 / 405.9 |
| BenchmarkResearch484/DrawEventsSize | 533.8 / 533.9 / 535.1 | 533.2 / 535.5 / 539.6 |
| BenchmarkResearch484/BuildUpdate | 3081 / 3083 / 3086 | 3042 / 3044 / 3055 |

## Diskbench, all runs that measured it
MiB/s, O_DIRECT overwrite of already-written space plus fdatasync after every group. The largest group is NOT a per-request fsync latency measurement.

| run | runner | group B | MiB/s |
|---|---|---|---|
| 42019370875 | ceiling-mixed-1 | 262144 | 341.909 |
| 42019370875 | ceiling-mixed-1 | 1048576 | 342.641 |
| 42019370875 | ceiling-mixed-1 | 4194304 | 343.112 |
| 42019370875 | ceiling-mixed-1 | best | 343.112 |
| 42019370875 | ceiling-mixed-2 | 262144 | 339.906 |
| 42019370875 | ceiling-mixed-2 | 1048576 | 342.296 |
| 42019370875 | ceiling-mixed-2 | 4194304 | 342.530 |
| 42019370875 | ceiling-mixed-2 | best | 342.530 |
| 42019785137 | ceiling-mixed-1 | 262144 | 337.251 |
| 42019785137 | ceiling-mixed-1 | 1048576 | 343.279 |
| 42019785137 | ceiling-mixed-1 | 4194304 | 343.260 |
| 42019785137 | ceiling-mixed-1 | best | 343.279 |
| 42019785137 | ceiling-mixed-2 | 262144 | 344.278 |
| 42019785137 | ceiling-mixed-2 | 1048576 | 344.444 |
| 42019785137 | ceiling-mixed-2 | 4194304 | 342.236 |
| 42019785137 | ceiling-mixed-2 | best | 344.444 |

## Partial primitive floor MODEL (not a measured end-to-end ceiling)
ASSUMPTIONS / guesses: requests and watch work evenly balanced over nodes; 4.17 GB/s raw AEAD, proportional scaling of the hot LIST encoder to the measured mean page; synthetic single-event watch sizes. Excludes indexing, checksums, Raft protobuf, storage CPU, GC, HTTP2, syscalls and kernel networking. Floor is only for the included primitive work. Request protobuf payload is replicated twice and forwarded for 2/3 of writes; leader peer crypto = 8B/3, each follower = 4B/3. WAL payload means 3 times the request body, not filesystem device bytes.

| op | leader us/op, MODEL | each follower us/op, MODEL | WAL request payload B/op, MODEL |
|---|---|---|---|
| get | 0.426 | 0.426 | 0.0 |
| list | 56.412 | 56.412 | 0.0 |
| update | 1.962 | 1.452 | 4785.0 |
| create | 1.661 | 1.163 | 4676.4 |
| delete | 0.660 | 0.622 | 361.5 |
| mix | 6.847 | 6.565 | 2651.6 |

| MODEL bound | RTT0 | RTT2 | RTT10 |
|---|---|---|---|
| leader core | 146040 | 146040 | 146040 |
| three cores, balanced total | 150172 | 150172 | 150172 |
| disk 343 MiB/s / request payload ONLY | 135639 | 135639 | 135639 |
| 288 in flight / (5 RTT / 3), no storage/CPU wait | not finite | 86400 | 17280 |

These optimistic subset calculations do not establish that tigerstore can reach them. The actual measured CPU-cost projections, storage traffic and generator ceiling above are much tighter for the present implementation.

## Dump table statistics
Computed exactly by object counts from internal/k8ssize/k8ssize.go; p99 uses nearest rank. Actual generated resource sizes are conditional draws from that same histogram.

| resource | objects | mean B | median B | p99 B | min B | max B | watch endpoint node |
|---|---|---|---|---|---|---|---|
| ALL | 6345 | 1458.406 | 750 | 10834 | 63 | 33365 | - |
| apiservices | 22 | 882.045 | 885.0 | 908 | 827 | 908 | 1 |
| certificatesigningrequests | 1 | 2400.000 | 2400 | 2400 | 2400 | 2400 | 2 |
| clusterrolebindings | 64 | 634.375 | 667.0 | 955 | 341 | 955 | 3 |
| clusterroles | 77 | 842.597 | 670 | 3811 | 242 | 3811 | 1 |
| configmaps | 385 | 6965.143 | 2642 | 33365 | 264 | 33365 | 2 |
| controllerrevisions | 2 | 2209.500 | 2209.5 | 2388 | 2031 | 2388 | 3 |
| csinodes | 1 | 682.000 | 682 | 682 | 682 | 682 | 1 |
| daemonsets | 2 | 3172.500 | 3172.5 | 3516 | 2829 | 3516 | 2 |
| deployments | 122 | 3393.492 | 3364.0 | 4327 | 3361 | 4466 | 3 |
| endpoints | 122 | 874.459 | 878.0 | 879 | 348 | 977 | 1 |
| endpointslices | 362 | 1267.359 | 1247.0 | 1876 | 406 | 1876 | 2 |
| events | 3895 | 736.664 | 664 | 899 | 490 | 899 | 3 |
| flowschemas | 11 | 1036.636 | 1047 | 1149 | 941 | 1149 | 1 |
| leases | 244 | 661.078 | 664.0 | 664 | 430 | 742 | 2 |
| minions | 1 | 4106.000 | 4106 | 4106 | 4106 | 4106 | 3 |
| namespaces | 17 | 307.706 | 290 | 559 | 290 | 559 | 1 |
| other | 127 | 535.228 | 535 | 951 | 63 | 1473 | 2 |
| pods | 129 | 5078.667 | 5303 | 6988 | 4455 | 7576 | 3 |
| priorityclasses | 2 | 391.000 | 391.0 | 407 | 375 | 407 | 1 |
| prioritylevelconfigurations | 8 | 614.000 | 641.0 | 645 | 503 | 645 | 2 |
| replicasets | 422 | 3224.727 | 3215.0 | 3293 | 2950 | 4103 | 3 |
| rolebindings | 12 | 628.583 | 646.0 | 978 | 382 | 978 | 1 |
| roles | 12 | 500.500 | 560.5 | 753 | 256 | 753 | 2 |
| secrets | 121 | 1679.545 | 825 | 4665 | 617 | 4665 | 3 |
| serviceaccounts | 61 | 130.000 | 126 | 529 | 108 | 529 | 1 |
| services | 122 | 715.172 | 712.0 | 714 | 638 | 1143 | 2 |
| storageclasses | 1 | 841.000 | 841 | 841 | 841 | 841 | 3 |

