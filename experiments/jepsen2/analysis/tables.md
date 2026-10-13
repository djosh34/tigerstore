# Generated tables

Core means the selected candidate histories and their direct controls. Auxiliary histories are separate.

| History | Role | Budget | Verdict | Operations | :ok | :fail | :info | Unknown keys | Max :info mutations/key | Analysis seconds |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 38428716698/jepsen-tigerstore-register-kill-1 | unknown | 200 | :unknown | 1636 | 1291 | 257 | 88 | 1/9 | 51 | 40.682 |
| 38427801610/jepsen-etcd-register-kill-1 | matched-control | 200 | True | 1956 | 1624 | 305 | 27 | 0/10 | 6 | 2.193 |
| 38430056885/jepsen-tigerstore-register-kill-rep2-1 | unknown | 200 | :unknown | 1888 | 1466 | 316 | 106 | 4/10 | 31 | 37.369 |
| 38430056885/jepsen-etcd-register-kill-rep2-1 | matched-control | 200 | True | 1429 | 1098 | 311 | 20 | 0/8 | 10 | 2.107 |
| 38430056885/jepsen-tigerstore-register-kill-rep3-1 | unknown | 200 | :unknown | 1774 | 1325 | 337 | 112 | 2/9 | 61 | 65.98 |
| 38430056885/jepsen-etcd-register-kill-rep3-1 | matched-control | 200 | True | 1656 | 1326 | 315 | 15 | 0/9 | 6 | 2.033 |
| 38432262482/jepsen-tigerstore-register-kill-rep1-1 | unknown | 50 | :unknown | 1653 | 1210 | 314 | 129 | 2/34 | 46 | 60.556 |
| 38430904489/jepsen-etcd-register-kill-rep1-1 | matched-control | 50 | True | 1607 | 1291 | 291 | 25 | 0/33 | 5 | 2.991 |
| 38430224306/jepsen-etcd-register-kill-rep1-1 | earlier-50-control | 50 | True | 1703 | 1331 | 347 | 25 | 0/35 | 6 | 3.061 |
| 38433448661/jepsen-etcd-register-kill-rep1-1 | bounded-control | 10 | True | 1782 | 1453 | 307 | 22 | 0/179 | 3 | 7.186 |
| 38434478844/jepsen-tigerstore-register-kill-rep1-1 | bounded-tigerstore | 10 | True | 2132 | 1680 | 342 | 110 | 0/214 | 10 | 12.482 |
| 38430224306/jepsen-etcd-register-kill-rep2-1 | earlier-50-control | 50 | True | 1675 | 1359 | 291 | 25 | 0/34 | 6 | 2.972 |
| 38433448661/jepsen-etcd-register-kill-rep2-1 | bounded-control | 10 | True | 2065 | 1708 | 336 | 21 | 0/207 | 3 | 2.759 |
| 38434478844/jepsen-tigerstore-register-kill-rep2-1 | bounded-tigerstore | 10 | True | 1906 | 1434 | 350 | 122 | 0/191 | 10 | 7.674 |
| 38428716698/jepsen-tigerstore-register-partition-1 | unknown | 200 | :unknown | 2389 | 1568 | 307 | 514 | 2/12 | 185 | 30.686 |
| 38427801610/jepsen-etcd-register-partition-1 | matched-control | 200 | True | 1799 | 1442 | 337 | 20 | 0/9 | 7 | 1.849 |
| 38430056885/jepsen-tigerstore-register-partition-rep2-1 | unknown | 200 | :unknown | 2140 | 1368 | 278 | 494 | 5/11 | 184 | 16.608 |
| 38430056885/jepsen-etcd-register-partition-rep2-1 | matched-control | 200 | True | 2253 | 1830 | 400 | 23 | 0/12 | 4 | 2.303 |
| 38430056885/jepsen-tigerstore-register-partition-rep3-1 | unknown | 200 | :unknown | 2159 | 1095 | 219 | 845 | 8/11 | 157 | 89.939 |
| 38430056885/jepsen-etcd-register-partition-rep3-1 | matched-control | 200 | True | 2188 | 1752 | 420 | 16 | 0/11 | 4 | 1.257 |
| 38432262482/jepsen-tigerstore-register-partition-rep1-1 | unknown | 50 | :unknown | 2348 | 891 | 221 | 1236 | 5/47 | 50 | 92.12 |
| 38430904489/jepsen-etcd-register-partition-rep1-1 | matched-control | 50 | True | 1744 | 1376 | 348 | 20 | 0/35 | 7 | 2.939 |
| 38430224306/jepsen-etcd-register-partition-rep1-1 | earlier-50-control | 50 | True | 2250 | 1787 | 448 | 15 | 0/45 | 4 | 3.705 |
| 38433448661/jepsen-etcd-register-partition-rep1-1 | bounded-control | 10 | True | 2056 | 1635 | 405 | 16 | 0/206 | 3 | 3.329 |
| 38434478844/jepsen-tigerstore-register-partition-rep1-1 | bounded-tigerstore | 10 | True | 2159 | 1533 | 304 | 322 | 0/216 | 10 | 27.479 |
| 38430224306/jepsen-etcd-register-partition-rep2-1 | earlier-50-control | 50 | True | 1555 | 1212 | 314 | 29 | 0/32 | 15 | 2.928 |
| 38433448661/jepsen-etcd-register-partition-rep2-1 | bounded-control | 10 | True | 2132 | 1669 | 449 | 14 | 0/214 | 4 | 3.077 |
| 38434478844/jepsen-tigerstore-register-partition-rep2-1 | bounded-tigerstore | 10 | True | 2327 | 1662 | 261 | 404 | 0/233 | 10 | 41.892 |

## Core per-store and per-operation counts

| Store | Operation | :ok | :fail | :info | Total |
| --- | --- | --- | --- | --- | --- |
| etcd | :all | 11739 | 2727 | 166 | 14632 |
| etcd | :read | 7956 | 125 | 0 | 8081 |
| etcd | :write | 3146 | 0 | 76 | 3222 |
| etcd | :cas | 637 | 2602 | 90 | 3329 |
| tigerstore | :all | 10214 | 2249 | 3524 | 15987 |
| tigerstore | :read | 7204 | 166 | 1 | 7371 |
| tigerstore | :write | 2527 | 0 | 1829 | 4356 |
| tigerstore | :cas | 483 | 2083 | 1694 | 4260 |

## Core latency summaries

All values are milliseconds. Quantiles use nearest rank. Includes completed client calls only.

| Store | Operation | Outcome | n | Median | p95 | p99 | Max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| etcd | :all | :ok | 11739 | 2.492828 | 6.148346 | 2392.820534 | 4966.011601 |
| etcd | :all | :fail | 2727 | 2.890793 | 3719.908827 | 5001.274114 | 5011.118458 |
| etcd | :all | :info | 166 | 5001.208873 | 5006.473349 | 5013.86315 | 5015.187169 |
| etcd | :read | :ok | 7956 | 2.325185 | 5.53779 | 2362.342517 | 4867.281098 |
| etcd | :read | :fail | 125 | 5001.011992 | 5002.616108 | 5005.642367 | 5011.118458 |
| etcd | :read | :info | 0 | None | None | None | None |
| etcd | :write | :ok | 3146 | 2.792337 | 7.463916 | 2422.944656 | 4966.011601 |
| etcd | :write | :fail | 0 | None | None | None | None |
| etcd | :write | :info | 76 | 5001.0188 | 5006.473349 | 5013.86315 | 5013.86315 |
| etcd | :cas | :ok | 637 | 2.985999 | 7.731216 | 2256.909875 | 4792.412189 |
| etcd | :cas | :fail | 2602 | 2.83853 | 7.291458 | 2575.986435 | 4973.131306 |
| etcd | :cas | :info | 90 | 5001.404648 | 5006.757209 | 5015.187169 | 5015.187169 |
| tigerstore | :all | :ok | 10214 | 1.975139 | 5.350898 | 1406.857495 | 4948.683959 |
| tigerstore | :all | :fail | 2249 | 2.672844 | 5000.786625 | 5001.277834 | 5004.225658 |
| tigerstore | :all | :info | 3524 | 5.578993 | 2534.824024 | 5002.857407 | 5008.531434 |
| tigerstore | :read | :ok | 7204 | 1.767965 | 4.373378 | 2356.233695 | 4924.905722 |
| tigerstore | :read | :fail | 166 | 5000.849447 | 5001.841449 | 5002.640944 | 5004.225658 |
| tigerstore | :read | :info | 1 | 8.273564 | 8.273564 | 8.273564 | 8.273564 |
| tigerstore | :write | :ok | 2527 | 2.475875 | 6.811567 | 80.951127 | 4722.054766 |
| tigerstore | :write | :fail | 0 | None | None | None | None |
| tigerstore | :write | :info | 1829 | 5.476263 | 5000.547108 | 5003.151244 | 5006.917955 |
| tigerstore | :cas | :ok | 483 | 2.709552 | 11.271844 | 147.946881 | 4948.683959 |
| tigerstore | :cas | :fail | 2083 | 2.578039 | 6.771604 | 60.757749 | 4874.54402 |
| tigerstore | :cas | :info | 1694 | 5.668777 | 26.413262 | 5002.126905 | 5008.531434 |

## Core counts for each history and operation

| History | Operation | :ok | :fail | :info |
| --- | --- | --- | --- | --- |
| 38428716698/jepsen-tigerstore-register-kill-1 | :read | 889 | 22 | 0 |
| 38428716698/jepsen-tigerstore-register-kill-1 | :write | 350 | 0 | 43 |
| 38428716698/jepsen-tigerstore-register-kill-1 | :cas | 52 | 235 | 45 |
| 38427801610/jepsen-etcd-register-kill-1 | :read | 1258 | 13 | 0 |
| 38427801610/jepsen-etcd-register-kill-1 | :write | 300 | 0 | 15 |
| 38427801610/jepsen-etcd-register-kill-1 | :cas | 66 | 292 | 12 |
| 38430056885/jepsen-tigerstore-register-kill-rep2-1 | :read | 1029 | 16 | 0 |
| 38430056885/jepsen-tigerstore-register-kill-rep2-1 | :write | 375 | 0 | 61 |
| 38430056885/jepsen-tigerstore-register-kill-rep2-1 | :cas | 62 | 300 | 45 |
| 38430056885/jepsen-etcd-register-kill-rep2-1 | :read | 730 | 25 | 0 |
| 38430056885/jepsen-etcd-register-kill-rep2-1 | :write | 303 | 0 | 9 |
| 38430056885/jepsen-etcd-register-kill-rep2-1 | :cas | 65 | 286 | 11 |
| 38430056885/jepsen-tigerstore-register-kill-rep3-1 | :read | 871 | 20 | 0 |
| 38430056885/jepsen-tigerstore-register-kill-rep3-1 | :write | 366 | 0 | 57 |
| 38430056885/jepsen-tigerstore-register-kill-rep3-1 | :cas | 88 | 317 | 55 |
| 38430056885/jepsen-etcd-register-kill-rep3-1 | :read | 856 | 13 | 0 |
| 38430056885/jepsen-etcd-register-kill-rep3-1 | :write | 385 | 0 | 10 |
| 38430056885/jepsen-etcd-register-kill-rep3-1 | :cas | 85 | 302 | 5 |
| 38432262482/jepsen-tigerstore-register-kill-rep1-1 | :read | 799 | 24 | 1 |
| 38432262482/jepsen-tigerstore-register-kill-rep1-1 | :write | 347 | 0 | 61 |
| 38432262482/jepsen-tigerstore-register-kill-rep1-1 | :cas | 64 | 290 | 67 |
| 38430904489/jepsen-etcd-register-kill-rep1-1 | :read | 858 | 17 | 0 |
| 38430904489/jepsen-etcd-register-kill-rep1-1 | :write | 376 | 0 | 9 |
| 38430904489/jepsen-etcd-register-kill-rep1-1 | :cas | 57 | 274 | 16 |
| 38428716698/jepsen-tigerstore-register-partition-1 | :read | 1143 | 16 | 0 |
| 38428716698/jepsen-tigerstore-register-partition-1 | :write | 362 | 0 | 256 |
| 38428716698/jepsen-tigerstore-register-partition-1 | :cas | 63 | 291 | 258 |
| 38427801610/jepsen-etcd-register-partition-1 | :read | 920 | 15 | 0 |
| 38427801610/jepsen-etcd-register-partition-1 | :write | 440 | 0 | 8 |
| 38427801610/jepsen-etcd-register-partition-1 | :cas | 82 | 322 | 12 |
| 38430056885/jepsen-tigerstore-register-partition-rep2-1 | :read | 986 | 15 | 0 |
| 38430056885/jepsen-tigerstore-register-partition-rep2-1 | :write | 312 | 0 | 260 |
| 38430056885/jepsen-tigerstore-register-partition-rep2-1 | :cas | 70 | 263 | 234 |
| 38430056885/jepsen-etcd-register-partition-rep2-1 | :read | 1273 | 12 | 0 |
| 38430056885/jepsen-etcd-register-partition-rep2-1 | :write | 468 | 0 | 8 |
| 38430056885/jepsen-etcd-register-partition-rep2-1 | :cas | 89 | 388 | 15 |
| 38430056885/jepsen-tigerstore-register-partition-rep3-1 | :read | 827 | 23 | 0 |
| 38430056885/jepsen-tigerstore-register-partition-rep3-1 | :write | 216 | 0 | 438 |
| 38430056885/jepsen-tigerstore-register-partition-rep3-1 | :cas | 52 | 196 | 407 |
| 38430056885/jepsen-etcd-register-partition-rep3-1 | :read | 1168 | 11 | 0 |
| 38430056885/jepsen-etcd-register-partition-rep3-1 | :write | 479 | 0 | 8 |
| 38430056885/jepsen-etcd-register-partition-rep3-1 | :cas | 105 | 409 | 8 |
| 38432262482/jepsen-tigerstore-register-partition-rep1-1 | :read | 660 | 30 | 0 |
| 38432262482/jepsen-tigerstore-register-partition-rep1-1 | :write | 199 | 0 | 653 |
| 38432262482/jepsen-tigerstore-register-partition-rep1-1 | :cas | 32 | 191 | 583 |
| 38430904489/jepsen-etcd-register-partition-rep1-1 | :read | 893 | 19 | 0 |
| 38430904489/jepsen-etcd-register-partition-rep1-1 | :write | 395 | 0 | 9 |
| 38430904489/jepsen-etcd-register-partition-rep1-1 | :cas | 88 | 329 | 11 |

## Core fault-edge summaries

Each edge log lists every invocation and completion in order, with exact open client calls.

| History | Action | Phase | Edges | Mean open | Maximum open | Edge log |
| --- | --- | --- | --- | --- | --- | --- |
| 38428716698/jepsen-tigerstore-register-kill-1 | start | invoke | 9 | 1.667 | 6 | [log](histories/38428716698/jepsen-tigerstore-register-kill-1/fault-edges.log) |
| 38428716698/jepsen-tigerstore-register-kill-1 | start | completion | 9 | 5.111 | 6 | [log](histories/38428716698/jepsen-tigerstore-register-kill-1/fault-edges.log) |
| 38428716698/jepsen-tigerstore-register-kill-1 | heal | invoke | 10 | 4.000 | 6 | [log](histories/38428716698/jepsen-tigerstore-register-kill-1/fault-edges.log) |
| 38428716698/jepsen-tigerstore-register-kill-1 | heal | completion | 10 | 4.000 | 6 | [log](histories/38428716698/jepsen-tigerstore-register-kill-1/fault-edges.log) |
| 38427801610/jepsen-etcd-register-kill-1 | start | invoke | 9 | 1.333 | 6 | [log](histories/38427801610/jepsen-etcd-register-kill-1/fault-edges.log) |
| 38427801610/jepsen-etcd-register-kill-1 | start | completion | 9 | 4.778 | 6 | [log](histories/38427801610/jepsen-etcd-register-kill-1/fault-edges.log) |
| 38427801610/jepsen-etcd-register-kill-1 | heal | invoke | 10 | 3.500 | 6 | [log](histories/38427801610/jepsen-etcd-register-kill-1/fault-edges.log) |
| 38427801610/jepsen-etcd-register-kill-1 | heal | completion | 10 | 3.600 | 6 | [log](histories/38427801610/jepsen-etcd-register-kill-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-kill-rep2-1 | start | invoke | 7 | 0.143 | 1 | [log](histories/38430056885/jepsen-tigerstore-register-kill-rep2-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-kill-rep2-1 | start | completion | 7 | 5.143 | 6 | [log](histories/38430056885/jepsen-tigerstore-register-kill-rep2-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-kill-rep2-1 | heal | invoke | 8 | 3.750 | 6 | [log](histories/38430056885/jepsen-tigerstore-register-kill-rep2-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-kill-rep2-1 | heal | completion | 8 | 3.750 | 6 | [log](histories/38430056885/jepsen-tigerstore-register-kill-rep2-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-kill-rep2-1 | start | invoke | 8 | 0.875 | 2 | [log](histories/38430056885/jepsen-etcd-register-kill-rep2-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-kill-rep2-1 | start | completion | 8 | 5.750 | 6 | [log](histories/38430056885/jepsen-etcd-register-kill-rep2-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-kill-rep2-1 | heal | invoke | 9 | 4.889 | 6 | [log](histories/38430056885/jepsen-etcd-register-kill-rep2-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-kill-rep2-1 | heal | completion | 9 | 4.889 | 6 | [log](histories/38430056885/jepsen-etcd-register-kill-rep2-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-kill-rep3-1 | start | invoke | 8 | 1.375 | 6 | [log](histories/38430056885/jepsen-tigerstore-register-kill-rep3-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-kill-rep3-1 | start | completion | 8 | 5.625 | 6 | [log](histories/38430056885/jepsen-tigerstore-register-kill-rep3-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-kill-rep3-1 | heal | invoke | 9 | 3.667 | 6 | [log](histories/38430056885/jepsen-tigerstore-register-kill-rep3-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-kill-rep3-1 | heal | completion | 9 | 3.667 | 6 | [log](histories/38430056885/jepsen-tigerstore-register-kill-rep3-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-kill-rep3-1 | start | invoke | 8 | 0.125 | 1 | [log](histories/38430056885/jepsen-etcd-register-kill-rep3-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-kill-rep3-1 | start | completion | 8 | 5.750 | 6 | [log](histories/38430056885/jepsen-etcd-register-kill-rep3-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-kill-rep3-1 | heal | invoke | 9 | 4.889 | 6 | [log](histories/38430056885/jepsen-etcd-register-kill-rep3-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-kill-rep3-1 | heal | completion | 9 | 4.889 | 6 | [log](histories/38430056885/jepsen-etcd-register-kill-rep3-1/fault-edges.log) |
| 38432262482/jepsen-tigerstore-register-kill-rep1-1 | start | invoke | 9 | 2.000 | 6 | [log](histories/38432262482/jepsen-tigerstore-register-kill-rep1-1/fault-edges.log) |
| 38432262482/jepsen-tigerstore-register-kill-rep1-1 | start | completion | 9 | 5.667 | 6 | [log](histories/38432262482/jepsen-tigerstore-register-kill-rep1-1/fault-edges.log) |
| 38432262482/jepsen-tigerstore-register-kill-rep1-1 | heal | invoke | 10 | 4.300 | 6 | [log](histories/38432262482/jepsen-tigerstore-register-kill-rep1-1/fault-edges.log) |
| 38432262482/jepsen-tigerstore-register-kill-rep1-1 | heal | completion | 10 | 4.200 | 6 | [log](histories/38432262482/jepsen-tigerstore-register-kill-rep1-1/fault-edges.log) |
| 38430904489/jepsen-etcd-register-kill-rep1-1 | start | invoke | 9 | 0.333 | 3 | [log](histories/38430904489/jepsen-etcd-register-kill-rep1-1/fault-edges.log) |
| 38430904489/jepsen-etcd-register-kill-rep1-1 | start | completion | 9 | 5.444 | 6 | [log](histories/38430904489/jepsen-etcd-register-kill-rep1-1/fault-edges.log) |
| 38430904489/jepsen-etcd-register-kill-rep1-1 | heal | invoke | 9 | 4.556 | 6 | [log](histories/38430904489/jepsen-etcd-register-kill-rep1-1/fault-edges.log) |
| 38430904489/jepsen-etcd-register-kill-rep1-1 | heal | completion | 9 | 4.556 | 6 | [log](histories/38430904489/jepsen-etcd-register-kill-rep1-1/fault-edges.log) |
| 38428716698/jepsen-tigerstore-register-partition-1 | start | invoke | 11 | 1.364 | 5 | [log](histories/38428716698/jepsen-tigerstore-register-partition-1/fault-edges.log) |
| 38428716698/jepsen-tigerstore-register-partition-1 | start | completion | 11 | 1.545 | 4 | [log](histories/38428716698/jepsen-tigerstore-register-partition-1/fault-edges.log) |
| 38428716698/jepsen-tigerstore-register-partition-1 | heal | invoke | 11 | 2.727 | 6 | [log](histories/38428716698/jepsen-tigerstore-register-partition-1/fault-edges.log) |
| 38428716698/jepsen-tigerstore-register-partition-1 | heal | completion | 11 | 2.727 | 6 | [log](histories/38428716698/jepsen-tigerstore-register-partition-1/fault-edges.log) |
| 38427801610/jepsen-etcd-register-partition-1 | start | invoke | 9 | 0.667 | 2 | [log](histories/38427801610/jepsen-etcd-register-partition-1/fault-edges.log) |
| 38427801610/jepsen-etcd-register-partition-1 | start | completion | 9 | 0.778 | 2 | [log](histories/38427801610/jepsen-etcd-register-partition-1/fault-edges.log) |
| 38427801610/jepsen-etcd-register-partition-1 | heal | invoke | 10 | 4.000 | 6 | [log](histories/38427801610/jepsen-etcd-register-partition-1/fault-edges.log) |
| 38427801610/jepsen-etcd-register-partition-1 | heal | completion | 10 | 4.000 | 6 | [log](histories/38427801610/jepsen-etcd-register-partition-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-partition-rep2-1 | start | invoke | 10 | 1.100 | 4 | [log](histories/38430056885/jepsen-tigerstore-register-partition-rep2-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-partition-rep2-1 | start | completion | 10 | 0.900 | 3 | [log](histories/38430056885/jepsen-tigerstore-register-partition-rep2-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-partition-rep2-1 | heal | invoke | 10 | 3.500 | 6 | [log](histories/38430056885/jepsen-tigerstore-register-partition-rep2-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-partition-rep2-1 | heal | completion | 10 | 3.200 | 6 | [log](histories/38430056885/jepsen-tigerstore-register-partition-rep2-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-partition-rep2-1 | start | invoke | 8 | 1.000 | 4 | [log](histories/38430056885/jepsen-etcd-register-partition-rep2-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-partition-rep2-1 | start | completion | 8 | 0.875 | 4 | [log](histories/38430056885/jepsen-etcd-register-partition-rep2-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-partition-rep2-1 | heal | invoke | 8 | 2.750 | 6 | [log](histories/38430056885/jepsen-etcd-register-partition-rep2-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-partition-rep2-1 | heal | completion | 8 | 2.750 | 6 | [log](histories/38430056885/jepsen-etcd-register-partition-rep2-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-partition-rep3-1 | start | invoke | 11 | 1.636 | 6 | [log](histories/38430056885/jepsen-tigerstore-register-partition-rep3-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-partition-rep3-1 | start | completion | 11 | 1.636 | 5 | [log](histories/38430056885/jepsen-tigerstore-register-partition-rep3-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-partition-rep3-1 | heal | invoke | 11 | 3.909 | 6 | [log](histories/38430056885/jepsen-tigerstore-register-partition-rep3-1/fault-edges.log) |
| 38430056885/jepsen-tigerstore-register-partition-rep3-1 | heal | completion | 11 | 3.909 | 6 | [log](histories/38430056885/jepsen-tigerstore-register-partition-rep3-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-partition-rep3-1 | start | invoke | 9 | 0.333 | 2 | [log](histories/38430056885/jepsen-etcd-register-partition-rep3-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-partition-rep3-1 | start | completion | 9 | 0.444 | 2 | [log](histories/38430056885/jepsen-etcd-register-partition-rep3-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-partition-rep3-1 | heal | invoke | 9 | 3.444 | 6 | [log](histories/38430056885/jepsen-etcd-register-partition-rep3-1/fault-edges.log) |
| 38430056885/jepsen-etcd-register-partition-rep3-1 | heal | completion | 9 | 3.444 | 6 | [log](histories/38430056885/jepsen-etcd-register-partition-rep3-1/fault-edges.log) |
| 38432262482/jepsen-tigerstore-register-partition-rep1-1 | start | invoke | 10 | 1.500 | 3 | [log](histories/38432262482/jepsen-tigerstore-register-partition-rep1-1/fault-edges.log) |
| 38432262482/jepsen-tigerstore-register-partition-rep1-1 | start | completion | 10 | 1.700 | 4 | [log](histories/38432262482/jepsen-tigerstore-register-partition-rep1-1/fault-edges.log) |
| 38432262482/jepsen-tigerstore-register-partition-rep1-1 | heal | invoke | 11 | 2.455 | 6 | [log](histories/38432262482/jepsen-tigerstore-register-partition-rep1-1/fault-edges.log) |
| 38432262482/jepsen-tigerstore-register-partition-rep1-1 | heal | completion | 11 | 2.545 | 6 | [log](histories/38432262482/jepsen-tigerstore-register-partition-rep1-1/fault-edges.log) |
| 38430904489/jepsen-etcd-register-partition-rep1-1 | start | invoke | 10 | 1.500 | 6 | [log](histories/38430904489/jepsen-etcd-register-partition-rep1-1/fault-edges.log) |
| 38430904489/jepsen-etcd-register-partition-rep1-1 | start | completion | 10 | 1.600 | 6 | [log](histories/38430904489/jepsen-etcd-register-partition-rep1-1/fault-edges.log) |
| 38430904489/jepsen-etcd-register-partition-rep1-1 | heal | invoke | 10 | 3.400 | 6 | [log](histories/38430904489/jepsen-etcd-register-partition-rep1-1/fault-edges.log) |
| 38430904489/jepsen-etcd-register-partition-rep1-1 | heal | completion | 10 | 3.100 | 6 | [log](histories/38430904489/jepsen-etcd-register-partition-rep1-1/fault-edges.log) |
