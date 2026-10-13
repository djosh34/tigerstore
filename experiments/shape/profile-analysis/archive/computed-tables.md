# Offline computed tables

Integer nanoseconds and per-sample attribution are in `computed.json.gz`. Raw pprof outputs are in `raw/`. File fsync requires the observed-file wrapper and excludes directory and unclassified syncs.

## Disjoint leader CPU us per success

| Cell | Success | Reader | Writer | Owner | WAL | Peer writer | Peer reader | Other | Total |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| list-1k-balanced-c64 | 5831 | 6.860 | 277.825 | 41.159 | 0.000 | 30.869 | 13.720 | 120.048 | 490.482 |
| list-1k-leader-c64 | 3929 | 12.726 | 1018.071 | 152.711 | 0.000 | 15.271 | 5.090 | 81.446 | 1285.314 |
| put-10k-leader-c256 | 36002 | 9.444 | 3.055 | 7.777 | 4.444 | 14.999 | 0.278 | 2.778 | 42.775 |
| put-1k-balanced-c256 | 182157 | 3.019 | 1.208 | 4.392 | 1.482 | 3.788 | 3.404 | 1.812 | 19.104 |
| put-1k-balanced-c4096 | 494908 | 1.293 | 0.343 | 2.162 | 0.303 | 2.081 | 0.828 | 0.566 | 7.577 |
| put-1k-leader-c256 | 226069 | 5.574 | 2.787 | 2.079 | 1.371 | 1.991 | 0.265 | 1.106 | 15.172 |
| put-1k-leader-c4096 | 406092 | 3.595 | 0.197 | 1.921 | 0.443 | 1.354 | 0.000 | 0.492 | 8.003 |
| range-1k-balanced-c1024 | 1024337 | 1.289 | 1.025 | 0.996 | 0.000 | 0.693 | 0.537 | 0.264 | 4.803 |
| range-1k-leader-c1024 | 587824 | 2.875 | 2.688 | 1.565 | 0.000 | 0.476 | 0.102 | 0.783 | 8.489 |

## Inclusive leader CPU us per success, overlapping

| Cell | TLS | socket calls | HTTP2/HPACK | core | raft | mvcc | WAL | observed_file_fsync | directory_fsync | unclassified_fsync | alloc/GC | memmove | entry codec |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| list-1k-balanced-c64 | 214.371 | 96.038 | 1.715 | 24.010 | 1.715 | 22.295 | 0.000 | 0.000 | 0.000 | 0.000 | 82.319 | 66.884 | 0.000 |
| list-1k-leader-c64 | 674.472 | 363.960 | 7.636 | 109.443 | 2.545 | 104.352 | 0.000 | 0.000 | 0.000 | 0.000 | 185.798 | 213.795 | 0.000 |
| put-10k-leader-c256 | 20.832 | 8.611 | 6.944 | 7.222 | 3.055 | 3.055 | 4.444 | 1.944 | 0.000 | 0.000 | 2.778 | 7.500 | 4.166 |
| put-1k-balanced-c256 | 6.643 | 4.557 | 2.361 | 3.733 | 1.976 | 0.878 | 1.427 | 0.769 | 0.000 | 0.000 | 2.361 | 0.823 | 1.372 |
| put-1k-balanced-c4096 | 2.950 | 1.697 | 0.869 | 2.122 | 0.525 | 0.950 | 0.303 | 0.101 | 0.000 | 0.000 | 1.132 | 0.869 | 1.455 |
| put-1k-leader-c256 | 6.281 | 4.689 | 3.937 | 1.327 | 0.310 | 0.442 | 1.371 | 0.840 | 0.000 | 0.000 | 1.371 | 0.885 | 0.840 |
| put-1k-leader-c4096 | 2.241 | 1.330 | 1.995 | 1.650 | 0.197 | 0.763 | 0.443 | 0.098 | 0.000 | 0.000 | 1.207 | 0.936 | 1.305 |
| range-1k-balanced-c1024 | 1.523 | 1.132 | 0.918 | 0.820 | 0.322 | 0.381 | 0.000 | 0.000 | 0.000 | 0.000 | 0.722 | 0.117 | 0.000 |
| range-1k-leader-c1024 | 1.922 | 1.344 | 1.786 | 1.072 | 0.068 | 0.783 | 0.000 | 0.000 | 0.000 | 0.000 | 1.650 | 0.663 | 0.000 |

## Sampled CPU us per success by node slot

| Cell | Leader | Follower1 | Follower2 |
| --- | --- | --- | --- |
| list-1k-balanced-c64 | 490.482 | 459.612 | 464.757 |
| list-1k-leader-c64 | 1285.314 | 7.636 | 15.271 |
| put-10k-leader-c256 | 42.775 | 17.221 | 16.944 |
| put-1k-balanced-c256 | 19.104 | 16.963 | 16.305 |
| put-1k-balanced-c4096 | 7.577 | 6.284 | 6.021 |
| put-1k-leader-c256 | 15.172 | 4.954 | 4.645 |
| put-1k-leader-c4096 | 8.003 | 2.438 | 2.389 |
| range-1k-balanced-c1024 | 4.803 | 4.374 | 4.442 |
| range-1k-leader-c1024 | 8.489 | 0.204 | 0.629 |

## Subject Go-state wall seconds

| Cell | Capture | Owner runnable | Owner select | WAL runnable | WAL syscall | Readers runnable | Writers runnable |
| --- | --- | --- | --- | --- | --- | --- | --- |
| put-10k-leader-c256 | 2.002040 | 0.088842 | 1.763457 | 0.024670 | 1.872282 | 1.158866 | 3.066818 |
| put-1k-leader-c256 | 2.000732 | 0.358348 | 1.262293 | 0.051990 | 1.744096 | 4.964355 | 9.435904 |
| put-1k-leader-c4096 | 2.000754 | 0.495107 | 0.903564 | 0.137848 | 1.367816 | 5.902167 | 8.096370 |

## Completed syscall spans, file and directory separated

| Cell | Kind | Count | Sum s | Median ms | p99 ms | Max ms |
| --- | --- | --- | --- | --- | --- | --- |
| put-10k-leader-c256 | observed_file_fsync | 151 | 1.544942336 | 8.829952 | 50.552640 | 70.406144 |
| put-10k-leader-c256 | directory_fsync | 2 | 0.000258880 | 0.195264 | 0.195264 | 0.195264 |
| put-10k-leader-c256 | unclassified_fsync | 0 | 0.000000000 | none | none | none |
| put-10k-leader-c256 | observed_file_write | 151 | 0.324141824 | 0.209600 | 4.941440 | 255.468800 |
| put-10k-leader-c256 | unclassified_file_write | 0 | 0.000000000 | none | none | none |
| put-1k-leader-c256 | observed_file_fsync | 1293 | 1.478552960 | 0.797568 | 9.172992 | 15.526400 |
| put-1k-leader-c256 | directory_fsync | 1 | 0.000000768 | 0.000768 | 0.000768 | 0.000768 |
| put-1k-leader-c256 | unclassified_fsync | 0 | 0.000000000 | none | none | none |
| put-1k-leader-c256 | observed_file_write | 1294 | 0.264905728 | 0.035648 | 2.330304 | 9.354496 |
| put-1k-leader-c256 | unclassified_file_write | 0 | 0.000000000 | none | none | none |
| put-1k-leader-c4096 | observed_file_fsync | 141 | 1.203333120 | 6.367808 | 27.304128 | 27.610304 |
| put-1k-leader-c4096 | directory_fsync | 3 | 0.000062016 | 0.001408 | 0.059968 | 0.059968 |
| put-1k-leader-c4096 | unclassified_fsync | 0 | 0.000000000 | none | none | none |
| put-1k-leader-c4096 | observed_file_write | 142 | 0.158130688 | 0.337408 | 6.276672 | 9.346496 |
| put-1k-leader-c4096 | unclassified_file_write | 0 | 0.000000000 | none | none | none |

## Whole-cohort fenced file-only leader counters

| Mode | Cell | Success | Fsyncs | Writes | Success/fsync | Mean write KiB | Fsync wall s | Write wall s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| cpu | list-1k-balanced-c64 | 5831 | 0 | 0 | none | none | 0.000000000 | 0.000000000 |
| cpu | list-1k-leader-c64 | 3929 | 0 | 0 | none | none | 0.000000000 | 0.000000000 |
| cpu | put-10k-leader-c256 | 36002 | 458 | 458 | 78.606987 | 791.241175 | 4.482790140 | 0.254438940 |
| cpu | put-1k-balanced-c256 | 182157 | 2741 | 2741 | 66.456403 | 71.567023 | 4.277748900 | 0.488096718 |
| cpu | put-1k-balanced-c4096 | 494908 | 444 | 444 | 1114.657658 | 1189.241598 | 4.098981527 | 0.484195039 |
| cpu | put-1k-leader-c256 | 226069 | 3086 | 3086 | 73.256319 | 78.086327 | 3.864502919 | 0.650660539 |
| cpu | put-1k-leader-c4096 | 406092 | 337 | 337 | 1205.020772 | 1283.906421 | 3.104325521 | 0.547267290 |
| cpu | range-1k-balanced-c1024 | 1024337 | 0 | 0 | none | none | 0.000000000 | 0.000000000 |
| cpu | range-1k-leader-c1024 | 587824 | 0 | 0 | none | none | 0.000000000 | 0.000000000 |
| trace | put-10k-leader-c256 | 26560 | 318 | 318 | 83.522013 | 840.635650 | 3.340474016 | 0.416204565 |
| trace | put-1k-leader-c256 | 182384 | 2663 | 2663 | 68.488171 | 72.940123 | 3.031125468 | 0.525635509 |
| trace | put-1k-leader-c4096 | 339462 | 261 | 261 | 1300.620690 | 1383.222222 | 2.391168130 | 0.591220348 |
