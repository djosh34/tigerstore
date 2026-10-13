# Generated numeric tables

Independent medians across five repetitions; host rows have one observation. Cores are medians of per-run CPU/elapsed, not products of separate medians. Paired cells show median [minimum, maximum]; negative savings are regressions. Profiles are operation-weighted sampled CPU, not process-CPU medians. All quantities are per successful Put unless labelled otherwise.

CPU spans workers through drain. Other counters span the wider replica-fenced interval. Retained heap is forced-GC for Tigerstore, not etcd. syscr/syscw include non-socket calls. Tigerstore syncs exclude directory sync; etcd WAL histogram counts can include them. See the report for interpretation and source pins.

## lean-initial

### run-38395499741-primary-c256.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 5 | 63,360.684 | 3.914 | 7.488 | 10.530 | 12.167 | 5.333 | 4.833 | 5.500 | 0.771 | 0.343 |
| etcd | 5 | 6,222.171 | 39.935 | 72.866 | 87.174 | 158.000 | 29.167 | 28.000 | 37.500 | 0.987 | 0.235 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 11.104 | 5,350.823 | 0.190 | 0.234 | 73.082 | 1,136.503 | 93.574 | 165.094 |
| baseline | follower1 | 2.897 | 1,458.341 | 0.070 | 0.030 | 68.886 | 1,139.917 | 87.762 | 110.504 |
| baseline | follower2 | 2.896 | 1,458.290 | 0.068 | 0.029 | 69.849 | 1,138.347 | 87.681 | 110.098 |
| etcd | leader | 260.022 | 34,354.027 | 0.381 | 4.054 | 17.172 | 2,794.974 | 39.554 | 88.742 |
| etcd | follower1 | 81.927 | 14,553.854 | 0.200 | 0.437 | 47.281 | 2,646.494 | 32.243 | 70.152 |
| etcd | follower2 | 81.568 | 14,476.329 | 0.206 | 0.437 | 46.693 | 2,647.518 | 28.471 | 70.992 |

### run-38395499741-primary-c64.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 5 | 33,371.039 | 1.869 | 3.979 | 10.503 | 24.167 | 13.833 | 12.000 | 9.667 | 0.796 | 0.317 |
| etcd | 5 | 5,202.417 | 11.597 | 24.271 | 29.281 | 188.500 | 45.833 | 43.833 | 42.833 | 0.983 | 0.224 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 16.429 | 5,471.317 | 0.716 | 0.526 | 18.773 | 1,298.978 | 92.758 | 156.941 |
| baseline | follower1 | 4.931 | 1,576.083 | 0.142 | 0.111 | 18.536 | 1,302.733 | 90.568 | 114.285 |
| baseline | follower2 | 4.909 | 1,575.489 | 0.142 | 0.110 | 18.674 | 1,301.299 | 90.566 | 115.309 |
| etcd | leader | 269.905 | 35,367.149 | 1.323 | 4.604 | 10.657 | 2,955.742 | 35.844 | 77.453 |
| etcd | follower1 | 91.243 | 15,762.631 | 0.329 | 0.558 | 15.823 | 2,832.998 | 29.718 | 69.523 |
| etcd | follower2 | 91.660 | 15,849.196 | 0.342 | 0.561 | 15.349 | 2,838.869 | 28.088 | 69.148 |

### run-38395499741-profile-c256.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline-control | 5 | 60,820.819 | 4.068 | 8.743 | 18.105 | 12.873 | 6.040 | 5.403 | 5.677 | 0.777 | 0.345 |
| baseline-profile | 5 | 61,863.136 | 3.985 | 8.588 | 17.419 | 12.957 | 6.170 | 5.536 | 5.720 | 0.804 | 0.357 |
| etcd-control | 5 | 5,585.841 | 44.628 | 82.562 | 105.957 | 176.148 | 32.625 | 30.856 | 40.054 | 0.985 | 0.223 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline-control | leader | 10.925 | 5,323.434 | 0.186 | 0.234 | 72.973 | 1,136.045 | 876.384 | 1,298.289 |
| baseline-control | follower1 | 2.740 | 1,459.889 | 0.067 | 0.029 | 69.706 | 1,138.606 | 871.418 | 925.684 |
| baseline-control | follower2 | 2.738 | 1,455.850 | 0.067 | 0.029 | 71.352 | 1,137.520 | 870.623 | 925.641 |
| baseline-profile | leader | 10.937 | 5,320.581 | 0.187 | 0.235 | 72.074 | 1,136.312 | 888.773 | 1,364.395 |
| baseline-profile | follower1 | 2.744 | 1,452.319 | 0.068 | 0.030 | 68.413 | 1,139.362 | 883.765 | 946.777 |
| baseline-profile | follower2 | 2.740 | 1,452.142 | 0.067 | 0.029 | 69.890 | 1,138.278 | 883.799 | 950.113 |
| etcd-control | leader | 260.347 | 34,406.695 | 0.376 | 4.043 | 15.970 | 2,820.880 | 32.548 | 90.715 |
| etcd-control | follower1 | 81.687 | 14,525.596 | 0.192 | 0.441 | 45.732 | 2,654.874 | 30.093 | 71.016 |
| etcd-control | follower2 | 81.869 | 14,644.094 | 0.202 | 0.438 | 47.392 | 2,651.845 | 28.643 | 71.199 |

### run-38395499741-profile-c64.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline-control | 5 | 24,652.766 | 2.523 | 4.643 | 10.186 | 29.873 | 15.572 | 15.767 | 11.499 | 0.733 | 0.283 |
| baseline-profile | 5 | 24,631.381 | 2.516 | 4.657 | 9.207 | 30.334 | 15.646 | 15.919 | 11.633 | 0.743 | 0.285 |
| etcd-control | 5 | 5,028.025 | 12.010 | 25.037 | 31.737 | 194.455 | 48.200 | 48.583 | 47.206 | 0.973 | 0.235 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline-control | leader | 16.297 | 5,420.518 | 0.719 | 0.525 | 18.960 | 1,297.977 | 369.957 | 598.578 |
| baseline-control | follower1 | 4.821 | 1,530.582 | 0.144 | 0.112 | 18.320 | 1,305.204 | 365.784 | 405.734 |
| baseline-control | follower2 | 4.803 | 1,532.156 | 0.141 | 0.107 | 19.163 | 1,295.492 | 365.869 | 406.551 |
| baseline-profile | leader | 16.334 | 5,394.716 | 0.719 | 0.525 | 18.871 | 1,299.021 | 366.470 | 582.930 |
| baseline-profile | follower1 | 4.859 | 1,505.482 | 0.146 | 0.111 | 18.473 | 1,303.660 | 364.872 | 405.027 |
| baseline-profile | follower2 | 4.835 | 1,504.309 | 0.141 | 0.108 | 19.091 | 1,295.480 | 364.865 | 405.223 |
| etcd-control | leader | 269.198 | 35,492.595 | 1.343 | 4.604 | 12.487 | 2,902.596 | 28.998 | 78.254 |
| etcd-control | follower1 | 91.129 | 15,963.929 | 0.336 | 0.556 | 15.909 | 2,832.681 | 28.193 | 69.266 |
| etcd-control | follower2 | 91.331 | 15,951.163 | 0.337 | 0.554 | 15.975 | 2,831.360 | 22.136 | 68.547 |

### profile-c256-attribution.json

256/baseline-profile/leader: 3,713,242 operations; sampled total 12.350 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| frontend reader | 3.296 |
| peer send | 2.397 |
| frontend reply and writer | 1.578 |
| apply | 1.066 |
| WAL fsync | 0.733 |
| core and driver other | 0.627 |
| runtime scheduler | 0.598 |
| WAL other | 0.547 |
| runtime GC | 0.428 |
| raft | 0.417 |
| peer receive | 0.409 |
| entry encode | 0.213 |
| other | 0.038 |
| frontend other | 0.003 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 4.584 |
| TLS without socket syscall samples | 1.489 |
| crypto implementation | 1.023 |
| socket syscall samples | 3.094 |
| all syscall samples | 4.519 |
| HTTP2 and HPACK inclusive | 1.969 |
| HTTP2 and HPACK excluding TLS descendants | 0.627 |
| entry encode | 0.213 |
| entry walk and parse | 1.115 |
| raft | 0.417 |
| peer send | 2.397 |
| peer receive | 0.409 |
| WAL total | 1.279 |
| file write under WAL | 0.452 |
| file fsync | 0.733 |
| apply | 1.066 |
| MVCC and index | 0.819 |
| reply encode and queue | 0.439 |
| frontend writer including TLS | 1.578 |
| frontend reader including TLS | 3.296 |
| allocation and GC | 1.705 |
| scheduler | 0.617 |

256/baseline-profile/follower1: 3,713,242 operations; sampled total 5.588 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 1.505 |
| apply | 1.145 |
| peer send | 0.749 |
| WAL fsync | 0.735 |
| WAL other | 0.679 |
| runtime scheduler | 0.320 |
| core and driver other | 0.186 |
| raft | 0.159 |
| runtime GC | 0.105 |
| other | 0.005 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 1.554 |
| TLS without socket syscall samples | 0.461 |
| crypto implementation | 0.337 |
| socket syscall samples | 1.093 |
| all syscall samples | 2.494 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.139 |
| raft | 0.159 |
| peer send | 0.749 |
| peer receive | 1.505 |
| WAL total | 1.414 |
| file write under WAL | 0.571 |
| file fsync | 0.735 |
| apply | 1.145 |
| MVCC and index | 0.988 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.504 |
| scheduler | 0.320 |

256/baseline-profile/follower2: 3,713,242 operations; sampled total 5.047 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 1.379 |
| apply | 1.169 |
| WAL fsync | 0.692 |
| peer send | 0.584 |
| WAL other | 0.555 |
| runtime scheduler | 0.269 |
| core and driver other | 0.175 |
| raft | 0.124 |
| runtime GC | 0.078 |
| other | 0.022 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 1.352 |
| TLS without socket syscall samples | 0.458 |
| crypto implementation | 0.285 |
| socket syscall samples | 0.894 |
| all syscall samples | 2.119 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.166 |
| raft | 0.124 |
| peer send | 0.584 |
| peer receive | 1.379 |
| WAL total | 1.247 |
| file write under WAL | 0.450 |
| file fsync | 0.692 |
| apply | 1.169 |
| MVCC and index | 1.034 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.382 |
| scheduler | 0.277 |

### profile-c64-attribution.json

64/baseline-profile/leader: 1,474,020 operations; sampled total 28.480 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| frontend reader | 4.824 |
| peer send | 4.403 |
| WAL fsync | 4.233 |
| frontend reply and writer | 3.962 |
| peer receive | 2.395 |
| runtime scheduler | 2.341 |
| apply | 1.872 |
| core and driver other | 1.398 |
| WAL other | 1.248 |
| raft | 0.882 |
| runtime GC | 0.502 |
| entry encode | 0.285 |
| other | 0.115 |
| frontend other | 0.020 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 10.122 |
| TLS without socket syscall samples | 2.497 |
| crypto implementation | 1.330 |
| socket syscall samples | 7.625 |
| all syscall samples | 13.941 |
| HTTP2 and HPACK inclusive | 2.720 |
| HTTP2 and HPACK excluding TLS descendants | 0.936 |
| entry encode | 0.285 |
| entry walk and parse | 1.927 |
| raft | 0.882 |
| peer send | 4.403 |
| peer receive | 2.395 |
| WAL total | 5.482 |
| file write under WAL | 1.119 |
| file fsync | 4.233 |
| apply | 1.872 |
| MVCC and index | 1.364 |
| reply encode and queue | 0.488 |
| frontend writer including TLS | 3.962 |
| frontend reader including TLS | 4.824 |
| allocation and GC | 2.551 |
| scheduler | 2.402 |

64/baseline-profile/follower1: 1,474,020 operations; sampled total 14.179 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| WAL fsync | 3.996 |
| peer receive | 3.094 |
| peer send | 1.967 |
| apply | 1.608 |
| WAL other | 1.201 |
| runtime scheduler | 1.106 |
| core and driver other | 0.658 |
| raft | 0.387 |
| runtime GC | 0.122 |
| other | 0.041 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 3.725 |
| TLS without socket syscall samples | 0.909 |
| crypto implementation | 0.400 |
| socket syscall samples | 2.815 |
| all syscall samples | 8.236 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.608 |
| raft | 0.387 |
| peer send | 1.967 |
| peer receive | 3.094 |
| WAL total | 5.197 |
| file write under WAL | 1.092 |
| file fsync | 3.996 |
| apply | 1.608 |
| MVCC and index | 1.398 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.875 |
| scheduler | 1.119 |

64/baseline-profile/follower2: 1,474,020 operations; sampled total 14.267 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| WAL fsync | 4.132 |
| peer receive | 2.944 |
| peer send | 1.893 |
| apply | 1.574 |
| WAL other | 1.221 |
| runtime scheduler | 1.099 |
| core and driver other | 0.685 |
| raft | 0.522 |
| runtime GC | 0.129 |
| other | 0.068 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 3.501 |
| TLS without socket syscall samples | 0.801 |
| crypto implementation | 0.387 |
| socket syscall samples | 2.700 |
| all syscall samples | 8.209 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.567 |
| raft | 0.522 |
| peer send | 1.893 |
| peer receive | 2.944 |
| WAL total | 5.353 |
| file write under WAL | 1.072 |
| file fsync | 4.132 |
| apply | 1.574 |
| MVCC and index | 1.364 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.929 |
| scheduler | 1.106 |

## lean-screen

### run-38397622813-primary-c256.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 5 | 64,561.568 | 3.791 | 7.715 | 10.325 | 13.667 | 6.167 | 6.167 | 6.000 | 0.882 | 0.391 |
| etcd | 5 | 5,986.765 | 41.376 | 76.354 | 101.590 | 166.000 | 31.167 | 31.167 | 39.333 | 0.995 | 0.235 |
| peer | 5 | 66,154.775 | 3.725 | 8.180 | 11.061 | 13.333 | 6.167 | 6.000 | 6.000 | 0.887 | 0.398 |
| wake | 5 | 64,885.288 | 3.803 | 8.186 | 10.707 | 13.667 | 6.333 | 6.167 | 6.000 | 0.877 | 0.389 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 11.182 | 5,357.755 | 0.191 | 0.235 | 69.204 | 1,138.688 | 93.131 | 163.086 |
| baseline | follower1 | 2.938 | 1,470.676 | 0.071 | 0.032 | 64.865 | 1,143.125 | 87.878 | 110.504 |
| baseline | follower2 | 2.933 | 1,470.785 | 0.069 | 0.031 | 67.568 | 1,141.077 | 87.881 | 110.496 |
| etcd | leader | 260.113 | 34,298.481 | 0.379 | 4.043 | 16.146 | 2,815.795 | 28.298 | 86.402 |
| etcd | follower1 | 82.149 | 14,547.190 | 0.200 | 0.439 | 46.476 | 2,650.726 | 27.639 | 69.148 |
| etcd | follower2 | 81.523 | 14,580.771 | 0.202 | 0.439 | 47.544 | 2,648.542 | 22.208 | 69.500 |
| peer | leader | 11.189 | 5,357.170 | 0.191 | 0.111 | 69.364 | 1,139.166 | 92.588 | 165.102 |
| peer | follower1 | 2.941 | 1,469.182 | 0.059 | 0.032 | 64.935 | 1,143.398 | 88.385 | 110.125 |
| peer | follower2 | 2.935 | 1,463.407 | 0.059 | 0.031 | 66.667 | 1,141.828 | 88.395 | 110.559 |
| wake | leader | 11.182 | 5,361.117 | 0.191 | 0.235 | 69.444 | 1,139.439 | 92.938 | 162.504 |
| wake | follower1 | 2.940 | 1,465.155 | 0.072 | 0.032 | 65.217 | 1,142.647 | 87.833 | 111.156 |
| wake | follower2 | 2.934 | 1,467.649 | 0.068 | 0.031 | 67.039 | 1,141.555 | 87.893 | 110.566 |

### run-38397622813-primary-c64.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 5 | 21,399.681 | 2.900 | 5.377 | 8.424 | 34.167 | 18.833 | 18.333 | 12.500 | 0.749 | 0.269 |
| etcd | 5 | 4,350.204 | 14.005 | 28.249 | 35.130 | 224.000 | 56.000 | 56.500 | 50.833 | 0.974 | 0.221 |
| peer | 5 | 22,282.028 | 2.789 | 5.213 | 8.564 | 34.500 | 18.833 | 18.333 | 12.333 | 0.755 | 0.281 |
| wake | 5 | 22,255.898 | 2.808 | 5.031 | 8.156 | 33.333 | 17.833 | 18.167 | 12.333 | 0.742 | 0.274 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 16.399 | 5,458.392 | 0.719 | 0.524 | 19.262 | 1,293.517 | 91.039 | 153.699 |
| baseline | follower1 | 4.943 | 1,566.054 | 0.144 | 0.110 | 18.715 | 1,300.617 | 89.837 | 114.223 |
| baseline | follower2 | 4.923 | 1,566.470 | 0.140 | 0.109 | 18.898 | 1,297.681 | 89.841 | 115.000 |
| etcd | leader | 269.756 | 35,462.149 | 1.307 | 4.610 | 11.767 | 2,936.149 | 34.655 | 78.688 |
| etcd | follower1 | 91.092 | 15,835.084 | 0.335 | 0.565 | 15.464 | 2,852.932 | 30.315 | 69.520 |
| etcd | follower2 | 91.724 | 15,976.586 | 0.336 | 0.561 | 15.404 | 2,851.635 | 26.548 | 69.863 |
| peer | leader | 16.352 | 5,465.492 | 0.717 | 0.428 | 18.838 | 1,299.456 | 93.473 | 155.551 |
| peer | follower1 | 4.890 | 1,570.799 | 0.132 | 0.113 | 18.248 | 1,306.351 | 90.586 | 114.805 |
| peer | follower2 | 4.883 | 1,570.662 | 0.131 | 0.107 | 19.243 | 1,294.677 | 90.566 | 113.539 |
| wake | leader | 16.387 | 5,462.115 | 0.718 | 0.520 | 19.336 | 1,292.766 | 91.555 | 154.160 |
| wake | follower1 | 4.928 | 1,570.287 | 0.141 | 0.109 | 18.797 | 1,298.705 | 89.935 | 114.855 |
| wake | follower2 | 4.899 | 1,567.317 | 0.138 | 0.109 | 18.927 | 1,297.203 | 89.974 | 115.562 |

### run-38397622813-profile-c256.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline-profile | 5 | 67,259.139 | 3.647 | 8.757 | 17.102 | 12.240 | 5.682 | 5.294 | 5.469 | 0.867 | 0.385 |
| etcd-control | 5 | 5,665.224 | 43.621 | 80.860 | 98.546 | 172.426 | 32.220 | 30.540 | 38.693 | 0.996 | 0.229 |
| peer-profile | 5 | 69,273.646 | 3.531 | 9.163 | 16.390 | 12.532 | 5.850 | 5.411 | 5.676 | 0.868 | 0.393 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline-profile | leader | 10.969 | 5,306.344 | 0.187 | 0.234 | 71.207 | 1,138.007 | 959.532 | 1,428.254 |
| baseline-profile | follower1 | 2.751 | 1,437.410 | 0.070 | 0.031 | 66.824 | 1,141.742 | 954.190 | 1,038.379 |
| baseline-profile | follower2 | 2.749 | 1,438.924 | 0.068 | 0.030 | 67.950 | 1,140.506 | 954.700 | 1,041.422 |
| etcd-control | leader | 260.467 | 34,523.630 | 0.380 | 4.046 | 16.573 | 2,808.271 | 38.627 | 91.414 |
| etcd-control | follower1 | 82.529 | 14,693.501 | 0.193 | 0.437 | 47.344 | 2,649.630 | 30.169 | 71.625 |
| etcd-control | follower2 | 81.670 | 14,550.902 | 0.193 | 0.439 | 47.395 | 2,651.062 | 28.822 | 72.551 |
| peer-profile | leader | 10.975 | 5,300.394 | 0.187 | 0.110 | 70.486 | 1,138.268 | 982.665 | 1,577.227 |
| peer-profile | follower1 | 2.758 | 1,439.804 | 0.059 | 0.031 | 66.373 | 1,142.214 | 981.477 | 1,044.613 |
| peer-profile | follower2 | 2.757 | 1,433.713 | 0.058 | 0.030 | 67.763 | 1,140.962 | 980.125 | 1,056.945 |

### primary-c256-paired-changes.json

| variant | Put/s gain % | leader CPU saved µs/P | leader CPU saved % | write calls saved % | p99 reduction % |
| --- | --- | --- | --- | --- | --- |
| peer | 0.405 [-3.428, 12.478] | 0.167 [-0.333, 0.333] | 1.235 [-2.439, 2.439] | 52.765 [52.233, 53.299] | -3.463 [-12.247, 1.681] |
| wake | -0.042 [-3.794, 9.651] | 0.000 [-0.167, 0.167] | 0.000 [-1.235, 1.220] | 0.233 [-0.496, 0.454] | -3.744 [-12.976, 2.184] |

### primary-c64-paired-changes.json

| variant | Put/s gain % | leader CPU saved µs/P | leader CPU saved % | write calls saved % | p99 reduction % |
| --- | --- | --- | --- | --- | --- |
| peer | 2.712 [-13.594, 51.130] | 0.167 [-1.333, 0.667] | 0.476 [-3.774, 1.970] | 19.786 [16.897, 20.634] | 10.417 [-148.349, 88.355] |
| wake | 1.080 [-15.983, 50.953] | 1.000 [-0.167, 1.667] | 2.970 [-0.488, 4.762] | 1.006 [-1.937, 3.760] | 6.429 [-377.997, 88.804] |

### profile-c256-paired-changes.json

| variant | Put/s gain % | leader CPU saved µs/P | leader CPU saved % | write calls saved % | p99 reduction % |
| --- | --- | --- | --- | --- | --- |
| peer-profile | 0.921 [-4.834, 5.962] | -0.175 [-0.370, 0.532] | -1.426 [-3.041, 3.999] | 53.127 [53.008, 53.301] | -4.147 [-28.543, 1.008] |

### profile-c256-attribution.json

256/baseline-profile/leader: 4,092,445 operations; sampled total 12.047 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| frontend reader | 2.908 |
| peer send | 2.334 |
| frontend reply and writer | 1.473 |
| apply | 1.236 |
| WAL fsync | 0.750 |
| WAL other | 0.650 |
| core and driver other | 0.633 |
| runtime scheduler | 0.569 |
| runtime GC | 0.484 |
| peer receive | 0.428 |
| raft | 0.357 |
| entry encode | 0.198 |
| other | 0.027 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 4.354 |
| TLS without socket syscall samples | 1.407 |
| crypto implementation | 0.960 |
| socket syscall samples | 2.947 |
| all syscall samples | 4.455 |
| HTTP2 and HPACK inclusive | 1.715 |
| HTTP2 and HPACK excluding TLS descendants | 0.572 |
| entry encode | 0.198 |
| entry walk and parse | 1.273 |
| raft | 0.357 |
| peer send | 2.334 |
| peer receive | 0.428 |
| WAL total | 1.400 |
| file write under WAL | 0.535 |
| file fsync | 0.750 |
| apply | 1.236 |
| MVCC and index | 0.938 |
| reply encode and queue | 0.396 |
| frontend writer including TLS | 1.473 |
| frontend reader including TLS | 2.908 |
| allocation and GC | 1.549 |
| scheduler | 0.584 |

256/baseline-profile/follower1: 4,092,445 operations; sampled total 5.378 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 1.427 |
| apply | 1.161 |
| WAL fsync | 0.743 |
| peer send | 0.621 |
| WAL other | 0.606 |
| runtime scheduler | 0.369 |
| core and driver other | 0.247 |
| raft | 0.103 |
| runtime GC | 0.081 |
| other | 0.022 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 1.434 |
| TLS without socket syscall samples | 0.445 |
| crypto implementation | 0.291 |
| socket syscall samples | 0.990 |
| all syscall samples | 2.365 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.161 |
| raft | 0.103 |
| peer send | 0.621 |
| peer receive | 1.427 |
| WAL total | 1.349 |
| file write under WAL | 0.503 |
| file fsync | 0.743 |
| apply | 1.161 |
| MVCC and index | 0.990 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.396 |
| scheduler | 0.384 |

256/baseline-profile/follower2: 4,092,445 operations; sampled total 5.007 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 1.332 |
| apply | 1.124 |
| WAL fsync | 0.672 |
| WAL other | 0.584 |
| peer send | 0.574 |
| runtime scheduler | 0.308 |
| core and driver other | 0.208 |
| raft | 0.134 |
| runtime GC | 0.066 |
| frontend other | 0.002 |
| other | 0.002 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 1.334 |
| TLS without socket syscall samples | 0.420 |
| crypto implementation | 0.281 |
| socket syscall samples | 0.914 |
| all syscall samples | 2.143 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.122 |
| raft | 0.134 |
| peer send | 0.574 |
| peer receive | 1.332 |
| WAL total | 1.256 |
| file write under WAL | 0.474 |
| file fsync | 0.672 |
| apply | 1.124 |
| MVCC and index | 0.980 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.393 |
| scheduler | 0.320 |

256/peer-profile/leader: 4,128,374 operations; sampled total 12.022 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| frontend reader | 3.149 |
| peer send | 2.006 |
| frontend reply and writer | 1.516 |
| apply | 1.090 |
| core and driver other | 0.736 |
| WAL other | 0.717 |
| WAL fsync | 0.702 |
| runtime scheduler | 0.535 |
| runtime GC | 0.487 |
| peer receive | 0.477 |
| raft | 0.349 |
| entry encode | 0.223 |
| other | 0.034 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 3.076 |
| TLS without socket syscall samples | 1.359 |
| crypto implementation | 0.930 |
| socket syscall samples | 2.720 |
| all syscall samples | 4.246 |
| HTTP2 and HPACK inclusive | 1.785 |
| HTTP2 and HPACK excluding TLS descendants | 0.613 |
| entry encode | 0.223 |
| entry walk and parse | 1.155 |
| raft | 0.349 |
| peer send | 2.006 |
| peer receive | 0.477 |
| WAL total | 1.419 |
| file write under WAL | 0.606 |
| file fsync | 0.702 |
| apply | 1.090 |
| MVCC and index | 0.811 |
| reply encode and queue | 0.424 |
| frontend writer including TLS | 1.516 |
| frontend reader including TLS | 3.149 |
| allocation and GC | 1.664 |
| scheduler | 0.560 |

256/peer-profile/follower1: 4,128,374 operations; sampled total 5.334 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 1.402 |
| apply | 1.197 |
| WAL other | 0.712 |
| WAL fsync | 0.693 |
| peer send | 0.593 |
| runtime scheduler | 0.329 |
| core and driver other | 0.191 |
| raft | 0.126 |
| runtime GC | 0.075 |
| other | 0.015 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 0.862 |
| TLS without socket syscall samples | 0.446 |
| crypto implementation | 0.276 |
| socket syscall samples | 0.850 |
| all syscall samples | 2.209 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.197 |
| raft | 0.126 |
| peer send | 0.593 |
| peer receive | 1.402 |
| WAL total | 1.405 |
| file write under WAL | 0.623 |
| file fsync | 0.693 |
| apply | 1.197 |
| MVCC and index | 1.042 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.443 |
| scheduler | 0.342 |

256/peer-profile/follower2: 4,128,374 operations; sampled total 4.990 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 1.291 |
| apply | 1.165 |
| WAL fsync | 0.732 |
| peer send | 0.576 |
| WAL other | 0.564 |
| runtime scheduler | 0.291 |
| core and driver other | 0.172 |
| raft | 0.104 |
| runtime GC | 0.080 |
| other | 0.015 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 0.860 |
| TLS without socket syscall samples | 0.431 |
| crypto implementation | 0.303 |
| socket syscall samples | 0.894 |
| all syscall samples | 2.202 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.165 |
| raft | 0.104 |
| peer send | 0.576 |
| peer receive | 1.291 |
| WAL total | 1.296 |
| file write under WAL | 0.475 |
| file fsync | 0.732 |
| apply | 1.165 |
| MVCC and index | 1.003 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.378 |
| scheduler | 0.298 |

## host-lean

### c256

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 1 | 48,393.280 | 4.835 | 18.174 | 49.233 | 9.667 | 4.000 | 4.000 | 4.667 | 0.468 | 0.226 |
| etcd | 1 | 14,242.093 | 16.591 | 37.012 | 41.737 | 98.667 | 19.333 | 19.500 | 25.500 | 1.405 | 0.363 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 11.063 | 5,344.571 | 0.193 | 0.236 | 78.947 | 1,131.315 | 94.468 | 164.703 |
| baseline | follower1 | 2.897 | 1,463.073 | 0.065 | 0.028 | 80.214 | 1,130.496 | 87.805 | 113.172 |
| baseline | follower2 | 2.885 | 1,468.141 | 0.062 | 0.027 | 82.988 | 1,128.789 | 87.886 | 114.473 |
| etcd | leader | 259.005 | 34,871.580 | 0.409 | 4.018 | 21.008 | 2,712.166 | 40.519 | 93.949 |
| etcd | follower1 | 80.039 | 14,247.846 | 0.181 | 0.422 | 51.903 | 2,595.635 | 25.867 | 75.969 |
| etcd | follower2 | 80.196 | 14,310.315 | 0.189 | 0.424 | 50.934 | 2,597.547 | 34.972 | 75.777 |

### c64

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 1 | 19,102.927 | 3.070 | 8.942 | 87.137 | 19.667 | 7.833 | 8.333 | 7.500 | 0.376 | 0.143 |
| etcd | 1 | 9,689.979 | 5.820 | 20.499 | 61.950 | 118.500 | 29.333 | 29.167 | 31.667 | 1.148 | 0.307 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 16.251 | 5,474.917 | 0.701 | 0.529 | 20.964 | 1,277.201 | 92.543 | 161.172 |
| baseline | follower1 | 4.907 | 1,591.676 | 0.134 | 0.092 | 22.497 | 1,263.684 | 90.670 | 117.145 |
| baseline | follower2 | 4.894 | 1,579.618 | 0.132 | 0.093 | 22.736 | 1,261.841 | 90.602 | 118.352 |
| etcd | leader | 268.139 | 35,264.043 | 1.268 | 4.521 | 10.911 | 2,900.105 | 26.605 | 80.578 |
| etcd | follower1 | 87.988 | 14,744.580 | 0.318 | 0.543 | 15.389 | 2,791.219 | 34.236 | 76.688 |
| etcd | follower2 | 87.282 | 14,625.425 | 0.312 | 0.538 | 15.873 | 2,783.027 | 34.939 | 74.734 |

## initial

### run-38387195114-primary-c256.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 5 | 38,695.455 | 5.993 | 13.625 | 16.469 | 22.667 | 9.333 | 8.667 | 24.333 | 0.861 | 0.939 |
| etcd | 5 | 5,618.937 | 43.901 | 79.556 | 98.405 | 176.333 | 31.333 | 30.000 | 57.000 | 0.990 | 0.318 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 17.716 | 5,595.436 | 0.837 | 0.384 | 32.293 | 1,207.910 | 59.541 | 108.141 |
| baseline | follower1 | 3.844 | 1,538.994 | 0.103 | 0.067 | 31.024 | 1,213.099 | 54.235 | 74.039 |
| baseline | follower2 | 3.836 | 1,548.084 | 0.098 | 0.066 | 31.480 | 1,211.187 | 54.244 | 74.254 |
| etcd | leader | 264.829 | 34,608.967 | 0.557 | 4.034 | 15.732 | 2,822.963 | 33.106 | 84.801 |
| etcd | follower1 | 81.930 | 14,668.677 | 0.198 | 0.442 | 45.802 | 2,650.249 | 21.839 | 65.297 |
| etcd | follower2 | 81.427 | 14,494.135 | 0.201 | 0.443 | 47.393 | 2,650.795 | 30.137 | 65.527 |

### run-38387195114-primary-c64.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 5 | 26,908.524 | 2.227 | 5.534 | 7.682 | 32.333 | 16.000 | 13.667 | 29.000 | 0.872 | 0.775 |
| etcd | 5 | 5,136.628 | 11.634 | 24.616 | 31.278 | 189.667 | 44.333 | 41.333 | 62.000 | 0.977 | 0.320 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 22.284 | 5,724.090 | 1.238 | 0.870 | 13.870 | 1,377.075 | 56.889 | 100.020 |
| baseline | follower1 | 5.867 | 1,634.815 | 0.177 | 0.153 | 13.556 | 1,383.765 | 54.855 | 76.008 |
| baseline | follower2 | 5.851 | 1,640.463 | 0.175 | 0.149 | 13.831 | 1,378.441 | 54.853 | 74.902 |
| etcd | leader | 275.764 | 35,829.030 | 1.680 | 5.066 | 11.308 | 2,930.142 | 29.872 | 72.750 |
| etcd | follower1 | 92.077 | 15,924.024 | 0.333 | 0.557 | 15.552 | 2,833.203 | 23.824 | 63.602 |
| etcd | follower2 | 92.088 | 16,062.698 | 0.357 | 0.559 | 16.017 | 2,826.923 | 29.577 | 63.625 |

### run-38387195114-profile-c256.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline-control | 5 | 42,641.600 | 5.498 | 13.072 | 21.269 | 20.907 | 8.917 | 7.995 | 22.567 | 0.901 | 0.966 |
| baseline-profile | 5 | 39,958.547 | 5.949 | 14.158 | 25.031 | 22.544 | 9.774 | 8.754 | 24.104 | 0.889 | 0.957 |
| etcd-control | 5 | 5,751.793 | 42.985 | 79.906 | 94.608 | 173.347 | 31.101 | 29.947 | 56.423 | 0.997 | 0.326 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline-control | leader | 17.588 | 5,474.263 | 0.737 | 0.375 | 30.679 | 1,214.686 | 619.846 | 991.539 |
| baseline-control | follower1 | 3.683 | 1,453.778 | 0.104 | 0.071 | 29.153 | 1,221.676 | 615.275 | 663.367 |
| baseline-control | follower2 | 3.661 | 1,453.616 | 0.101 | 0.069 | 29.971 | 1,217.819 | 615.474 | 664.402 |
| baseline-profile | leader | 17.610 | 5,487.817 | 0.754 | 0.379 | 30.293 | 1,215.382 | 586.455 | 925.445 |
| baseline-profile | follower1 | 3.686 | 1,468.882 | 0.104 | 0.071 | 28.957 | 1,221.631 | 582.357 | 630.809 |
| baseline-profile | follower2 | 3.673 | 1,468.294 | 0.102 | 0.070 | 29.601 | 1,218.535 | 582.176 | 632.770 |
| etcd-control | leader | 264.021 | 34,695.734 | 0.519 | 4.044 | 14.812 | 2,839.068 | 38.977 | 91.926 |
| etcd-control | follower1 | 81.471 | 14,570.954 | 0.196 | 0.441 | 47.105 | 2,653.473 | 34.190 | 71.754 |
| etcd-control | follower2 | 81.359 | 14,750.380 | 0.205 | 0.439 | 48.034 | 2,650.756 | 27.257 | 71.355 |

### run-38387195114-profile-c64.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline-control | 5 | 26,465.855 | 2.274 | 5.377 | 10.582 | 31.955 | 15.395 | 14.167 | 29.038 | 0.845 | 0.767 |
| baseline-profile | 5 | 25,169.965 | 2.374 | 5.473 | 9.375 | 32.142 | 15.227 | 13.903 | 29.361 | 0.809 | 0.739 |
| etcd-control | 5 | 5,100.620 | 11.887 | 24.684 | 31.115 | 190.380 | 44.574 | 43.693 | 64.657 | 0.971 | 0.327 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline-control | leader | 21.604 | 5,708.989 | 1.174 | 0.855 | 14.781 | 1,359.569 | 414.070 | 643.820 |
| baseline-control | follower1 | 5.464 | 1,635.681 | 0.166 | 0.142 | 14.444 | 1,366.030 | 410.251 | 473.164 |
| baseline-control | follower2 | 5.436 | 1,633.491 | 0.164 | 0.139 | 14.823 | 1,358.900 | 410.397 | 471.047 |
| baseline-profile | leader | 21.571 | 5,714.628 | 1.196 | 0.855 | 15.140 | 1,352.947 | 395.362 | 611.266 |
| baseline-profile | follower1 | 5.466 | 1,642.344 | 0.166 | 0.138 | 14.887 | 1,357.575 | 392.194 | 450.020 |
| baseline-profile | follower2 | 5.428 | 1,640.955 | 0.164 | 0.133 | 15.510 | 1,346.514 | 392.040 | 448.410 |
| etcd-control | leader | 275.260 | 35,600.529 | 1.656 | 5.043 | 10.309 | 2,969.509 | 30.821 | 77.367 |
| etcd-control | follower1 | 90.372 | 15,624.728 | 0.327 | 0.556 | 15.632 | 2,835.417 | 28.716 | 69.348 |
| etcd-control | follower2 | 91.244 | 15,896.524 | 0.333 | 0.559 | 15.597 | 2,835.687 | 21.589 | 69.141 |

### profile-c256-attribution.json

256/baseline-profile/leader: 2,344,998 operations; sampled total 21.382 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| frontend reader | 5.932 |
| peer send | 3.211 |
| frontend reply and writer | 2.827 |
| runtime scheduler | 1.638 |
| WAL fsync | 1.569 |
| apply | 1.429 |
| core and driver other | 1.147 |
| WAL other | 1.062 |
| peer receive | 0.934 |
| raft | 0.759 |
| runtime GC | 0.512 |
| entry encode | 0.286 |
| other | 0.077 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 8.217 |
| TLS without socket syscall samples | 2.205 |
| crypto implementation | 1.207 |
| socket syscall samples | 6.013 |
| all syscall samples | 9.326 |
| HTTP2 and HPACK inclusive | 3.834 |
| HTTP2 and HPACK excluding TLS descendants | 0.964 |
| entry encode | 0.286 |
| entry walk and parse | 1.467 |
| raft | 0.759 |
| peer send | 3.211 |
| peer receive | 0.934 |
| WAL total | 2.631 |
| file write under WAL | 0.930 |
| file fsync | 1.569 |
| apply | 1.429 |
| MVCC and index | 1.134 |
| reply encode and queue | 0.520 |
| frontend writer including TLS | 2.827 |
| frontend reader including TLS | 5.932 |
| allocation and GC | 2.247 |
| scheduler | 1.689 |

256/baseline-profile/follower1: 2,344,998 operations; sampled total 9.045 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 2.162 |
| peer send | 1.586 |
| WAL fsync | 1.360 |
| apply | 1.237 |
| WAL other | 1.079 |
| runtime scheduler | 0.712 |
| core and driver other | 0.465 |
| raft | 0.299 |
| runtime GC | 0.119 |
| other | 0.026 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 2.815 |
| TLS without socket syscall samples | 0.610 |
| crypto implementation | 0.350 |
| socket syscall samples | 2.205 |
| all syscall samples | 4.652 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.237 |
| raft | 0.299 |
| peer send | 1.586 |
| peer receive | 2.162 |
| WAL total | 2.439 |
| file write under WAL | 0.938 |
| file fsync | 1.360 |
| apply | 1.237 |
| MVCC and index | 1.083 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.618 |
| scheduler | 0.729 |

256/baseline-profile/follower2: 2,344,998 operations; sampled total 8.124 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 1.928 |
| peer send | 1.488 |
| WAL fsync | 1.207 |
| apply | 1.173 |
| WAL other | 0.883 |
| runtime scheduler | 0.618 |
| core and driver other | 0.443 |
| raft | 0.243 |
| runtime GC | 0.124 |
| other | 0.017 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 2.537 |
| TLS without socket syscall samples | 0.563 |
| crypto implementation | 0.311 |
| socket syscall samples | 1.974 |
| all syscall samples | 4.149 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.173 |
| raft | 0.243 |
| peer send | 1.488 |
| peer receive | 1.928 |
| WAL total | 2.090 |
| file write under WAL | 0.776 |
| file fsync | 1.207 |
| apply | 1.173 |
| MVCC and index | 0.998 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.670 |
| scheduler | 0.648 |

### profile-c64-attribution.json

64/baseline-profile/leader: 1,492,704 operations; sampled total 31.091 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| frontend reader | 6.706 |
| frontend reply and writer | 6.096 |
| peer send | 3.812 |
| WAL fsync | 2.914 |
| runtime scheduler | 2.305 |
| apply | 1.795 |
| WAL other | 1.789 |
| core and driver other | 1.755 |
| peer receive | 1.742 |
| raft | 1.092 |
| runtime GC | 0.677 |
| entry encode | 0.355 |
| other | 0.047 |
| frontend other | 0.007 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 12.521 |
| TLS without socket syscall samples | 2.740 |
| crypto implementation | 1.239 |
| socket syscall samples | 9.781 |
| all syscall samples | 15.261 |
| HTTP2 and HPACK inclusive | 4.261 |
| HTTP2 and HPACK excluding TLS descendants | 1.092 |
| entry encode | 0.355 |
| entry walk and parse | 1.903 |
| raft | 1.092 |
| peer send | 3.812 |
| peer receive | 1.742 |
| WAL total | 4.703 |
| file write under WAL | 1.621 |
| file fsync | 2.914 |
| apply | 1.795 |
| MVCC and index | 1.380 |
| reply encode and queue | 0.536 |
| frontend writer including TLS | 6.096 |
| frontend reader including TLS | 6.706 |
| allocation and GC | 2.901 |
| scheduler | 2.385 |

64/baseline-profile/follower1: 1,492,704 operations; sampled total 14.062 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 3.303 |
| peer send | 2.539 |
| WAL fsync | 2.298 |
| apply | 1.588 |
| WAL other | 1.434 |
| runtime scheduler | 1.347 |
| core and driver other | 0.764 |
| raft | 0.616 |
| runtime GC | 0.121 |
| other | 0.054 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 4.355 |
| TLS without socket syscall samples | 0.904 |
| crypto implementation | 0.375 |
| socket syscall samples | 3.450 |
| all syscall samples | 7.356 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.588 |
| raft | 0.616 |
| peer send | 2.539 |
| peer receive | 3.303 |
| WAL total | 3.731 |
| file write under WAL | 1.300 |
| file fsync | 2.298 |
| apply | 1.588 |
| MVCC and index | 1.333 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 1.058 |
| scheduler | 1.367 |

64/baseline-profile/follower2: 1,492,704 operations; sampled total 13.057 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 3.122 |
| WAL fsync | 2.251 |
| peer send | 2.164 |
| apply | 1.521 |
| WAL other | 1.367 |
| runtime scheduler | 1.239 |
| core and driver other | 0.717 |
| raft | 0.543 |
| runtime GC | 0.107 |
| other | 0.027 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 3.839 |
| TLS without socket syscall samples | 0.831 |
| crypto implementation | 0.402 |
| socket syscall samples | 3.008 |
| all syscall samples | 6.820 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.507 |
| raft | 0.543 |
| peer send | 2.164 |
| peer receive | 3.122 |
| WAL total | 3.618 |
| file write under WAL | 1.172 |
| file fsync | 2.251 |
| apply | 1.521 |
| MVCC and index | 1.266 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.924 |
| scheduler | 1.266 |

## screen

### run-38389143652-primary-c256.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 5 | 40,739.838 | 5.803 | 12.995 | 15.549 | 21.000 | 9.667 | 8.667 | 23.667 | 0.868 | 0.960 |
| both | 5 | 42,160.038 | 5.498 | 13.647 | 21.020 | 20.667 | 10.000 | 8.667 | 22.667 | 0.874 | 0.956 |
| etcd | 5 | 6,078.689 | 40.339 | 76.419 | 89.089 | 160.333 | 29.333 | 28.667 | 54.667 | 0.987 | 0.331 |
| peer | 5 | 42,968.325 | 5.514 | 12.540 | 15.659 | 20.667 | 9.333 | 8.667 | 22.333 | 0.888 | 0.960 |
| wake | 5 | 41,316.810 | 5.591 | 12.647 | 17.233 | 20.667 | 9.000 | 8.333 | 23.000 | 0.867 | 0.964 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 18.014 | 5,620.976 | 0.887 | 0.388 | 29.880 | 1,218.014 | 59.716 | 107.602 |
| baseline | follower1 | 4.027 | 1,564.683 | 0.106 | 0.072 | 29.042 | 1,221.291 | 54.412 | 73.594 |
| baseline | follower2 | 4.000 | 1,563.604 | 0.101 | 0.069 | 30.769 | 1,213.372 | 54.420 | 74.176 |
| both | leader | 18.339 | 5,608.442 | 0.868 | 0.283 | 27.298 | 1,230.302 | 59.757 | 108.801 |
| both | follower1 | 4.203 | 1,563.119 | 0.101 | 0.079 | 26.478 | 1,234.944 | 54.941 | 73.965 |
| both | follower2 | 4.190 | 1,562.717 | 0.099 | 0.076 | 27.248 | 1,230.575 | 54.937 | 73.719 |
| etcd | leader | 263.937 | 34,697.488 | 0.563 | 4.059 | 15.617 | 2,816.819 | 41.059 | 85.719 |
| etcd | follower1 | 81.188 | 14,642.408 | 0.195 | 0.437 | 48.465 | 2,641.920 | 29.675 | 66.109 |
| etcd | follower2 | 81.218 | 14,637.636 | 0.200 | 0.438 | 47.695 | 2,642.466 | 24.120 | 65.176 |
| peer | leader | 18.170 | 5,620.250 | 0.843 | 0.278 | 28.736 | 1,222.929 | 58.998 | 111.992 |
| peer | follower1 | 4.092 | 1,559.132 | 0.097 | 0.075 | 27.778 | 1,228.663 | 54.967 | 74.129 |
| peer | follower2 | 4.095 | 1,558.340 | 0.095 | 0.071 | 29.499 | 1,219.106 | 54.992 | 74.043 |
| wake | leader | 17.938 | 5,613.947 | 0.859 | 0.388 | 31.056 | 1,212.006 | 57.944 | 110.246 |
| wake | follower1 | 3.983 | 1,558.546 | 0.102 | 0.070 | 29.762 | 1,217.741 | 54.395 | 74.191 |
| wake | follower2 | 3.984 | 1,558.649 | 0.100 | 0.066 | 31.579 | 1,210.095 | 54.391 | 73.301 |

### run-38389143652-primary-c64.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 5 | 22,162.311 | 2.724 | 6.148 | 8.184 | 37.000 | 19.333 | 16.333 | 33.667 | 0.829 | 0.748 |
| both | 5 | 23,406.399 | 2.578 | 5.914 | 10.173 | 36.667 | 19.000 | 16.333 | 33.333 | 0.858 | 0.762 |
| etcd | 5 | 4,481.981 | 13.218 | 27.921 | 34.616 | 213.000 | 51.667 | 48.333 | 72.333 | 0.985 | 0.332 |
| peer | 5 | 22,791.834 | 2.652 | 6.100 | 8.790 | 36.667 | 18.667 | 16.000 | 33.000 | 0.836 | 0.752 |
| wake | 5 | 22,650.013 | 2.653 | 6.239 | 11.331 | 37.000 | 19.333 | 16.667 | 33.667 | 0.831 | 0.747 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 22.160 | 5,720.577 | 1.254 | 0.869 | 14.382 | 1,367.108 | 56.282 | 101.871 |
| baseline | follower1 | 5.855 | 1,635.006 | 0.173 | 0.148 | 14.052 | 1,372.979 | 54.821 | 75.195 |
| baseline | follower2 | 5.813 | 1,631.050 | 0.172 | 0.144 | 14.423 | 1,366.426 | 54.819 | 76.062 |
| both | leader | 22.165 | 5,721.916 | 1.251 | 0.781 | 14.430 | 1,365.197 | 56.579 | 112.285 |
| both | follower1 | 5.851 | 1,636.750 | 0.159 | 0.146 | 14.211 | 1,370.795 | 55.428 | 75.199 |
| both | follower2 | 5.820 | 1,634.445 | 0.156 | 0.144 | 14.444 | 1,364.651 | 55.413 | 75.258 |
| etcd | leader | 275.198 | 35,293.651 | 1.701 | 5.040 | 9.743 | 2,997.999 | 27.195 | 72.859 |
| etcd | follower1 | 90.374 | 15,523.775 | 0.307 | 0.562 | 15.456 | 2,844.672 | 22.979 | 63.742 |
| etcd | follower2 | 90.869 | 15,535.059 | 0.330 | 0.570 | 14.970 | 2,854.639 | 22.900 | 63.934 |
| peer | leader | 22.034 | 5,722.087 | 1.239 | 0.777 | 14.627 | 1,361.374 | 58.136 | 108.363 |
| peer | follower1 | 5.789 | 1,636.186 | 0.161 | 0.145 | 14.306 | 1,367.791 | 55.343 | 75.742 |
| peer | follower2 | 5.784 | 1,632.574 | 0.160 | 0.142 | 14.606 | 1,361.783 | 55.334 | 74.984 |
| wake | leader | 22.226 | 5,727.796 | 1.248 | 0.870 | 14.265 | 1,369.702 | 56.338 | 104.379 |
| wake | follower1 | 5.891 | 1,640.178 | 0.174 | 0.150 | 13.812 | 1,378.031 | 54.924 | 75.250 |
| wake | follower2 | 5.863 | 1,636.748 | 0.175 | 0.145 | 14.313 | 1,368.610 | 54.919 | 74.961 |

### c256-paired-changes.json

| variant | Put/s gain % | leader CPU saved µs/P | leader CPU saved % | write calls saved % | p99 reduction % |
| --- | --- | --- | --- | --- | --- |
| both | 3.486 [-2.976, 7.501] | 0.667 [0.000, 1.000] | 3.175 [0.000, 4.545] | 26.671 [26.082, 29.998] | -5.019 [-19.510, 5.865] |
| peer | 5.485 [3.094, 7.902] | 1.000 [-0.333, 1.000] | 4.545 [-1.587, 4.762] | 28.558 [24.841, 29.460] | 5.850 [-5.456, 8.365] |
| wake | -0.221 [-4.821, 7.978] | 0.667 [-1.000, 1.667] | 3.125 [-4.762, 7.576] | 0.000 [-2.278, 0.750] | 1.024 [-319.158, 6.335] |

### c64-paired-changes.json

| variant | Put/s gain % | leader CPU saved µs/P | leader CPU saved % | write calls saved % | p99 reduction % |
| --- | --- | --- | --- | --- | --- |
| both | 2.911 [1.075, 8.294] | 0.667 [0.333, 1.667] | 1.786 [0.901, 4.425] | 10.454 [8.367, 11.085] | 3.815 [-2.376, 5.292] |
| peer | 0.750 [-1.040, 4.646] | 0.333 [0.333, 1.000] | 0.909 [0.893, 2.655] | 10.915 [8.037, 11.278] | 0.787 [-1.730, 8.232] |
| wake | -0.657 [-1.641, 3.118] | 0.333 [-0.667, 0.667] | 0.885 [-1.818, 1.802] | 0.218 [-2.316, 0.393] | -5.262 [-9.285, 5.889] |

## confirm

### run-38390703857-primary-c256.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 5 | 41,259.667 | 5.489 | 13.692 | 28.159 | 20.791 | 9.899 | 8.658 | 22.456 | 0.878 | 0.961 |
| etcd | 5 | 6,047.827 | 40.947 | 74.614 | 94.818 | 164.843 | 30.796 | 29.746 | 54.856 | 0.996 | 0.332 |
| peer | 5 | 42,380.886 | 5.625 | 12.627 | 17.558 | 20.996 | 9.908 | 8.631 | 22.923 | 0.890 | 0.971 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 17.744 | 5,486.071 | 0.790 | 0.381 | 29.130 | 1,220.884 | 601.753 | 964.977 |
| baseline | follower1 | 3.786 | 1,465.532 | 0.106 | 0.074 | 28.037 | 1,226.326 | 599.607 | 650.469 |
| baseline | follower2 | 3.771 | 1,465.538 | 0.104 | 0.072 | 28.875 | 1,222.108 | 599.594 | 643.516 |
| etcd | leader | 264.174 | 34,601.112 | 0.519 | 4.063 | 15.179 | 2,829.797 | 35.669 | 89.910 |
| etcd | follower1 | 81.272 | 14,640.701 | 0.194 | 0.440 | 46.437 | 2,650.771 | 29.395 | 70.664 |
| etcd | follower2 | 81.284 | 14,594.354 | 0.199 | 0.438 | 47.231 | 2,649.753 | 26.044 | 71.961 |
| peer | leader | 17.796 | 5,491.068 | 0.785 | 0.272 | 28.924 | 1,222.432 | 618.556 | 1,002.512 |
| peer | follower1 | 3.815 | 1,465.313 | 0.097 | 0.075 | 27.685 | 1,229.221 | 614.997 | 666.461 |
| peer | follower2 | 3.792 | 1,464.097 | 0.095 | 0.073 | 28.495 | 1,225.000 | 615.044 | 660.832 |

### run-38390703857-primary-c64.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 5 | 24,825.664 | 2.424 | 5.697 | 10.804 | 34.547 | 17.217 | 14.700 | 31.385 | 0.855 | 0.781 |
| etcd | 5 | 4,856.278 | 12.321 | 25.813 | 30.636 | 200.969 | 47.677 | 44.419 | 69.115 | 0.976 | 0.336 |
| peer | 5 | 24,795.211 | 2.419 | 5.659 | 9.856 | 34.261 | 17.238 | 14.886 | 31.078 | 0.857 | 0.772 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 21.914 | 5,693.467 | 1.156 | 0.864 | 14.287 | 1,368.281 | 386.743 | 624.500 |
| baseline | follower1 | 5.625 | 1,616.628 | 0.172 | 0.148 | 13.911 | 1,376.044 | 382.941 | 436.734 |
| baseline | follower2 | 5.589 | 1,615.119 | 0.171 | 0.144 | 14.385 | 1,366.329 | 382.918 | 436.289 |
| etcd | leader | 275.483 | 35,764.515 | 1.696 | 5.064 | 10.943 | 2,950.460 | 25.362 | 77.859 |
| etcd | follower1 | 91.126 | 15,897.478 | 0.326 | 0.559 | 15.785 | 2,836.920 | 29.428 | 68.832 |
| etcd | follower2 | 92.000 | 15,999.076 | 0.348 | 0.564 | 15.244 | 2,845.277 | 21.538 | 68.824 |
| peer | leader | 21.887 | 5,683.119 | 1.170 | 0.773 | 14.226 | 1,370.510 | 383.222 | 603.215 |
| peer | follower1 | 5.606 | 1,607.330 | 0.160 | 0.148 | 13.929 | 1,376.638 | 381.238 | 427.465 |
| peer | follower2 | 5.556 | 1,606.023 | 0.158 | 0.146 | 14.077 | 1,373.560 | 381.202 | 434.754 |

### run-38390703857-profile-c256.tar.gz

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline-profile | 5 | 46,880.975 | 4.982 | 11.899 | 25.943 | 18.753 | 8.199 | 7.106 | 20.474 | 0.884 | 0.961 |
| etcd-control | 5 | 6,530.986 | 37.627 | 70.011 | 83.138 | 152.613 | 29.300 | 27.644 | 49.210 | 0.997 | 0.322 |
| peer-profile | 5 | 43,000.939 | 5.090 | 20.293 | 33.952 | 18.979 | 7.956 | 6.931 | 20.875 | 0.841 | 0.923 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline-profile | leader | 17.280 | 5,444.312 | 0.758 | 0.369 | 34.329 | 1,200.382 | 673.274 | 1,061.285 |
| baseline-profile | follower1 | 3.511 | 1,432.887 | 0.096 | 0.063 | 32.949 | 1,205.359 | 669.092 | 731.945 |
| baseline-profile | follower2 | 3.499 | 1,431.390 | 0.094 | 0.061 | 33.876 | 1,201.965 | 669.240 | 722.008 |
| etcd-control | leader | 264.257 | 34,556.224 | 0.500 | 4.066 | 15.563 | 2,820.908 | 37.681 | 90.840 |
| etcd-control | follower1 | 81.175 | 14,557.589 | 0.194 | 0.437 | 46.995 | 2,645.709 | 26.060 | 72.855 |
| etcd-control | follower2 | 81.231 | 14,590.932 | 0.205 | 0.438 | 46.176 | 2,647.041 | 23.408 | 71.543 |
| peer-profile | leader | 17.256 | 5,455.243 | 0.783 | 0.256 | 35.107 | 1,197.702 | 624.367 | 1,046.648 |
| peer-profile | follower1 | 3.512 | 1,446.273 | 0.086 | 0.061 | 33.846 | 1,202.066 | 618.656 | 663.363 |
| peer-profile | follower2 | 3.498 | 1,446.759 | 0.085 | 0.060 | 34.504 | 1,198.818 | 618.765 | 662.879 |

### primary-c256-paired-changes.json

| variant | Put/s gain % | leader CPU saved µs/P | leader CPU saved % | write calls saved % | p99 reduction % |
| --- | --- | --- | --- | --- | --- |
| peer | 2.375 [-2.704, 8.071] | 0.153 [-0.834, 0.482] | 0.715 [-4.139, 2.265] | 28.197 [27.791, 28.960] | 6.270 [-2.755, 44.559] |

### primary-c64-paired-changes.json

| variant | Put/s gain % | leader CPU saved µs/P | leader CPU saved % | write calls saved % | p99 reduction % |
| --- | --- | --- | --- | --- | --- |
| peer | 0.979 [-2.028, 12.283] | 0.234 [-0.210, 0.491] | 0.675 [-0.610, 1.417] | 10.220 [9.950, 10.873] | 0.665 [0.014, 24.920] |

### profile-c256-paired-changes.json

| variant | Put/s gain % | leader CPU saved µs/P | leader CPU saved % | write calls saved % | p99 reduction % |
| --- | --- | --- | --- | --- | --- |
| peer-profile | -8.993 [-38.254, 1.302] | -0.288 [-1.175, -0.114] | -1.543 [-6.232, -0.621] | 30.162 [26.506, 31.356] | -73.587 [-226.716, 1.321] |

### profile-c256-attribution.json

256/baseline-profile/leader: 2,824,537 operations; sampled total 17.939 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| frontend reader | 5.342 |
| peer send | 2.542 |
| frontend reply and writer | 2.354 |
| runtime scheduler | 1.529 |
| WAL fsync | 1.115 |
| apply | 1.105 |
| core and driver other | 0.991 |
| WAL other | 0.952 |
| peer receive | 0.800 |
| raft | 0.474 |
| runtime GC | 0.474 |
| entry encode | 0.195 |
| other | 0.057 |
| frontend other | 0.007 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 6.911 |
| TLS without socket syscall samples | 1.855 |
| crypto implementation | 1.005 |
| socket syscall samples | 5.056 |
| all syscall samples | 7.601 |
| HTTP2 and HPACK inclusive | 3.353 |
| HTTP2 and HPACK excluding TLS descendants | 0.871 |
| entry encode | 0.195 |
| entry walk and parse | 1.165 |
| raft | 0.474 |
| peer send | 2.542 |
| peer receive | 0.800 |
| WAL total | 2.068 |
| file write under WAL | 0.790 |
| file fsync | 1.115 |
| apply | 1.105 |
| MVCC and index | 0.811 |
| reply encode and queue | 0.386 |
| frontend writer including TLS | 2.354 |
| frontend reader including TLS | 5.342 |
| allocation and GC | 1.675 |
| scheduler | 1.572 |

256/baseline-profile/follower1: 2,824,537 operations; sampled total 7.438 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 1.951 |
| WAL fsync | 1.136 |
| peer send | 1.136 |
| apply | 1.133 |
| WAL other | 0.846 |
| runtime scheduler | 0.531 |
| core and driver other | 0.365 |
| raft | 0.202 |
| runtime GC | 0.113 |
| other | 0.021 |
| frontend other | 0.004 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 2.245 |
| TLS without socket syscall samples | 0.566 |
| crypto implementation | 0.319 |
| socket syscall samples | 1.678 |
| all syscall samples | 3.703 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.129 |
| raft | 0.202 |
| peer send | 1.136 |
| peer receive | 1.951 |
| WAL total | 1.983 |
| file write under WAL | 0.743 |
| file fsync | 1.136 |
| apply | 1.133 |
| MVCC and index | 0.981 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.570 |
| scheduler | 0.552 |

256/baseline-profile/follower2: 2,824,537 operations; sampled total 6.497 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 1.731 |
| WAL fsync | 1.062 |
| apply | 0.984 |
| peer send | 0.963 |
| WAL other | 0.708 |
| runtime scheduler | 0.450 |
| core and driver other | 0.301 |
| raft | 0.163 |
| runtime GC | 0.117 |
| other | 0.018 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 1.866 |
| TLS without socket syscall samples | 0.492 |
| crypto implementation | 0.276 |
| socket syscall samples | 1.374 |
| all syscall samples | 3.151 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 0.984 |
| raft | 0.163 |
| peer send | 0.963 |
| peer receive | 1.731 |
| WAL total | 1.770 |
| file write under WAL | 0.584 |
| file fsync | 1.062 |
| apply | 0.984 |
| MVCC and index | 0.864 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.457 |
| scheduler | 0.460 |

256/peer-profile/leader: 2,465,840 operations; sampled total 18.302 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| frontend reader | 5.511 |
| frontend reply and writer | 2.539 |
| peer send | 2.490 |
| runtime scheduler | 1.513 |
| apply | 1.253 |
| WAL fsync | 1.233 |
| core and driver other | 1.018 |
| WAL other | 0.807 |
| peer receive | 0.730 |
| raft | 0.495 |
| runtime GC | 0.466 |
| entry encode | 0.207 |
| other | 0.036 |
| frontend other | 0.004 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 5.572 |
| TLS without socket syscall samples | 1.979 |
| crypto implementation | 1.075 |
| socket syscall samples | 4.883 |
| all syscall samples | 7.503 |
| HTTP2 and HPACK inclusive | 3.329 |
| HTTP2 and HPACK excluding TLS descendants | 0.799 |
| entry encode | 0.207 |
| entry walk and parse | 1.298 |
| raft | 0.495 |
| peer send | 2.490 |
| peer receive | 0.730 |
| WAL total | 2.040 |
| file write under WAL | 0.673 |
| file fsync | 1.233 |
| apply | 1.253 |
| MVCC and index | 0.912 |
| reply encode and queue | 0.349 |
| frontend writer including TLS | 2.539 |
| frontend reader including TLS | 5.511 |
| allocation and GC | 1.894 |
| scheduler | 1.545 |

256/peer-profile/follower1: 2,465,840 operations; sampled total 7.336 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 1.675 |
| apply | 1.237 |
| peer send | 1.127 |
| WAL fsync | 1.087 |
| WAL other | 0.868 |
| runtime scheduler | 0.580 |
| core and driver other | 0.373 |
| raft | 0.272 |
| runtime GC | 0.089 |
| other | 0.028 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |
| frontend other | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 1.196 |
| TLS without socket syscall samples | 0.552 |
| crypto implementation | 0.296 |
| socket syscall samples | 1.521 |
| all syscall samples | 3.532 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.237 |
| raft | 0.272 |
| peer send | 1.127 |
| peer receive | 1.675 |
| WAL total | 1.955 |
| file write under WAL | 0.791 |
| file fsync | 1.087 |
| apply | 1.237 |
| MVCC and index | 1.026 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.564 |
| scheduler | 0.596 |

256/peer-profile/follower2: 2,465,840 operations; sampled total 6.412 µs/Put.

| disjoint context | µs/Put |
| --- | --- |
| peer receive | 1.496 |
| peer send | 1.050 |
| apply | 1.022 |
| WAL fsync | 0.949 |
| WAL other | 0.742 |
| runtime scheduler | 0.434 |
| core and driver other | 0.341 |
| raft | 0.251 |
| runtime GC | 0.101 |
| other | 0.020 |
| frontend other | 0.004 |
| frontend reply and writer | 0.000 |
| frontend reader | 0.000 |
| entry encode | 0.000 |

| overlapping mechanism (do not add) | µs/Put |
| --- | --- |
| TLS with socket descendants | 0.985 |
| TLS without socket syscall samples | 0.499 |
| crypto implementation | 0.333 |
| socket syscall samples | 1.330 |
| all syscall samples | 3.021 |
| HTTP2 and HPACK inclusive | 0.000 |
| HTTP2 and HPACK excluding TLS descendants | 0.000 |
| entry encode | 0.000 |
| entry walk and parse | 1.022 |
| raft | 0.251 |
| peer send | 1.050 |
| peer receive | 1.496 |
| WAL total | 1.691 |
| file write under WAL | 0.608 |
| file fsync | 0.949 |
| apply | 1.022 |
| MVCC and index | 0.868 |
| reply encode and queue | 0.000 |
| frontend writer including TLS | 0.000 |
| frontend reader including TLS | 0.000 |
| allocation and GC | 0.560 |
| scheduler | 0.450 |

## host

### c256

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 1 | 45,613.819 | 5.205 | 11.718 | 16.607 | 17.667 | 4.667 | 4.333 | 18.667 | 0.806 | 0.851 |
| etcd | 1 | 14,331.666 | 17.162 | 35.599 | 40.633 | 101.667 | 19.667 | 19.000 | 36.667 | 1.457 | 0.525 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 16.546 | 5,524.802 | 1.020 | 0.367 | 54.845 | 1,155.209 | 57.206 | 102.668 |
| baseline | follower1 | 3.296 | 1,510.890 | 0.075 | 0.040 | 56.926 | 1,152.478 | 53.428 | 77.902 |
| baseline | follower2 | 3.278 | 1,498.625 | 0.073 | 0.038 | 57.034 | 1,152.341 | 53.358 | 76.555 |
| etcd | leader | 263.432 | 35,007.446 | 0.572 | 4.091 | 18.880 | 2,733.807 | 48.484 | 95.195 |
| etcd | follower1 | 79.134 | 13,997.115 | 0.188 | 0.427 | 50.934 | 2,597.683 | 32.056 | 69.355 |
| etcd | follower2 | 80.550 | 14,114.405 | 0.185 | 0.430 | 48.780 | 2,600.823 | 27.016 | 72.004 |

### c64

| variant | n | Put/s | p50 ms | p99 ms | p99.9 ms | leader µs/P | follower1 µs/P | follower2 µs/P | generator µs/P | leader cores | generator cores |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 1 | 18,812.689 | 3.307 | 6.663 | 8.439 | 27.667 | 9.667 | 9.667 | 23.000 | 0.520 | 0.433 |
| etcd | 1 | 9,616.315 | 6.149 | 16.083 | 20.024 | 122.333 | 29.000 | 28.000 | 44.667 | 1.176 | 0.430 |

| variant | role | alloc objects/P | alloc bytes/P | syscr/P | syscw/P | Puts/sync | proc write_bytes/P | retained heap MiB | retained RSS MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | leader | 21.134 | 5,688.993 | 1.229 | 0.868 | 18.149 | 1,307.853 | 56.183 | 108.383 |
| baseline | follower1 | 5.385 | 1,610.346 | 0.151 | 0.111 | 19.169 | 1,295.838 | 54.684 | 76.328 |
| baseline | follower2 | 5.359 | 1,613.088 | 0.148 | 0.110 | 19.481 | 1,292.288 | 54.716 | 78.273 |
| etcd | leader | 273.591 | 35,117.961 | 1.658 | 4.867 | 9.239 | 2,968.781 | 31.917 | 74.945 |
| etcd | follower1 | 87.286 | 14,284.628 | 0.302 | 0.550 | 14.684 | 2,804.668 | 29.351 | 69.699 |
| etcd | follower2 | 85.674 | 14,270.943 | 0.292 | 0.531 | 16.520 | 2,773.265 | 25.782 | 70.301 |
