All operation checksums match across four indexes.

| Metric | Arena random | Slice + buffer | google/btree random | Arena bulk |
|---|---:|---:|---:|---:|
| index_B_key | 47.804 | 84.000 | 92.642 | 44.448 |
| total_B_key | 131.271 | 167.468 | 176.110 | 127.916 |
| scan_heap_MB | 0.066 | 240.036 | 362.805 | 0.066 |
| GC_wall_ms | 1.175 | 109.000 | 578.274 | 1.188 |
| GC_cpu_ms | 0.100 | 88.486 | 547.632 | 0.116 |
| GC_STW_us | 5.375 | 12.500 | 22.417 | 6.458 |
| prefix_keys100 ns/op | 2,285 | 2,701 | 9,195 | 2,014 |
| get ns/op | 906 | 1,735 | 2,038 | 944 |
| miss ns/op | 894 | 1,658 | 2,038 | 899 |
| mvcc_hot4 ns/op | 1,545 | 2,332 | 2,643 | 1,595 |
| prefix_limit100 ns/op | 1,639 | 2,268 | 7,938 | 1,848 |
| count_ns1000 ns/op | 1,494 | 2,395 | 64,262 | 1,963 |
| count_resource1m ns/op | 313 | 192 | 60,965,381 | 310 |
| historical_count_ns1000 ns/op | 15,981 | 12,259 | 86,959 | 14,902 |
| put_overwrite ns/op | 1,270 | 5,363 | 2,381 | 1,316 |
| first_range_after_writes ns/op | 4,750 | 7,240,731 | 11,208 | 4,542 |
| prefix_keys100_after_overwrites ns/op | 2,307 | 3,650 | 9,322 | 2,008 |
| get_after_overwrites ns/op | 970 | 1,288 | 1,944 | 924 |
| miss_after_overwrites ns/op | 1,149 | 1,537 | 2,002 | 895 |
| mvcc_hot4_after_overwrites ns/op | 1,896 | 2,206 | 2,718 | 1,509 |
| prefix_limit100_after_overwrites ns/op | 1,974 | 3,462 | 8,847 | 1,486 |
| count_ns1000_after_overwrites ns/op | 2,171 | 5,150 | 64,921 | 1,466 |
| count_resource1m_after_overwrites ns/op | 325 | 2,456,009 | 62,576,260 | 307 |
| historical_count_ns1000_after_overwrites ns/op | 36,923 | 33,087 | 92,492 | 31,446 |
| put_create ns/op | 1,479 | 3,558 | 2,287 | 1,109 |
