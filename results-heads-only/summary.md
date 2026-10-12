All operation checksums match across four indexes.

| Metric | Arena random | Slice + buffer | google/btree random | Arena bulk |
|---|---:|---:|---:|---:|
| index_B_key | 47.804 | 84.000 | 92.642 | 44.448 |
| total_B_key | 131.271 | 167.468 | 176.110 | 127.916 |
| scan_heap_MB | 0.066 | 240.036 | 362.805 | 0.066 |
| GC_wall_ms | 1.206 | 99.634 | 527.680 | 1.181 |
| GC_cpu_ms | 0.156 | 82.697 | 488.596 | 0.100 |
| GC_STW_us | 9.083 | 11.833 | 18.834 | 4.250 |
| get ns/op | 925 | 1,421 | 1,939 | 882 |
| miss ns/op | 902 | 1,454 | 1,897 | 869 |
| mvcc_hot4 ns/op | 1,437 | 2,015 | 2,557 | 1,497 |
| prefix_limit100 ns/op | 1,628 | 2,097 | 5,635 | 1,484 |
| count_ns1000 ns/op | 1,534 | 2,190 | 40,866 | 1,689 |
| count_resource1m ns/op | 305 | 182 | 37,132,249 | 299 |
| historical_count_ns1000 ns/op | 15,845 | 11,720 | 49,088 | 15,013 |
| put_overwrite ns/op | 1,272 | 5,652 | 2,231 | 1,422 |
| first_range_after_writes ns/op | 5,125 | 7,472,065 | 9,417 | 4,625 |
| get_after_overwrites ns/op | 903 | 1,249 | 1,792 | 1,008 |
| miss_after_overwrites ns/op | 886 | 1,499 | 1,856 | 991 |
| mvcc_hot4_after_overwrites ns/op | 1,444 | 2,110 | 2,540 | 1,727 |
| prefix_limit100_after_overwrites ns/op | 1,623 | 3,295 | 5,722 | 1,821 |
| count_ns1000_after_overwrites ns/op | 1,469 | 4,992 | 41,538 | 1,493 |
| count_resource1m_after_overwrites ns/op | 310 | 2,333,582 | 37,927,119 | 315 |
| historical_count_ns1000_after_overwrites ns/op | 32,510 | 33,043 | 64,114 | 31,371 |
| put_create ns/op | 1,072 | 3,354 | 2,029 | 1,027 |
