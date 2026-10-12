10M-key experiment complete. [Throwaway code, commands and raw results](https://github.com/djosh34/tigerstore/tree/exp/index-10m-20261012), measured source `f33157e`. Nothing merged.

Go 1.27.2 on `lima-opencode-vm`, Apple-reported ARM64, 8 vCPUs, 11 GiB RAM. One application goroutine, CPU 6 affinity, GOMAXPROCS=1, GOGC=100, 4 GiB Go memory limit. Shared host, not a GitHub runner.

Keys were synthetic `/registry/<resource>/<namespace>/<name>`, 44 to 52 bytes, with 1,000 keys per namespace. Both trees were random-loaded. Ten percent of keys had four retained versions. Historical Get targets those keys and follows two predecessors. All implementations used identical 64-byte version records.

Operation rows are ns/op. Reads are medians of five batches before writes, with an empty slice buffer. Puts include version append; their amortized means include every buffer merge and final drain. Pages copy full keys and version heads, not values.

| Measurement | Arena B+tree | Slice + 64K buffer | google/btree v1.1.3 |
|---|---:|---:|---:|
| Get | 906 | 1,735 | 2,038 |
| Put overwrite | 1,270 | 5,363 | 2,381 |
| Put new key | 1,479 | 3,558 | 2,287 |
| Prefix page, limit 100 | 2,285 | 2,701 | 9,195 |
| Current count, 1K keys | 1,494 | 2,395 | 64,262 |
| Current count, 1M keys | 313 | 192 | 60,965,381 |
| Historical Get | 1,545 | 2,332 | 2,643 |
| Historical count, 1K keys | 15,981 | 12,259 | 86,959 |
| Index bytes/key | 47.8 | 84.0 | 92.6 |
| Bytes/key including MVCC | 131.3 | 167.5 | 176.1 |
| Full GC CPU, ms | 0.10 | 88.5 | 547.6 |

GC scans 0.066/240/363 MB respectively. Median stop-the-world pauses were 5/13/22 microseconds, not the full collection times. The slice's largest merge took 521 ms; its first range after 32K updates took 7.24 ms. Bulk-loading the arena reduced its index to 44.4 bytes/key. Tests passed and every operation checksum matched across implementations.

I recommend keeping the arena B+tree direction and using google/btree as an oracle. Avoid whole-base slice merges on the committer. Historical counts still need attention; the slice won that read here.

Full-store gains remain a guess. These heap figures exclude values, watches and recovery machinery; no RPC, disk, compaction or etcd baseline ran. Stock million-key counts varied from 37 to 61 ms between two builds; the cause is unproven.
