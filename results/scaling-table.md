| Runner | Workload | etcd ops/s at 1024 / 4096 / 8192 | tigerstore ops/s at 1024 / 4096 / 8192 | Tiger leader cores at 1024 / 4096 / 8192 |
|---|---|---|---|---|
| arm64 | put | 13,200 / 12,974 / invalid | 85,638 / 92,071 / not run | 0.92 / 0.81 / not run |
| arm64 | txn | 12,550 / 11,964 / invalid | 66,137 / 71,416 / not run | 0.91 / 0.86 / not run |
| arm64 | range | 24,179 / 21,302 / 20,278 | 292,284 / 358,380 / 378,429 | 2.28 / 2.43 / 2.45 |
| amd64 | put | 12,725 / 12,252 / invalid | 59,858 / 61,702 / not run | 0.73 / 0.71 / not run |
| amd64 | txn | 9,779 / 9,213 / invalid | 45,469 / 50,991 / not run | 0.87 / 0.91 / not run |
| amd64 | range | 21,644 / 19,025 / 18,279 | 241,542 / 263,566 / 271,541 | 2.47 / 2.56 / 2.57 |
