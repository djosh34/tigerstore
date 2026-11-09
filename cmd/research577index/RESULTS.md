# #577 runner results

Measured commit: `20c8c0cbfb9e652141bf15cbdde268aacfe5ffb8`.
Workflow: https://github.com/djosh34/tigerstore/actions/runs/42697278993
- Matrix run 1: https://github.com/djosh34/tigerstore/actions/runs/42697278993/job/129432375944
- Matrix run 2: https://github.com/djosh34/tigerstore/actions/runs/42697278993/job/129432375277

Both jobs succeeded: Neoverse-N2, 4 cores, 16.7 GB RAM, Go 1.27.2, pinned CPU 0. Each cell is median / minimum across six repetitions (three per job, >=300 ms measured CPU per repetition). Raw JSONL, runner.txt and initial methodology are in artifacts research577index-1 and research577index-2. See corrected README: Raft internally clones payloads, contrary to the initial artifact methodology claim.

## A: memory and indexes

| Primitive | Size | ns/op median / min | allocations/op |
|---|---:|---:|---:|
| chase.serial | 4 MiB | 32.6 / 32.5 | 0 |
| chase.mlp8 | 4 MiB | 4.5 / 4.5 | 0 |
| chase.serial | 32 MiB | 132.6 / 106.4 | 0 |
| chase.mlp8 | 32 MiB | 6.2 / 6.1 | 0 |
| chase.serial | 128 MiB | 135.6 / 130.0 | 0 |
| chase.mlp8 | 128 MiB | 17.5 / 17.1 | 0 |
| chase.serial | 512 MiB | 147.6 / 146.5 | 0 |
| chase.mlp8 | 512 MiB | 19.4 / 19.3 | 0 |
| chase.serial | 1024 MiB | 157.8 / 156.4 | 0 |
| chase.mlp8 | 1024 MiB | 20.9 / 20.8 | 0 |
| flat.lookup | 100k keys | 164.8 / 158.9 | 0 |
| flat.batch8 | 100k keys | 124.0 / 123.2 | 0 |
| flat.batch16 | 100k keys | 119.4 / 118.9 | 0 |
| flat.insert | 100k keys | 106.4 / 105.7 | 0 |
| flat.delete | 100k keys | 156.9 / 153.8 | 0 |
| go-map.lookup | 100k keys | 123.0 / 122.5 | 0 |
| btree.seek | 100k keys | 580.4 / 575.9 | 0 |
| btree.namespace-list500 | 100k keys | 1,774.3 / 1,743.0 | 0 |
| btree.seek-iterate500 | 100k keys | 4,698.2 / 4,681.7 | 0 |
| btree.insert | 100k keys | 586.0 / 570.4 | 0.000166911 |
| btree.delete | 100k keys | 715.2 / 680.6 | 2.26844e-06 |
| array.seek | 100k keys | 521.7 / 501.3 | 0 |
| array.namespace-list500 | 100k keys | 1,338.9 / 1,263.8 | 0 |
| array.seek-iterate500 | 100k keys | 1,085.5 / 1,024.1 | 0 |
| array.insert | 100k keys | 26,657.1 / 26,596.7 | 0 |
| array.delete | 100k keys | 26,896.0 / 26,789.3 | 0 |
| flat.lookup | 1M keys | 422.5 / 414.7 | 0 |
| flat.batch8 | 1M keys | 349.2 / 341.2 | 0 |
| flat.batch16 | 1M keys | 341.8 / 329.8 | 0 |
| flat.insert | 1M keys | 182.9 / 180.7 | 0 |
| flat.delete | 1M keys | 407.7 / 396.4 | 0 |
| go-map.lookup | 1M keys | 346.3 / 343.3 | 0 |
| btree.seek | 1M keys | 1,467.2 / 1,440.3 | 0 |
| btree.namespace-list500 | 1M keys | 19,614.5 / 18,645.1 | 0 |
| btree.seek-iterate500 | 1M keys | 8,098.5 / 7,917.4 | 0 |
| btree.insert | 1M keys | 1,194.9 / 1,190.5 | 0.000520395 |
| btree.delete | 1M keys | 1,530.1 / 1,475.5 | 0 |
| array.seek | 1M keys | 1,230.0 / 1,207.6 | 0 |
| array.namespace-list500 | 1M keys | 9,867.8 / 8,854.0 | 0 |
| array.seek-iterate500 | 1M keys | 1,940.5 / 1,895.6 | 0 |
| array.insert | 1M keys | 241,342.2 / 239,990.9 | 0 |
| array.delete | 1M keys | 241,741.4 / 240,770.4 | 0 |
| flat.lookup | 10M keys | 544.4 / 533.4 | 0 |
| flat.batch8 | 10M keys | 448.4 / 444.8 | 0 |
| flat.batch16 | 10M keys | 440.5 / 435.2 | 0 |
| flat.insert | 10M keys | 293.3 / 284.3 | 0 |
| flat.delete | 10M keys | 547.5 / 540.6 | 0 |

Chases are ns/hop; batches are ns/key. Namespace LIST returns up to 500 keys, averaging 39.1 at 100k and 322.8 at 1M. seek-iterate500 visits 500 successors without checking namespace boundaries. Hash occupancy: 38.1%, 47.7%, 59.6% respectively. All hash and array mutations use pre-existing arena records; arena insertion/copy cost is excluded. The sorted array has O(N) actual shifts. Key lengths are 49–72 bytes (mean 52.56 at 1M).

## B: leader-side Raft

| Payload | Entries/Ready | Write ns/batch median / min | ReadIndex ns/round median / min | Write allocations / bytes per batch |
|---|---:|---:|---:|---:|
| 1 KiB | 1 | 5,461.2 / 5,440.2 | 2,247.4 / 2,180.5 | 83.031 / 4,409 |
| 1 KiB | 8 | 24,861.6 / 24,693.4 | 2,392.6 / 2,243.6 | 403.039 / 25,272 |
| 1 KiB | 64 | 173,118.0 / 172,009.2 | 2,425.1 / 2,354.3 | 2,877.056 / 188,889 |
| 64 KiB | 1 | 13,904.0 / 10,746.6 | 2,469.4 / 2,412.9 | 83.031 / 68,921 |
| 64 KiB | 8 | 62,120.9 / 54,694.7 | 2,523.5 / 2,368.2 | 403.040 / 541,368 |
| 64 KiB | 64 | 403,048.6 / 373,422.6 | 2,495.2 / 2,410.4 | 2,877.058 / 4,317,657 |
| 512 KiB | 1 | 37,020.6 / 33,302.1 | 2,505.0 / 2,383.4 | 83.032 / 527,673 |
| 512 KiB | 8 | 236,834.0 / 219,196.9 | 2,523.3 / 2,467.2 | 403.041 / 4,211,383 |
| 512 KiB | 64 | 1,616,380.7 / 1,516,918.1 | 2,450.2 / 2,360.2 | 2,877.070 / 33,677,837 |
| Bookkeeping only | 1 | 3.8 / 3.8 | — | 0 / 0 |
| Bookkeeping only | 8 | 15.0 / 14.8 | — | 0 / 0 |
| Bookkeeping only | 64 | 118.8 / 116.2 | — | 0 / 0 |

Each ReadIndex round allocates exactly 29 objects / 1,304 bytes in every configuration. Entry payloads are reused by the benchmark, but raft.appendEntry internally proto.Clones them; its allocation and copy costs are included. Source: https://github.com/etcd-io/raft/blob/v3.7.0/raft.go#L812-L820 . AsyncStorageWrites forbids Advance; append/apply response messages replace it. Both real followers run outside all CPU/allocation intervals. Leader MemoryStorage handlers are included; periodic compaction is excluded.

## Interpretation

- At 1M, successful point lookup is 346 ns for Go map; naive flat is 423 ns, improved to 342 ns with batch-16. Flat does not automatically beat Go map. One 1 GiB dependent hop is 158 ns; eight-way overlap is 20.9 ns/hop. Exact misses per lookup were not measured.
- String BTree seeks cost 1.47 us; inserts/deletes 1.19/1.53 us. At 10% insert, 10% delete, 2% namespace LIST, just the ordered mutation/LIST components contribute about 0.665 us per mixed op, before all other work.
- One 1 KiB-entry Raft batch costs 5.46 us, plus 2.25 us for a safe read round if required. This is per packed batch, not per client operation. At 512 KiB x64 entries, writes cost 1.62 ms including payload cloning.
- The sorted array makes iteration cheap but costs 241 us per mutation at 1M, so it cannot meet the mixed budget. No-allocation bookkeeping is only 3.8 ns for one descriptor, not a complete consensus implementation.

## Limits and spread

These are primitive floors, not a complete 10 us leader budget. GC service, serialization, network, disk, value processing, timers and long-run new-key churn are excluded. Queries use a random permutation (uniform without replacement within a pass), not IID draws. Only successful point lookups are measured. The cache-friendlier ordered alternative is a sorted array, not a B+tree. No assembly-prefetch variant or incompatible AsyncStorageWrites+Advance variant is claimed. No hardware cache-miss counters were collected.

The largest run-median difference is 14% (100k array seek); 1M Go-map medians are 349.4/345.8 ns, flat lookup 431.3/416.7 ns, and namespace BTree LIST 20,098.8/18,772.1 ns. Most other run medians differ by <5%. Both runs completed every requested size, including 10M hashes and 1 GiB chases; local execution was small smoke tests only.
