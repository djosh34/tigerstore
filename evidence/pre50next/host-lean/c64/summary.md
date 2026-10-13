# Put results

Medians of per-run statistics. Profile controls and primary runs are separate.

| Variant | Reps | Put/s | p99 ms | Leader CPU us/Put | Leader allocations/Put |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline | 1 | 19102.9 | 8.942 | 19.667 | 16.25 |
| etcd | 1 | 9690.0 | 20.499 | 118.500 | 268.14 |

CPU comes from the generator worker interval through drain. Allocation, I/O and sync counters use the wider all-replica-fenced interval. Etcd syncs count WAL observations, not backend commits. Forced-GC retained heap is only available for Tigerstore.
