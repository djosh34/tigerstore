# Put results

Medians of per-run statistics. Profile controls and primary runs are separate.

| Variant | Reps | Put/s | p99 ms | Leader CPU us/Put | Leader allocations/Put |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline | 1 | 48393.3 | 18.174 | 9.667 | 11.06 |
| etcd | 1 | 14242.1 | 37.012 | 98.667 | 259.01 |

CPU comes from the generator worker interval through drain. Allocation, I/O and sync counters use the wider all-replica-fenced interval. Etcd syncs count WAL observations, not backend commits. Forced-GC retained heap is only available for Tigerstore.
