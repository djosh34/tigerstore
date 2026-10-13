# Put results

Medians of per-run statistics. Profile controls and primary runs are separate.

| Variant | Reps | Put/s | p99 ms | Leader CPU us/Put | Leader allocations/Put |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline | 1 | 45613.8 | 11.718 | 17.667 | 16.55 |
| etcd | 1 | 14331.7 | 35.599 | 101.667 | 263.43 |

CPU comes from the generator worker interval through drain. Allocation, I/O and sync counters use the wider all-replica-fenced interval. Etcd syncs count WAL observations, not backend commits. Forced-GC retained heap is only available for Tigerstore.
