# Key index experiment

Does a prefix-compressed arena B+tree justify its complexity at 10 million Kubernetes-shaped keys?

This is throwaway code for tigerstore #47, not a storage engine. It must never merge to main.

## Run

```sh
go mod download
gofmt -w *.go
go vet ./...
GOMAXPROCS=1 go test -count=1 ./...
go build -o bench .
python3 run.py
```

`run.py` runs four fresh processes serially on CPU 6. Each uses Go 1.27.2, one application goroutine, `GOMAXPROCS=1`, `GOGC=100`, and a 4 GiB Go memory limit. Go runtime goroutines still exist. It refuses to start after 2026-10-13T22:00:00Z. The host is shared; process affinity is not CPU isolation. No system or network settings change.

## Workload

Ten resources, 1,000 namespaces per resource, 1,000 objects per namespace. Keys look like `/registry/configmaps/ns-00000/app-0123456789abcdef`. Names are 16 hex digits drawn from a fixed seeded set and sorted within each namespace. This is a balanced synthetic distribution, not a Kubernetes capture. Keys are 44 to 52 bytes long, depending on resource. All keys are initially live.

Each index maps a string to a 32-bit version head. All three use the same pointer-free version arena, with 64-byte descriptors holding revision, create revision, version number, lease, value location, predecessor, value length and flags. Ten percent of keys have four retained versions, the rest one. The common history costs about 83.5 bytes per initial key, including slab slack. No value payloads exist.

The main tree comparisons use a seeded random insertion permutation of all 10 million keys. The slice is sorted by definition. A fourth process bulk-loads the arena tree to 75% leaf slot occupancy, to expose the effect of loading order.

- The arena B+tree uses 4,076-byte leaves in pointer-free slabs, 128 leaf slots, a common prefix per leaf, suffix bytes, integer page IDs, linked leaves, and live subtree counts. Internal fanout is 64. It supports actual inserts, prefix changes, splits, updates and tombstone counts. Its experimental key limit is 96 bytes. It has no reclamation, physical deletion, persistence, snapshots or concurrency support.
- `google/btree v1.1.3` uses `BTreeG[item]`, degree 32. Keys are separately allocated strings. Count walks the range; the stock tree has no subtree counts.
- The sorted slice stores the same items and a 32-bit prefix-live-count array. A Go map buffers up to 65,536 distinct writes. A full buffer sorts and merges the entire base, then rebuilds the count array. A range lazily sorts a dirty buffer and merges both sources. No background worker does this work.

Prebuilt queries are excluded from timed operations and loaded bytes/key. Point operations cycle through 262,144 uniformly sampled requests with a fixed seed. Each timing repetition uses an offset into this stream. Five repetitions share each process and index; they are not five independent builds.

Get, miss and historical get run 200,000 operations per repetition. Historical get samples only the ten percent of keys with four versions, at the revision after their first update. That requires two predecessor steps before subsequent writes. Prefix range returns up to 100 version heads, without reconstructing key strings or reading values. Namespace count covers 1,000 keys; resource count covers one million. Historical namespace count walks visibility metadata. Current counts use live counts where available; they do not substitute for historical counts.

Put overwrite includes Get, a 64-byte version append and Put. Put create includes version append and tree insertion. Keys are prebuilt. There are one million timed overwrites and 100,000 timed creates. Buffer flushes occur synchronously inside Put. A final `put_*_drain` charges any residual buffer merge to that write phase; aggregate write results must include this time. Do not take the median of create batches, since one batch may contain the only full flush. A separate 32,768-update phase leaves a partly full buffer for the second read suite. The first range after these updates reports the lazy sort cost separately. That buffer is drained outside the following create timing.

## Memory and GC

Loaded heap bytes/key are live Go heap after full collection, including index capacity and key storage. They exclude generator strings, values, WAL, watches, event replay, lease attachment lists and recovery machinery. Index-only bytes/key subtract the separately measured common version arena. They are not RSS or a complete store capacity estimate.

Five forced collections run immediately after loading, before query allocation. Each records whole-collection wall time, GC CPU and stop-the-world pause separately. `/gc/scan/heap:bytes` records the scannable heap. Every timed batch also reports allocated bytes/op, automatic GC count, GC CPU and pause time. A full collection's duration is not a stop-the-world pause.

## Checks and limitations

The tests compare 75,000 random keys and 10,000 updates/tombstones against an independent map and sorted-key oracle. They check point results, ordered traversal and counts over 500 random ranges per index, bulk loading, a large prefix shrink and historical deletion/recreation visibility. The large run checks 10,000 point queries, 4,096 namespace counts and every inserted key. Result checksums should match across all implementations.

This does not prove production correctness, historical count scaling at millions of keys, deletion/compaction performance, or a sustained steady state. Writes retain all versions. Page/key reconstruction and serialization costs are absent. No etcd baseline runs because this measures index implementations, not comparable complete stores. No runner, Raft, TLS, network or durability claim follows from these numbers.
