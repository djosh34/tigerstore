# Key index experiment

Does a prefix-compressed arena B+tree justify its complexity at 10 million Kubernetes-shaped keys?

This is throwaway code for tigerstore #47, not a storage engine. It must never merge to main.

## Result

Keep the arena B+tree direction. It used 47.8 index bytes/key after random insertion, versus 84.0 for the buffered slice and 92.6 for google/btree. Including the common MVCC arena, those became 131.3, 167.5 and 176.1 bytes/key. It won point reads and writes in this experiment. The slice won warm million-key counts and historical namespace counts before churn.

The slice's largest observed flush took 521 ms on the only application goroutine. Its first range after 32,768 updates spent 7.24 ms sorting the buffer. Charging both triggered flushes and the final drain gives 5,363 ns/overwrite and 3,558 ns/create, not the tens of nanoseconds seen when merely adding to the buffer.

Median full-collection GC CPU was 0.10 ms for the arena, 88.49 ms for the slice and 547.63 ms for google/btree. Scannable heaps were 0.066, 240.0 and 362.8 MB. Corresponding median stop-the-world pauses were only 5.4, 12.5 and 22.4 microseconds. Do not mistake full-collection CPU or wall time for a pause. The slice allocated about 4,582 bytes per timed overwrite including merges and spent 359 ms of GC CPU across that phase. The arena allocated about 63 bytes per overwrite from version slab growth and ran no automatic GC during that phase.

The random-load arena and bulk-load arena occupied 47.8 and 44.4 index bytes/key. This difference is measured, not a projected packing factor. The full-key page run used source commit `f33157e4fddff73de732fb9e374212decea26f03`. Its results are in [results/summary.md](results/summary.md), with raw JSON, machine details, process counters and validation output in the same directory. `summarize.py` verifies identical checksums across all four implementations and computes the table.

[results-heads-only/summary.md](results-heads-only/summary.md) records an earlier complete run at `c8cdfb1`, before adding the full-key page measurement. Stock-tree million-key count medians differed between the two builds, 37.1 ms and 61.0 ms, without GC in those timed batches. The cause is unproven. The final google/btree run recorded one major fault; the other final runs recorded none. The shared host had small amounts of swap traffic. Do not treat exact speed ratios as stable runner measurements.

Production gains remain a guess. These numbers support the index direction, not a complete-store capacity or throughput claim. Historical counts still walk versions; the arena is not faster for every operation. Keep a simple tree/map oracle and test reclamation, long keys and recovery separately before using a production implementation.

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

Get, miss and historical get run 200,000 operations per repetition. Historical get samples only the ten percent of keys with four versions, at the revision after their first update. That requires two predecessor steps before subsequent writes. `prefix_keys100` copies up to 100 full keys and version heads into a reused result buffer. `prefix_limit100` returns only heads, without reconstructing keys. Neither reads values. Namespace count covers 1,000 keys; resource count covers one million. Historical namespace count walks visibility metadata. Current counts use live counts where available; they do not substitute for historical counts.

Put overwrite includes Get, a 64-byte version append and Put. Put create includes version append and tree insertion. Keys are prebuilt. There are one million timed overwrites and 100,000 timed creates. Buffer flushes occur synchronously inside Put. A final `put_*_drain` charges any residual buffer merge to that write phase; aggregate write results must include this time. Do not take the median of create batches, since one batch may contain the only full flush. A separate 32,768-update phase leaves a partly full buffer for the second read suite. The first range after these updates reports the lazy sort cost separately. That buffer is drained outside the following create timing.

## Memory and GC

Loaded heap bytes/key are live Go heap after full collection, including index capacity and key storage. They exclude generator strings, values, WAL, watches, event replay, lease attachment lists and recovery machinery. Index-only bytes/key subtract the separately measured common version arena. They are not RSS or a complete store capacity estimate.

Five forced collections run immediately after loading, before query allocation. Each records whole-collection wall time, GC CPU and stop-the-world pause separately. `/gc/scan/heap:bytes` records the scannable heap. Every timed batch also reports allocated bytes/op, automatic GC count, GC CPU and pause time. A full collection's duration is not a stop-the-world pause.

## Checks and limitations

The tests compare 75,000 random keys and 10,000 updates/tombstones against an independent map and sorted-key oracle. They check point results, ordered traversal and counts over 500 random ranges per index, bulk loading, a large prefix shrink and historical deletion/recreation visibility. The large run checks 10,000 point queries, 4,096 namespace counts and every inserted key. Result checksums should match across all implementations.

This does not prove production correctness, historical count scaling at millions of keys, deletion/compaction performance, or a sustained steady state. Writes retain all versions. Serialization costs are absent. Only `prefix_keys100` includes key reconstruction. No etcd baseline runs because this measures index implementations, not comparable complete stores. No runner, Raft, TLS, network or durability claim follows from these numbers.
