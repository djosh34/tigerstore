# Issue 50 allocation experiment results

Recommendation: keep the PR77 allocation path for now. Neither prototype produced a clear leader CPU improvement that justifies its additional retained heap and ownership bookkeeping. If fixed admission capacity is required for a separate reason, independently retired ring slots are preferable to these eight-slot arena regions.

This is throwaway branch `exp/50-alloc`. No production PR or merge.

## Sources and execution

- Successful run: https://github.com/djosh34/tigerstore/actions/runs/38386296161
- Pristine production source: `0d406cb09aa99dea7ef1488476d6ebc6b032a6b8`, PR77.
- Experiment and observation source: `da1581def021bc01df4ca32690b937be3dfa8232`.
- Nine reused benchmark files match `0f82f687e5a21fa6265e508866f07dc3fffeaae9` byte for byte.
- Free `ubuntu-24.04-arm`, four Neoverse-N2 CPUs, Go 1.27.2. Leader CPU 0, followers CPUs 1 and 2, generator CPU 3. Each server has two Go processors on one pinned CPU; generator has one. Disk barriers remain enabled, with no early acknowledgment change.
- Two jobs, 64 and 256 clients. Each job alternates pristine base, ring and arena back to back, reversing order on even repetitions. Five fresh clusters per mode, 60000 successful 1KiB TLS leader Puts per primary cohort, 16 client connections and 4096 keys. All 30 windows passed exact success/revision, unchanged-term, replica catch-up and placement checks.
- The pristine performance binary differs only by the identical observation hook. Test overlays add the allocation/history fixture and its optional clock. Only allocation fixtures use production's 100ms clock; other fixtures retain their 10ms clock. All history checks also reopen every node's WAL and reread all 96 distinct historical values.

## Primary measurements

These are medians of five windows. Allocation counters cover the whole leader process, including background work and observation, not only managed storage. Retained heap is the whole leader heap after forced GC outside the timed window.

| Clients | Mode | Allocations/Put | Allocated bytes/Put | Leader CPU us/Put | Put/s | p99 ms | GC CPU share % | Retained heap MiB |
|---:|---|---:|---:|---:|---:|---:|---:|---:|
| 64 | Base | 30.45 | 6555 | 33.17 | 25806 | 5.647 | 1.34 | 108.55 |
| 64 | Ring | 18.37 | 3903 | 33.67 | 25651 | 5.267 | 0.70 | 159.76 |
| 64 | Arena | 18.42 | 3905 | 33.33 | 24884 | 5.210 | 0.58 | 159.48 |
| 256 | Base | 26.76 | 6451 | 20.83 | 43600 | 12.254 | 2.05 | 108.51 |
| 256 | Ring | 15.67 | 3731 | 20.67 | 44362 | 11.515 | 0.96 | 157.93 |
| 256 | Arena | 15.60 | 3721 | 20.67 | 44122 | 12.039 | 0.83 | 157.92 |

Median paired CPU changes versus base were ring 0.00% at 64 and -0.79% at 256, arena +0.50% at 64 and 0.00% at 256. CPU ranges overlap. The generator used about 0.97 to 0.99 cores at 256 clients, limiting conclusions from throughput alone. Arena's 64-client median p999 was 23.486ms versus base 8.613ms, so its lower median p99 is not a general tail-latency win.

GC share means delta Go `/cpu/classes/gc/total` divided by delta Go total-minus-idle classes. Go describes these as estimates, not OS CPU time. Go 1.27.2 `runtime/metrics.go:746-754` reads the last STW CPU-statistics snapshot, so the ratio is GC-boundary quantized. No forced GC runs inside primary windows. Preseeding also increases the heap goal, so lower GC share is not an isolated proof of allocation reuse's benefit. A memory-matched GC control was not run.

See [runs.csv](runs.csv), [summary.csv](summary.csv), [ranges and paired changes](ranges-and-paired-changes.json), and raw `c64`/`c256` outputs. Reproduce:

```sh
python3 experiments/alloc50/summarize.py evidence/alloc50/final-run
python3 evidence/alloc50/final-run/derive.py
```

## Allocation attribution

Separate rate-1 leader-process profiles contain exactly 2000 successful serial Puts. Matching production symbols and full zero-drop views confirm 30000 managed object contributions disappear in both modes, or 15/Put. Those are ten server sites, two node sites and three core sites. Canonical saved storage is 2416 allocated bytes/Put. Eighteen generic probe RPCs remain; their larger candidate stream size accounts for the 576-byte difference between the raw mixed-site byte delta and the canonical target delta.

The sixteenth stable serial site, owned `core.encodeBatch` bytes, remains 2000 objects and 2304000 bytes. Raft clones/messages/effects, peer protobuf decoding, MVCC/state lookup, HTTP2/HPACK frames and timers remain. Some may support further reuse, but this experiment did not address them. Entry.Data still backs retained Raft/MVCC state and must not recycle with a request slot.

The serial 15-object saving must not be projected onto the batched primary cohorts. Whole-process census tests include the client and all three nodes and must not be labeled leader-only. See [final attribution](profile-attribution/README.md), [validation audit](validation-audit.md), the earlier [source census](../../../experiments/alloc50/source-audit.md), and matching symbols in each job directory.

## Capacity and stalled readers

Both designs preseed at startup. Ring has 4096 independent slots in a circular free-slot queue. Arena has 512 eight-slot regions, reserved at HEADERS and sealed at reader batch boundaries. It returns a region only after every member's references retire. Both reserve 48MiB of body classes and fit the 64MiB managed budget with an 8MiB conservative driver reserve. Each connection may hold 256 charged capacity units and 8MiB of body/reply capacity. There is no managed heap fallback. Generic RPCs, connection/transport buffers, timers and control/error output remain outside the hard pool accounting. This is not a total server heap limit.

The stalled client advertises zero stream reply credit and never reads after SETTINGS. Its bounded sender plans 4096 Puts while an independent TLS connection continues healthy Puts. Actual sends differ from planned sends when TCP writing stalls.

| Clients | Mode | Completed raw DATA | Committed slow Puts | Charged slots | Refusals observed | Healthy Puts during hold |
|---:|---|---:|---:|---:|---:|---:|
| 64 | Base | 4096 | 4096 | No managed gauge | No managed gauge | 1536 |
| 64 | Ring | 3087 | 256 | 256 | 588 | 1560 |
| 64 | Arena | 4096 | 32 | 256 | 4064 | 1556 |
| 256 | Base | 4096 | 4096 | No managed gauge | No managed gauge | 1536 |
| 256 | Ring | 3031 | 256 | 256 | 549 | 1564 |
| 256 | Arena | 4096 | 32 | 256 | 4064 | 1555 |

Ring attempts were 3088 and 3032; remaining completed writes could still reside in TCP buffers and are not counted as server admissions. The main goroutine deliberately closed both unfinished senders, joined them, and validated close-induced errors. Their pre-close leader dumps show socket-read waits, writer select and node-committer select at those instants, not a writer-held global mutex. They do not establish the cause of the client TCP stalls or exclude transient bugs.

Both managed modes reached quota/refusal in about 16 to 26ms, sustained it for 1.35 to 1.42s with healthy progress and stable charged reservations, and recovered to zero held slots plus the 8MiB reserve after close. All sampled held/peak reservations remained within configured limits and fallback remained zero. Arena's singleton regions pin eight units per slow Put, so it commits only 32 versus ring's 256 at the same charged quota. The experiment-default diagnostic also sent and committed 4096, with unset strategy and no pool.

For `/alloc50/large`, a 1572826-byte value succeeds at the 1572864-byte InternalRaftRequest boundary. The next byte and a literal 1572864-byte value return request-too-large without changing the stored value. Raw 2MiB ingress and ownership tests also pass; the ingress ceiling is not the stored-value limit.

## Ownership and validation limits

- Immutable encoded Raft buffers, WAL data, peer copies and retained MVCC values keep their existing owners. Neither allocator pools them or acknowledges before the existing local append barrier.
- A workspace remains pinned while the reader uses it, while it is queued before node submission, while the node/core holds it, while a timer callback is active, while output is listed, and while actual writer frames reference its bytes.
- Core removes settled descriptors from both timeout and proposal lists before callbacks. Node shutdown prevents late acceptance and drains accepted inbox links, saving next before a callback can recycle a slot.
- `Timer.Stop(false)` retains the callback reference. END_STREAM bookkeeping is not enough to reclaim output; frames retire after `bw.Flush` or abort cleanup. Arena retirement additionally waits for every member.
- ARM race tests passed for reset-before-flush, reader pins, active callbacks, frame retirement, orphaned inbox links, core stale references and historical/WAL values. Separate sender race tests also passed. Local changed-package short suites, vet and lint passed with the required memory/parallelism limits. Tests and source inspection are not a proof against every interleaving; short mutex operations remain, including one completion lock per managed request.

Earlier failed attempts are retained in sibling directories. The first failed a 200ms allocation-fixture disk fence and an inappropriate seed GC-counter gate. The second completed 30 valid timings but submitted stalled writes synchronously before starting healthy traffic. This successful run, not a mixture of cherry-picked windows, supplies the table above.
