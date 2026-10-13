# Final-run validation audit

Run **38386296161**, source **da1581def021bc01df4ca32690b937be3dfa8232**. Read-only JSON/log/stack inspection; no Go execution or source edits. Performance conclusions belong to the separate metrics report.

## Identity and gates

Both cells record pristine base `0d406cb09aa99dea7ef1488476d6ebc6b032a6b8` and copied harness `0f82f687e5a21fa6265e508866f07dc3fffeaae9`. Decompressed hashes of all six retained symbol binaries match `versions.txt` and are identical between cells. Base source-manifest comparison against its Git archive finds exactly four differences: added `cmd/tigerstore/bench75.go`, the main observation hook, and the two documented server test-fixture overlays. No other base Go/module file differs. Observation/main and overlaid fixture hashes match the experiment archive's manifests.

All eight case JSONs and their driver validation records are valid. Final all-three-replica fences agree in revision and term **1**. Revision accounting agrees exactly in every case: pre-case **84097 + two accepted boundary Puts + observed slow-key Version + healthy successes + one post-close Put**. This accounts for the writes observed by the final fence, not a general linearizability proof.

Race logs report success for selected server/node/core lifetime tests and the separate cases sender tests. Eight standalone census logs report PASS, with before/after profiles and matching test binaries retained. The parent reproduced the strict overall summary successfully; this audit did not overwrite its derived CSVs.

## Actual slow-client outcomes

Each case **planned 4096** streams and reports zero reply reads after handshake. H/D are complete client-side HEADERS/DATA writes, **not server admissions**. Version is the independently observed committed overwrite count.

| Cell | Binary/mode | Actual attempts | H / D | Slow Version | Healthy Puts while stalled | Sender outcome |
|---|---|---:|---:|---:|---:|---|
| c64 | pristine base | 4096 | 4096 / 4096 | 4096 | 1536 | All sent, no error |
| c64 | experiment default | 4096 | 4096 / 4096 | 4096 | 1536 | All sent, no error |
| c64 | ring | 3088 | 3087 / 3087 | 256 | 1560 | Intentional-close error in HEADERS |
| c64 | arena | 4096 | 4096 / 4096 | 32 | 1556 | All sent, no error |
| c256 | pristine base | 4096 | 4096 / 4096 | 4096 | 1536 | All sent, no error |
| c256 | experiment default | 4096 | 4096 / 4096 | 4096 | 1536 | All sent, no error |
| c256 | ring | 3032 | 3032 / 3031 | 256 | 1564 | Intentional-close error in DATA |
| c256 | arena | 4096 | 4096 / 4096 | 32 | 1555 | All sent, no error |

All senders finished/joined; neither ring error preceded intentional close. Complete-DATA byte counts equal D × **1047**, and committed counts satisfy D+1. Ring's buffered/sent frames substantially exceed processed commits plus refusals: do not classify the remainder as admitted, committed, or individually refused. Arena's 32 committed calls pin 256 reserved capacity units; its eight-slot regions are not 256 active calls.

## Managed bounds and recovery

Budgets: **4096 global slots / 67108864 bytes**, **256 capacity units / 8388608 bytes per connection**. Held bytes include the **8388608-byte driver reserve**. Peaks below are cumulative since cluster start, including the prelude and large-value checks, not slow-only peaks.

| Cell/mode | Quota observed / sustained seconds | Stable held slots / bytes | Peak slots / bytes | Refusals |
|---|---:|---:|---:|---:|
| c64 ring | 0.016524 / 1.423758 | 256 / 9166848 | 257 / 10486752 | 588 |
| c64 arena | 0.024727 / 1.399061 | 256 / 9168128 | 480 / 10493736 | 4064 |
| c256 ring | 0.017201 / 1.349711 | 256 / 9166848 | 257 / 10486752 | 549 |
| c256 arena | 0.025678 / 1.387156 | 256 / 9168128 | 1904 / 14186288 | 4064 |

All three forced-GC stalled snapshots have the same held slots/bytes within each managed case. All 124/120/128/119 post-quota allocator polls respectively retain 256 slots, show refusal growth, remain within budgets and report zero explicit managed fallback. Every managed close recovers to **zero held slots / 8388608 held bytes**, exactly its before-slow baseline, and the post-close healthy Put advances revision. Whole-process heap does **not** return to its pre-case value; additional live log/MVCC history and runtime state remain.

Pristine base has no allocator interface (`null`); experiment default has mode **0**, zero prototype budgets/live/peak/refusal/fallback counters, and `TIGER_ALLOC_STRATEGY` is unset on all three processes. Default and pristine cases both complete/commit all 4096 slow Puts and pass the same size/progress checks. These observations do not establish binary or performance equivalence.

All eight size cases accept value lengths **1572825/1572826** (InternalRaftRequest **1572863/1572864** bytes), reject the next payload byte and the exact **1572864-byte value payload** (request **1572902** bytes), and preserve the prior accepted value after rejection.

## Blocked-prefix stacks

Only ring needs prefix dumps. Both leader dumps contain 17 goroutines: the two frontend readers are in **IO wait → TCP Read → TLS readRecordOrCCS → Framer.ReadFrame → readLoop**; both frontend writers are in **writeLoop select**; the node committer is in **commit select**, and persistence is also in select. No mutex/semaphore-wait or socket-Write stack appears in these two snapshots. Thus they do **not** show a reader holding the committer on a connection lock; the observed independent healthy progress is consistent with connection-local pressure, not a demonstrated global deadlock.

These are leader-only instantaneous stacks, not client-sender stacks or TCP packet/socket telemetry. They cannot pinpoint why a client write prefix stalled, exclude transient coupling between snapshots, or prove isolation under globally full pools, many slow connections, leadership changes, or other RPC workloads. No arena dump was required because its raw sender completed.

## Standalone source census

Each row is **256 sequential 1KiB Puts**, rate **1**, in a fresh process. Cell labels name runner jobs, not census concurrency. Counters include **client + leader + two followers**, not leader-only; retained heap includes startup and profiling state.

| Cell / census mode | Mallocs/Put | Allocated bytes/Put | HeapAlloc after forced GC (bytes) |
|---|---:|---:|---:|
| c64 pristine | 310.91015625 | 19134.53125 | 20246176 |
| c64 experiment base | 310.49609375 | 19140.125 | 19174520 |
| c64 ring | 295.1328125 | 16665.5 | 187009280 |
| c64 arena | 295.42578125 | 16701.6875 | 185744128 |
| c256 pristine | 309.74609375 | 19122.5625 | 18406808 |
| c256 experiment base | 309.98046875 | 19139.28125 | 19423208 |
| c256 ring | 296.109375 | 16751.625 | 187433752 |
| c256 arena | 294.859375 | 16650.9375 | 185738224 |

These census counts must not be substituted for concurrent leader metrics or summed from profile blocks as if identical to runtime Mallocs. Successful gates, bounded observations and recovered counters are positive evidence for the exercised paths—not proof that lifetime/race/durability bugs are absent, nor grounds to select a winner from allocation counts alone.
