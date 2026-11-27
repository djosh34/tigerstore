# Fetch cursor accounting and activation gate

Part of #833. Production range/watch cursor routing is enabled for validation. The settled #904/#908/#912 main-parity boundaries are implemented; qualifying Actions measurements and broad active-routing validation remain outstanding. This document records demonstrated gates, not a completed performance campaign.

## Allocation inventory

| Owner | Charged storage and lifetime |
| --- | --- |
| P: reply stages | Whole 8,192-byte pages (8,128-byte payload), full index/root units, request/cursor metadata. Admission precedes allocation. Partial pages remain fully charged. Cancellation retains storage until matching worker acknowledgement. |
| P: output finish | Socket replacement/copy escrow; escrow converts once into actual backing. Protected output keeps that backing across socket flushes. Empty completed backing is released without an unfunded shrink-copy. |
| P: normal reads | The full existing read-half entitlement is withheld from output, not merely currently allocated input. Only actual active-input-turn debt belongs to the independent R reserve. |
| P: CONTINUATION | Bounded 64-KiB backing, including unmaterialized backing for clients that normal read admission can activate. Explicit bounded allocation prevents append-capacity overshoot. Reclamation restores the entitlement. |
| P: connection handoff | A new connection/handshake-leftover copy cannot bypass ordinary input/connection funding. It waits in the existing handoff until funding exists; peers are not held behind it. No new request refusal or input-turn timeout is introduced. |
| P: ordinary ready replies | Protected within-share reads and applied Put/Delete/Txn replies cannot consume another fill's backing. They either use an already-funded non-growing socket, obtain a complete staged destination, or follow existing wait/held-write handling. No new applied-write refusal/capture format is introduced. |
| P: parked size hints | Physical framed-size hints reuse the existing body word. `roomRead` is back to main's 80-byte footprint, rather than growing its high-water container to 112 bytes. |
| W: shared core | One 80-KiB worker-stack/control reservation, one persistent worker and one running I/O operation. |
| W: cursor plan | The other 304 KiB of the 384-KiB cursor reservation includes the fixed 2,048 x 64-byte descriptors, bounded read/coalescing scratch, source/control rounding and completion ownership. Arena credit survives its arena-bearing callback frame. |
| W: legacy | Charged 8-KiB roots, full paged index capacity, rounded payload and retained needed results. No second B/32, uncharged map, or TTL eviction of a needed dependency. |
| W/P: canonical legacy adapter | Canonical protobuf bytes use W when the complete input fits; otherwise they use P. The existing transport/order entries are explicitly W-funded (8 bytes each, wide form 16), not uncharged per-record padding. W-backed input transfers to a pre-funded complete P destination before retirement. Large P-backed output retains its charged W index until DATA/reset; tiny selected output has a funded copy and retires the all-input view immediately. Main's sort representation is unchanged. |
| P: canonical retry copies | Exact-capacity record construction avoids append-growth overlap. Raw retry promises include canonical/projection allocation bounds; a structurally non-fitting raw finish uses the canonical adapter instead of waiting indefinitely. |
| Main parity: inherited watch controls | #912 leaves unchanged control output and held backing outside the proof. The inherited held-control debit is identified separately; admission/backpressure is unchanged. `held` stays exactly 56 bytes, rather than exempting the former added 24 bytes per entry. |
| P: ordered-output handles | Intrusive stage/material nodes live inside their already-funded root/control units. Handles reuse `held`'s existing four padding bytes. No new map, growing index, per-control owner or retained high-water allocation is introduced. |

All physical source leases survive through I/O acknowledgement. Completion cannot publish to a replacement client/store/watcher, and cannot clear a newer operation. WAL generations and checkpoint inodes are independent of logical record offsets. Normal validated current values can promote to the ordinary store slab; originating fulfillment is not reported as distinct-request reuse.

## The three round-5 contracts

1. `TestAStagedReplyCannotSpendTheLogicalInputTurn`: at 83 MiB, a completed window-blocked response retains 1 MiB of logical output-reserve usage with independent P backing. With the read half full and `partial == 0`, the real frame reader encounters DATA ahead of WINDOW_UPDATE and obtains the full 2-MiB input turn. The update and a separately funded 8-KiB reader progress while a larger waiter stays live, without advancing the clock. `TestAnUnbackedOutputCannotBorrowTheInputGuarantee` covers exhausted P. Both explicitly enable cursor-mode accounting.
2. `TestSharedLegacyCursorRuntimeBudgetMatrix`: two distinct 1,280-KiB values for a keys-only value sort, a held-reply consumer and a new cursor, in both admission orders and both running-cancellation cases. Needed results survive expiry advancement. Both W and independently backed P are asserted on every transition, not merely separate allocator arithmetic.
3. `TestSmallWatchCommitsToQueueBeforeDataAndSurvivesInstall`: complete validated small messages advance their watch cursor at ordered queue transfer even with zero connection DATA window. Real snapshot installation cannot retract/replay that message; reopening the window yields original header/current/previous values exactly once. Recreated-member/stale-ACK coverage is separate. `TestLargeWatchTakesHoldsOnlyWithFirstData` retains the stricter first-DATA rule for genuinely large holds.

## Combined runtime matrix

Maximum observed funding ledgers across both admission orders and cancellation cases:

| Configured budget | Shared W peak / limit (bytes) | Independently backed P peak / limit (bytes) | Coexistence |
| --- | --- | --- | --- |
| 83 MiB | 2,719,744 / 2,719,744 | 4,587,520 / 5,439,488 | Non-fitting legacy/cursor combination waits. |
| 137 MiB | 4,046,848 / 4,489,216 | 5,668,864 / 8,978,432 | Fitting reservations coexist. |
| 191 MiB | 4,046,848 / 6,258,688 | 6,684,672 / 12,517,376 | Fitting reservations coexist. |

These are real mixed-consumer cases, including high-water metadata and held-reply retry. The separate #912 control-parity test below additionally checks protected P while inherited control replies accumulate. Socket-drain parity additionally checks identical main/candidate wire length and capacity after opening both windows. Broad active-routing validation remains in progress.

## Option A / #904

`TestMainHeldCaptureParityAndBoundedMaterialization904` unconditionally asserts identical main/cursor-mode `hr.counted`, client `heldCounted`, and capture `growOut` delta: **8,388,608 bytes** at 83 MiB. That regression excludes exactly the unchanged inherited capture debit; the separate #908 ruling below applies only to main's unchanged sort arrays. Materialization, the 1,343,244-byte reply, retained results, work structures and output/copy ownership are not exempt. All 65,536 previous values are decoded and partial output cannot return live backing.

## Option A / #908: unchanged main sort scratch

A pristine-main reproducer at `2e1d1e1a796c5c926802d26b789b23bada57d15e` successfully serves a value sort over 65,536 four-byte keys with one-byte values, `limit=1`, and `keys_only=true`.

The unchanged `shapeKVs` holds its candidate `[][]byte` backing while allocating/sorting `[]kvRec`:

- `kvRec`: 96 bytes; slice element: 24 bytes.
- Unavoidable lower bound: `65,536 * (96 + 24) = 7,864,320 bytes`.
- Actual candidate capacity was 72,362, giving **8,028,144 bytes** before allocator rounding, payload, output, or growth-copy overlap.
- Existing P: **5,439,488 bytes**. Even P+R is only **7,536,640 bytes**.
- No held-write capture exists in this state. It is not #904.

The manager's option-A ruling excludes those unchanged arrays. `TestLegacySortScratchParity908` observes real main/cursor capacities and asserts the exact 65,536-key reference, with no capture. `TestLegacyDenseMetadataUsesBoundedMaterializationLookup` now completes the real 115,000-record sort and asserts scratch **14,472,432 bytes on both paths**, bounded lookup, finite progress, correct selection, P/W ceilings and final retirement. Only main's candidate/parsed-record arrays are excluded; new transport indices, canonical data, copies and output remain charged. #908 is referenced, not repaired.

`TestLegacyTinyInputsWithOversizedLastRecordComplete` additionally preserves main's valid pre-next-record overshoot: 100,000 tiny inputs followed by a final 1,280-KiB value. Its deterministic red was the added transport-prefix debit, not #908. Separating that explicitly charged W metadata from canonical bytes makes it finish without changing main's admission, comparator, sorting algorithm or selected set.

## Main watch-control parity / #912

Pristine main `0cd3dd28c5550cea1e2ea4f5dddd264687820f74` accepts 250,000 progress requests on a DATA-window-blocked watch stream. Their 1,750,000 gRPC input bytes fit one normal 2-MiB input turn. Already-counted P reaches **5,909,630 > 5,439,488 bytes**. The separate main `[]held` capacity is **294,180 x 56 = 16,474,080 bytes**, alone larger than P+R. No keys, writes, capture, sort, expiry or sleeps are involved. The exact standalone reproducer is published on #912.

The manager's settled #912 boundary excludes inherited control output and backing, not new growth. `TestWatchControlCannotSpendProtectedFetchCapacity` now compares 250,000 real progress requests against the legacy path: **5,500,000 queued bytes**, **294,180 held entries of exactly 56 bytes**, identical counts/bytes/capacities. Protected fetch P remains **5,431,326 / 5,439,488 bytes**. No request is refused and no timeout advances. Reset and connection close retire the separate inherited debit. Opening both windows preserves main's exact socket length/capacity; only capacity actually added by control serialization is attributed to #912. That debit follows shrink/drop, never becoming free P. A fetch borrowing a control-grown socket reserves an independent 64-KiB finish promise; an empty control-grown socket is dropped before replacement rather than retained for the fetch. `TestControlSocketCannotBecomeUnfundedFetchBacking` verifies the mixed-client lifetime and funded replacement. `TestHeldOutputHandlesKeepMainLayoutAndRetire` checks exact size/offsets, funded owner sizes, handle wrap/collision, multiplexed reset and final drain retirement. #912 itself is unchanged.

## Activation-trial corrections

An active-routing broad race trial exposed deferred-handshake starvation, plus two storage-shape assumptions in unit tests. `TestDeferredHandshakeAlsoRequestsBufferRotation` deterministically fails before the fairness correction and passes after it: deferred leftovers participate in ordinary idle-buffer rotation. The unchanged 60-client integration case then completes with zero refusals. Resident/metadata fills can finish during planning without borrowing a worker arena; whole-reply assertions accept either valid complete backing, and replacement checks distinguish new queued output from leaked old owners.

## Active-routing checksum continuation

The first active-routing Actions run (`46022298611`, head `925582f3`) failed simulator/replay cases. `TestSeeds/1` reproduced locally without races: a completed descriptor for one version of a key seeded a newly selected, zero-byte version of the same key when a source/descriptor boundary ended the piece. ACK matching now requires matching byte progress, length and checksum, not merely key ID. `TestWatchFillChecksumDoesNotCrossVersionAtDescriptorBoundary` decodes all 2,050 same-key versions, fails before the fix and passes after it. The original simulator `TestSeeds/1` passes after the fix; no simulator assertion or expected failure is changed.

## Measurement state

Local original-workload diagnostics passed: both legacy control and cursor tests retain every value, retry, deadline and budget assertion. The cursor cold-worker test explicitly uses the supported refused-NOWAIT condition, because kernel readahead can otherwise warm later pieces after one fadvise. Its race 80-range phase was 2.267 s. This is not an equal-cache paired comparison or a qualifying campaign.

After merging main's #907 fix normally, targeted race validation passes (8.276 s), including all three contracts, combined runtime matrix, #904/#908/#912 parity, dense materialization and large-watch first-DATA holds. All four required v2.14.0 lint configurations reported zero issues at the first active-routing checkpoint; they are rerun after subsequent fixes. The earlier expanded 71.005-s run predates the #912 ruling and failed its then-unexempted control gate; the first active-routing Actions run exposed a real checksum continuation bug, described below, and a new run is required after its fix.

The offline `scripts/fetch-measure-gate.py` checks named/reuse evidence and thresholds (15 synthetic tests), but explicitly does **not** certify the separate original-workload gate. Initial original-workload tooling now lives in `scripts/fetch-original-measure.py` and `bench/fetch-measure/`: portable matched overlays preserve all original value/share/deadline/fetch assertions, collect every RPC's end-to-end duration and busy retries, blocking/NOWAIT calls/bytes and kernel physical-read bytes, and archive source patches/raw logs. `bench.yml` has an opt-in `fetch_measure` dispatch path, with race and 0/5-ms durable-WAL-batch modes, five AB/BA pairs, automatic extra-five extension and immediate hard-target stop. The adapter/gate has 15 synthetic tests (30 including the existing named/reuse gate); actionlint passes. The original overlays have now compiled and executed on Actions for both refs; the hard-target outcome is recorded below. The complete named/Kubernetes/reuse instrumentation and adapter remain unfinished.

**Approved pin update:** the manager's option-A ruling permits re-pinning the deliberate production-routing change. `TestLeaseFreeForkTrace` now pins `cee1eb73ec6ac127 / df529d402093fa52`; its comment names hot-watch staging/socket flush batching at 1,847 ms of aging on node 2 and links the [three-tree findings](https://github.com/djosh34/tigerstore/pull/897#issuecomment-7222981838). The scoped gosim run passes aging (10,480 witness steps), fork0 (15,348 steps), all five existing semantic checkers, and the new pin. Nothing else is weakened. All four v2.14.0 lint configurations report zero issues.

**Measurement ruling required:** [original non-race campaign 46034356851](https://github.com/djosh34/tigerstore/actions/runs/46034356851), immutable main `55b32bb9` versus candidate `c084b588`, stopped on its first AB pair. Both used 96 MiB, the original 8,000-key/80-range data and assertions, fadvise preparation, and the same supported refused-NOWAIT condition. Main: wall 1,684.728781 ms; p95 1,669.033559 ms; max 1,682.944488 ms; 95 total busy retries, maximum 3/range. Candidate: wall 1,714.413694 ms; p95 1,685.987438 ms; max 1,711.896241 ms; zero retries. The candidate misses the existing 1,000-ms non-race p95/max gate. The adapter emitted `RULING`; no deadline was relaxed and no five-pair median or throughput-regression verdict is claimed. The single-pair throughput ratio is 0.9826850934, not a paired-median result. Both sides pass all value/fetch/share assertions; raw 80-RPC samples and SHA-checked logs remain in the Actions artifact.

Per the stop-at-ruling instruction, [aged images 1–8 run 46034254401](https://github.com/djosh34/tigerstore/actions/runs/46034254401) and [long seed 1 run 46034259447](https://github.com/djosh34/tigerstore/actions/runs/46034259447) were cancelled. Before cancellation, images 3 and 8 and all their forks pass; image 4 and forks 0–2 pass; image 5's aging passes. No completed failed image/fork verdict appears in the retained partial logs; uncompleted checks are not passes. The long seed has no completed witness or verdict. Its retained cgroup `memory.peak` samples reach 1,515,118,592 bytes (1,444.9296875 MiB) over 111.052 seconds before cancellation; this is a partial-run observed high-water value, **not** the requested completed whole-run peak. The full long-seed gate remains unfulfilled.

Main's comparison evidence was checked: aged run 45927138125 has only image 5 forks 0/1/3 failing with the known #799 post-healing too-many-requests signature; all other image/fork verdicts pass. Main long run 46003153998 passes at 54,217,568 witness steps and sampled system-used peak 10,263 MiB. The candidate's dedicated cgroup measurement is labeled separately from that sampled reference.

`bench/fetch-measure-plan.json` pins the measured refs and records `RULING_REQUIRED` with the exact first-pair evidence. Race/5-ms, named/Kubernetes, past-RAM/evicted/resident, reuse and ceiling campaigns have not been completed. No old disabled-routing result is presented as #833 evidence.
