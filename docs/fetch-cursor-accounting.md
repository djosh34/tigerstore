# Fetch cursor accounting and activation gate

Part of #833. Production routing is **not enabled**: the sort-scratch gate below is unresolved. This document describes the implemented accounting and the tested cases, not a completed global budget proof.

## Allocation inventory

| Owner | Charged storage and lifetime |
| --- | --- |
| P: reply stages | Whole 8,192-byte pages (8,128-byte payload), full index/root units, request/cursor metadata. Admission precedes allocation. Partial pages remain fully charged. Cancellation retains storage until matching worker acknowledgement. |
| P: output finish | Socket replacement/copy escrow; escrow converts once into actual backing. Protected output keeps that backing across socket flushes. Empty completed backing is released without an unfunded shrink-copy. |
| P: normal reads | The full existing read-half entitlement is withheld from output, not merely currently allocated input. Only actual active-input-turn debt belongs to the independent R reserve. |
| P: CONTINUATION | Bounded 64-KiB backing, including unmaterialized backing for clients that normal read admission can activate. Explicit bounded allocation prevents append-capacity overshoot. Reclamation restores the entitlement. |
| P: connection handoff | A new connection/handshake-leftover copy cannot bypass ordinary input/connection funding. It waits in the existing handoff until funding exists; peers are not held behind it. No new request refusal or input-turn timeout is introduced. |
| P: parked size hints | Physical framed-size hints reuse the existing body word. `roomRead` is back to main's 80-byte footprint, rather than growing its high-water container to 112 bytes. |
| W: shared core | One 80-KiB worker-stack/control reservation, one persistent worker and one running I/O operation. |
| W: cursor plan | The other 304 KiB of the 384-KiB cursor reservation includes the fixed 2,048 x 64-byte descriptors, bounded read/coalescing scratch, source/control rounding and completion ownership. Arena credit survives its arena-bearing callback frame. |
| W: legacy | Charged 8-KiB roots, full paged index capacity, rounded payload and retained needed results. No second B/32, uncharged map, or TTL eviction of a needed dependency. |
| P: materialized legacy fallback | Canonical materialization, in-place output links, ordered-message ownership, and temporary retry storage. Main's old sort scratch must also be funded: omitting it was an accounting defect, not a permitted exemption. |

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

These are real mixed-consumer cases, including high-water metadata and held-reply retry. They are **not** a proof that every valid legacy sort can finish: the structural case below remains a failing gate.

## Option A / #904

`TestMainHeldCaptureParityAndBoundedMaterialization904` unconditionally asserts identical main/cursor-mode `hr.counted`, client `heldCounted`, and capture `growOut` delta: **8,388,608 bytes** at 83 MiB. Only that unchanged inherited capture debit is outside this proof. Materialization, the 1,343,244-byte reply, retained results, work structures and output/copy ownership are not exempt. All 65,536 previous values are decoded and partial output cannot return live backing.

## Concrete activation blocker: unchanged sort scratch

A pristine-main reproducer at `2e1d1e1a796c5c926802d26b789b23bada57d15e` successfully serves a value sort over 65,536 four-byte keys with one-byte values, `limit=1`, and `keys_only=true`.

The unchanged `shapeKVs` holds its candidate `[][]byte` backing while allocating/sorting `[]kvRec`:

- `kvRec`: 96 bytes; slice element: 24 bytes.
- Unavoidable lower bound: `65,536 * (96 + 24) = 7,864,320 bytes`.
- Actual candidate capacity was 72,362, giving **8,028,144 bytes** before allocator rounding, payload, output, or growth-copy overlap.
- Existing P: **5,439,488 bytes**. Even P+R is only **7,536,640 bytes**.
- No held-write capture exists in this state. It is not #904.

The old materialized fallback omitted this scratch and falsely appeared bounded. Its promise now includes scratch and candidate growth/high-water backing. Consequently `TestLegacyDenseMetadataUsesBoundedMaterializationLookup` honestly fails its progress gate: its 115,000-record case needs **24,141,824 bytes** before dispatch against P of **5,439,488 bytes**. The 65,536-record cold proof likewise cannot start. Waiting indefinitely is not an implementation success.

Round 4 freezes main's sort handling and excludes the sort/permutation redesign. No scratch exemption, increased minimum, changed sort admission, or cap error has been chosen. A ruling on changing the temporary sort representation while preserving main's admission/comparator/ordering algorithm is required before this gate can be completed safely.

## Measurement state

The active original workload passes local diagnostics, including an expanded component race run with an 80-range phase of 2.479 s. This is not the five-run campaign or the qualifying paired measurement gate.

Qualifying active-routing Actions measurements have **not** been run: the mandatory budget/liveness gate above fails. Measuring the production-gated binary would not establish #833. Required named range/list/watch, Kubernetes profiles, qualifying memory/cache modes, reuse, tails/refusals, paired AB/BA runs and the throughput ruling thresholds remain prerequisites, not implied successes.
