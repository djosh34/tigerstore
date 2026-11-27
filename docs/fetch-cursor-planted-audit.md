# Cursor routing: planted-delivery audit

Part of #833. This changes deliberate mutation plumbing, not production
routing, the simulator's seed budgets, acceptance patterns, or checkers.
The 1,000-ms measurement ruling remains pending; no measurement is dispatched.

## Confirmed B06a/B06b gap

At pre-fix head `8dd2b737`, the hooks still execute: B06b duplicates the
first event in `watches.push`, and B06a drops the final event of a multi-key
write in `flushEvents`. Both mutate `watches.events`. The cursor branch then
discards that list and reconstructs delivery from the unmutated change index.
The mutation's *effect*, not necessarily its hook, is bypassed.

- Routing on, local seed 1: neither is caught.
- Same head, only temporary `cursorFetch: false`: both are caught at seed 1,
  with ordinary watch event-count errors and passing unplanted controls.
- Independent full-budget pre-fix Actions reproduction:
  https://github.com/djosh34/tigerstore/actions/runs/46038575076
  Both are not caught in 32 seeds, matching the original failing workflow.

## Same mutation on the new delivery path

In `planted` builds only, `watchDeliveryPlant` retains the exact already-mutated
live delivery sequence as logical `(revision, key ID, deletion)` entries. The
cursor's shared event iterator uses that sequence for metadata sizing and actual
serialization, resolving the normal current/previous records through the usual
store/source/CRC machinery. It neither changes the shared change index nor
introduces framing/checksum errors as substitutes for missing/duplicate events.

Scope is deliberately no broader than main:

- Capture only happens at the existing live-event handoff, after the original
  B06a/B06b hooks. Historical or already-unsynced watchers are not newly mutated.
- Main's slow-client/logical-room predicates are checked without taking credit.
  A live list main would discard for catch-up does not become a new mutation.
- B06a keeps its existing linked-list semantics: for multiple writes collected
  in a turn, only the final multi-event write's final event is dropped. A
  one-event write is not dropped. B06b duplicates each write's first event.
- Resumable ordinals preserve the selected sequence across pieces. State retires
  at queue/revision commit, cancellation, or store replacement; watcher reuse
  cannot inherit it.
- Non-planted builds use an empty, inlineable no-op type. Production `watcher`
  stays **144 bytes**, with `liveBytes` at **128** and `big` at **136**. No new
  production array/map, per-watcher debit, or fetched-value cache is introduced.

`TestCursorPlantedLiveDeliveryExactlyMatchesMainMutation` compares complete
protobuf bytes against the real legacy mutation for single-event, multi-key,
and multiple-write deliveries, both resident and cold. It fails before the
handoff fix and passes after it. Separate tests enforce unchanged historical /
blocked-live scope, denied-room accounting, resumption, retirement, cancellation,
store replacement, and the production layout. The production lease-free pin
still matches `cee1eb73ec6ac127 / df529d402093fa52` without another re-pin.

## Inventory: `grep -rn 'planted.On' internal/wl`

| Plant(s) | Site and production cursor reachability |
| --- | --- |
| B06a / B06b | Original `flushEvents` / `watches.push` live-list mutations now survive the cursor handoff as described above. No new historical/catch-up mutation. |
| B06c | `watcher.withPrev` is used by both ordinary encoding and the cursor's `eachRevEvent` metadata/serialization. Previous KVs are omitted on the actual new path. |
| B06d | The progress-before-events mutation remains before `flushEvents`' routing branch. Cursor queue commit does not bypass the premature progress output. |
| B07 | Start-revision mutation is in shared `watchRequest`, before either delivery path. |
| B08 | Tombstone removal is in shared store compaction; both legacy lookup and cursor change/state resolution observe it. |
| B18 | `closeWatchStream`'s deliberate early return still precedes watch removal, including for cursor-backed streams. |
| B31 | The hook is present in `at`, `keyAt`, `stateAt`, and `liveAt`. Cursor range planning and selection use `stateAt`; legacy sorts/materialization use the ordinary historical lookup functions. There is no cursor-only revision-correct fallback hiding this plant. |
| B01a / B01b | ReadIndex bypass / early-applied-index gate remain in shared request admission and read-batch readiness, before `serveRange` selects the cursor or legacy implementation. |
| B24 | Shared apply writes the deliberately wrong value offset into store metadata. Both the cursor's leased source reads and legacy retry read that metadata and retain value CRC checking. |
| B09a / B09b | Shared apply revisions / store create-version history; no separate cursor apply path. |
| B02 / B03 / B04a / B04b / B04c / B05 / B11 / B12 / B16 / B19 / B20 / B21 / B22 / B23 / B27 / B33 | Shared WAL durability/recovery, storage, checkpoint, snapshot installation, and forwarded-batch paths. The cursor does not replace those mutations or their normal verification. No `planted.On` site exists only in the retired fetch implementation. |
| B10 / B17 / B28 / B34 / B35 / B36 | Shared lease/leadership/membership/silence logic, not alternative fetch or output implementations. |
| B15 (outside wl) | `internal/sim/history.go` deliberately changes timeout ambiguity during history conversion, independent of delivery routing. |

No other fetch/range/watch mutation needs relocation. The affected local
single-seed checks catch B06a, B06b, B06c, B06d, B07, B18, B24, B31, B01a and
B01b at seed 1, all with passing controls. B08 needs seed 2, exactly as in the
pre-fix head's planted workflow: its existing “checkpoint ... does not load”
finding is retained, not newly reclassified or made the B06a/B06b catch. The
full planted workflow verifies the complete existing inventory on Actions.
