# Diagnosis-only history for #289

This branch is not for merging. No PR is intended.

`diag289-seed7004.json.gz` is the exact `history.json` produced by the
simulator's `-sim.out` in the failed `TestEndWaitsForCatchUp`, seed 7004:

- PR #282 head of that run: `250b8881`.
- Original run: https://github.com/djosh34/tigerstore/actions/runs/39245189697/job/118035291347
- Artifact: `race-sim`, path `sim-out/TestEndWaitsForCatchUp/history.json`.
- Uncompressed SHA-256: `5b548148898f2262b26115412dc15302b877a8b544c99626cb4e3f5e409c6cea`.
- 12,130 operations, highest header revision 4294.

## Stable reproduction

Run in GitHub Actions for `-race`, never on the shared development host:

```
DIAG289=1 go test -race -v -count=1 -timeout=4m ./check -run '^TestDiag289History$'
```

Baseline commit `79d3d7fe` keeps #284's witness fix; its Actions workflow
also substitutes just `check/witness_revisions.go` from main `fa55dee2`.
Both versions time out at the original one-minute checker limit under
`-race`. Both pass without race instrumentation. The comparisons use the
same saved history, not different simulator schedules.

The proposed optimization is in `7418e312`. The diagnosis workflow also
runs #284's legal reduced history and stale variant, the existing targeted
witness tests, and a single fresh simulator seed with `-sim.out` (preserved
even on success). All CPU/alloc profiles and per-mode wall timings are in
Actions artifacts.

## Cause and optimization argument

Optional ambiguous compactions op 70 (to revision 2) and op 127 (to 33)
remain due even though acked op 92 already compacted to 37. `takes` checks
`readsFit` before asking the model whether the trial compaction can apply.
Each optional's `readsFit` is therefore called **12,741 times**, every time
on an already-compacted trial. It rechecks read op 54 (header revision 25,
requested revision 1) and builds event/state indexes across the growing
history. There are zero search backtracks on this history.

The baseline CPU profile puts essentially all Go-side search CPU below
`readsFit -> placeSeen`, especially `eventBounds` scans/sorts. The baseline
race allocation profile attributes about 13.83 GB of cumulative allocations
to `readsFit` (not live memory). This happens with main's witness too; #284
is not the source.

The optimization tests the model on the private trial clone **before**
`readsFit`. All existing predicates still have to pass. `readsFit` reads
`w`, not the trial state, and `placeSeen(..., false)` does not place ops.
Callers discard a failed trial clone. Therefore exchanging the two
conjuncts changes neither accepted trials nor the live store; it only skips
expensive read placement when the model already rejects the trial.
`expireEnd` uses the same ordering, still allowing unseen revision bumps.
There is no permanent rejection cache, so restoring a gap also restores
all state relevant to future candidates.

`TestDiag289RejectedOptional` is an algorithmic red/green check: an
already-applied compaction must be refused without attempting read
placement. It fails on the baseline, passes with the optimization.
The exact-history replay is the time-limit red/green check under `-race`.
