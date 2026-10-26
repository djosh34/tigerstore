# #441 revision witness experiment — never merge

Branch: `exp/441-witness`, based on `exp/441-sim-speed`. No PR or issue comments.
All timings and counters below are **measured**, not estimates. These are native
checker times on Actions ubuntu-24.04-arm, GOMAXPROCS=2, GOFLAGS=-p=1,
GOMEMLIMIT=12GiB. Each variant ran in a separate process on the same runner and
same decoded history, with a GC before starting its checker timer. JSON loading
and CPU-profile shutdown are excluded. These are **not whole-seed speedups**.

## Paired baseline versus all exact optimizations (`ultimate`)

Seed suffixes below stand for 2026102600 followed by the suffix.

| Virtual minutes | Seed suffix | Operations | Baseline seconds | Ultimate seconds | Speedup |
|---|---:|---:|---:|---:|---:|
| 60 | 09001 | 59,485 | 122.373391 | 0.339231 | 360.7x |
| 60 | 09002 | 59,487 | 11.739390 | 0.219265 | 53.5x |
| 60 | 16001 | 59,381 | 18.338660 | 0.245882 | 74.6x |
| 120 | 09001 | 128,798 | 462.360285 | 1.120214 | 412.7x |
| 120 | 09002 | 125,998 | 80.803657 | 0.915215 | 88.3x |
| 120 | 16001 | 118,272 | 907.644932 | 1.211748 | 749.0x |
| 240 | 12001 | 238,012 | 95.826188 | 1.523197 | 62.9x |

All variants passed every history in this table. Only one full 240-minute seed
was measured; do not extrapolate its compaction mix to the other eight seeds.

## Search work and the actual bottleneck

There were **zero backtracks, at any depth, in all seven real histories**, in
both baseline and optimized runs. The regression fixture seed-59113215 does
backtrack: 25 returns to a gap, maximum revision distance 2; instrumentation is
not simply stuck at zero.

At 60 minutes there were 1,742–1,780 ambiguous mutation candidates, 128–143 gap
frames, and 13,126–14,922 fits calls. At 120 minutes: 3,560–3,856 candidates,
292–345 gaps, and 70,078–77,944 fits calls. The 240-minute seed had 7,179
candidates, 630 gaps, 283,073 fits calls, and 4,522,770 candidate scans. These
candidate counts exclude ambiguous reads and grants whose generated ID nobody
learned; they are not counts of all ambiguous calls.

The key universe was 8,692–9,365 keys. A trial cloned the entire dense key
array even for a read-only failed CAS or a compaction that touches no keys.
The main difference between equally long seeds was redundant optional
compaction work, not backtracking: at 120 minutes seed 09002 made 148,589
optional trials, seed 09001 3,633,472, and seed 16001 7,131,397. Baseline
cumulative allocations were 194 GB, 2.14 TB and 4.43 TB respectively; these are
allocation traffic, **not peak resident memory**.

Ambiguity/gaps increase candidate trials; key spread increases the cost of
every dense copy and of reconstructing every answer state. Longer histories
increase both candidate work and optional-compaction retries. This is expensive
forward replay on these samples, not exponential backtracking.

## Changes and ablations

* `undo`, `check/witness_trial.go`: trial steps borrow live key cells, save only
  potentially written cells, and restore before applying the chosen event.
  Scalars/leases remain private. Both transaction branches are conservatively
  recorded; range deletes and revokes are covered. Alone, this gives
  9.1x/1.5x/12.9x on the three 120-minute histories, and 1.65x at 240 minutes.
* `scoped`, `check/witness_scoped.go`: answer replay reconstructs only the union
  of keys in every requested range, both transaction branches, and compares.
  It does not merely replay returned keys. Revoke still uses all keys. Alone,
  this gives 1.0–2.9x at 120 minutes, 2.41x at 240 minutes, and up to 7.0x at
  60 minutes. On the 240-minute history it reduces answer key cells from
  638,880,300 to 917,444.
* `optional`: compact/grant trials copy metadata rather than keys. Alone,
  120-minute seed 16001 falls from 917.385 s to 73.578 s (12.5x), but it leaves
  the redundant scheduling work. `retired` instead skips an optional
  compaction already superseded by live.compacted; eligibility is recomputed
  after rollback. Alone on 60-minute seed 09001: 123.854 s to 23.038 s (5.4x).
* `prefilter`: evaluate compares and reject a read-only selected transaction
  branch before copying anything. At 240 minutes this avoids 260,199 of the
  283,073 candidate copies. Due optional mutations cannot change keys when
  successfully inserted without a revision, so they cannot change that branch.
* `checkpoints`: mark event/version/used-log positions instead of retaining a
  whole key array and used map per gap; reconstruct on rollback. At 60 minutes
  it gives roughly 1.1x, not the main speedup. The key-to-putter `indexed`
  experiment avoids zero candidate scans on those three histories and gives
  no reliable benefit: observed unique values already resolve those writers.

`best` combines the earlier exact changes but still clones candidate key
arrays. Adding undo (`ultimate`) is another 1.55–1.83x at 120 minutes and 2.33x
at 240 minutes. The combined changes interact: an answer optimization alone
cannot remove optional-compaction allocation traffic, and vice versa.

## Checking power and tests

No deliberate loss of checking power: no sampling, reduced key set of the
history, shortened history, weaker counts, dropped comparisons/responses,
ignored mutations, or skipped real-time verification. Undo is semantically the
same trial model. Scoped states contain all keys a request can inspect or
change, including keys absent from the recorded answer. Skipped optional
compactions already return errCompacted and can legally be left out. Existing
search limits/timeouts still fail rather than pass. No history splitting or
independent-per-key assumption was used.

`GOMAXPROCS=2 GOFLAGS=-p=1 go test ./check` passes with `ultimate` as the default,
including every existing must-fail witness regression. An added differential
test checks 300 generated true histories and their stale-read mutations across
all variants: 238 corrupt histories are rejected by every variant. Another
compares scoped outcomes and undo outcomes/restoration with full dense replay
at every state of mixed range/transaction/lease/compaction histories.

## Reproduction and workflow runs

Build `go build -o checkhistory ./cmd/checkhistory`. Run
`checkhistory -checkers witness-revisions -witness-stats -witness-variant ultimate history.json.gz`.
Use `baseline` for the original algorithm. `-witness-stats` emits final JSON
metrics and, on the final branch, live counters to stderr about every ten
seconds. `-cpuprofile file.cpu` records checker-only CPU samples.

Source artifacts: 40282688114 (60m), 40282690495 (120m), 40282629536 (240m).
Paired final measurements, algorithm commit 2b342307:
* 40287963255: all three 60m histories, ultimate/undo/best/baseline.
* 40287965973: all three 120m histories, same variants.
* 40288051135: 240m seed 202610260012001, plus optional/scoped.
Ablations: 40283485615 (checkpoints/indexed); 40285222051 (60m optional/scoped/
prefilter/tuned); 40285224598 (same, 120m); 40286770652 (60m retired/best).
The last code commit adds live counters but does not change the search.
Superseded waiting/queued experiments on this branch were canceled; no other
branch or another worker's run was modified.
