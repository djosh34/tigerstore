# #485: planted-bug scenario-length experiment

## Result

**There is no unique winner for distinct bugs/hour at this budget:** 20, 40,
80, 160 and 320 simulated faulted seconds each caught all 29 included bugs
within one runner-hour shared equally across mutants. 20 s is the shortest
length on that observed plateau. This is a ceiling on a known, easy-to-catch
mutant suite, not a measured rate of finding unknown production bugs.

**Repeat-catch throughput peaks at 160 s, but the 80–320 s curve is flat:**
80 s is 2.49% below the peak and 320 s is 1.94% below it. 40 s is 8.97%
below it. Counting repeated detections heavily weights easy mutants
(B06d, B07, B20 and B24). Do not equate thousands of repeated catches with
thousands of distinct bugs. If a fresh unplanted control is charged for
each eligible finding, **40 s** instead maximizes validated catches/hour.

All rates below use one sequential gosim process per runner and equal
wall-time shares across the 29 mutants, excluding builds and the plain
survey. The four-process column is **estimated at 3.4x**, not measured.

| Faulted seconds | Observed distinct / 1 runner-h | Repeat catches / runner-h | 4-process estimate | Fresh-control validated catches / runner-h | Plug-in expected distinct, 1 / 4 processes |
|---|---:|---:|---:|---:|---:|
| 5 | 25 | 5069.6 | 17236.5 | 735.7 | 25.109 / 26.698 |
| 10 | 28 | 4542.4 | 15444.3 | 796.2 | 27.178 / 27.895 |
| 20 | 29 | 5192.5 | 17654.6 | 1088.8 | 28.925 / 29.000 |
| 40 | 29 | 6451.4 | 21934.6 | 1323.7 | 28.999 / 29.000 |
| 80 | 29 | 6910.0 | 23494.2 | 984.6 | 28.999 / 29.000 |
| 160 | 29 | 7086.8 | 24095.1 | 975.9 | 28.935 / 29.000 |
| 320 | 29 | 6949.6 | 23628.8 | 693.9 | 28.751 / 28.991 |

The observed distinct counts happen to remain 25, 28, 29, 29, 29, 29, 29
after charging standalone controls. The plug-in expected counts need not
match the observed counts. 40 and 80 s give essentially indistinguishable
near-29 expected coverage; ranking their fourth decimal place is not
meaningful. Four-process expected counts round to 29 at 20–160 s, not a
guarantee of complete coverage.

## Complete per-bug results

- [tables.md](tables.md): **all 203 bug × length cells**, each with catches /
  seeds, catch percentage and nominal 95% Wilson interval, measured wall
  seconds/seed, catches/hour with one process and at estimated 3.4x throughput.
- [cells.csv](cells.csv): full-precision 210 cells, including seven plain
  cells, standalone-control rates, failures, baseline rejects and provenance.
- [totals.csv](totals.csv): full-precision duration totals.
- [inventory.csv](inventory.csv): each included bug's original TestPlanted
  seed budget, `want` expression and witness requirement. The experiment
  does **not** stop at those original seed budgets or at a first catch.
- [provenance.json](provenance.json): run/job IDs, source SHAs, elapsed
  runner-hours and SHA-256 of each downloaded seeds file.

## Short-only / long-only

Across the complete 180-second-per-cell survey, **B11 is caught only at
D >= 20 s** (0/81 at 5 s; 0/86 at 10 s), and **B25 only at D >= 10 s**
(0/81 at 5 s). Every other included mutant was caught at every length.
No bug is short-only. No bug requires 40 s or longer in this sample.
Within the stricter equal-share one-hour prefix, 5 s additionally misses
B04a and B04b; their first catch occurs later in the 180-second cell.
Zero catches is not proof that a shorter seed can never catch a bug.

Longer is not uniformly better: e.g. B01b, B05 and B23 have their highest
repeat-catch rates at 5 s. B25 peaks at 40 s, then falls from 34/132 to
2/57 by 320 s. B02, B03, B04b, B11 and B12 benefit strongly from longer
faulted phases. See the full cell table rather than a single average.

## Plain config, useful work and false failures

| Faulted seconds | Plain failures / survey seeds | Measured wall s / plain seed |
|---|---:|---:|
| 5 | 0/80 | 2.259 |
| 10 | 0/84 | 2.148 |
| 20 | 0/104 | 1.750 |
| 40 | 0/126 | 1.431 |
| 80 | 0/109 | 1.664 |
| 160 | 0/85 | 2.127 |
| 320 | 0/58 | 3.138 |

**0/646 plain survey seeds failed.** Separately, catch-selected unplanted
controls hit witness-checker step-limit timeouts at (D=80, seed=201) and
(D=320, seed=1529). Seed 201 was run in both shards, giving three actual
failed control executions at two distinct duration/seed pairs. These
are inconclusive checker/search failures, not demonstrated consistency
violations. They rejected 11 would-be catches at 80 s and two at 320 s.
They must not be silently described as zero failures in all plain runs,
nor included in an unbiased plain-survey failure percentage.

**Answered and failed call counts are not recorded** in this experiment's
seed lines. The schema has finding, wall time, catch flags, provenance and
RSS, not call counters; successful plain seeds do not emit their histories
in these artifacts. Thus no per-length call totals or productive-work
rates can be recovered from these rows. The enclosing #485 investigation
reports permanent fencing in 60% of 20 s seeds and every seed at >=40 s;
this experiment did not independently measure those percentages. Its
non-monotonic plain costs (40 s cheaper than 5 s) are a warning against
assuming longer D means proportionately more useful cluster work.

## Execution failures and recovery

Primary run: https://github.com/djosh34/tigerstore/actions/runs/42020412801

- SHA `0a63d0659c2c27772e9e9e195d4cae45a3e937ba` (protocol introduced in
  `cf69e35fc6173ec9cb1b49287ecf05923aec1d35`).
- Eight jobs succeeded. Five completed every assigned cell but exited 1
  because a gosim seed panicked: B12 has two `tocommit ... out of range`
  panics (D=5 and D=10), and B20 has 12 `requested index is unavailable
  due to compaction` panics across D=10/40/80/160/320. These 14 missing-result
  rows stay in the denominator as other failures, never catches. The test
  marks itself failed but continues surveying. Re-running those
  deterministic product panics would not repair measurement coverage.
- D=320, shard 1 instead exited 2 at the native driver's 150-minute
  timeout. It finished B24 seeds 1–1847 (153.630400679 s survey time),
  then timed out while starting seed 1848, before B25/B27.

Targeted recovery: https://github.com/djosh34/tigerstore/actions/runs/42042610751

- SHA `29ed415de5254943e7a4528272d1f785d2be4f0c`.
- All 14 jobs succeeded. Thirteen built then skipped simulation; only
  D=320/shard 1 ran the recovery manifest: B24 from seed 1848 with the
  remaining 26.369599321 s, plus B25/B27 from seed 1 for 180 s each.
- Added B24 seeds 1848–2149, 57 B25 seeds and 62 B27 seeds. Survey rows
  combine into consecutive, duplicate-free prefixes in all 210 cells.
- Recovery used the updated experiment branch: its additional simulator
  change supports `EXP485_FAULTS`, but this variable is unset and the
  default config/faults are unchanged. No bug definition or `want` changed.
- The recovery manifest is archived as `../recovery-42042610751.json`;
  the active `recovery.json` is removed after completion. Copy it back
  only to intentionally repeat that recovery (which would duplicate rows).

Combined: **90,913 seed rows, 68,850 survey seeds (including 646 plain),
61,295 validated catches**, 167 nonmatching/missing-result survey failures,
and 13 baseline rejections. No cells missing. Survey execution costs
10.546 runner-hours including the plain survey. Recorded seed+actual
control execution costs 24.023 runner-hours; summed Actions job elapsed
costs 24.651 runner-hours (23.696 primary + 0.955 recovery), including
build/setup/teardown. This excludes canceled/no-op later analysis pushes.
Recovery was under the 30 additional runner-hour limit.

## What the numbers do and do not measure

A catch must match the original bug's `want`, satisfy both witness modes
for B01a/B01b, and pass the identical unplanted seed. Other failures remain
in the denominator. Every cell spends >=180 seconds on consecutive survey
seeds starting at 1, finishing its last seed, without stopping on a catch.
D is the **faulted** phase; healing remains 10 simulated seconds and
other grace/restart/checker work is additional. Bugs may terminate a seed
early; wall time includes that behavior, hashing and artifact transport.

For bug b: p = k/n, s = total survey wall seconds/n, repeat catches/h =
3600*k/total survey wall seconds. Equal-time portfolio throughput is the
**mean**, not the sum, of the 29 per-bug rates. Observed one-hour distinct
counts use completed seeds within 3600/29 = 124.138 s per mutant. This is
a replayable prefix calculation, not a new one-hour campaign. The control
variant charges each eligible finding's control time even if originally
cached. It does not charge an exhaustive plain survey or build per hour.

Four-process rates use the requested 3.4x assumption from #448. Expected
distinct counts sum 1-(1-p)^floor(124.138/s) (or 3.4 times the budget),
using plug-in p and average seed cost. They assume independent future
seeds; probability estimates for zero observed catches are zero, not
upper confidence bounds. No four-process experiment was run here.

Wilson intervals are nominal, per-cell, descriptive 95% binomial
intervals, not simultaneous confidence bounds or throughput intervals.
Consecutive shared seed prefixes, fixed-time outcome-dependent stopping,
separate Actions machines and short cells limit causal comparisons;
no significance claim is made about the near-flat optimum.

The scenario schedules guaranteed one-off faults inside D as well as
fixed-period faults. Changing D changes fault density/opportunities, not
only history length. Permanent fencing and low useful throughput limit
extrapolation to healthy long-running clusters. Mutants with immediate
checks can dominate aggregate repeat-catch throughput. This measures
this branch's DefaultConfig planted suite and existing checkers, **not**
unknown-bug yield in general.

## Excluded bugs

- B17: stalledLeaderConfig, no random faults, fixed 25 s duration and
  stall/live windows.
- B22: damagedSnapshotConfig, fixed 8 s duration/8 s heal, selected faults.
- B28: membershipConfig, fixed 60 s/20 s heal, membership workload and
  custom fault periods.
- B31: cacheConfig via linearizableConfig, fixed 5 s/8 s heal, fewer
  clients, no leases and a special cache budget.
- B32: loneHeaderConfig, fixed 10 s/8 s heal, HoldDown/InstallCrash,
  64-byte WAL segments.
- B13, B14, B26, B29, B30: fixed package-local node/peer scenarios, not
  internal/sim DefaultConfig seeded cases.

## Reproduce analysis

Download all primary artifacts into one empty directory and the recovery
D320-shard1 artifact into a separate empty subdirectory (do not overwrite
the primary shard). Then:

```sh
python3 research/485/planted/analyze.py /tmp/combined-artifacts --out /tmp/analysis
python3 -m unittest discover -s research/485/planted -p 'test_*.py' -v
```

The analyzer refuses missing cells unless `--allow-incomplete` is explicit,
rejects duplicate/gapped survey prefixes and invalid catches, excludes
control rows from survey statistics, uses all 29 mutants in equal-share
denominators even for provisional analysis, and floors estimated trials
to a whole number of seeds. Six unit tests pass. No issue comment, PR or
main-branch change is part of this experiment.
