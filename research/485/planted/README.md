# #485 planted-bug duration experiment

Exp branch only; not a PR, not a main change. Base `020e8c8f`.

Completed results: [results/RESULTS.md](results/RESULTS.md), with the full
203-cell interval/cost/rate table in [results/tables.md](results/tables.md).
Primary run 42020412801 plus targeted recovery 42042610751; no missing cells.
Run analysis tests with `python3 -m unittest discover -s research/485/planted`.
`analyze.py ARTIFACT_DIRECTORY --out OUTPUT_DIRECTORY` reads both runs'
JSONL files recursively. Keep their artifacts in separate subdirectories.

The archived `recovery-42042610751.json` records the continuation of the
one timed-out shard. `run.py` uses an optional active `recovery.json` to
skip unrelated matrix simulations and pass per-bug starts/remaining
budgets to the experiment test. No active manifest remains after recovery.

## Protocol

29 DefaultConfig planted cases plus the identical config without a bug.
Faulted phase D = 5, 10, 20, 40, 80, 160, 320 simulated seconds. Heal stays
10 s; clients, leases, other end-of-run grace and faults are unchanged.
Every cell starts at seed 1 and runs the consecutive prefix that takes at
least 180 wall seconds of survey gosim runs (finish the last seed). Never
stop at the first catch. Different lengths therefore have nested seed
prefixes, not equal sample counts. The stopping rule depends on cost,
not on catches, but cost itself can depend on catching a bug early.

14 ARM Actions runners: length x two bug shards, one sequential process
per runner. `GOMAXPROCS=2 GOFLAGS=-p=1` for builds and drivers. No measured
4-process experiment; estimate that rate as 3.4 times measured one-process
throughput, using #448, not 4 times. 180 s x 30 x 7 = 10.5 runner-hours of
survey; controls, builds and overhead are additional. A hard 155 min x
14 job timeout caps this workflow at 36.2 runner-hours even on failure.

The workflow copies `exp485_planted_test.go.txt` to `internal/sim` for the
build only. It also corrects the inherited stale three-argument `save`
call there (otherwise the base planted binary does not build). Neither
change modifies committed shared simulator sources.

A catch must match the original `plantedCases[bug].want`, satisfy both
witness modes for B01a/B01b, **and the identical unplanted seed must pass**.
The latter avoids counting known open bugs as catches. Controls are cached
by seed within each runner/shard (all included configs are DefaultConfig).
Plain survey runs also fill this cache. Separate `phase=control` rows are
not an unbiased plain survey and must not be included in its false-failure
rate. A nonmatching finding is `other_fail`; a matching finding rejected
by its unplanted control is `baseline_fail`, not a catch. Witness refusal
is not counted as a catch. Unexpected process/test failures fail the job;
no omitted artifact is silently treated as a passing run.

Each native driver result line is `EXP485 {json}`. Rows record duration,
seed, expected catch, other failure, baseline rejection, real wall ms
around `simrun.Do`, start/end real Unix ms, full finding, first simulation
ms explicitly present in the finding, and control wall ms/cache reuse.
Wall includes gosim execution, seed hashing and artifact transport, but
not the separate unplanted control; standalone validated-catch cost can
be computed as `wall_ms + control_wall_ms` (including cached controls as
if paid anew). Workflow elapsed cost instead pays only controls actually
run. Simulation ms from `call at` is the violating call's time, not the
time at which an end-of-run checker discovered it; null means unavailable.

`run.py` samples `/proc` every 100 ms, sums RSS of `translated*`, and records
the native driver separately. `seeds.jsonl` joins peak samples to each
seed's wall interval. This is sampled RSS, not exact kernel high-water RSS;
short seeds can have no sample, and a persistent translated worker's heap
can include retained pages from preceding seeds. Artifacts include full
seed logs, RSS samples, metadata, seeds JSONL, commit and run ID.

## Included

B01a B01b B02 B03 B04a B04b B04c B05 B06a B06b B06c B06d B07 B08 B09a
B09b B10 B11 B12 B15 B16 B18 B19 B20 B21 B23 B24 B25 B27.

## Excluded

- B17: stalledLeaderConfig sets faults empty, Duration=25 s, stall/live times.
- B22: damagedSnapshotConfig sets Duration=8 s, Heal=8 s, only HoldDown and
  FlipSnapshot faults.
- B28: membershipConfig sets Duration=60 s, Heal=20 s, membership workload
  and custom fault periods.
- B31: cacheConfig -> linearizableConfig sets Duration=5 s, Heal=8 s,
  fewer clients and no leases; cache budget is special too.
- B32: loneHeaderConfig sets Duration=10 s, Heal=8 s, HoldDown and
  InstallCrash faults, 64-byte WAL segments.
- B13, B14, B26, B29, B30: fixed TestPlanted scenarios in node/peer, not
  internal/sim DefaultConfig seed scenarios.

## Why short seeds are fault-denser

`Sim.schedule` schedules a guaranteed uniform-time crash and link change
in addition to the repeating streams. `scheduleDiskFaults` adds a due disk
fault by D/4 (retried/armed until a write), one checkpoint in the latter
half, step-crash guarantee at D/2, rot due at 3D/4, and one held-down
follower. `scheduleNetFaults` retries once-per-seed link stalls, process
pauses, half-open links, resets and slow watchers by 3D/4, and a netem fault
until a link actually carries affected traffic. `scheduleGrayFaults`
guarantees a gray period with a lost append (can persist after healing)
and arms recovery crashes from D/4 until one happens. Some guarantees
cannot take effect if prerequisites never occur. FlipSnapshot, FetchCrash,
SlowFetch and GrayClient are opportunity/workload-driven, not a uniform
Poisson rate. Repeating faults keep their fixed periods. Thus changing D
changes guaranteed faults per simulated second as well as history length;
this is a comparison of existing scenarios, not constant fault density.

## Analysis definitions

For survey rows per bug/length, p = validated catches / seeds,
s/seed = sum(wall_ms)/1000/seeds, and catches/runner-hour =
3600 * validated catches / sum(wall_ms/1000). Also report standalone
validated-catch rates charging each catch's control, and empirical bugs
caught within 3600/29 seconds per bug (one runner-hour equally allocated
across mutants), only completed seeds charged to that budget. Four-process
rates and extrapolated distinct-bug probabilities are estimates. Do not
sum per-bug hourly rates and label the sum a feasible shared-runner rate;
an equal allocation gives their mean. Zero observed catches is not proof
of zero probability. Survey all seeds, including other failures, in the
denominator and report those failures separately.
