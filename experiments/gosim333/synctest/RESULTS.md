# #333: past the synctest gate (Actions-only bounded round)

**Tested implementation commit:** `03124ced9402c1e0da14b152ad345dd1e4e11387`,
branch `exp/333-synctest`, based on `1f3ecd0b`.
Own worktree `/tmp/r333-synctest`; no local Go build/test/translation, no
subagents, main changes, PR, or issue comment. All heavy work ran on
`ubuntu-24.04-arm`, Go 1.27.2 (`results/round10/artifact/environment.txt:1-3`).
The inherited tooling/dependency upgrades and #333 translation workarounds
remain; this is not main's byte-identical module graph (`tigerstore-go.mod`).

## Result

**One real TestSeeds trial passes**, tigerstore seed 1, gosim seed 1; its
repeat passes, as does gosim seed 2 (`results/round10/artifact/status.tsv:6-11`).
No new blocker after the eighth runtime blocker below. Stopped the fix loop
there; subsequent work only ran the requested replay/other-test checks.

* `TestMembershipFaults`, one tigerstore/gosim seed: pass (`membership.log:13-15`).
* Cleaner, only `/1`, gosim seed 1: pass (`cleaner.log:9-10`).
* One B01a TestPlanted trial: pass, **expected stale-read bug caught**
  (`planted.log:14-16`). The wrapper sets its seed budget to one and calls real
  TestPlanted; this is not a whole planted inventory campaign.
* Upstream example and real-driver/simfs probe still pass (`status.tsv:4-5`).

Paths without a directory above mean `results/round10/artifact/`.

## Ordered blockers and fixes

`results/blockers.txt:1-9` points to the exact archived raw log line for each
observation, including full stack traces. R-number identifies the Actions run
in the next section; source files below are under this directory unless noted.

| Order / run | Observed blocker | Narrow adaptation before the next attempt |
|---|---|---|
| Compile prerequisite / R1 | duplicate Run linkname / missing Bubble.Run body | Do not hook methods named Run as the package function Run. `fixes.py:5-11`. |
| 1 / R2 | `crypto/internal/fips140.getIndicator` | Per-simulated-goroutine byte state, with paired setter; both actually reached. `gosim/fips333.go:5-16`; Go 1.27.2 `runtime/runtime1.go:776-784`. |
| 2 / R3 | `crypto/internal/fips140/nistec.p256SelectAffine` | Official `purego` build tag, not dummy crypto. Go 1.27.2 `nistec/p256.go:5` / `p256_asm.go:13`; `actions.sh:4-7`. |
| 3 / R4 | `weak.runtime_registerWeakPointer` | **Diagnostic strong-backed weak handles**, paired makeStrong function; both later reached. Drops GC/weak eviction. `gosim/weak333.go:8-20`; Go 1.27.2 `weak/pointer.go:14-59,69-102`. |
| 4 / R5 | `internal/sync.runtime_canSpin` | Delegate old gosim sync hook (false: never host-spin). `gosim/sync333.go:5-10`. |
| 5 / R6 | `internal/sync.runtime_nanotime` | Delegate old gosim virtual starvation clock. `gosim/sync333.go:12-17`. |
| 6 / R7 | `internal/sync.runtime_SemacquireMutex` | Delegate old gosim mutex semaphore. `gosim/sync333.go:19-24`. |
| 7 / R8 | `internal/sync.runtime_Semrelease` | Delegate old gosim semaphore release. `gosim/sync333.go:26-30`. |
| 8 / R9 | host `runtime.AddCleanup: cleanup function closes over ptr` in TLS certificate cache | **Approximation-induced:** the weak handle captured by TLS's eviction closure is now strong. Route AddCleanup to explicit no-GC/no-cleanup adapter; never run translated callbacks on host GC workers. `gosim/cleanup333.go:5-15`, `fixes.py`; Go 1.27.2 `crypto/tls/cache.go:31-38`, `runtime/mcleanup.go:164-181`. |
| R10 | no blocker; selected tests pass | Stop. |

The four moved mutex APIs are declared in Go 1.27.2
`src/internal/sync/runtime.go:11-46`; their reused implementations are pinned
gosim `ffd3a613542675755e4cbf8186b5edaf404ed95c`,
`internal/hooks/go123/sync.go:11-17,41-50`.
Primary Go source links use tag `go1.27.2`, for example:
https://github.com/golang/go/blob/go1.27.2/src/runtime/runtime1.go#L776-L784
https://github.com/golang/go/blob/go1.27.2/src/crypto/tls/cache.go#L31-L38

Additional first-use markers prove paired setters/recoveries actually execute,
not merely compile: `seed-1a.log:3-11` lists indicator get/set, weak register,
AddCleanup, canSpin, nanotime, mutex acquire/release, weak makeStrong.

## Exactly what the adapter drops

`gosim/synctest333.go:8-39`, `apply.py:14-36`:

* One machine-wide active-bubble bit during Run, including goroutines created
  outside it; no per-goroutine or foreign-bubble membership and no isolated
  bubble clock. Run calls f on its current goroutine and does not join other
  goroutines or reject nesting. Gosim's existing virtual clock drives time.
* associate/isAssociated/disassociate only track pointer membership in that
  one active bubble. acquire/release have no liveness accounting.
* runtimeIsBubbled reflects that bit; runtimeNow omits monotonic time while it
  is true, matching Go `runtime/time.go:16-30` (added in R7).
* Durable WaitGroup waits use the same gosim semaphore, without synctest idle
  accounting. Wait **remains a loud missing-hook stub**; no selected run hit it.
* testingSynctestTest calls f with the same gosim T and returns !t.Failed().
  No special bubble T.Context, cleanup-at-bubble-exit, T.Run/T.Parallel rules,
  or bubble-specific failure/deadlock behavior. Ordinary gosim cleanup remains.

Also explicitly dropped: weak/GC reachability, cache eviction and cleanup
execution/validation (except nil ptr). These retain objects and do not model
finalization. `runtime.Cleanup{}` provides a zero/no-op Stop handle. This was a
**hook-reachability experiment**, not a sound Go 1.27 synctest or GC port.
Passing under these approximations does not establish faithful native-Go
behavior or correctness for untested seeds/tests.

## Exact replay comparison

`results/round10/artifact/comparison.txt:1-11` records comparisons made **on the
runner**, not inferred from hashes. No normalization of serialized result bytes:

| Output | Both gosim-seed-1 runs | Gosim seed 2 (tigerstore seed remains 1) |
|---|---|---|
| tigerstore Checkable history JSON | byte-identical, 5,854,139 bytes | different, 4,494,061 bytes |
| tigerstore Result.Trace text | byte-identical, 25,141 bytes | different, 18,896 bytes |
| gosim checksum-event JSONL | byte-identical, 1,164,973,770 bytes | different, 518,984,115 bytes |

Event streams contain every raw JSON line whose msg is checksummer, from
separate `-force-checksum` runs (`compare.py:12-18`); full application logs are
not substituted for scheduler events. The recorder serializes real Result.Trace
and Result.Checkable in test cleanup; it does not invent a history. It copies
immutable strings to host files for artifacts, without feeding host file data,
time or randomness back into the simulation (`apply.py`, `gosim/options333.go`).

All three first attempts passed (not failures with matching diagnostics).
This establishes repeatability for these concrete patched runs only.

## Actions runs / exact tested commits

| Run | Commit | URL |
|---|---|---|
| R1 | 0bc4c0c2 | https://github.com/djosh34/tigerstore/actions/runs/39383505031 |
| R2 | 7f46e7fc | https://github.com/djosh34/tigerstore/actions/runs/39383773086 |
| R3 | 58535401 | https://github.com/djosh34/tigerstore/actions/runs/39384371283 |
| R4 | 528d64fa | https://github.com/djosh34/tigerstore/actions/runs/39384964091 |
| R5 | 6bc6404c | https://github.com/djosh34/tigerstore/actions/runs/39385556714 |
| R6 | 8458fdd3 | https://github.com/djosh34/tigerstore/actions/runs/39386090601 |
| R7 | 30defbe3 | https://github.com/djosh34/tigerstore/actions/runs/39386666425 |
| R8 | 835df224 | https://github.com/djosh34/tigerstore/actions/runs/39387221430 |
| R9 | 2f8d74d2 | https://github.com/djosh34/tigerstore/actions/runs/39387733427 |
| R10 | 03124ced | https://github.com/djosh34/tigerstore/actions/runs/39388355445 |

R10's complete logs/history/events/patches artifact (406 MB ZIP, multi-GB raw):
https://github.com/djosh34/tigerstore/actions/runs/39388355445/artifacts/12249115864
The small committed subset was fetched with HTTP ranges; **no large local
extraction/build**. `artifact-members.tsv` lists skipped large members. The
runner comparison plus SHA-256 values is committed in comparison.txt. Full raw
artifacts have 14-day retention; all code and small primary result logs persist
on this experiment branch.

## Reproduction / commands

Exact executed remote commands are `results/round10/artifact/commands.log`;
local shell commands through evidence extraction are `commands.log`.
Workflow `.github/workflows/gosim333-synctest.yml` runs only on pushes to this
experiment branch affecting its workflow/experiment implementation files.
No workflow exists on main from this experiment. To repeat the tested run:

```sh
gh run rerun 39388355445 -R djosh34/tigerstore
gh run watch 39388355445 -R djosh34/tigerstore --interval 30 --exit-status
```

The workflow runs `bash experiments/gosim333/synctest/actions.sh`; that calls
run.sh prepare/translate/compile/example/probe, then the seed/replay/membership/
cleaner/planted commands. It uses GOFLAGS=-p=2 (plus -mod=mod), GOMAXPROCS=2 and
flock around all Go build/translate/test commands. apply.py applies the adapter
AFTER cumulative runtime-explore.patch. Every run uploads the resulting full
gosim.patch and tigerstore.patch; R10's copies are in the committed small subset.
