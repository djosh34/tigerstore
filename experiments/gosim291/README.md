# Can gosim run tigerstore's real driver on simfs deterministically?

Issue #291. Throwaway branch `exp/291-gosim`. Tested on Linux arm64 with
Go 1.27.2 and gosim `ffd3a613542675755e4cbf8186b5edaf404ed95c`.

**No, not with current gosim on Go 1.27.2.** The CLI builds, but its translator
cannot translate the standard library. This does not disprove the approach after
a gosim port. No tigerstore run reached gosim's scheduler.

## Run

From this branch, with Git and Go available and network access for modules:

```sh
bash experiments/gosim291/run.sh all
```

This builds one thing at a time with `GOMAXPROCS=2 GOFLAGS=-p=2`, pins gosim,
fetches Go 1.23.12 for the upstream control, and writes `results/repro-*.log`.
The script exits zero when the native/control runs pass and all three blocked
translation attempts exit one. That is a successful reproduction of the blocker,
not a passing gosim tigerstore test.

To see each error directly:

```sh
bash experiments/gosim291/run.sh setup
bash experiments/gosim291/run.sh native    # passes
bash experiments/gosim291/run.sh upstream  # fails in x/tools loader
bash experiments/gosim291/run.sh tools     # fails on missing stdlib hooks
bash experiments/gosim291/run.sh audit     # diagnostic only; fails later
bash experiments/gosim291/run.sh control   # upstream example on Go 1.23.12
```

The separate module leaves tigerstore's production sources and dependency
versions alone. Generated clones and binaries live under ignored `.cache/`.

## Answers to the issue

1. The CLI builds on Go 1.27.2. Translation of both gosim's own example and
   `TestDriverGate` fails with `package "internal/coverage/rtcov" without types
   was imported from "runtime"`. Updating `x/tools` from v0.27.0 to v0.52.0
   fixes that loader error, then translation stops at `internal/cpu.getisar1`
   on arm64. It also reports an unknown FIPS `monoTime` linkname.
2. Blocked before execution. I stopped at a one-node prerequisite instead of
   writing an unreachable three-node cluster. The native prerequisite uses
   `node.NewOnFS`, `Node.Run`, `Node.Do`, the real WAL, and tigerstore's `simfs`.
   A put and a linearizable read pass in 0.10 s. It does not test TCP, crashes,
   restarts, or replay.
3. No tigerstore trace or history exists to compare. As an environment control,
   upstream `TestGosim` on Go 1.23.12 passes seeds 1, 1, 2. The two seed-1 JSON
   traces are byte-identical; seed 2 differs. See `results/control-comparison.txt`.
   That is not evidence of deterministic tigerstore replay.
4. `tools-update.patch` only updates gosim's tooling dependencies.
   `hook-audit.patch` removes the translator's first missing-body exit to list
   more blockers. It does not implement any hooks, and I never execute its output.
   That audit reports 80 missing function bodies and 35 unknown linknames, then
   stops at `need public export hack for encoding/json/jsontext.export`.
   The missing hooks include new `internal/sync` mutex operations,
   `sync.runtime_SemacquireWaitGroup`, `time.runtimeNow`, `internal/synctest`,
   weak pointers, runtime atomics, and FIPS crypto. These are diagnostics, not
   115 independently proven runtime failures.
   The native prerequisite fakes peers with a no-op transport and process memory
   measurement with a zero-returning callback. Its wrapper only locks simfs.
   `NewOnFS` already avoids mmap and OS disk calls, including fdatasync.
   I did not patch unsafe code or prove it safe under gosim. There is no
   `testing/synctest` import in the prerequisite, but Go 1.27's standard library
   still pulls in `internal/synctest`.
5. Zero tigerstore seeds executed under gosim, so seeds/s and a speedup are
   unmeasurable. One run of today's `TestSeeds`, seed 1, took 0.91 s here,
   about 1.10 seeds/s, with two scheduler threads on the shared host. The issue's
   reference is 0.35 s/seed, about 2.86 seeds/s. Neither is a controlled benchmark.
6. No gosim tigerstore seed batch ran. No tigerstore bug found. The one current
   simulator seed passed. No long run or Actions seed campaign was justified.

## What it would take

Port gosim's standard-library rewrites, assembly retention rules and runtime
hooks to Go 1.27, including its moved synchronization implementations and clock
entry points. Then validate its reflection/unsafe assumptions and rerun this
prerequisite before adding TCP and power cuts. Simply allowing unknown linknames
to call the host runtime would not establish deterministic scheduling.

Using Go 1.23 is not a drop-in workaround. The toolchain rejects tigerstore's
`go 1.27.2` requirement. Its current etcd API and Raft dependencies require Go
1.26, and production code uses `sync.WaitGroup.Go` and `errors.AsType`.
Backporting those sources and downgrading dependencies would test a different
build. I did not do that.

The checked-in logs contain the actual failures and the native/control results.
To repeat the one existing-simulator measurement from the repository root:

```sh
GOMAXPROCS=2 GOFLAGS=-p=2 go test -v -count=1 ./internal/sim \
  -run '^TestSeeds$' -sim.seed=1 -sim.seeds=1
```
