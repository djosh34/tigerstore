# Research #485 (exp branch only, never merged)

The most the simulator could do under plan 5's decisions, and which
scenario length finds the most bugs per hour.

Base: `exp/473-slowdown` (`ae336143`): #441's exact witness speedups and
hardware CRC32 under gosim, #473's real-time marks and profile windows.
On this branch the state hash folds leases in without sorting them by
default (#483); `EXP473_HASH=sort` gives the old hash.

Each part has its own directory under `research/485/` and its own workflow
`.github/workflows/exp485-<part>.yml`, which runs on a push to
`exp/485-sim-ceiling` that changes that part's directory.
