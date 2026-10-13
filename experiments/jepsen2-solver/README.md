# Versioned register solver

Reliability tooling only. No product changes. Actual solves and JVM validation run only on GitHub Actions. The runner starts no new work at or after 2026-10-13T22:00:00Z.

## Exact finite encoding

For one complete key projection, retain every original invocation and response. Each successful operation is required. An info or unfinished operation is optional. A selected info has an invocation lower bound but no response upper bound. A failed operation is excluded under Knossos 0.3.15's `history/complete` and `history/without-failures` rules. In particular, upstream does not model a failed CAS as an observed compare mismatch. This solver makes the same limited claim, not a stronger failed-CAS specification.

There are at most N mutations, where N counts all nonfailed write/CAS calls. An included mutation chooses a unique resulting version between 1 and M. M is the number of included mutations. These constraints fill every version from 1 through M exactly once. Version 0 has value nil. A write supplies the value at its chosen version. A CAS must compare equal to the value at the previous version, then supplies its new value. A nonnil successful version fixes the chosen version. A read selects a version between 0 and M and checks each nonnil observation against that state. Nil read values and versions remain wildcards, exactly as in upstream.

Each version has a strictly increasing rational linearization time. A read's rational time falls strictly between the mutations before and after its chosen version. Selected operations occur after their original invocation and, for successes only, before their original completion. This implies all completed-before-invoked real-time edges. Original event sequence order, not wall-clock timestamps or rewritten response indices, supplies the bounds. Unused mutation slots lie after the selected ones. A finite bound beyond the final event is sufficient because at most N pending mutations can be placed there.

Every legal completion maps to this encoding by numbering its mutations and choosing rational times between event boundaries. Conversely, sorting a satisfying assignment's times gives a legal completion. All state values come from initial nil or original mutation arguments. There is no history truncation, no info-to-fail rewrite, and no enforced abort classification.

The first search may require optional operations with a `no leader` response to be omitted. SAT from that restricted search is still a legal completion. UNSAT or timeout triggers the unrestricted encoding with every optional operation available. Only an unrestricted UNSAT is reported as a model-search result, and never as a product defect without further review.

## Witness checks

Each SAT output records every original operation, its response, and an explicit included, omitted, or upstream-fail decision. It includes the complete key event projection, hashes of original files, the linearization order, rational times, and state after each step. A separate Python verifier reconstructs calls from the full source history, checks complete coverage and the saved independent projection, checks time intervals and every real-time edge, and replays the register semantics without Z3. A separate Clojure verifier reconstructs the same calls and runs the exact upstream `VersionedRegister` record through `knossos.model/step`. A SAT result becomes a validated pass only after both verifiers succeed. Timeouts, missing outputs, process errors, or failed validation remain unknown.

The pinned upstream source is `jepsen-io/etcd` commit `b47cc401f27bfb7c28afd9ac088b5f0dca4501b8`. The source record is retained without changes. Saved-history extraction and archive SHA-256 verification reuse the offline recheck helper. Eight original OOM histories are checked, including all their keys. A full-history verdict requires validated witnesses for every key. Original fault observations and other checker components remain separate requirements.

## Runner

The isolated branch is `exp/jepsen2-solver`; tooling is published under `experiments/jepsen2-solver`. Its Actions workflow runs one job per original history and starts with case 0 key 3. Python and Z3 solve only on those runners. No database containers, product builds, or host JVMs are needed.

Runner commands and evidence links will be recorded in `published.json` and `EVIDENCE.md` after dispatch.
