# Saved register recheck

Throwaway reliability tooling only. No product changes. Do not start work after 2026-10-13T22:00:00Z. No JVM or build ran on the agent host.

## Fresh workload patch

Apply the existing DB adapter first, then:

```sh
python3 checker/patch-checker.py .jepsen/etcd
```

This replaces the project's heap option with `-Xmx10g`. It places a single shared `checker/concurrency-limit 1` inside `independent/checker`, around the per-key composition, and selects `:algorithm :wgl`. Clients, errors, generators, the versioned register model, CAS, and the number of operations per key do not change. Wrapping the outer independent checker would not serialize its per-key searches.

## Exact offline commands on Actions

Files in this directory will be published as `experiments/jepsen2-recheck` on `exp/jepsen2-recheck`. The supplied workflow runs eight jobs, one for each original unknown history. It needs Java 25, Leiningen 2.12.0, and enough RAM for a 10g heap. It starts no DB containers and builds no product code.

```sh
python3 experiments/jepsen2-recheck/prepare.py experiments/jepsen/evidence saved
bash experiments/jepsen2-recheck/run-offline.sh saved 0 recheck-results/0
```

Repeat the second command with case IDs 1 through 7 and different output paths. `prepare.py` verifies the three archive SHA-256 hashes against retained `SHA256SUMS`, then extracts only the full histories, original results, and independent histories/results. It writes a manifest with every extracted file's SHA-256. Case IDs are:

| ID | Original run | Case |
| --- | --- | --- |
| 0 | 38428716698 | tigerstore register kill |
| 1 | 38428716698 | tigerstore register partition |
| 2 | 38430056885 | tigerstore register kill rep2 |
| 3 | 38430056885 | tigerstore register kill rep3 |
| 4 | 38430056885 | tigerstore register partition rep2 |
| 5 | 38430056885 | tigerstore register partition rep3 |
| 6 | 38432262482 | tigerstore register kill, 50 ops/key |
| 7 | 38432262482 | tigerstore register partition, 50 ops/key |

Every key runs in a fresh JVM with explicit `java -Xmx10g`. WGL gets 120 seconds. On an unknown verdict, crash, or timeout, the runner tries Knossos linear for 180 seconds, then competition for 180 seconds. All three use the same model and original operations. The external timeout kills the entire JVM, so no abandoned worker overlaps the next attempt. A definite false verdict is retained and stops attempts for that key. It is not retried until a solver returns true.

The runner checks every key, including previously passing ones. It compares the full history's complete key projection with the retained independent history before analysis. It verifies that the full history's key set equals the retained independent directories. It refuses unknown process types or malformed client operations. It drops only nonclient nemesis records before calling Knossos. These are unpaired info events, not register calls, and do not impose register state transitions. Their removal preserves the relative order of every client event. It does not remove any client operation, rewrite errors, or change invocation/completion order.

`summary.json` is true only when every expected key has a definite true verdict with a successful process exit and written analysis. Missing results, unknowns, OOM, deadline stops, and timeouts cannot pass. False takes precedence over unknown. Original evidence remains untouched. This replaces only the original register workload verdict; the original fault observations and other composed checker results remain separate requirements.

`offline.selftest` runs on Actions before rechecking. It tests required unknown writes, required unknown CAS, CAS comparison failures, stale reads, version increments, and writes that may take effect after an info completion.

## Why the checker remains valid

The exact upstream register workload already uses `independent/checker`. It does not check the entire multi-key history as one Knossos register. Jepsen 0.3.11's `independent/checker` materializes per-key histories, unwraps tuple values, includes unkeyed nemesis records in each history, and checks keys through `bounded-pmap`. Reducing operations per key bounded existing independent searches. It did not introduce key independence.

Each operation in this workload reads or modifies one register key. Its CAS compares that key's value and writes only that key. The model's version is the key's etcd version, not a shared cluster revision. Linearizability is compositional for independent objects. Each key must have a legal order that respects the original completed-operation real-time constraints. Their legal orders can be combined into a legal history of the product object. This argument would not justify independent checking of cross-key transactions or global revision constraints; this workload has neither.

The offline model is the exact `VersionedRegister` record from upstream commit `b47cc401f27bfb7c28afd9ac088b5f0dca4501b8`. Only its namespace and requires differ. It preserves upstream handling of nil versions and nil read values. It does not strengthen or weaken the original model, and makes no claim beyond that model. Failed operations retain upstream Knossos semantics; no `:info` is relabelled `:fail`.

Knossos preprocesses successful completions into invocation values, removes definitely failed operations, and synthesizes info completions for unfinished invocations. WGL retains ambiguous mutations as optional pending operations. Its linked-list preprocessing moves info responses to the end, allowing an uncertain write or CAS to linearize after the client stopped waiting. A mutation that is necessary for an observed version/value cannot simply disappear. An uncertain CAS still must satisfy its compare operand and version increment if it linearizes. This is the same uncertainty semantics as the existing upstream checker, not omission of unknown writes.

Jepsen defaults to Knossos competition, which runs WGL and linear searches simultaneously per key. The concurrency limit cuts active per-key analyses to one; WGL cuts that key to one algorithm. Scheduling and solver selection change resource use, not the register specification. The offline process isolation also avoids retaining failed search heaps between attempts.

## Memory finding and limits

Jepsen 0.3.11 depends on Knossos 0.3.15. Knossos's `search/run` installs a JVM-wide memory notification listener. Its memory helper sets a collection usage threshold at 90% of the selected heap pool's maximum. Exceeding that threshold causes active searches to abort with `:cause :out-of-memory`. The result need not mean the OS killed Java or that Java threw `OutOfMemoryError`. Concurrent key searches share the heap and can all receive the notification. WGL stores explored configurations in a hash set; many ambiguous writes/CAS can still exhaust 10g on one key.

Neither 10g nor WGL guarantees resolution. If all three attempts remain unknown, retain the original inconclusive status and report the key, solver, heap, and cause. A larger dedicated runner or an independently validated equivalent CAS/version solver is the next step. Elle's append/transaction models are not a replacement for this register/CAS specification.

## Pinned source locations

- Register/model: https://github.com/jepsen-io/etcd/blob/b47cc401f27bfb7c28afd9ac088b5f0dca4501b8/src/jepsen/etcd/register.clj
- Jepsen 0.3.11 tag commit: `437d99caabf4573fd9e0e935dd202a6e567b8c26`.
- Independent split and bounded parallel check: https://github.com/jepsen-io/jepsen/blob/437d99caabf4573fd9e0e935dd202a6e567b8c26/jepsen/src/jepsen/independent.clj#L271-L387
- Limit and algorithm selection: https://github.com/jepsen-io/jepsen/blob/437d99caabf4573fd9e0e935dd202a6e567b8c26/jepsen/src/jepsen/checker.clj#L115-L258
- Knossos 0.3.15 tag commit: `9242b067e78fc414832ac6acab8af44ddb5e0d60`.
- WGL uncertain operations: https://github.com/jepsen-io/knossos/blob/9242b067e78fc414832ac6acab8af44ddb5e0d60/src/knossos/wgl.clj#L492-L635
- Memory threshold: https://github.com/jepsen-io/knossos/blob/9242b067e78fc414832ac6acab8af44ddb5e0d60/src/knossos/memory.clj
- Search abort and competition: https://github.com/jepsen-io/knossos/blob/9242b067e78fc414832ac6acab8af44ddb5e0d60/src/knossos/search.clj
