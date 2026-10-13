# Offline recheck status

Knossos counts below use the seven completed history jobs. Cancellation of case 5 was requested as superseded at 2026-10-13T18:23:39Z after parent supplied verified witnesses for every key. Actions confirms the run and case 5 ended cancelled. Cancellation is incomplete evidence, not a pass. The separate exact witness solver has resolved all eight original histories.

## Final witness result

Parent reports Actions run [38444996427](https://github.com/djosh34/tigerstore/actions/runs/38444996427) succeeded for all eight histories and all 143 keys. Every SAT witness passed separate Python verification and exact upstream Clojure-model replay. The solver retained all 15,987 original operations and included 16 info mutations in its witnesses. I independently counted 15,987 client invocations in the retained original full histories.

Parent's `solver/old-verdicts.json` records archive, full-history, per-key, and witness hashes, both verifier audits, and artifact URLs. The original fault observations and non-workload checks remain separate. The witness artifacts themselves were collected by parent, not by this helper.

A replay-verified legal witness proves that a linearization exists. It does not need to enumerate every other possible order. Optional pending info mutations may be omitted from the chosen completed extension, or included after their info event, exactly as the upstream uncertainty semantics permit. No original operation is removed from the evidence or relabelled as a definite failure.

## Knossos resource experiment

- Main recheck: https://github.com/djosh34/tigerstore/actions/runs/38441161188
- Tooling branch: `exp/jepsen2-recheck`, commit `04f994e3a2da59ab92e9ccf50cb6f9c8702eccbe`.
- Four 30-minute WGL retries: https://github.com/djosh34/tigerstore/actions/runs/38443204924
- Retry branch: `exp/jepsen2-recheck-long`, commit `2abd5854f639383b4d638e0bfe6fc1b01f20d275`.

No product files changed. All JVMs ran on GitHub Actions, never this host.

The recheck verifies every key and every client event against the original full-history projection. It uses an exact copy of the upstream model, Knossos 0.3.15, and a fresh 10g JVM for each attempt. WGL gets 120 seconds, then unresolved keys get 180 seconds each under linear and competition. All 18 regression checks passed under the three algorithms on Actions.

Seven history jobs completed before cancellation. Their 132 key histories contain 117 definite true verdicts and 15 unknowns. Case 5's retained partial artifacts add two true keys and six completed unknown keys. Its key 8 was interrupted, and keys 9 and 10 did not start. Across the full 143-key manifest, this Knossos experiment therefore has 119 true keys, 21 completed unknowns, and three incomplete checks. No false verdict was returned. Case 5's overall Knossos verdict remains incomplete.

| Case ID | Original run | Case | True / total keys | Still unknown |
| --- | --- | --- | --- | --- |
| 0 | 38428716698 | kill-1 | 8 / 9 | 3 |
| 1 | 38428716698 | partition-1 | 10 / 12 | 4, 5 |
| 2 | 38430056885 | kill-rep2-1 | 7 / 10 | 4, 5, 7 |
| 3 | 38430056885 | kill-rep3-1 | 7 / 9 | 3, 6 |
| 4 | 38430056885 | partition-rep2-1 | 7 / 11 | 0, 2, 7, 8 |
| 5 | 38430056885 | partition-rep3-1, cancelled | 2 / 11 | 0, 1, 2, 3, 4, 5; 8 interrupted; 9, 10 not started |
| 6 | 38432262482 | kill-rep1-1, 50/key | 33 / 34 | 3 |
| 7 | 38432262482 | partition-rep1-1, 50/key | 45 / 47 | 25, 30 |

All 21 completed unknowns hit the external WGL time limit. Linear and competition returned Knossos `:out-of-memory` except case 1/key 4, case 4/key 8, and case 5/key 3, which timed out under all algorithms, plus case 5/key 5 and case 7/key 30, where linear timed out and competition returned OOM. Incomplete keys have no definite verdict.

Six formerly OOM keys now have definite WGL true verdicts. They are case 2/key 3, case 4/key 1, case 6/key 14, and case 7/keys 14, 21, 23. For example, case 6/key 14 checks 100 client events including 18 unknown mutations, has an identical full-history projection, and returns true with final version 36/value 3.

`original-unknown-keys.json` lists all 29 original OOM keys. The retained evidence contains the unresolved-key inventory and each attempt's cause, downloaded analyses, logs, runtime metadata, input hashes, and verdicts. The machine-readable summaries do not pass missing or unknown keys. `evidence/SHA256SUMS` hashes the retained compressed recheck and long-WGL artifacts. Original full histories remain in the three pinned archives already retained under `experiments/jepsen/evidence`.

The four longer WGL retries also returned `:unknown, :cause :out-of-memory` at 10g, before the 30-minute budget expired. They checked case 0/key 3, case 1/keys 4 and 5, and case 6/key 3. Their artifacts are retained in `evidence/long-wgl-38443204924.tar.gz` and Actions run `38443204924`.

A 10g heap and serialized independent checks reduce resource competition but do not solve the retained histories by themselves. Do not replace an original unknown history with these narrower partial results. A definite result for every expected key remains necessary. Parent's separate exact witness solver uses Python and upstream-model replay. Parent's final solver result validates all 143 keys in cases 0 through 7. Those witness results are separate from this Knossos result inventory and have not been independently collected here. A verified legal witness proves existence of a linearization without exhaustively searching the other orders.
