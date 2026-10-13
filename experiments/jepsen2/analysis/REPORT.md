# Saved register evidence analysis

## Finding

The eight unresolved tigerstore register histories contain many more ambiguous operations, mostly fast `etcdserver: no leader` responses. They do not show generally slower requests than the matched etcd controls.

Across these eight tigerstore histories, 3,358 of 3,524 `:info` completions carry `[:unavailable "etcdserver: no leader"]`. Of those 3,358 responses, 3,127 finish within 10 ms and 3,322 within 100 ms. The eight matched etcd controls contain no recorded no-leader responses. Their 166 `:info` completions have a median latency of 5,001.209 ms. Tigerstore's `:info` median is 5.579 ms.

Fast unknown responses let clients issue more mutations while prior mutations remain ambiguous to the checker. This explains the shape of the difficult histories better than a slow-request explanation. The saved evidence does not establish a consistency violation, and this analysis does not relabel any operation.

## Scope and provenance

The primary scope is exactly eight unresolved tigerstore histories and eight matched etcd controls. All counts in the main sections below use only those 16 histories.

- The six 200-operation tigerstore histories use run `38428716698` for the first kill and partition attempts and `38430056885` for repetitions 2 and 3. Their controls are the corresponding cases in `38427801610` and `38430056885`.
- The two 50-operation tigerstore histories use run `38432262482`. Their direct controls are the cases in `38430904489`, after the same readiness correction. Four earlier 50-operation controls in `38430224306` are auxiliary, not part of the primary control totals.
- Four 10-operation tigerstore histories and four 10-operation controls are auxiliary evidence for bounded checking.

The default analyzer extracts 28 histories, but marks these scopes separately. Its `core` store and configuration counts include only the 16 primary histories. The optional `all-register/` output includes all 37 saved register histories with results. It includes no set, append, or watch histories.

The source evidence is `experiments/jepsen/evidence` in the experiment checkout, or `../../jepsen/evidence` relative to this directory after integration into `experiments/jepsen2/analysis`. The analyzer accepts this location through `--evidence`. I read the experiment's original `REPORT.md`, `README.md`, and saved-evidence scripts. Original archives and repository files remained unchanged. Every original archive used here passed its saved SHA-256 check. The extracted histories retain their exact bytes and original outcome labels. `summary.json` records each main history's SHA-256 and archive hashes.

The original runs used tigerstore `90870876b6459334c7eddd67c0f9d184267f2eba`, etcd v3.7.2, Jepsen 0.3.11, and Knossos 0.3.15. The heap cap was 4 GiB. Commands match their workload settings, not their random fault schedules.

## Counts by store and operation

These are completed client operations, not successful writes or counts of history lines. A call contributes one outcome even though its history normally contains an invocation and a completion. Nemesis records are excluded.

| Store | Operation | :ok | :fail | :info | Total |
| --- | --- | --- | --- | --- | --- |
| Etcd | All | 11,739 | 2,727 | 166 | 14,632 |
| Etcd | Read | 7,956 | 125 | 0 | 8,081 |
| Etcd | Write | 3,146 | 0 | 76 | 3,222 |
| Etcd | CAS | 637 | 2,602 | 90 | 3,329 |
| Tigerstore | All | 10,214 | 2,249 | 3,524 | 15,987 |
| Tigerstore | Read | 7,204 | 166 | 1 | 7,371 |
| Tigerstore | Write | 2,527 | 0 | 1,829 | 4,356 |
| Tigerstore | CAS | 483 | 2,083 | 1,694 | 4,260 |

Tigerstore has 21.23 times as many `:info` completions as its controls. Its mutation count is 8,616 versus 6,551. Unknown mutations are 3,523 versus 166. The remaining tigerstore `:info` is a read with a `StacklessClosedChannelException`, preserved as recorded.

All 2,602 etcd CAS failures and all 2,083 tigerstore CAS failures are `:did-not-succeed`. They are expected compare-and-set mismatches, not transport failures. The failed reads are `[:timeout nil]`, 125 for etcd and 166 for tigerstore. Both stores have zero recorded definite write failures.

The unknown errors break down as follows.

| Store | Recorded error | :info count |
| --- | --- | --- |
| Etcd | `[:timeout nil]` | 161 |
| Etcd | `[:unavailable "Network closed for unknown reason"]` | 3 |
| Etcd | `"indeterminate: Connection reset"` | 2 |
| Tigerstore | `[:unavailable "etcdserver: no leader"]` | 3,358 |
| Tigerstore | `[:timeout nil]` | 164 |
| Tigerstore | `[:unavailable "Network closed for unknown reason"]` | 1 |
| Tigerstore | `"indeterminate: "`, the read exception | 1 |

`counts-by-history.csv` and the generated [tables](tables.md) contain counts for every history and every operation. `errors.csv` includes outcome, operation, exact error, and latency summaries.

## Counts for each primary history

Pair labels identify fault, operations per key, and repetition. The full run IDs and artifact names are in [tables.md](tables.md) and `summary.json`.

| Pair | Tigerstore :ok | Tigerstore :fail | Tigerstore :info | Etcd :ok | Etcd :fail | Etcd :info | Tigerstore unknown keys |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Kill, 200, repetition 1 | 1,291 | 257 | 88 | 1,624 | 305 | 27 | 1 of 9 |
| Kill, 200, repetition 2 | 1,466 | 316 | 106 | 1,098 | 311 | 20 | 4 of 10 |
| Kill, 200, repetition 3 | 1,325 | 337 | 112 | 1,326 | 315 | 15 | 2 of 9 |
| Kill, 50, repetition 1 | 1,210 | 314 | 129 | 1,291 | 291 | 25 | 2 of 34 |
| Partition, 200, repetition 1 | 1,568 | 307 | 514 | 1,442 | 337 | 20 | 2 of 12 |
| Partition, 200, repetition 2 | 1,368 | 278 | 494 | 1,830 | 400 | 23 | 5 of 11 |
| Partition, 200, repetition 3 | 1,095 | 219 | 845 | 1,752 | 420 | 16 | 8 of 11 |
| Partition, 50, repetition 1 | 891 | 221 | 1,236 | 1,376 | 348 | 20 | 5 of 47 |

All eight tigerstore main results are `:valid? :unknown`. All 29 unknown independent-key results give `:cause :out-of-memory`. The controls have 127 independent keys, all with definite valid results. Tigerstore has 143 keys in this primary scope, with 114 definite valid results and 29 unknown results.

## Counts by fault and budget

This table also uses only the eight unresolved histories and eight direct controls. No-leader medians refer only to mutations with that exact error. The 10 ms column counts those same calls, not all unknown calls.

| Store | Fault | Ops/key | Histories | :ok | :fail | :info | No-leader :info | No-leader median ms | No-leader at most 10 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Etcd | Kill | 200 | 3 | 4,048 | 931 | 62 | 0 | Not applicable | 0 |
| Etcd | Kill | 50 | 1 | 1,291 | 291 | 25 | 0 | Not applicable | 0 |
| Etcd | Partition | 200 | 3 | 5,024 | 1,157 | 59 | 0 | Not applicable | 0 |
| Etcd | Partition | 50 | 1 | 1,376 | 348 | 20 | 0 | Not applicable | 0 |
| Tigerstore | Kill | 200 | 3 | 4,082 | 910 | 306 | 239 | 6.266 | 192 |
| Tigerstore | Kill | 50 | 1 | 1,210 | 314 | 129 | 106 | 7.706 | 79 |
| Tigerstore | Partition | 200 | 3 | 4,031 | 804 | 1,853 | 1,794 | 5.628 | 1,684 |
| Tigerstore | Partition | 50 | 1 | 891 | 221 | 1,236 | 1,219 | 4.977 | 1,172 |

Across the four partition histories, tigerstore has 3,089 unknown calls versus 79 for etcd. Tigerstore's median unknown latency is 5.423 ms versus 5,001.343 ms for etcd. The partition no-leader responses have maxima of 18.931 to 29.677 ms across the individual histories. They are not five-second calls.

Kill histories have a longer no-leader tail. Across the four tigerstore kill histories, there are 345 no-leader unknown calls and 435 unknown calls in total. Some no-leader completions took up to 4,979.789 ms. The conclusion is that most such errors are fast, not that every no-leader error returns immediately.

`configuration-summary.csv`, `counts-by-configuration.csv`, and `latencies-by-configuration.csv` provide all fault, budget, store, operation, and outcome groups. Auxiliary scopes have separate rows.

## Latencies by operation and outcome

All values below are milliseconds, measured from client invocation to recorded completion. Quantiles use nearest rank. These are client-observed durations, not server handler timings. Client retries and connection handling can contribute.

| Operation | Outcome | Etcd median | Etcd p95 | Etcd p99 | Tigerstore median | Tigerstore p95 | Tigerstore p99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Read | :ok | 2.325 | 5.538 | 2,362.343 | 1.768 | 4.373 | 2,356.234 |
| Read | :fail | 5,001.012 | 5,002.616 | 5,005.642 | 5,000.849 | 5,001.841 | 5,002.641 |
| Read | :info | No calls | No calls | No calls | 8.274 | 8.274 | 8.274 |
| Write | :ok | 2.792 | 7.464 | 2,422.945 | 2.476 | 6.812 | 80.951 |
| Write | :fail | No calls | No calls | No calls | No calls | No calls | No calls |
| Write | :info | 5,001.019 | 5,006.473 | 5,013.863 | 5.476 | 5,000.547 | 5,003.151 |
| CAS | :ok | 2.986 | 7.731 | 2,256.910 | 2.710 | 11.272 | 147.947 |
| CAS | :fail | 2.839 | 7.291 | 2,575.986 | 2.578 | 6.772 | 60.758 |
| CAS | :info | 5,001.405 | 5,006.757 | 5,015.187 | 5.669 | 26.413 | 5,002.127 |

`latencies-by-history.csv` includes sample counts, minimum, mean, median, p95, p99, and maximum for each history, operation, and outcome. `latencies-by-store.csv` supplies the same data for each explicit scope. Empty groups remain empty, not zero-latency groups.

## Open calls at every fault edge

An open client call has a recorded invocation before the edge and no recorded completion before it. Its eventual `:ok`, `:fail`, or `:info` does not change whether it was open at that instant. Ordering uses history indices, with nanosecond timestamps retained.

Nemesis invocation and completion both have recorded type `:info`. The analyzer identifies their phase by pairing sequential nemesis records with the same function. It does not count both as completed faults. Fault start uses `:kill`, `:pause`, or `:start-partition`; healing uses `:start`, `:resume`, or `:stop-partition`.

The primary histories have 598 nemesis records, comprising 299 invocation/completion pairs. Tigerstore contributes 310 records and etcd 288. They include final healing operations, so healing counts can exceed fault-start counts. Every client invocation in these saved histories has a matching completion. There are no unpaired client informational records. At most six client calls are open at any point.

| Store | Fault | Edge | Phase | Recorded edges | Mean open calls | Maximum |
| --- | --- | --- | --- | --- | --- | --- |
| Etcd | Kill | Start | Invocation | 34 | 0.676 | 6 |
| Etcd | Kill | Start | Completion | 34 | 5.412 | 6 |
| Etcd | Kill | Heal | Invocation | 37 | 4.432 | 6 |
| Etcd | Kill | Heal | Completion | 37 | 4.459 | 6 |
| Etcd | Partition | Start | Invocation | 36 | 0.889 | 6 |
| Etcd | Partition | Start | Completion | 36 | 0.944 | 6 |
| Etcd | Partition | Heal | Invocation | 37 | 3.432 | 6 |
| Etcd | Partition | Heal | Completion | 37 | 3.351 | 6 |
| Tigerstore | Kill | Start | Invocation | 33 | 1.364 | 6 |
| Tigerstore | Kill | Start | Completion | 33 | 5.394 | 6 |
| Tigerstore | Kill | Heal | Invocation | 37 | 3.946 | 6 |
| Tigerstore | Kill | Heal | Completion | 37 | 3.919 | 6 |
| Tigerstore | Partition | Start | Invocation | 42 | 1.405 | 6 |
| Tigerstore | Partition | Start | Completion | 42 | 1.452 | 5 |
| Tigerstore | Partition | Heal | Invocation | 43 | 3.140 | 6 |
| Tigerstore | Partition | Heal | Completion | 43 | 3.093 | 6 |

These tables include attempted starts with empty targets. In the primary tigerstore histories, three kill completions have empty target maps and three partition completions have zero isolated links. Three etcd partition completions also have zero isolated links. `applied_nonempty` distinguishes those starts from nonempty completed injections. The saved fault-observation summaries reported no coverage errors for any of these histories. This analysis retains those observations; it does not claim every selected start applied a fault.

For a concrete edge, the first tigerstore partition history records the first start invocation at index 294 with zero open calls. Its completion at index 298 has one open read. The first healing invocation at index 504 has four open calls, one read, one write, and two CAS operations. Healing completes at index 509 with those same four calls still open. Those mutations later return `:info`. Conflating the two nemesis phases would lose this timing distinction.

Every extracted case has a readable `histories/<run>/<artifact>/fault-edges.log`, including all starts and heals, both phases, exact indices and times, per-operation open counts, and every open call's invocation and eventual completion. The [generated tables](tables.md) link the primary logs. `fault-events.csv` includes the same edges plus cumulative prior ambiguous mutations by key.

## Why the checker needs memory

Upstream already checks independent keys. The register workload uses `independent/checker` around the versioned-register model. Its generator reserves three reader threads and three mutation threads with the configured six-client concurrency. Independent checking does not remove ambiguity within a key.

Upstream `with-errors` classifies `UNAVAILABLE` as indefinite. Register marks reads as idempotent, so known indefinite read errors become `:fail`. The same errors on writes and CAS remain `:info`. A failed CAS comparison is a definite `:fail`. This analysis uses those original classifications.

For the linear algorithm, successful completions supply returned versions and values. Unknown mutations keep the invoked version unspecified. Their invocations remain candidates for linearization. WGL moves informational completions to the end, explores possible applications with backtracking, and caches sets of linearized operations and model states. Unknown operations can be omitted or applied where the model permits. Their quick client response does not close the checker uncertainty interval.

This distinction is measurable. There are at most six physically open client calls, but the first tigerstore partition run has up to 185 ambiguous mutations on one key. The corresponding controls have at most seven on a key. Across the three 200-operation tigerstore partition runs, per-history maxima are 185, 184, and 157. The controls' maxima are 7, 4, and 4. For 200-operation kills, tigerstore maxima are 51, 31, and 61 versus 6, 10, and 6 for etcd.

The 50-operation tigerstore partition run has ten keys with all 50 calls recorded as `:info`. Nine of those keys have all 50 calls report no leader; the other has 49 no-leader errors and one timeout. Key 7 completes 50 unknown mutations in 1.479 seconds, key 29 in 1.477 seconds, and key 38 in 1.442 seconds. They consume the per-key budget quickly.

All ten all-unknown keys have definite valid checker results. With no successful observation forcing their effects, the checker can leave those operations pending. The five unknown keys in that same run are different keys, 14, 21, 23, 25, and 30, with 19, 23, 20, 35, and 48 unknown mutations respectively. Saved configurations show up to 49 pending candidates on key 30. Ambiguity count alone does not predict which key will exhaust checking resources. Successful observations and version constraints matter.

Measured checker evidence follows.

| Fault and budget | Tigerstore analysis seconds, by repetition | Control analysis seconds, by repetition | Tigerstore memory-aborted keys |
| --- | --- | --- | --- |
| Kill, 200 | 40.682, 37.369, 65.980 | 2.193, 2.107, 2.033 | 1, 4, 2 |
| Kill, 50 | 60.556 | 2.991 | 2 |
| Partition, 200 | 30.686, 16.608, 89.939 | 1.849, 2.303, 1.257 | 2, 5, 8 |
| Partition, 50 | 92.120 | 2.939 | 5 |

These durations are saved log time from `Analyzing...` to `Analysis complete`, not Actions job durations. Tigerstore logs contain 33 memory-abort lines; the primary control logs contain none. Knossos installs a heap-pool collection-usage listener at 90% of the selected pool's maximum. The resulting abort returns `:unknown` with cause `:out-of-memory`. This is recorded checker memory pressure, not proof of a JVM crash or a measured whole-heap allocation size.

The original checker defaults to competition between the linear and WGL algorithms. Independent key checks can also run concurrently. They share the 4 GiB JVM budget, and a memory-pressure notification can abort several active checks. The 29 unknown key results are not 29 demonstrated individually intractable histories.

Saved reporters show linear estimated costs as high as `4.69E+284` in the third 200-operation partition run and `2.27E+71` in the first kill run. The linear estimator grows roughly as the pending-set cardinality times its factorial. These logged costs are estimates, not measured search-node counts. No saved counter proves that the checker explored that many nodes. Raw reporter lines are retained in each case's `checker-excerpts.log`.

The model memoizer's 1,024-state limit appears in both stores, with 206 log lines in the unresolved tigerstore histories and 126 in their controls. That message alone does not diagnose the failures. Memory aborts, per-key ambiguity, and analysis durations are the stronger evidence.

The four auxiliary 10-operation tigerstore cases have 958 unknown mutations in total, 878 with no-leader errors. They all pass while retaining those unknown labels. Their per-key unknown-mutation maximum is 10. Their analysis times are 7.674 to 41.892 seconds. This supports bounding ambiguity per independent key, but does not certify the eight larger unresolved histories.

## Require-leader and the fresh run

The parent's separate differential probe reports that both stores implement require-leader behavior, and that tigerstore rejects a leaderless proposal quickly while etcd waits. This saved-history analysis does not rerun that probe or independently prove the server path for every response. The latency and error counts agree with its explanation. There is no evidence here of a missing require-leader implementation or a product consistency bug.

The shared `UNAVAILABLE` error classification does not prove which underlying RPC attempts executed. Client retries and response loss can matter. Even when an inspected server path rejects before proposal, this report does not convert saved `:info` operations to `:fail`.

The announced run `38440292743` uses tigerstore `b5f2e28`, 200 and 50 operations per key, two repetitions, and kill, partition, kill plus pause, partition plus pause, and all three faults. It changes the checker to explicit WGL, a 10 GiB heap, and `checker/concurrency-limit 1` around the shared per-key linearizable checker. I read the new adapter and workflow. The model and operation classifications stay unchanged.

No artifacts for that run were present in the saved evidence directory during this analysis. Its results are therefore not included above. The analyzer supports its artifact names, combined faults, WGL result records, and arbitrary budgets. It records added heap settings, WGL selection, and concurrency limits from each saved patch. The larger heap and serialized linearizable checks remove resource confounders, but only retained results can establish their outcome.

## Optional all-register totals

`all-register/` is a separate scope, not an addition to the primary counts.

| Store | Histories with results | :ok | :fail | :info | Total operations | Verdicts |
| --- | --- | --- | --- | --- | --- | --- |
| Etcd | 21 | 32,761 | 7,751 | 366 | 40,878 | 21 definite valid |
| Tigerstore | 16 | 24,394 | 5,173 | 4,528 | 34,095 | 8 definite valid, 8 unknown |

These 37 histories contain 74,973 completed client calls and 1,184 nemesis records. They do not include blocked clock preflights, which have no register history or checker result. They include no evidence from the announced fresh run.

## Reproduction and files

Run the commands in [README.md](README.md) before the task deadline. `analyze.py` uses only the Python standard library. It reads archives and artifact directories, pairs recorded operations, aggregates observations, and verifies internal counts. It does not run Knossos, a JVM, a build, a workload, or a fault.

The primary extraction and aggregation took about 11 seconds and peaked at about 96 MiB RSS. Optional all-register analysis took about 15 seconds and peaked at about 120 MiB RSS. Exact process measurements are in the corresponding `analysis-runtime.json` files.

Source references used for the checker explanation are saved under `sources/`, with URLs and hashes in `sources/manifest.json`. The key references are `etcd-register.clj`, `etcd-client.clj`, `jepsen-checker.clj`, `jepsen-independent.clj`, `knossos-history.clj`, `knossos-wgl.clj`, `knossos-dll-history.clj`, `knossos-config.clj`, and `knossos-memory.clj`.

No comments or issues were posted. No subagents, builds, JVMs, workloads, or test suites ran on this host.
