# Revision witness backtracking on #425

Diagnosis for plan 5, outcome #499. This branch is a prototype only, never merged or cherry-picked. The worker rewrites any fix.

## Input and base

Base `b82961a29a96bbd8fcbe3417884d84a6c143e088`, branch `work/499-witness`. It includes the exact try/undo, scoped-answer and compaction speedups. The first fetch ran before that branch was available. The second fetch found it and the diagnosis branch was created from it.

The saved input is `check/testdata/witness-425.json.gz`, copied from `/tmp/r482/h425/TestMembershipFaults_202610250103508/history.json`. It has 11,184 operations and highest observed revision 4,238. Decompressed SHA-256:

```
a352b5233af3774a311b71413d6e244af80a7703770fd3ec3e2fb90ae7bde662
```

This is a checker regression, not a simulator regression. The environment fix changes the seed's execution, so a simulator rerun no longer supplies this input. The test removes `Log` and every operation's `Effect` before checking, so the revision search cannot get an order from the committed log.

## Cause

`revisionOrder.reachable` already computes necessary bounds for future observed key counts. It counts keys that must survive and keys that might exist after the remaining writes. `may` overestimates what remaining writes can do. Its `lostCompare` check correctly rules out a transaction whose required mod revision can never return.

But `reachable` used only the nearest revision with any count, not all the counts in its existing 64-revision window. The nearest count could concern unrelated keys. An irreversible loss would survive this check, then be detected only after the search permuted the intervening gaps.

For #425, the gaps are revisions 110 through 119. Before gap 110 the store contains `k0`, `k2`, `k3`, `k6` and `k7`. `k7` was created and last written at revision 107. There are 21 candidates at gap 110, including old ambiguous transactions, later puts, and these deletes:

| Operation | Request |
| --- | --- |
| 295 | delete `k7` |
| 260 | delete [`k6`, `k8`) |
| 318 | delete [`k5`, `k8`) |

Each of those three deletes loses `k7` irreversibly before revision 126. No remaining acked put can restore it in time. The ambiguous put in op 192 requires `k7`'s mod revision to equal 64, which is already impossible at revision 109, even before a deletion.

The next observed count is at revision 120, before the acked range delete at revision 121. It concerns [`k1`, `k4`), so it says nothing about `k7`. The count at revision 125 from op 329's delete at revision 126 requires exactly one `k7`. Considering that count immediately rejects all three harmful deletes. In the trial after each delete, its lower and upper bounds are both zero, but the observed count is one.

Without the fix, the search chose op 193 at gap 110 and op 295 at gap 111, then spent the whole 50-million-step budget searching suffixes below that already impossible prefix. At exhaustion its gap frames were:

```
revision  tried  candidates
110       3     21
111       4     20
112       5     19
113      12     18
114       2     17
115       7     16
116      12     15
117       8     14
118       4     13
```

The last three candidates were op 313 at gap 116, op 305 at gap 117, and op 260 at gap 118, exactly as in #425. The furthest failure was the acked delete of `k7` at revision 126 getting `deleted:0`, leaving model revision 125 instead of producing revision 126. Other suffixes failed at the limited read at revision 125, because they preserved an old life of `k0` or had the wrong total count. Reordering those suffixes cannot restore `k7`.

A correct history does not require this exploration. The prototype accepts the following gap writers, then the fixed acked writers:

```
revision: 110 111 112 113 114 115 116 117 118 119
op:       193 237 248 172 300 301 302 309 313 320
```

This is a client-valid witness, not a reconstruction of the actual log. Ambiguous calls may take effect at any time after their call, including after their error return. The final replay checks every required response and real-time constraint.

## Exact prototype

Check every observed count within `countHorizon`, rather than just the nearest observed revision. The candidate order, model, final observation pass and real-time verification are unchanged. The horizon is still 64; there is no unbounded scan into the rest of the history.

Each additional count is a necessary condition, using the same conservative `may` bounds as before. If even the optimistic maximum number of keys is below the observation, no continuation can produce it. If the keys that cannot be deleted already exceed the observation, no continuation can produce it either. A possible continuation is not removed. A history still passes only when the existing model replay and final verification accept a complete order. There is no fallback that treats a timeout or a missing witness as success.

## Results on this host

All Go commands used `GOFLAGS=-p=2`. One heavy command ran at a time. No simulator campaigns ran.

| Input and version | Result | Steps | Backtracks | Search time |
| --- | --- | ---: | ---: | ---: |
| #425, base, budget 50,000,000 | timed out at revision 119 | 50,000,001 including the stopping tick | 14,918,951 | 290.048 s |
| #425, prototype | pass | 21,537 | 192 | 0.027 to 0.033 s |
| #425, base, new regression budget 25,000 | timed out | 25,001 including the stopping tick | 7,622 | 0.140 s |

The base run's position and backtrack count exactly reproduce #425's message. For that measurement the production diff was reversed and the same internal search driver was run with `DIAG_STEPS=50000000`. Restoring only the production diff gives the passing result.

Reproduction on the prototype:

```sh
GOFLAGS=-p=2 go test ./check -run 'TestWitness425|TestReachableBeyondNearestCount|TestFastWitnessMatchesSlow' -v -count=1
GOFLAGS=-p=2 go test ./check -count=1
GOFLAGS=-p=2 go run ./cmd/checkhistory -checkers witness-revisions -timeout 10m check/testdata/witness-425.json.gz
```

The full `go test ./check -count=1` passed in 13.014 s. `TestFastWitnessMatchesSlow` still rejected all 238 stale histories with both fast and slow replay. The public `checkhistory` command passed the original, unstripped input in 0.0308 s. `TestWitness425` also calls the public `Witnessed` API with a 25,000-step budget after measuring the internal search.

`TestReachableBeyondNearestCount` is the short unit reproduction. It places an unrelated count at revision 3 and a decisive count at revision 5. It checks both lost keys and surplus keys, and distinguishes a repair before the observation from one after it. Reversing the production diff makes the four impossible cases incorrectly return reachable, and makes `TestWitness425` exceed its step budget. Both tests pass with the diff restored.

## The two #485 control histories are unavailable

Downloaded with `gh run download`:

- Run [42020412801](https://github.com/djosh34/tigerstore/actions/runs/42020412801), artifacts `exp485-planted-D80-shard0`, `exp485-planted-D80-shard1`, `exp485-planted-D320-shard0`, `exp485-planted-D320-shard1`.
- Recovery run [42042610751](https://github.com/djosh34/tigerstore/actions/runs/42042610751), artifacts `exp485-planted-D320-shard0`, `exp485-planted-D320-shard1`.

The original artifacts contain only `metadata.json`, `rss.csv`, `seed.log` and `seeds.jsonl`. The recovery shard 0 contains only `recovery-skipped.json`. None contains a history. The workflow uploads the experiment output directory, and `research/485/planted/run.py` and its test only persist findings and measurements there, not the underlying histories.

Both failures are present in the original run's logs:

| Control | Ops | Step budget | Backtracks | Search position |
| --- | ---: | ---: | ---: | --- |
| 80 s, seed 201, shard 0 | 12,480 | 50,000,000 | 24,249,340 | revision 35 of 1,518, 14 gaps |
| 320 s, seed 1529, shard 1 | 33,054 | 50,000,000 | 8,832,327 | revision 127 of 1,802, 13 gaps |

The 80 s failure reaches a resolved put at revision 35 that cannot fit. The 320 s failure reaches a read at revision 135 with `k5` version 2 instead of 4. These logs alone do not prove the same pruning change fixes either history. Their before/after step counts and passing results remain unmeasured. Replaying them on today's simulator would not recover the original input reliably because #482 changed the environment. This diagnosis makes no claim that all three histories are fixed.
