# Jepsen round 2

The eight original inconclusive register histories are linearizable under the unchanged upstream versioned-register model. All 143 independent keys now have concrete linearizations checked against their full original histories. Increasing the heap alone did not resolve them.

Product code is unchanged. Builds, Jepsen, fault injection, solver searches and JVM checks ran only on free GitHub runners. Local work was source inspection, history accounting and evidence packaging.

## What made the original histories difficult

The primary comparison is the eight unresolved tigerstore histories and their eight direct etcd controls, not every workload from round 1. Tigerstore was `90870876b6459334c7eddd67c0f9d184267f2eba`. Etcd was stock v3.7.2.

| Store | Completed calls | :ok | :fail | :info |
| --- | ---: | ---: | ---: | ---: |
| Etcd | 14,632 | 11,739 | 2,727 | 166 |
| Tigerstore | 15,987 | 10,214 | 2,249 | 3,524 |

Most `:fail` calls are ordinary CAS comparison mismatches, 2,602 on etcd and 2,083 on tigerstore. The other failures are read timeouts. Neither store has a definite write failure in these histories. Tigerstore has 3,523 indeterminate mutations and one indeterminate read exception. Etcd has 166 indeterminate mutations.

Client-observed latency summaries follow. These are observations under different random fault schedules, not throughput benchmarks.

| Store | Outcome | Median ms | p95 ms | p99 ms |
| --- | --- | ---: | ---: | ---: |
| Etcd | :ok | 2.493 | 6.148 | 2,392.821 |
| Etcd | :fail | 2.891 | 3,719.909 | 5,001.274 |
| Etcd | :info | 5,001.209 | 5,006.473 | 5,013.863 |
| Tigerstore | :ok | 1.975 | 5.351 | 1,406.857 |
| Tigerstore | :fail | 2.673 | 5,000.787 | 5,001.278 |
| Tigerstore | :info | 5.579 | 2,534.824 | 5,002.857 |

Of tigerstore's 3,524 indeterminate completions, 3,358 report `[:unavailable "etcdserver: no leader"]`. Of those, 3,127 finish within 10 ms and 3,322 within 100 ms. The eight etcd controls have no recorded no-leader response. Their unknown calls mostly reach the client's five-second timeout. The difference is predominantly faster rejection, not slower failure.

[The analysis](analysis/REPORT.md) gives counts and latency distributions for each history, operation, outcome and fault/budget combination. It also reports all 37 saved register histories separately, so those totals cannot be confused with the matched primary comparison.

### Calls open at fault boundaries

[Every fault-edge record](analysis/fault-events.csv) and the per-history `fault-edges.log` files under [analysis/histories](analysis/histories/) record exact event indices, times, open call counts by operation, and the outstanding calls. Both nemesis invocation and completion are included. They are different instants even though upstream records both as `:info`.

The primary comparison has 598 nemesis records, or 299 invocation/completion pairs. No more than six client calls are physically open at once. For example, the first tigerstore partition starts at index 294 with zero open calls. Its completion at index 298 has one open read. Healing starts at index 504 with four open calls, one read, one write and two CAS calls. All four are still open at healing completion, index 509.

The checker has a different problem. An indeterminate mutation can remain possible after its client returns. One tigerstore key has 185 such mutations, while the corresponding etcd history has at most seven per key. Fast no-leader replies let clients issue many new calls without resolving earlier uncertainty. Mixed successful observations and unknown mutations force a search through possible effects and orders. All-unknown keys can be easy because no successful observation requires an effect.

Upstream already uses Jepsen's independent-key checker. It also runs Knossos's linear and WGL searches in competition and can check several keys concurrently. These searches shared the original 4 GiB heap. All 29 originally unknown key results report `:out-of-memory`. Independence between keys does not remove ambiguity within a key.

## Is the difference a product bug?

The [matched leaderless probe](https://github.com/djosh34/tigerstore/blob/ac8276a28a9fda1ea66722fb1de1f7629b9c60fb/experiments/jepsen2-leader/REPORT.md) tested tigerstore `b5f2e28ef2edd94393d62952b1568763ebc12fe2` against stock etcd v3.7.2. Three independent jobs each tested a surviving former leader and a surviving follower. All 12 cases passed.

Etcd's asynchronous raft `Node` stops accepting proposals while it has no leader. Its request can wait for recovery. Tigerstore calls `RawNode.Propose`; a leaderless RawNode rejects before acceptance, and tigerstore returns Unavailable/no leader. Direct no-retry Put and CAS calls took less than one millisecond to reject on tigerstore. Etcd reached the one-second probe deadline.

This is a client-visible availability disadvantage for an unretried request during a brief outage. With healing scheduled 300 ms after submission, all six etcd Puts succeeded in 313 to 1,972 ms. All six tigerstore Puts rejected immediately and remained absent after recovery. The acknowledged seed survived in every case.

It is not a demonstrated consistency defect or violation of the supported API contract. No inspected contract requires identical admission timing during quorum loss. Both stores honor `hasleader=true` on unary RPCs, reject new require-leader watches, and close existing require-leader watches after leader loss. Plain watches remain open and deliver a post-heal sentinel. A missing require-leader implementation does not explain these histories.

The Jepsen client conservatively treats Unavailable mutations as unknown. We kept that classification. A source-level rejection path does not justify changing a recorded call to a definite failure when client retries and response loss may matter. No product bug was filed for the timing difference.

## How the histories obtained definite verdicts

First, the offline recheck gave each independent key its own 10 GiB JVM. It tried WGL, then linear and competition when needed. [Retained attempts](https://github.com/djosh34/tigerstore/tree/48af5469af41f2e6835a896958cb666163ecd84d/experiments/jepsen2-recheck) contain 119 true key verdicts, 21 unknowns and three incomplete checks when the remaining job was cancelled after certificates resolved the histories. All four longer WGL retries also returned `:out-of-memory` with a 10 GiB heap. These remain failed or incomplete attempts, not passes.

The fallback searches for an explicit linearization of each key. Z3 chooses which optional unknown operations occur, orders mutations by their version increments, checks CAS operands and returned values, and places successful operations inside their original invocation/response intervals. Unknown operations have no upper bound at their `:info` event.

A separate Python verifier reconstructs calls from the full original history and checks every decision, value and real-time edge. A Clojure verifier also reads the original history and replays the chosen order through the unchanged upstream `VersionedRegister` record. Neither verifier relies on the solver's claim of success. A SAT result alone does not pass. Both verifiers must accept it, and every expected key must be present.

All 15,987 original calls remain in the decision records. The eight certificates include 16 unknown mutations, including two ordered after their `:info` responses. Other optional operations are omitted in the selected legal completion, not relabelled as definite aborts. Definite failures retain upstream Knossos semantics. No history was shortened and no successful operation was dropped.

[Permanent original-history certificates](https://github.com/djosh34/tigerstore/tree/cd0ba590f0eb4e60a9f51bac41b0721883d970b4/experiments/jepsen2-solver) include original archives and hashes, per-key decisions, both verifier outputs, selftests and the exact executed scripts. The original OOM records remain unchanged. A legal completion proves the recorded history has a linearization; it does not establish which ambiguous RPCs actually committed.

The solver tests include fixed cases, 80 exhaustive differential cases and tampered-certificate rejection by both verifiers. Its first attempt exposed EDN set/symbol metadata parser gaps. Those failed attempts and the regression remain archived. The [provenance ledger](https://github.com/djosh34/tigerstore/blob/fb1cd3648d7e7d9a354a19568697b4e8b805ffaf/experiments/jepsen2-solver/MODEL_PROVENANCE.md) also corrects an early whole-file hash label. It described the checker-patched register source, not the literal upstream file. The model record is byte-identical to pinned upstream source, as both the helper and parent independently checked.

Elle was not substituted for this versioned register/CAS model. A different workload's Elle verdict would not certify these histories.

## Fresh workloads and commits

The exact workload settings remain `--concurrency 2n --rate 30 --time-limit 90 --nemesis-interval 5`, with 200 and 50 operations per key. Each of `kill`, `partition`, `kill,pause`, `partition,pause`, and `kill,partition,pause` ran twice per tested store/commit. Each case used a fresh free `ubuntu-24.04` runner, one control container and three database containers. Client endpoints were plain gRPC and peers used mTLS.

The Jepsen etcd commit, dependency versions and container digests remain pinned as in [round 1](../jepsen/REPORT.md). The online checker uses a 10 GiB heap, serial per-key linearizability checks and WGL. The model, client, generator and nemesis implementations remain unchanged.

On a runner before the deadline:

```sh
bash experiments/jepsen2/run.sh etcd register partition 90 200
PRODUCT_SHA=2b072ac7e7acb4aeef1a6e794145914201607b8c \
  bash experiments/jepsen2/run.sh tigerstore register partition 90 200
```

Tigerstore `b5f2e28` was newest main when the round began. After main advanced, the full 20-case candidate matrix ran again at `2b072ac7e7acb4aeef1a6e794145914201607b8c`, newest main when that second matrix started. The same 20 stock controls precede both candidate matrices. Matching commands do not mean identical random fault schedules, and the products did not share a runner.

| Store and commit | Histories | Calls | Keys | Original online register result | Final register verdict | Fault evidence |
| --- | ---: | ---: | ---: | --- | --- | --- |
| Etcd v3.7.2 | 20 | 31,522 | 410 | 20 true | 20 valid | 20 observed |
| Tigerstore `b5f2e28` | 20 | 35,967 | 452 | 4 true, 11 unknown, 5 missing after timeout | 20 valid | 19 observed, 1 failed |
| Tigerstore `2b072ac` | 20 | 34,436 | 432 | 20 unknown | 20 valid | 20 observed |

The first live matrix is [run 38440292743](https://github.com/djosh34/tigerstore/actions/runs/38440292743). Its Actions job results remain 23 successes and 17 failures. The latest-main matrix is [run 38448413416](https://github.com/djosh34/tigerstore/actions/runs/38448413416); all 20 original jobs remain failed because their online checker returned unknown. Certificates supply definite register verdicts without changing those original job results.

Fresh call outcomes are:

| Store and commit | :ok | :fail | :info |
| --- | ---: | ---: | ---: |
| Etcd v3.7.2 | 25,123 | 5,869 | 530 |
| Tigerstore `b5f2e28` | 22,752 | 5,339 | 7,876 |
| Tigerstore `2b072ac` | 23,626 | 5,489 | 5,321 |

The latest-main certificates passed in [run 38454147853](https://github.com/djosh34/tigerstore/actions/runs/38454147853), including all 432 keys and 34,436 calls. Their witnesses include 66 unknown operations. All original non-workload checker components returned true, and every requested fault class has observed injection and healing. [Permanent latest-main certificates and report](https://github.com/djosh34/tigerstore/blob/fb1cd3648d7e7d9a354a19568697b4e8b805ffaf/experiments/jepsen2-solver/latest20-report.md).

Across the eight original and 60 new histories, [the audited final manifest](evidence/final-verdicts.json) covers 68 histories, 1,437 per-history keys and 117,912 calls. Every register verdict is valid. [CASES.md](CASES.md) lists each history, its original result and separate fault verdict. The parent independently checked certificate key sets, original history hashes, operation accounting and tested product commits.

The first fresh matrix's 40 histories contain 67,489 calls and 862 keys. All 40 obtained independently validated register certificates in [run 38448351446](https://github.com/djosh34/tigerstore/actions/runs/38448351446). The witnesses include 134 unknown operations. Five online timeouts left some per-key files unwritten; the offline runner generated 46 missing projections from complete full histories and checked each against its source. [Permanent certificates and source hashes](https://github.com/djosh34/tigerstore/blob/cd0ba590f0eb4e60a9f51bac41b0721883d970b4/experiments/jepsen2-solver/fresh40-report.md).

## Fault evidence and failed attempts

Every claimed fault requires recorded injection/healing and independent observations. Kills require replacement PIDs, pauses require a stopped process state, and partitions require DROP rules with packet hits. The raw-history summary checks these even when the online checker did not return. Empty partition maps with no links do not count as an applied partition. These checks prove each requested class took effect, not every random target selection.

One first-wave tigerstore case, `kill,partition,pause`, 50 operations per key, repetition 1, has a valid register history but no observed stopped process. Its fault-coverage check remains failed. Its green model verdict does not certify a pause. The other 39 first-wave histories satisfy the fault checks.

Both latest-main repetitions of that same `kill,partition,pause`, 50-operations-per-key command have observed stopped processes as well as kills, partition packet hits and healing. Every one of the 20 latest-main histories passes the fault-evidence checks. The earlier missing-pause attempt still counts as a failed attempt.

The report does not turn the original Knossos OOMs, online timeouts, missed pause, parser failures or the first leader-probe setup failure into successful attempts. Their evidence remains public. The certified histories and the successful coverage runs are identified separately.

## Scope and retained files

No definite tigerstore consistency anomaly was found, so no product `bug` issue was opened. These are versioned-register and targeted leaderless checks, not completion of #49. They do not certify every RPC, watches in all fault schedules, independent clock skew, power-loss durability or the etcd robustness suite.

[Evidence](evidence/) retains complete full histories, available independent histories, original checker results, server logs/data, exact commands, binary hashes, upstream patches, fault observations and Actions metadata. Plot images are omitted; their source histories remain. The analysis and solver branches retain reproducible scripts and certificates independently of Actions artifact expiration. No product code was changed or merged.
