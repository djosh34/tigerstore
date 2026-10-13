# Jepsen round 3

Tested tigerstore main `499a4c6759d0c1276aa4257af462295216f8ab21` against stock etcd v3.7.2, `68c065e562994b89e333e77b039ad066f933c586`. No product code changed. Builds, workloads, faults, solver searches and model replay ran only on free `ubuntu-24.04` GitHub runners. Host work was source inspection, evidence accounting and compression.

All 12 recorded histories have valid final workload verdicts and observed requested fault classes. No product correctness failure was confirmed. Two candidate online register attempts remain failed because their checking command timed out. Certificates give those histories valid register-model verdicts without changing the failed attempts.

| Store | Histories | Recorded calls | Original online passes | Original online timeouts | Final workload verdicts | Fault coverage |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| Tigerstore `499a4c6` | 6 | 6,844 | 4 | 2 | All valid | All observed |
| Stock etcd v3.7.2 | 6 | 6,894 | 6 | 0 | All valid | All observed |

[Every history and verdict](CASES.md). [Machine-readable final manifest](evidence/final-verdicts.json).

## Commands and versions

Six candidate jobs ran in parallel. Six matching controls used separate runners as capacity became available. Each store ran register, set and append twice, with a fresh three-node cluster per job.

```sh
PRODUCT_SHA=499a4c6759d0c1276aa4257af462295216f8ab21 \
  bash experiments/jepsen3/run.sh tigerstore register kill,partition,pause 90 50
bash experiments/jepsen3/run.sh etcd register kill,partition,pause 90 50
```

Each workload used this inner command, with its own `--workload` value:

```sh
lein run test --nodes n1,n2,n3 --username root \
  --ssh-private-key /root/.ssh/id_rsa --version 3.7.2 \
  --workload register --nemesis kill,partition,pause \
  --concurrency 2n --rate 30 --time-limit 90 \
  --nemesis-interval 5 --ops-per-key 50
```

The pinned upstream set and append generators ignore `--ops-per-key`. Set uses one set, checked with `checker/set-full` and `:linearizable? true`. Append uses three keys, transactions of up to four operations, and Elle's strict-serializable model. Their generators and checkers stayed unchanged. Register uses independent keys and the upstream versioned-register/CAS model.

Reused [round 1](source/issue49-comments.json) and [round 2](source/round2-report.md) scripts. Jepsen etcd is pinned at `b47cc401f27bfb7c28afd9ac088b5f0dca4501b8`, with Jepsen 0.3.11 and jetcd 0.8.5. The Docker setup is pinned at `4b5707813a0a24966082ae4fd2dd223da447e499`; all three image digests are in [run.sh](run.sh). Clients use plain gRPC and peers use mTLS. Tigerstore built with Go 1.27.2. DB setup and consensus readiness are adapted as in round 2. Register checking uses serial WGL and a 10 GiB heap. The online command has a 300-second bound. Each artifact retains its exact command, binary hash, product SHA and upstream patch.

## Register certificates and failed attempts

Both candidate register commands exited with timeout status 124. Neither wrote a final aggregate `results.edn`. Their full histories, fault observations, logs and available partial checker results remain unchanged in the workload archive. These are checker execution failures, not confirmed consistency violations.

The reused round-2 solver generated explicit legal completions from each full original history. A separate Python verifier checked operation accounting, values and real-time constraints. The Clojure verifier replayed each witness through the exact pinned upstream `VersionedRegister` record. Both verifiers had to pass for every key. Each certificate job compared its stored upstream source with a fresh fetch from the pinned commit, ran 99 fixed/differential solver tests including 80 exhaustive small-history comparisons, and tested tampered-witness rejection by both verifiers.

| Store | Register histories | Calls | Keys | Final certificate verdict | Included ambiguous calls |
| --- | ---: | ---: | ---: | --- | ---: |
| Tigerstore | 2 | 2,423 | 49 | All valid | 7 |
| Etcd | 2 | 2,174 | 44 | All valid | 11 |

No history was shortened, successful call dropped, or unknown response changed to a definite abort. Optional unknown calls have no completion upper bound at their `:info` responses. The independent archive/certificate audit matches original full-history bytes and hashes, key sets, call counts, witness hashes and both verifier outputs. [Certificate verdicts](evidence/register-verdicts.json). The certificates certify register-model histories only; they do not supply the missing aggregate non-workload results from the two timed-out online attempts.

## Fault evidence

Every history has completed nonempty kill/restart, pause/resume and partition/heal operations. Independent one-second observations recorded replacement PIDs, stopped process states and DROP rules with packet hits. The maximum observed DROP counter in each candidate history ranged from 19 to 34 packets. Full observer logs and all fault edges are retained.

This proves that each requested fault class took effect and healed in each history. It does not prove every random target selection succeeded. [Candidate audit](evidence/38468196852-audit.json), [control audit](evidence/38468255858-audit.json).

## Runs and permanent evidence

- [Candidate workloads, 38468196852](https://github.com/djosh34/tigerstore/actions/runs/38468196852), four successful jobs and two failed online register attempts.
- [Etcd workloads, 38468255858](https://github.com/djosh34/tigerstore/actions/runs/38468255858), six successful jobs.
- [Candidate certificates, 38469322232](https://github.com/djosh34/tigerstore/actions/runs/38469322232), both successful.
- [Etcd certificates, 38469359753](https://github.com/djosh34/tigerstore/actions/runs/38469359753), both successful.

[Four permanent compressed archives](evidence/) contain complete histories, original checker output files, fault observations, logs, commands, product hashes and certificate inputs/witnesses/verifier outputs/selftests. Actions metadata and [SHA-256 sums](evidence/SHA256SUMS) are committed alongside them. The workload archives omit plot images and duplicate timestamp/current copies, not history or checker inputs. [Archive audit](evidence/archive-audit.json) matches all 12 full histories byte-for-byte. [Original checker verdict inventory](evidence/original-checker-verdicts.json) retains the hashes and verdicts of all 67 available result files, separately from the two missing aggregate results.

The last job completed at `2026-10-13T21:20:16Z`. All four workflows have finished. Testing stopped before the requested 21:50Z deadline. No subagents were created. No issue #49 summary or bug issue was posted. Existing #105 and #88 are not new findings from this campaign.

This round does not close #49. It does not test independent clock skew, watch completeness, power-loss durability, file faults or the etcd robustness suite. SIGKILL is not power loss. Random schedules differ between repetitions and products.
