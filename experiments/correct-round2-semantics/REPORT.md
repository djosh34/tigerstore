# Reliability semantics round 2

Product commit `5d964228952d6983ba273bc2e6813770aad796c6`. Product code is unchanged. Both free runner architectures compare tigerstore with stock etcd v3.7.2, with two repetitions per case and seed 4901. The workflow has a four-job concurrency limit.

## Final results

| Run | Tooling | Attempts | Reached controller | Passed | Failed | Operations | Repository verdicts |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [38431093077](https://github.com/djosh34/tigerstore/actions/runs/38431093077) | b8d1dba5 | 24 | 24 | 0 | 24 | 2,689 | 96, all failed |
| [38432805534](https://github.com/djosh34/tigerstore/actions/runs/38432805534) | 0faa2e2b | 24 | 24 | 20 | 4 | 2,224 | 336, all passed |
| [38434252340](https://github.com/djosh34/tigerstore/actions/runs/38434252340) | 57f8adfb | 8 | 8 | 8 | 0 | 896 | 128, all passed |
| Total | | 56 | 56 | 28 | 28 | 5,809 | 560: 464 passed, 96 failed |

Every recorded client history ran all four repository checkers against each available WAL set: 140 enriched histories, including original disks and 84 cold surviving-pair sets. Operations include 40 explicit synthetic lease-expiry operations, so there were 5,769 recorded request operations. Keepalive and TTL diagnostics are separate. Three initial Linearizable verdicts timed out at 30 seconds. No later checker timed out. Setup/build failures were zero.

The final matched matrix has 24 passing cases: lease, compaction and 192 concurrent prev_kv watchers, both products, both runner architectures, two repetitions each. All passing cases verified peer blackhole, paused-leader replacement and killed-leader restart, plus three independent cold surviving-pair checks. Earlier failed attempts remain failed.

Testing stopped after the final workflow completed at `2026-10-13T16:58:05Z`. No further campaigns were started.

The initial run used RequestProgress without the `hasleader` metadata of the watch context. clientv3 therefore sent it to a different watch stream. All 24 cases failed their required progress-through-target assertion. They remain failed evidence. No product defect follows from these failures. All recorded histories still invoked Linearizable, Watches, Durable and Revisions against the preserved original WAL set. Cold-pair checks could not start after the controller failure. Three lease Linearizable checks timed out at 30 seconds and remain FAILED.

The corrected run `38432805534` passed all eight lease cases and all eight 192-watcher cases, plus all four etcd compaction cases. It fixed the progress context rather than relaxing completeness. The lease workload uses fewer unrelated mutations and more reads of attached keys to bound Porcupine's expiry search. It still requires the original coverage floor and invokes all four checkers. It adds an ambiguous grant while all peers are blackholed, a majority grant and two-key expiry while the old leader is paused, and an isolated-endpoint grant with its unknown outcome preserved. The main lease uses TTL 12 and fourteen healthy renewals before its final expiry. These changes do not reclassify earlier failures.

Four tigerstore compaction cases remain FAILED. Their four repository checkers passed, but a direct assertion incorrectly required every during-fault old-revision watch to cancel. The paused member could open its watch before applying the compaction and legally replay its retained history to the terminal target. The two healthy members canceled their old watches. Stock etcd canceled all three in these particular runs. This is an arrival/apply-order difference, not a confirmed product defect.

Tooling `57f8adfb` strengthens that test. It keeps the during-fault watches and their complete replay checks. After healing, it requires a linearizable apply barrier and a rejected historical Range on every member before three additional C-1 watches, each required to cancel exactly once at C. All eight repeated compaction comparisons passed on both architectures, with 128 passing repository verdicts. This does not erase the earlier four failures.

Setup/build failures: zero. Every attempted case reached the controller and recorded a history. No failed conversion or missing-manifest fallback was needed in these runs. The unit test `TestFourVerdictsWithMissingLog` verifies that a missing log still produces all four failed repository verdicts.

## Evidence and limits

`results/<run>/` retains every artifact attempt, client and enriched histories, normalization decisions, receipt reads, fault installation/hit/heal records, server logs and checker verdicts. Each artifact directory has a `manifest.json` with original file sizes and SHA-256 hashes. Full server disks remain in the Actions artifacts. The complete initial archives were collected one ZIP at a time without extracting WAL files; temporary files were deleted after each archive. An earlier interrupted bulk fetch extracted raw disks, which were removed after their evidence was archived. New jobs publish separate compact artifacts.

Setup/build failures count as attempts, not controller cases. No recorded history means no checker claim. Failed conversion still invokes all four checkers against raw operations and watches, and reports the missing oracle as FAILED.

Grant and expiry have repository-format operations. Keepalive and TTL calls are raw diagnostics because the format has no keepalive operation. The independent decoder uses committed stamps, never watch output, to decide whether a tigerstore expiry takes effect. Watch replay checks each key's values, previous values, metadata and delete revision against that normalized log. Timing assertions separately reject early expiry. Leader changes may extend a lease, and etcd may legally let a lease expire when no renewal succeeds for its TTL. A post-heal TTL zero alone is not a product bug.

SIGKILL does not simulate power loss. The tigerstore physical WAL reader uses the product WAL parser on private byte copies. Repository models are separate from the product state machine. This campaign does not satisfy all of #49.

All 2,408 manifest entries passed a local streaming size/hash audit. Counts are in `results/integrity-audit.json`; the parent performs the independent coverage audit. The permanent evidence commit is `ce6af614a1f0b02243bc7b74669fbc3e8ec1eb92` on branch `exp/correct-round2-semantics`. The final tested campaign commit is `57f8adfbaae3404fe730b0c7ed0aff7252d6e66a`. The report-only final branch commit is sent to the parent with the results links.

Confirmed product failures and bug issues: zero. Existing watchdog issue [#88](https://github.com/djosh34/tigerstore/issues/88) remains open. No summary comment was posted on #49.
