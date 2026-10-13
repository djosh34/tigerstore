# Correctness testing round 3

This is a reliability-only experiment for djosh34/tigerstore issue #49. Product code is unchanged. Builds, workloads, faults and expensive checkers run only on free GitHub-hosted runners.

The initial main pin is `499a4c6759d0c1276aa4257af462295216f8ab21`, inspected at 2026-10-13T21:05Z. It includes metrics, RangeStream and the client-certificate request-size fix.

The requested campaigns are:

- Jepsen register, set and append with `kill,partition,pause`, 50 operations per key, twice each. Reuse the Jepsen round 2 certificate checks for register histories that the online checker cannot resolve.
- Round 2 combined-fault cases with concurrent RangeStream LISTs and watches. Require fault evidence and explicit correctness verdicts.

The scripts live on `exp/correct-round3-jepsen` and `exp/correct-round3-combined`. The parent report and independent evidence audit live on `exp/correct-round3`.

Every failed or incomplete attempt remains part of the report. Missing fault hits, checker timeouts and absent verdicts do not pass. Confirmed product failures become bug sub-issues of #49. Existing issues are checked before filing duplicates.

Testing stops before the summary comment at 21:55Z on 2026-10-13. The absolute deadline is 22:00Z. These experiments do not close #49 or prove power-loss durability.
