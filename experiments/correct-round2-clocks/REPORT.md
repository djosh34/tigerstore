# Round 2 clock, CPU and large-value results

Status at 2026-10-13T16:55Z: near-limit fault comparisons are still running. These are reliability tests for #49, not product changes or a claim that #49 is complete.

Product is pinned to `5d964228952d6983ba273bc2e6813770aad796c6`. Both free runner architectures compare unchanged tigerstore with checksum-pinned stock etcd v3.7.2. No PR, product edit, local fault run or host-clock change was made.

## Completed validation runs

| Run | Attempts reaching controller | Passed runs | Failed runs | Operations | WAL history sets | Checker verdicts | Passed verdicts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| [38430785949](https://github.com/djosh34/tigerstore/actions/runs/38430785949) | 8 | 0 | 8 | 171 | 8 | 32 | 16 |
| [38431403716](https://github.com/djosh34/tigerstore/actions/runs/38431403716) | 8 | 6 | 2 | 2,691 | 26 | 104 | 96 |
| Total | 16 | 6 | 10 | 2,862 | 34 | 136 | 112 |

Every recorded history invoked all four repository checkers: `Linearizable`, `Watches`, `Durable` and `Revisions`. No checker timed out and no synthetic missing verdict was needed. Runs with missing fault hits remain failed even when their available history passes.

The first run had two setup defects. Namespace re-exec lost the clock extension and six cases failed with a missing `clock_fault` method. The two etcd near-limit cases failed during admission setup. Stock etcd adds the TLS certificate's username to its internal request header, so the test's first strict-ID-only size calculation was too large. The corrected extension run retained two further failures from that admission assumption. A per-member boundary search now measures both the admitted and rejected sizes, including negative header allowances. These ten failed attempts remain in the permanent archives. They are test-tool failures, not confirmed product defects.

The corrected run verified +3600-second and -3600-second realtime jumps on one selected leader for both products on both architectures. Actual server syscall traces show the modified results. Three independent Go canaries under identical wrappers confirm the wall-clock jump on the selected node, unchanged monotonic elapsed time, and unchanged clocks on the other two nodes. Tigerstore's zero-offset traced near-limit control also passed on both architectures, including all three cold surviving pairs and all four checkers.

## Remaining campaign

[38432244287](https://github.com/djosh34/tigerstore/actions/runs/38432244287) runs 24 near-limit cases. Each product and architecture gets a 5% CPU leader quota, a 10% CPU follower quota, leader and follower clock jumps, a zero-offset traced control, and leader/follower SIGKILL and SIGSTOP schedules. A matching repetition will follow after this run completes.

Near-limit traffic is bounded at three sequential clients, one operation per second each, six hot keys and 40 seconds. Each member receives an exact 1.5 MiB value and must reject its framed request. Each member's largest admitted boundary supplies the Put and CAS values under faults. Full values and previous values remain in the histories. [CONTRACT.md](CONTRACT.md) describes the mechanisms and failed-run rules.

## Evidence and limits

Permanent histories, fault evidence, server clock traces, Go canaries, server logs, all checker verdicts, runner facts and complete job logs are in [results](results). Each artifact has a path, byte count and SHA-256 manifest. A streaming local audit verified all 674 files in the two completed runs against their manifests, covering 10,387,581,723 original bytes. Current runner jobs create compact artifacts. Legacy ZIP members stream directly into gzip without extracting disks. Full server disks remain in Actions artifacts.

The clock wrapper changes syscall scheduling. Zero-offset traced controls are required; a traced failure alone would not establish a clock-induced defect. CPU quota proof requires actual cgroup membership and increasing throttle counters, not command success. SIGKILL does not simulate power loss. The physical tigerstore WAL reader is shared with the product. A seed fixes workload and fault choices, not kernel scheduling.

Confirmed product failures so far: zero. New bug issues: zero. Existing #88 remains open; #90 added stress tests and logs, not a semantic repair. These bounded runs do not establish that #88 is fixed.
