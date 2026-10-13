# Round 2 clock, CPU and large-value results

Finished on 2026-10-13. Reliability testing only for #49. Product is pinned to `5d964228952d6983ba273bc2e6813770aad796c6`. Later main, including `b5f2e28`, is not covered. Both free runner architectures compared unchanged tigerstore with checksum-pinned stock etcd v3.7.2. No PR, product edit, local fault run, host-clock change or summary comment on #49 was made.

## Counts

| Run | Attempts/controllers | Passed runs | Failed runs | Operations | WAL history sets | Checker verdicts | Passed verdicts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| [38430785949](https://github.com/djosh34/tigerstore/actions/runs/38430785949) | 8/8 | 0 | 8 | 171 | 8 | 32 | 16 |
| [38431403716](https://github.com/djosh34/tigerstore/actions/runs/38431403716) | 8/8 | 6 | 2 | 2,691 | 26 | 104 | 96 |
| [38432244287](https://github.com/djosh34/tigerstore/actions/runs/38432244287) | 24/24 | 24 | 0 | 3,637 | 96 | 384 | 384 |
| Total | 40/40 | 30 | 10 | 6,499 | 130 | 520 | 496 |

Tigerstore had 20 attempts, 16 passed, 3,382 operations, 68 history sets and 272 verdicts, 264 passed. Stock etcd had 20 attempts, 14 passed, 3,117 operations, 62 history sets and 248 verdicts, 232 passed. There were zero build-only failures.

Every recorded history invoked all four repository checkers: `Linearizable`, `Watches`, `Durable` and `Revisions`. There were 24 failed verdicts, zero repository checker timeouts and zero synthetic missing verdicts. The slowest linearizability check took 27.449 ms. Missing fault hits remain failed runs even when their available histories pass.

All 24 final cases passed on `ubuntu-24.04-arm` and `ubuntu-24.04`. Each product and architecture got a 5% leader CPU quota, a 10% follower CPU quota, leader and follower clock jumps, a zero-offset traced control, and leader/follower SIGKILL and SIGSTOP schedules. Every passing case independently cold-started each surviving pair and checked its full terminal values and revisions. The owner stopped further passing-case repetitions after this campaign.

## Exact request sizes

The exact value size was 1,572,864 bytes. All 96 exact-size Put requests received definite `InvalidArgument: etcdserver: request is too large` responses, 48 per product. Framing makes that value exceed the default Raft request cap.

The final campaign measured 72 member-pinned admission boundaries, 36 per product. Each largest admitted Put succeeded and the next value byte was definitely rejected. With `/correct-round1/k0` and requested previous values, the admitted byte counts were:

| Product | Largest admitted Put value | Successful CAS mutation value sizes observed |
| --- | ---: | --- |
| Tigerstore | 1,572,820 | 1,572,762 and 1,572,764 |
| Stock etcd v3.7.2 | 1,572,810 or 1,572,811, by member | 1,572,752; 1,572,753; 1,572,754; 1,572,755 |

CAS values include the whole transaction's framing allowance. The final workload committed 262 large Puts and 327 large CAS mutations on tigerstore, and 258 large Puts and 319 large CAS mutations on etcd. Full values, previous values and watch events remain in the histories and checker inputs. Metadata in [facts.json](facts.json) supplements those histories; it does not replace them with hashes.

Traffic was bounded at three sequential clients, one operation per second each, six hot keys and 40 seconds. It mixed Put, both CAS branches, range delete and Range, with three member-pinned watches requesting previous values. This is not a saturation test.

## Actual CPU throttling

Only the selected native server entered the quota cgroup. The controller read actual PID membership, `cpu.max`, `cpu.stat` and process CPU counters. All eight quota cases increased both throttle counters while the server consumed CPU. Quotas aggregate all server threads and mean a percentage of one CPU, not of the runner. Each hold lasted approximately eight seconds.

| Runner | Product | `cpu.max` | Added throttle periods | Added `throttled_usec` | Server CPU seconds |
| --- | --- | --- | ---: | ---: | ---: |
| ARM64 | etcd | `5000 100000` | 49 | 8,388,239 | 0.279723 |
| ARM64 | etcd | `10000 100000` | 13 | 779,743 | 0.212255 |
| ARM64 | tigerstore | `5000 100000` | 22 | 2,241,059 | 0.136676 |
| ARM64 | tigerstore | `10000 100000` | 2 | 23,853 | 0.092343 |
| AMD64 | etcd | `5000 100000` | 64 | 11,243,522 | 0.323823 |
| AMD64 | etcd | `10000 100000` | 28 | 3,036,899 | 0.346834 |
| AMD64 | tigerstore | `5000 100000` | 34 | 5,565,965 | 0.195620 |
| AMD64 | tigerstore | `10000 100000` | 10 | 1,162,899 | 0.161923 |

Cgroup throttle time can aggregate across CPUs; it is not a wall-time percentage. Healing restored the original cgroup membership and required cluster readiness within 18 seconds. CPU and clock cases also required four seconds of post-heal traffic. Cold pairs had a 25-second readiness bound.

## Verified clock scope

Twelve cases verified 24 signed realtime jumps, +3600 and -3600 seconds, on one selected member only. Four were small-value mechanism comparisons; eight used near-limit values with leader and follower targets. Six zero-offset traced controls passed, including both products and architectures in the final campaign.

The external native ptrace wrapper clears the child's auxiliary-vector vDSO pointer at exec and offsets successful `clock_gettime(CLOCK_REALTIME)` results. Server executable bytes are unchanged. Actual server traces recorded 166 to 3,404 affected clock results per jump. Independent Go canaries under identical wrappers showed the selected wall-clock discontinuity, unchanged monotonic elapsed time and unchanged clocks on the other two members. Monotonic jumps and clock-rate drift were not tested. The host clock was never changed.

Tracing changes syscall scheduling. Zero-offset controls are required before attributing a traced failure to a clock fault. [CONTRACT.md](CONTRACT.md) describes the mechanism and failure rules.

## Retained failures

The first run had two setup defects. Namespace re-exec lost the clock extension and six cases failed with a missing `clock_fault` method. Two etcd near-limit cases failed during admission setup. Stock etcd's TLS-derived request-header metadata made the test's strict-ID-only size calculation too large. The corrected extension run retained two further failures from that admission assumption. The final per-member binary search measured the admitted and rejected boundaries instead of assuming the complete header size.

All ten failed cases, their available histories, all checker failures and complete job logs remain archived. They are test-tool failures, not confirmed product defects. Atomic offset-file replacement also removed a pre-run review concern about readers seeing a truncated file.

## Permanent evidence and limits

[results](results) contains all 40 attempts, histories, fault evidence, actual server clock traces, Go canaries, server logs, checker verdicts, runner facts and complete job logs. The three script commits tested were `19861fd6`, `2fe9e28d` and `84dcb18f`; seeds were 149 and 150. Each artifact has a path, original byte count and SHA-256 manifest.

The parent's independent streaming audit verified all 2,098 manifest entries, 155,583,566,377 original bytes, and confirmed the counts above. A separate local hashing attempt hit its 360-second tool timeout after verifying 35 manifests; that failed evidence-processing attempt is preserved in [integrity-first-attempt.json](integrity-first-attempt.json). It was not a repository checker or fault-case timeout. No hash rerun was started after the parent completed its full audit.

Archives occupy about 170 MiB without extracting server disks. Current jobs create compact artifacts; legacy ZIP members stream directly into gzip. Full server disks remain in Actions artifacts. SIGKILL does not simulate power loss, and the physical tigerstore WAL reader is shared with the product. A seed fixes workload and fault choices, not kernel scheduling.

Confirmed product failures: zero. New bug issues: zero. Existing #88 remains open and did not reproduce here. #90 added stress tests and logs, not a semantic repair. These bounded tests do not establish that #88 is fixed or that #49 is complete. No further campaign is running.
