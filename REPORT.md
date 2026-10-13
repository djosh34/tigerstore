# Round 2 faults, interim report

Tested product main `5d964228952d6983ba273bc2e6813770aad796c6`. Product code is unchanged. The independent workflow uses both free runner architectures and stock etcd v3.7.2. No PR and no summary comment on #49.

## Attempts retained so far

- [Permission pilot, 38431592382](https://github.com/djosh34/tigerstore/actions/runs/38431592382), tooling `6f71eae947b62d11faf54b2cae9bac409337aba9`: four failed controller attempts, zero recorded operation histories, 16 failed checker verdicts from four unreadable/missing-history inputs. Distinct node UIDs could not traverse private runner directories. Both products failed before traffic. This is a proven setup defect. All four attempts are archived under `experiments/correct-round2/results/run-38431592382`.
- [Fault pilot, 38431990591](https://github.com/djosh34/tigerstore/actions/runs/38431990591), tooling `bda8ac37aa26b40e74afc9e473c042b0705f741c`: four failed cases with four recorded operation histories. Three controllers completed their faults and cold recovery; election-pause failed its actual-hit requirement. The election's leaderless interval was too short to observe, so no pause was claimed. Whole-history Linearizable timed out at 30 seconds for the 623-op, 16-client partition/kill history and the 9,631-op, 256-client ENOSPC history. Each timed out on all four original/cold variants. The other three checkers passed on all variants of those two histories. The etcd one-way history passed all four checkers on all variants. The failed election history ran all four checkers on its one saved original variant and failed its missing terminal target.

The second pilot also checked converter node-metadata arrays as if they were operation histories. Those 13 extra invocations generated 52 failed verdicts and remain in the archive. This is a proven tool defect, now covered by an offline regression. The second pilot has 13 genuine history variants and 104 total verdicts, 40 passed and 64 failed. Eight verdicts are Linearizable timeouts. Diagnostic arrays are counted as extra checker inputs, not as operation histories.

The disk pilot proved 256 clients ready and complete at the marker barrier and measured 256 simultaneous unary calls. It retained all 2,048 burst Puts. Its failed full-history checks do not establish 256-client correctness.

All eight attempts reached the controller. None passed the full job. Across the pilots, four raw operation histories and 13 genuine variants were recorded. All four repository checkers ran on every variant, including the failed election run. Total verdicts including missing-history and diagnostic inputs are 120, with 40 passed and 80 failed. No product correctness failure is confirmed and no bug issue is filed.

## Next batch

The full 40-case matrix keeps all five schedules, two architectures, two products and two repetitions. Normal schedules now use three clients to bound ambiguous-operation search. Disk schedules keep 256 clients and a synchronized single-Put burst, then read only until the actual server ENOSPC fault heals. Paced traffic uses a three-second client interval, about 85 offered operations/second. Every writer and ambiguous outcome stays in the unmodified whole history. Checker timeouts remain failures.

The election schedule now cuts a survivor's outgoing peer packets before killing the old leader. This keeps the election open long enough to prove the leaderless interval before SIGSTOP. Every extra cut requires installed, exercised and healed evidence. The new batch also requires progress from every client and no failed unary calls after healing plus a ten-second grace period, under two-second request deadlines.

Compact archives are generated on the runners. Only those archives are downloaded to this host. Full server disks remain in Actions artifacts. Each permanent file has the base-format path/SHA-256/byte-count manifest. Runner step outcomes remain in `attempt.json`, including build/setup failures. Local offline tests passed; an initial self-test expected Linearizable at the old report index and failed after checker order changed. The test now checks the new index. An initial local archiver parsed coverage dictionaries as verdict arrays; that tool error was fixed before the second runner pilot. Neither was a product execution.
