# Round 2 fault results

Tested main `5d964228952d6983ba273bc2e6813770aad796c6`, newest at the start. Later main `b5f2e28` is not covered by this evidence. Product code is unchanged. Tests ran on free `ubuntu-24.04-arm` and `ubuntu-24.04` runners against tigerstore and stock etcd v3.7.2. No PR was opened and no summary was posted on #49.

## Exact totals

| Measurement | Count |
| --- | ---: |
| Attempts retained | 56 |
| Attempts that reached the controller | 56 |
| Full-case passes / failures | 32 / 24 |
| Controllers that completed their fault and recovery checks | 51 |
| Recorded raw operation histories | 52 |
| Original/cold history variants | 205 |
| Recorded operations | 66,057 |
| Successful operations / ambiguous failures | 62,914 / 3,143 |
| Checker verdicts, including explicit failed missing-history and diagnostic inputs | 888 |
| Passed / failed verdicts | 744 / 144 |
| Linearizable timeout verdicts | 72 |
| Confirmed product failures / bug issues filed | 0 / 0 |

All four repository checkers ran on every one of the 205 history variants, 820 verdicts. Four missing-history inputs and 13 mistakenly selected diagnostic arrays produced another 68 failed verdicts. These are counted separately, not called operation histories. The other four non-timeout failures belong to the incomplete election-pause history's missing terminal target. No checker returned an illegal-history verdict in the completed matrix or supplementary cases.

There are 209 archived disk-set records, including four setup cases with no usable WAL or operation history. All files passed the independent archive SHA-256/byte-count audit. No required checker is absent. `experiments/correct-round2/final-audit.json`, `final-counts.json` and `final-target-counts.json` contain the audit and machine-readable totals.

| Product | Attempts | Full passes | Operations | Verdicts | Passed verdicts | Timeout verdicts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tigerstore | 30 | 16 | 37,910 | 468 | 376 | 40 |
| etcd v3.7.2 | 26 | 16 | 28,147 | 420 | 368 | 32 |

The first controller started at 16:35:11Z on 2026-10-13. The last workflow finished at 17:24:22Z. Aggregate controller runtime was 44m45s. Testing stopped before the deadline. No further campaign is planned.

## Runs and outcomes

| Run | Tooling commit | Cases | Passed | Failed | Verdicts | Passed verdicts | Timeout verdicts |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| [Permission pilot, 38431592382](https://github.com/djosh34/tigerstore/actions/runs/38431592382) | `6f71eae947b62d11faf54b2cae9bac409337aba9` | 4 | 0 | 4 | 16 | 0 | 0 |
| [Fault pilot, 38431990591](https://github.com/djosh34/tigerstore/actions/runs/38431990591) | `bda8ac37aa26b40e74afc9e473c042b0705f741c` | 4 | 0 | 4 | 104 | 40 | 8 |
| [Full paired matrix, 38433467628](https://github.com/djosh34/tigerstore/actions/runs/38433467628) | `32a2bd45ad0e05a18cb7d973c30dcdc54331e14a` | 40 | 32 | 8 | 640 | 608 | 32 |
| [Supplementary heavy reads, 38437268687](https://github.com/djosh34/tigerstore/actions/runs/38437268687) | `550f754240b4872ec47883e6f6122ade1ab205f7` | 8 | 0 | 8 | 128 | 96 | 32 |

Permanent compact evidence is in `experiments/correct-round2/results/run-<id>`. Full server disks remain in the linked Actions artifacts. Only compact archives were downloaded for the full matrices. The local evidence directory is about 44 MiB.

## What passed

The full matrix ran two repetitions of each schedule on each product and architecture. All 32 normal-load cases passed every checker and cold surviving-pair recovery test. They used three member-pinned logical clients, six keys, two offered operations per client per second and 40 seconds of traffic.

- Partition the old leader's outgoing peer packets, observe a newly elected leader on the surviving pair, then kill that leader while the partition remains installed. Require SIGKILL waitpid evidence, restart and heal.
- Hold a survivor's outgoing peer packets to keep an election open, kill the old leader, observe both survivors reporting leader ID zero, then pause the cut survivor for three seconds. Require stopped task states and unchanged CPU ticks, then resume and heal. This proves a leaderless election interval, not a specific Raft vote instruction boundary.
- Cut one node's outgoing peer TCP packets for five seconds. Reverse packets and client/watch/control ports do not match the rule. Missing TCP ACKs can later stall reverse byte streams, so this is asymmetric packet loss, not a promise of an unaffected reverse TLS stream.
- Flap those outgoing links six times, alternating 0.4-second and 1.2-second cuts with 0.3-second healed intervals. Require actual peer TCP drops on every installation.

All three member-pinned watches requested `prev_kv`. Watch coverage required updates, deletes, previous values and progress through the terminal revision. Every client had to progress after healing plus a ten-second allowance, with two-second request deadlines. The full matrix completed 104 required fault instances with requested, installed, exercised and healed evidence.

## Heavy-load results remain failed

The initial heavy-write pilot released eight unpaced Puts per client after ENOSPC installation, 2,048 burst writes, plus paced traffic. It recorded 9,631 operations, proved 256 clients ready and complete, and measured 256 in flight. Linearizable timed out on all four variants. This failed attempt remains archived.

The full matrix reduced that burst to one Put per client and paced traffic to one operation per client every three seconds, about 85 offered operations per second. After the burst, clients issued only reads until the disk healed. All eight cases proved 256 clients ready and complete, measured 256 in flight, hit the elected leader's own ENOSPC error and recovered each cold pair. Watches, Durable and Revisions passed on all 32 variants. Linearizable timed out after 30 seconds on all 32. Both products and architectures had this failure. These eight cases are FAILED.

The supplementary schedule acknowledged 256 barrier-synchronized Puts before filling the disk. It then parked 256 read clients, verified filesystem ENOSPC, and released 256 Range calls plus one separately recorded 1-MiB trigger Put to the leader. The trigger is client 40000 in the same raw history. All 256 read calls and the trigger started while the filler remained installed. The server itself exited with ENOSPC, the controller held the filler for one more second, then freed space and restarted. All eight controllers passed. Peak total concurrency was 256 or 257, including the extra trigger.

These eight supplementary cases also remain FAILED. Watches, Durable and Revisions passed on all 32 variants, but full-history Linearizable timed out on every variant. Both ARM etcd supplementary histories had zero ambiguous mutations and still timed out. The exact cause of the search cost is not proven. The attempted reduction did not establish full 256-client linearizability or close the earlier under-fault writer gap. No timeout was raised, no writer was omitted, and no ambiguous mutation was rewritten from WAL evidence.

## Failed tools and limits

The first four cases failed before traffic because distinct packet-origin UIDs could not access private runner ancestor/certificate directories. Both products' logs prove the permission error. Runner-only permissions were corrected. These cases have zero operation histories and four failed missing-history checker inputs.

The next election-pause pilot missed its millisecond leaderless window. The controller failed the actual-hit requirement instead of claiming a pause. The later held-election schedule fixed the timing and passed twice on both products and architectures. The failed pilot's history still ran all four checkers.

That pilot's checker driver also mistook converter `.oracle.json` node metadata for histories. Its 13 extra invocations and 52 failed verdicts remain archived. A regression now excludes these metadata files. An initial local archiver also tried to parse coverage dictionaries as verdict arrays; the fix preceded runner-side compact archiving. A local offline self-test initially expected Linearizable at the old report index after checker order changed. It was corrected. None of these is a product failure.

The 72 whole-history timeouts remain failed evidence, not skips. The full and supplementary disk comparisons reproduced them on both products. They do not prove an invalid tigerstore history. No distinct product failure was confirmed, so no bug or sub-issue was created. Existing issues were checked and saved before the campaign.

SIGKILL is not power loss. Cold-pair receipts check loss of one member, but tigerstore's physical WAL converter shares its WAL reader with the product. These runs do not close #49. Evidence applies only to the pinned commit.

Scripts are in `experiments/correct-round2` and reuse `experiments/correct-round1`. The independent workflow is `.github/workflows/correct-round2-faults.yml`, with `max-parallel: 4`. The archive audit was copied without edits from parent branch commit `6442e73cab7568fb07996765aeb86311ff266c78`; its provenance is committed. Local Go checks used `GOFLAGS=-p=2 GOMEMLIMIT=2GiB` and `-parallel 2`, one run at a time. All faults ran on GitHub runners.
