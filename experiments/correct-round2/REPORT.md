# Correctness testing round 2

Tested tigerstore `5d964228952d6983ba273bc2e6813770aad796c6`, the newest main when this round started, against stock etcd v3.7.2. Each case used three real processes with client and peer TLS on `ubuntu-24.04-arm` or `ubuntu-24.04`. No product code changed. All faults ran on GitHub-hosted runners.

The scripts reuse round 1's recorder, WAL adapters, controller and cold-recovery checks. The frontend, batching and wire-form changes named in the request were already present in round 1's `9087087`. The intervening `5d964228` adds stress testing and diagnostic logging.

No product failure was confirmed. No bug issue was filed. This round is not a clean pass. Whole-history linearizability checking did not finish for the 256-client ENOSPC workloads on either product. Those runs remain failed.

## Results

Ran 152 cases. Tigerstore passed 44 of 78; stock etcd passed 46 of 74. The 148 recorded client histories contain 78,365 operations, including 40 synthetic lease expiries. All four repository checkers ran on each of 475 genuine history variants, producing 1,900 verdicts: 1,704 passed and 196 failed. There were 75 timeout verdicts across 21 cases. Another 68 failed checker attempts used missing-history or mistakenly selected metadata inputs; they are not operation histories.

| Part | Cases | Passed | Failed | History variants | Checker verdicts | Timed-out verdicts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Combined faults and 256 clients | 56 | 32 | 24 | 205 | 888 | 72 |
| Leases, compaction and many watches | 56 | 28 | 28 | 140 | 560 | 3 |
| Clocks, CPU quotas and large values | 40 | 30 | 10 | 130 | 520 | 0 |
| Total | 152 | 90 | 62 | 475 | 1,968 | 75 |

The verdict column includes the 68 failed non-history inputs. `summary.json` separates those from the 1,900 genuine history verdicts.

A case passes only when its controller verifies every required fault, all three cold-surviving pairs recover the terminal receipt, and every required repository checker passes. The checkers are Linearizable, Watches, Durable and Revisions. A checker timeout, a missing fault hit or an incomplete check fails the case.

Most completed cases have four disk-set histories and 16 verdicts. Failed cases can have only the original disk set or a fallback raw-history check. Four early setup cases failed because the per-node packet-origin UIDs could not access private runner directories. They recorded no operations. They produced failed missing-input checks, not successful history validation. An early checker driver also sent 13 diagnostic metadata files to the checkers by mistake. Those 52 failed verdicts remain in the record and are separate from real histories.

## Coverage and findings

### Combined faults and one-way links

The final ordinary-fault matrix passed twice on each architecture and product. It covers an outgoing peer partition of the old leader followed by killing the newly elected leader, a verified process pause during a held leaderless election, one-way peer cuts, and six link flaps. Packet rules select per-node process UIDs and peer TCP port 2380, so they cover connections initiated by either peer without selecting client traffic. Actual peer packet-drop counters prove the cuts took effect.

The election test first missed its short leaderless interval. That attempt failed. The corrected schedule holds a survivor's outgoing peer cut while the controller observes leaderlessness and pauses that survivor. It records the extra cut rather than claiming to hit a particular Raft instruction.

### Leader ENOSPC and 256 clients

Each final heavy-write case starts 256 clients across all three members. A barrier releases their Put calls after the leader's ext4 filesystem is full. The controller requires actual server ENOSPC, frees the space and restarts the member. All eight matched cases verified the fault and recovery, with 256 clients ready and complete and a measured peak of 256 calls in flight.

All eight remain failed because Linearizable timed out at 30 seconds on each of their four history variants. Watches, Durable and Revisions passed. Stock etcd showed the same checker limit. Earlier pilot timeouts remain failed too. No ambiguous mutation was omitted, marked aborted from its timeout, or rewritten using the WAL.

A separate supplementary schedule acknowledges a 256-Put burst before filling the disk, starts 256 reads while the filler remains, and records one 1-MiB trigger Put. This is concurrent reads under ENOSPC, not heavy writes throughout the fault. All eight supplementary controllers passed. All eight cases still failed because Linearizable timed out on all four variants per case. The other three checkers passed. Both ARM etcd supplementary histories had zero ambiguous mutations and still timed out. The cause of the search cost is not proven. This reduction did not replace the heavy-write failures or close their validation gap.

### Leases and compaction

The final matched semantic matrix has 24 passing cases across both products and architectures, with two repetitions each. Lease cases grant during a full peer blackhole, grant through the healthy majority while the old leader is paused, retain the isolated endpoint's ambiguous grant, renew the main lease and wait for expiry. Checks require the attached keys to disappear together at one revision, with exactly one delete per key on each watch, the expected previous values and no early expiry. Keepalive and TTL responses are retained separately because the repository history format has no keepalive operation. Explicit ambiguous revoke operations model possible expiry.

Compaction commits during a leader pause. Old watches either receive a compaction cancellation or legally replay retained history before their member applies the compaction. C+1 watches resume through the terminal target. After healing, each member must pass a linearizable apply barrier and reject a historical Range before three additional C-1 watches must cancel exactly once at C.

The initial progress requests omitted the watches' `hasleader` metadata and targeted a different clientv3 stream. All 24 initial cases failed; three lease linearizability checks also timed out. Correcting the stream metadata did not relax completeness. Four later tigerstore compaction cases failed an incorrect requirement that all during-fault watches cancel. The paused member could still replay retained history. All four repository checkers passed on those cases, but the cases remain failed. The final eight barrier-based compaction comparisons passed.

### Many watches

Eight cases with 192 concurrent watchers requesting `prev_kv` passed across both architectures and products, with two repetitions each. They endured the full peer blackhole, leader pause and leader kill. Each watch had to reach an independently read terminal revision. Values, order, duplicates and previous values were checked against the committed history.

### CPU quotas, clocks and large values

All 24 final near-limit comparisons passed. Eight cases constrain a leader to 5 percent or a follower to 10 percent of one CPU for eight seconds. Recorded `cpu.max`, process membership, CPU use and increasing throttle counters prove the actual node was throttled.

Clock cases move one leader's or follower's CLOCK_REALTIME forward by 3,600 seconds and backward by 3,600 seconds. Server syscall traces and Go canaries prove the selected node changed while other nodes and monotonic time did not. Zero-offset traced controls also ran. The wrapper clears the child's vDSO pointer and intercepts its clock syscalls; it does not change executable bytes or the host clock.

A value of exactly 1.5 MiB exceeds etcd's default request cap once the key and framing are included. The campaign recorded 48 definite rejections per product for that exact size. Accepted Put payloads were 1,572,820 bytes on tigerstore and 1,572,810 to 1,572,811 bytes on etcd with the test client's TLS identity. The scripts measure the boundary on each member and verify that one more byte is rejected. Near-limit Put and Txn traffic then runs through signals, quotas or clock jumps, preserving full value bytes in the histories.

Ten clock-campaign attempts remain failed. Six lost the controller subclass when re-executing inside the namespace, so the requested fault did not run. Four etcd boundary setups miscounted the authenticated request header. Corrected reruns do not erase these attempts.

## Evidence

The separate reports contain every run link, test-tool commit, script and seed. Permanent gzip archives retain original and enriched histories, fault phases, server logs, receipts, normalization decisions and checker results. Manifests record original byte lengths and SHA-256 hashes. Full server disks remain in Actions artifacts.

The parent independently audited every archived manifest and checked fault phases, case results, required checker names and offline exit codes. `audit-evidence.py` streams decompression, so large values do not require loading all histories into memory. Its unit tests reject missing hits, missing reports, empty checker results and timeouts. The audit confirms the reported failures; it does not convert them to passes.

- [Combined faults report](https://github.com/djosh34/tigerstore/blob/374b44c6d0a4e0fe732275285aa7e85118e4e7d7/REPORT.md) and [scripts and evidence](https://github.com/djosh34/tigerstore/tree/374b44c6d0a4e0fe732275285aa7e85118e4e7d7/experiments/correct-round2).
- [Lease, compaction and watch report, scripts and evidence](https://github.com/djosh34/tigerstore/tree/32a438e53be46195e766b9c5f19d5d0730c75714/experiments/correct-round2-semantics).
- [Clock, quota and large-value report, scripts and evidence](https://github.com/djosh34/tigerstore/tree/6651b1ae5928972bcd7e66234b61b70faa7f9096/experiments/correct-round2-clocks).

`summary.json` pins the source commits and all ten workflow runs. All three helpers were deleted after their evidence was pushed and independently audited. Their worktrees were removed. The last fault workflow completed at 2026-10-13T17:24:22Z. No further campaign is running.

## Limits

SIGKILL tests process-crash recovery, not power loss. The kernel page cache survives. Tigerstore's physical WAL reader is shared with the product, though the command adapters and checker model are separate. Clock jumps affect realtime, not monotonic time. The 256-client whole-history linearizability gap remains open. Main advanced during testing to `b5f2e28ef2edd94393d62952b1568763ebc12fe2`, with a simulator-test bound change and Maintenance.Alarm support. These runs do not certify that later commit. This round does not close issue #49 or certify requirements it did not test.
