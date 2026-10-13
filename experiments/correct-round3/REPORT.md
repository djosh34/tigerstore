# Correctness testing round 3

Tested main `499a4c6759d0c1276aa4257af462295216f8ab21`, newest when the campaigns launched. It includes metrics, RangeStream and the client-certificate request-size fix. Product code is unchanged. All builds, workloads, faults and expensive checks ran on free GitHub-hosted runners. Local work was source inspection and evidence accounting.

No product failure was confirmed. No bug issue was filed. This is not a clean pass because 16 combined-fault cases still have unresolved whole-history linearizability checks.

## Jepsen

Register, set and append each ran twice with `kill,partition,pause`, concurrency `2n`, rate 30, 90 seconds, five-second nemesis intervals and `--ops-per-key 50`. Both tigerstore and stock etcd v3.7.2 ran the six commands. The unchanged upstream set and append generators ignore `ops-per-key`. Set uses one set and append uses three keys. Only the register generator actually bounds operations per key at 50.

| Store | Histories | Calls | Final workload verdicts |
| --- | ---: | ---: | --- |
| Tigerstore | 6 | 6,844 | All valid |
| Etcd v3.7.2 | 6 | 6,894 | All valid |

Every history has observed kills and replacement PIDs, stopped processes and resumes, partition packet hits and healing. The four tigerstore set/append histories passed the upstream online checker. Both tigerstore register jobs reached the online time limit without a final result. Those original failed attempts remain retained.

The round 2 certificate method supplied definite register verdicts. Both verifiers checked each complete original history, without relabelling uncertain operations as aborted. Across both products, certificates cover 93 per-history keys and 4,597 calls. The parent's independent archive audit checked full-history hashes, exact key sets, operation accounting, both verifier exit codes and the tested product pin. It did not rerun the solver locally.

- [Tigerstore live run](https://github.com/djosh34/tigerstore/actions/runs/38468196852)
- [Etcd live run](https://github.com/djosh34/tigerstore/actions/runs/38468255858)
- [Tigerstore certificates](https://github.com/djosh34/tigerstore/actions/runs/38469322232)
- [Etcd certificates](https://github.com/djosh34/tigerstore/actions/runs/38469359753)
- [Pinned scripts, report and permanent evidence](https://github.com/djosh34/tigerstore/tree/058594029a280b46fd3c09117f53d7eef1ad1fa9/experiments/jepsen3)

## Combined faults, RangeStream and watches

Reused round 2's four ordinary combined-fault scenarios: isolate the old leader then kill the new leader, pause during a verified leaderless election, one-way peer cuts, and six link flaps. Every matrix ran twice on both ARM64 and AMD64, against both products. Each case used three real processes and 45 seconds of traffic.

The LIST dataset has 32 mutable keys, enough for multiple RangeStream chunks. Each installed fault includes a successful serializable LIST and fresh watch replay before healing. Checks require the first ten-key chunk, final-only header/count/more, ordered unique keys, full KV metadata equal to historical unary Range, and watch replay with previous values through the independently captured LIST revision. Linearizable LISTs and three member-pinned watches also run concurrently.

| Matrix | Cases | Passed | Failed | Recorded operations | Linearizability timeouts |
| --- | ---: | ---: | ---: | ---: | ---: |
| Initial snapshot/watch checks | 32 | 22 | 10 | 27,945 | 40 |
| LIST freshness in whole-history checker | 32 | 27 | 5 | 67,061 | 20 |
| Lower load, same freshness checks | 32 | 31 | 1 | 22,530 | 4 |
| Total | 96 | 80 | 16 | 117,536 | 64 |

Tigerstore passed 43 of 48 cases and etcd passed 37 of 48. Every failure is a 30-second Linearizable timeout on each of the case's four disk-set histories. Watches, Durable and Revisions passed every verdict. All controllers verified their requested faults, in-fault probes and cold recovery. The final matrix passed all 16 tigerstore cases and 15 of 16 etcd cases. Its remaining failure is an etcd ARM one-way case with 720 operations.

The initial matrix compared streams to their declared historical snapshots but did not put LIST calls into the linearizability history. It cannot establish latest-read freshness. The next two matrices record each successful linearizable LIST at its invocation/EOF bounds as a Range operation in the original history. A negative regression requires rejection of a stale LIST. Those two matrices include 54,672 recorded LIST calls. The final matrix reduces ordinary traffic from five to one operation per second per client, LIST mutations from five to one per second, and LIST reads from ten to four per second per member. Passing reduced-load cases do not validate the earlier timed-out histories.

All four repository checkers ran on 384 original/cold-recovery variants, producing 1,536 verdicts. Of these, 1,472 passed and 64 timed out. The parent independently checked every archived manifest's hashes and lengths, all fault phases, checker presence and verdicts. A separate audit checked all 288 required in-fault probes, their chunk payloads and their complete containment between fault installation and healing.

- [Initial matrix](https://github.com/djosh34/tigerstore/actions/runs/38468658463)
- [Freshness matrix](https://github.com/djosh34/tigerstore/actions/runs/38468977502)
- [Lower-load matrix](https://github.com/djosh34/tigerstore/actions/runs/38469549517)
- [Pinned scripts, report and permanent evidence](https://github.com/djosh34/tigerstore/tree/93c4b85b2bd14e495aebf154356641093f76c45a/experiments/correct-round3)

## Limits and audit files

The failed online Jepsen attempts and every combined-fault timeout remain failures in their original jobs. None was discarded. No confirmed product defect emerged, so there is no new bug sub-issue. Existing bugs remain outside this result.

This round did not repeat round 2's 256-client ENOSPC cases, leases, compaction or clock tests. It does not certify power-loss durability. SIGKILL preserves the kernel page cache, and the physical tigerstore WAL reader is shared with the product. The in-fault serializable probes do not promise quorum reads while the cluster is leaderless. This round does not close #49.

The last campaign job finished at 2026-10-13T21:26:27Z. All three experiment workflows are disabled, and both helpers were deleted. No testing remains active.

Main advanced at 21:29:42Z to `769523480dad5d8e55f02e779a671b9b345955f4`. Its only change from the tested pin is `cmd/loadgen/conn_test.go`, a fake-server test setup fix. The tested server source is unchanged by that commit, but the executed product SHA remains `499a4c6`.

The parent audit outputs are `audit-jepsen.json`, `audit-combined.json` and `audit-probes.json`. Their scripts are alongside this report. Permanent compressed histories and checker evidence are on the two linked experiment branches. Full server disks remain in Actions artifacts.
