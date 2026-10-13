# Leaderless differential results

Tigerstore rejects leaderless mutations sooner than etcd. This is an availability tradeoff versus etcd, not a demonstrated product defect. Etcd can wait through a brief outage and complete the original request. Tigerstore returns an error and requires a later client request. No inspected contract requires identical admission timing during quorum loss.

All 12 targeted cases passed in [Actions run 38440882457](https://github.com/djosh34/tigerstore/actions/runs/38440882457). Three independent free runners each ran stock etcd, then tigerstore. Each store ran both a leader-survivor and a follower-survivor case with fresh three-node storage. Every case stopped two processes, observed their Linux state `T`, observed `Status.Leader==0` on the survivor, tested unary calls and watches, resumed the processes, and checked recovery. Each observed difference therefore repeated twice beyond the first observation.

No product code changed. No host build or reliability test ran. No issue or comment was posted.

## Exact versions

| Component | Commit or hash |
| --- | --- |
| Tigerstore main inspected and tested | `b5f2e28ef2edd94393d62952b1568763ebc12fe2` |
| Etcd v3.7.2 inspected and tested | `68c065e562994b89e333e77b039ad066f933c586` |
| Shared raft v3.7.0 inspected | `b867cf13f6bc0dae21204302df97bc2355c3af55` |
| Passing workflow and probe commit | `23a155d506cd4de673d31de618507f1bc486ecc8` |
| Tigerstore executable SHA256 | `65a67b2af67e0ab8d155efc5c1dacd3f922ed75b87a4cfa58ceaa073692d3f9e` |
| Probe executable SHA256 | `d0e30438e5d5a3964f994768f899b939085467610b9a993042eff15f434aabbf` |
| Etcd executable SHA256 | `7a0c00e4ee33bea9bdec0c8abd771ef4e1685deb134a17cf58c11985d2fe51ca` |
| Etcd release archive SHA256 | `3a3679bc51a4ee9d30bccea1da7cd4fe62c6fc1d2ca1255068d2c53bf3026135` |

The executable hashes match across all three repetitions. Tigerstore and the probe built with Go 1.27.2. The stock etcd release reports Go 1.26.8. Runners were `ubuntu-24.04`, x86-64, four logical CPUs, AMD EPYC 9V74, Linux `6.17.0-1022-azure`. Processes shared the runner CPUs without affinity; `GOMAXPROCS=2`. This is not a throughput benchmark. Peer traffic used fresh mTLS certificates. Client traffic used HTTP. Both stores used default 100 ms ticks and a one-second election timeout.

The three successful jobs ran from 17:42:59Z to 17:43:57Z, 17:43:58Z, and 17:43:59Z on 2026-10-13. Job durations were 58, 59, and 60 seconds.

## Direct unary results

The probe uses protobuf gRPC clients on one fixed endpoint with retries disabled. Each row has six calls unless noted. All leaderless calls had a one-second client deadline. The across-heal Put had a five-second deadline; stopped peers resumed 300 ms after submission.

Durations below are integer milliseconds. The probe truncates elapsed time, so `0` means less than one millisecond, not zero latency. Quantiles use nearest rank, `ceil(p*n)`. With only six calls, p95 and p99 equal the maximum. All samples and exact errors are in [summary.json](evidence/38440882457/summary.json).

| Call while leaderless | Store | Result | Min | p50 | p95 | p99 | Max |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| Put, plain | etcd | 6 DeadlineExceeded | 1000 | 1000 | 1001 | 1001 | 1001 |
| Put, plain | tigerstore | 6 Unavailable/no leader | 0 | 0 | 0 | 0 | 0 |
| CAS Txn, plain | etcd | 6 DeadlineExceeded | 1000 | 1000 | 1000 | 1000 | 1000 |
| CAS Txn, plain | tigerstore | 6 Unavailable/no leader | 0 | 0 | 0 | 0 | 0 |
| Linearizable Range, plain | etcd | 6 DeadlineExceeded | 1000 | 1000 | 1000 | 1000 | 1000 |
| Linearizable Range, plain | tigerstore | 6 DeadlineExceeded | 1000 | 1000 | 1000 | 1000 | 1000 |
| Serializable Range, plain | both, separately | 6 OK each | 0 | 0 | 0 | 0 | 0 |
| MemberList, plain | both, separately | 6 OK each | 0 | 0 | 0 | 0 | 0 |
| Status, plain | etcd | 6 OK | 0 | 0 | 1 | 1 | 1 |
| Status, plain | tigerstore | 6 OK | 0 | 0 | 0 | 0 | 0 |
| All six unary methods, require-leader | both, separately | 36 Unavailable/no leader each | 0 | 0 | 0 | 0 | 0 |
| Put across heal, plain | etcd | 6 OK and read back | 313 | 320 | 1972 | 1972 | 1972 |
| Put across heal, plain | tigerstore | 6 Unavailable/no leader, absent after heal | 0 | 0 | 0 | 0 | 0 |

Etcd's across-heal samples were `313, 317, 320, 1569, 1776, 1972` ms. The first three are follower-survivor cases; the last three are leader-survivor cases. Tigerstore's six samples were all below one millisecond.

Exact error strings for the table:

- Every no-leader error on both stores was `rpc error: code = Unavailable desc = etcdserver: no leader`.
- All six plain linearizable Range errors on each store were `rpc error: code = DeadlineExceeded desc = context deadline exceeded`.
- Etcd's six plain Put errors included five `rpc error: code = DeadlineExceeded desc = context deadline exceeded` and one `rpc error: code = DeadlineExceeded desc = stream terminated by RST_STREAM with error code: CANCEL`.
- Etcd's six plain CAS Txn errors had the same five/one split. Both reset descriptions occurred in repetition 1's follower-survivor case and still had code DeadlineExceeded.
- Successful calls had a nil error.

None of the require-leader-rejected mutations appeared after recovery. Every acknowledged seed retained its value. The six etcd across-heal successes appeared in subsequent linearizable reads. Tigerstore's rejected across-heal writes remained absent.

## Watch results

Every row has six watches per store. The new leaderless require-leader watch rejected before creation on both stores. Existing require-leader watches closed on both stores. Every termination used the exact no-leader error above.

| Measurement | Store | Min ms | p50 ms | p95 ms | p99 ms | Max ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| New required watch, start to rejection | etcd | 0 | 0 | 1 | 1 | 1 |
| New required watch, start to rejection | tigerstore | 0 | 0 | 1 | 1 | 1 |
| Existing required watch, confirmed leaderless to termination | etcd | 2001 | 2078 | 2698 | 2698 | 2698 |
| Existing required watch, confirmed leaderless to termination | tigerstore | 1991 | 2006 | 2792 | 2792 | 2792 |

The latter interval starts at the probe's first leaderless Status observation, not at the monitor's first no-leader check. The three-check policy and monitor phase explain why it need not measure exactly three seconds from that observation. Existing required watches lived 3911 to 3994 ms from creation on etcd and 3001 to 4001 ms on tigerstore.

Both existing plain watches and plain watches created while leaderless stayed open and delivered the post-heal sentinel in all six cases per store. A new required watch also created successfully after healing in every case. Each store's 18 remaining open watches then ended under deliberate cleanup cancellation with `rpc error: code = Canceled desc = context canceled`, after the passing verdict. These cleanup cancellations are not fault failures.

## Why this does not establish a product bug

[The source comparison](SOURCE.md) explains the result. Etcd uses raft's asynchronous Node, which disables mutation admission while it has no leader. Requests can wait for recovery. Tigerstore uses RawNode directly. A leaderless RawNode rejects the proposal immediately, before acceptance, and tigerstore returns Unavailable/no leader. Both choices preserve the tested data and consistency guarantees; their availability during brief outages differs.

Faster rejected operations can create more unknown mutations in a fixed-duration Jepsen history when the upstream client conservatively records Unavailable as ambiguous. This result supports that explanation but does not measure its contribution in the eight saved OOM histories. Those histories remain inconclusive. Do not relabel their errors as definite aborts or claim that this targeted pass certifies them. This pause/resume probe also does not certify crash or power-loss durability.

## Retained evidence and reproduction

- [Passing evidence](evidence/38440882457/) contains all 12 JSONL histories, server logs, exact commands, version records, binary hashes, Actions metadata, and the derived summary.
- Server data directories contain large preallocated WALs. After downloading the original artifacts, each `data1`, `data2`, and `data3` directory was archived byte-for-byte as an adjacent `.tgz` with `tar -I 'gzip -1'`. No data file was omitted. The uncompressed copies were removed to save space. Extract with `tar xzf data1.tgz`.
- [SHA256SUMS](evidence/SHA256SUMS) covers all retained evidence except itself.
- [The first attempt](https://github.com/djosh34/tigerstore/actions/runs/38440727514) failed before any server or probe ran. Actions' shallow checkout lacked the pinned product SHA, so `git worktree add` failed with `fatal: invalid reference: b5f2e28ef2edd94393d62952b1568763ebc12fe2`. All three failed job logs and version records remain in [its evidence directory](evidence/38440727514/). The next commit fetched that exact SHA before building. The failed attempt is not counted as a reliability case.
- [run.sh](run.sh), [probe/main.go](probe/main.go), and [.github/workflows/jepsen2-leader.yml](../../.github/workflows/jepsen2-leader.yml) are unchanged from the successful run's commit. The push workflow is restricted to `exp/jepsen2-leader` and those script paths. Evidence/report-only pushes do not rerun it.

The workflow and probe refuse new starts at or after 2026-10-13T22:00:00Z. For offline summary reproduction, run `python3 experiments/jepsen2-leader/summarize.py experiments/jepsen2-leader/evidence/38440882457`. This only reads saved histories.
