# Combined-fault correctness round 3

Tested tigerstore main `499a4c6759d0c1276aa4257af462295216f8ab21` against stock etcd v3.7.2. Product code is unchanged. All clusters, builds and model checkers ran on free `ubuntu-24.04-arm` and `ubuntu-24.04` runners. No subagents were created. Nothing was posted to #49.

Testing has stopped. The last runner job completed at **2026-10-13T21:26:27Z**. The experiment workflow is disabled.

## Results

| Matrix | Cases | Passed | Failed | Recorded operations | Checker verdicts | Timeout verdicts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Initial snapshot-content checks | 32 | 22 | 10 | 27,945 | 512 | 40 |
| LIST freshness in whole history | 32 | 27 | 5 | 67,061 | 512 | 20 |
| Lower-load whole-history checks | 32 | 31 | 1 | 22,530 | 512 | 4 |
| Total | 96 | 80 | 16 | 117,536 | 1,536 | 64 |

Tigerstore passed 43 of 48 cases; etcd passed 37 of 48. Every controller completed its required fault evidence, receipt inspection and three independent cold pair recoveries. The 96 client histories produced 384 genuine original/cold-recovery history variants. Watches, Durable and Revisions passed on every variant. Linearizable passed on 320 variants and timed out on 64. No checker returned a definite consistency violation.

All 16 failed cases remain failed. Each has four 30-second Linearizable timeout verdicts. The final lower-load failure is stock etcd on ARM, one-way partition, seed 350, repetition 1. Its 721 recorded operations became 720 model operations after the checker excluded a failed read. Tigerstore passed all 16 final lower-load cases, twice for every scenario on each architecture. This does not establish a clean matched final matrix because one control remains unverified.

No product failure was confirmed. No bug issue was filed. Existing #88 and #105 were not duplicated. No 256-client ENOSPC case ran in this campaign.

## Faults and concurrent checks

Each matrix ran partition-old-leader then kill-new-leader, pause during a verified leaderless election, one-way peer cuts, and six link flaps. Both products and both architectures ran each script twice. Client and control traffic stayed outside peer packet selectors. Fault records retain requested, installed, exercised and healed phases, dropped peer packet counters, replacement leader status, waitpid evidence and stopped task/CPU evidence.

Each workload seeded 32 permanent LIST keys, then updated their values and versions throughout traffic. Three member-pinned watchers requested PrevKV and covered the whole workload prefix. The repository watch checker required complete delivery through the independently captured terminal revision, against each persisted/recovered log history. Three member-pinned linearizable RangeStream readers ran during traffic and faults.

Every completed LIST had to exercise the first-10 multi-chunk path, carry header/count/more only on its final chunk, preserve sorted unique keys, and return all 32 keys. The checker compared every key, value, create revision, modification revision, version and lease to unary historical Range at the final stream-header revision.

The initial matrix did **not** put RangeStream calls into the linearizability history. Its content comparisons could accept a self-consistent stale latest snapshot. Keep those 32 attempts separate from freshness coverage.

The second and third matrices recorded 54,672 successful linearizable RangeStreams as ordinary Range operations in the original history. Their monotonic bounds are stream invocation and EOF, before the subsequent historical comparison. The existing whole-history checker therefore checks freshness as well as snapshot contents. The final tooling also has a negative regression that requires rejection of a stale LIST after an acknowledged delete. All successfully merged streams remain in the history even if the following historical comparison fails.

Each exercised fault also held its installed state until a fresh, explicitly serializable RangeStream and non-require-leader watch completed on a live, unpaused member. The watch replayed the mutable LIST prefix from revision 1, checked increasing unique mutation revisions and PrevKV/version chains, and reported progress through the independently captured LIST target. Its replayed state had to equal historical Range at that target. These probes prove checked reads and watch replay during a fault. They do not promise linearizable read progress while quorum is unavailable. The separately recorded concurrent linearizable LISTs retain that distinction.

All **288 fault probes** passed. Every probe started and finished inside its fault's installed-to-healed window. `run/list-watch-overlap.json` records the number of successful linearizable LISTs wholly within each fault window, including explicit zero counts where only the serializable probe completed.

## Loads and pinned sources

All cases used three hot clients, six hot keys, 1,024-byte hot mutation values and 45 seconds of traffic. The first two matrices used five hot operations per second per client, five LIST dataset updates per second, and ten LIST reads per second per member. The final matrix used one hot operation per second per client, one LIST update per second, and four LIST reads per second per member. Lower-load passing histories do not validate the earlier timed-out histories. Failed mutation outcomes remain ambiguous in all matrices.

Reused round 2 reporting/checker scripts from `378066b99664070098acadbec05a478348e87327` and the ordinary combined-fault controller from `374b44c6d0a4e0fe732275285aa7e85118e4e7d7`. Go was 1.27.2.

| Run | Tooling commit | Scope |
| --- | --- | --- |
| [38468658463](https://github.com/djosh34/tigerstore/actions/runs/38468658463) | `5d7e3a45f244c40a6384f02e8109a2d54c7abdd4` | Initial snapshot-content checks |
| [38468977502](https://github.com/djosh34/tigerstore/actions/runs/38468977502) | `1281e8186e0e3b1d09a0a0e861e36591bd4e86d4` | EOF-bounded LIST history |
| [38469549517](https://github.com/djosh34/tigerstore/actions/runs/38469549517) | `6a5634c6e8dd8739c6fcd0e75ccb2932528e7bbb` | Lower load and stale-LIST regression |

The campaign ran from 21:11:16Z to 21:26:27Z. Aggregate fault-job runtime, including setup and upload, was 15,981 seconds, or 4h26m21s. Every run has 32 fault jobs plus one planning job. No further matrix was dispatched.

## Permanent evidence and audits

`evidence/<run-id>/` contains all 96 compact case archives, all 99 final job records, run metadata and metadata for all 192 Actions artifacts. Each compact case retains compressed client histories, all four disk-set histories, checker verdicts, raw watch/LIST responses, fault probes, controller commands, logs, receipts, runner details and an uncompressed SHA256/length manifest. Inner names retain `correct-round2-faults` because the existing round 2 archiver was reused. The run ID and attempt ledger distinguish the actual round 3 attempts.

`audit.json` gives every case, its 16 checker verdicts, recorded LIST count, probe results and exact totals. `audit.py` independently checks probe chunks, full KV metadata, watch replay and fault-window bounds. It also verifies hashes for the small assertion inputs. Its corruption tests pass. The parent independently verified every archive manifest hash and length, all 288 probe payloads and overlap bounds, and the 96-case totals. The parent's audit notice is retained in `parent-audit-notice.json`.

Full original and recovered server disks remain in the Actions artifacts. Compressed permanent histories and verdicts are committed here rather than left only in expiring artifacts. SIGKILL does not test power loss, and tigerstore's physical WAL reader is shared with the product. This round does not close #49 or certify the 16 timed-out histories.
