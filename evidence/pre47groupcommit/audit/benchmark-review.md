# Group-commit benchmark audit

Initial scripts reviewed at `fe9efd247bdac9dcc297838f38745c83b59255fd`. Final fence source reviewed at parent `b2458f2b1dfc41a1a18592b3deac59189cce2a4d`, which includes candidate driver `07a986e`. Review completed on 2026-10-13 before the 22:00Z deadline. Read-only inspection; no benchmark run, push or GitHub comment by this auditor.

## Verdict

The experiment can compare this driver candidate against the observability-only baseline and stock etcd for the stated leader-only Put workload. No fairness defect blocks that comparison. Report the counters as gross completed Puts per measured sync call, not logical batch size. The final source has one small remaining scrape-boundary race described below. The parent received it before this report.

The first runner launch used the earlier fence logic. The parent reported that its cancellation request succeeded and will archive the reason and run link. Do not combine that launch's measurements with results from the final fence version.

## What is controlled

`run.sh` pins baseline `81053bcc54ac315b6ac9302dbe09b5eb591c18db`. Its diff from main `b8bac8b` adds the debug HTTP service, load generator and osfs timing wrappers, with no core, node-driver or peer-transport change. Candidate and baseline use the same timing wrappers and environment, including `PRE50_DISK_TIMING=1`. The wrappers add time reads and atomic counters per file operation, so measured gains include any benefit from fewer instrumentation calls. This is the intended instrumented comparison, not an uninstrumented release claim.

The three jobs use 16, 64 and 256 closed-loop workers. Each job starts five fresh clusters of each store, one store at a time. The generator is identical for all stores, with 16 TLS 1.3 gRPC connections to the current leader, 4096 keys, 1 KiB values, 4096 seed Puts, 20000 warmup Puts and 60000 measured Puts. Each process uses GOMAXPROCS=1. Existing server threads move to leader core 0 and follower cores 1 and 2, and post-run affinity checks require every server thread to remain there. The generator and Python control process use core 3.

All stores use the same temporary-directory disk placement, TLS certificates and cluster shape. The script enables ext4 barriers, records the resulting target mount options and rejects an explicit nobarrier or barrier=0 mount. Stock etcd is v3.7.2, with an executable SHA-256 check, an 8 GiB backend quota and TLS 1.3 limits. The evidence records process placement, kernel and CPU details, disk and mount details, Go version, executable hashes and Go build metadata.

The generator excludes connection setup and probes from elapsed throughput and samples process CPU around the request window. It checks no RPC errors, exactly 60000 successful Puts, one reply term, valid response headers, exactly one revision increment per successful Put, and unchanged process start times. Its /proc/stat indexes for process user/system CPU and start time are correct.

## Final fences

The original scripts sampled file-sync counters outside the generator window without requiring all followers to catch up. Majority acknowledgement does not prove the third replica has completed the measured cohort.

Commit `4e1ab70` added serializable gets on every replica before and after measurement. Each fence requires the exact expected revision. It archives the headers and requires all their terms to equal the sole measurement-reply term. This fixes the prior mismatch where etcd had an explicit term postcheck but tigerstore exposed only a leader ID. For tigerstore, the raft async apply rule also proves those applied entries were locally stable.

Stock etcd is different. The inspected source is `/home/joshazimullah.linux/go/pkg/mod/go.etcd.io/etcd/server/v3@v3.7.2`:

- `etcdserver/raft.go:219-257` sends committed entries to apply before WAL.Save finishes.
- `etcdserver/server.go:971-983` applies the entries and advances the apply wait before waiting on apply.notifyc. A serializable read at the final revision therefore does not, by itself, prove that the local WAL Save completed.
- `etcdserver/raft.go:256-262` updates the proposals_committed_total gauge only after successful storage.Save.
- `storage/wal/wal.go:995-1035` writes entries and hard state, syncing when entries, term or vote require it. Ordered commit-only saves follow earlier synced entry saves.

Commit `b2458f2` adds the needed etcd fence. After the all-node revision fence, disk() captures each endpoint's raftIndex and polls its post-Save committed gauge until it reaches that target. The target includes the already-observed applied cohort. A reached gauge proves the ordered Save pipeline has passed that index. The source archives target and gauge values and fixes the comment that had incorrectly applied tigerstore's async-local-stability rule to stock etcd.

### Remaining scrape-boundary race at b2458f2

The code uses the same /metrics response both to discover that the gauge reached the target and to extract the WAL histogram count and sum. A metrics response is not an atomic snapshot of independent metric families. The histogram can be sampled just before the final sync observation and the committed gauge just after Save. That response meets the fence while its histogram still omits the final observation.

Fetch a fresh /metrics response after the gauge loop succeeds, and use that new response for the archived histogram and counter deltas. Its whole collection begins after the fence completed. This is a likely one-observation discrepancy, not a throughput or durability bug, but it matters when claiming exact final-cohort sync counts. I sent this precise change to the parent before stopping.

## How to interpret the counters

- Tigerstore's `syncs` counts all osfs file Sync calls, including segment-header syncs. It excludes directory syncs. `writes` and `write_bytes` count file Write calls and bytes, including headers. Counts also include calls that return an error, though a server failure should invalidate the benchmark.
- The etcd WAL histogram is not only append-group fdatasync. `storage/wal/wal.go:845` records a directory fsync at segment rotation in the same histogram, and other WAL paths also observe it. Backend commit counts are separate and the summary reports them.
- Neither denominator counts logical PersistJobs, hard-state records or operations per Raft entry. A larger ratio does not prove a particular logical entry batch size. Mean file-write bytes can support the physical grouping explanation, but counters alone do not measure that mechanism.
- Both counter windows include work after the generator stops while followers catch up. Generator CPU and elapsed throughput exclude this catch-up. Archive and state that distinction. Do not divide the extended sync-wall sum by generator elapsed time and call it a within-window disk utilization measurement.
- The before fence does not make future background commit-only work impossible. The denominator remains all measured file/WAL sync activity during the extended cohort window, which is why gross sync density is the right name.

## Scope and reporting limits

This is a leader-only, loopback, 1 KiB Put comparison with no injected network delay. It is not follower ingress, Kubernetes mixed workloads, one-client latency, 5 ms links, a maximum-object benchmark or a gray-disk test. At fixed 16 connections, more workers add stream concurrency, not connections. If the generator saturates core 3, throughput cannot show the server's unconstrained ceiling. Report its occupied core count alongside each server's.

Candidate always runs in the middle. Baseline and etcd alternate first and last, with baseline first on three repetitions and last on two. This reduces some order drift but is not full position randomization. State the order. For the candidate-versus-baseline effect, report per-repetition paired ratios and a spread as well as marginal medians. Median ratios and the ratio of medians need not agree.

The summaries should remain claims about these instrumented fresh clusters on each runner. Do not pool different concurrency jobs as though they shared one physical runner. Preserve every run row and invalid-run artifact rather than publishing only medians.

`summarize.py` currently rejects invalid measurements, RPC errors, missing successes, zero sync counts and fewer than five rows per store. It relies on the producer for the workload and metadata. It does not independently check `op == put`, expected worker count from the c-directory, 16 connections, key/value size, exact repetition IDs 1 through 5, counter monotonicity for every field, finite numbers, or replay the fence assertions. These are useful archive-validation additions, not blockers for a fresh runner where the checked producer emits the JSON.

There is also no explicit assertion that all three server processes remain alive throughout the measurement; generator CPU samples and the end affinity/status checks make an exited process fail later. The 15 s fence loop is not a strict wall timeout because each etcdctl subprocess has its own command duration. The enclosing runner timeout is the final bound, and a timeout must remain an invalid run.

## Checks performed

`bash -n experiments/pre47groupcommit/run.sh` passed. Both Python scripts compiled from their source text without writing pycache in the parent worktree. Parent `git diff --check` passed. I did not execute the runner script, remount filesystems or start clusters. Driver correctness results are in `driver-review.md`; raft contracts and simulator gaps are in `contracts.md`.
