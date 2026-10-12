# etcd robustness source audit for tigerstore #49

## Scope and pins

Inspected actual source from etcd **v3.7.2**, commit `68c065e562994b89e333e77b039ad066f933c586`. GitHub's annotated tag object is `cb9b1aa2b1237b59255d65773c34ab0cd9df3679`; it points to that commit. The module is `go.etcd.io/etcd/tests/v3`, and the package directory is `tests/robustness`. It pins Porcupine v1.1.0, gofail v0.2.0 and Raft v3.7.0. [Dependency pins][deps]

Also inspected gofail v0.2.0 at `ed9ca8045cc22a3045168306f2039e9f0a5b397e`, issues #46, #1, #49, #20 and #33 with comments, and `docs/design.md` on main at `91c4f974a6661ac02c2e7747781ea215353c643b`. Work began at 2026-10-12T22:16:52Z, before the deadline. This was a source audit, not an etcd build or a chaos run. Nothing was posted or pushed.

Recommendation: copy the client-history discipline, targeted triggers and checker rules. Replace etcd's process control and WAL reader. Do not treat upstream's skipped checks, fault activation or report revalidation as proof of complete coverage or deterministic replay.

## Failpoint lifecycle

- Registration has two layers. `gofail enable` rewrites `// gofail: var ...` comments and generates package variables calling `runtime.NewFailpoint`. Runtime registration stores each name and applies startup environment terms. The robustness suite separately lists candidate faults in `allFailpoints`. A binary hook is not automatically an exploratory test. Concrete hooks surround WAL saving, snapshot persistence, snapshot application and old-WAL release. [Instrumentation][instrument] [Generated bindings][binding] [Rewritten hooks][rewriting] [Runtime registration][registration] [Raft hooks][raft-hooks]
- Selection filters the explicit list through `Available` on every member, then randomly chooses one. Availability considers binary hook discovery, node role, cluster size, peer proxy, traffic and snapshot thresholds. HTTP GET discovers hook names; PUT arms a hook and DELETE deactivates it. Startup-only faults use `GOFAIL_FAILPOINTS`; running processes use `GOFAIL_HTTP`. [Selection][selection] [Availability][availability] [HTTP control][control] [Startup environment][startup]
- Triggering is deliberate where normal traffic is insufficient. Defrag calls Defragment. Compaction waits for enough revisions and calls Compact. Snapshot faults isolate a follower until the revision gap exceeds snapshot-count plus catch-up entries, then restore traffic. Trigger clients' relevant operations join the history. Compaction triggers exclude periodic-compaction profiles and the peer proxy. [Triggers][triggers] [Snapshot trigger][snapshot-trigger]
- Panic faults arm `panic`, run the trigger, wait for process exit, optionally discard LazyFS unsynced data, and restart. Injection uses a one-minute default context, checks every member's gRPC health before and after, and records the interval. This checks recovery, not every durability promise. [Panic path][panic] [Injection][injection]
- Verification has gaps. The startup sleep path explicitly has a TODO to verify execution; the running sleep path waits one second and deactivates without checking a hit. gofail v0.2.0 already exposes GET `/<name>/count`. The blackhole implementation explicitly warns that an alternate peer pipeline can still carry traffic. [Sleep gap][sleep-gap] [Execution count][count] [Bypass warning][blackhole]

For tigerstore, record requested, armed, hit, healed and verified separately. Fail when a required hook never executes or a directed-link cut has a bypass. Confirm the named hook caused the exit, rather than accepting any process death. Put crash hooks around tigerstore's actual append, sync, root-install, snapshot, cleanup and recovery steps, not etcd's bbolt internals.

## Traffic and timing

The runner starts injection, traffic and per-member watch collection concurrently under one monotonic base time. It waits roughly one second before injection and one second after recovery, with 200 ms jitter. Traffic then stops, waits for its workers, sleeps another second and performs a successful `tombstone=true` Put. Its revision bounds watch collection. [Runner][runner] [Timing constants][timing] [Traffic shutdown][traffic-end]

Profiles use six member-pinned and two cluster clients, plus separate watch clients and compaction. The shared rate limiter has maxima of 100, 200 or **1,000 QPS**, not a universal 200 QPS cap. Nonunique requests have concurrency limit three. Requests usually time out after 200 ms; traffic watches after 500 ms. Failed requests force a successful list before another write to limit ambiguity. Only pre-fault minimum QPS gates the run; post-fault coverage remains a TODO. [Profiles][profiles] [Error scheduling][error-scheduling]

Copy both hot-key etcd traffic and Kubernetes shapes: MOD-based create/update/delete, failed-CAS reads, revision-pinned pagination, list-then-watch with `RequireLeader`, and the VERSION-based compactor transaction. Unique Put values identify requests; multi-op transactions force at least one unique Put. The default lease TTL is 7,200 seconds, so short upstream cases do not test expiry timing. Add explicit expiry, slow-client, large-value, overload and numeric healthy-suffix scenarios. Keep small independent checker workloads separate from throughput tests. [Kubernetes traffic][kube] [Unique transactions][unique-txn]

## Identity, histories and unknown outcomes

Client IDs identify reports; stream IDs identify sequential Porcupine lanes; request IDs make writes unique. The recording client serializes KV calls and records invocation/response times with `time.Since(baseTime)`. An ambiguous failed mutation starts a new stream for subsequent requests. The raw report retains the observed error-return time; checker preparation extends that operation's return to `MaxInt64`. A timeout therefore permits a later commit rather than proving abortion. The nondeterministic model considers both unchanged and applied states. [Identity][identity] [Recorder][recorder] [Failed lanes][lanes] [Preparation][prepare] [Unknown outcomes][unknown]

Upstream narrows ambiguity using WAL occurrence counts and watch revisions. It can remove unique failed mutations absent from the WAL, mark others persisted, and shorten return bounds using later persisted operations. This depends on complete, trustworthy evidence. Preserve immutable raw client histories and run an independent unpatched check on isolated small cases. Use WAL/watch-assisted patching only as a separately reported check. Never omit interfering writers from a checked history. [History patching][patch]

The model is a subset, not the entire etcd specification. Historical Range initially gets a partial response because the online model retains only current state; a later replay check compares values at the **requested** revision. Transaction revision increases once only if the selected branch changes state, including a mutating failure branch. Values of 20 bytes or more become 32-bit FNV hashes, and recorded value metadata lacks create revision. Keep full bytes and required MVCC fields in tigerstore's evidence. Add tests for the recorder and model's unsupported options rather than silently dropping them. [Historical and transaction semantics][semantics] [Historical replay check][historical-check] [Lossy values][lossy]

## Persisted evidence and checkers

The WAL reader decodes entries and HardState and replaces overwritten suffixes. Cluster merge chooses the entry with most matching copies at each index, using higher term to resolve some ties above the minimum observed commit. It does **not** require a majority at every index or truncate the returned history at that commit. WAL presence is not itself proof of commitment. Read errors on individual members are logged and skipped; unexpected EOF can trigger repair of the inspected WAL. This is not an erase-one-disk durability test or a complete snapshot-state reconstruction. [WAL collection][wal] [Merge rules][merge] [Repair][repair] [Decoder][decoder]

Replace this with a tigerstore-specific export that understands packed RPCs, durable roots, retained segments and checkpoints. Preserve original disk images before parsing or repair. Report per-node decode failures. Check acknowledged and read-observed effects after cold recovery with each disk erased in turn. Replica equality alone can pass when all replicas lose the same write.

The checker first validates empty-start and nonoverlapping-lane assumptions, then Porcupine, then watch and historical reads against replay. Porcupine `Unknown` is a timeout failure. A separate model deadline flag fires at 110% of the nominal timeout because a long Step can defeat Porcupine's timeout. Use an outer process watchdog too. Important porting trap: an empty persisted-request list skips watch and historical checks, and the zero-value Result status named `Unknown` returns no error. Require an explicit Success for every required tigerstore check. [Pipeline][pipeline] [Timeout handling][timeout] [Skipped-result behavior][results]

Watch validators cover filter, order, uniqueness, revision atomicity, progress bookmarks, resumability, reliability, IsCreate and PrevKV. But reliability ends at the last observed event/progress revision; resumability accepts a missing first event. A silent watch can therefore escape these checks. PrevKV validation permits nil under compaction. Require complete delivery through an independently obtained terminal revision after healing, and require PrevKV on requested update/delete events, not creates. Reassemble fragments before atomicity checks if fragmentation is supported. The collector resumes at last revision plus one, but jumps over compaction cancellations; that gap must be explicit. [Watch bounds][watch-bounds] [PrevKV exception][prevkv] [Collector][collector]

The runner also compares HashKV and compaction revision across members at a common final revision. Copy the convergence check, but add the cold-recovery and receipt-ledger tests above. [Hash check][hash]

## Artifacts, replay and shrinking

Reports save `server-*` data directories, per-client operation/watch JSON and a small traffic-detail file. Failed, panicked or `PERSIST_RESULTS` runs retain them; successful runs otherwise remove them. Directory copying is not an atomic snapshot and does not stop the servers. Client reports attach only after the scenario returns, so a mid-scenario panic can save disks without a complete history. [Capture ordering][capture] This version only installs the visualizer after successful validation, and its visualizer skips error results. Do not assume every illegal history has an HTML diagnosis despite the README examples. Failpoint interval/name information feeds traffic statistics, not a persisted complete fault journal. Capture that journal yourself. [Report implementation][report] [Visualizer gate][visualizer]

`TestDataReports` reloads saved histories and WALs and reruns validators. That is **offline revalidation**, not rerunning processes or their schedule. Options have a wall-time-seeded RNG; traffic, faults and jitter also use global randomness and goroutines. No automatic execution replay or shrinker appears in the inspected robustness source. Named regression scenarios fix configurations and faults, but still use timed traffic. [Report revalidation][revalidate] [Random options][random-options] [Named scenarios][scenarios]

For #49, save binary/source versions, seed, full configuration, raw history, fault decisions with connection/direction/offset and timestamps, hook hits, server logs, checker statuses and original disks. A seed reproduces policy, not real kernel timing. Reduce a failing schedule while preserving its assertion, then add a named regression at the layer that reproduces it. Do not demand a TLS or scheduler failure replay inside the deterministic core simulator.

## Suggested first slice

1. Port the recorder and restricted model with legal/illegal fixtures, especially late commits after timeout. Run an isolated small-history external checker without WAL patching.
2. Force one crash, one verified directed cut and one stalled leader-disk case. Require hook/proxy hit evidence and a bounded healthy suffix.
3. Add complete-through-target watch checks, final replica equality and erase-one-disk cold recovery. Missing evidence or skipped check fails the job.
4. Preserve one full artifact per failure and provide offline revalidation. Add explicit minimized regressions before expanding random campaigns.

[deps]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/go.mod#L1-L43
[instrument]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/Makefile#L94-L109
[binding]: https://github.com/etcd-io/gofail/blob/ed9ca8045cc22a3045168306f2039e9f0a5b397e/code/binding.go#L31-L50
[rewriting]: https://github.com/etcd-io/gofail/blob/ed9ca8045cc22a3045168306f2039e9f0a5b397e/code/failpoint.go#L60-L80
[startup]: https://github.com/etcd-io/gofail/blob/ed9ca8045cc22a3045168306f2039e9f0a5b397e/runtime/runtime.go#L43-L60
[registration]: https://github.com/etcd-io/gofail/blob/ed9ca8045cc22a3045168306f2039e9f0a5b397e/runtime/runtime.go#L137-L151
[raft-hooks]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/raft.go#L245-L285
[selection]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/failpoint/failpoint.go#L38-L80
[availability]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/failpoint/gofail.go#L139-L164
[control]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/framework/e2e/etcd_process.go#L365-L502
[triggers]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/failpoint/trigger.go#L39-L107
[snapshot-trigger]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/failpoint/network.go#L93-L154
[panic]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/failpoint/gofail.go#L81-L137
[injection]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/failpoint/failpoint.go#L82-L143
[sleep-gap]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/failpoint/gofail.go#L171-L237
[count]: https://github.com/etcd-io/gofail/blob/ed9ca8045cc22a3045168306f2039e9f0a5b397e/runtime/http.go#L88-L113
[blackhole]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/failpoint/network.go#L71-L90
[runner]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/main_test.go#L125-L189
[timing]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/main_test.go#L45-L49
[traffic-end]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/traffic/traffic.go#L143-L184
[profiles]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/traffic/traffic.go#L49-L142
[error-scheduling]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/traffic/etcd.go#L139-L160
[kube]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/traffic/kubernetes.go#L179-L325
[unique-txn]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/traffic/etcd.go#L365-L413
[identity]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/identity/id.go#L19-L48
[recorder]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/client/client.go#L34-L126
[lanes]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/model/history.go#L304-L340
[prepare]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/validate/validate.go#L69-L94
[unknown]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/model/non_deterministic.go#L151-L207
[patch]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/validate/patch_history.go#L34-L210
[semantics]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/model/deterministic.go#L163-L238
[historical-check]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/validate/operations.go#L98-L130
[lossy]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/model/types.go#L219-L246
[wal]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/report/wal.go#L64-L97
[merge]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/report/wal.go#L99-L194
[repair]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/report/wal.go#L206-L228
[decoder]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/report/wal.go#L368-L419
[pipeline]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/validate/validate.go#L32-L62
[timeout]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/validate/operations.go#L33-L83
[results]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/validate/result.go#L37-L83
[watch-bounds]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/validate/watch.go#L196-L252
[prevkv]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/validate/watch.go#L254-L315
[collector]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/client/watch.go#L60-L121
[hash]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/client/kvhash.go#L29-L69
[report]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/report/report.go#L59-L115
[capture]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/main_test.go#L90-L115
[visualizer]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/main_test.go#L117-L123
[revalidate]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/validate/validate_test.go#L34-L64
[random-options]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/options/cluster_options.go#L24-L49
[scenarios]: https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/tests/robustness/scenarios/scenarios.go#L187-L259
