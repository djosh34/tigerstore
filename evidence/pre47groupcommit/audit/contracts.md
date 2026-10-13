# Append-clocked batching audit

Inspected on 2026-10-13 before the 2026-10-13T22:00:00Z deadline. Initial core, node, WAL and simulator source was commit `b8bac8b371c94f6d17136ad14aaa04354bbff01e`. The parent has since added observability and benchmark scripts to its `repo` worktree. I made no changes in `repo` or `driver`, created no subagents, and posted nothing to GitHub.

## Decision

The proposed driver-only gate can preserve safety. Defer client `Submit` calls outside the core, keep stepping peers and ticking, and do not call `Process` while any previously emitted append group awaits completion. On successful completion, call `Persisted` for each original job in order, then submit the deferred requests and run `Process`. Preserve every original storage response and every suffix replacement.

This is a disk-clocked scheduling experiment, not the existing design's nonblocking control path. It delays heartbeats, votes, ReadIndex, apply, timeout replies and leader observations behind disk completion. It also does not automatically become part of simulator coverage, because the simulator drives `core.Node` directly and never calls the real driver.

Coalescing the last non-nil hard state within one group is safe if all entries remain in job order, the last hard state goes after all those entries, the whole group syncs before any response, and an error fences the node. No WAL format change is necessary. Do not merge hard-state fields independently or move a hard state before entries it commits.

## What raft v3.7.0 actually requires

The source inspected is `/home/joshazimullah.linux/go/pkg/mod/go.etcd.io/raft/v3@v3.7.0`.

- `rawnode.go:131-135,397-442`. `RawNode.Ready` immediately calls `acceptReady`. It updates the previous hard state, clears outbound messages and read states, marks unstable entries in progress, and accepts applying entries. A retrieved Ready cannot be discarded, superseded by a later Ready, or treated as a peek. Defer the call itself.
- `rawnode.go:440-445`; `log.go:215-276`. With `AsyncStorageWrites`, `applyUnstableEntries` returns false. Apply stops at `min(committed, unstable.offset-1)`. Remote majority commit may advance while local disk stalls, but the origin cannot apply the new locally unstable entry and ack its write. This holds for leaders and followers.
- `raft.go:153-187`. Local messages to the same storage thread must be reliable and ordered. Writes for append messages with responses must be durable before those responses. Different storage threads may run independently. Async storage does not permit dropping old jobs.
- `rawnode.go:200-217,223-259`. An append message may contain no entries. It can carry only a hard state, or only responses dependent on prior appends. Those jobs still have to run. Do not decide whether a completion matters from entry count, bytes written, or a nil hard state.
- `rawnode.go:246-259`. Response order is deliberate. Leader self `MsgAppResp` comes before `MsgStorageAppendResp`, so raft can use unstable terms on its fast path. Preserve the original list, rather than inventing a synthetic completion.
- `rawnode.go:266-360`; `raft.go:1166-1185,1197-1204`. The stability response carries both the current Raft term and the last entry's index and log term. Old-current-term responses must be ignored after a term change. This prevents the documented ABA race. A later hard-state-only append can carry the current-term stability witness for entries from an earlier job. Losing it can leave apply stuck after the disk is durable.
- `rawnode.go:365-395`; `raft.go:1205-1211`. Apply responses have term zero and return the exact applied entries. They advance raft's applied index and release its outstanding-apply byte quota and uncommitted-size quota. Returning them early, omitting them or replaying them is not equivalent to applying entries.
- `rawnode.go:475-485`. Do not call `Advance` under async storage.
- `node.go:51-113`. Ready fields are read-only. Direct peer messages in async Ready may be sent immediately. The response dependencies live inside local storage messages. Blanket Ready gating delays messages raft deliberately separates from disk persistence.
- `log_unstable.go:23-44,93-108,138-174,178-194,214-234,237-251`; `storage.go:293-328`. Raft retains shared entry objects. Suffix replacement allocates protected slice storage so old accepted Ready entries remain intact. The application must not mutate entries, their protobuf scalar pointers, their data, or returned messages. It may retain references until consumers finish. Clearing the slice that holds jobs is different from clearing `job.Entries` or poisoning entry data.

The local contract tests in `contracts_test.go` use the actual pinned raft package. `go test -mod=readonly -count=1 -v ./...` passed. They check committed-but-unstable apply suppression, immediate Ready acceptance, the entry-free current-term completion needed after a term change, and accepted-entry lifetime across suffix replacement. The initial build failed because `Status.Commit` is a pointer in v3.7.0; I corrected the test to use `GetCommit`. See `contracts.log`.

## Driver and WAL invariants

1. One owner goroutine calls every RawNode and core method. The disk goroutine never calls `Persisted`, steps raft or mutates a job.
2. The gate begins when `Process` produces append jobs, not when the disk worker happens to receive them. It ends only after the owner observes a successful completion for that group.
3. A held client request does not enter `core.Submit`. `queueWrite` auto-proposes when the request-byte cap is crossed. Gating just `Process.propose` is insufficient.
4. A completion reports exactly the original ordered jobs once. The original response lists remain attached. Empty and hard-state-only jobs count.
5. The WAL writes all entry records in the emitted order. Repeated indexes are suffix replacement, not duplicates to sort or remove. The last non-nil hard state is a complete snapshot, including vote zero, rather than a fieldwise maximum.
6. `WAL.Sync` must finish successfully before any `Persisted` call, including segment close, creation, header sync and directory sync that `Sync` performs after a rotation. Failures may leave durable bytes, but must release no completion.
7. `core.Persisted` installs each job into MemoryStorage before its responses can remove entries from unstable. Do not signal stability first or expose new MemoryStorage entries as durable before fsync.
8. Errors release no success reply or positive durable response from the failed group. The real driver must stop. Clients need a stopped or ambiguous error, not a definite promise that an already-proposed write cannot commit elsewhere.
9. Deferred client requests retain request order and unique IDs. One flush can create multiple capped Raft entries through existing `queueWrite`; it must not create one unbounded entry.
10. Apply, request replies and watches occur at most once. A context timeout does not cancel an already-proposed write. A completion from an earlier process generation cannot reach a restarted core.
11. The queue has an explicit memory bound or documented request-count bound. A count of 8192 permits about 12 GiB of 1.5 MiB requests in the deferred queue alone. There is another 8192-input inbox, plus upstream blocked RPCs. This is not a useful byte-memory bound on an 11 GiB host.

## Failure schedules and checks

These are required additions unless a narrower current test is named. Every targeted test should assert that the intended stall, crash or response condition occurred. Final convergence alone is not enough.

### 1. A gate starts too late or resumes too early

Hold one disk completion after `Flush`, inject a burst of client writes, incoming appends and ticks, and keep the disk paused. Check that the owner produces no second append group and calls no further Ready during the interval. After the completion, check exactly one next group contains the held workload, subject to the entry cap. Repeat on a follower receiving several leader appends. Count logical jobs separately from physical writes and syncs.

Current simulator runs Process after every input and can queue more persist jobs while busy. It does not implement or assert this gate. Current core tests persist immediately.

### 2. Submit bypasses the gate at the byte cap

During the stall, submit enough valid requests to cross `MaxBatchBytes` twice. Verify none reaches raft before release. After release, decode every emitted entry and require each request exactly once in FIFO order. Check the actual encoded entry sizes as well as protobuf request sizes.

Current workload has six single-flight clients with small values, so it is not a dependable cap-crossing test. `core.queueWrite` counts protobuf request bytes but the codec adds origin, boot, ID, kind and length bytes. Thus the advertised 4 MiB cap is not a strict encoded-byte cap even before this patch.

### 3. A follower or leader acks locally unstable entries

On a follower, send a committed append, stop its fsync, and inspect every peer response, client reply and watch response. No durable append acknowledgement or newly applied event may escape. On a leader, keep its own append unsynced while two followers sync and acknowledge it. Majority commit can advance, but the origin's reply, applied index and watch event must not advance through its unstable entries. Release the local sync and require one reply.

Raft contract tests here prove the library's apply rule. The random simulator has crashes and sub-100 ms slow writes and final durability checks, but no always-on response-versus-local-stability assertion or deterministic own-disk-stall schedule.

### 4. Storage response order or entry-free completions are lost

Persist an append under term T, change the core to T+1 before delivering its completion, then stop new proposals. A new hard-state-only job must carry raft's T+1 stability witness. Deliver original job completions in order and require eventual apply, zero duplicate apply and full quota release. Assert the leader self `MsgAppResp` precedes `MsgStorageAppendResp`. Also exercise an append job with neither entries nor hard state but with dependent responses.

The local `TestEmptyAppendResponseAfterTermChangeUnblocksApply` passed. Current simulator random term changes may exercise this but records no scenario coverage floor. It does not deliberately omit or reorder those completions.

### 5. A stale completion stabilizes the wrong suffix

Hold append A, replace its uncommitted suffix under a higher term with B, and then restore entries matching A's index and log term under a still later current term. Complete A before B and the restored append. Compare the stable Storage view and recovered disk prefix with the current raft view before each stability response. Require no old-term stabilization, no apply of replaced values and eventual stabilization after the newest current-term job. Use a RawNode unit schedule for the source's five-node ABA case if a three-node schedule cannot reproduce it.

Current random suffix changes plus final convergence can catch outcomes of this failure, but there is no named ABA schedule or intermediate invariant.

### 6. Hard-state coalescing breaks a vote or zero reset

Within one ordered group, use a term/vote update, a later term update with vote zero, a further vote, a commit update and trailing nil hard-state jobs. After sync and cold recovery require the exact last non-nil tuple. Count hard-state records and require one new record. Across two sync groups, require the earlier group's vote survives every crash in the later group. Never take the maximum vote or retain an old nonzero vote after a term reset.

The existing WAL test checks hard-state recovery but does not test the proposed driver's coalescing. The 500-seed WAL crash test primarily checks entry strings and uses commit zero for its random appends; it does not maintain a vote oracle. Add the tuple assertions there or in a driver persistence test.

### 7. Coalescing reorders suffix replacements

In one group append indexes 1 through 4, replace indexes 3 and 4 under a higher term, then append index 5. Sync, cold-recover and compare exact index, term, entry type and data. Include a replacement that shortens the uncommitted tail. Persist the final hard state only after all ordered entries. Crash at every record boundary before sync and require the previous synced committed prefix, plus only a legal unacknowledged suffix.

`wal.TestAppendAndRecover` and `TestCrashKeepsSyncedEntries` already exercise replacement. They do not test the changed driver grouping or all-record-boundary crashes. Do not use a test that overwrites an already committed prefix as evidence of a legal Raft schedule.

### 8. A write or fsync error releases responses or retries

Fail the group's write, segment fsync, segment creation, header write or sync, or directory sync. Require zero `Persisted` calls for the group, zero success replies from it, a stopped node and no retry. Keep the other two nodes healthy and require continued service within a fixed bound. Repeat with a failure after the old segment sync, when bytes may already be durable.

`simfs.Fault` can fail operations before they change state. Current cluster simulator treats every disk error as a failed run rather than expected node fencing. WAL tests use operation failures for crash-in-recovery, not this driver stop path. Add error injection and explicit stopped-node expectations.

### 9. A short write appears successful

Provide a File.Write implementation that returns fewer bytes than requested with a nil error. Require `io.ErrShortWrite`, no sync success and no completion. `WAL.Flush` currently accepts such a result, clears the buffer and can report success. The concrete `os.File.Write` contract returns an error on a short write, so this is not a demonstrated osfs failure. It is a gap in the broader WAL File interface and simulated fault model.

Current simfs Write always writes all bytes or zero with an error. It cannot exercise this case without a wrapper or added fault.

### 10. The gate deadlocks with no writes

Stall and release a term/vote-only append, then stop clients. Require remaining vote responses and apply acknowledgements to finish without another client input. Repeat with a recovered log awaiting replay and with a response-only append. Ensure disk enqueue and completion channels remain selectable when the client queue is full.

Random runs have ticks and active clients until the quiet period, but no named term-only stall test.

### 11. A full deferred queue stops peer heartbeats

Drive deferred requests to the cap while a follower's disk stalls. Continue healthy leader heartbeats and completion delivery. Require that the disk completion still drains and the queue clears. Record whether disabling the shared inbox also blocks peer messages. Bound memory in bytes or state the count-bound limitation. Test real-driver channels, because simulator events do not have this inbox.

If the same inbox carries clients and peers, disabling it at the client cap blocks both. It contradicts an unconditional claim that peer stepping continues throughout disk stalls. Safety remains, but avoid claiming a control-path liveness bound that the code does not satisfy.

### 12. Blanket Ready hold causes election churn and stale leader state

Pause a healthy leader's disk for more than the randomized election timeout, with healthy links and peers. A separate case stalls a follower long enough that CheckQuorum stops counting it. Record terms, leader changes and externally reported `Node.Leader`. Require the healthy majority to recover service within 10 election ticks after a replacement leader becomes usable, or choose a justified bound that includes randomized election and persistence delays. If the goal is surviving long disk stalls without churn, this gate fails that goal by design; separate control-message processing is needed.

Current random slow writes stop below one heartbeat tick and turn off during healing. The only current numeric liveness test starts two seconds after all faults heal. It does not check healthy-majority service during a gray leader's disk stall, or the freshness of `Node.Leader` used by require-leader watches.

### 13. Timeout replies and watch output wait indefinitely

While disk completion remains withheld, tick past `RequestTicks`, submit a watch create, cancel and progress request, and cancel client contexts. Specify whether core timeout/watch output is allowed to wait for the disk. After release require replies at most once, no request ID reuse and watch event order intact. A timed-out submitted write may later commit, which is allowed; it must not receive a second reply.

The driver gate can let `Tick` enqueue timeout and progress output, and `Watch` enqueue create/cancel/catch-up output, without calling Process to drain it. The network handler's context may finish independently. Neither current simulator nor core immediate-persistence tests reproduce this driver delay.

### 14. Async buffers are reused too early

At disk and peer handoff, save hashes of entries, data, scalar fields, contexts and response lists. Continue receiving replacement appends. Verify hashes at consumption and completion. Poison only buffers whose ownership actually ends. Do not poison shared raft entry bytes merely because one disk job finished; raft and MemoryStorage retain them.

Current simulator marshals peer messages and copies disk Write bytes. It does not poison buffers at every asynchronous boundary despite the design requirement, and does not run real peer writer concurrency. Race-enabled real-driver tests are needed too.

### 15. Ack durability passes only because a live replica repairs lost bytes

At each successful write or returned linearizable value, record the stable entry witnesses. Erase each node's disk in turn, power-crash the other two before recovery, cold-restart only those two and require all acknowledged values and revisions. Do not heal a node from an old live in-memory leader before the assertion. Test a crash immediately after physical fsync but before owner completion as well.

`simfs.Erase` exists but has no callers in the cluster or WAL tests inspected. The simulator's durability checker uses the final converged log and state. Random crashes can expose some losses, but there is no required erase-one-disk cold-restart campaign.

### 16. A restarted core receives an old completion

Crash while one append is in flight, restart, then deliver the saved old completion. It must not call `Persisted` on the new core or answer a new request whose numeric ID matches an old one. Check the completion's generation explicitly in deterministic tests.

The simulator already tags disk events with node generation and drops stale completions in `diskDone`. Request origin includes a boot ID. The real driver does not restart a core in place, so process boundaries naturally discard these channels. Keep both properties if recovery changes.

## Current coverage, without the design's future promises

Implemented at the initially inspected commit:

- The real core, WAL and MVCC run in the cluster simulator. Directed network links preserve message order and drop messages when links break. Peer messages are marshalled and copied.
- Random node crashes, delayed restarts, random directed-link masks and switching, message delay, and a 2% chance of 20 to 100 ms disk delay during the fault phase. Link masks are sampled, not exhaustively walked once per PR.
- `simfs.Crash` retains stable data and a random prefix of unsynced appends, sometimes with a garbage tail. File and directory durability are separate. There is an operation-error hook.
- Final replica convergence, historical read consistency, acknowledged-write presence and values, CAS, real-time revision witnesses, unique write log membership, strict watch completeness and a post-healing liveness floor.
- Independent durability, revision and watch checkers on full seeds. Porcupine on shorter histories. Seed replay and cross-process trace comparison in CI.
- WAL recovery and corruption tests, including 500 seeds of random append, suffix replacement and crash, repeated failures during recovery, and each of six header-only replacement steps.
- Simulator stale disk completions drop by generation.

Not implemented by those files:

- The real node driver's append-clocked gate or its channels in the simulator.
- Deterministic installed-fault coverage for every storage step, all link masks, indefinite or election-length fsync stalls, short or misdirected writes, cluster disk-error fencing, gray-leader liveness, pauses, clock skew/jumps, erase-one-disk cold restarts, snapshot install, and asynchronous buffer poisoning.
- A test-only planted-bug catalogue with fixed seed budgets. The documentation describes manual planted experiments, not a persistent build-tag suite.
- Counters proving the candidate's batching mechanism, or per-reply stable-quorum witnesses. These need driver tests or shared deterministic scheduling code.

`go test -mod=readonly -count=1 -short ./internal/core ./internal/wal ./internal/simfs ./internal/sim` passed in the parent's `repo` worktree. See `baseline-short.log`. This is baseline coverage evidence, not a test of the candidate's driver gate or a claim that the full simulator design is complete.

## Patch review status

The evolving driver-only diff inspected at 2026-10-13T00:49Z follows the safe gate and last-hard-state approach above. It still needs its finished tests and final commit reviewed. Its 8192-request deferred count and shared-inbox backpressure have the memory and peer-liveness limits named above. I remain available for the finished patch.
