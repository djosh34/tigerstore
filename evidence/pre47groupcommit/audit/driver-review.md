# Final driver patch review

Reviewed `a33f15a946caf20ae8751c44d6ec4682da3d8d81` in sibling `driver`, cherry-picked as `07a986e` in parent `repo`. Review completed before the 2026-10-13T22:00:00Z deadline. I changed neither worktree.

Approved for the throwaway runner experiment. I found no blocking safety defect in the production diff. This is not approval to merge it as the finished core design.

## Mechanism checked

- `inflight` becomes true immediately when Process returns a persist group, before its disk enqueue. The queue has one slot and the owner cannot generate another group while it waits.
- Client requests wait outside `core.Submit`, so the existing byte-cap auto-propose cannot bypass the gate. Incoming peer messages, unreachable reports and ticks still run until the deferred-count limit disables the shared inbox.
- The owner never calls Process while gated. Thus it never retrieves and loses an accepted Ready. Once a group completes, it calls Persisted for each original job in order, submits deferred requests in order and runs Process.
- The disk worker appends each job's entries in order, retains the last non-nil complete hard state, appends it after all entries and calls WAL.Sync once. Suffix replacements remain in their original order.
- The disk worker returns the original jobs only on full Sync success. Failure returns an error without jobs; the owner stops without stepping their responses. Successful completion retains core.Persisted's MemoryStorage-before-response ordering.
- Clearing the deferred request slice does not clear raft entry objects or job response lists.

## Tests checked and rerun

The new tests cover held Submit across multiple request-byte caps, accumulated requests in several capped entries but one physical group, owner completion and failure handling, follower Ready gating with ordered durable append responses, last-hard-state coalescing with a suffix replacement, and an injected fsync failure in the real persistence worker.

I reran:

```text
go test -mod=readonly -race -count=5 ./internal/node
go test -mod=readonly -count=1 -short ./internal/core ./internal/wal ./internal/server
```

Both commands passed. See `driver-tests.log`. `git diff a33f15a^ a33f15a --check` passed. The driver worktree had no further changes to the reviewed node files.

## Limits that remain

- Physical group bytes are not capped. Multiple capped entries share one fsync. The test documents this distinction, and the production comment now states it.
- The 8192 deferred-request limit bounds count, not memory. At the maximum request size it permits about 12 GiB of request payload before counting the inbox or blocked upstream RPCs. The benchmark's 16, 64 and 256 one-flight clients with 1 KiB values do not exercise that limit.
- The full deferred queue disables the whole inbox and therefore peer stepping as well as client intake. Ticks and disk completion still drain.
- Blanket Process gating delays heartbeat transmission, votes, ReadIndex, apply, core timeout replies, watch output and Node.Leader observations. The new tests set Tick to one hour, so they do not test election-length stalls, timeout progress or gray-node liveness.
- Tests do not add a term-change completion schedule, response-only append schedule, vote-zero coalescing check, crash-at-record-boundary cohort test, queue-full test or erase-one-disk cold-restart test.
- The coalescing test recovers the synced simulated disk but does not power-crash it first. It checks recovered state and one hard-state record. The injected failure test supplies the independent check that a skipped fsync would not pass.
- The cluster simulator still drives core.Node directly. Passing its seeds does not exercise this driver's gate or channels.

`contracts.md` gives source-backed invariants and the missing fault/check schedules. These limits do not block a measured prototype comparison; they block claims that this change implements the complete storage-stall and simulation design.
