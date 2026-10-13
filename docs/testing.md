# Planted bugs

Each planted bug switches on a deliberate defect behind the `planted` build tag (`internal/planted`). CI runs one `planted` job per bug: `TestPlanted -planted=<name>` runs the bug's configuration from seed 1 until the run fails. A run counts as a catch only when its failure matches the bug's expected assertion. Any other failure, and any crash, fails the job, and so does a budget with no catch. Every configuration also runs without the bug in `TestSeeds` or `TestStalledLeaderIsReplaced`, where it must pass. The inventory, budgets and assertions come from the semantics pre-work on #47. `TestPlantedInventory` checks that this list, the test cases and the CI matrix name the same bugs.

| Bug | Defect | Expected assertion | Budget | Status |
| --- | --- | --- | --- | --- |
| B01a | Linearizable reads skip ReadIndex | A read returns an older revision than a call that finished before it started | 64 | planted; its CI job must catch it |
| B01b | A read is served before it is applied | Same as B01a | 64 | planted; its CI job must catch it |
| B02 | The leader's disk driver reports an append durable before its fsync | A node recovers less than it reported durable, or an erase-one-disk cold restart loses an applied entry | 128 | planted; its CI job must catch it |
| B03 | A follower's disk driver reports an append durable before its fsync | Same as B02 | 128 | planted; its CI job must catch it |
| B04a | WAL recovery accepts a record whose checksum fails | Recovery refuses the log (see deviations) | 256 | planted; its CI job must catch it |
| B04c | WAL recovery skips checksums, and a payload bit flips so that the record still decodes | Two nodes applied different entries at one index, or the state disagrees with the model or the final log | 256 | planted; its CI job must catch it |
| B04b | Torn-tail repair also cuts the last synced record | A node recovers less than it reported durable | 256 | planted; its CI job must catch it |
| B06a | A watcher drops the last event of a multi-key write | The watch history differs from the committed history | 32 | planted; its CI job must catch it |
| B06b | A watcher gets the first event of every write twice | Same as B06a | 32 | planted; its CI job must catch it |
| B06c | Update and delete events lose their previous value | Same as B06a | 32 | planted; its CI job must catch it |
| B06d | A progress notification goes out ahead of the events it covers | An event at or below a progress revision arrives after that progress | 32 | planted; its CI job must catch it |
| B07 | A new watch starts one revision early | A watch gets an event older than one it already got | 64 | planted; its CI job must catch it |
| B08 | Compaction drops the tombstone at the compaction revision | Same as B06a | 32 | planted; its CI job must catch it |
| B09a | Each changed key of a write gets its own revision | A read or the replayed log disagrees with the model | 16 | planted; its CI job must catch it |
| B09b | A recreated key keeps its old version and create revision | Same as B09a | 16 | planted; its CI job must catch it |
| B10 | Followers expire leases on their own clock | Nodes that applied the same log hold different states, a read disagrees with the model, or a watch sees a delete no replicated expiry made | 64 | planted; its CI job must catch it |
| B11 | The disk driver treats a failed fsync as success | A node recovers less than it reported durable, or recovery refuses the log | 64 | planted; its CI job must catch it |
| B12 | The WAL keeps going after a short write | Same as B11 (see deviations) | 256 | planted; its CI job must catch it |
| B15 | The history treats a timed-out call as certainly aborted | The durability checker finds a committed write that no client sent | 32 | planted; its CI job must catch it |
| B17 | The stalled-disk watchdog never fires | The stalled leader never stops | 64 | planted; its CI job must catch it |
| B18 | Closing a watch stream leaves its watchers | A node answers a stream that it closed, or holds watchers of closed streams | 16 | planted; its CI job must catch it |
| B05 | A snapshot becomes authority before its files are synced | Cold recovery after a crash at the install boundary is consistent | 128 | deferred: there are no snapshots yet |
| B13 | A slow watcher blocks apply | Healthy reads and writes finish within 10 election timeouts | 64 | deferred: the watch queues live in `internal/node`, which the simulator does not run |
| B14 | A peer writer has no write deadline | The deadline drops the stalled writer and the quorum commits | 64 | deferred: the simulator does not run `internal/peer` |
| B16 | An old segment is unlinked before its replacement is synced | Every recoverable root resolves to verified bytes | 128 | deferred: there is no segment cleaning |

## Deviations from the pre-work

The owner approved these on #47:

- **B04a.** With checksums skipped, the record decoder still rejects the random bytes of a torn record, so the bug shows as recovery refusing the log, not as invented state. B04c covers invented state: under it, the damage a crash leaves is one flipped payload bit in an entry, which still decodes, and recovery accepts a record that fails its checksum only if it still decodes cleanly. The node is not marked damaged, so the run must catch what it applies.
- **B12.** The pre-work's bug is recovery accepting a partial append after ENOSPC. The plant is the write side: the WAL ignores the short write and keeps appending after the partial bytes. That is caught as acknowledged data that recovery does not return, which is the pre-work's assertion.
- **B17.** The pre-work's bug holds the core across a stalled fsync. The core never waits for the disk; the driver does. The plant switches off the leader's stalled-disk watchdog, and the assertion is the same: the stalled leader must stop.
- There is no forced seed-0 schedule; each bug runs the full fault mix from seed 1.

B05, B13, B14 and B16 follow in a later pull request, and #47 does not close without them. Any of them that targets code this design lacks is replaced by the nearest real bug of the same class.
