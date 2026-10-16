# Planted bugs

Each planted bug switches on a deliberate defect behind the `planted` build tag (`internal/planted`). CI runs one `planted` job per bug: `TestPlanted -planted=<name>` in `internal/sim`, `internal/node` and `internal/peer`. In the simulator it runs the bug's configuration from seed 1 until the run fails. A run counts as a catch only when its failure matches the bug's expected assertion. Any other failure, and any crash, fails the job, and so does a budget with no catch. Every configuration also runs without the bug in `TestSeeds`, `TestStalledLeaderIsReplaced`, `TestSlowWatcherIsCut` or `TestStalledPeerIsDropped`, where it must pass. The inventory, budgets and assertions come from the semantics pre-work on #47. `TestPlantedInventory` checks that this list, the test cases and the CI matrix name the same bugs.

| Bug | Defect | Expected assertion | Budget | Status |
| --- | --- | --- | --- | --- |
| B01a | Linearizable reads skip ReadIndex | A read returns an older revision than a call that finished before it started | 64 | planted; its CI job must catch it |
| B01b | A read is served before it is applied | Same as B01a | 64 | planted; its CI job must catch it |
| B02 | The leader's disk driver reports an append durable before its fsync | A node recovers less than it reported durable, or an erase-one-disk cold restart loses an applied entry | 128 | planted; its CI job must catch it |
| B03 | A follower's disk driver reports an append durable before its fsync | Same as B02 | 128 | planted; its CI job must catch it |
| B04a | WAL recovery accepts a record whose checksum fails | Recovery refuses the log (see deviations) | 256 | planted; its CI job must catch it |
| B04c | WAL recovery skips checksums, and a payload bit flips so that the record still decodes | Two nodes applied different entries at one index, the state disagrees with the model or the final log, or a node starts on a disk with a flipped value without noticing | 256 | planted; its CI job must catch it |
| B04b | Torn-tail repair also cuts the last synced record | A node recovers less than it reported durable, or, when the cut record was a vote, two nodes applied different entries at one index | 256 | planted; its CI job must catch it |
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
| B17 | The stalled-disk watchdog never fires | The stalled leader never hands over | 64 | planted; its CI job must catch it |
| B18 | Closing a watch stream leaves its watchers | A node answers a stream that it closed, or holds watchers of closed streams | 16 | planted; its CI job must catch it |
| B19 | Removing old segments drops one that recovery from the durable checkpoint still needs | Recovery refuses the log, or a value read finds its entry gone | 64 | planted; its CI job must catch it |
| B20 | Entries leave memory above the applied index | A node holds entries in memory from past its applied index plus one | 64 | planted; its CI job must catch it |
| B21 | A checkpoint names the committed index instead of the applied one | A node's state at an index differs from the record, or two nodes write different checkpoints at one index, or a checkpoint file is written twice | 64 | planted; its CI job must catch it |
| B22 | A follower installs a snapshot without checking it | After a damaged stream: two nodes applied different entries or reached different states at one index, a value does not match its checksum, an entry does not decode, or the node cannot recover its log | 64 | planted; its CI job must catch it |
| B23 | A snapshot install stops one entry short of Raft's snapshot | A node's state at an index differs from the record | 64 | planted; its CI job must catch it |
| B24 | The index names a value one byte after where it is in its Raft entry | A node stops on a value that does not match its checksum | 16 | planted; its CI job must catch it |
| B25 | Value reads skip the checksum | After a bit of a value flips on a running node: a read or a watch event returns the damaged value | 64 | planted; its CI job must catch it |
| B05 | Replaces the snapshot bug, as there are no snapshots: a new WAL segment takes records before its directory entry is synced | A node recovers less than it reported durable, an erase-one-disk cold restart loses an applied entry, or recovery refuses the log | 128 | planted; its CI job must catch it |
| B13 | The node driver waits for a slow watch stream instead of closing it | `internal/node`: beside a watch stream with a queue of 8 that is never read, 50 writes, then a Put and a Range, each finish within 10 election timeouts, and the stream is closed as too slow | one fixed scenario | planted; its CI job must catch it |
| B14 | The peer writer has no write deadline | `internal/peer`: through a proxy that stops reading one connection, the write deadline drops it and reports the peer unreachable, and a message sent after the stall arrives over a new connection. Without the bug, `TestStalledPeerLinkKeepsQuorum` in `internal/server` also stalls the link from node 1 to node 2 of a 3-node cluster and requires every write to commit within 10 election timeouts through the healthy quorum | one fixed scenario | planted; its CI job must catch it |
| B16 | Replaces the segment-cleaning bug, as there is no cleaning: WAL rotation moves to a new segment without the old one's last sync | Same as B05 | 128 | planted; its CI job must catch it |

## Deviations from the pre-work

The owner approved these on #47:

- **B13 and B14** are tested by one fixed scenario each instead of 64 seeds; B14 adds the 3-node quorum check.

- **B04a.** With checksums skipped, the record decoder still rejects the random bytes of a torn record, so the bug shows as recovery refusing the log, not as invented state. B04c covers invented state: under it, the damage a crash leaves is one flipped payload bit in an entry, which still decodes, and recovery accepts a record that fails its checksum only if it still decodes cleanly. The node is not marked damaged, so the run must catch what it applies.
- **B12.** The pre-work's bug is recovery accepting a partial append after ENOSPC. The plant is the write side: the WAL ignores the short write and keeps appending after the partial bytes. That is caught as acknowledged data that recovery does not return, which is the pre-work's assertion, or as a node that reads a value its log no longer holds and stops.
- **B04c and values.** A flipped payload bit can land in a value instead of an entry's other fields. Then the node stops when it reads the value and finds it does not match its checksum, which also counts: the node noticed. Since the members start the log (#147), seed 1 lands there.
- **B17.** The pre-work's bug holds the core across a stalled fsync. The core never waits for the disk; the driver does. The plant switches off the leader's stalled-disk watchdog, and the assertion is that the stalled leader must hand leadership over (it stopped before #156).
- There is no forced seed-0 schedule; each bug runs the full fault mix from seed 1.

B05 and B16 targeted code that did not exist then (snapshots, segment cleaning), so, as the owner decided, each was replaced by the nearest real bug in the WAL. Snapshots and the removal of old segments now exist, with B19 to B23 of their own (#143); cleaning comes with #144. For B05 that is the same class: new on-disk state, here a segment, used before its metadata is synced. B16's class is destroying durable data before its replacement is durable. Its instance here is removing a segment before the root that makes it obsolete is synced, and the removal of old segments came later, with a bug of its own (B19). So B16 plants a step that existed when it was written, rotation without the old segment's last sync, which is closer to the B02/B11 class of reporting data durable without an fsync. B13 and B14 are in `internal/node` and `internal/peer`, which the simulator does not run; each of those packages has its own `TestPlanted` with one fixed scenario, and it counts only the scenario's own detecting failure as a catch.

# Real-process chaos

The `chaos` job in CI runs `cmd/chaos` on three `ubuntu-24.04-arm` runners with two seeds each. The seeds come from the run ID, and the job prints them. It is part of #49. One run does this:

1. It starts a fresh 3-node cluster of `cmd/tigerstore` processes with client and peer TLS and client certificates.
2. It puts one seeded proxy (`chaos/proxy`) on every link: one for each client-to-node connection and one for each of the six directed node-to-node links. Node i's `--initial-cluster` names each peer at the proxy of the link from i to that peer, so every Raft message crosses a proxy. The lists therefore differ between nodes, and so do the cluster IDs in response headers; nothing checks those. The proxies relay the TLS bytes without decrypting them.
3. Three stock clientv3 v3.7.2 clients, each pinned to one node through its proxy, send Put, CAS Txn, DeleteRange and Range calls on six hot keys, 10 calls per second each. A fourth client per node sends only linearizable Range calls on the hot keys at the same rate: a writer on a lagging follower spends the lag waiting for each write, while the reader keeps asking that follower. Three watches from revision 1 ask for previous values and progress notifications. This recorder comes from the correctness round 1 harness on #49. A failed call is recorded as ambiguous unless the error is a clear rejection.
4. It injects each fault of the table below once, in a seeded order. Each fault holds for 1.5 to 3.5 s, with 1 to 2 s between faults. A fault aimed at the leader or a follower picks its target when it starts.
5. It heals every fault, stops the random calls, and waits until all three nodes name one leader and report one revision. Then it waits 8 s, longer than the server's 7 s request timeout, writes barriers, reads the final state from every node as the target revision, and waits until every watch has an event or a progress notification at or past the target. A watch that resumed after the last write starts above the target and gets no progress notification, from tigerstore or etcd, so an event at the target counts too.
6. It stops the nodes and reads the committed log from private copies of their WALs; the committed prefixes must agree. It runs `check.Linearizable`, `check.Watches`, `check.Durable` and `check.Revisions` on the history. Each checker also fails if the history has no log, no watches, or a target where the log does not end, or if the coverage floor is missed: both CAS outcomes, a multi-key delete, watch puts, deletes and previous values, and successful calls from every client.

The `chaos-planted` job shows the job can fail: it builds tigerstore with planted bug B01a (a linearizable read served like a serializable one), B06c (watch events without previous values) or B09a (each key of a write gets its own revision), runs seeds 1 and 2, and passes only if the matching checker (Linearizable, Watches, Revisions) reported its own finding (`checker_error` in `checks.json`), not only a shared precondition such as the coverage floor. Durable has no planted case yet: SIGKILL keeps the page cache, so the bugs that lose unsynced writes (B02, B03, B11) need the file-fault layer. Left for later pieces: a delay longer than the election timeout (1 s) and a both-way cut of every peer link of one node (`proxy.Cut` with `Both` exists but is not scheduled), and tying a fault on a client link to a write from that link's own client. The run fails if a fault it installed overlaps no write: a Put, Delete or Txn whose call ran between the fault's install and its heal, so peer heartbeats or a signal to an idle node cannot count as the workload seeing the fault. Before Porcupine runs, the linearizability input leaves out ambiguous writes that did not take effect: those whose request the committed log holds no more often than successful calls sent it. Porcupine keeps an ambiguous write open until the end of the history, and in the first CI run two seeds, each with 66 to 79 ambiguous calls, timed out after 60 s; with this rule they check in 2 ms. Leaving out an ambiguous call can only make the check stricter. The other three checkers get the whole history.

| Fault | What the run does | Evidence that it took effect |
| --- | --- | --- |
| `sigkill-leader`, `sigkill-follower` | SIGKILL, restart after the hold | The wait status says the process was killed by SIGKILL; the restart has a new pid |
| `sigstop-leader`, `sigstop-follower` | SIGSTOP, SIGCONT after the hold | `/proc/<pid>/stat` shows state T after SIGSTOP and another state after SIGCONT |
| `delay` | The leader's link to one follower delays each read by 300 to 800 ms, less than an election timeout, so the follower keeps its leader but applies behind | Proxy hits, one per delayed read |
| `switching-delay` | Every peer link to and from the leader switches between 0 to 2 ms and 20 to 80 ms of delay every 100 to 400 ms | Proxy hits |
| `stall` | One busy link stops reading and writing; the senders block, and the held bytes go out at the heal | Proxy hits |
| `drop` | One busy link deletes one byte of the first read, then of one read in four | Proxy hits |
| `corrupt` | Like `drop`, but flips one bit | Proxy hits |
| `duplicate` | Like `drop`, but repeats one byte | Proxy hits |
| `reset` | Every busy link of one node closes its open and new connections with RST | Proxy hits, one per connection |
| `one-direction-cut` | The leader's messages to one follower vanish; the follower's messages to the leader still arrive | Proxy hits |
| `slow-link` | The leader's link to one follower is limited to 8 to 32 KiB/s while its link to the other follower runs at full speed | Proxy hits |

Busy links are the ones that carry traffic: every client link, and the peer links to and from the leader. Followers do not send to each other, so a fault on a link between two followers would have nothing to act on. A peer link carries messages only forward, from the node that dialed, so peer faults act forward; a fault on a client link acts forward, backward or both, as the seed picks. A dropped, corrupted, duplicated or cut byte breaks the TLS stream, so the heal closes such connections with RST, and peers and clients reconnect.

A run fails if a fault was not installed (the proxy does not report the requested fault, or the signal failed), had no effect (no proxy hit during the hold, no SIGKILL in the wait status, no stopped state), or did not heal. It also fails if a node exited on its own, if the cluster does not converge within 30 s of the heal, if the final reads fail or a watch does not reach the target, or if a checker fails.

Each run writes its evidence to `<out>/seed-<seed>/`, pass or fail: `config.json`, the planned `schedule.json`, `faults.json` (for each fault the request, the installed proxy state, the state before and after the heal, pids and process states, and times), `proxy-events.json`, `chaos.log`, the node logs and data directories, `wal.json`, `history.json`, `watch-responses.json`, `coverage.json`, `checks.json` and `verdict.json`. When the job fails it uploads all of it as the `chaos-evidence-<shard>` artifact. A seed reproduces with

    go build -o /tmp/tigerstore ./cmd/tigerstore
    go run ./cmd/chaos -tigerstore /tmp/tigerstore -seed <seed>

and `-faults` runs a subset. `go run ./cmd/chaos -check <dir>/history.json` reruns only the checkers on a saved history. A seed fixes the schedule, the proxies' choices for each connection and the workload's calls. It does not fix kernel timing or the order of connections, so a rerun injects the same faults in the same order but does not repeat the interleaving.

Not covered yet; these are later pieces of #49:

- A delay on client links: the `delay` fault delays only the leader's link to one follower, so that a follower lags (B01a), and no fault delays a client link.
- tc netem packet faults: loss, reordering and corruption, and 1 to 10 ms between nodes. Deleting stream bytes in the proxy is a different fault from packet loss, which TCP repairs.
- CPU throttling with cgroup quotas.
- Disk faults on real processes: a fault-injecting file layer with torn writes and failing or stalling fsync, and real ENOSPC.
- Crashes timed inside WAL recovery, and erase-one-disk cold restarts of real processes.
- Gray nodes and barely alive clients, with numeric liveness bounds.
- Several faults at once; the schedule injects one at a time.
- Nightly long campaigns as many short jobs.

# Kubernetes storage tests

`apiservertest/run.sh <version>` runs the etcd3 storage tests of `k8s.io/apiserver` (`pkg/storage/etcd3/...` and `pkg/storage/testing`) against tigerstore, unchanged. The `apiserver` job in ci runs it for v0.35.0, v0.36.0 and v0.37.0 (Kubernetes 1.35 to 1.37). The 1.37 tests use RangeStream (`TestGetListStreamMultiChunk`, `TestWatchChanSync/RangeStream`, `TestWatchChanSyncStream*`).

`apiservertest/prepare` replaces the module's `testserver/test_server.go`, which embeds etcd, with an adapter that keeps the version's own client recorders. It also changes the one `*embed.Config` in `store_test.go` to the adapter's `Config`. Every `RunEtcd` call starts a fresh 3-node tigerstore over TLS with client certificates through `apiservertest/factory`, because deleting keys would not reset revisions, history, compaction or leases. The factory passes the progress interval the watch tests ask for as `--watch-progress-notify-interval`. `run.sh` fails if the tests still import `go.etcd.io/etcd/server/v3`.

To tell a slow runner from a slow store, the factory starts a 3-node etcd with the same certificates and layout when `ETCD` names its binary, and the `apiserver-compare` workflow (started by hand) runs chosen tests many times against tigerstore and etcd v3.7.2 in turn in one job. It fails if a round ran no test.

No test is skipped. A test that cannot apply to a non-etcd server would be listed here with the reason; none is so far.
