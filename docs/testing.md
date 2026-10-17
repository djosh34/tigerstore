# Planted bugs

Each planted bug switches on a deliberate defect behind the `planted` build tag (`internal/planted`). The `planted` job of `planted.yml` runs every bug in turn: `TestPlanted -planted=<name>` in `internal/sim`, `internal/node` and `internal/peer`. `planted.yml` runs on the push to `main`, nightly, and on a pull request that changes `check/`, `internal/sim/`, `cmd/chaos/`, `chaos/` or `planted.yml` itself; it is not part of the required check `ci`. In the simulator it runs the bug's configuration from its first seed until the run fails. A run counts as a catch only when its failure matches the bug's expected assertion. Any other failure, and any crash, fails the job, and so does a budget with no catch. Every configuration also runs without the bug in `TestSeeds`, `TestStalledLeaderIsReplaced`, `TestSlowWatcherIsCut` or `TestStalledPeerIsDropped`, where it must pass. The inventory, budgets and assertions come from the semantics pre-work on #47. `TestPlantedInventory` checks that this list, the test cases and the job's `BUGS` list name the same bugs.

| Bug | Defect | Expected assertion | Budget | Status |
| --- | --- | --- | --- | --- |
| B01a | Linearizable reads skip ReadIndex | A read returns an older revision than a call that finished before it started, and the witness checker fails the same seed in both modes | 64 | planted; its CI job must catch it |
| B01b | A read is served before it is applied | Same as B01a, the witness checker included | 64 | planted; its CI job must catch it |
| B02 | The leader's disk driver reports an append durable before its fsync | A node recovers less than it reported durable, an erase-one-disk cold restart loses an applied entry, or, with a lost vote or term, two nodes apply different entries | 128 | planted; its CI job must catch it |
| B03 | A follower's disk driver reports an append durable before its fsync | Same as B02 | 128 | planted; its CI job must catch it |
| B04a | WAL recovery accepts a record whose checksum fails | Recovery refuses the log (see deviations) | 256 | planted; its CI job must catch it |
| B04c | WAL recovery skips checksums, and a payload bit flips so that the record still decodes | Two nodes applied different entries at one index, the state disagrees with the model or the final log, or a node starts on a disk with a flipped value without noticing | 256 | planted; its CI job must catch it |
| B04b | Torn-tail repair also cuts the last synced record | A node recovers less than it reported durable, or, when the cut record was a vote, two nodes applied different entries at one index | 256 | planted; its CI job must catch it |
| B06a | A watcher drops the last event of a multi-key write | The watch history differs from the committed history | 32 | planted; its CI job must catch it |
| B06b | A watcher gets the first event of every write twice | Same as B06a | 32 | planted; its CI job must catch it |
| B06c | Update and delete events lose their previous value | Same as B06a | 32 | planted; its CI job must catch it |
| B06d | A progress notification goes out ahead of the events it covers | An event at or below a progress revision arrives after that progress | 32 | planted; its CI job must catch it |
| B07 | A new watch starts one revision early | A watch gets an event older than one it already got | 64 | planted; its CI job must catch it |
| B08 | Compaction drops the tombstone at the compaction revision | Same as B06a | 128 | planted; its CI job must catch it |
| B09a | Each changed key of a write gets its own revision | A read or the replayed log disagrees with the model | 16 | planted; its CI job must catch it |
| B09b | A recreated key keeps its old version and create revision | Same as B09a | 16 | planted; its CI job must catch it |
| B10 | Followers expire leases on their own clock | Nodes that applied the same log hold different states, a read disagrees with the model, or a watch sees a delete no replicated expiry made | 64 | planted; its CI job must catch it |
| B11 | The disk driver treats a failed fsync as success | A node recovers less than it reported durable, or recovery refuses the log | 64 | planted; its CI job must catch it |
| B12 | The WAL keeps going after a short write | Same as B11 (see deviations) | 256 | planted; its CI job must catch it |
| B15 | The history treats a timed-out call as certainly aborted | The durability checker finds a committed write that no client sent | 32 | planted; its CI job must catch it |
| B17 | The stalled-disk watchdog never fires | The stalled leader never hands over, or calls time out under the liveness rule, as followers forward writes to the stalled leader | 64 | planted; its CI job must catch it |
| B18 | Closing a watch stream leaves its watchers | A node holds watchers of closed streams | 16 | planted; its CI job must catch it |
| B19 | Removing old segments drops one that recovery from the durable checkpoint still needs | Recovery refuses the log, a value read finds its entry gone, or a node never catches up | 64 | planted; its CI job must catch it |
| B20 | Entries leave memory above the applied index | A node holds entries in memory from past its applied index plus one | 64 | planted; its CI job must catch it |
| B21 | A checkpoint names the committed index instead of the applied one | A node's state at an index differs from the record, or two nodes write different checkpoints at one index, or a checkpoint file is written twice | 64 | planted; its CI job must catch it |
| B22 | A follower installs a snapshot without checking it | After a damaged stream: two nodes applied different entries or reached different states at one index, a value does not match its checksum, an entry does not decode, or the node cannot recover its log | 256 | planted; its CI job must catch it |
| B23 | A restart replays the log one entry short of its commit index | Two nodes applied the same log to different states, or a node never catches up | 64 | planted; its CI job must catch it |
| B24 | The index names a value one byte after where it is in its Raft entry | A node stops on a value that does not match its checksum | 16 | planted; its CI job must catch it |
| B25 | Value reads skip the checksum | After a bit of a value flips on a running node: a read or a watch event returns the damaged value | 64 | planted; its CI job must catch it |
| B27 | The core goes on after a snapshot install instead of starting again from the log | Two nodes applied the same log to different states, a cold restart does not converge, or the node never catches up, as the node driver's disk goroutine ends at an install | 64 | planted; its CI job must catch it |
| B28 | MemberUpdate acknowledges a change without updating the peer URLs | The membership response disagrees with the requested URLs, during faults or after healing | 16 | planted; its CI job must catch it |
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
- There is no forced seed-0 schedule; each bug runs the full fault mix from its listed first seed (seed 1 unless stated otherwise).

B23 and B27 were in the in-place snapshot install, which #238 replaced with a restart from the log: B23 stopped the install one entry short, and B27 let a checkpoint finished during an install ask for its root (#182), which now never lands, as a driver writes nothing after an install. Each became the nearest real bug of the restart. B05 and B16 targeted code that did not exist then (snapshots, segment cleaning), so, as the owner decided, each was replaced by the nearest real bug in the WAL. Snapshots and the removal of old segments now exist, with B19 to B23 of their own (#143); cleaning comes with #144. For B05 that is the same class: new on-disk state, here a segment, used before its metadata is synced. B16's class is destroying durable data before its replacement is durable. Its instance here is removing a segment before the root that makes it obsolete is synced, and the removal of old segments came later, with a bug of its own (B19). So B16 plants a step that existed when it was written, rotation without the old segment's last sync, which is closer to the B02/B11 class of reporting data durable without an fsync. B13 and B14 are in `internal/node` and `internal/peer`, which the simulator does not run; each of those packages has its own `TestPlanted` with one fixed scenario, and it counts only the scenario's own detecting failure as a catch.

# Real-process chaos

The `chaos` job in CI runs `cmd/chaos` on three `ubuntu-24.04-arm` runners with two seeds each, on one runner for a pull request. The seeds come from the run ID, and the job prints them. It is part of #49. One run does this:

1. It starts a fresh 3-node cluster of `cmd/tigerstore` processes with client and peer TLS and client certificates.
2. It puts one seeded proxy (`chaos/proxy`) on every link: one for each client-to-node connection and one for each of the six directed node-to-node links. Node i's `--initial-cluster` names each peer at the proxy of the link from i to that peer, so every Raft message crosses a proxy. The lists therefore differ between nodes, and so do the cluster IDs in response headers; nothing checks those. The proxies relay the TLS bytes without decrypting them.
3. Three stock clientv3 v3.7.2 clients, each pinned to one node through its proxy, send Put, CAS Txn, DeleteRange and Range calls on six hot keys, 10 calls per second each. A fourth client per node sends only linearizable Range calls on the hot keys at the same rate: a writer on a lagging follower spends the lag waiting for each write, while the reader keeps asking that follower. Three watches from revision 1 ask for previous values and progress notifications. This recorder comes from the correctness round 1 harness on #49. A failed call is recorded as ambiguous unless the error is a clear rejection.
4. It injects each fault of the table below once, in a seeded order. Each fault holds for 1.5 to 3.5 s, with 1 to 2 s between faults. A fault aimed at the leader or a follower picks its target when it starts.
5. It heals every fault, stops the random calls, and waits until all three nodes name one leader and report one revision. Then it waits 8 s, longer than the server's 7 s request timeout, writes barriers, reads the final state from every node as the target revision, and waits until every watch has an event or a progress notification at or past the target. A watch that resumed after the last write starts above the target and gets no progress notification, from tigerstore or etcd, so an event at the target counts too.
6. It stops the nodes and reads the committed log from private copies of their WALs; the committed prefixes must agree. It runs `check.Linearizable`, `check.Witnessed` in both modes, `check.Watches`, `check.Durable` and `check.Revisions` on the history. Each checker also fails if the history has no log, no watches, or a target where the log does not end, or if the coverage floor is missed: both CAS outcomes, a multi-key delete, watch puts, deletes and previous values, and successful calls from every client.

The `chaos-planted` job of `planted.yml` shows the job can fail: it builds tigerstore with planted bug B01a (a linearizable read served like a serializable one), B06c (watch events without previous values) or B09a (each key of a write gets its own revision), runs seeds 1 and 2 for each pair of bug and checker in turn, and passes only if the matching checker (Linearizable and the witness checker in each mode for B01a, Watches, Revisions) reported its own finding (`checker_error` in `checks.json`), not only a shared precondition such as the coverage floor. Durable has no planted case yet: SIGKILL keeps the page cache, so the bugs that lose unsynced writes (B02, B03, B11) need the file-fault layer. Left for later pieces: a delay longer than the election timeout (1 s) and a both-way cut of every peer link of one node (`proxy.Cut` with `Both` exists but is not scheduled), and tying a fault on a client link to a write from that link's own client. The run fails if a fault it installed overlaps no write: a Put, Delete or Txn whose call ran between the fault's install and its heal, so peer heartbeats or a signal to an idle node cannot count as the workload seeing the fault. Before Porcupine runs, the linearizability input leaves out ambiguous writes that did not take effect: those whose request the committed log holds no more often than successful calls sent it. Porcupine keeps an ambiguous write open until the end of the history, and in the first CI run two seeds, each with 66 to 79 ambiguous calls, timed out after 60 s; with this rule they check in 2 ms. Leaving out an ambiguous call can only make the check stricter. The other checkers get the whole history.

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

## The environment of `-env`

With `-env`, a run gives each node an environment of its own, through `sudo -n` (`chaos/env`). Each node gets its own disk: a file on a loop device, under a device-mapper target that is linear when healthy, then a small ext4 file system (512 MiB) that fills for real. With `-lazyfs <binary>` ([Jepsen's LazyFS](https://github.com/dsrhaslab/lazyfs), which `scripts/lazyfs.sh` builds at a pinned commit) on top, a write stays in LazyFS's cache until an fsync covers it, so a power cut loses exactly what no fsync covered. Each node also runs in a cgroup of its own, with the cpu, memory and io controllers. A prio qdisc on `lo` puts every proxy port behind a netem qdisc of its own: each peer link gets a seeded one-way delay of 1 to 10 ms (`latency.json`), and client links get none. The nodes run with `--unsafe-window-bytes 16384 --unsafe-checkpoint-bytes 65536 --unsafe-segment-bytes 32768`, which are for tests only. With them, a follower stopped for a few seconds falls behind the window and needs a snapshot, and checkpoints and segment removal come often. During the run, every log file except spares is archived every 100 ms. The checkers read each node's own log plus the archived segments below its oldest one, which the node removed below a checkpoint. So the log they see still starts at entry 1.

LazyFS answers an fsync with success even when the disk under it failed the write or is full. A node on it would be told that data is durable when it is not. Its fsync also only writes to the disk under it and never syncs that disk, so a slow or stalled disk does not hold the node's fsync. So `disk-slow`, `disk-stall`, `fsync-fail` and `disk-full` run only without `-lazyfs`, and `power-loss` and `torn-write` only with it. The `chaos-env` job of `ci` runs one seed in each mode on every PR. Its lazyfs row first runs the tests of `chaos/env` itself, `CHAOS_LAZYFS=<binary> go test -tags chaosenv ./chaos/env/`. `chaos-campaign.yml` runs nightly, and by hand with `gh workflow run chaos-campaign.yml`: 20 jobs of three seeds, half of them in each mode.

| Fault | What the run does | Evidence that it took effect |
| --- | --- | --- |
| `netem-loss`, `netem-reorder`, `netem-corrupt` | A busy peer link loses 20% of its packets, reorders half of them, or corrupts 5%, on top of its delay | The link's qdisc sent packets during the hold; for loss, it dropped some |
| `cpu-gray` | A node, the leader in a third of the runs, is limited to 2 to 5% of one CPU, while a client reads through it without pause | `nr_throttled` of its cgroup grew |
| `disk-slow` | A node's disk delays reads by a quarter of 100 to 400 ms and writes by all of it | The node logged a write slower than a tick |
| `disk-stall` | A node's disk takes no I/O for the hold | The node logged a write slower than a tick once it healed |
| `fsync-fail` | Every write to a node's disk fails; after the hold the disk is healed, `e2fsck` runs (ext4 may have gone read-only), and the node starts again | The node's log names an I/O error or a read-only file system |
| `disk-full` | A file takes every free byte of a node's ext4; it goes after the hold, and the node starts again | The node's log names `no space left on device` or NOSPACE |
| `power-loss` | SIGKILL, and LazyFS drops every write no fsync covered; restart after the hold | LazyFS's log says the cache is cleared |
| `torn-write` | The next write to a node's newest segment keeps its first third, and LazyFS crashes, as a power cut mid-write would; the node is killed, LazyFS mounted again and the node started | LazyFS crashed |
| `crash-in-recovery` | Five times: drop the caches, start the node on a disk slowed to 50 ms a read, and kill it 5 to 100 ms after it logged `recovering the log`; then start it on a healthy disk | At least one kill came after `recovering the log` and before `recovered the log` |
| `crash-in-install` | A follower is stopped while 300 writes of 1 KiB go through the leader; once resumed it fetches a snapshot, and it is killed during the fetch, then started again | The node logged `fetching the snapshot` and not `installed the snapshot` before the kill |
| `barely-alive-client` | A client opens a watch with previous values and then stops reading its connection for the hold, during writes | Once it read again, the watch delivered every revision from its start up to a read through another node, or was canceled; a revision it skipped fails the run |
| `several-at-once` | `cpu-gray` on one node and `netem-loss` on a leader link to another, at once | Both took effect |
| `replace-member` | Last in every run. Either every node is killed (under LazyFS, a power loss that drops what no fsync covered), one disk is erased and the other two start again from theirs, or one node's oldest segment gets a corrupt byte and its restart refuses the log. Then the member is removed through a healthy node, added as a new member and started on an empty disk with `--initial-cluster-state existing` (#161) | The two nodes elected a leader within 10 election timeouts, or the restart refused the log; the cluster converged with the new member |

Gray faults are every fault above but `crash-in-install`, `several-at-once` and `replace-member`, whose erased case probes the two nodes left. During each one, a prober writes through the two healthy nodes (all three for `barely-alive-client`), one write every 500 ms, for at least 3 s and three writes. A write is retried, as a client retries, until it commits. Every write must commit within 10 election timeouts of its first try: the liveness bound of #47. The probes are part of the history, so every checker checks them.

Not covered yet: a delay on client links. The `delay` fault delays only the leader's link to one follower, so that a follower lags (B01a), and no fault delays a client link.

# Kubernetes storage tests

`apiservertest/run.sh <version>` runs the etcd3 storage tests of `k8s.io/apiserver` (`pkg/storage/etcd3/...` and `pkg/storage/testing`) against tigerstore, unchanged. The `apiserver` job in ci runs it for v0.35.0, v0.36.0 and v0.37.0 (Kubernetes 1.35 to 1.37) on the push to `main` and nightly, not on a pull request. The 1.37 tests use RangeStream (`TestGetListStreamMultiChunk`, `TestWatchChanSync/RangeStream`, `TestWatchChanSyncStream*`).

`apiservertest/prepare` replaces the module's `testserver/test_server.go`, which embeds etcd, with an adapter that keeps the version's own client recorders. It also changes the one `*embed.Config` in `store_test.go` to the adapter's `Config`. Every `RunEtcd` call starts a fresh 3-node tigerstore over TLS with client certificates through `apiservertest/factory`, because deleting keys would not reset revisions, history, compaction or leases. The factory passes the progress interval the watch tests ask for as `--watch-progress-notify-interval`, and `--unsafe-no-fsync`, as the version's own test server sets `UnsafeNoFsync`: `TestCompact` gives a request 100 ms, which one fsync on a shared runner can pass. `run.sh` fails if the tests still import `go.etcd.io/etcd/server/v3`.

To tell a slow runner from a slow store, the factory starts a 3-node etcd with the same certificates and layout when `ETCD` names its binary, and the `apiserver-compare` workflow (started by hand) runs chosen tests many times against tigerstore and etcd v3.7.2 in turn in one job. It fails if a round ran no test.

No test is skipped. A test that cannot apply to a non-etcd server would be listed here with the reason; none is so far.

# Fuzz tests, coverage and the race detector

The `fuzz` job in CI runs each fuzz test of a hand-written decoder for 20 s, one after the other, from its saved corpus in the package's `testdata/fuzz`:

| Fuzz test | Decoder | What it requires |
| --- | --- | --- |
| `core.FuzzDecodeAlias` | `DecodeAlias` for transactions, deletes and ranges | The message `proto.Unmarshal` gives, or an error from both |
| `core.FuzzParsePut` | `ParsePut` | The fields, the size and the error of `proto.Unmarshal` |
| `core.FuzzReceiveSnapshot` | The follower's snapshot receiver | No crash, and a log it took opens again |
| `wal.FuzzOpen` | WAL recovery of two segments | No crash, and every entry it reports reads back as that entry or fails |
| `mvcc.FuzzLoad` | `Store.Load` of a checkpoint's keys | No crash, and a store it loaded answers a keys-only range over every key |
| `peer.FuzzFrames` | The peer transport's frame reader | It stops with an error, never crashes, and delivers only the peer's own messages |
| `server.FuzzHTTP2` | The server's HTTP/2 and gRPC framing, on a real one-node server | No crash; within 5 s the server answers and closes the connection, or resets it |

A failure's input is uploaded as an artifact; copied into `testdata/fuzz/<test>/`, it reproduces with `go test -run '<test>/<input>' <package>`.

The `coverage` job fails when the statement coverage of the `internal/...` packages, measured across every package's tests (`-coverpkg=./internal/...`), is below 80%. It was 86.8% when the job was added.

The `test` job runs every package but the simulator with `-race`; `race-sim` runs the simulator with it, with every node's goroutines, in its own job on the push to `main` and nightly. Three jobs run real processes built with `-race`, where a data race stops the process (`GORACE=halt_on_error=1`) and a race report in a log fails the run, also one at shutdown: the third `difftest` job (`RACE=1 scripts/difftest.sh tigerstore`, which builds the tests with `-race` too), the race row of `apiserver` (`RACE=1 apiservertest/run.sh v0.37.0`), and `chaos-race`, which runs two chaos seeds that the `chaos` job does not, with race-built `tigerstore` and `chaos` binaries. `chaos-race`, like `apiserver`, runs on the push to `main` and nightly.

`core.TestMembershipChangesMatchRaft` is the component test of the membership checks: random member adds, learner adds, removals, promotions and updates on random clusters. Every change the checks let through must be one raft's `confchange` takes, since raft panics on any error, and the members after it must equal raft's configuration; a refused change goes to raft as a change of nobody, as in etcd.

# Differential fuzzing against etcd

`scripts/difffuzz.sh` starts a fresh etcd v3.7.2 and a fresh 3-node tigerstore side by side, both with client TLS, and runs the difftest's `TestDiffFuzz` against both. It sends random sequences of 30 requests, each under keys of its own: puts with leases, previous values and the ignore flags; reads of one key, of ranges and of everything from a key, at the current, past, future, zero and negative revisions, with limits and count- or keys-only; deletes; transactions with up to two compares on every target and up to three puts, deletes and reads in each branch; compactions; lease grants with picked and asked-for IDs and revokes, also of leases nobody granted; and watches from past revisions. Every answer is compared as the difftest records it: revisions relative to the sequence's start, lease IDs by the order the sequence granted them, values byte for byte, transaction results, and error codes and messages. A mismatch is shrunk, by removing ever smaller runs of requests while one remains, and saved as a named case in `difftest/testdata/fuzz`, which `TestDiffFuzzFound` replays on every run.

The `difffuzz` job of `ci` fuzzes for 90 s from a seed of the run ID. `difffuzz.yml` is the long run, started by hand (`gh workflow run difffuzz.yml -f duration=2h`): four jobs with seeds of their own. With planted bug B09a built in (`BUILD_FLAGS='-tags planted -ldflags=-X=github.com/djosh34/tigerstore/internal/planted.Active=B09a'`), the first sequence differed, and its case shrank to 6 requests.

The first long run on `main` ([38909081059](https://github.com/djosh34/tigerstore/actions/runs/38909081059), 2 hours) found two things:
- Three of its four shards found #203 within 5 minutes: the watch event of a delete that follows a put of the same key in one Txn carried the put's value as its previous value. etcd sends the key as it was before the Txn. Their shrunk cases are in `difftest/testdata/fuzz`.
- The fourth shard stopped etcd v3.7.2 itself. The shrunk sequence: a Txn branch deletes a key and then puts it again, then a compaction at that Txn's revision. After it, etcd's index still has the put but its backend does not, so the next read of the key ends etcd with "range failed to find revision pair". tigerstore answers it. Kubernetes never deletes and puts a key in one Txn, so the generator no longer puts a key that an earlier delete of the same branch removed (`TestDiffFuzzNoPutAfterDelete`). `scripts/difffuzz.sh` takes `FUZZ_ETCD_PORT` for a second run beside the first, and a failing run names the seed of its last sequence.

# Membership and metadata recovery

`sim.TestMembershipFaults` adds a fourth member as a learner, updates its peer URLs, promotes it and removes it. KV clients and watches continue through the simulator's disk, link, process and clock faults, sparser than in the KV runs over a fault phase of 60 s: as etcd's, a node refuses a member change unless it has been connected to the other voters for 5 s. The run retries ambiguous changes after a linearizable member list. A separate oracle checks the returned members against the requested changes, including a mistake shared by every replica. The KV history still runs through durability, revision, watch, Porcupine and both witness checkers. Each operation must complete during the fault phase in the seed set. Permanent corruption is disabled in these runs; the ordinary three-voter seeds retain it. The erase-one-disk checks still run during membership changes. Cold copies preserve member IDs and the run file, but not a process's volatile removal notice. Recovery must read its disk; a peer's transport can then tell a member that missed its removal to stop when it connects.

`TestMembershipColdRestart` keeps named membership recovery seeds. `core.TestRemovedLeaderDrainsCommit` covers [#211](https://github.com/djosh34/tigerstore/issues/211).

`node.TestMetadataAfterCleaning` holds a follower down while membership, users, roles, permissions, auth enablement and both alarms change. The real cleaner must copy a cold value to a pack and remove every entry through those changes on both donors. The test checks each entry is absent, restarts both donors, then starts the old follower and requires a snapshot fetch. It checks the members, users, permissions, alarms and cold value after donor restart, follower catch-up and another follower restart. These are real node drivers and files with an in-process peer transport, not separate server processes.

# Simulator seed campaign

`sim-campaign.yml`, started by hand (`gh workflow run sim-campaign.yml -f first=<seed>`), runs `TestSeeds` on 100,000 seeds from `first`: 20 jobs of two processes of 2,500 seeds, every checker on each, the witness checker in both modes among them. Each process of at least 64 seeds also runs the deliberate stalled-leader handoff and gray-leader stop scenarios. Their watchdog deadlines and client-liveness checks must pass. Each process of 2,000 seeds or more must also have at least one of each in its random seeds; CI's shards of 400 seeds are too small for that rule (#183, #218).

# Soak

`soak.yml`, started by hand on `main` (`gh workflow run soak.yml --ref main`), runs `cmd/soak` for 5 hours: three tigerstore processes, each in a cgroup of its own with a 640 MiB memory limit and a 3 GiB disk budget, under 600,000 Kubernetes-shaped objects. They hold about 0.9 GB of values. The preloaded events expire with their leases after 2 minutes, which leaves about 0.6 GB: still more than the value cache's share of the limit, so reads miss the cache.

Sixteen workers make 400 calls per second in loadgen's mixed proportions:
- creates and updates in Txns that compare the create or mod revision, as the apiserver does;
- deletes;
- linearizable reads and paginated lists, as Range calls;
- events on leases of 120 s that expire, at most 1,000 on a lease and a new lease every minute, as the apiserver reuses them.

Alongside them run a watch on every resource prefix, with progress requests, and a compaction every 5 minutes to the revision of the one before.

Each worker owns its objects and makes one call at a time, so it knows what every answer must hold:
- An ambiguous write is resolved by the next read of the object.
- A compare that fails, a read or a list that differs from the model, and a header below an answer that came before the call are findings.
- Every watch must send exactly the changes the workers made at the revisions it covers.

The faults:
- A node is killed every 4 minutes on average, half the time the leader, and started again after 1 to 10 s.
- Every 30 minutes on average, a follower is stopped while 256 MiB of writes go in, and it must fetch a snapshot when it resumes. The workers' client leaves it out while it is stopped: a call waiting in a stopped node's socket applies when the node resumes, after the worker gave up and read the object, so the model would take it for a write no one made.

After each fault:
- after a kill, the other two nodes must commit a write within 10 s;
- a restarted or resumed node must commit a write through itself within 2 minutes, which covers its recovery or snapshot install;
- the nodes must agree on `HashKV` at one revision.

No node may exit on its own or be killed by its memory limit, no alarm may go off, and a data directory may not pass its budget by more than 256 MiB.

The run writes `report.json`: the calls, their errors and ambiguous answers, watch events, peak memory and disk of each node, its bytes cleaned and its cache hits and misses since its last start, and the longest recovery and catch-up. A soak against planted B01a (stale reads) and against B09a (one revision per key of a write) fails within 3 minutes.

# More data than memory

`past-ram.yml` checks that tigerstore holds more live values than its memory limit (#144). The required `ci` gate calls it. A pull request runs only its `storage` job; the push to `main`, the nightly run and a manual dispatch (`gh workflow run past-ram.yml --ref <branch>`) also run the process measurements.

## Storage checks

- `sim.TestPastRAMRecovery` runs 16 simulator seeds with a small value cache and all six checkers. On each node's durable disk it then copies the values the durable checkpoint names out of the old segments into a pack, the way the cleaner does, and removes those segments. It crashes after the pack sync, with the root buffered, after the root sync and after the removal. At each point it checks that every checkpoint value is still there, that the original segment files are gone, and that a recovered core matches a model replayed from the simulator's own committed history, not from the disk under test. Seeds must have cache hits and cold reads.
- `node.TestConcurrentCleanerSimulation` runs the real node driver and paced cleaner on a simulated disk under `testing/synctest`, for 8 seeds that change the interleaving of file calls. Clients overwrite and compact, readers hold cold values and snapshots are served while the cleaner packs and removes segments. Copies of the durable disk taken at each cleaner step must resolve every value of their durable checkpoint, and a restart from the final crash copy must return every acknowledged value.
- `node.TestColdReaderDuringCleaning`, `core.TestColdReadEIO`, `core.TestColdReadsCompleteInReverse` and `core.TestCacheReferenceIdentity` cover a reader holding a value while its segment is cleaned, EIO on a cold read, delayed reads that complete out of order after an overwrite, and a cache that must not return an overwritten value at the same offset.
- `node.TestCleaningCycles` logs the bytes the cleaner copied per acknowledged Put. That counts pack bytes only: WAL, checkpoint, root and journal writes are not in it.

Three planted bugs belong to this: B29 removes segments before the root that replaces them is synced, B30 leaves one checkpoint value out of a pack, and B31 makes a cache lookup ignore the Raft index. B29 and B30 must fail seed 1 of the concurrent cleaner scenario with a lost value; B31 must fail the simulator with the cache on.

## Process measurements

Each capacity job runs three members of one store, tigerstore or etcd v3.7.2, through `scripts/past-ram.py` and the `cmd/pastram` driver. Every member gets its own cgroup with a 640 MiB `memory.max` that includes its page cache, no swap, and `GOMEMLIMIT=560MiB`. The driver preloads objects with the Kubernetes size histogram of `internal/k8ssize`, 64 Puts per Txn, and records the last acknowledged count; one more Txn may have an unknown outcome.

| Mode | What it does |
| --- | --- |
| `proof` | Preloads 3.1 times the limit in live values under `/registry/`, then runs 3 minutes of load: a bulk client of cold point reads and bounded LISTs checked against the generator, and recorded clients doing Put, Delete, Get, LIST, CAS Txns, leases, `prev_kv` watches and a compaction under `/proof/`. Member 2 is killed after 30 s and restarted after 5 s. Afterwards every bulk key is checked byte for byte through every member, and the six checkers run on the recorded history and the committed log decoded from the members' WALs. It reports throughput, latency and server CPU. |
| `max` | Grows the live object count until something fails, etcd at its 8 GiB quota and uncapped, tigerstore uncapped. It names what failed and the count. |
| `cache` | Reads after the proof's preload, 8 in 10 aimed at the first 1% of keys. One job keeps the value cache; the other builds tigerstore with the cache limit set to 0, so the memory goes to the kernel's page cache. Both report reads per second, p99, cache hits and the bytes read from disk. |

A job fails when a checker, a value check or the driver fails, on a time limit, and on an OOM kill in tigerstore's proof or a cache case. The `max` cases and etcd's proof can end in an OOM kill during preload; the script then reports `capacity-limit` or `reference-preload-limit`, with the count and the cgroup's OOM counters, and the job passes. That outcome is the measured limit, not a pass of the proof. A small etcd proof with a 16 MiB preload checks the recorder and the checkers against etcd and must pass. The `etcd-index` job measures etcd's index bytes per key with its own `treeIndex` and the same keys; tigerstore reports its index bytes in `/metrics`.

Results go on the issue that asked for them, not here.
