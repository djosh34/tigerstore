# Planted bugs

Each planted bug switches on a deliberate defect behind the `planted` build tag (`internal/planted`). The `planted` job of `planted.yml` runs every bug in turn under gosim: `TestPlanted` with `SIM_PLANTED=<name>` in `internal/sim`, `internal/node` and `internal/peer`, as `SIM_PLANTED=<name> TAGS=planted scripts/sim.sh ./internal/sim '^TestPlanted$'` does. `planted.yml` runs on the push to `main`, nightly, and on a pull request that changes `check/`, `internal/sim/`, `internal/simrun/`, `scripts/sim.sh`, `tools/gosim/` or `planted.yml` itself; it is not part of the required check `ci`. In the simulator it runs the bug's configuration from its first seed until the run fails, each seed a gosim run of its own. The run that caught the bug is saved with its trace, history and disks, uploaded as the `planted` artifact, and its replay command printed. A run counts as a catch only when its failure matches the bug's expected assertion. Any other failure, and any crash, fails the job, and so does a budget with no catch. While #357 and #361 are open, a seed that fails as one of them does (`openBugs` in `internal/sim/planted_test.go`) catches nothing, unless the bug's expected assertion matches too and the seed passes without the planted bug; a seed that fails without the bug on anything else fails the job. Every configuration also runs without the bug in `TestSeeds`, `TestStalledLeaderIsReplaced`, `TestEachFaultTakesEffect` (B22's damaged snapshot), `TestLoneSegmentHeader` (B32), `TestSlowWatcherIsCut` or `TestStalledPeerIsDropped`, where it must pass, and the seed of every catch in the simulator runs again without the bug, as a gosim run of its own, where it must pass too: a catch counts only then. The inventory, budgets and assertions come from the semantics pre-work on #47. `TestPlantedInventory` checks that this list, the test cases and the job's `BUGS` list name the same bugs.

| Bug | Defect | Expected assertion | Budget | Status |
| --- | --- | --- | --- | --- |
| B01a | Linearizable reads skip ReadIndex | A read returns an older revision than a call that finished before it started, and the witness checker fails the same seed in both modes | 64 | planted; its CI job must catch it |
| B01b | A read is served before it is applied | Same as B01a, the witness checker included | 64 | planted; its CI job must catch it |
| B02 | The leader's disk driver reports an append durable before its fsync | A node recovers less than it reported durable or starts again with an older term or vote than it held durable, an erase-one-disk cold restart loses an applied entry, or, with a lost vote or term, two nodes apply different entries | 128 | planted; its CI job must catch it |
| B03 | A follower's disk driver reports an append durable before its fsync | Same as B02 | 128 | planted; its CI job must catch it |
| B04a | WAL recovery accepts a record whose checksum fails | Recovery refuses the log, or a node starts on a damaged disk without noticing (see deviations) | 256 | planted; its CI job must catch it |
| B04c | WAL recovery skips checksums, and a payload bit flips so that the record still decodes | Two nodes applied different entries at one index, the state disagrees with the model or the final log, or a node starts on a disk with a flipped value without noticing | 256 | planted; its CI job must catch it |
| B04b | Torn-tail repair also cuts the last synced record | A node recovers less than it reported durable, or, when the cut record was a vote, it starts again with an older term or vote than it held durable, or two nodes applied different entries at one index, or different states from one log | 256 | planted; its CI job must catch it |
| B06a | A watcher drops the last event of a multi-key write | The watch history differs from the committed history | 32 | planted; its CI job must catch it |
| B06b | A watcher gets the first event of every write twice | Same as B06a | 32 | planted; its CI job must catch it |
| B06c | Update and delete events lose their previous value | Same as B06a | 32 | planted; its CI job must catch it |
| B06d | A progress notification goes out ahead of the events it covers | An event at or below a progress revision arrives after that progress | 32 | planted; its CI job must catch it |
| B07 | A new watch starts one revision early | A watch gets an event older than one it already got | 64 | planted; its CI job must catch it |
| B08 | Compaction drops the tombstone at the compaction revision | Same as B06a | 128 | planted; its CI job must catch it |
| B09a | Each changed key of a write gets its own revision | A read or the replayed log disagrees with the model | 16 | planted; its CI job must catch it |
| B09b | A recreated key keeps its old version and create revision | Same as B09a | 16 | planted; its CI job must catch it |
| B10 | Followers expire leases on their own clock | Nodes that applied the same log hold different states, a read disagrees with the model, or a watch sees a delete no replicated expiry made | 64 | planted; its CI job must catch it |
| B11 | The disk driver treats a failed fsync as success | A node recovers less than it reported durable or starts again with an older term or vote than it held durable, or recovery refuses the log | 64 | planted; its CI job must catch it |
| B12 | The WAL keeps going after a short write | Same as B11 (see deviations) | 256 | planted; its CI job must catch it |
| B15 | The history treats a timed-out call as certainly aborted | The durability checker finds a committed write that no client sent | 32 | planted; its CI job must catch it |
| B17 | The stalled leader never goes silent | The stalled leader never goes silent, or calls time out under the liveness rule, as followers forward writes to the stalled leader | 64 | planted; its CI job must catch it |
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
| B32 | The WAL writes a new segment's header alone, before its first records (#295) | Recovery refuses the log after a crash during a snapshot install kept the records and lost the header (`TestLoneSegmentHeader`'s configuration) | 128 | planted; its CI job must catch it |

## Deviations from the pre-work

The owner approved these on #47:

- **B13 and B14** are tested by one fixed scenario each instead of 64 seeds; B14 adds the 3-node quorum check.

- **B04a.** With checksums skipped, the record decoder still rejects the random bytes of a torn record, so the bug shows as recovery refusing the log, not as invented state, or as a node that starts on a segment the simulator damaged on purpose, when the damage still decodes. B04c covers invented state: under it, the damage a crash leaves is one flipped payload bit in an entry, which still decodes, and recovery accepts a record that fails its checksum only if it still decodes cleanly. The node is not marked damaged, so the run must catch what it applies.
- **B12.** The pre-work's bug is recovery accepting a partial append after ENOSPC. The plant is the write side: the WAL ignores the short write and keeps appending after the partial bytes. That is caught as acknowledged data that recovery does not return, which is the pre-work's assertion, or as a node that reads a value its log no longer holds, or an entry whose record the short write damaged, and stops.
- **B04c and values.** A flipped payload bit can land in a value instead of an entry's other fields. Then the node stops when it reads the value and finds it does not match its checksum, which also counts: the node noticed. Since the members start the log (#147), seed 1 lands there.
- **B17.** The pre-work's bug holds the core across a stalled fsync. The core never waits for the disk; the driver does. The plant keeps a leader whose own disk is stuck sending to its peers, and the assertion is that the stalled leader must go silent so that the others elect another (#233; it handed over before, and stopped before #156).
- There is no forced seed-0 schedule; each bug runs the full fault mix from its listed first seed (seed 1 unless stated otherwise).

B23 and B27 were in the in-place snapshot install, which #238 replaced with a restart from the log: B23 stopped the install one entry short, and B27 let a checkpoint finished during an install ask for its root (#182), which now never lands, as a driver writes nothing after an install. Each became the nearest real bug of the restart. B05 and B16 targeted code that did not exist then (snapshots, segment cleaning), so, as the owner decided, each was replaced by the nearest real bug in the WAL. Snapshots and the removal of old segments now exist, with B19 to B23 of their own (#143); cleaning comes with #144. For B05 that is the same class: new on-disk state, here a segment, used before its metadata is synced. B16's class is destroying durable data before its replacement is durable. Its instance here is removing a segment before the root that makes it obsolete is synced, and the removal of old segments came later, with a bug of its own (B19). So B16 plants a step that existed when it was written, rotation without the old segment's last sync, which is closer to the B02/B11 class of reporting data durable without an fsync. B13 and B14 are in `internal/node` and `internal/peer`, which the simulator does not run; each of those packages has its own `TestPlanted` with one fixed scenario, and it counts only the scenario's own detecting failure as a catch.

# Real-process tests and the simulator

The deterministic simulator has a fault for every fault a real-process test injects ([#341](https://github.com/djosh34/tigerstore/issues/341); the table under "The simulator's faults" maps them). So the real-process tests are cut to what the simulator cannot cover: the nightly chaos run (`chaos-env.yml`), as the check that the simulated disk behaves as real ext4 does; the soak; the past-RAM process measurements; the fault-free differential tests and fuzzing against etcd v3.7.2; the Kubernetes tests; the rolling upgrade; the bench; the stress test; the race builds; and the static checks.

A failure in a real-process test is first reproduced in the simulator. If the simulator could represent it but cannot express it yet, the fault model gets the missing fault in the same fix, and the regression is a short named simulator scenario. A failure of what the simulator does not model, such as real memory and cgroups, the kernel and `internal/osfs`, speed, or two binaries, gets a short regression in the real test that found it, with the reason it cannot run in the simulator recorded beside it.

# Real-process chaos

`chaos-env.yml` runs one seed of `cmd/chaos -env` nightly on `ubuntu-26.04-arm`, and by hand with `gh workflow run chaos-env.yml [--ref <branch>] [-f seed=<seed>]`. It also runs on a pull request that changes `chaos/`, `cmd/chaos/` or `chaos-env.yml`. The seed comes from the run ID, and the job prints it. It is not part of the required check `ci`. It first runs the tests of `chaos/env` itself, `go test -tags chaosenv ./chaos/env/`. One run does this:

1. It starts a fresh 3-node cluster of `cmd/tigerstore` processes with client and peer TLS and client certificates.
2. It puts one seeded proxy (`chaos/proxy`) on every link: one for each client-to-node connection and one for each of the six directed node-to-node links. Node i's `--initial-cluster` names each peer at the proxy of the link from i to that peer, so every Raft message crosses a proxy. The lists therefore differ between nodes, and so do the cluster IDs in response headers; nothing checks those. The proxies relay the TLS bytes without decrypting them.
3. Three stock clientv3 v3.7.2 clients, each pinned to one node through its proxy, send Put, CAS Txn, DeleteRange and Range calls on six hot keys, 10 calls per second each. A fourth client per node sends only linearizable Range calls on the hot keys at the same rate: a writer on a lagging follower spends the lag waiting for each write, while the reader keeps asking that follower. Three watches from revision 1 ask for previous values and progress notifications. This recorder comes from the correctness round 1 harness on #49. A failed call is recorded as ambiguous unless the error is a clear rejection.
4. It injects each fault of the tables below once, in a seeded order. Each fault holds for 1.5 to 3.5 s, with 1 to 2 s between faults. A fault aimed at the leader or a follower picks its target when it starts.
5. It heals every fault, stops the random calls, and waits until all three nodes name one leader and report one revision. Then it waits 8 s, longer than the server's 7 s request timeout, writes barriers, reads the final state from every node as the target revision, and waits until every watch has an event or a progress notification at or past the target. A watch that resumed after the last write starts above the target and gets no progress notification, from tigerstore or etcd, so an event at the target counts too.
6. It stops the nodes and reads the committed log from private copies of their WALs; the committed prefixes must agree. It runs `check.Linearizable` if the history is short enough for it (`check.ShortEnough`, 4,000 operations; a run has about 8,000, so in practice it is skipped), `check.Witnessed` in both modes, `check.Watches`, `check.Durable` and `check.Revisions` on the history. Each checker also fails if the history has no log, no watches, or a target where the log does not end, or if the coverage floor is missed: both CAS outcomes, a multi-key delete, watch puts, deletes and previous values, and successful calls from every client.

Left for later pieces: a delay longer than the election timeout (1 s) and a both-way cut of every peer link of one node (`proxy.Cut` with `Both` exists but is not scheduled), and tying a fault on a client link to a write from that link's own client. A fault overlapped a write when a Put, Delete or Txn call ran between the fault's install and its heal; `faults.json` records that beside its effect, as peer heartbeats or a signal to an idle node do not show that the workload saw the fault. Before Porcupine runs, the linearizability input leaves out ambiguous writes that did not take effect: those whose request the committed log holds no more often than successful calls sent it. Porcupine keeps an ambiguous write open until the end of the history, and in the first CI run two seeds, each with 66 to 79 ambiguous calls, timed out after 60 s; with this rule they check in 2 ms. Leaving out an ambiguous call can only make the check stricter. The other checkers get the whole history.

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

A run fails if a fault was not installed (the proxy does not report the requested fault, or the signal failed) or did not heal. Whether it took effect (a proxy hit during the hold, SIGKILL in the wait status, a stopped state) and overlapped a write is recorded in `faults.json` as evidence; no run fails on it. A run also fails if a node exited on its own, if the cluster does not converge within 30 s of the heal, if the final reads fail or a watch does not reach the target, or if a checker fails.

Each run writes its evidence to `<out>/seed-<seed>/`, pass or fail: `config.json`, the planned `schedule.json`, `faults.json` (for each fault the request, the installed proxy state, the state before and after the heal, pids and process states, and times), `proxy-events.json`, `chaos.log`, the node logs and data directories, `wal.json`, `history.json`, `watch-responses.json`, `coverage.json`, `checks.json`, `nodes.txt` (how each node stood before the stop) and `verdict.json`. When a step fails, `chaos.log` also gets how each node stood then. When the job fails it uploads all of it but the disks as the `chaos-env-evidence` artifact. A failed run prints the command that runs it again, with every flag that shapes it; in short, a seed reproduces with

    go build -o /tmp/tigerstore ./cmd/tigerstore
    go run ./cmd/chaos -tigerstore /tmp/tigerstore -seed <seed> -env

and `-faults` runs a subset. Without `-env` the run needs no `sudo` and leaves out the faults of the environment. `go run ./cmd/chaos -check <dir>/history.json` reruns only the checkers on a saved history. A seed fixes the schedule, the proxies' choices for each connection and the workload's calls. It does not fix kernel timing or the order of connections, so a rerun injects the same faults in the same order but does not repeat the interleaving.

## The environment of `-env`

With `-env`, a run gives each node an environment of its own, through `sudo -n` (`chaos/env`). Each node gets its own disk: a file on a loop device, under a device-mapper target that is linear when healthy, then a small ext4 file system (512 MiB) that fills for real. Each node also runs in a cgroup of its own, with the cpu, memory and io controllers. A prio qdisc on `lo` puts every proxy port behind a netem qdisc of its own: each peer link gets a seeded one-way delay of 1 to 10 ms (`latency.json`), and client links get none. The nodes run with `--unsafe-window-bytes 16384 --unsafe-checkpoint-bytes 65536 --unsafe-segment-bytes 32768`, which are for tests only. With them, a follower stopped for a few seconds falls behind the window and needs a snapshot, and checkpoints and segment removal come often. During the run, every log file except spares is archived every 100 ms. The checkers read each node's own log plus the archived segments below its oldest one, which the node removed below a checkpoint. So the log they see still starts at entry 1.

| Fault | What the run does | Evidence that it took effect |
| --- | --- | --- |
| `netem-loss`, `netem-reorder`, `netem-corrupt` | A busy peer link loses 20% of its packets, reorders half of them, or corrupts 5%, on top of its delay | The link's qdisc sent packets during the hold; for loss, it dropped some |
| `cpu-gray` | A node, the leader in a third of the runs, is limited to 2 to 5% of one CPU, while a client reads through it without pause | `nr_throttled` of its cgroup grew |
| `disk-slow` | A node's disk delays reads by a quarter of 100 to 400 ms and writes by all of it | The node logged a write slower than a tick |
| `disk-stall` | A node's disk takes no I/O for the hold | The node logged a write slower than a tick once it healed |
| `fsync-fail` | Every write to a node's disk fails; after the hold the disk is healed, `e2fsck` runs (ext4 may have gone read-only), and the node starts again | The node's log names an I/O error or a read-only file system |
| `disk-full` | A file takes every free byte of a node's ext4; it goes after the hold, and the node starts again | The node's log names `no space left on device` or NOSPACE |
| `crash-in-recovery` | Five times: drop the caches, start the node on a disk slowed to 50 ms a read, and kill it 5 to 100 ms after it logged `recovering the log`; then start it on a healthy disk | At least one kill came after `recovering the log` and before `recovered the log` |
| `crash-in-install` | A follower is stopped while 300 writes of 1 KiB go through the leader; once resumed it fetches a snapshot, with the replies on its own links held, and it is killed once it logged the fetch and a reply was held, then started again; a follower that caught up from the log goes through it again, up to three times | The node logged `fetching the snapshot` and not `installed the snapshot` before the kill |
| `barely-alive-client` | A client opens a watch with previous values and then stops reading its connection for the hold, during writes | Once it read again, the watch delivered every revision from its start up to a read through another node, or was canceled; a revision it skipped fails the run |
| `several-at-once` | `cpu-gray` on one node and `netem-loss` on a leader link to another, at once | Both took effect |
| `replace-member` | Last in every run. Either every node is killed, one disk is erased and the other two start again from theirs, or one node's oldest segment gets a corrupt byte and its restart refuses the log. Then the member is removed through a healthy node, added as a new member and started on an empty disk with `--initial-cluster-state existing` (#161) | The two nodes elected a leader within the liveness bound below, or the restart refused the log; the cluster converged with the new member |

Gray faults are every fault above but `crash-in-install`, `several-at-once` and `replace-member`, whose erased case probes the two nodes left. During each one, a prober writes through the two healthy nodes (all three for `barely-alive-client`), one write every 500 ms, for at least 3 s and three writes. A write is retried, as a client retries, until it commits. Every write must commit within two of etcd's request timeouts (2 × 7 s) of its first try: the liveness bound of #47. A proposal that a follower forwarded to a leader that then went away is lost and times out after one, as in etcd (#230), and its retry goes through. The probes are part of the history, so every checker checks them.

Not covered yet: a delay on client links. The `delay` fault delays only the leader's link to one follower, so that a follower lags, and no fault delays a client link.

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

The `coverage` job fails when the statement coverage of the `internal/...` packages, measured across every package's tests (`-coverpkg=./internal/...`), is below 80%. It was 86.8% when the job was added. The simulator's tests run under gosim, where coverage is not measured, so the simulator's own packages, `internal/sim` and `internal/simrun`, are left out of it; it was 86.6% then. The job runs the simulator's tests that the `sim` and `membership` jobs do not in a step of its own.

The `test` job runs every package but the simulator with `-race`; `race-sim` runs the simulator with it, natively, with every node's goroutines, in its own job on the push to `main` and nightly. Three jobs run real processes built with `-race`, where a data race stops the process (`GORACE=halt_on_error=1`) and a race report in a log fails the run, also one at shutdown: the third `difftest` job (`RACE=1 scripts/difftest.sh tigerstore`, which builds the tests with `-race` too), the race row of `difffuzz` (`RACE=1 scripts/difffuzz.sh`, the same), and the race row of `apiserver` (`RACE=1 apiservertest/run.sh v0.37.0`), which runs on the push to `main` and nightly.

`core.TestMembershipChangesMatchRaft` is the component test of the membership checks: random member adds, learner adds, removals, promotions and updates on random clusters. Every change the checks let through must be one raft's `confchange` takes, since raft panics on any error, and the members after it must equal raft's configuration; a refused change goes to raft as a change of nobody, as in etcd.

# Differential fuzzing against etcd

`scripts/difffuzz.sh` starts a fresh etcd v3.7.2 and a fresh 3-node tigerstore side by side, both with client TLS, and runs the difftest's `TestDiffFuzz` against both. It sends random sequences of 30 requests, each under keys of its own: puts with leases, previous values and the ignore flags; reads of one key, of ranges and of everything from a key, at the current, past, future, zero and negative revisions, with limits and count- or keys-only; deletes; transactions with up to two compares on every target and up to three puts, deletes and reads in each branch; compactions; lease grants with picked and asked-for IDs and revokes, also of leases nobody granted; and watches from past revisions. Every answer is compared as the difftest records it: revisions relative to the sequence's start, lease IDs by the order the sequence granted them, values byte for byte, transaction results, and error codes and messages. A mismatch is shrunk, by removing ever smaller runs of requests while one remains, and saved as a named case in `difftest/testdata/fuzz`, which `TestDiffFuzzFound` replays on every run.

The `difffuzz` job of `ci` fuzzes for 90 s from a seed of the run ID. `difffuzz.yml` is the long run, started by hand (`gh workflow run difffuzz.yml -f duration=2h`): four jobs with seeds of their own. With planted bug B09a built in (`BUILD_FLAGS='-tags planted -ldflags=-X=github.com/djosh34/tigerstore/internal/planted.Active=B09a'`), the first sequence differed, and its case shrank to 6 requests.

The first long run on `main` ([38909081059](https://github.com/djosh34/tigerstore/actions/runs/38909081059), 2 hours) found two things:
- Three of its four shards found #203 within 5 minutes: the watch event of a delete that follows a put of the same key in one Txn carried the put's value as its previous value. etcd sends the key as it was before the Txn. Their shrunk cases are in `difftest/testdata/fuzz`.
- The fourth shard stopped etcd v3.7.2 itself. The shrunk sequence: a Txn branch deletes a key and then puts it again, then a compaction at that Txn's revision. After it, etcd's index still has the put but its backend does not, so the next read of the key ends etcd with "range failed to find revision pair". tigerstore answers it. Kubernetes never deletes and puts a key in one Txn, so the generator no longer puts a key that an earlier delete of the same branch removed (`TestDiffFuzzNoPutAfterDelete`). `scripts/difffuzz.sh` takes `FUZZ_ETCD_PORT` for a second run beside the first, and a failing run names the seed of its last sequence.

# Membership and metadata recovery

`sim.TestMembershipFaults` adds a fourth member as a learner, updates its peer URLs, promotes it and removes it. KV clients and watches continue through the simulator's disk, link, process and clock faults, sparser than in the KV runs over a fault phase of 60 s: as etcd's, a node refuses a member change unless it has been connected to the other voters for 5 s. The run retries ambiguous changes after a linearizable member list. A separate oracle checks the returned members against the requested changes, including a mistake shared by every replica. The KV history still runs through durability, revision, watch and both witness checkers, and through Porcupine only if it is short enough (`check.ShortEnough`); a membership history has 5,000 to 6,700 operations, and Porcupine did not finish some in 3 minutes (#244). Each operation must complete during the fault phase in the seed set. Permanent corruption is disabled in these runs; the ordinary three-voter seeds retain it. The erase-one-disk checks still run during membership changes. Cold copies preserve member IDs and the run file, but not a process's volatile removal notice. Recovery must read its disk; a peer's transport can then tell a member that missed its removal to stop when it connects.

`TestMembershipColdRestart` keeps named membership recovery seeds. `core.TestRemovedLeaderDrainsCommit` covers [#211](https://github.com/djosh34/tigerstore/issues/211).

`node.TestMetadataAfterCleaning` holds a follower down while membership, users, roles, permissions, auth enablement and both alarms change. The real cleaner must copy a cold value to a pack and remove every entry through those changes on both donors. The test checks each entry is absent, restarts both donors, then starts the old follower and requires a snapshot fetch. It checks the members, users, permissions, alarms and cold value after donor restart, follower catch-up and another follower restart. These are real node drivers and files with an in-process peer transport, not separate server processes.

# The deterministic simulator

The simulator's tests (`internal/sim`, and `TestConcurrentCleanerSimulation` and `TestPlanted` in `internal/node` and `internal/peer`) run under [gosim](https://github.com/djosh34/gosim), a port of Jelle van den Hooff's deterministic simulator to Go 1.27.2. gosim translates the tests and every package they use into code that runs on its own scheduler, clock, network and random source, all decided by its seed. So a seed plays out the same way every time: the same trace, history and disks. `go.mod` pins the fork's `main` commit with a `replace` directive; its command builds from `tools/gosim`, a module of its own, so it does not move tigerstore's dependencies.

`scripts/sim.sh <package> <test pattern> [test flags]` runs them:

    SIM_SEED=17 SIM_SEEDS=1 scripts/sim.sh ./internal/sim '^TestSeeds$'

It builds the gosim command, translates the package's tests (`gosim build-tests`), builds the package's native test binary and runs it with the pattern; flags such as `-test.v` or `-test.timeout 30m` go to that binary. `TAGS=planted` builds the planted bugs in. Inside the native binary each simulator test runs its seeds in gosim (`internal/simrun`): every harness seed is a gosim run of its own, with gosim's seed equal to the harness seed, so a seed from the middle of a campaign plays out alone as it did there. The native test applies the rules over a set of seeds, such as the share of seeds each fault took effect in, and writes what a run saved to `$SIM_OUT`. A test with fixed seeds of its own runs each of them the same way (`simrun.Each`), and a test of one seed runs as one gosim run with that seed (`simrun.Drive`). A seed of a set replays alone with `SIM_SEED=<seed> SIM_SEEDS=1`; the rules over a whole set do not apply to one seed. The tests read their settings from the environment, which reaches them inside gosim, where they get no flags:

- `SIM_SEED` and `SIM_SEEDS`: the first seed of a test's set and how many. `TestSeeds`, `TestLinearizable` and `TestMembershipFaults` run 16 from seed 1 by default.
- `SIM_TWICE`: how many seeds of a set, spread over it, run a second time; each must give the same hashes of its trace, its history (every call and watch response, with exact times) and every node's disk, now and after its last crash. Otherwise the seed fails with both sets of hashes, and both runs are saved, as `<run>-first` and `<run>-second`: the first run's files go to `$SIM_OUT` before the second starts and are removed again when the hashes agree, so the driver holds one run's files at a time. A run of a long profile (`SIM_LONG=1`) saves `calls.txt` only when it has no `history.json`, which holds the same calls. CI's `sim` job runs 20 of each test's seeds twice.
- `SIM_OUT`: where a failed run saves its trace, nodes, hashes, calls, history, disks and replay command.
- `SIM_PLANTED`: the planted bug `TestPlanted` switches on.
- `SIM_LONG`: with `1`, `TestKubernetesLoad` runs its long profile, as the nightly `kube-sim.yml` does.

Plain `go test` skips the simulator's tests, with the `scripts/sim.sh` command that runs them. Under `-race` they run natively instead, in `testing/synctest` bubbles, as the `race-sim` job does; Go's scheduler then decides the order of the work, and a seed can play out differently from run to run. There is no other native run of the simulator.

gosim does not implement every function of Go's runtime that tigerstore's dependencies reach. `internal/sim/unimplemented.txt`, generated by `gosim translate -unimplemented`, lists each one the translation meets on arm64; each fails through `gosimruntime.Unimplemented(name)`, which ends the run with the function's name. The `gosim-hooks` job of `ci` generates the list again and fails on a difference. `scripts/sim.sh` refuses a test binary that can reach one that `internal/sim/reachable.txt` does not list, with the reason the tests never call it.

Inside gosim the clock stands still while a goroutine computes, so a checker's timeout never comes. The simulator's checks are bounded by the steps of their search too (`check.MaxSteps`). Within one gosim run memory is never returned, as the port keeps weak pointers strong, about 30 MB per simulation; one more reason for a gosim run per seed. The fork's README lists its other limits.

`internal/sim/known-failures.txt` lists seeds that fail because of an open `bug` issue, each with its issue. `scripts/sim.sh` skips them and prints a `known failure:` line for each; every other failure fails the job. It holds seeds of the tests that run sets of seeds (`simrun.Seeds` and `simrun.Each`); a test of one seed (`simrun.Drive`) cannot be listed. Another package with such tests, such as `internal/node` for the cleaner simulation, gets a `known-failures.txt` of its own. While the file lists any seed, CI runs fixed seeds only: the `sim` job's shards 2 and 3 run seeds 801 to 1600 instead of fresh ones, and `membership` runs 65 to 128, since a fresh seed that fails on a known bug cannot be listed. Any change to the simulator or what it runs makes every seed play out differently, so the list fits one commit: after such a change, empty it, run `gh workflow run ci.yml -f fixed=true` on the branch, and list every seed that failed on an open bug. Under the race detector a seed does not play out the same way twice, so no list can hold its failures, and `race-sim` runs every fault.

tigerstore stays on Go 1.27.x. A Go upgrade waits until the gosim port for that version passes the replay gate: CI's double runs, and a seed of a campaign that replays alone with the same hashes.

## The simulator's faults

Every default seed (`sim.DefaultConfig`) has all of these at once, at seeded times, for 20 virtual seconds; then the faults heal and the clients run on for 10 s. `TestEachFaultTakesEffect` runs each new fault alone on three seeds, each a gosim run of its own, and requires that it took effect in two of them; `TestSeeds` requires in every run the faults that come once in every run, and over a set of 64 seeds or more the rare ones.

- **Disks** (`internal/simfs`). A power loss keeps a node's synced bytes and the directory as of its last directory sync, plus a crash image of what came since: a prefix of the bytes written in order (with garbage at its end one time in four), any subset of the writes whole, or any subset torn at sector (512 B) or page (4 KiB) boundaries; each creation, rename and removal since the last directory sync survives or not on its own. Half the crashes are power losses; the other half kill the process and keep the page cache, and one kill in three is followed 0.1 to 3 s later by a power loss, while the node is down or after it started again. A failed sync makes part of its writes durable and marks the rest clean, as Linux does. A disk fault fails the next sync, write (EIO, or a short write with ENOSPC) or any other file or directory operation; a file system shuts down (EIO on everything) or goes read-only (EROFS on every change) for 0.5 to 3 s; a disk fills up for 1.5 to 3.5 s. Every node has a 32 KiB disk budget, so it cleans, and some runs raise the NOSPACE alarm, and a 128 KiB disk. `TestCleanerSettles` requires fewer than 1,000 packs a run: before the fix of [#362](https://github.com/djosh34/tigerstore/issues/362), the cleaner copied a pack of small live values again on every pass and filled the disk. Damage after a successful sync, once per run on one node so a quorum stays: a flipped bit in the root checkpoint, a misdirected write over a sealed segment, a lost write (a zeroed sector), a sector that fails every read, a read that returns wrong data once, or a damaged last record that holds only a new term or vote. The node must refuse to start on damage it can detect, start again after a misread, and never lose a durable term and vote (#357). Slow log syncs, stalled disks, crashes inside recovery and at each step of a checkpoint or snapshot install, damaged snapshot streams and rotten values on a running node come as before.
- **Network** (`internal/sim/net.go`). Links between the nodes go down one way or both ways, and the links between the clients' host and each node go down too. Each link has a delay of its own and spikes up to 5 ms. About every 3 s a link to or from the leader, or a client's link, gets for 1.5 to 3.5 s one of: 20% packet loss or 5% corrupted packets, which TCP retransmits after 200 to 400 ms; reordering, which delays half the packets up to 10 ms; a limit of 8 to 32 KiB/s; 300 to 800 ms more delay; a delay that switches between 0 to 2 ms and 20 to 80 ms every 100 to 400 ms; or a flipped, dropped or duplicated byte in the stream, which TLS refuses, and whose connections the fault's end resets. Links stall, one stall in four past the 5 s peer write timeout; connections reset; links go half-open. A connection holds 2 KiB in each direction that its reader has not taken, and a write past that waits for room or its write deadline.
- **Processes and clients.** Pauses of 0.2 to 2 s are the gray CPU, and the per-node tick rate, up to 5% fast or slow, is the clock fault. Gray nodes, a barely alive client whose watch streams are never read, and watch clients that turn slow for 2 to 5 s, until the node closes their stream 8 responses behind, are scheduled. Besides six clients at `Node.Do`, three clients of etcd's Go client (clientv3 v3.7.2) each call the real gRPC server of one node over the simulator's network, with TLS, so every link fault reaches them. Their lease calls go through clientv3's lessor on a plain lease client, which sends a call again only when it never left: clientv3's own lease client sends a grant again after it may have taken effect, which makes a second lease. Their reconnect backoff is at most 1 s, not gRPC's 2 minutes.

Each fault of the real-process chaos run has a simulator equivalent:

| Chaos fault | In the simulator |
| --- | --- |
| `sigkill-leader`, `sigkill-follower` | A kill that keeps the page cache, of whichever node the schedule picks, the leader among them; a power loss |
| `sigstop-leader`, `sigstop-follower` | A pause |
| `delay` | A delayed link, 300 to 800 ms |
| `switching-delay` | A switching delay |
| `stall` | A link stall, with the writers blocking on full buffers |
| `drop`, `corrupt`, `duplicate` | A dropped, flipped or duplicated byte, which TLS refuses |
| `reset` | A reset of the connections between two hosts |
| `one-direction-cut` | The link masks, one-way and both-way, on peer and client links |
| `slow-link` | A slow link, 8 to 32 KiB/s |
| `netem-loss`, `netem-reorder`, `netem-corrupt` | A lossy, reordering or corrupting link; every link's own delay, as the 1 to 10 ms of netem on each peer link |
| `cpu-gray` | Pauses |
| `disk-slow`, `disk-stall` | Slow log syncs, a gray node's slow fsyncs, disk stalls and a stalled leader |
| `fsync-fail` | A failed sync that kept part of its writes, EIO on any operation, a file system that shuts down or goes read-only |
| `disk-full` | A full disk, the disk's capacity and short writes with ENOSPC |
| `crash-in-recovery` | A crash inside recovery |
| `crash-in-install` | A crash at a step of a snapshot install, and a crash of the snapshot's source during the stream |
| `barely-alive-client` | The gray client, and slow watchers |
| `several-at-once` | Every default seed: all faults at once |
| `replace-member` | Erasing each disk in turn and cold-starting the other two (whose disks are copies taken by a power cut of the whole cluster), fencing a damaged disk, and `TestMembershipFaults`, which adds a member on an empty disk and removes it |

What the simulator cannot reach: the real kernel, file system and `internal/osfs`; cgroup limits and the memory a process uses, as all nodes share one process; real time, so a slow CPU only shows as a pause; and per-node clock jumps, which no real-process test injects either.

Known limits until the parked recovery format ([#322](https://github.com/djosh34/tigerstore/issues/322)): a crash image that keeps a later sector of a write but loses an earlier one can make recovery refuse the log, as damage looks the same; the node then refuses to start, which the simulator counts as a safe stop (`TornRefusals`), and it draws such images only while no other node is out, so a quorum stays. An image of whole writes must never be refused (#295). Damage to the last record of the log looks like a torn write and is cut off; the simulator damages a last record only where that loses no entry, and the node must keep its term and vote.

`TestLoneSegmentHeader` runs #295's path: a follower held down past the leader's window fetches a snapshot, with segments of 64 bytes so that the install often comes on a fresh segment, and crashes right before the install's first sync of its log (`Faults.InstallCrash`). A crash image of whole writes can then keep the records and lose an earlier write of the header alone. With the fix there is no such write, and every seed passes; planted bug B32 writes the header alone, as before the fix, and `TestPlanted` must catch it with that configuration.

# Simulator seed campaign

`sim-campaign.yml`, started by hand (`gh workflow run sim-campaign.yml -f test=<test> -f first=<seed>`), runs one test under gosim in 20 jobs of two processes from seed `first`: `TestSeeds` on 100,000 seeds, 2,500 a process, every checker on each, the witness checker in both modes among them, or `TestMembershipFaults` on 10,000 seeds, 250 a process. 1% of the seeds of each process run twice and must give the same hashes. For `TestSeeds`, each process of at least 64 seeds also runs the deliberate stalled-leader scenario. Its silence deadline and client-liveness checks must pass. Each process of 2,000 seeds or more must also have a stalled leader that went silent in its random seeds, and a write that a crash tore; CI's shards of 400 seeds are too small for those rules (#183, #218, #378). Each fault of the schedule must take effect in 95% of a process's seeds; a fault that misses in one seed fails no run on its own. A failed seed uploads its trace, how it left each node and its history.

# Kubernetes-shaped load in the simulator

`TestKubernetesLoad` runs the soak's load (`cmd/soak`) in the simulator, under every fault of `sim.DefaultConfig`, with every checker of a default seed: the simulator's own rules and the durability, revision, watch and witness checkers of package `check`. Its long profile has 10,000 objects and lasts 4 virtual hours, then the faults heal and the clients run on for 10 s. Its faults come twice as far apart as in a default seed, a crash every 6 s on average and so on. Seed 7, 10,000 objects for 30 virtual minutes: at the default rates the cluster answered 2,061 of 22,645 calls and committed 1,637 writes; twice as far apart, 15,984 of 35,701 and 11,258 writes, at a peak of 1.8 GiB; ten times as far apart, 54,687 of 74,785, but at 3.3 GiB, which over 4 hours does not fit in a runner's memory. Sixteen clients each own their share of the objects under `/registry/<resource>/sim-c<client>/`, with values of the sizes a Kubernetes 1.37 cluster held (`internal/k8ssize`). A client first creates its objects, eight in a Txn, then makes calls in the soak's mix, about 400 a second over all clients when the cluster answers, and backs off for up to a second after a failure:
- creates, updates and deletes in Txns that compare the create or mod revision the client last saw, with a read of the object as the else branch, as the apiserver does;
- linearizable reads, and lists of one of its resources in pages of 25 at the first page's revision;
- events on leases of 120 s, at most 1,000 on a lease and a new lease every minute, which vanish when the lease expires;
- a compaction every 5 virtual minutes to the newest revision seen when the interval before ended, tried again until it lands.

A watch client watches each resource prefix, with previous values, progress notifications and a progress request about every 5 s; as the other watch clients, it drops its stream now and then and resumes on any node. Each seed must have compacted, paged through a list, had a lease expire, and had progress requests answered; a seed of the long profile must also have had each fault of a default seed take effect. The checks follow each lease's own TTL: a lease may outlive its last grant or keepalive by twice its TTL, as a leader change gives it the full TTL again, so the run's end waits 242 s after the clients stop, and no lease may expire before its TTL ran out.

In the long profile all of them call `Node.Do`: under gosim a call of etcd's Go client can spin for good in gRPC's transparent retry, which one long seed in seven met ([#440](https://github.com/djosh34/tigerstore/issues/440)). By default, as in `ci`'s `coverage` job, it runs a short profile instead, where three of the clients use etcd's Go client: 2 seeds of 1,000 objects for 2 virtual minutes, with a compaction every 20 s. `SIM_LONG=1` selects the long one. Under the race detector, in `race-sim`, it skips: natively a run can hang when a TLS alert waits for a write lock and the synctest clock stops ([#439](https://github.com/djosh34/tigerstore/issues/439)).

`kube-sim.yml` runs the long profile every night on `main`, and by hand (`gh workflow run kube-sim.yml --ref <branch>`). Its 20 jobs each run new seeds one after the other, each a process of its own (`scripts/kube-sim.sh`), and start seeds while the job has had less than 165 minutes; a seed may take 180 minutes, so that a job ends within GitHub's 6 hours. On the PR's final head (run 40227804313), 45 of 59 long seeds passed, taking 50 to 116 minutes, 87 on average, at 4.5 to 5.5 GiB; the other 14 reached the 2-hour limit of that run and failed no check. So a night runs about 40 to 60 seeds, not the 500 of 12 minutes the plan expected: the witness check that does not use the log grows faster than the history (88 s for one virtual hour of seed 7 here, 289 s for two). The first seed of job 0 runs twice and must give the same hashes; with the first run's files on disk, that pair peaked at 11.4 GiB.

    SIM_LONG=1 SIM_SEED=<seed> SIM_SEEDS=1 scripts/sim.sh ./internal/sim '^TestKubernetesLoad$'

A long seed is one gosim run of up to about three hours and 6 GiB; `scripts/kube-sim.sh` runs seeds as the workflow does, with its memory guard. `hashes` writes the history and each disk straight into its hash: built in memory first, a seed of 20,000 objects took 14.8 GiB there.

# When a test fails

Every failure says how the nodes stood and prints the command that runs it again:
- A simulator run that fails logs its trace and, for each node, whether it runs or why it went down, Raft's role, term, leader, commit and applied index at its last turn, its members and its last log lines, the hashes of its trace, history and disks, then `replay:` with the `scripts/sim.sh` command and the variables of its seed and planted bug. With `$SIM_OUT`, it saves them there with its history, which `cmd/checkhistory` checks again, and its disks. Under gosim the replay plays the seed out exactly as it failed, with the same hashes.
- Chaos, the soak and the rolling-upgrade tests print each node's process state, status, members and last log lines, and every flag of the run.
- A checker that times out says how far its search got: Porcupine the longest order it found and the op it could not place, the witness checker its revision, open gaps and candidates.
- `scripts/replay.sh` turns a `go test` or `scripts/sim.sh` log into one command per failed test; every workflow runs it or prints its script's command, and uploads its logs on failure.

Whether a fault took effect is a number counted over a set of runs, with a minimum: 95% of the seeds in the simulator (`effects` in `internal/sim`). Porcupine runs only on histories of up to 4,000 operations (`check.ShortEnough`); the witness checker runs on every history. Simulated-time tests assert in simulated time, and real-time bounds use etcd's 7 s request timeout.

# Soak

`soak.yml` runs `cmd/soak` every night on `main`, for 5 hours with the date, `YYYYMMDD`, as its seed; started by hand (`gh workflow run soak.yml --ref main`) it takes `-f duration=` and `-f seed=`. Each run: three tigerstore processes, each in a cgroup of its own with a 640 MiB memory limit and a 3 GiB disk budget, under 600,000 Kubernetes-shaped objects. They hold about 0.9 GB of values. The preloaded events expire with their leases after 2 minutes, which leaves about 0.6 GB: still more than the value cache's share of the limit, so reads miss the cache.

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
- after a kill, the other two nodes must commit a write within 7 s, etcd's request timeout;
- a restarted or resumed node must commit a write through itself within 2 minutes, which covers its recovery or snapshot install;
- the nodes must agree on `HashKV` at one revision.

No node may exit on its own or be killed by its memory limit, no alarm may go off, and a data directory may not pass its budget by more than 256 MiB.

The run writes `report.json`: the calls, their errors and ambiguous answers, watch events, peak memory and disk of each node, its bytes cleaned and its cache hits and misses since its last start, and the longest recovery and catch-up. A soak against planted B01a (stale reads) and against B09a (one revision per key of a write) fails within 3 minutes.

# More data than memory

`past-ram.yml` checks that tigerstore holds more live values than its memory limit (#144). The required `ci` gate calls it. A pull request runs only its `storage` job; the push to `main`, the nightly run and a manual dispatch (`gh workflow run past-ram.yml --ref <branch>`) also run the process measurements.

## Storage checks

- `sim.TestPastRAMRecovery` runs 16 simulator seeds under gosim with a small value cache and all six checkers. On each node's durable disk it then copies the values the durable checkpoint names out of the old segments into a pack, the way the cleaner does, and removes those segments. It crashes after the pack sync, with the root buffered, after the root sync and after the removal. At each point it checks that every checkpoint value is still there, that the original segment files are gone, and that a recovered core matches a model replayed from the simulator's own committed history, not from the disk under test. Seeds must have cache hits and cold reads.
- `node.TestConcurrentCleanerSimulation` runs the real node driver and paced cleaner on a simulated disk, for 8 seeds, each a gosim run whose seed decides the interleaving of file calls. Clients overwrite and compact, readers hold cold values and snapshots are served while the cleaner packs and removes segments. Copies of the durable disk taken at each cleaner step must resolve every value of their durable checkpoint, and a restart from the final crash copy must return every acknowledged value.
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

# Bench with latency between nodes

The `bench` workflow measures tigerstore and etcd v3.7.2 with 0, 2 and 10 ms of round-trip time (RTT) between nodes ([#395](https://github.com/djosh34/tigerstore/issues/395), [#414](https://github.com/djosh34/tigerstore/issues/414)). It has one job per workload, put-1k, txn-1k and mixed-k8s. Each job runs the RTTs in turn on one runner, with 5 max and 5 rate repetitions per store and RTT.

`bench/netem.sh` delays every packet to or from a peer port on `lo` by half the RTT with `tc netem`. Client traffic goes through a plain FIFO with no delay. Both directions of a connection leave through the root qdisc of `lo`, so a round trip between two nodes takes the whole RTT. Both stores send all node-to-node traffic to peer ports, from ephemeral ports ([#400](https://github.com/djosh34/tigerstore/issues/400)). At 0 ms the same qdiscs run with no delay, so every RTT has the same packet path; the 0 ms numbers are therefore not those of older runs over plain loopback. Before each RTT, a probe records the TCP connect time to a peer port and to a client port, and the report shows both.

The shared rate at an RTT is half of etcd's median max at that RTT. Where etcd has no max result, it is the rate of the job's first RTT, 0 ms by default. If etcd has none there either, no rate runs run at that RTT, and the report names it with the failed runs.

mixed-k8s keeps 288 requests in flight: 6 connections of 48 streams each. Its reads are linearizable and go to all three nodes, so at 10 ms a request waits about 17 ms before any work. 288 in flight then allow about 17,000 requests/s, twice tigerstore's max at 0 ms. The 96 of 6 × 16 capped the generator near 5,700/s. `bench/cluster.sh check` fails a run if the leader or the term changed during it, so every result that did not fail shows that the store kept its leader at that load.

A run that fails writes its cause to `<target>-<mode>-<rep>.failed`, and the other runs still run. `cmd/benchreport` then writes one table across RTTs: per workload and store, the median max throughput, its ratio to etcd's, the shared rate, and p50 and p99 at that rate. The failed runs with their causes and the detail tables follow. The job fails on a failed tigerstore run, a harness failure or a missed bar. The bars hold at every RTT above 0: each tigerstore build's median max throughput is at least 10 times etcd's for put-1k and txn-1k, and 2.5 times for mixed-k8s ([#404](https://github.com/djosh34/tigerstore/issues/404)). A failed etcd run is reported and fails nothing; tigerstore's result then stands alone. A drop in tigerstore's own throughput with RTT is allowed.

The put-1k and txn-1k jobs run the disk and CPU layouts and `diskbench` at the first RTT only, since they measure the disk and the CPU, not the network.

To run it on a branch against `main` and etcd:

```sh
gh workflow run bench.yml --ref main -f candidate=<branch>
```

The inputs `benchmarks`, `rtts`, `targets`, `modes`, `reps` and `layouts` change what runs. By hand, on a Linux machine with sudo and the binaries in `bin` (`etcd`, `etcdctl`, `loadgen`, `benchreport` and a tigerstore named `candidate`):

```sh
scripts/dev-certs.sh certs
sudo modprobe sch_netem
bench/netem.sh on 10
TARGETS="etcd candidate" CERTS=certs bench/run.sh put-1k results/rtt-10
bench/netem.sh off
bin/benchreport results
```
