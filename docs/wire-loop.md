# One loop per node, bytes in wire form

This is the clean rewrite of prototype 6 of #602 (`proto/602-6-wire-loop`, log in [#602](https://github.com/djosh34/tigerstore/issues/602#issuecomment-6824341133)). It lands as one PR on `work/602-wire-loop`, never split (#603, "One clean PR, and a 15-minute bench", rules 2, 6 and 7). This file is its design and its checklist. A worker who takes over the branch reads it first and keeps it true.

## The bar

The PR merges only when the quick bench of its final head against `main` shows:
- crd-heavy gaining about what prototype 6 gained, +111% to +180%, and at least +80% at 0, 2 and 10 ms, by the paired ratios;
- put and txn within the noise.

Run it with `gh workflow run bench.yml --repo djosh34/tigerstore --ref main -f candidate=<head> -f baseline=main`.

Nothing is lowered on the way (#603, "Boil the ocean", rule 6):
- every safety guarantee: linearizable reads and writes, no acknowledged write lost, atomic and isolated transactions;
- the etcd v3 API as Kubernetes uses it;
- the simulator runs the real code;
- Go.

## Where the time goes, and what each part saves

From the prototype's design ([#602](https://github.com/djosh34/tigerstore/issues/602#issuecomment-6817173146)). The leader spends about 10 µs per crd-heavy op on `main`. The prototype measured 2.2–4.8.

| # | Part | Replaces | Leader µs/op, estimate |
|---|---|---|---|
| 1 | Own client stack: TLS 1.3 records after crypto/tls's handshake, opened in place; HTTP/2 frames and gRPC read in place; hot calls decoded by aliasing the record; replies written into record plaintext and sealed in place; one write per connection per turn | `internal/server`'s goroutines, x/net's Framer, protobuf's Marshal and its copies | ~2.5 → ~0.8 |
| 2 | One event loop per node: one thread runs every client and peer socket, Raft's `RawNode`, the apply and the replies; the WAL's sync on one helper that wakes the loop | goroutines, channels and locks per connection and message | ~0.8 → ~0.1 |
| 3 | Replication in wire form: a turn's writes form one batch, whose bytes are the WAL record, the peers' frame payload and the followers' WAL record; #626's naming of forwarded entries stays | peer encode and decode, WAL encode | ~2.6 → ~1.0 |
| 4 | Store in wire form: each key's live revision kept as its encoded `mvccpb.KeyValue` in pointer-free memory behind a hash index; older revisions point at their values in the log; ranges through an ordered index | `internal/mvcc`'s index and Txn apply, watch matching | ~2.4 → ~0.7 |
| 5 | Memory sized at startup from the budget: rings, stages, send buffers and pools; values the budget can't hold stay on disk with the cache | allocation and GC | ~1.2 → ~0.1 |

## Code to start from

These are clean, reviewed code, merged and then reverted by #684 because, alone, they did not make crd-heavy faster. They are parts of this design. Take them back as a start, not as they were:

- **The loop and its poller**, from #638 (4d8b4e9f) and #642 (d6f0b527):
  - `internal/epoll` and `internal/loop`;
  - the loop's connections in `internal/server/loopconn.go` and `internal/peer/loop.go`;
  - the simulator's poller in `internal/sim/net.go`, so the simulator runs the real loop.
- **TLS 1.3 records** from #646 (9d713113): `internal/loop/records.go` with its fuzz target, and `internal/peer/tconn.go`.
- **Frames and requests read in place** from #671 (4bbd6b52): `internal/server/frame.go`, `arena.go`, `deadline.go` and `core.Slabs`, with `FuzzFrames` and the h2spec job (`scripts/h2spec.sh`). The h2spec job comes back with this parser; #672 lists the cases the server fails.
- **Replies written into the record**, from #679 (closed, branch `work/602-h2-write`). Its review threads name the pitfalls: the pass's budget, credit in the same write, the order of control frames.
- **A turn's bound** from #649 (f5eabd96), the turn part. Its overload heartbeat tests, `TestHeartbeatsUnder(Read)?Overload`, come back too. A loop must not hold a heartbeat past a fraction of the election timeout (#647). #660 is what the bound's estimate got wrong.

The prototype (`internal/wl` on `proto/602-6-wire-loop`) is the reference for parts 3–5 and for how the parts fit. It is never merged or cherry-picked (#596).

## What the clean version adds to the prototype

From the prototype's log. Each item is off the steady state, so the speed stays.

- **Client stack:**
  - TLS key updates and alerts as crypto/tls handles them;
  - HTTP/2 send flow control;
  - every etcd call, the rare ones on a slower path;
  - request limits and auth as today;
  - a big LIST or RangeStream written over several turns.
- **Loop:**
  - every socket and the disk behind the poller, which the simulator implements;
  - no disk read on the loop: `core.termAt` today reads the WAL on the committer (#682).
- **Replication:**
  - recovery from the WAL, with checksums, torn tails and hard state;
  - snapshots to a follower behind the retained log;
  - a batch named only in the live pipeline's appends, never in entries Raft resends from storage;
  - each follower paced by bytes in flight, not by dropped appends;
  - held appends bounded in memory;
  - simulator scenarios for a lost batch and a stalled follower.
- **Store:**
  - compaction that frees history;
  - checkpoints so the log can be truncated;
  - lease expiry and Revoke's deletions;
  - reads at an old revision that see keys deleted since;
  - watches from a past revision;
  - membership, alarms, defragment and the corruption check as today.
- **Memory:**
  - everything sized from the budget, with rings of tens of MB, not the prototype's 768 MiB;
  - each connection's send buffer bounded;
  - the value cache's bounded steps (#647) kept. #686 is their cost on crd-heavy at 10 ms, to be fixed here or right after.

## Tests

- Every part runs in the simulator, with scenarios for what it touches.
- The real-process tests stay, as cut by #390.
- A test fails with the state of every node and the command that replays it (#498, Rules).
- Before each push, run #498's checks: gofmt, vet, golangci-lint with `difftest,planted`, `go test -short ./...`, and `scripts/sim.sh` for the simulator tests touched.
- Many-seed and long runs go to CI, never this host.

## How the clean version keeps what the prototype broke

- **Live values** (part 4). The store holds each key's newest value in its slabs, within the budget (`internal/mvcc/held.go`), in place of the core's value cache. A value that a write replaces stays readable until the core's turn ends (`Store.Release`), so the turn's watch events carry it as their previous value without the disk. The prototype kept every record and read old values back as zeros; here an old revision still comes from the log.
- **The disk** (part 3). The log's newest segment is written with `O_DIRECT|O_DSYNC` (`osfs.OpenLog`): each write is durable as it returns and no `fdatasync` follows, as in the prototype. The log keeps its segments, checksums, roots and recovery; the prototype's ring overwrote values it still named. The last block is written again with the next append: its bytes up to the old end are the same, so a torn write keeps them. `simfs.Disk.Direct` models these writes. The simulator runs the log on the buffered model, a disk that keeps less than one with direct writes, and the node still syncs wherever that disk needs it.
- **Bigger turns** (part 2). After a busy turn the loop pauses for what is left of `node.DefaultTurnGap` (500 µs) from its start, in `nanosleep`, so the next turn takes more with one read, write and wait per connection. The prototype's turns grew from the 800 µs it left between syncs. A drained socket is not read again until epoll reports it.

## Status

The PR body keeps a list of what is done on the branch, what is benched, and what is left.
