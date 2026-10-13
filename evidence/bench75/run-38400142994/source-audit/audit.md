# PR84 read-clock: final lean Put64 / Put256 source audit

## Immutable binding and scope

**FINAL, parent-confirmed.** [Actions run 38400142994](https://github.com/djosh34/tigerstore/actions/runs/38400142994), resolved **2026-10-13T12:49:30Z**. Exactly two Put jobs, **64 and 256 total outstanding**, main/PR84/stock etcd, **five repetitions per store and workload**. No CAS or profile scope.

| Role | Exact audited commit |
|---|---|
| M — main | `aa9d9b6eea79c73f12fe0153aefa809841967e88` |
| R84 — PR84 read-clock | `d5681651cd554b8804ed3a77b9ff019ade6815bf` |
| Harness/generator | `6148beab08e91a02c243ccd2d2b788ecdacd45fd` |
| Stock etcd v3.7.2 | `68c065e562994b89e333e77b039ad066f933c586` |

PR84's provenance reference is **`refs/pull/84/head`**, retained for future branch deletion. Only the parent-confirmed immutable object above is audited; later branch heads are not substituted. The provisional main/candidate happen to equal final server SHAs, but the report and sites have been explicitly rebound FINAL.

All writes confined to `/home/joshazimullah.linux/work_mounts/scratch/bench84-lean-source-audit`. Repository and **sealed PR80 scratch are read-only**; no modifications to either. No subagents, GitHub writes, Go executions, builds, tests or benchmarks. Deadline `2026-10-13T22:00:00Z`. Selected files only, no whole export, dependency ZIP duplication or bare object repo. Source-only checks assert exact site text and byte equality against immutable `git show`; `verified-sites.md/json` and small source/workload/reference manifests plus `SHA256SUMS` retain integrity evidence.

Ordinary successful unary Put, stable leader endpoint/established replicated cluster, one nonempty **1,024-byte value**, no compression/lease/watches/PrevKv/Ignore flags/auth/fault/restart/deadline firing. One full-value stage materializes that value in a different byte backing store. Chunks into one destination count once; a codec caller and implementing byte instruction count once. Slice headers, structs, key metadata, entry pointers, sizing and checksums are not value-byte copies. **These are not physical-copy totals or performance ratios.** TLS/Framer/bufio/socket/kernel/page-cache buffers, runtime append-growth relocation and unenumerated context/library lifecycle work are excluded, **not zero**.

## 1. Put value stages — actual read-clock diff checked

PR84 production changes are confined to `internal/server/batch.go`, `internal/server/h2.go`, and `internal/node/node.go`: buffered-availability predicate, reader admission-clock gate, and a Taken callback/channel. Its other changed source paths are documentation and two peer test files. **No new owned request-body buffer is introduced.** The same existing 64-KiB bufio reader remains at M `internal/server/h2.go:121`; R84 `internal/server/h2.go:126` with size `internal/server/h2.go:43`; frame reads remain M `internal/server/h2.go:263`; R84 `internal/server/h2.go:271` / M `internal/server/h2.go:267`; R84 `internal/server/h2.go:275`. New `nextFrameBuffered` peeks **nine header bytes**, M `internal/server/batch.go:93`; R84 `internal/server/batch.go:103`, and checks availability `internal/server/batch.go:108`; this is not a full-value materialization. `awaitTaken` precedes the existing reads at `internal/server/h2.go:268`. `stream.go`, core codec/wire/apply, MVCC, WAL and peer production value paths are unchanged between the bound targets.

| Named leader full-input-value stage | M Put | R84 Put | Exact site(s) |
|---|---:|---:|---|
| Reusable Framer DATA → owned message accumulation | 1 | 1 | `internal/server/stream.go:127`; full-size reserve `internal/server/stream.go:124` |
| Frontend owning Put.Value protobuf decode | 0 | 0 | wire ParsePut `internal/server/batch.go:59` aliases the owned body |
| Request body → encoded Raft entry | 1 | 1 | `internal/core/codec.go:68`; reserve `internal/core/codec.go:37` |
| Retained leader Raft proto2 Entry.Data deep clone | 1 | 1 | proposal `internal/core/node.go:585` → `dependency-sources/raft-v3.7.0/raft.go:817` → `dependency-sources/protobuf-v1.36.11/internal/impl/merge.go:188` |
| Entry.Data → WAL staging | 1 | 1 | `internal/wal/wal.go:368` → `internal/wal/wal.go:351` (one stage) |
| Apply owning Put.Value protobuf decode | 0 | 0 | Put-kind dispatch `internal/core/node.go:858` → ParsePut `internal/core/node.go:894` |
| Index value assignment | 0 | 0 | retained slice header `internal/mvcc/ops.go:259`; metadata version append `internal/mvcc/ops.go:250` |
| Ordinary Put reply input value | 0 | 0 | M `internal/server/batch.go:265`; R84 `internal/server/batch.go:308`; metadata only, no PrevKv |
| **Known leader input-value subtotal, peers excluded** | **4** | **4** | same at Put64 and Put256 |

ParsePut consumes capped byte aliases at `internal/core/wire.go:61` / `internal/core/wire.go:67` / `internal/core/wire.go:71`; apply's capped batch slice `internal/core/codec.go:103` and direct state path `internal/core/state.go:110` preserve entry-backed values. Storage `internal/core/storage.go:39` retains entry pointers; no-watch path `internal/core/watch.go:272` avoids watch encodes in scope. WAL Write/Sync are durability operations, not another enumerated value buffer. One live value can pin a larger entry backing array; this is not WAL-offset-addressed storage or zero-copy end-to-end.

| One newly received, persisted and applied follower | M | R84 | Exact site(s) |
|---|---:|---:|---|
| Peer owning proto2 Entry.Data decode | 1 | 1 | `internal/peer/peer.go:248` → `dependency-sources/protobuf-v1.36.11/internal/impl/codec_gen.go:5418` |
| Newly persisted WAL staging | 1 | 1 | `internal/wal/wal.go:368` → `internal/wal/wal.go:351` |
| Apply owning value decode / index assignment | 0 | 0 | ParsePut / retained index sites above |
| **Follower input subtotal** | **2** | **2** | not a fixed cluster delivery population |
| Sender peer Entry.Data encode, separate occurrence | **+1** | **+1** | `internal/peer/peer.go:426` → `dependency-sources/protobuf-v1.36.11/encoding/protowire/wire.go:455` |
| **One sender occurrence + that new follower entry** | **3** | **3** | no assumed fanout/batch size |

Follower `dependency-sources/raft-v3.7.0/raft.go:1800` → `dependency-sources/raft-v3.7.0/log.go:127` → `dependency-sources/raft-v3.7.0/log_unstable.go:205` retains entry pointers, not the leader proposal clone. Full-value copies do not amortize as 1/batch-size: every copied entry contains each member value. Duplicate decode, retransmission, new WAL persistence/apply and recipient fanout have separate actual populations. No factor of two follower deliveries, one fsync/request or all-follower completion at quorum reply is presumed.

## 2. Repository channels — PR84 adds a conditional connection clock

Count successful send and selected receive separately; creation and close separately. Failed nonblocking sends/defaults, unselected cancellation arms, nil-channel attempts, Done() access, atomic operations and direct method calls are not channel operations.

Let N be requests in a fully consumed cohort; A actual consumed admission batches; Q actual persist-queue arrays; F actual synced-worker completion groups; O/I successfully queued-and-consumed outbound / delivered-and-consumed inbound peer messages at this node; U successful nonblocking Unreachable reports consumed; Sw/Rw successful writer wakes / selected idle-writer wake receives; Ticks selected core tick receives. In R84, **K_s/K_r** are successful Taken sends / selected reader Taken receives. These populations are distinct, not measured batch sizes or automatically equal between targets.

| Population / classification | Sends | Receives | Normal closes | Exact counted sites |
|---|---:|---:|---:|---|
| Fixed request-private inbox/reply | **0** | **0** | **0** | SubmitBatch M `internal/server/batch.go:126`; R84 `internal/server/batch.go:136`, no private reply channel |
| A — admission, batch-amortized | A | A | 0 | `internal/node/node.go:136`; alternatives M `internal/node/node.go:335`; R84 `internal/node/node.go:339` / M `internal/node/node.go:340`; R84 `internal/node/node.go:344` |
| Completion direct/atomic list | 0 | 0 | 0 | M `internal/node/node.go:432`; R84 `internal/node/node.go:436`; publish M `internal/server/batch.go:212`; R84 `internal/server/batch.go:255`; collect M `internal/server/batch.go:225`; R84 `internal/server/batch.go:268` |
| Q — persist queue arrays, amortized | Q | Q | 0 | M `internal/node/node.go:353`; R84 `internal/node/node.go:357`; alternatives M `internal/node/node.go:486`; R84 `internal/node/node.go:491` / M `internal/node/node.go:493`; R84 `internal/node/node.go:498` |
| F — actual synced-worker groups | F | F | 0 | M `internal/node/node.go:507`; R84 `internal/node/node.go:512` / M `internal/node/node.go:346`; R84 `internal/node/node.go:350` |
| O — outbound peer queues, protocol-dependent | O | O | 0 | `internal/peer/peer.go:304`; alternatives `internal/peer/peer.go:315` / `internal/peer/peer.go:321` / `internal/peer/peer.go:324` |
| I — inbound peer → inbox | I | I | 0 | M `internal/node/node.go:266`; R84 `internal/node/node.go:270`; same inbox receive alternatives |
| U — conditional successful Unreachable report | U | U | 0 | M `internal/node/node.go:274`; R84 `internal/node/node.go:278`; same inbox receive alternatives |
| Conditional/coalesced writer wake | Sw | Rw | 0 | M `internal/server/h2.go:201`; R84 `internal/server/h2.go:206` / `internal/server/write.go:52` |
| Background core tick | 0 | Ticks | 0 | M `internal/node/node.go:357`; R84 `internal/node/node.go:361` |
| **R84-only conditional/coalesced Taken clock** | **K_s** | **K_r** | **0** | `internal/server/batch.go:215` / `internal/server/batch.go:231` |

```
Main sends    = A + Q + F + O + I + U + Sw
Main receives = A + Q + F + O + I + U + Rw + Ticks
PR84 sends    = A + Q + F + O + I + U + Sw + K_s
PR84 receives = A + Q + F + O + I + U + Rw + Ticks + K_r
Follower sends    = Q_f + F_f + O_f + I_f + U_f
Follower receives = Q_f + F_f + O_f + I_f + U_f + Ticks_f
Normal-request explicit repository closes = 0
Fresh per-request private reply channels = 0
```

Divide actual populations by N for per-Put amortization; **do not invent batch sizes**. R84 creates **one additional capacity-one Taken channel per connection**, `internal/server/h2.go:125`, not one per request and not a completion/reply channel. The committer calls Taken directly after SubmitAll at `internal/node/node.go:453`; each callback attempts a nonblocking send, which may select default when its token is already buffered. Thus K_s ≤ actual Taken callback attempts (= consumed connection admission batches for this scope), not necessarily K_s=A. The reader sets its flag after successful handoff, `internal/server/batch.go:138`, but returns without a receive when no flag or a complete next frame is buffered, `internal/server/batch.go:227`. K_r is only the selected receipt; it is **not fixed per Put/batch/ReadFrame/syscall**. Tokens carry no request/batch IDs and can be coalesced; do not treat one receipt as proof the exact latest batch completed or applied. There is no explicit close of this channel in normal requests.

Across a connection interval, `K_r = K_s + initial_taken_token_occupancy - final_taken_token_occupancy`; similarly `Rw = Sw + initial_wake_occupancy - final_wake_occupancy`. Counts can cross measurement boundaries. There may be a buffered Taken token after useful work ends. Buffered frames still flow without waiting, and neither the availability predicate nor the clock directly establishes a socket-read or kernel-syscall population. No source proof of an invariant “one syscall per committer turn” is claimed.

Followers with no client workload have no A/frontend wakes/Taken clock for these leader-origin requests. Their Q/F/peer populations are independent. Receive alternatives are one receipt, not additive for one message. One directed peer message has queue send+receive at sender and inbox send+receive at receiver, four explicit operations across endpoints, not a fixed four per Put. Heartbeats/acks/control/commit-only work can pay channels without this value.

Both retain SubmitAll M `internal/node/node.go:448`; R84 `internal/node/node.go:452` / one pending backing array `internal/core/node.go:373`, no extra core channel. Unary delivery `internal/server/stream.go:139` returns before streaming signal `internal/server/stream.go:148`; streaming-only channel allocation M `internal/server/h2.go:578`; R84 `internal/server/h2.go:586` does not occur for Put. Unary end-input M `internal/server/h2.go:650`; R84 `internal/server/h2.go:658` and writer-space `internal/server/write.go:130` attempts are nil/default, zero. DATA credit wake M `internal/server/h2.go:392`; R84 `internal/server/h2.go:400` and completion wake M `internal/server/batch.go:216`; R84 `internal/server/batch.go:259` attempt the shared writer signal; only successful sends/selected receives count. Retained Node.Do creation/send/receive M `internal/node/node.go:281`; R84 `internal/node/node.go:285` / M `internal/node/node.go:420`; R84 `internal/node/node.go:424` / M `internal/node/node.go:286`; R84 `internal/node/node.go:290` is not this route. Node/watch/server-stop closes M `internal/node/node.go:308`; R84 `internal/node/node.go:312` / M `internal/node/node.go:394`; R84 `internal/node/node.go:398` / `internal/server/server.go:189`, Taken node-stop `internal/server/batch.go:234`, context cancellation, streaming/fault/library lifecycle channels are excluded, not asserted absent.

## 3. Unchanged lean workload and etcd reuse

Committed engine `experiments/bench75/load/newlean.go` is **byte-identical** to `e3bc29f5de3b9184b8aae5f64ed290681d6e1d6f` (verified, engine SHA256 in `workload-manifest.json`). Generator/harness files were read only at the final commit, not mutable working-tree source. Same original **4,096 Kube-shaped keys**, independent SHA256-derived **1KiB constant per-key values**, and global `i*2654435761%4096` selection: `experiments/bench75/load/main.go:121` / `experiments/bench75/load/main.go:125` / `experiments/bench75/load/main.go:128` / `experiments/bench75/load/newlean.go:128` / `experiments/bench75/load/newlean.go:551`. Put templates `experiments/bench75/load/newlean.go:50` and warmed HPACK blocks `experiments/bench75/load/newlean.go:296` / `experiments/bench75/load/newlean.go:303` are prebuilt. No retries or timed pre-reads.

**16 active TLS1.3 mTLS raw connections + 1 idle grpc control connection**; 64 or 256 total closed-loop slots (4 or 16 per active connection): `experiments/bench75/load/main.go:96` / `experiments/bench75/load/newlean.go:198` / `experiments/bench75/load/newlean.go:182` / `experiments/bench75/load/main.go:100`. Two raw client goroutines per connection (reader + owner), not one per RPC. **16 untimed warm Put RPC/writes per subprocess** precede the before revision probe and CPU snapshots: `experiments/bench75/load/newlean.go:224` / `experiments/bench75/load/main.go:146` / `experiments/bench75/load/main.go:153` / `experiments/bench75/load/main.go:262` / `experiments/bench75/load/main.go:279`. Warm writes are outside N/revision fencing.

No grpc-timeout in timed raw fields `experiments/bench75/load/newlean.go:253`; server timeout guard M `internal/server/h2.go:590`; R84 `internal/server/h2.go:598` does not arm M `internal/server/h2.go:593`; R84 `internal/server/h2.go:601`. Grpc control Range/probes have 10s deadlines outside the cohort. Actual DATA can fragment with send credit/maxFrame at `experiments/bench75/load/newlean.go:589`; TLS segmentation is not HTTP/2 DATA fragmentation. Receive credits coalesce at **64KiB**, `experiments/bench75/load/newlean.go:30` / `experiments/bench75/load/newlean.go:709` / `experiments/bench75/load/newlean.go:717`; 10s write-deadline renewal happens only with ≤5s remaining, `experiments/bench75/load/newlean.go:278`. These may affect actual control/wake/batch populations, not the verified owned request accumulator destination count. Five-rep harness `experiments/bench75/run.py:277`; requested Put64/256 suite supported at `.github/workflows/bench75.yml:88`, lean env `.github/workflows/bench75.yml:98`.

Stock etcd v3.7.2 and pins are unchanged. Reuse `evidence/bench75/run-38387706444/etcd-source-audit/audit.md`, its verified sites/dependency proofs, and sealed PR80 `lean-etcd-scope.md` **by reference**, all hashes checked in `reference-manifest.json`; no release/dependency refetch. Put known immediate leader stages remain **8 new input + conditional codec coalescing C**, including WAL PageWriter. **Deferred bbolt leaf-page serialization remains extra/separate**, excluded from that immediate subtotal. C is not asserted zero: implemented flow-control fragmentation does not establish one codec input buffer for every request. Peers have separate occurrence populations, no assumed batches/fanout. This is not a physical 4-vs-8 copy ratio.

Etcd explicit fixed request core remains **3 sends + 3 selected receives, 2 closes, 2 fresh reply channels**, plus actual Ready/scheduler/peer populations from the prior verified census. Its old extra grpc `cancelUpdated` deadline synchronization creation/close is guarded by `timeoutSet` (`deps/google.golang.org_grpc@v1.83.2/internal/transport/http2_server.go:602-615` in prior evidence) and **does not execute for timed raw calls without grpc-timeout**. Control deadlines are not charged per timed Put. No blind reuse of the old grpc-go client deadline/segmentation/population claims.

## 4. Headline CPU / proc IO boundary — no source-derived runtime counts

The requested headline is **leader CPU/Put** and leader **`/proc/PID/io` syscr/syscw deltas per Put**, not physical copies or logical frame calls. The bound client reads process CPU at `experiments/bench75/load/main.go:36`, IO counters at `experiments/bench75/load/main.go:57`, snapshots before/after at `experiments/bench75/load/main.go:262` / `experiments/bench75/load/main.go:288`, and reports arithmetic at `experiments/bench75/load/main.go:350` / `experiments/bench75/load/main.go:383` / `experiments/bench75/load/main.go:384`. Those are accounting source sites, **not inferred actual runtime results**. Syscr/syscw are process IO syscall counters, not quantities that can be recovered by counting source ReadFrame, TLS, WAL.Write or Sync sites; they are not fsync counts or cluster-wide totals.

PR84 is designed to delay otherwise-unbuffered reader work until a Taken signal, allowing later arrivals to be read/batched together. Source does **not** establish its realized syscall reduction, CPU/Put change, throughput, generator cost or correctness for all traffic. Parent will report actual validated five-rep ARM measurements. **No presumed 2.8µs generator baseline**, no substitution of PR80 run 38397810888 numbers, and no predicted PR84 performance. No benchmark/profile/Go execution was done here.

## Bottom line

**Put leader new-input stages: main 4, PR84 4.** One newly persisted/applied follower: **2 in each**; sender encoding adds **1 per actual occurrence**. Fixed request-private channels remain zero. **PR84 adds conditional/coalesced per-connection Taken sends K_s and receipts K_r**, not a fixed per-Put handoff or new payload buffer. Etcd reuse keeps **8+C** immediate input stages and **3s/3r/2 closes/2 fresh core replies**, omitting the inapplicable raw grpc deadline-sync close. No actual CPU/syscr/syscw outcome is inferred from source. Final artifacts are ready for parent archive/post on PR84 after measurement validation.
