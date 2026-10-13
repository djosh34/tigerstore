## Experiment 2: whole-batch writes

The ordinary whole-batch variant won this test. At 64 and 256 clients it used 32.33 and 22.33 microseconds of leader CPU per Put. Paired reductions against main were 69.50% and 77.85%. Preallocated rings and recycled batch arenas reduced allocations but used more leader CPU and retained more memory.

The allocation experiment is incomplete against the owner's full target. Neither allocator makes the entire hot path allocation-free. Both still use the connection ledger mutex and the inherited blocking inbox admission. This is evidence for the next implementation, not code to merge.

[Throwaway branch](https://github.com/djosh34/tigerstore/tree/exp/pre-50-wholebatch). Nothing merged and no PR opened.

### Sources and commands

- Measured candidate and harness: `948395e603211cd4753c89b24d6bcb9d0dabdfcc`.
- Main: `eb8a20eda7b1436e3319b924753469f2d8c0ee99`.
- Stock etcd v3.7.2. Its binary SHA-256 is `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d`.
- Starting Framer/HPACK prototype: `exp/pre-47-grpc` at `d2408301966b99471bfed06d4c2dc1cd1e5af462`.
- Harness sources: `exp/pre-50-profile` at `1d90dcebf4ff9c910f7053dc0634d251767fd92b` and `exp/bench-68` at `32ceb80fb36fd4229c2e2e7594b40da197fe4eaf`.
- [Final run 38371258674](https://github.com/djosh34/tigerstore/actions/runs/38371258674). All three paired jobs and the race-test job passed. Jobs took less than 12 minutes.

On disposable free `ubuntu-24.04-arm` runners only:

```sh
git checkout 948395e603211cd4753c89b24d6bcb9d0dabdfcc
CLIENTS=16 timeout --kill-after=5s 780s bash experiments/wholebatch/run.sh
# Separate jobs use CLIENTS=64 and CLIENTS=256.
```

The script exports `GOFLAGS=-p=2 GOMEMLIMIT=2GiB`. It archives both exact sources and adds the same counting/debug overlay. The archived binaries, overlay, commands and hashes are in the evidence. The generator uses `-total 30000 -workers 16|64|256 -conns 16 -health-interval 100ms`.

Every job runs main, etcd and all five candidate controls back to back for five repetitions. Leader CPU 0, followers 1 and 2, generator 3. All servers have two Go processors; the generator has one. Client and peer TLS remain enabled. Ext4 barriers remain enabled. Main and the prototype use Go 1.27.2; the stock etcd binary uses Go 1.26.8.

The workload is leader-only closed-loop 1 KiB Put over 4,096 keys, with 4,096 seed writes and 8,000 warmup writes before each measured window. One additional persistent HTTP/2 connection alternates GET `/health` and `/version` every 100 ms on the same leader TLS port. Its CPU and drain time are inside the measured windows. Every probe succeeded. Separate curl checks verified both endpoints over HTTP/1.1 and HTTP/2 for all 105 clusters. There is no grpc-go server on the candidate's active client listener.

[Experiment 1](https://github.com/djosh34/tigerstore/issues/74#issuecomment-6141258914) dropped its native candidates and declared no frontend winner. The final comparison therefore uses its exact main source, not a purported native winner. Earlier incomplete native controls remain in the archived preliminary run and are not recommended.

### What the stages change

| Variant | Path |
| --- | --- |
| `main` | Existing ServeHTTP frontend and individual Node.Do requests. |
| `rpc` | Custom Framer/HPACK, generated protobuf, individual Node.Do calls and per-RPC goroutines. |
| `admit` | Batched admission and per-connection completion, with the legacy protobuf command path. |
| `full` | Compact wire-backed Put/Txn descriptors, batch-owned pending records, exact-capacity entry encoding, entry-backed MVCC values and no generic protobuf decode per compact command at apply. |
| `ring` | Full plus preallocated transient byte banks and fixed command/reply slots. |
| `pool` | Full plus bounded, lazily allocated batch chunks recycled between batches. Same slots, classes and byte budgets as ring. |

Admission and completion change together. Their individual CPU contributions are not isolated. The wire stage also changes value ownership, entry allocation and compact pending-reference cleanup. These are end-to-end contrasts, not attribution to individual functions.

Successful write responses are encoded after durable application and before the writer handoff. This is not speculative precommit execution. Ordinary full mode has one reader and one writer per gRPC connection, with no per-Put goroutine or reply channel. It flushes already-buffered requests without a fill timer. At low concurrency many admitted batches still contain one command.

### Throughput, latency and per-node costs

Each cell is the median of five per-run statistics. CPU, allocation and sync triples are leader / follower 1 / follower 2. Latency triples are p50 / p95 / p99 in milliseconds. CPU is process user plus system microseconds per successful Put. Allocation counts include all process goroutines.

| Clients | Variant | Put/s | Latency ms | CPU us/Put by node | Allocations/Put by node | Puts/file sync by node |
| ---: | --- | ---: | --- | --- | --- | --- |
|16|main|5708|2.70/3.82/5.73|161.33/44.67/39.67|160.37/18.27/18.24|4.35/3.91/3.94|
|16|etcd|3445|4.20/6.92/11.13|260.67/83.00/76.33|299.55/105.83/105.82|4.54/4.65/4.62|
|16|rpc|9540|1.63/2.27/3.03|85.67/44.33/37.67|60.48/19.47/19.40|4.02/3.95/4.03|
|16|admit|8799|1.77/2.49/3.33|92.33/51.00/43.00|62.16/20.27/20.15|3.84/3.79/3.86|
|16|full|9752|1.60/2.24/2.92|81.67/45.00/38.33|56.61/18.21/18.11|3.91/3.85/3.89|
|16|ring|9077|1.74/2.37/2.84|93.67/45.00/39.00|47.35/17.68/17.59|3.96/3.78/3.94|
|16|pool|9052|1.73/2.37/2.83|93.67/45.33/38.67|47.57/17.93/17.81|3.88/3.72/3.82|
|64|main|9058|6.60/10.41/15.81|105.67/16.67/14.67|143.89/10.28/10.28|16.12/14.49/14.52|
|64|etcd|5399|11.19/19.29/22.99|182.67/43.67/41.00|275.75/92.12/92.63|12.38/16.46/16.30|
|64|rpc|22687|2.64/4.37/7.01|40.33/16.33/14.33|43.44/10.53/10.52|14.96/14.03/14.13|
|64|admit|25579|2.33/4.06/5.94|35.00/16.67/14.33|42.05/10.77/10.74|14.48/13.91/14.31|
|64|full|26685|2.23/3.82/5.49|32.33/15.33/13.33|36.73/8.85/8.83|14.60/14.41/15.20|
|64|ring|23848|2.48/4.12/5.63|36.33/16.33/14.00|28.78/8.68/8.65|14.42/13.79/14.01|
|64|pool|24450|2.46/3.94/5.56|36.00/16.00/13.67|28.69/8.63/8.61|14.53/13.82/14.17|
|256|main|9570|24.87/42.52/54.65|102.00/9.33/8.67|144.77/10.18/10.18|13.09/41.15/40.87|
|256|etcd|5919|42.08/63.64/78.34|168.33/31.33/30.67|264.78/82.54/81.76|14.50/44.58/45.73|
|256|rpc|34743|6.60/13.02/17.17|28.00/8.33/8.00|38.74/8.36/8.35|46.58/42.61/42.92|
|256|admit|40757|5.55/11.13/13.01|23.33/9.00/8.33|36.97/8.64/8.64|37.78/34.52/35.38|
|256|full|42030|5.51/10.70/12.92|22.33/9.33/8.33|32.79/6.82/6.82|32.64/30.86/31.81|
|256|ring|39953|5.86/10.97/13.26|24.00/8.00/7.67|24.76/6.47/6.47|42.61/38.12/39.01|
|256|pool|38354|6.01/11.67/14.07|24.67/8.33/7.33|24.79/6.49/6.49|42.08/37.45/38.31|

The raw tables also contain allocation bytes, generator CPU, p90, p99.9 and per-node retained heap. CPU and allocation windows do not fence follower catch-up, so a follower's last work can fall outside them. Allocation scrapes bracket the CPU window and include small observation overhead. File-sync counters use the wider fully applied cohort on every node. Etcd additionally waits for its post-WAL.Save gauge before sampling. Tigerstore counts file fsyncs; etcd counts WAL sync observations, not its separate backend commits. Directory syncs are excluded. No intrusive profiler runs in these windows.

### Fraction of leader CPU removed by each step

These are medians of paired per-repetition reductions, normalized to that repetition's main CPU/Put. Negative means a regression. Independent medians do not necessarily add to the median total.

| Clients | Main to custom RPC | RPC to batch admission/completion | Batch to wire-backed full | Main to full total |
| ---: | ---: | ---: | ---: | ---: |
| 16 | 47.60% | -0.21% | 3.80% | 49.28% |
| 64 | 61.71% | 5.38% | 2.52% | 69.50% |
| 256 | 72.55% | 4.90% | 0.98% | 77.85% |

Against the immediately preceding stage, admission/completion removed -0.40%, 14.05% and 17.72%; the wire stage removed 6.76%, 7.62% and 4.35%, in client-count order. There was no admission win at 16 clients. Do not assign the combined admission/completion result to admission alone.

### Rings versus pooled arenas

Both use 80 MiB input banks, 80 MiB output banks and 28 MiB typed command/reply arrays per server. Ring allocates all buffer bytes at startup. Pool allocates chunks on first use and recycles them; it materialized 32 MiB of buffer bytes on the measured leader. Both use the same refcounts and rotating fixed slots. This is a bounded segmented allocator comparison, not a completed allocation-free owner.

GC figures below are Go runtime `/cpu/classes/gc/total:cpu-seconds` accounting per Put, not sampled process CPU. Retained heap is after eight quiet seconds and forced GC, outside every headline window. It includes log/history and the retained allocator banks. RSS and all-node snapshots are in the raw evidence. Stock etcd does not expose the same GC counter here.

| Clients | Variant | Leader allocated bytes/Put | GC accounting us/Put | Leader retained heap MiB |
| ---: | --- | ---: | ---: | ---: |
|16|main|21474|3.005|96.7|
|16|full|7587|1.022|54.3|
|16|ring|5759|0.481|242.3|
|16|pool|5769|0.823|114.4|
|64|main|21980|2.992|99.0|
|64|full|6953|0.783|55.4|
|64|ring|5052|0.374|243.5|
|64|pool|5047|0.810|115.4|
|256|main|21902|4.450|100.9|
|256|full|6871|1.009|55.0|
|256|ring|4937|0.395|243.1|
|256|pool|4936|1.178|115.0|

Against ordinary full mode, ring increased paired leader CPU by 13.50%, 12.37% and 7.46%. Pool increased it by 13.47%, 12.37% and 9.84%. Lower allocation counts did not produce a CPU win. All 90 allocator-node retained snapshots had zero bank rejections and zero pinned chunks after cleanup. Startup cost itself was not timed.

The source still allocates core pending batches, typed request/response adapters, MVCC state, Raft protobufs and transport objects. Txn descriptor arrays also allocate. The ledger mutex remains, and SubmitBatch can wait for the inherited inbox. The zero-allocation and no-blocking goals remain unmet. These numbers reject these two allocator implementations as speed improvements on this workload; they do not reject every possible arena or preallocated owner design. Refcount/slot overhead and changed GC pacing are possible explanations, not profiled causes.

### Channel operations and value copies

Main's Node.Do boundary performs one admission send and one reply-channel send per Put, plus their receives. It allocates a reply channel for each RPC. Full mode has no per-Put reply channel. Its measured Put handoffs were:

| Clients | Full admission sends/Put | Full writer-batch sends/Put | Frontend control sends/Put |
| ---: | ---: | ---: | ---: |
|16|1.000|1.000|3.033|
|64|0.743|0.276|1.563|
|256|0.931|0.088|1.165|

The first two columns subtract the separately counted health/version handoffs. Control counts include that small probe workload. They count successful sends, with corresponding receives on completed handoffs, not every runtime channel operation. HTTP/2 controls still cross individually. Main's grpc-go internal channels and both stores' Raft/WAL/peer channels are not counted here. Completion grouping was much stronger than reader-side admission grouping at 256 clients.

Code-visible value copies after an assembled request body:

| Step | Main | Full, ring and pool |
| --- | --- | --- |
| Body into decoded request value | 1 | 0, borrowed slice |
| Command into Raft entry | 1, plus possible buffer-growth copies | 1, exact reserved entry |
| Committed entry into MVCC value | 1 | 0, borrowed slice |
| Retained database value copies after pending cleanup | 2 | 1 |

These are code-path copy counts, not a count of all memcpy calls. Framer reassembly, TLS, kernel I/O, WAL encoding and peer-message serialization still copy bytes. The custom reader must copy Framer DATA before ReadFrame reuses it. Reads and watch event objects then reference the log-backed value; outward serialization copies from those slices into owned response bytes. The custom Watch RPC itself is not implemented yet.

`TestWireReadsAndWatchValuesReferenceLog` checks pointer identity for current/historical Range values, watch event values and prev_kv against the committed entry, and checks that response encoding owns its output. Receive-buffer poisoning, transient reuse and cold recovery tests also pass. No generic protobuf unmarshal runs per compact Put/Txn during apply, but typed adapters still allocate.

### Lifetimes, slow clients and large values

- The reader owns reassembly bytes until publication. Admission descriptors borrow immutable bodies. Default full clears compact references after entry encoding or completion. Arena modes conservatively retain receive/descriptor regions through real completion, including reset and late completion after disconnect.
- Raft entries are separate immutable GC-owned allocations. Raft storage, WAL jobs, peer queues, retransmission and MVCC may all retain them. An RPC acknowledgement never permits recycling those bytes. The WAL copies entries into its write buffer; ordered sync completion still releases Raft's persistence responses.
- MVCC, read responses and watch events may reference an entry. Encoded client frames own separate bytes. Slow clients do not pin log slices through the writer queue. The log and history still grow without cleaning, so this is not a whole-database memory bound.
- Each connection allows 1,024 partial/admitted/response-pending streams and accounts at most 16 MiB of input buffers and 16 MiB of encoded response bytes. In arena modes, chunk capacity can exceed useful wire bytes; the server-wide 80 MiB bank bounds each direction. One slow client can pin whole chunks. Descriptor banks and TLS/HTTP buffers are additional memory. There is no global connection-count or total-process byte cap; the banks bound their own bytes, not the entire server.
- The owner sizes responses before encoding and never waits for writer-queue space. Reservations last through actual Flush or drop. Full banks or queues abort the connection before overwriting live bytes. Bank acquisition does not wait. This does not remove the separate ledger mutex or inbox wait noted above.
- Tests accept a 1,572,736-byte value across repeated connections in both allocator modes. A literal 1.5 MiB value is correctly rejected because the key and InternalRaftRequest wrapper make the request exceed etcd's 1.5 MiB limit. Tests cover zero HTTP/2 credit, large response classes, the response cap, reset, closed-client completion and no overwrite on exhaustion. Large-value and slow-client throughput were not measured.

### Guarantees and compatibility

The Raft persistence methods, disk ordering, WAL and MVCC implementation are unchanged. Compact commands enter the existing ordered proposal/apply path. Successful write completions come only from the core's replies after the required durable application; there is no speculative state or early success response. Entry kinds 9/10 are experimental and require the prototype binary on every replica, not a mixed-version rollout.

The runner passed:

```sh
GOFLAGS=-p=2 GOMEMLIMIT=2GiB go test -race -count=1 -parallel 2 -timeout 5m \
  ./internal/batchcmd ./internal/batchfront ./internal/batchmem \
  ./internal/core ./internal/node ./internal/lint
```

Checks include durable-before-reply, crash replay, CAS success/failure and atomic rejection, receive/entry aliasing, flow control and byte bounds. The three-node TLS test writes through every endpoint and cold-starts each surviving pair without the third node. This is focused coverage, not a new whole-system chaos campaign or proof of every compatibility guarantee.

One review found that TLS Close could write close_notify and block the owner on a nonreading client. The raw-socket abort fixes it. Its regression fails with the old close and passes with the fix. The initial run was cancelled rather than used as evidence.

Stock generated clients and etcdctl can use the supported unary calls. Full Txn is limited to single-key compares, Put branches and point Range branches. Other forms return Unimplemented. HTTP health/version now work on the same port, but basic HTTP health is not `etcdctl endpoint health`: the latter also calls AlarmList, which this prototype and pinned main do not implement.

Before stock clientv3 and kube-apiserver can use this as a replacement, the frontend still needs Watch multiplexing, replay, compaction cancellation, progress and slow-consumer recovery; Lease RPCs and the keepalive stream; Maintenance beyond Status; grpc-timeout and cancellation handling; require-leader metadata; and proper GOAWAY/draining. Request compression and the complete HTTP/2 state machine are also missing. MemberList retains main's endpoint-list limitation. No Kubernetes or full clientv3 compatibility claim follows from these unary measurements.

### Evidence and limits

`summary.csv`, `runs.csv`, `paired.csv` and `stage-cpu.json` accompany this report. `run-38371258674.tar.gz` contains all raw final artifacts, exact commands, binary hashes, HTTP checks, counter snapshots and test output. Offline reproduction:

```sh
mkdir raw
tar xzf evidence/wholebatch/run-38371258674.tar.gz -C raw
python3 experiments/wholebatch/summarize.py raw
python3 experiments/wholebatch/analyze.py raw
```

The successful Put-only control is archived as run 38368777795. It is not substituted for the final live-health workload. The earlier pre-HTTP run 38363905577 passed 64/256 clients but timed out during the last 16-client native control; it remains labelled partial. Run 38363438163 was cancelled after the TLS-close finding. None is presented as a passing final matrix.

All 105 final windows had 30,000 successful Puts, zero RPC errors, the exact revision delta, stable leadership/term, correct CPU placement and fully applied sync fences. The conclusions cover this closed-loop 1 KiB workload on these runners. They do not establish open-loop behavior, regional delay, mixed follower ingress, watch load, compaction load or a zero-allocation steady state. No local benchmark ran. All helpers and their worktrees are deleted. Only exp/ branches were pushed.
