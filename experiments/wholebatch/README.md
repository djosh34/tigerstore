# Whole-batch write path

Question: how much leader CPU does a batch admission and completion path remove after replacing the HTTP handler frontend, and how much more does a wire-backed command codec remove?

This is throwaway code for issue #74, experiment 2. Nothing on this branch is a proposed merge. The starting main commit is `eb8a20eda7b1436e3319b924753469f2d8c0ee99`.

Sources:

- Custom x/net HTTP/2 Framer and HPACK experiment, `exp/pre-47-grpc` at `d2408301966b99471bfed06d4c2dc1cd1e5af462`.
- Profile harness, `exp/pre-50-profile` at `1d90dcebf4ff9c910f7053dc0634d251767fd92b`.
- Group commit comparison, `exp/bench-68` at `32ceb80fb36fd4229c2e2e7594b40da197fe4eaf`.

## Controls

The default server remains unchanged. The prototype uses `TIGERSTORE_FRONTEND=wholebatch` and three modes:

| Mode | Admission and completion | Command codec |
| --- | --- | --- |
| `rpc` | One request at a time, legacy request handlers | Generated protobuf |
| `admit` | One admission per batch, completions grouped per connection | Generated protobuf |
| `full` | One admission per batch, completions grouped per connection | Wire-backed descriptors |

The `rpc` ablation intentionally retains per-RPC goroutines so a blocked write cannot stop the connection reader from admitting other streams. The full path has one reader and one writer per connection. Admission and completion change together in this experiment. Their separate CPU contributions cannot be inferred from that contrast.

The existing Raft owner still decides order, revisions and transaction outcomes. Replies still originate in committed application. This prototype does not execute writes speculatively or release successful replies before durable application. The asynchronous Raft persistence response dependencies remain unchanged.

## Measurement requirements

All performance measurements run on free `ubuntu-24.04-arm` runners. Each client-count job runs main, the prototype variants and etcd v3.7.2 back to back for five repetitions. The client counts are 16, 64 and 256. The generator uses 16 connections, 1 KiB values and 4,096 keys. Every process has a pinned physical CPU. The leader uses CPU 0, followers use 1 and 2, and the generator uses 3. Servers receive two Go processors. Filesystem barriers stay enabled. TLS and client certificates remain enabled.

Record process CPU per successful Put on every node, allocation count and bytes per Put, file fsyncs per Put, throughput and latency percentiles. Label instrumented samples and missing counters. Compare paired repetitions within each job. Do not treat a CPU profile percentage as a measured end-to-end reduction.

An earlier runner invocation included native frontend `2d4052e96939b12fd68f0252dfd1154127cdf7c6` and tuned native frontend `78db0c56fe2b051b651bbfa3f1aed03ca3724b9c`. Both start from the same main commit. Experiment 1 later dropped those incomplete prototypes without declaring a winner. The final runner replaces them with the fixed-ring and pooled-arena variants. See [HARNESS.md](HARNESS.md) for measurement windows, instrumentation, exact commands and validity checks.

## Memory ownership

| Storage | Readers and release condition |
| --- | --- |
| Framer DATA scratch | The reader consumes it before the next `ReadFrame`. It copies payload into owned stream reassembly storage. |
| Receive bodies and compact descriptors | The reader stops mutating a body before publication. The inbox and pending commands may reference it. The core copies each body once into an immutable entry, then clears compact pending references. A queued timeout or a dropped proposal also clears them. The reader never pools those buffers. |
| Pending descriptor storage | `SubmitBatch` allocates one array of pending records and descriptors. Individual command fields are cleared after encoding or completion, so one unfinished sibling does not keep completed receive bodies alive. Pending metadata may remain until timeout cleanup. Txn descriptor arrays still allocate separately. |
| Raft entry | Raft storage, retransmission, WAL jobs, peer queues and MVCC can all retain its bytes. These are GC-owned immutable allocations, not reusable receive buffers. A successful RPC is not permission to recycle an entry. |
| WAL encoding | WAL append copies the entry into its write buffer. The disk worker retains ordered jobs until sync completion. The prototype leaves these dependencies and persistence responses unchanged. |
| MVCC and reads | Values reference committed entry bytes. History, read results and watch results may retain those slices. The log still grows without checkpoints or cleaning. This experiment does not bound database memory. |
| Client responses | The owner sizes and encodes complete response frames after durable apply. The writer queue retains only those owned frame bytes, not input descriptors or entry slices. Bytes remain charged while queued, while flow-control-blocked, and through the writer's flush. |

The full path bounds each connection to 1,024 active or partial streams, 16 MiB of receive-buffer capacities and admitted receive bodies, and 16 MiB of encoded responses. It reserves response bytes before encoding. An oversized response or full queue aborts the connection rather than waiting on the owner. These are per-connection budgets, not a process-wide memory limit. Framer, TLS, fixed reader/writer buffers, descriptor metadata, maps and kernel buffers are additional memory. Legacy protobuf copies in `rpc` and `admit` are intentionally outside the receive-buffer budget.

Aborting a TLS client closes the raw transport. A source review found that graceful `tls.Conn.Close` could make the owner write `close_notify` and block on a nonreading peer. `TestTLSAbortDoesNotWriteToSlowPeer` fails with the original closure and passes with the raw-transport abort. The initial runner invocation was cancelled after that finding; its results are not used.

The prototype is not a zero-allocation inner loop. Put validation and apply still use typed adapters. Txn validation and apply still construct typed request graphs, but no generic protobuf decoder runs on compact commands at apply and no decoder copies their values. MVCC, responses, Raft and peer protobufs still allocate. No allocator or reclaimable ring is implemented.

## Compatibility scope

Generated gRPC clients can use Put, supported Txn, Range, Status and MemberList. Range, DeleteRange and Compact use the legacy batch fallback. Full Txn supports single-key compares, Put branches and point Range branches. Unsupported Txn fields return Unimplemented. The other modes retain the ordinary core's broader Txn semantics.

This is not ready for kube-apiserver. Watch and Lease streaming behavior are missing. GET `/health` and `/version` now use the same TLS port over HTTP/1.1 and custom HTTP/2, without grpc-go on that listener. HTTP/2 health uses the existing batched owner read path. Request compression, grpc-timeout metadata and the complete HTTP/2 state machine are not implemented. Admission-limit violations close the connection instead of returning a per-stream ResourceExhausted status. Responses above the 16 MiB budget also close the connection.

Stock clientv3 needs those streaming methods, retry and cancellation behavior exercised under endpoint changes, and metadata handling beyond this unary subset. Basic etcdctl Put/Get/Txn can use the supported calls, but Watch and Lease cannot. `etcdctl endpoint health` also calls AlarmList, which this prototype and the pinned main do not implement. The full RPC frontend still needs Watch create/cancel, ordered replay, compaction responses, progress and slow-consumer restart; Lease grant/revoke/TTL/list and the bidirectional keepalive stream; Maintenance methods beyond Status; grpc-timeout parsing and cancellation; require-leader metadata; graceful GOAWAY/draining and late-stream rules. kube-apiserver also needs its remaining transaction/read forms and the request-size and error boundaries already covered by the normal compatibility work. Health/version GET probes do not exercise those RPCs.

## Targeted checks

The runner executes race tests for `internal/batchcmd`, `internal/batchfront`, `internal/core`, `internal/node` and the core determinism lint. New tests cover wire-byte ownership, receive-buffer poisoning after entry encoding, MVCC aliasing, durable-before-reply, crash replay, CAS success/failure, atomic rejection, full Put fields, timeout cleanup, buffered batch admission, prebuilt response ownership, flow-control-blocked reservations, queue-full owner progress and idle writer deadlines.

`TestBatchColdQuorum` writes through all three TLS endpoints, runs successful and failed CAS, stops all nodes, and cold-starts each possible surviving pair. It checks all acknowledged values. This is a focused recovery test, not a fresh whole-system chaos campaign. The unchanged simulator paths alone do not certify the new frontend.

## Result

The [final report](../../evidence/wholebatch/REPORT.md) records 105 passing measured windows on candidate `948395e603211cd4753c89b24d6bcb9d0dabdfcc`. Shared-port HTTP/2 health/version probes ran during Put load. Full mode reached 26,685 and 42,030 Put/s at 64 and 256 clients, with 32.33 and 22.33 leader CPU microseconds per Put. Its paired leader CPU reductions against main were 69.50% and 77.85%.

Fixed rings and recycled batch arenas reduced allocations but increased CPU and retained memory. They are bounded transient-buffer experiments, not allocation-free implementations. The connection ledger mutex and blocking inbox admission remain. Arena chunk capacity may exceed per-connection payload accounting; each direction has a separate 80 MiB bank limit per server, plus 28 MiB of typed descriptor arrays across both directions. There is no global connection-count or whole-database memory bound.
