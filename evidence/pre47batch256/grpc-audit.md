# gRPC concurrency and flow-control audit

## Scope and result

Audited checkout `79bd08f326ec8c02e06c9fbc1e8f8373ec5234d7`, before this audit's instrumentation. The measured prototype was `fad17fe3fe64c73075da484c773f4859bc4e8e18`; the paired baseline was `81053bcc54ac315b6ac9302dbe09b5eb591c18db`. A local Git comparison found no differences between measured source and audited checkout in `internal/server/server.go`, `internal/node/node.go`, `experiments/pre50profile/load/main.go`, or `experiments/pre47groupcommit/run.py`.

Read `../issue46.md`, `../issue1.md`, `../design-main.md`, and the unslop instructions. No runner jobs, issue comments, pushes, subagents, or full builds were started. The only code changes are diagnostic server files, requested by the parent after the read-only audit began.

There is no gRPC stream limit of 16, no one-RPC-per-connection rule, and no handler mutex that serializes Puts. The default HTTP/2 windows also do not impose a 256-client admission limit in this workload. Sixteen outstanding 1 KiB Puts use about 17 KiB of DATA credit on each connection, below its initial 65,535-byte window.

The likely explanation is small decoded-arrival groups on a serial persistence clock, with extra leader persistence rounds for hard-state changes. Closed-loop clients wait throughout the RPC, not just for enqueueing into the next append group. gRPC and Go scheduling can change the grouping of arrivals and replies without a hard concurrency limit. This is a hypothesis, not a measured attribution. Count entry-bearing and hard-state-only syncs before treating 1.36 Puts/fsync as a logical batch size.

## Exact dependency sources

The runner's `evidence/pre47groupcommit/c256/versions.txt` records grpc-go `v1.84.0`, etcd API `v3.7.2`, Raft `v3.7.0`, x/net `v0.58.0`, protobuf `v1.36.11`, and Go `1.27.2` for the prototype and generator. It records grpc-go checksum `h1:soMyaPJ8pAak5PIQ0DGBUir0XRo2fRoMqhNWMLlLxO0=`. Stock etcd is `3.7.2`, SHA `68c065e`, built with Go `1.26.8`.

I inspected the installed exact-version sources, not current upstream documentation. Dependency references below resolve under `/home/joshazimullah.linux/go/pkg/mod/`:

- `grpc/` means `google.golang.org/grpc@v1.84.0/`.
- `api/` means `go.etcd.io/etcd/api/v3@v3.7.2/`.
- `raft/` means `go.etcd.io/raft/v3@v3.7.0/`.

### Handler concurrency

| Source | Evidence |
| --- | --- |
| `internal/server/server.go:46-58` | Native `grpc.NewServer` with `MaxConcurrentStreams(1 << 20)`. No stream-worker pool, unary interceptor, or static flow-control setting in the measured source. |
| `grpc/server.go:1071-1111` | One connection context and handler quota per transport. Each admitted stream calls `go f()` unless an explicitly configured stream worker accepts it. No workers are configured here. The quota is 1,048,576 per connection, not across the server. |
| `grpc/internal/transport/http2_server.go:180-205,552-564` | The server advertises its configured stream limit in SETTINGS and refuses streams only when that connection's active-stream count reaches the limit. |
| `grpc/internal/transport/defaults.go:33`; `grpc/internal/transport/http2_client.go:1322-1368` | The client starts with a provisional quota of 100 streams. The first server SETTINGS changes it to the advertised limit. Even the provisional quota exceeds the 16 workers assigned to a connection. |
| `grpc/internal/transport/http2_client.go:857-890` | New streams wait only when `streamQuota <= 0`; otherwise each stream consumes one slot. This is multiplexing, not a mutex held until a previous RPC completes. |
| `api/etcdserverpb/rpc_grpc.pb.go:246-262` | `_KV_Put_Handler` decodes a request then calls `KVServer.Put`. |
| `internal/server/server.go:77-89,109-115`; `internal/node/node.go:196-211` | Put calls `Node.Do`, which enqueues into the inbox and waits on that request's own buffered reply channel. The wait does not hold a connection lock. |

HTTP/2 framing remains serial within each direction of a connection. `grpc/internal/transport/http2_server.go:646-709` has a connection reader that dispatches headers, DATA, SETTINGS, and WINDOW_UPDATE. `grpc/internal/transport/controlbuf.go:574-658` has one loopy writer that drains queued work and flushes it. These goroutines share the server's single Go P with handlers, the committer, and other work. That is a scheduling constraint, not a stream-count cap.

### Windows and credit

| Source | Evidence |
| --- | --- |
| `grpc/internal/transport/defaults.go:27-45` | Initial connection and stream flow-control windows are 65,535 bytes. Per-stream application write quota is 65,536 bytes. These are different limits. |
| `grpc/internal/transport/http2_server.go:193-224,301-306` | The server starts with default receive windows and enables a BDP estimator unless static windows were requested. Enlarging connection credit uses WINDOW_UPDATE on stream 0; enlarging stream credit uses SETTINGS_INITIAL_WINDOW_SIZE. |
| `grpc/internal/transport/http2_client.go:317-318,390-398` | The client also starts with default receive windows and dynamic BDP estimation. Neither generator nor server configures static windows in the original experiment. |
| `grpc/internal/transport/http2_server.go:775-842` | Connection receive credit updates when transport DATA arrives, independently of application reads or commit. Waiting for Put durability does not hold connection credit until the response. |
| `grpc/internal/transport/flowcontrol.go:81-112,185-226` | Connection updates occur after about one quarter of the window. Stream updates follow application consumption. A 1 KiB unary request fits its stream window without waiting for a refill. |
| `grpc/internal/transport/flowcontrol.go:54-66`; `grpc/internal/transport/http2_server.go:1161-1196` | Write quota limits how much each stream can queue. The first small Put response can queue immediately; successful queueing does not prove socket flush or peer receipt. |
| `grpc/internal/transport/controlbuf.go:950-1058` | Actual DATA transmission uses the minimum of frame allowance, stream credit, and connection credit. `sendQuota == 0` stops DATA output, but does not create a one-stream-only transport. |
| `grpc/internal/transport/bdp_estimator.go:27-43,85-139` | Dynamic windows can grow to 16 MiB. Growth requires a sample at least 0.66 times the current estimate and the highest observed sample bandwidth. Defaults are starting values, not measured steady-state credit. |
| `grpc/server.go:280-334` | Both legacy InitialWindowSize options and the explicit Static options disable dynamic control in this exact version. A static connection option alone also disables dynamic stream control. The requested ablation sets both. |

The generator's keys are 34 bytes. A PutRequest with that key and a 1,024-byte value encodes to 1,063 protobuf bytes, then a five-byte gRPC prefix makes 1,068 DATA bytes. Thus 16 workers per connection can initially send `16 * 1068 = 17088` bytes. HTTP/2 header blocks do not consume DATA flow credit. A connection WINDOW_UPDATE is already eligible near that amount. The sixteen initial connection windows total 1,048,560 bytes, while the complete 256-request cohort carries 273,408 DATA bytes.

This arithmetic argues against default credit preventing the initial cohort from reaching the server. It does not prove that later cohorts never stall. Unprocessed WINDOW_UPDATE frames, delayed reads, or queued output can temporarily exhaust credit. The actual SETTINGS/WINDOW_UPDATE sequence must settle that question. Server receive-window settings govern client-to-server DATA; they do not directly enlarge the client's receive windows for responses.

### Generator scheduling and runner layout

`experiments/pre50profile/load/main.go:62-64,84-102,149-207` establishes the following:

- There are 256 worker goroutines and 16 independent gRPC ClientConns. Every connection targets the chosen leader directly. This is not clientv3 endpoint balancing or follower forwarding.
- Each connection performs a successful Range before timed work. The start channel releases all workers together.
- Worker `w` always uses `clients[w % conns]`, so each connection has sixteen closed-loop workers. A worker starts its next Put only after the previous Put returns and its latency/header accounting completes.
- A shared atomic counter assigns operation numbers. There is no pacing timer, token bucket, batch barrier, or explicit `Gosched` in the generator loop.
- The measured latency includes client serialization, scheduling, transport queues, server work, commit, and response completion. `grpc/call.go:69-95` sends the unary request and waits for both response and trailers before returning.

`experiments/pre47groupcommit/run.py:76,99-115,174-180` pins leader, two followers, and generator to cores 0, 1, 2, and 3. Every process has `GOMAXPROCS=1`. Each load invocation is a new process, including seed, warmup, and measured Put. The harness never passes `-conns`, so the default 16 applies. Warmup connections and their BDP estimates do not survive into the measured invocation.

`grpc/internal/transport/controlbuf.go:574-658` explicitly batches writes until queued work runs out. If its buffered batch is under 1,000 bytes, it calls `runtime.Gosched()` once before flushing. `grpc/clientconn.go:142-143` and `grpc/server.go:188-196` set 32 KiB read/write buffers. The gRPC writer's batching and yielding differ from the append clock and may shift the phases of reply-driven request cohorts.

The recorded runner is a four-core ARM Neoverse-N2 VM. `c256/environment.txt` initially shows ext4 with `nobarrier`; `experiments/pre47groupcommit/run.sh:22-25` remounts with barriers before measurement and verifies the resulting options. The three WALs share the runner disk. CPU affinity verifies process placement, not isolation from kernel or VM work.

## What 1.36 Puts/fsync means

`evidence/pre47groupcommit/summary.csv` reports the prototype medians at 256 clients:

| Quantity | Value |
| --- | ---: |
| Throughput | 4,922.44 Put/s |
| Mean latency | 51.9406 ms |
| Leader Puts/file fsync | 1.35737 |
| Follower Puts/file fsync | 41.1805 and 41.4938 |
| Leader occupied cores | 0.3815 |
| Generator occupied cores | 0.1428 |

These are medians of separate per-run statistics. Do not multiply independent medians to claim an exact measured rate or time. For the five individual prototype runs, calculations from each `put.json` and `disk.json` give:

| Repetition | Put/s | Leader syncs | Puts/sync | Mean file Sync wall time, us | Sync wall / client elapsed | Warmup Put/s |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4922.4 | 41370 | 1.450 | 216.6 | 0.735 | 28612.3 |
| 2 | 4630.9 | 45384 | 1.322 | 207.4 | 0.726 | 17142.2 |
| 3 | 5122.1 | 44203 | 1.357 | 186.7 | 0.704 | 5118.0 |
| 4 | 4481.4 | 47565 | 1.261 | 203.9 | 0.725 | 8110.5 |
| 5 | 5269.0 | 38852 | 1.544 | 213.4 | 0.728 | 9337.7 |

The disk counter interval extends through the replica fences, beyond client CPU timing. Sync wall time includes descheduling and syscall latency, not just hardware service time. It can overlap CPU work, so CPU occupancy plus Sync time is not a time budget.

In each run, throughput times mean latency is about 255.7 operations. Nearly all workers therefore remain inside an RPC on average. That does not place 256 decoded requests in the leader's inbox. Requests can wait for replication/apply or sit elsewhere in the client/transport/reply path. The generator's low CPU occupancy does not prove it cannot introduce scheduling gaps, but it argues against a continuously CPU-saturated generator being the sole bottleneck.

At the observed throughput and 187 to 217 us average Sync calls, a smooth arrival process supplies only about one new Put per Sync interval. Increasing the number of closed-loop workers while throughput stays near 5,000/s mostly increases waiting time. It does not force the batch builder to see 256 arrivals during the next append.

There is also a denominator problem:

- `internal/node/node.go:244-306` holds decoded client requests outside the core during an append. On completion it submits the deferred cohort plus a snapshot of queued inbox inputs. Only arrivals available at that point enter the next Process. The inbox correction does not wait for runnable gRPC handlers that have not yet enqueued.
- The same loop skips `Process` entirely while `inflight`. It gates Ready processing, apply, replies, and outgoing control effects as well as proposal generation. This differs from the intended fully pipelined design.
- `internal/core/node.go:422-440,448-489` proposes queued requests, then turns every local append message into a persist job. A hard-state job can have no entries.
- `raft/rawnode.go:152-173,189-208,223-244` generates a storage-append message for changed hard state even without new entries. `MustSync` is true for entries or term/vote changes, not a commit-only change. The prototype driver does not use that distinction.
- `internal/node/node.go:368-401` buffers all jobs in the group, keeps its final hard state, and always calls WAL.Sync. `internal/wal/wal.go:339-355,376-397` writes and syncs a dirty buffer containing only a hard-state record as well as buffers containing entries.
- `internal/osfs/pre50.go:16-18,32-40` counts file Sync calls, including segment-header syncs, but excludes directories. Empty clean WAL.Sync calls do not produce a file Sync. `RESULTS.md` explicitly warns that this metric is gross sync density rather than logical Raft-entry batch size.

Therefore 1.36 does not imply the leader proposes 1.36 Puts per entry. Commit-only persistence rounds can lower this ratio. The sources prove those rounds are possible, not how many occurred in this run. Followers can collect many leader entries per received/durable group while the leader persists smaller groups and separate commit updates. Their roughly 41 Puts/sync do not prove that incoming gRPC requests reached the leader in groups of 41.

My first hypothesis is excessive leader persistence rounds combined with reply-driven, fragmented arrivals into the append-clocked driver. My second is single-P scheduling among connection readers/writers, handlers, the committer, and replies. A steady default-window or stream-admission cap is less likely given the source and byte counts. Run 1's 28.6k/s warmup at the same 256-worker, 16-connection settings also contradicts a fixed 5k/s transport cap. It is not a controlled causal comparison because the phases use new clients and a growing store.

## Minimal measurements and ablations

First collect a short complete capture at 256 workers with an uninstrumented baseline alongside it. Avoid writing a trace event to disk on the request path. Report dropped events and capture boundaries. Separate instrumentation cost from the cause under investigation.

1. Count proposed requests per entry and per entry-bearing physical group. Count hard-state-only groups separately. Record group start/end, sync duration, inbox snapshot, deferred count, pending replies, and persist-job count. This establishes whether the small ratio reflects small proposals, extra syncs, or both. Do not remove syncs merely because a MustSync flag is false; preserve async append ordering and response obligations.
2. Record gRPC Begin, InPayload, response queueing, and End with RPC and connection IDs. Compare decoded Put arrivals during each append against the requests submitted to the following Process. High active-RPC counts with sparse payload arrivals indicate a different wait stage from high decoded-arrival counts with tiny physical groups.
3. Record client issue/return timestamps or use the already planned native Go traces to locate runnable versus blocked time. Stats InHeader runs inside `handleStream`, after handler scheduling, not at the connection's header-read instant. Stats alone cannot attribute the delay before that goroutine runs. The supplied native traces can also test whether `GOMAXPROCS=2` on the same pinned core changes scheduling without adding CPU capacity.
4. Compare default dynamic windows, static 65,535-byte windows, and static 16 MiB windows with all other settings unchanged. Default versus 16 MiB changes both window size and BDP behavior. Static small versus static large isolates window size more cleanly. Server-only settings test request-direction credit. A response-direction claim needs client receive-window evidence.
5. Compare 16 versus 256 ClientConns at 256 workers. The latter means one closed-loop worker per connection; it changes transport goroutine count, TLS/socket overhead, and scheduling as well as sharing. A win is evidence for a connection-path effect, not proof of flow-control starvation. A low stream override such as one stream per connection is a useful admission-control negative control, not a production fix.

The parent reports initial runner `38334185473` covers trace P1/P2, static 16 MiB receive windows at P1, 256 connections at P1, and native Go runtime traces. It also reports clean P2 uses the exact uninstrumented prototype binary with `GOMAXPROCS=2` pinned to the same core. I have not inspected that run's artifacts and make no claims about its outcome.

The opt-in HTTP/2 capture below permits actual credit reconstruction. For client-to-server connection DATA, start with 65,535 bytes, add server-to-client stream-0 WINDOW_UPDATE increments, and subtract inbound DATA frame lengths. For response DATA, use inbound stream-0 WINDOW_UPDATE and outbound DATA. Stream credit starts at the peer's advertised initial window, adjusted by subsequent initial-window SETTINGS changes and per-stream WINDOW_UPDATE. DATA lengths include padding. SETTINGS and frame reads/writes can share one underlying I/O call, so event timestamps are not packet timestamps. A zero-credit interval is persuasive only when DATA is pending. Credit accounting alone cannot distinguish idle streams from blocked writers.

## Instrumentation delivered

The stats patch is commit `162a0f725dbe9a0a4a00fd5f076fbfcf92304279`, containing only `internal/server/pre47.go`, `internal/server/pre47_test.go`, and the options append in `internal/server/server.go`. The parent reports it has cherry-picked this commit, passed tests, and made StatsHandler conditional on `PRE47_TRACE=1`. My original version installed the hooks unconditionally to preserve IDs before capture; use the parent's opt-in version for clean measurements.

The follow-up HTTP/2 patch is commit `cdef91783742733436ae4cc0dc336877e4b96a07`. It contains `internal/server/pre47_h2.go`, `internal/server/pre47_h2_test.go`, a TLS-credentials wrapper call in `server.go`, and remote-port logging in stats TagConn. It activates only when `PRE47_H2_TRACE=1`. `h2_conn_port` and `grpc_conn_port` associate the two independent ID spaces through the remote TCP port. Those port events require capture to include connection setup; a mid-connection capture may lack the mapping. Preserve the opt-in StatsHandler change when cherry-picking.

The credentials wrapper delegates TLS handshake/authentication and clones the underlying credentials. It wraps only the successful server handshake's plaintext connection. Read and Write have separate locks and parsers, and process only the bytes actually read or accepted. It skips the 24-byte inbound client preface, stores a nine-byte frame header and up to eight control bytes, and discards all DATA, header-block, unknown-frame, and GOAWAY debug contents. It logs DATA length/stream ID, WINDOW_UPDATE increment, the three requested SETTINGS fields, RST error code, and GOAWAY last-stream ID/error code. It has no message-body buffer and no frame-size-dependent allocation.

Stats events use RPC ID, connection ID, and then size or active counts. Begin/End carry global and per-connection counts after the update. Payload events carry gRPC wire bytes and uncompressed bytes. RPC method IDs are Put 1, Range 2, Txn 3, DeleteRange 4, other 0. `grpc_end_result` carries duration in nanoseconds and status code. Concurrent count updates are not one atomic snapshot.

Important source limits:

- `grpc/server.go:1576-1591,1300-1323,1351-1369` emits InHeader after TagRPC and Begin at the start of processRPC; End occurs when server processing returns. These count active server RPC execution, not exact transport-map membership or client-visible completion.
- `grpc/stream.go:1863-1887,1914-1949` emits OutPayload after queuing the response and InPayload after decode. They are not socket timestamps.
- `grpc/internal/transport/http2_server.go:1073-1081,1148-1154` emits outbound header/trailer stats before HPACK compression. OutHeader has no wire-length field; OutTrailer.WireLength is deprecated and never set. The stats patch records `18446744073709551615` for unknown, not zero. Actual outbound header/trailer byte counts are outside this requested HTTP/2 tracer's event set too.
- Stats hooks do not expose SETTINGS, WINDOW_UPDATE, BDP estimates, or a writer's zero-credit stall directly. The new frame tracer adds SETTINGS/WINDOW_UPDATE evidence, but still does not expose queued-data readiness or kernel scheduling delay.

Local validation ran `gofmt`, `git diff --check`, and ten repetitions of `go test -vet=off -overlay .pre47-check/overlay.json -run '^TestPre47' -count=10 ./internal/server`. All targeted tests passed. The overlay supplied only the parent's declared expclock interface as a no-op, without creating or modifying the real package. It was removed afterward. An earlier run's tests passed but the command failed because vet could not chdir into the virtual package directory; the successful rerun explicitly disabled vet. Parent integration tests against its actual expclock implementation remain required. No race build or complete server test suite ran here.
