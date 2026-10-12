# Request costs before outcome 50

Throwaway measurements for #50. Never merge this branch. The workflow runs only on pushes to `exp/pre50-primitives` that change this experiment or its workflow. The job uses the free `ubuntu-24.04-arm` runner, has a 15-minute timeout and refuses to start after 2026-10-13T22:00:00Z. There is no manual or main-branch trigger.

## Run

```sh
# On the disposable Actions VM, the workflow first runs:
sudo mount -o remount,barrier=1 /
# Then, from the repository root:
timeout 660 bash experiments/pre50-primitives/run.sh
```

The script rejects non-ext4 storage and `nobarrier` or `barrier=0` before and after timing. The remount command belongs to the disposable VM, not the owner's host. Linux defaults may omit an explicit `barrier=1` from mount output. We retain the VM's other mount options. Successful fsync is the durability interface under test. Guest barriers do not prove hypervisor or physical power-loss guarantees. Three replicas share one machine and disk failure domain.

The script finds the first four allowed CPUs. Local tests and the quorum leader use the first CPU. Followers use the second and third. The no-op gRPC server uses the second and its client uses the fourth. Every process has `GOMAXPROCS=1`. Each program verifies that every observed thread has the same singleton `Cpus_allowed_list`. It records thread IDs at start and end, or in the server's measured result before termination. New threads inherit process affinity. This is process pinning, not host CPU isolation. Runner background work can share these CPUs.

Go 1.27.2, etcd API v3.7.2, Raft v3.7.0, grpc-go v1.83.2, protobuf v1.36.11 and x/net v0.58.0. `go.sum`, all resolved modules, `go version -m`, source hashes and the executable hash are evidence. `actions/checkout`, `setup-go` and `upload-artifact` use exact commit pins in the workflow. Build caches, temporary files and WALs live under this experiment's `.run` directory and are deleted at exit. Raw evidence and source remain for artifact upload. No issue comments or merges belong to this branch.

## Timing

Five repeats per case, with fixed operation counts for in-memory tests. Each repeat includes a full GC before the measured loop. No adaptive benchmark selection or best-run selection. CPU means process user plus system time from `getrusage`, including the Go runtime and GC during the window. Wall includes elapsed time, loop overhead and preemption. The runner has no CPU-frequency isolation. Very short fast-operation windows can be noisy, so the report preserves repetition ranges.

The loop-control test estimates the per-iteration function call and sink bookkeeping. The fsync-probe-pair control runs the same two `getrusage` calls and elapsed-time probes placed around each `File.Sync`. Its cost remains charged to combined write-plus-sync timing. Fsync-only CPU includes part of the second probe and therefore is approximate syscall-stage attribution, not hardware service time. We do not subtract probe cost. Quantiles are medians of individual run quantiles, not pooled sample quantiles. Process CPU and elapsed wall are not interchangeable.

## Local cases

- AES-128-GCM Seal/Open uses a 16-byte key, 12-byte nonce, a five-byte TLS header as additional data, and payload plus a one-byte TLS inner content type. Seal writes to reusable output and changes the sequence nonce each call. Open authenticates a fixed valid input repeatedly. These measure AEAD calls, not a complete TLS connection. The synthetic key is not a production key-management policy.
- `crypto/tls` does real TLS 1.3 handshakes with a test certificate outside timing. Both endpoints assert `TLS_AES_128_GCM_SHA256`. After the handshake an in-memory `net.Conn` adapter captures record writes or supplies record bytes. Encryption includes `tls.Conn.Write`, record preparation, sequence state, locking and output through `io.Discard`, but not a socket syscall. Decryption consumes 100,000 prebuilt genuine records once, in order, using `tls.Conn.Read` and `io.ReadFull`. No replay or invented ciphertext. It includes reader/framing overhead and a length check. The full plaintext comparison happens after timing. The 1KiB case has 1,046 wire bytes per record. The small reply case uses the actual encoded PutResponse.
- The HTTP/2 case calls x/net/http2 `Framer.ReadFrame` on a cached 1KiB DATA frame. It resets a `bytes.Reader` each iteration and checks type, stream and payload length. It includes allocation/copy by the library, not HPACK, stream scheduling, HTTP/2 flow control, sockets or gRPC dispatch.
- Request and response codecs use actual etcd API messages. The PutRequest has a 32-byte Kubernetes-shaped key and a 1,024-byte value. The PutResponse contains a small response header and no echoed value. Decode validates the uncompressed five-byte gRPC envelope then allocates a fresh message and calls protobuf Unmarshal. Encode calls Size, allocates a fresh envelope with capacity for the message, marshals and writes the four-byte length. No compression. Decode does not traverse the decoded message with Size during timing. An envelope and codec are not a complete gRPC call.
- The memory-ring append copies metadata and 1KiB into 4,096 bounded slots. It is custom bookkeeping, not stock Raft or persistence.
- The stock Raft case calls `MemoryStorage.Append` with one prebuilt immutable Entry pointer each time and validates LastIndex after timing. One million entries share one immutable payload. Entry construction is outside timing. This measures mutex and append-slice bookkeeping, including slice growth. MemoryStorage retains pointers and does not deep-copy the value. It does not measure `RawNode.Propose`, Ready, elections, replication or applying a command.
- The file append encodes a stock Raft Entry protobuf into a reusable frame buffer, adds a four-byte length and appends through a 64KiB `bufio.Writer` to a new regular file. Ordinary buffer flushes and kernel page-cache writes happen inside timing. The final partial-buffer flush has separate CPU/wall evidence. No fsync, checksum, recovery framing, segment rotation or durable directory installation. This is nondurable and not an etcd WAL.
- Memcpy copies a cached 1KiB source to one cached destination using Go `copy`. The program consumes every destination byte after timing and keeps the destination live. This is not a cold-memory or bulk-bandwidth test.

## Disk

Each batch-size case creates a 64MiB file, writes every extent and syncs the file and directory outside timing. Two-second measured windows cyclically overwrite initialized extents with 4KiB, 64KiB or 1MiB `WriteAt`, then `File.Sync` per batch. This is fsync, not fdatasync. Report write-plus-sync CPU/wall, fsync-only instrumented CPU/wall, exact counts and latency samples summarized to quantiles. Batch order reverses on even repeats. One writer only, unlike the three-writer/quorum contention case. These overwrites do not include ongoing file growth or segment installation.

## Three-process TLS quorum exchange

Three Go processes terminate actual mutual TLS 1.3 on loopback, with AES-128-GCM asserted. Each wire entry is a stock Entry protobuf carrying a 1KiB value, framed by a four-byte length. Twenty warmup exchanges happen before timing. The leader marshals each entry and sends it concurrently to two followers. Followers decode and validate it, then append the received bytes. The leader appends its own bytes in parallel with the exchanges. A follower replies with a one-byte acknowledgment only after its append completes.

Nondurable mode copies entries into bounded 16MiB memory rings on all three nodes. Durable mode uses growing regular files, writes the frame and entry, and calls `File.Sync` on each node before that node's completion counts. Each node initializes and syncs its WAL file and directory before warmup. The experiment does not implement a recoverable WAL, Raft agreement or a replicated database. It tests the timing pattern of local completion plus a follower acknowledgment.

Quorum latency stops when the leader has completed locally and has the first successful follower acknowledgment. The script still drains the second follower before proposing the next entry. Therefore elapsed throughput is an all-followers-drained, single-entry closed-loop rate, not a pipelined quorum throughput ceiling. Per-process CPU divides each node's own measured user-plus-system CPU by the same completed entry count. Follower CPU windows include blocked receives and their final acknowledgment. Boundary skew of one ready acknowledgment is small but not zero. Nondurable has 10,000 measured entries per repeat; durable has 3,000. Both followers stay caught up. No election, apply, client-facing RPC, watch, index, batching or artificial peer delay is present.

## Complete no-op unary RPC

A separate grpc-go server and client use actual etcd KV Put messages over TLS 1.3 with mutual certificate authentication. The negotiated cipher is checked by the TLS configuration on both ends. There is one connection, 500 warmup calls, then two-second measured windows with one or 64 closed-loop workers. The handler validates the value size and returns the actual small PutResponse. It does no consensus or storage. Client latency starts immediately before invoking Put and ends after checking the response header.

Control Puts bracket server process CPU accounting outside the client timed window. The server CPU window includes transport processing between the start and stop controls and runtime work, with small boundary overhead. Request counts must match client and server. The server prints its result and records its thread affinity before the script terminates it. This measures gRPC dispatch plus TLS, HTTP/2, protobuf and runtime as a complete no-op RPC, not a pure handler-dispatch instruction count. At one worker it pays scheduler wakeup costs that differ from saturated load.

## Limits

The reciprocal of a local operation cost is not whole-database speed. Do not sum medians into a database latency percentile, or add overlapping CPU cases. The full no-op RPC already contains TLS, framing and codecs. There is no database, full consensus, MVCC, index, watch fanout, overload handling, crash durability or recovery here. Results are five short repetitions on one VM, not independent-machine confidence intervals. The parent owns a separate etcd baseline and posts one consolidated #50 comment.
