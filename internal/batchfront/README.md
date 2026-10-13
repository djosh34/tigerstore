# Whole-batch frontend prototype

Throwaway experiment, not a production server. The default listener is unchanged. Select this listener with `TIGERSTORE_FRONTEND=wholebatch`.

`TIGERSTORE_BATCH_MODE` selects:

- `rpc`: x/net Framer and HPACK, generated protobuf decode, per-RPC goroutine and `Node.Do`. The reader stays multiplexed.
- `admit`: generated protobuf decode and legacy core, batch admission and grouped completion.
- `full`, the default for this listener: compact borrowed Put/CAS descriptors and wire-backed core entries, batch admission and grouped completion. Range, DeleteRange and Compact use the asynchronous legacy batch fallback. Status uses a synchronous setup-call fallback. MemberList is answered locally.

Admission and completion change together between rpc and admit. This is not an admission-only ablation. There is no fill timer. Already-buffered complete frames are combined, up to 256 commands or 4 MiB, and admitted before the reader blocks on an incomplete next frame.

## Ownership and bounds

The Framer reuses its DATA storage. The reader copies each payload into a stream-owned reassembly buffer. Full-mode descriptors borrow that immutable body after END_STREAM. They never borrow Framer storage. The core copies commands into an entry and clears pending descriptor references. MVCC value slices borrow GC-owned entry bytes, never pooled receive storage.

Each connection admits at most 1024 streams, including partial, admitted and response-pending streams. Stream receive-buffer capacities and admitted receive bodies share a 16 MiB cap. An individual gRPC receive body accepts the existing 1.5 MiB core request limit plus 512 KiB transport overhead. Admission budget remains charged until completion, including after an admitted stream is reset. Legacy protobuf decode adds its deliberate ablation copies outside the receive-buffer accounting.

Responses are sized before encoding. A single response, including frame headers, must fit the 16 MiB connection output cap. All queued, flow-control-blocked and currently-writing response bytes stay reserved until the writer flushes or drops their owned buffers. Oversized responses or a full output/control queue close the connection. The owner never waits for channel space or network I/O. Header/trailer success HPACK blocks are prebuilt and stateless. Marshal writes into the reserved response buffer; DATA headers are inserted in place. Writer queues hold only owned encoded frames, not descriptors, protobufs or log references.

`TIGERSTORE_BATCH_STATS=1` logs commands, admission sends, completion batches, replies and the per-connection output-byte highwater at shutdown. Counts include fallback commands. Completion counts also include setup calls and immediate errors.

`Stats.ControlSends` counts successful sends on the reader-to-writer control channel, including the stream-open send for every RPC. `Stats.ResponseSends` counts successful sends of encoded response groups, including HTTP responses. These counters do not count all runtime channel operations or grpc-go internals.

## Compatibility gaps

TLS and mTLS use the ordinary server TLS configuration with ALPN h2 and http/1.1. GET `/health` and `/version` share the port. HTTP/2 endpoints use the existing Framer reader and writer; HTTP/1.1 uses a standard HTTP server only for these endpoints. Health always makes a real Range, linearizable unless `serializable=true`, with a seven-second response timeout. HTTP/2 health uses asynchronous legacy batch admission and grouped completions in every mode. The HTTP/2 and gRPC wire format is unchanged. Standard generated clients work for Put, supported Txn, Range, Status and MemberList.

Full Txn supports the compact codec's single-key compares, nonnested Put and point Range branches. Other Txn features return Unimplemented rather than silently changing their meaning. rpc and admit use the ordinary legacy core instead.

Watch, Lease, Auth and other unsupported methods return Unimplemented. Request compression and multiple messages per unary stream are unsupported. gRPC timeout metadata is not interpreted; disconnect/reset does not cancel a submitted command, which still gets the core's normal request timeout. This is not a complete HTTP/2 protocol implementation. Receive-limit or admission-limit violations close the connection rather than returning a per-stream ResourceExhausted status. Bounded huge Range replies may close the connection even though the normal server permits larger replies.

Focused tests cover generated-client compatibility across modes, multiplexed rpc, buffered batch admission, immutable Framer payload ownership, response frame ownership, byte reservations through partial flow-control delivery, oversized response rejection before allocation, queue-full owner progress, partial/admitted resets and idle writer deadlines. The core helper's tests cover durable reply timing and entry-backed value ownership. Cluster durability tests are supplied by the parent integration experiment.
