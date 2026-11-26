# Differential-test large-put timelines

`scripts/difftest.sh tigerstore` sets `TIGERSTORE_TRACE_LARGE_PUTS=1`.
The default production binary does not enable this diagnostic. It can be
explicitly enabled with that environment variable, or `wl.Config.TraceLargePuts`.
Disabled diagnostics read no clocks and allocate no per-request metadata.

With it enabled, the receiving server logs `large-put` events for Put and Txn
messages of at least 1 MiB. The threshold counts the gRPC message's declared
protobuf bytes, not the value alone, so every >=1-MiB Put/TxnPut is included.
All large Txns are traced, including read-only or malformed ones: inspecting
all their branches just to decide whether to log would add work and miss
requests refused before decoding. Large decompressed requests are also
recognized even if their compressed messages were smaller, including a
bounded prefix of one refused during decompression (`decode_refused`).
No keys, values, auth identities or request bodies are logged or retained.

Each line has:

- `request=node/run/sequence`: unique within this node's persisted run;
- `kind`, `bytes`, `decoded_bytes`, `decoded_complete`: kind, declared gRPC
  message size, decoded protobuf size and whether decoding completed. Before
  the body arrives the decoded size is zero; after a decompression refusal it
  is only the size of the observed bounded prefix, not the full message;
- `batch`, `op`, `index`: local proposal batch, operation position and
  eventual Raft index;
- `stage`, `at_ns`, `elapsed_ns`, `code`: the observed stage, Unix nanoseconds,
  monotonic elapsed nanoseconds since recognition, and the gRPC reply code
  (`-1` when this is not a reply).

The stages are:

1. `receive`: the message header first identifies a large request. This may be
   before its body arrives; a gRPC-size refusal can happen here. For a
   compressed message first recognized after decoding, reception/decompression
   before recognition is not included in `elapsed_ns`.
2. `received`: the full message is available, before admission checks.
3. `batched`: admitted into a local batch. `batch` and `op` now identify it
   even if its client disconnects.
4. `propose_begin`, then `proposed` or `proposal_rejected`: around inline
   batch forwarding and Raft's `Propose`. Thus the forwarding/sealing cost is
   included, not silently left out of the proposal interval.
5. `commit`: this server's Raft first hands the committed entry to the apply
   thread in a Ready, before that Ready's append/send/apply work. This is a
   **local observation**, not a timestamp from the remote leader or an fsync.
6. `apply_begin`, `apply`: before and after state-machine work, before reply
   construction or waiting for reply-buffer room. An apply may refuse a
   request; the reply code says whether it succeeded.
7. `reply_queued`: response/trailers have been queued in the socket buffer or
   HTTP/2 flow-control queue. This is **not a socket flush or proof of client
   receipt**. A slow client's reply may remain queued.

`stream_closed` without `reply_queued` means the client cancelled/disconnected.
Batch metadata survives a cancellation or a server timeout, allowing a later
commit/apply to be logged under the same request ID. Completed batches are
removed immediately. Lost proposals are kept only until their fixed-size
forward-table slot is reused (`forgotten`), so diagnostics cannot grow without
bound if proposals never commit. Missing later stages mean **not observed**,
not proof the operation aborted. Events can legitimately show a timeout or
closed stream before a late commit/apply.

With `KEEP=/absolute/path`, difftest saves **successful and failed** server
logs under `$KEEP/<TestName>/data/node*.log`. Every suite is copied before the
next fresh cluster replaces its logs. The CI difftest artifacts upload even
on success and retain these small text logs for **14 days**. No disks or
request payloads are added to the artifacts.
