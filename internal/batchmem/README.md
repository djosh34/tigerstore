# Bounded batch memory experiment

`TIGERSTORE_BATCH_MODE=full` supports three allocation controls in one executable:

- Unset `TIGERSTORE_BATCH_MEMORY` keeps ordinary heap allocation.
- `TIGERSTORE_BATCH_MEMORY=ring` allocates transient buffer banks at `batchfront.New`.
- `TIGERSTORE_BATCH_MEMORY=pool` allocates each bounded chunk on first use and recycles it between batches. It does not use `sync.Pool` or an unbounded fallback.

Both experimental modes use the same chunk classes, fixed descriptor slots, refcounts and rejection policy. Smallest suitable chunks are preferred. A chunk cannot be acquired while any reader holds a reference. Exhaustion rejects the connection before overwriting live bytes. This is a segmented ring, not a byte-addressed circular buffer. The pool is a bounded batch-chunk pool, not a pool of individual RPC objects.

## Budgets and lifetimes

Each server has 256 input chunks of 64 KiB and 16 of 4 MiB, totaling 80 MiB. Output has 256 chunks of 64 KiB, eight of 4 MiB and two of 16 MiB, also totaling 80 MiB. The large output class preserves the existing 16 MiB response limit. Each connection still has its 16 MiB input/output limits and 1,024-stream limit.

Command and reply descriptor banks each have 256 slots. Command slots hold 256 commands, reply slots hold up to 1,024 replies. Both modes allocate their typed descriptor arrays at startup. `Stats.Memory.DescriptorBytes` counts these arrays. Bank metadata and connection setup memory are additional. Ring startup reserves 160 MiB of buffer bytes; pool materializes no buffer bytes until acquisition, but it can retain all 160 MiB after enough different chunks have been used.

Receive buffers have a reader-owner reference plus one reference for each stream that borrows them. Command descriptor slots have an admission-owner reference plus one reference per admitted command. Sending a batch to the node does not release the command references. Resetting a submitted stream does not release them either. Real completion releases receive and command storage conservatively, after encoding or cancellation has made the core stop using it. Inputs that were never submitted are released when the reader exits. Late completions on a closed connection still release their references.

Reply frames have a completion-owner reference plus one reference per encoded response. Writers release response references only after `bufio.Writer.Flush`, or after dropping a reset/closed stream. Zero HTTP/2 credit keeps both wire bytes and their accounting pinned. Writer failure drains pending and queued responses. Publication and final draining use the existing connection ledger mutex to prevent a late publication from leaking a buffer.

Raft entries are separate immutable GC-owned allocations. The core copies the command body into its retained entry, and MVCC aliases that entry. No entry is pooled or recycled on acknowledgement. WAL persistence, Raft storage responses and acknowledgements are unchanged.

## What this does not achieve

This is a bounded transient allocator comparison, not a whole-loop zero-allocation implementation. Core admission records, pending records, request/response objects, MVCC and Raft protobuf work still allocate. Txn descriptor decoding and legacy methods still allocate. TLS, HTTP/2, HPACK and connection setup still allocate. Live log/history remains outside these transient banks and grows without cleaning.

The existing `c.mu` ledger remains blocking in both experimental modes. `Node.SubmitBatch` and `SubmitLegacyBatch` can still wait for the inherited inbox to accept an admission batch. The no-blocking hot-path goal is unmet. Rejecting healthy ledger contention with `TryLock` would make the comparison invalid, so this experiment does not do that. The bank acquisition and writer-queue publication themselves do not wait for space.

HTTP health uses a timeout that can answer before real core completion. Its setup-only descriptors stay GC-owned rather than being recycled on that early timeout. Its HTTP/2 wire buffer uses the output bank. HTTP/1 setup continues through the helper's ordinary HTTP server. Status and other setup calls retain their existing behavior.

The default control keeps ordinary heap allocation for receive, command/reply slices and wire bytes. The shared budget/encoded structs have extra ownership fields in this build, so comparisons are between modes of this executable, not a byte-identical comparison with its ancestor.

## Checks and measurement

Targeted correctness checks:

```sh
GOFLAGS=-p=2 GOMEMLIMIT=2GiB go test ./internal/batchfront ./internal/batchmem -short -parallel 2 -count=1
GOFLAGS=-p=2 GOMEMLIMIT=2GiB go vet ./internal/batchfront ./internal/batchmem
```

Tests check submitted descriptors staying pinned after the send and reset, no overwrite on bank exhaustion, near-limit Put/Get across repeated connections, log-value immutability after transient reuse, exact 1.5 MiB value rejection, large response classes, byte caps, late closed-connection completion, and actual writer flush/zero-credit lifetimes. The accepted large value is `core.MaxRequestBytes-128`. An exact `core.MaxRequestBytes` value cannot fit etcd's InternalRaftRequest wrapper.

All performance measurement belongs to the parent's ARM Actions job. Report allocation count and bytes per completed operation, GC CPU, process CPU per operation, retained heap after quiescence and forced GC, bank rejection counts and successful operation counts. `Stats.Memory.Input` and `Output` expose configured byte budgets, materialized retained bytes, pinned in-use bytes and their highwater marks. These are chunk bytes, not the sum of useful payload lengths. They exclude log/database bytes and descriptor/connection metadata. Pair ring, pool and unset controls in the same executable and workload. No local benchmark numbers or whole-loop zero-allocation claims are supplied by this package.
