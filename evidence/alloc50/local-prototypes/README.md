# Throwaway allocation prototypes (never merge)

Question: can bounded transient wire-Put storage remove managed request-path churn without recycling immutable retained Raft/MVCC bytes?

## Modes and accounting

- Unset/`base`/unknown `TIGER_ALLOC_STRATEGY`: original RPC path; no allocator pool.
- `ring`: 4096 startup slots, circular fixed free-slot queue, independently retired slots.
- `arena`: startup bounded pool of 512 **eight-slot batch regions**. A region begins at HEADERS, is sealed on reader flush/buffer exhaustion or its eighth HEADERS, and retires only after all of its streams' owners have retired. Incomplete streams may remain in sealed regions. One node handoff can coalesce multiple regions. No per-object sync.Pool; no lazy startup-memory advantage over ring.
- Both: 256 reserved slots and 8MiB reserved body capacity per connection. Arena charges all eight reserved slots, including unused slots, until whole-region retirement.
- Body/reply byte classes: 8MiB common-small backing (ring: 4096 x 2KiB; arena: 512 x 16KiB bump regions), 64 x 128KiB, 16 x 2MiB = 48MiB. Descriptors and a conservative 8MiB core/node map/list reserve are included in the 64MiB budget by a compile-time size guard. The reserve is charged in held/peak bytes even when there are no live calls.
- HEADERS slot/quota exhaustion returns REFUSED_STREAM without a heap stream. Body capacity exhaustion ends the call with ResourceExhausted. No managed heap fallback. Separate small and large classes prevent large-body holders consuming small-body capacity.
- Scope is wire Put transient storage only. Generic RPCs, connection buffers/contexts/maps, HTTP2/HPACK/TLS, control/error output and timers are not globally byte-bounded by this allocator. This is **not** a hard bound on total server heap.

Observer: `Server.AllocationStats() map[string]uint64`, mandatory keys `mode`, `slot_budget`, `byte_budget`, `per_connection_slots`, `held_slots`, `held_bytes`, `peak_slots`, `peak_bytes`, `denied_count`, `fallback_allocations`. Extra keys `driver_reserve_bytes`, `per_connection_bytes`, `timer_allocations`, `unmanaged_rpc_count`. `fallback_allocations` counts managed heap fallbacks only (there are none); it is not a total allocation counter.

## Ownership and residual allocations

Slots embed stream, core pending workspace, request descriptor, scalar reply, reply encoding bytes, and three output descriptors. Optional linked `node.ManagedBatch` replaces request/tag and node batch arrays/objects; an intrusive completion queue replaces Completion arrays and server finished/completion arrays. CompleteOne copies into that queue under one short O(1) compMu operation per request, intentionally retaining per-request mutex overhead. It encodes only on the writer. Closed-connection callbacks additionally take the short connection lifetime lock.

References are explicit: reader lookup/frame pin, stream-map ownership, queued-before-flush admission, node completion, active timer callback, ready-list membership, and writer frames. Timer.Stop(false) leaves the callback pin alive. Writer payload refs retire only after actual bw.Flush or abort cleanup. Connection close does not retire unsubmitted batch links until readerDone; late completions can release closed connections without a live writer. Node shutdown excludes late submitters and drains accepted-but-unprocessed inbox chains, saving next before each recycling callback.

Core removes settled managed descriptors from BOTH timeout and proposal lists at Process boundaries before handing replies to the driver. Shutdown drops all remaining core managed holders. Request validation errors also use the supplied reply workspace. Reusable core list/map capacity is reserved at node startup. Map infrastructure can still have runtime rehash/growth allocations, especially mixed generic workloads; the experiment does not substitute fixed maps or claim zero total hot-path allocations.

Raft encodeBatch still allocates an owned proposal byte buffer. Raft's clone, retained Entry.Data, WAL, peer unmarshal, effects for persistence/messages, MVCC history/tree/version objects and generic replies remain unchanged. No entry-data pooling, no early acknowledgment. Timer objects/bound callbacks, error status construction and connection/transport work still allocate. The expected eliminated census subset is 15 of the baseline 16 explicit transient workspace sites per serial Put; profile evidence is required before claiming that measured saving.

## Local validation

Commands use `GOFLAGS=-p=2 GOMEMLIMIT=2GiB`, `-parallel 2`, no benchmarks.

- Compile: core/node/server packages passed.
- `TestAllocation*`, `TestLargeValues`, `TestDeadlinesLeaderAndUnknownMethods`, `TestPutResponseEncoding`: passed in base/ring/arena; `focused-tests.log`.
- Parent `TestAllocationPath` and `TestAllocationValueLifetimes`: passed all three modes, including the 3-node TLS/retained-history path.
- Focused ring `-race` run passed; `ring-race.log`. This one run had already completed when the parent's subsequent no-local-race instruction arrived. Further race gates belong to the parent runner.
- Allocator unit tests additionally cover quota isolation, reset-before-flush/reader pins, flush/abort frame ownership, active Stop(false) callback, raw 2MiB ingress, zero allocations in bounded slot/body acquire/retire primitives, core stale timeout/proposal retirement, and node orphaned inbox links.

Latest managed stream id/needLeader/rpc publication fix was formatted after those runs; parent runner owns the subsequent gates. No local throughput benchmark or quantitative performance claim was made.
