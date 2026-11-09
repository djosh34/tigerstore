# #577 primitive floors

Run `go test ./cmd/research577index`, build, then `taskset -c 0 ./research577index`.
`-smoke -reps 1 -duration 2ms` is only an integration check. Production repetitions
are >=300 ms measured thread CPU each, three repetitions, two independent ARM
matrix runners. JSONL has individual repetitions; units are ns/operation (ns/hop
for chases, ns/key for batched hashing, ns/batch for Raft). Summaries should give
the median and minimum of all six observations, not a best-case-only result.

GOMAXPROCS=1; locked OS thread; CLOCK_THREAD_CPUTIME_ID, with the median empty
clock-pair overhead subtracted per interval. Setup, restore, runtime.MemStats,
manual GC and Raft follower work are outside CPU intervals. GC is disabled;
allocator fast-path costs ARE included, GC service costs are NOT. Mallocs and
TotalAlloc are sampled across the measured work, excluding follower allocations.
This is a floor experiment, not throughput or a production latency prediction.

## A

* Each chase is a randomized single cycle, one uint32 next-line index per 64-byte
  cache line. Eight-chase mode starts at eight equally spaced points and explicitly
  interleaves eight loads. The working-set byte size includes cache-line padding.
* Key names copy mixedName's spelling and cluster-scoped separator and draw
  resources with k8ssize.Draw. Sixteen owners x 64 namespaces approximates the
  bench's 1,024 groups per resource. Lengths and average capped namespace-list
  cardinality are emitted. This is a synthetic snapshot, NOT a dump of live keys.
* Point queries traverse a full pseudorandom permutation, repeated as needed,
  without PRNG cost inside timing. This is uniform random without replacement
  within a pass (not independent draws with replacement). There is no hot subset.
  A separate byte arena holds query keys, avoiding pointer-equality shortcuts.
  Query-index and query-string fetch costs remain in timing.
* Flat hash: xxhash64; linear probe; 16-byte entries (hash64, slot32, state32),
  8-byte offset/length records, one byte arena. Capacity is power-of-two at <=65%
  occupancy; actual load factors are emitted (different sizes round differently).
  Successful lookup compares the entire key. Software batching has three phases:
  hash all; load initial probes all; compare all (remaining collisions scalar).
  No assembly prefetch is used. Batch size 8/16 amortizes across independent ops.
* Mutations are measured separately: insertion deletes a random chunk before the
  interval; deletion restores it afterwards. Cardinality stays near N. Tombstones
  are reused by same-key reinsertion. This is steady-state single-key replacement,
  NOT long-term churn with new keys; arena allocation/compaction is excluded.
* BTreeG[string], google/btree v1.1.3, degree 32. Note current mvcc store on this
  branch actually indexes uint32 arena slots with a key comparator; the requested
  string-key benchmark is not an exact measurement of that implementation.
* Cache-friendlier alternative: a sorted contiguous array of 16-byte records:
  eight lexical prefix bytes after /registry/, plus arena offset/length. Seek
  uses prefix comparison then full key; insert/delete really shift the suffix
  (O(N)). It is deliberately simple, correct, and bad for mutation-heavy loads;
  no claim is made that it is an optimized B+tree.
* namespace-list500 seeks a namespace prefix, checks its boundary, and returns
  up to 500 keys. seek-iterate500 instead visits 500 successors starting at a
  full key (except near the array end), irrespective of namespace boundary;
  this isolates a full-page iteration even for sparse namespaces. No values,
  serialization, or per-returned-key allocation is included.
* Go map and both ordered structures run at 100k/1M only. Flat hash additionally
  runs at 10M. Chases span 4 MiB..1 GiB. Only small smoke workloads run locally.

## B

Three real RawNodes and three MemoryStorages, raft v3.7.0; static three-voter
configuration, AsyncStorageWrites, CheckQuorum, PreVote, ReadOnlySafe. A real
election and warm-up occur outside timing. Each write batch proposes 1/8/64
entries before consuming Ready. The pump routes both followers' real MsgAppResp
messages to the leader and verifies every proposed entry is committed/applied.
It handles async append/apply messages, their attached response messages, and
leader MemoryStorage operations. RawNode.Advance is intentionally NOT called:
raft v3.7.0 rawnode.go explicitly forbids Advance with AsyncStorageWrites; the
MsgStorageAppendResp/MsgStorageApplyResp steps replace it.

Only leader turns (Propose/Step/HasReady/Ready/storage handlers) are enclosed in
CPU/allocation samples. Followers run synchronously on the same pinned core,
but entirely outside those intervals. Transport queues are preallocated, and
there is no serialization, memcpy of payloads, disk durability or network I/O.
The 1/64/512 KiB immutable payload buffer is reused: this measures RawNode's
reference/size bookkeeping, not encoding or copying the payload bytes.

One ReadOnlySafe ReadIndex round is measured separately, including heartbeat
responses from real followers and Ready.ReadStates completion. Write and read
samples each accumulate >=300 ms leader CPU per repetition; if either reaches
that duration first, only the other kind continues. MemoryStorage compaction
is outside samples every 256 writes, and its allocation/CPU costs are excluded.
Reading MemStats outside each leader turn perturbs caches; empty timestamp
subtraction cannot remove that perturbation. There are no periodic Tick calls
in this isolated steady-term experiment, so election/check-quorum maintenance
is not amortized here.

The no-allocation comparison appends 1/8/64 entry descriptors to a preallocated
ring, stores three durable indexes (leader and two followers), and computes the
median. It assumes successful durable acknowledgements without failure paths.
It is only the bookkeeping floor, not a substitute consensus implementation.
