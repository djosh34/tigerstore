# Compression experiment for #132

This branch is an experiment, not a merge recommendation. The verdict and
Actions links go in one comment on #148 after the measurements finish.

## Format and execution

Cluster version 4 permits compressed log entries. The existing version
announcement and confirmation protocol controls activation. Before every
voter supports version 4, proposals still use the old format and the disk
writer leaves them alone.

A raw format-4 entry has the same layout and length as format 3, with its
version byte changed. A zero batch count marks a compressed entry. It is
followed by a little-endian 32-bit decoded length and one zstd frame.
Actual batches have positive counts. The encoder is klauspost/compress
v1.20.3 at zstd level 1, without cgo or a dictionary.

The disk writer samples at most 1 KiB of each entry's byte frequencies.
It skips small or apparently incompressible entries. If an attempted
compression does not shorten the entry, it keeps the original bytes.
Compression happens before the writer takes the WAL lock, not on the
committer. Each WAL owns its codecs. They are not shared between nodes
or virtual-time test runs.

Raft, peer traffic and the in-memory window keep raw entries. This is a
log-only experiment, not compression of peer traffic. Replay and cold
read helpers decode stored entries. A value's offset includes the
original two-byte entry header. Compressed entries do not expose a file
offset to the mmap path. Cleaning reads decoded values and copies them
into the existing raw packs, keeping their Raft positions.

## Measurement

Dispatch `bench.yml` on `prep/132-compression`, with the candidate and
baseline pinned to commit IDs, `benchmarks=compression`, `modes=max rate`,
`reps=5` and empty `layouts`.

The workflow recreates #135's Kubernetes 1.37 application fleet in kind,
including server-side apply, controller-managed objects and JSON custom
resources. It reads a live etcd dump and deletes kind before timing. The
dump remains private to the runner. Only aggregates enter the artifacts.

The two Put cases use the same corpus and native mixed value lengths:

- `put-k8sreal` sends unchanged stored Kubernetes values.
- `put-k8srandom` replaces each value with random bytes of exactly the
  same length, independently for each connection.

Each connection samples the corpus uniformly. Keys use the bench's
Kubernetes-shaped key generator. Random templates repeat when sampled
again; they are not freshly encrypted on every request. This is a live
snapshot distribution, not #135's write trace or the older `mixed-k8s`
size histogram. Values outside `/registry/` or outside 8 to 51,200 bytes
are excluded, with counts in the generator log. The size cap leaves
room for request framing within the generator's existing 60 KiB limit.

The existing bench runs etcd v3.7.2, pinned main and the candidate back to
back on one arm64 runner. All three replicas share its disk, with write
barriers enabled and TLS. The generator and each replica have separate
pinned CPU cores. Every replica has GOMEMLIMIT 3 GiB. Each repetition
starts fresh clusters, and alternating target order limits time drift.
Maximum load measures throughput. Open loop offers half etcd's median
maximum rate to all three stores. Each run has 3 seconds of warmup and
10 seconds of measurement.

The report gives throughput and p99, plus two byte counts per successful
Put in the window. Both byte counts sum all three replicas:

- The WAL counter measures actual Flush bytes, including framing and
  hard state. It excludes checkpoints, cleaning packs and filesystem
  overhead. The three counters are scraped in sequence at each window
  boundary, so they are not one atomic sample.
- Block-device writes include the filesystem journal and other runner
  writes. They are not compressed payload sizes.

Candidate median throughput below baseline minimum, or candidate median
p99 above baseline maximum, is a regression under #144's stated rule.
Saving bytes alone does not pass #132. If writes fail the rule, stop and
keep the branch. Passing writes would still need #144's memory-limited
cold point-read, LIST and watch-prev_kv comparisons before any merge.

## Tests

The branch-only CI runs WAL, core, node and load-generator tests under
race, vet and a cgo-disabled server build. Direct regressions cover
version activation, old-format bypass, incompressible fallback, decoded
offsets after reopen, corruption rejection and recovery after cleaning
removed the original compressed segment. The codec-disabled control
must fail on the expected compression-size assertion. The corpus test
checks original value bytes, independent random streams and multi-frame
requests.

The prep-only CI commits are not integration changes. Drop them if any
of this experiment is used elsewhere. No PR is opened by this worker.
