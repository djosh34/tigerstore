# Shape observation

Experiment-only hooks on pinned main `e5c3327e3800edf64f86ed8c1971ba7ebc1e8342`.
No generator, benchmark, storage format or public API changes.

Set `SHAPE_DEBUG=127.0.0.1:42001` per node, or pass
`--shape-debug=127.0.0.1:42001`. The flag overrides the environment, including
`--shape-debug=` to disable it. The default is empty. Only literal loopback
IPs are accepted, including IPv6 `[::1]:42001`. Each node needs its own port.
The listener is plain HTTP, without authentication. Do not proxy it publicly.

`GET /debug/shape` returns JSON. Scrapes read counters and immutable copies;
they never enqueue a client request, call Maintenance.Status, read live core
state, trigger a core turn or force GC. A dedicated goroutine requests an
owner snapshot every 100 ms over a separate one-slot channel. Busy-owner
requests coalesce. The owner publishes between normal turns without calling
Process or Tick. Samples can be delayed by a long core turn or scheduler
stall. There is no bound on their age. A scrape is not an applied-index or
persistence fence.

## JSON v1

All numbers are integers. The fields are:

```text
schema_version: 1
captured_unix_ns: time at response construction
sample_interval_ns: 100000000
wall_ns_bounds: [1000, 4000, 16000, 64000, 256000, 1024000,
                 4096000, 16384000, 65536000, 262144000, 1048576000]
files:
  write: {calls, errors, bytes, wall_ns_sum, wall_ns_buckets}
  fsync: {calls, errors, bytes, wall_ns_sum, wall_ns_buckets}
node: null before the first owner snapshot, otherwise:
  sequence, sampled_unix_ns,
  inbox_events, persist_queue_batches, persist_backlog_batches,
  persisted_groups, pending_proposals, queued_writes, queued_reads,
  read_index_reads, read_index_batches, leader, term, applied, revision,
  raft_progress: array sorted by tracked member Raft ID, each element:
    {id, match, next, state, recent_active, msg_app_flow_paused,
     inflight_count, inflight_full}
node_sample_age_ns: -1 before the first sample, otherwise monotonic age
peers: null before sampling or for a transport without ObserveQueues,
       otherwise an array sorted by peer Raft ID, each element:
  {id, control_bytes, data_bytes, control_messages, data_messages}
peers_sampled_unix_ns: 0 before the first peer sample
peers_sample_age_ns: -1 before the first peer sample, otherwise monotonic age
```

### File counters

Counters start before WAL recovery and never reset within the process.
Subtract two snapshots from the same process for interval totals. The caller
must track process identity outside this endpoint and reject a restart.

`calls` counts completed `os.File.Write` or `os.File.Sync` invocations,
including failures. `errors` counts non-nil returned errors. A short write
with an error is one failed call. `write.bytes` counts positive returned byte
counts, including bytes written before an error, not requested bytes.
`fsync.bytes` is always zero. No retries or extra disk operations are added.
The wrapper calls the original operation once and returns its original byte
count and error.

`wall_ns_sum` sums elapsed wall nanoseconds around the original operation.
It includes syscall waiting and descheduling during that call. It excludes
counter updates and does not measure CPU, WAL group duration, queueing,
commit latency or request latency. It is not a critical-path decomposition.
Every call, successful or failed, contributes one histogram observation.

`wall_ns_buckets` has 12 **disjoint**, non-cumulative counts. Bucket 0 is
`[0,1000]` ns; each later bounded bucket is `(previous_bound,bound]` ns.
Bucket 11 is `(1048576000,+infinity)` ns. Compute bucket deltas directly.
The bounds correspond to 1, 4, 16, 64, 256 microseconds, then 1.024, 4.096,
16.384, 65.536, 262.144 milliseconds and 1.048576 seconds. Percentiles from
these buckets are intervals, not exact latency quantiles.

All WAL file calls count, including segment headers, rotation and recovery
repair. Directory fsyncs, reads, opens, closes and truncations do not count.
These are file fsyncs, not logical Raft jobs or fsyncs of only client writes.
One WAL group can include metadata, forwarded traffic and several entries.
Do not compare these totals to etcd backend commits or fdatasync counters
without naming the different definitions.

Counters load independently. A scrape during a completion can see part of
its counter update. Each field is monotonic, but calls, bucket sums and error
counts need not agree at that instant. They agree after writers quiesce.
External benchmark fences remain the caller's responsibility.

### Queue depths and gauges

`inbox_events` counts inbox events, not operations. One event may hold a
whole frontend batch. It excludes callers blocked before admission.
`persist_queue_batches` counts channel batches waiting for the disk worker;
`persist_backlog_batches` counts batches held by the owner before that
channel. `persisted_groups` counts completed disk groups waiting for the
owner. These counts exclude jobs already drained into an active disk group.
They are batch/group counts, not job, entry or byte totals. Channel lengths
can change during a sample, so their sum is approximate.

`pending_proposals` counts the core's live waiting-write map, including
writes not yet proposed and writes awaiting apply. It excludes forwarded
writes originated on another node. `queued_writes` counts physical proposal
queue slots and overlaps `pending_proposals`; do not add them. `queued_reads`
counts slots waiting to be put into a ReadIndex batch. `read_index_reads`
counts slots in retained ReadIndex batches; `read_index_batches` counts those
batches. Physical queue counts can include timed-out references until the
existing cleanup runs. Serializable reads normally bypass these queues.
None of these counts measures queue residence time or a peak between samples.

`leader` is the known Raft leader ID, with zero for unknown. `term` is Raft's
current term, including a term learned before the next Process call.
`applied` is the last applied Raft log index; `revision` is the MVCC revision.
These four fields come from one owner sample. They may lag the live node.
The observation copies no values or keys and changes no core state.

`raft_progress` comes from `RawNode.Status()` in the same owner sample.
Only leaders have replication progress; other roles report an empty array.
The array includes the leader's own tracked member entry. `match` is the
highest log index that Raft knows matches this member; `next` is the next
index to send. `state` is Raft's string, such as `StateProbe`,
`StateReplicate` or `StateSnapshot`. `recent_active` is Raft's activity flag,
not a timestamp or a promise that the member is currently reachable.
`msg_app_flow_paused` is Raft's raw flow-pause flag, not a measurement of
transport blockage or the complete `IsPaused` state predicate.

`inflight_count` counts Raft MsgApp inflight slots, not entries, RPCs or
queued transport messages. `inflight_full` is Raft's `Inflights.Full()`.
It can be true from either the message-count or byte cap. Pinned main
configures 256 messages and 32 MiB per follower. In replicate state, Full
with count below 256 is evidence of the byte cap; Full with count 256
cannot distinguish a count-only limit from both caps at once. The byte
cap is soft, so one message may cross it. The leader's own progress is
not a follower replication window. No inflight-byte count is reported,
because Raft does not export it. No reflection or unsafe access is used.
These sampled fields do not prove how long a limit held between samples.

Peer bytes are the existing queue budget estimates, not encoded or TCP
bytes. The estimate is 64 bytes plus context bytes, plus 32 and data bytes
per entry, plus snapshot data bytes. Reservation occurs before enqueue and
can briefly appear even when admission fails. Fields load independently.
They exclude a message already taken by the writer, frame/TLS buffers and
kernel socket buffers. Peer sampling runs outside the owner and has its own
timestamp; it is not simultaneous with the owner or file counters.

## Profiling and cost

The same opt-in listener provides `/debug/pprof/`, `profile`, `trace`,
`cmdline`, `symbol` and the standard named profile handlers. CPU profiling
and tracing begin only on an explicit request. Startup does not change
block, mutex or memory profiling rates. Go's default heap sampling remains.
Run intrusive captures in separate runner jobs, not headline windows.

Disabled observation creates no listener, sampler, counter object or timed
file wrapper. The owner loop has one disabled select case. File creation
and opening check a nil observation pointer; off-mode Write and Sync retain
the original adapter methods. The peer send/write path is unchanged.

Enabled file calls add two clock reads, a bounded bucket search, three atomic
increments, and one more increment each for positive bytes and an error.
The 100 ms sampler adds a goroutine, timer, nonblocking request and atomic
publication. Each owner sample allocates an immutable object, copies lengths/gauges
and walks the read-batch list once, not its requests or keys. The added
Raft progress hook calls `RawNode.Status()`, which clones the entire Raft
membership configuration and, on leaders, the whole progress map and each
inflight ring buffer. Raft clones a ring's allocated buffer length, not
only its active slots. Even a previously full but now empty ring can cost
up to 256 records per follower to copy. The hook also allocates a scalar
progress slice and sorts it by member ID. This is control-plane cloning
work on the owner at the 100 ms sample rate, with additional allocation
and GC cost. It is not a free length lookup. `WithProgress` is not used
because it clears Inflights from the reported progress.
Peer sampling reads existing atomics/channel lengths and allocates one
small sorted slice and publication object. Scrapes allocate copies, including the progress slice, bucket arrays
and JSON outside the owner. No new core lock or clock import exists.

These hooks consume CPU and can delay normal work by their execution cost.
They do not add batching timers, change persistence dependencies, reorder
persistence responses, move acknowledgement release or change API results.
Measure disabled-versus-enabled controls on the same free ARM runner before
claiming an overhead budget. No overhead number or performance result is
established by the focused local correctness tests.
