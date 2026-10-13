# etcd balanced C4096 failures: snapshot catch-up and admission

## Conclusion

The existing artifacts establish that follower n2 received and applied a leader snapshot during each failed repetition. Repetition 4 also crossed etcd's existing apply-backlog admission threshold. Snapshot installation, unanswered proposal waiters and the existing seven-second timeout path explain the observed pattern without requiring a new defect or a leader change.

The initial reason n2 fell behind is not established. CPU/scheduling, storage, GC and transport contributions cannot be separated from these artifacts. Do not label this an OOM, disk failure or network outage. Keep both invalid repetitions in the five-repetition result; do not compute a passing-only headline.

## Inputs and exact paths

Artifact root:

```text
/home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/results/closed/shape-put-1k-balanced-c4096
```

All repetition paths below are relative to this root:

```text
runner.txt
plan.json
failures.json
commands.json
put-1k-balanced-c4096/etcd-3/{load.json,audit.json,before.json,after.json,samples.jsonl,placement.json,n1.log,n2.log,n3.log}
put-1k-balanced-c4096/etcd-4/{load.json,audit.json,before.json,after.json,samples.jsonl,placement.json,n1.log,n2.log,n3.log}
```

Controls are `put-1k-balanced-c4096/etcd-{1,2,5}/` with the same filenames. In `before.json` and `after.json`, `/nodes/0`, `/nodes/1`, `/nodes/2` contain Prometheus text for n1, n2 and n3, respectively. The leader is n1 at `127.0.0.1:40010`; n2 is `127.0.0.1:40020`, Raft ID `1b835d3a0ab2f999`. The leader's Raft ID is `bb0a12d34d6e033e`.

`runner.txt` identifies harness commit `7043fcd6a83a76b3a22753d54c8aca2739286e50`, etcd v3.7.2, Git SHA `68c065e`, Go 1.26.8, linux/arm64. The etcd binary SHA-256 is `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d`.

Four CPUs were available. Servers were pinned to one CPU each, with `GOMAXPROCS=2`; n2 used CPU1. The generator and sampler shared CPU3. The Go memory limit was 2 GiB per process. These are source/artifact observations, not a rerun.

## Outcome accounting

From `load.json`, `audit.json` and the fenced status records:

| Quantity | Repetition 3 | Repetition 4 |
| --- | ---: | ---: |
| Arrival cohort | 29,971 | 28,631 |
| Successful replies through drain | 29,259 | 25,864 |
| Unavailable timeout replies | 712 | 2,036 |
| Too-many-requests replies, reported as gRPC Unknown | 0 | 731 |
| Unsent | 0 | 0 |
| Fenced revision before -> after | 17,619 -> 47,589 | 16,008 -> 43,908 |
| Revision delta | 29,970 | 27,900 |
| Revision delta minus successful replies | 711 | 2,036 |
| Through-drain duration | 9.858931 s | 9.076560 s |

Every error was attributed to the n2 endpoint. Endpoint sends remained balanced within one request. All three nodes reported term 2 before and after; `etcd_server_leader_changes_seen_total` remained 1. Process PIDs/start times were unchanged: rep 3 PIDs 5600/5601/5602, rep 4 PIDs 5762/5763/5764.

This is aggregate evidence of ambiguous write outcomes: the revision delta exceeds successful replies by 711 and 2,036. Under this fenced Put-only cohort, those increments are consistent with timed-out writes that committed. The artifacts do not identify each timed-out request's log index. Rep 3 has one fewer revision increment than requests; rep 4's missing 731 increments exactly match the admission rejections.

## Snapshot evidence and exact metric changes

These changes are between the corresponding `before.json` and `after.json`:

| Metric and node | Repetition 3 | Repetition 4 |
| --- | ---: | ---: |
| n1 `etcd_network_snapshot_send_success{To="1b835d3a0ab2f999"}` | absent -> 1 | absent -> 1 |
| n2 `etcd_network_snapshot_receive_success{From="bb0a12d34d6e033e"}` | absent -> 1 | absent -> 1 |
| n2 `etcd_snap_db_save_total_duration_seconds_count` | 0 -> 1 | 0 -> 1 |
| n2 `etcd_snap_db_save_total_duration_seconds_sum` | 0 -> 0.575417771 | 0 -> 0.490818065 |
| n2 `etcd_snap_db_fsync_duration_seconds_count` | 0 -> 1 | 0 -> 1 |
| n2 `etcd_snap_db_fsync_duration_seconds_sum` | 0 -> 0.168689809 | 0 -> 0.195923528 |
| n2 `etcd_server_proposals_failed_total` | 0 -> 712 | 0 -> 2,036 |

“Absent” means the labelled series was not present in the baseline scrape; the final series records one success. Reps 1, 2 and 5 have no received-snapshot success series and zero snapshot-DB-save count deltas on every node.

The sampled `etcd_server_snapshot_apply_in_progress_total` reaches 1 on n2 in both failed repetitions. Thus the evidence includes installation, not merely snapshot receipt:

- Rep 3 `samples.jsonl:24-26`: n2's post-WAL-save commit/applied gauges move from 37,969/35,333 to 38,914/35,333, then 39,859/39,859. Snapshot apply is 1 in the first two samples and 0 in the third.
- Rep 4 `samples.jsonl:23-25`: the gauges are 38,245/32,682, then 39,190/32,682 twice; snapshot apply is 1. The sampled difference reaches 6,508. By line 28, both gauges are 43,915, but 2,036 proposals remain pending.
- Rep 3 lines 60 and 70 likewise show both gauges at 47,596 while 712 proposals remain pending. Failed-proposal counters rise near seven-second request expiry, after apply has caught up.

The gauge difference is **post-save minus applied**, not an exact live commit-to-apply gap. The admission guard uses separate live indices.

n2 closes its peer streams at `14:26:38.686646Z` in rep 3 (`n2.log:510-519`) and `14:27:00.843436Z` in rep 4 (`n2.log:491-500`). Both occur inside the three-second arrival window, not cluster cleanup. Other members log EOF/broken-pipe consequences at those times.

n2 logs seven-second Put durations, for example rep 3 `n2.log:650` (`7.000547709s`) and rep 4 `n2.log:614` (`7.000277349s`). Sampled pending-proposal maxima are 3,320/3,510; goroutine maxima are 10,510/11,078. Sampled n2 heap allocation remains below 200 MB and RSS is approximately 350 MB at its maximum. There is no recorded OOM, quota alarm or failed storage syscall establishing those alternative causes.

Snapshot DB fsyncs are separate from `etcd_disk_wal_fsync_duration_seconds`: the WAL-only summary does not include these snapshot DB calls. Their elapsed times are not a critical-path decomposition.

## Source explanation, pinned etcd v3.7.2

Source pin: `68c065e562994b89e333e77b039ad066f933c586`. Local inspected source is under:

```text
/home/joshazimullah.linux/go/pkg/mod/go.etcd.io/etcd/server/v3@v3.7.2/
```

1. **Admission rejection is established.** `processInternalRaftRequestOnce` checks live applied and committed indices before allocating the request header, registering a waiter or proposing. Ordinary Put requests are rejected when committed is more than 5,000 entries ahead of applied. The priority exception is for LeaseRevoke, not Put. This explains rep 4's 731 too-many-requests responses; its sampled 6,508 difference corroborates the backlog condition.
   Sources: [v3_server.go:50-52](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/v3_server.go#L50-L52), [v3_server.go:1058-1063](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/v3_server.go#L1058-L1063), [util.go:60-74](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/util.go#L60-L74).

2. **Snapshot recovery resets transport and advances state without replaying all covered request results.** `applySnapshot` restores the backend, removes all peers, adds them again and advances the applied snapshot index. `applyEntries` skips entries already covered by that index. Normal per-request waiter notification occurs during individual entry application. This strongly supports snapshot-covered writes without corresponding individual replies as the reason pending proposals remain after the applied gauges catch up. It is a source-supported mechanism, not per-request traced attribution.
   Sources: [server.go:997-1143](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/server.go#L997-L1143), [server.go:1185-1207](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/server.go#L1185-L1207), [server.go:1945-1983](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/server.go#L1945-L1983). The receive-success counter is incremented after the snapshot handler accepts/processes the snapshot: [rafthttp/http.go:279-333](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/api/rafthttp/http.go#L279-L333).

3. **Seven-second Unavailable responses match the existing timeout classifier.** Request timeout is five seconds plus twice the election timeout, seven seconds with the default one-second election timeout. On expiry, a follower checks whether its connection to the leader has remained active since the request started. Resetting peers during snapshot restore breaks that continuity even if the leader and term remain unchanged and connections recover. The resulting error is “request timed out, possibly due to connection lost,” mapped to gRPC Unavailable. It does not prove an external network outage.
   Sources: [config.go:320-324](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/config/config.go#L320-L324), [v3_server.go:1108-1129](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/v3_server.go#L1108-L1129), [server.go:2359-2387](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/server.go#L2359-L2387), [util.go:52-55](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/util.go#L52-L55), [v3rpc/util.go:56-58](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/api/v3rpc/util.go#L56-L58).

The default snapshot count is 10,000 and retained catch-up entries are 5,000. Log compaction can therefore require a sufficiently lagging follower to recover by snapshot during these short, high-count runs. The successful send/receive metrics establish that this recovery occurred; they do not identify the initial resource bottleneck.
Sources: [server.go:78-85](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/server.go#L78-L85), [server.go:2140-2168](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/server.go#L2140-L2168).

No rerun, server execution, public comment or push was performed for this analysis.
