# Pilot workload audit

Evidence root: `/home/joshazimullah.linux/work_mounts/scratch/kube-burner/runs/38420832209`.

Reviewed all four stores/architectures using each `workload/*-burner.log`, phase `result.json`, native indexed JSON, `events.jsonl`, `errors.jsonl` and `summary.json`. No runner or workload changes made in this audit.

## Intended operations occurred

Counts below are identical in all four runs. Creation and patch counts come from successful native debug log entries, not configured iteration counts.

| Phase | Observed operations |
| --- | --- |
| Mixed | 1,250 ConfigMap, 30 Secret, 20 Service, 10 Deployment and 20 standalone Pod creations. These include the initial eight namespaces plus deletion/recreation of two namespaces. Native verification found the intended final 1,000 ConfigMaps, 24 Secrets, 16 Services, eight Deployments and 16 standalone Pods. Phase counts found 32 total workload Pods. |
| Node | 96 successful Pod creations: 80 initial plus two confirmed delete/recreate cycles of eight Pods each. Final native verification found 80 node-density Pods. Whole-workload counts reached 112 Pods in nine namespaces. |
| Update | Exactly three successful patch rounds: 24 Deployment, 3,000 ConfigMap, 72 Secret, 48 Service and 336 Pod patches. Each round selected eight Deployments, 1,000 ConfigMaps, 24 Secrets, 16 Services and 112 Pods. Native measurement indexed 48 distinct replacement Pod names, confirming actual rollouts rather than metadata-only Deployment updates. Final counts remained 112 Pods. |
| Delete | Removal attempts covered eight Deployments, 112 Pods, 16 Services, 1,000 ConfigMaps, 24 Secrets and nine namespaces. Final run-owned counts were empty, and whole-cluster baseline counts matched in every run. |

All 16 kube-burner phase commands exited zero, and all 16 indexed native job summaries had `passed: true`. Mixed, node and update logs contain no native error or warning lines. No Kubernetes Warning Events were exported.

| Run | Whole runner seconds | Delete Pod NotFound errors |
| --- | ---: | ---: |
| etcd X64 | 236.15 | 13 |
| Tigerstore X64 | 226.01 | 8 |
| etcd ARM64 | 228.70 | 8 |
| Tigerstore ARM64 | 223.53 | 5 |

These are single pilot runs, not performance conclusions.

## Measurement and deletion caveats

Each run indexed 40 mixed Pod timings, 80 node Pod timings and 48 update Pod timings, with Ready quantiles present. Node churn is not fully represented in its native latency sample. Event evidence contains 96 distinct node Pod UIDs with Started events, but only 80 with Scheduled events; native node measurements have no `churnMetric: true` records. The two churn cycles happened, but do not describe the native node quantiles as covering all 96 creations or claim 96 observed scheduler placements.

Every native deletion error is a NotFound for a Deployment-generated Pod. Deleting Deployments first lets garbage collection race the later broad Pod deletion list. This happened on both stores and architectures despite final cleanup succeeding. Restricting explicit Pod deletion to `app=density-pause` plus the run UUID removes that overlap while namespace deletion still waits for remaining descendants. Preserve the pilot errors rather than rewriting the pilot as a clean run.

The pilot's failed wrapper results also include background API error counters. Those are observations, not proof of workload or store failure. The separate `workloadPassed` classification is appropriate for the rerun. The current pilot still has real logged delete-operation errors even though native job summaries passed, so retaining the stricter operation-error classification until the selector fix is justified.

Mass deletion here is mostly individual resource deletion. The final namespace deletions occur after the 1,000 ConfigMap delete requests, rather than testing a 1,000-ConfigMap namespace cascade. The mixed churn phase does test namespace cascade deletion across 250 ConfigMaps.

## Final matrix audit

Evidence root: `/home/joshazimullah.linux/work_mounts/scratch/kube-burner/runs/38422256112`. Compared all 12 runs, six per store, against native logs, indexed measurements, phase results, final counts, storage verification and the parent's `repo/evidence/kube-burner/final-38422256112/summary/errors.json` and `log-errors-warnings.jsonl`.

All 48 phase commands exited zero and all 48 native job summaries passed. All 12 wrappers have `workloadPassed: true` and zero workload failures. Native kube-burner logs contain zero error, fatal or warning lines, including deletion. Whole-run durations were 221.48 to 237.74 seconds.

Actual successful operations were identical in every run:

| Phase | Per-run observed counts |
| --- | --- |
| Mixed | 1,250 ConfigMap, 30 Secret, 20 Service, 10 Deployment and 20 standalone Pod creations, including one two-namespace churn cycle. Final mixed counts were 1,000 ConfigMaps and 32 total Pods. |
| Node | 96 Pod creations, including two delete/recreate cycles of eight. Final node count was 80 and whole-workload count was 112 Pods. |
| Update | 24 Deployment, 3,000 ConfigMap, 72 Secret, 48 Service and 336 Pod patches. Three complete rounds occurred. |
| Delete | Eight Deployment, 96 standalone Pod, 16 Service, 1,000 ConfigMap, 24 Secret and nine Namespace removal attempts. The narrower Pod selector eliminated the pilot's explicit deletion/Deployment-GC overlap. |

Each run indexed exactly 40 mixed, 80 node and 48 update Pod timings and one Ready quantile document per corresponding phase. Totals across the matrix are 480, 960 and 576 Pod timing documents and 36 Ready quantile documents. The pilot's node-churn sampling caveat remains; the 80 native node samples must not be presented as all 96 creations.

Final run-owned Kubernetes counts were empty and whole-cluster baseline counts matched in all 12 runs. Every `storage-delete-verification.json` passed on all three members, giving 36 successful direct storage checks with zero live workload keys under `/registry/`. This verifies removal of the run's keys, not an empty registry or absence of historical tombstones.

### Final error classifications

The wrappers retained 259 API error-counter records as observations. Across the six runs per store, request-counter deltas were:

| HTTP code | etcd | Tigerstore |
| --- | ---: | ---: |
| 403 | 150 | 156 |
| 404 | 584 | 548 |
| 409 | 442 | 439 |
| 422 | 0 | 0 |
| 500 | 0 | 0 |

These include background authorization, deleted-object reads/status updates and conflict retries. They are not native workload-operation failures.

There was one distinct workload Warning Event, only on etcd X64 repetition 3. Its ADDED and DELETED watch records produce two retained observations of the same Event UID. The reason was `ClusterIPNotAllocated`, with message `Cluster IP [IPv4]: 10.96.8.171 is not allocated; repairing`, for Service `density-2-2` in mixed namespace 2. Thus a blanket claim of no Warning Events would be incorrect. No Tigerstore workload Warning Event was recorded.

Controller logs show no new Tigerstore-only failure signature. Shared classes include ReplicaSet/DaemonSet read-after-write informer consistency retries, EndpointSlice cache retries, startup lease authorization failures and a startup PodCIDR assignment race. Controller-manager stale-version retry lines total 380 for etcd versus 342 for Tigerstore; EndpointSlice cache retry lines total 266 versus 265. The PodCIDR race emitted three error lines on each store. All reconciliations needed by the workload still reached the verified ready and deletion states.

Three diagnostic signatures appeared only on Tigerstore in this matrix, but do not demonstrate a new storage failure:

- All six Tigerstore endpoint-health checks exited 1 because `Maintenance/Alarm` is unimplemented, with 18 client warning lines. This is the already-known diagnostic incompatibility, separate from the successful storage reads/deletion checks.
- Tigerstore X64 repetition 2 has one apiserver `watch.go:446` error, `Failed to encode watch event`, with `http2: stream closed`. It is a closed client watch stream, not evidence of a failed workload operation. Source is `diagnostics/burner-control-plane-kube-apiserver-277f9c638422.stderr:176`.
- Tigerstore X64 repetition 1 has three kubelet container-GC NotFound observations paired with three missing dead-log-symlink errors. They concern containerd/local filesystem cleanup after deletion, not storage RPC failures. Source is `diagnostics/burner-worker-kubelet.stdout`, lines 1585-1586, 1594-1595 and 1616-1617.

No source edits or new runs were performed for this final audit. Pilot failures remain documented separately above.
