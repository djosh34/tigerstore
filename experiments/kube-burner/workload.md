# Workload runner

Ready for the parent's real cluster run. No Kubernetes workload ran on this host.

```sh
python3 workload.py --out "$OUT/workload"
```

`kube-burner` v2.8.6 and `kubectl` must be on PATH. Inherit the private `KUBECONFIG`, `PRIVATE`, and `STORE` from the parent. `--out` must be an absolute, fresh directory. The runner uses `metrics.py` beside itself, or `--metrics /absolute/path/metrics.py`.

## Phases

- `mixed` creates eight namespaces. Each has one two-pod Deployment, two standalone Pods, two Services, 125 ConfigMaps and three Secrets. It churns two namespaces once. Creation QPS is 40 with burst 50.
- `node` adds 80 standalone Pods in one namespace. It deletes and recreates eight Pods twice. Kubernetes generates scheduling and container lifecycle Events.
- `update` performs three sequential rounds of Deployment rollouts and ConfigMap, Secret, Service and Pod patches. It waits for Deployment readiness between rounds. QPS is 40 with burst 50. The rollout patch changes the pod-template generation annotation and uses the current kube-burner RunID so the native watcher can observe replacement Pods.
- `delete` deletes run-owned Deployments, Pods, Services, ConfigMaps, Secrets and then Namespaces. kube-burner waits for each selected resource type to disappear. The final counts include ReplicaSets.

There are 112 steady workload Pods. Zero rollout surge and zero termination grace bound the planned workload peak to 128. The runner requires exactly three `burner-` nodes and at most 22 baseline Pods. This is a sizing bound, not a live enforcement of a cluster-wide pod limit. Pod requests total about 224 millicores and 448 MiB at steady state. Each Pod uses the preloaded `registry.k8s.io/pause:3.10` image. No preload DaemonSet runs.

The default whole-run limit is 570 seconds, including snapshots and counts. Phase workload limits are 150, 150, 150 and 90 seconds. The runner shortens those limits to reserve deletion time. Skipped phases and timeouts are failed results. It starts nothing at or after 2026-10-13T22:00:00Z. Interrupted or timed-out runs can leave objects behind, which the evidence must not call successful deletion.

## Evidence and failure handling

Each phase calls `python3 metrics.py snapshot BEFORE`, runs stock kube-burner directly against the inherited kubeconfig, captures `AFTER`, and calls `python3 metrics.py diff BEFORE AFTER diff.json`. There is no timing proxy or custom API client.

`summary.json` includes total and per-phase wall durations, command return codes, run-owned counts, API histogram results and successful create/delete counter deltas when available. API counters include background cluster traffic. `native-summary.json` retains native `podLatencyQuantilesMeasurement` and `jobSummary` records. Raw native JSON stays in each phase's `indexed/` directory. Native pod latency units are milliseconds. API histogram quantiles are estimates in seconds, not exact per-object timings.

Generic object request latency is not a native v2.8.6 measurement. The separate API-server histograms in `diff.json` provide p50/p99 estimates and request rates by resource and verb when the helper has valid data. Missing measurements stay missing. `jobLatency` measures Kubernetes Job objects, not kube-burner phases, so this workload uses `jobSummary` for phase duration. The pause image does not listen on the Service port. `serviceLatency` would produce failed connectivity checks rather than a meaningful measurement, so it is not enabled.

`commands.jsonl` records command exits and durations. `*-burner.log` preserves stdout/stderr from each kube-burner invocation. `errors.jsonl` records failed commands, native error/fatal lines, failed native job summaries, Kubernetes Warning Events and observed API 4xx/5xx counter deltas. API errors and Warning Events are observations with `workloadFailure: false`. They remain in `errors.jsonl` but do not fail the workload by themselves. Native operation error lines, failed native job summaries, failed commands, skipped phases, missing required native measurements and leftover run-owned objects have `workloadFailure: true`. `summary.json` separates `workloadPassed` and `workloadFailureCount` from the total `errorCount` and observation-only `observedErrorCount`. The compatibility field `passed` equals `workloadPassed`, and the exit code follows `workloadPassed`. Observed API errors include background traffic and do not establish a store bug. Update pod quantiles can be absent if no new ready Pods reach the native watcher; this is an explicit measurement issue rather than proof of storage failure.

`baseline-counts.log`, `final-counts.log`, and phase count logs contain only object kinds, namespaces and names. No Secret payloads or kubeconfig contents enter evidence. The cluster-wide Event stream stays under `PRIVATE`; `events.jsonl` exports only this run's namespaces. Final run-owned object counts must be zero. Whole-cluster count drift is retained but does not itself fail the workload, because unrelated controllers can change their objects.

## Upstream inspected before implementation

Source inspection used the v2.8.6 tag, including configuration defaults, patch/delete handlers, sequential object waiting, job summary JSON fields and pod watcher selectors.

- https://github.com/kube-burner/kube-burner/blob/v2.8.6/pkg/config/types.go
- https://github.com/kube-burner/kube-burner/blob/v2.8.6/pkg/config/config.go
- https://github.com/kube-burner/kube-burner/blob/v2.8.6/pkg/burner/create.go
- https://github.com/kube-burner/kube-burner/blob/v2.8.6/pkg/burner/patch.go
- https://github.com/kube-burner/kube-burner/blob/v2.8.6/pkg/burner/delete.go
- https://github.com/kube-burner/kube-burner/blob/v2.8.6/pkg/burner/utils.go
- https://github.com/kube-burner/kube-burner/blob/v2.8.6/pkg/burner/metadata.go
- https://github.com/kube-burner/kube-burner/blob/v2.8.6/pkg/burner/job.go
- https://github.com/kube-burner/kube-burner/blob/v2.8.6/pkg/measurements/pod_latency.go
- https://kube-burner.github.io/kube-burner/latest/reference/configuration/
- https://kube-burner.github.io/kube-burner/latest/measurements/

The source defines `jobSummary.passed` as a Boolean. Each phase uses one job and keeps native indexing enabled. Namespace and object selectors use one fresh UUID across all phases. Automatic cleanup and global garbage collection are disabled to avoid deleting another run.
