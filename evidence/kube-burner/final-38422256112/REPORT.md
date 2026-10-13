# Kubernetes on tigerstore, three repetitions per store and architecture

All 12 final jobs passed in [run 38422256112](https://github.com/djosh34/tigerstore/actions/runs/38422256112). All 48 kube-burner phase commands and native job summaries passed. Every run finished with zero run-owned objects, and linearizable registry-key reads from all three store members found no remaining workload keys. This is workload compatibility evidence, not full conformance or the 50k-object test.

[Branch](https://github.com/djosh34/tigerstore/tree/exp/kube-burner). [Complete evidence and per-run numbers](https://github.com/djosh34/tigerstore/tree/exp/kube-burner/evidence/kube-burner/final-38422256112). [Scripts and workload](https://github.com/djosh34/tigerstore/tree/292f36bfdcbb16aa2aa3d68e6a71ca46b8da97b9/experiments/kube-burner). No product code changed; nothing merged.

## Exact versions and setup

- Measured tigerstore main `90870876b6459334c7eddd67c0f9d184267f2eba`, current when this experiment began. Each job built a separate clean checkout with Go 1.27.2, `CGO_ENABLED=0 GOFLAGS=-p=2 GOMEMLIMIT=2GiB go build -trimpath -o "$BIN/tigerstore" ./cmd/tigerstore`. All six binaries record that revision and `vcs.modified=false`.
- Experiment commit `292f36bfdcbb16aa2aa3d68e6a71ca46b8da97b9`.
- Stock etcd v3.7.2, upstream `68c065e562994b89e333e77b039ad066f933c586`, release binary built with Go 1.26.8. Archive checksums verified.
- Latest kube-burner release v2.8.6, `7c9ba86b36d89201ee4dd7aa86819010bd31bfb8`, built with Go 1.25.14. Kind v0.33.0, `407a9675e6d9af1200b5f57f9ca52ec6cdacce74`, built with Go 1.26.7.
- Kubernetes and kubectl v1.37.0, `f54c212e3a2f75d674b717a9b29052b20b60aefc`, built with Go 1.26.6. Node image `kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5`.

Each store ran in a separate free runner job of the same label, as requested. Each repetition used a fresh VM, certificates, three-member store and three-node kind cluster. Client and peer connections use certificate-authenticated TLS. All processes share four vCPUs and about 16 GB RAM, without CPU pinning or injected delay. Data directories are on ext4 with barriers enabled. Etcd uses an 8 GiB quota and default safe persistence. All replicas share a VM and disk, so this is not a multi-host durability result.

ARM jobs all reported Neoverse-N2, image `ubuntu24-arm64 20261004.142.1`. AMD64 image `ubuntu24 20261004.327.1` supplied different CPUs: etcd repetitions used Xeon 8573C, EPYC 7763 and EPYC 9V74; tigerstore used Xeon 8370C, EPYC 7763 and Xeon 8370C. The AMD64 results are not hardware-matched causal evidence. Versions, binary hashes, mounts and exact container arguments are in every raw archive.

## Workload and measurement

Eight mixed namespaces hold 1,000 ConfigMaps, 24 synthetic Secrets, 16 Services, eight two-pod Deployments and 16 standalone Pods. The mixed phase deletes and recreates two namespaces. Node density adds 80 Pods, then deletes and recreates eight twice. Three update rounds perform 3,480 patches, including actual Deployment rollouts. Mass deletion removes the objects and nine namespaces. Before deletion the tracked inventory contains 1,185 objects, including Pods and ReplicaSets. Events, EndpointSlices and unlabelled namespace defaults are outside that inventory, but the final registry-key check covers their namespace keys too.

Each run observed 1,330 successful mixed object-create calls plus namespace creation, 96 node Pod-create calls and 48 rollout replacement Pods. Kubernetes generated 1,094 distinct workload Events per run, except etcd AMD64 repetition 3 with 1,095. Containers run preloaded `pause:3.10`; this does not measure an application or Service connectivity. QPS/burst are 40/50 for mixed and update, 15/20 for node density, and 20/25 for deletion. This is a rate-limited workload, not a capacity test.

Every table cell below is **min / median / max across three repetitions**. Percentiles are per-run statistics, not pooled samples. The linked CSV retains every repetition, phase, resource, verb, status code and member CPU value.

## Runtime, CPU and storage traffic

| Metric | AMD64 etcd | AMD64 tigerstore | ARM64 etcd | ARM64 tigerstore |
|---|---:|---:|---:|---:|
| Workload wall time, s | 228.96 / 229.49 / 237.74 | 227.44 / 227.95 / 234.39 | 223.65 / 226.73 / 226.87 | 221.48 / 222.91 / 225.74 |
| Whole script through diagnostics, s | 289.30 / 294.98 / 297.19 | 308.88 / 314.76 / 318.51 | 284.44 / 285.64 / 289.12 | 296.76 / 304.10 / 304.24 |
| Mixed phase, s | 44.25 / 44.28 / 45.20 | 44.21 / 44.25 / 44.31 | 43.19 / 44.20 / 44.21 | 43.18 / 43.21 / 44.20 |
| Node phase, s | 20.66 / 21.71 / 26.63 | 20.94 / 21.60 / 26.57 | 18.63 / 18.69 / 18.83 | 17.64 / 18.54 / 19.82 |
| Update phase, s | 86.57 / 86.59 / 86.67 | 86.57 / 86.63 / 86.65 | 86.58 / 86.58 / 86.62 | 86.58 / 86.62 / 86.63 |
| Deletion phase, s | 65.03 / 67.02 / 67.28 | 65.04 / 65.04 / 65.05 | 65.01 / 67.03 / 67.04 | 65.02 / 65.03 / 65.05 |
| All three server processes, CPU s | 41.68 / 50.12 / 56.61 | 26.44 / 28.46 / 37.84 | 48.75 / 50.97 / 54.80 | 33.17 / 33.32 / 37.36 |
| Average occupied server cores, sum | 0.181 / 0.218 / 0.237 | 0.115 / 0.124 / 0.161 | 0.217 / 0.224 / 0.241 | 0.148 / 0.150 / 0.165 |
| API-observed storage RPC/s | 76.77 / 79.53 / 79.64 | 77.63 / 80.07 / 80.09 | 80.02 / 80.31 / 81.14 | 80.71 / 81.60 / 82.33 |
| Successful object-create API calls/s, mixed | 35.82 / 36.38 / 36.44 | 36.43 / 36.55 / 36.59 | 36.58 / 36.59 / 37.44 | 36.58 / 37.48 / 37.50 |
| Successful object-delete API calls/s, delete | 22.37 / 22.39 / 23.10 | 23.07 / 23.13 / 23.13 | 22.45 / 22.45 / 23.14 | 23.13 / 23.14 / 23.17 |
| Verified tracked-object removal/s | 17.61 / 17.68 / 18.22 | 18.22 / 18.22 / 18.22 | 17.68 / 17.68 / 18.23 | 18.22 / 18.22 / 18.23 |

CPU is user plus system time from each server's actual `/proc/PID/stat`, with PID, start-time and container identity checks. It excludes I/O wait and other processes. Storage throughput is the API server's `etcd_request_duration_seconds_count` delta per second, including controller traffic. It is not a count of successful application writes or a saturation maximum. Snapshot and CPU boundaries have their own recorded timestamps. The whole script time includes downloads and tigerstore compilation, but excludes final teardown and artifact upload; it is not a database performance metric.

The stock-etcd native `grpc_server_handled_total` rate, summed across members, was AMD64 **78.43 / 81.12 / 81.30 RPC/s**, ARM64 **81.66 / 81.93 / 82.80 RPC/s**. Long-lived Watch streams count when completed, not once per event. Tigerstore-native counters are unavailable because `/metrics` returns 404, filed as #94. They are not zero. Raw etcd proposal, byte and method counters are retained too.

Creation/deletion API rates include successful controller calls but exclude subresources. Repeated calls are not deduplicated into object identities. Verified removal rate is exactly 1,185 tracked objects divided by the deletion phase duration. Storage verification checks current keys, not removal of historical MVCC versions or WAL bytes. Most mass deletion is individual deletes; the mixed churn phase separately exercises namespace cascade deletion of 250 ConfigMaps.

## Native kube-burner pod readiness latency

| Metric | AMD64 etcd | AMD64 tigerstore | ARM64 etcd | ARM64 tigerstore |
|---|---:|---:|---:|---:|
| Mixed Ready p50, s | 1.00 / 1.00 / 2.00 | 1.00 / 1.00 / 2.00 | 1.00 / 1.00 / 1.00 | 1.00 / 1.00 / 1.00 |
| Mixed Ready p99, s | 2.00 / 2.00 / 2.00 | 2.00 / 2.00 / 2.00 | 1.00 / 2.00 / 2.00 | 1.50 / 2.00 / 2.00 |
| Node density Ready p50, s | 6.00 / 6.00 / 9.00 | 6.00 / 6.00 / 9.00 | 3.00 / 4.00 / 4.00 | 3.00 / 3.00 / 5.00 |
| Node density Ready p99, s | 8.00 / 9.00 / 11.50 | 8.50 / 9.00 / 12.00 | 5.50 / 6.00 / 6.00 | 5.50 / 6.00 / 7.00 |
| Update rollouts Ready p50, s | 2.00 / 2.00 / 2.00 | 2.00 / 2.00 / 2.00 | 1.00 / 1.00 / 1.00 | 1.00 / 1.00 / 1.00 |
| Update rollouts Ready p99, s | 2.00 / 2.00 / 3.00 | 2.00 / 2.00 / 3.00 | 2.00 / 2.00 / 2.00 | 2.00 / 2.00 / 2.00 |

Each repetition has 40 mixed, 80 node-density and 48 rollout latency samples. Node churn made 96 Pod creations but its native latency sample covers only 80. Only 80 node-density Pod UIDs have observed Scheduled events; do not call this all-creation latency. Readiness measurements include scheduler/kubelet/container work, and Kubernetes condition timestamps have coarse resolution.

Kube-burner v2.8.6 has no generic object-request latency measurement. No proxy or patched binary was added to manufacture one. The following are API-server object-request histogram estimates, not native kube-burner object percentiles.

## API request latency from apiserver metrics

| Metric | AMD64 etcd | AMD64 tigerstore | ARM64 etcd | ARM64 tigerstore |
|---|---:|---:|---:|---:|
| All non-WATCH/CONNECT requests p50, ms | 4.356 / 4.633 / 4.814 | 3.521 / 3.677 / 4.125 | 3.752 / 3.902 / 4.339 | 3.539 / 3.625 / 4.035 |
| All non-WATCH/CONNECT requests p99, ms | 248.219 / 313.888 / 567.191 | 198.267 / 200.019 / 247.059 | 188.117 / 193.294 / 259.811 | 143.428 / 160.761 / 174.174 |
| namespaces p50, ms | 2.948 / 3.021 / 3.658 | 2.706 / 2.706 / 2.726 | 2.789 / 2.939 / 2.955 | 2.818 / 2.826 / 2.870 |
| namespaces p99, ms | 24.575 / 24.625 / 291.000 | 22.912 / 22.912 / 24.188 | 24.254 / 24.277 / 24.362 | 23.229 / 23.264 / 23.785 |
| deployments p50, ms | 12.198 / 12.742 / 15.765 | 4.473 / 4.865 / 11.510 | 5.602 / 5.629 / 10.204 | 4.392 / 4.849 / 8.315 |
| deployments p99, ms | 98.403 / 183.583 / 190.381 | 83.375 / 95.568 / 102.250 | 96.667 / 97.250 / 144.286 | 85.700 / 93.386 / 94.281 |
| pods p50, ms | 12.348 / 14.642 / 17.123 | 6.830 / 8.456 / 11.979 | 8.989 / 11.345 / 14.503 | 5.392 / 7.096 / 11.718 |
| pods p99, ms | 384.085 / 426.880 / 1940.167 | 374.707 / 390.400 / 392.868 | 406.952 / 455.250 / 564.000 | 192.225 / 276.158 / 278.923 |
| services p50, ms | 4.478 / 4.918 / 6.250 | 3.488 / 3.704 / 4.286 | 4.167 / 4.615 / 4.688 | 3.846 / 4.286 / 4.478 |
| services p99, ms | 196.000 / 480.000 / 580.000 | 99.286 / 140.000 / 386.667 | 176.000 / 280.000 / 340.000 | 99.412 / 140.000 / 352.000 |
| configmaps p50, ms | 3.014 / 3.227 / 3.547 | 2.728 / 2.799 / 3.004 | 2.787 / 2.844 / 3.000 | 2.715 / 2.769 / 2.909 |
| configmaps p99, ms | 105.481 / 182.132 / 252.264 | 43.709 / 49.960 / 77.227 | 57.313 / 64.167 / 67.021 | 47.163 / 76.107 / 87.108 |
| secrets p50, ms | 2.757 / 2.917 / 3.096 | 2.580 / 2.597 / 2.701 | 2.701 / 2.738 / 2.917 | 2.648 / 2.648 / 2.738 |
| secrets p99, ms | 92.375 / 119.500 / 179.667 | 59.750 / 86.583 / 89.833 | 83.900 / 86.583 / 94.917 | 73.167 / 83.900 / 119.500 |

These merge resource request histograms over the whole workload, excluding WATCH and CONNECT. They include multiple verbs, subresources and controller traffic. For example, the Pods group includes binding requests; it is not pure Pod-create latency. Percentiles use linear interpolation within histogram buckets. Raw buckets allow recalculation. New label series get a zero baseline only when both scrapes succeed and exporter start time is unchanged; missing counters and resets remain explicit. In two runs, the request counter and duration histogram differ by one observation in the raw metrics. This is consistent with non-atomic scraping; each family uses its own recorded values.

## Errors and earlier attempts

Final runs had zero kube-burner command failures, zero failed native job summaries, zero native error/fatal lines, zero remaining workload keys and zero measured API HTTP 500 increments. They were not error-free Kubernetes traces. All observed errors and warnings remain in the raw logs, `workload/errors.jsonl`, detailed API status counters, and [the log index](https://github.com/djosh34/tigerstore/blob/exp/kube-burner/evidence/kube-burner/final-38422256112/summary/log-errors-warnings.jsonl). The index contains repeated log records, not unique incidents.

| Metric | AMD64 etcd | AMD64 tigerstore | ARM64 etcd | ARM64 tigerstore |
|---|---:|---:|---:|---:|
| API HTTP 403 responses | 23 / 24 / 26 | 25 / 25 / 28 | 23 / 26 / 28 | 24 / 27 / 27 |
| API HTTP 404 responses | 91 / 96 / 126 | 82 / 89 / 94 | 85 / 90 / 96 | 89 / 97 / 97 |
| API HTTP 409 responses | 79 / 82 / 85 | 73 / 80 / 82 | 63 / 63 / 70 | 64 / 66 / 74 |
| API HTTP 500 responses | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 | 0 / 0 / 0 |

Both stores logged startup namespace/RBAC failures, controller cache-version conflicts, EndpointSlice retry messages, kubelet status errors after object deletion, and container/cgroup cleanup errors. Stock etcd also logged peer-startup and slow-request warnings. Their presence in both stores is evidence against treating the message alone as a tigerstore-only bug, not proof that every underlying cause was isolated. One distinct workload Warning Event occurred on etcd AMD64 repetition 3: `ClusterIPNotAllocated`, `Cluster IP [IPv4]: 10.96.8.171 is not allocated; repairing`, for Service `density-2-2`. Its ADDED and DELETED notifications are both retained. No tigerstore workload Warning Event was recorded.

Two isolated tigerstore diagnostics remain unconfirmed as storage bugs: AMD64 repetition 2 logged one `Failed to encode watch event` with `http2: stream closed`; repetition 1 logged three containerd-GC NotFound messages paired with missing dead-log-symlink errors. They did not fail the workload and were not reproduced twice as tigerstore-only storage failures. The [workload audit](https://github.com/djosh34/tigerstore/blob/exp/kube-burner/experiments/kube-burner/workload-audit.md) gives exact files and lines. These observations are not silently treated as an error-free run.

Confirmed tigerstore-only failures, each reproduced on both architectures and again in the final repetitions, are labelled `bug` and attached as sub-issues of #48:

- #92: stock `etcdctl endpoint health` fails because `Maintenance.Alarm` is Unimplemented. All six final tigerstore probes failed; all six stock probes passed. The experiment records that failure and gates both stores on linearizable reads instead. No product fix was made.
- #94: the authenticated stock-etcd `/metrics` scrape returns HTTP 404 on tigerstore and 200 on etcd. This is an observability gap, not a Kubernetes workload failure. Common storage-RPC and OS-CPU measurements remain available.

Earlier attempts are retained, not counted in the final three repetitions:

1. `38419020116`, code `116fab403d088d715b7f661db8e1703438747ef0`: stock boot passed on both architectures; both tigerstore jobs stopped at the Alarm health probe before kind.
2. `38419585923`, code `cfd3b2e44ce7815ff76af272d0362d47dbbb2613`: all four boot-only jobs passed after changing the readiness gate, with Alarm failures still recorded.
3. `38420832209`, code `a503526ee91d4a816f9a0a3b3f0d82a7e4a5a984`: all 16 native workload phases passed and deleted their objects, but the wrapper failed every job. It wrongly classified all API error counters as workload failures, and its Pod deletion selection raced Deployment garbage collection. Both stores logged Pod NotFound deletions. The final script separates observed errors from workload failures and selects only standalone Pods for direct deletion. The pilot archive keeps the original failed results.

## Reproduction

The workflow creates the clean product checkout and exact environment. Before the stated deadline, run the retained branch with:

```sh
gh workflow run kube-burner.yml --repo djosh34/tigerstore --ref exp/kube-burner
```

The measured workflow executes this once in each matrix job:

```sh
TIGERSTORE_REF=90870876b6459334c7eddd67c0f9d184267f2eba \
  STORE=tigerstore REP=1 \
  timeout --kill-after=20s 1260s bash experiments/kube-burner/run.sh
# STORE=etcd for the control; REP=1,2,3; both free runner labels.
# RUNNER_TEMP, RUNNER_ARCH and GITHUB_SHA are supplied by Actions.
```

For each of `workload-create.yml`, `workload-node.yml`, `workload-update.yml` and `workload-delete.yml`, the runner calls:

```sh
kube-burner init -c <config> --uuid <run-uuid> \
  --user-data <phase/user-data.json> --timeout <phase-budget>s \
  --skip-log-file --log-level debug
python3 experiments/kube-burner/metrics.py snapshot <before-or-after>
python3 experiments/kube-burner/metrics.py diff <before> <after> <diff.json>
```

Exact argv, generated user data, durations and exits are in each `commands.jsonl`. After extracting the twelve raw archives into separate directories, recompute tables with:

```sh
python3 experiments/kube-burner/summarize.py <extracted-run-root> <summary-output>
python3 experiments/kube-burner/error-index.py <extracted-run-root> <summary-output>
python3 experiments/kube-burner/metrics.py selftest
```

The scripts refuse new runs at or after 2026-10-13T22:00:00Z. No heavy builds, Kubernetes clusters or benchmarks ran on the agent host. Local validation used Python syntax/arithmetic tests, 11 metric self-tests, shellcheck and actionlint. No product packages changed. Both Sol helpers audited the evidence and were deleted. Their reports are committed beside the scripts. Extracted local run data was removed after archiving; the experiment branch and complete raw archives remain.

## What this proves

This pinned main commit boots real Kubernetes 1.37 and completes the stated mixed workload, updates and deletion on both runner labels, three times each. The final state checks passed on all three replicas every time. This load used less measured server CPU on tigerstore, while workload-time spreads overlap. API tails do not improve for every resource. Different AMD64 CPUs and separate VMs prevent attributing all differences to the store.

Unproven: full conformance, the upstream storage-suite gate, the 50k-object test, maximum throughput or capacity, production application behavior, multi-host latency, and crash durability. A general Kubernetes speedup remains a guess. Do not close #48 or #50 from this experiment.
