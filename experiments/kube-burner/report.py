#!/usr/bin/env python3
"""Render the final report from archived spread rows."""
import json
from pathlib import Path
import statistics

base = Path('evidence/kube-burner/final-38422256112')
spread = json.loads((base / 'summary/spread.json').read_text())
index = {(r['arch'], r['store'], r['metric']): r for r in spread}
columns = [('X64', 'etcd'), ('X64', 'tigerstore'), ('ARM64', 'etcd'), ('ARM64', 'tigerstore')]
lines = []


def text(value):
    lines.append(value.strip() + '\n')


def table(metrics):
    lines.extend(['| Metric | AMD64 etcd | AMD64 tigerstore | ARM64 etcd | ARM64 tigerstore |',
                  '|---|---:|---:|---:|---:|'])
    for label, name, scale, digits in metrics:
        cells = []
        for arch, store in columns:
            row = index[arch, store, name]
            assert row['n'] == 3 and row['all_workloads_passed'], (arch, store, name, row)
            cells.append(' / '.join(f'{row[k] * scale:.{digits}f}' for k in ['min', 'median', 'max']))
        lines.append('| ' + label + ' | ' + ' | '.join(cells) + ' |')
    lines.append('')


text('''# Kubernetes on tigerstore, three repetitions per store and architecture

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
''')
text('## Runtime, CPU and storage traffic')
table([
 ('Workload wall time, s', 'total.workload', 1, 2),
 ('Whole script through diagnostics, s', 'total.script', 1, 2),
 ('Mixed phase, s', 'mixed.duration', 1, 2),
 ('Node phase, s', 'node.duration', 1, 2),
 ('Update phase, s', 'update.duration', 1, 2),
 ('Deletion phase, s', 'delete.duration', 1, 2),
 ('All three server processes, CPU s', 'total.cpu.all', 1, 2),
 ('Average occupied server cores, sum', 'total.cpu.cores', 1, 3),
 ('API-observed storage RPC/s', 'total.storage.etcd_request_duration_seconds', 1, 2),
 ('Successful object-create API calls/s, mixed', 'mixed.api.creates.rate', 1, 2),
 ('Successful object-delete API calls/s, delete', 'delete.api.deletes.rate', 1, 2),
 ('Verified tracked-object removal/s', 'delete.verified_deletion_rate', 1, 2),
])
text('''CPU is user plus system time from each server's actual `/proc/PID/stat`, with PID, start-time and container identity checks. It excludes I/O wait and other processes. Storage throughput is the API server's `etcd_request_duration_seconds_count` delta per second, including controller traffic. It is not a count of successful application writes or a saturation maximum. Snapshot and CPU boundaries have their own recorded timestamps. The whole script time includes downloads and tigerstore compilation, but excludes final teardown and artifact upload; it is not a database performance metric.

The stock-etcd native `grpc_server_handled_total` rate, summed across members, was AMD64 **78.43 / 81.12 / 81.30 RPC/s**, ARM64 **81.66 / 81.93 / 82.80 RPC/s**. Long-lived Watch streams count when completed, not once per event. Tigerstore-native counters are unavailable because `/metrics` returns 404, filed as #94. They are not zero. Raw etcd proposal, byte and method counters are retained too.

Creation/deletion API rates include successful controller calls but exclude subresources. Repeated calls are not deduplicated into object identities. Verified removal rate is exactly 1,185 tracked objects divided by the deletion phase duration. Storage verification checks current keys, not removal of historical MVCC versions or WAL bytes. Most mass deletion is individual deletes; the mixed churn phase separately exercises namespace cascade deletion of 250 ConfigMaps.
''')
text('## Native kube-burner pod readiness latency')
table([(f'{phase} Ready {q}, s', f'{key}.pod.Ready.{q}', .001, 2)
       for key, phase in [('mixed', 'Mixed'), ('node', 'Node density'), ('update', 'Update rollouts')]
       for q in ['p50', 'p99']])
text('''Each repetition has 40 mixed, 80 node-density and 48 rollout latency samples. Node churn made 96 Pod creations but its native latency sample covers only 80. Only 80 node-density Pod UIDs have observed Scheduled events; do not call this all-creation latency. Readiness measurements include scheduler/kubelet/container work, and Kubernetes condition timestamps have coarse resolution.

Kube-burner v2.8.6 has no generic object-request latency measurement. No proxy or patched binary was added to manufacture one. The following are API-server object-request histogram estimates, not native kube-burner object percentiles.
''')
text('## API request latency from apiserver metrics')
table([('All non-WATCH/CONNECT requests p50, ms', 'total.api.all.p50', 1, 3),
       ('All non-WATCH/CONNECT requests p99, ms', 'total.api.all.p99', 1, 3)] +
      [(f'{resource} {q}, ms', f'total.api.resource.{resource}.{q}', 1, 3)
       for resource in ['namespaces', 'deployments', 'pods', 'services', 'configmaps', 'secrets']
       for q in ['p50', 'p99']])
text('''These merge resource request histograms over the whole workload, excluding WATCH and CONNECT. They include multiple verbs, subresources and controller traffic. For example, the Pods group includes binding requests; it is not pure Pod-create latency. Percentiles use linear interpolation within histogram buckets. Raw buckets allow recalculation. New label series get a zero baseline only when both scrapes succeed and exporter start time is unchanged; missing counters and resets remain explicit. In two runs, the request counter and duration histogram differ by one observation in the raw metrics. This is consistent with non-atomic scraping; each family uses its own recorded values.

## Errors and earlier attempts

Final runs had zero kube-burner command failures, zero failed native job summaries, zero native error/fatal lines, zero remaining workload keys and zero measured API HTTP 500 increments. They were not error-free Kubernetes traces. All observed errors and warnings remain in the raw logs, `workload/errors.jsonl`, detailed API status counters, and [the log index](https://github.com/djosh34/tigerstore/blob/exp/kube-burner/evidence/kube-burner/final-38422256112/summary/log-errors-warnings.jsonl). The index contains repeated log records, not unique incidents.
''')
table([(f'API HTTP {code} responses', 'total.api.http' + code, 1, 0) for code in ['403', '404', '409', '500']])
text(r'''Both stores logged startup namespace/RBAC failures, controller cache-version conflicts, EndpointSlice retry messages, kubelet status errors after object deletion, and container/cgroup cleanup errors. Stock etcd also logged peer-startup and slow-request warnings. Their presence in both stores is evidence against treating the message alone as a tigerstore-only bug, not proof that every underlying cause was isolated. One distinct workload Warning Event occurred on etcd AMD64 repetition 3: `ClusterIPNotAllocated`, `Cluster IP [IPv4]: 10.96.8.171 is not allocated; repairing`, for Service `density-2-2`. Its ADDED and DELETED notifications are both retained. No tigerstore workload Warning Event was recorded.

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
''')
(base / 'REPORT.md').write_text('\n'.join(lines))
print(base / 'REPORT.md')
