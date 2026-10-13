# Kubernetes comparison

Throwaway experiment for #48 and #50. No product files change. Do not merge this branch.

## Targets and environment

The experiment pins main at `90870876b6459334c7eddd67c0f9d184267f2eba`, current when work began. Each tigerstore job checks out that commit separately and builds it with Go 1.27.2, `CGO_ENABLED=0`, `GOFLAGS=-p=2`, `GOMEMLIMIT=2GiB`, and `go build -trimpath`. The stock target is the checksum-verified etcd v3.7.2 release, upstream commit `68c065e562994b89e333e77b039ad066f933c586`.

Kube-burner v2.8.6 was the latest release on 2026-10-13. Its tag resolves to `7c9ba86b36d89201ee4dd7aa86819010bd31bfb8`. Kind v0.33.0 resolves to `407a9675e6d9af1200b5f57f9ca52ec6cdacce74`. Kubernetes and kubectl are v1.37.0. The multi-architecture node image is `kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5`. Binary hashes and build identities are saved for each job.

Each job uses one free `ubuntu-24.04` or `ubuntu-24.04-arm` runner. Three store containers run outside kind on its Docker bridge. Kind has one control-plane node and two workers. All processes share the runner's CPUs with no pinning or added delay. Both stores require client certificates and encrypted peer connections. Etcd has an 8 GiB quota and default safe persistence. Tigerstore has no enforced quota. Both use separate data directories on the runner's ext4 root mount after enabling barriers. All replicas share one VM, disk and failure domain. This does not prove power-loss durability or multi-host performance.

The topology and certificates follow the stock-etcd pre-work on #48, script commit `617e1063ce8c65e889377f5facbc887f2e9f1411`. No private key, kubeconfig, raw Secret, or etcd data directory enters the evidence.

## Running

Only run on a fresh disposable GitHub runner, before 2026-10-13T22:00:00Z. The workflow builds and downloads there, not on the agent host.

```sh
# Check out this experiment branch, then check out the pinned main into product/.
# actions/setup-go installs Go 1.27.2.
export TIGERSTORE_REF=90870876b6459334c7eddd67c0f9d184267f2eba
export STORE=tigerstore REP=1
# RUNNER_TEMP, RUNNER_ARCH and GITHUB_SHA come from Actions.
timeout --kill-after=20s 1260s bash experiments/kube-burner/run.sh
```

Use `STORE=etcd` for the baseline. `BOOT_ONLY=1` runs a diagnostic boot without a workload. The comparison matrix runs each store in a separate job of the same runner type, as requested, rather than a paired back-to-back run on one VM. Three repetitions mean three fresh VMs, certificates, stores and Kubernetes clusters per target and architecture. Report each architecture independently.

## Measurements and limits

`workload.md` describes the mixed workload. `metrics.md` describes the counters and histogram estimates. Stock kube-burner talks directly to the API server with its original kubeconfig. There is no instrumenting proxy or patched kube-burner binary.

Native pod readiness distributions measure scheduling and container startup as well as storage work. Kube-burner v2.8.6 has no generic object-request latency measurement. API histograms provide per-resource and per-verb object-request p50/p99 estimates instead, with that distinction in the report. They cover controller traffic too, not just kube-burner requests. Histograms estimate quantiles by interpolation, not exact request samples.

The common storage rate is the API server's client-observed storage RPC count delta per second. It is not a count of successful application writes. Also report MVCC revision changes and stock etcd's available native counters. Tigerstore has no `/metrics` endpoint on this commit. Read CPU from the three actual server PIDs through `/proc`, without product instrumentation. Do not claim a server-native tigerstore throughput counter exists.

Keep every failed attempt. `collect.py` archives all available store, API server, controller, scheduler, CoreDNS and kubelet logs. HTTP status counter deltas and raw logs retain errors, including expected conflicts and startup races. A green workflow is not an error-free trace. Confirm tigerstore-only failures twice and file them as `bug` sub-issues of #48.

## Initial observations

- Boot-only run `38419020116`, experiment `116fab403d088d715b7f661db8e1703438747ef0`, passed with stock etcd on both architectures. Both tigerstore jobs failed before kind because `etcdctl endpoint health` calls the unsupported `Maintenance.Alarm` RPC. Bug #92 records both confirmations and the stock controls.
- Boot-only run `38419585923`, experiment `cfd3b2e44ce7815ff76af272d0362d47dbbb2613`, booted kind successfully on both stores and architectures. This version records endpoint-health failure separately and uses linearizable reads for readiness on both targets. It does not fix #92.
- Both stores' boot logs contain transient namespace-not-found, initial RBAC lease failures and controller cache `read version ... not as new as written version` errors. Those messages alone do not establish a tigerstore defect. Full logs remain archived.
