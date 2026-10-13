# Kubernetes value size histogram

Throwaway branch `exp/size-histogram` for #50 and a follow-up to PR #95. Nothing is intended to merge. The original compression experiment remains unchanged under `experiments/compress50`.

This repeats that experiment's kind v0.33.0, digest-pinned Kubernetes v1.37.0 node, stock etcd, 250-Pod limit and exact `workload.py`. The workload creates 120 applications across 12 namespaces, applies four generations and deletes and recreates one application per namespace. It waits for Deployment availability. Controllers still run during the snapshot, so counts and lengths may differ from the earlier inventory. This measures current live values, not historical MVCC versions, write arrival frequencies or a production workload.

## Data handling

The static Go exporter runs inside the kind node. A single `crictl exec` runs stock `etcdctl get '' --prefix --write-out=json` inside its etcd container and pipes the response to the exporter inside the node. Raw values exist only in that pipe and process memory. No dump file is created. Only CSV rows with an allowlisted resource label and decoded value byte length leave the node. The exporter never prints keys, values, JSON parse fragments or value hashes. It rejects incomplete responses, duplicate keys and malformed JSON before writing output. Core dumps and shell tracing are disabled.

Resource names come from key prefixes using the compression experiment's classifier. For example, `/registry/services/specs/` maps to `services`, `/registry/services/endpoints/` maps to `endpoints`, and recognized API-group prefixes use their resource segment. Unknown prefixes map to `other`. No value parsing supplies a Kind, namespace or object name.

A private runner file holds the length-only rows until Python reduces them. The script deletes that file and kind on exit. The artifact allowlist contains only `histogram.csv`, environment and version metadata, and workload counters. Kubeconfig stays in the private temporary directory. No raw values, certificates, container logs or trained dictionaries are uploaded.

## CSV and empirical sampling

`histogram.csv` has one `summary` row per resource and one `bucket` row per distinct integer byte length in that resource. Exact size buckets are compact frequency tables. They preserve the observed distribution without interpolation or a fitted beta draw.

Summary rows have object count, total value bytes, object share of the snapshot, value-byte share, minimum, p10, p50, p90, p99 and maximum. Quantiles use nearest rank, `ceil(percent * count / 100)`, on sorted lengths. Shares are fractions printed to 12 decimal places. Counts and byte totals are exact; use counts rather than rounded shares for sampling.

Bucket rows have `size_bytes`, count, total value bytes in that bucket and its conditional share within the resource. Empty summary-only columns stay empty. To replace PR #95's beta draw, select a resource with probability `resource_count / total_count`, then select an exact length with probability `bucket_count / resource_count`. Equivalently, draw one integer across all object counts. Generate fresh synthetic value bytes. Do not treat value-byte share as object probability. This branch does not modify PR #95.

## Run and validate

Only the free `ubuntu-24.04-arm` workflow runs the cluster. It refuses starts at or after 2026-10-13T22:00:00Z and has a 15-minute job limit. The script checks the deadline before the build, downloads, cluster, workload and dump. Local validation builds only the standard-library exporter and runs small tests, never Kubernetes or a benchmark.

```sh
GO111MODULE=off go test ./experiments/size-histogram/export
GO111MODULE=off go vet ./experiments/size-histogram/export
python3 -m unittest discover -s experiments/size-histogram -p 'test_*.py'
shellcheck experiments/size-histogram/run.sh
actionlint .github/workflows/exp-compress50.yml
```

The runner uses Go 1.27.2 and records actual Kubernetes, kubectl, etcd, runner image, CPU and memory versions. The workflow tests the reducer before collecting any data. The committed evidence directory identifies the successful Actions run and source revision.
