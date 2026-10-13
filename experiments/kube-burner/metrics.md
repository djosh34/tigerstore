# Kubernetes comparison metrics

Run from this directory with the parent's configured `kubectl`, Docker access, and readable host `/proc` files. The script uses Python's standard library. It does not start containers or change the cluster.

```sh
STORE=etcd python3 metrics.py snapshot evidence/etcd/before
# Run one workload phase.
STORE=etcd python3 metrics.py snapshot evidence/etcd/after
python3 metrics.py diff evidence/etcd/before evidence/etcd/after evidence/etcd/phase.json
```

Use `STORE=tigerstore` for Tigerstore. `snapshot --store tigerstore OUTDIR` also works. Capture separate before/after pairs for each phase and for the whole run. `diff` takes snapshot directories, not JSON filenames. Its output directory must already exist. The script refuses to overwrite an existing snapshot.

## Fixed topology and evidence

- `kubectl --request-timeout=20s get --raw=/metrics` captures the configured apiserver.
- Docker containers are `burner-control-plane` and `external-etcd-1`, `external-etcd-2`, `external-etcd-3`. There is no container or network discovery.
- Endpoint status uses this exact command:

```sh
docker exec external-etcd-1 /bin-etcd/etcdctl \
  --endpoints=https://external-etcd-1:2379,https://external-etcd-2:2379,https://external-etcd-3:2379 \
  --cacert=/certs/ca.crt --cert=/certs/client.crt --key=/certs/client.key \
  --command-timeout=10s endpoint status -w json
```

- Each server metrics scrape runs `curl` inside `burner-control-plane` against `https://external-etcd-N:2379/metrics`. It uses `ca.crt`, `client.crt`, and `client.key` from `/etc/kubernetes/pki/external`. Each curl has a 15-second timeout. HTTP 404 means unsupported telemetry, not a workload failure.
- `snapshot.json` records commands, return codes, stderr, issues, parsed samples, host boot ID, UTC timestamps, and monotonic start/end timestamps. Raw responses are `apiserver.prom`, `external-etcd-N.prom`, and `etcd-status.json`. The script records certificate paths, never certificate or key contents.

Snapshots are sequential rather than atomic. Rate denominators use the elapsed time between the midpoints of the relevant collection operations. CPU samples have their own timestamps. The before and after snapshots must share a known host boot ID. The recorded start/end timestamps expose the collection uncertainty. Capture the workload outside both snapshot commands to avoid measuring overlapping collection work. Background Kubernetes traffic and metric scrapes remain part of the observed interval.

## Summary fields

`apiserver.request_duration` reports p50 and p99 seconds for `apiserver_request_duration_seconds`, overall and by verb. It excludes WATCH and CONNECT. `by_resource` and `by_resource_verb` also report API latencies by Kubernetes resource and resource plus verb. These are API request latencies for resource types, not timings for individual object names and not kube-burner object lifecycle latencies. No proxy measures them. Quantiles use cumulative bucket deltas, linear interpolation within the bucket, and the last finite boundary when the requested quantile falls in `+Inf`. These are histogram estimates, not exact request percentiles. Labels other than verb aggregate into the overall or verb histogram only after each original histogram passes validation. Empty histograms have null quantiles.

`apiserver.requests` reports `apiserver_request_total` deltas and requests per second with the same exclusions. `status_details` retains each status code and the original labels, including resource and verb. HTTP 4xx and 5xx are errors. If no error counter series exists at either endpoint, the error total is null with `no_error_series`, not an assumed zero. `requests_including_watch_connect` separately reports all verbs.

`storage_rpc.histograms` reports the apiserver's `etcd_request_duration_seconds` histogram count deltas and rates. It also checks the `apiserver_etcd_request_duration_seconds` spelling and explicitly reports it missing if absent. These are client-observed storage RPCs, not server counters. Retries, background work, and multiple RPCs per Kubernetes request can affect them. Do not add the two metric families if both exist. They may describe the same work.

`etcd_revision.endpoints` reports revision deltas per replica and preserves cluster/member IDs. A revision delta counts MVCC revisions, not RPCs or writes. A transaction may contain several writes under one revision. Replicas can lag. Never sum their revision deltas.

`server_metrics` reports per-server counter deltas when stock etcd exposes them. `grpc_server_handled_total` retains method and gRPC status labels. Proposal, MVCC operation, and network-byte metrics remain separate. Some versions do not expose every listed metric. Proposal counters describe replicated work, so summing them across replicas would double-count it. Tigerstore's expected HTTP 404 produces unsupported availability and null counters. The apiserver RPC and endpoint revision measurements remain independent of server metrics support.

`cpu.containers` reports CPU for each exact container main PID from `/proc/PID/stat` fields 14 and 15, `utime` and `stime`. It divides tick deltas by `SC_CLK_TCK`. `average_cores` is CPU seconds divided by elapsed seconds; `percent_one_core` multiplies that by 100. The identity includes container ID, host PID, process start-time ticks, and Docker container start time. The script verifies Docker identity again after sampling. Changed identities, read failures, and regressed counters invalidate CPU results. It retains the raw stat line and child CPU fields but does not add child CPU. The control-plane main PID is not the apiserver process, and these numbers are not whole-container or cgroup CPU totals.

## Missing data and exits

Failed commands, invalid samples, mismatched histogram boundaries, and counter regressions produce explicit issues and null results. Missing entire metrics after or at both endpoints remain invalid. A disappearing label series remains invalid, even if the process start time matches. Optional stock metrics absent at both endpoints have `exposure: not_exposed`; they do not invalidate other metric families or imply a failed scrape.

Prometheus counters can initialize label series lazily. The script infers a zero before value for a newly appearing series or entire metric only when both scrapes succeeded without parsing errors and both expose the same `process_start_time_seconds`. Diff additionally requires the same known host boot ID for this inference. Every such row has `inferred_zero: true`, `observed_before: null`, and an effective `before: 0`. The same rule applies to histogram buckets and counts. Unknown process identity prevents inference. Changed process identity invalidates the source's counter deltas, even if the numeric counters increased. Equal process start times establish exporter process continuity, not continuity across deliberate in-process metric deletion or resets.

`comparable_series_delta` is a partial sum of computable series, including explicitly inferred baselines. It is not the total when `complete` is false. A valid observed counter delta of zero stays zero. Missing counters never silently become zero.

A snapshot or diff exits zero once it writes its evidence, even with unavailable telemetry. Inspect `issues`, availability, completeness, and null fields before using a result. Invalid arguments, invalid snapshot input, or filesystem failures exit 2. Workload success must come from the workload runner, not from these telemetry commands.

Run `python3 metrics.py selftest` for synthetic parser, inference, restart, resource grouping, histogram, missing-data, and CPU identity tests. They make no external commands or file writes.
