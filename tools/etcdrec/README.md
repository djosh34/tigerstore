# Research ticket 579: on-runner etcd recording

Research branch only. No tigerstore backend or etcd client-side load generator
is used. `boot.sh` adapts `k8stest/boot.sh` / `k8stest/kind.yaml`: kind 0.33.0,
Kubernetes 1.37.0 at the existing pinned image digest, one real worker,
a single **stock etcd 3.7.2 member**, mutually authenticated TLS on both proxy
hops. The apiserver's sole etcd endpoint is the recorder. Health checks bypass
the recorder. The single upstream gRPC connection is an instrumentation choice;
reported connection counts are the original downstream apiserver connections,
not the recorder's backend connection.

`main.go` forwards protobuf bytes unchanged through `UnknownServiceHandler`.
Request/response metadata, status, deadlines and half-close are forwarded. It
decodes a copy through etcd's protobuf descriptors, converts keys/range ends to
strings, and replaces every other bytes field (including nested values and
comparison values) with its byte length. Finite `KV/RangeStream` is recorded as
well as `KV/Range` (etcd 3.7 supports the former). JSONL contains connection open/close,
RPC start/end, and every request/response message. `bytes` is the uncompressed
protobuf payload size (not HTTP2, TLS, TCP or gRPC framing bytes). Each nested
message's `_bytes` is its protobuf serialized size. Range replies retain only
header, count, KV count, more and total size, including each RangeStream chunk;
watch events retain key, sizes,
revisions and event kind. No object values are persisted by the recorder.

KWOK is pinned to v0.8.0, with release `kwok.yaml` and `stage-fast.yaml`.
The `pod-complete` stage is deleted: upstream load Jobs run pause indefinitely,
and CL2 waits for their parallelism count to be Running, not completed. Other
stages are unchanged, and effective stages are saved in artifacts. Nodes are annotated for KWOK; their allocatable/capacity is
32 CPUs, 128Gi and 110 pods. The real worker is tainted against test workloads.
Kind's infrastructure DaemonSets select real nodes only, preventing synthetic
kube-proxy/kindnet pods from being counted as idle workloads. Three minutes of
settling precede the first measurement; idle is ten minutes.

The kwok-friendly clusterloader2 variant is based on perf-tests commit
`01a4367bce12691ea1e910394290b72a90b4fa65`, `testing/load/config.yaml`.
The upstream create / scale-and-update / delete modules and object templates
are retained; 30 pods per fake node, nominal ten pod/s creation/deletion pacing,
and ten API QPS default. PVs are disabled because these are simulated nodes.
The global measurements module (Prometheus, network/SLO, etc.), DNS checks,
separate scheduler-throughput microbenchmark and separate pod-startup-latency
microbenchmark are replaced with one-second sleeps. The lifecycle's own
`WaitForControlledPodsRunning` measurements remain. This is **not** a full
upstream scalability/SLO test and proves no real kubelet or networking capacity.
Exact altered files and parameter overrides are in workflow artifacts.

## Reproduce

```sh
gh workflow run research-579-recording.yml --ref research/579-recording \
  -f nodes=100 -f idle_seconds=600 -f run_load=true
gh workflow run research-579-recording.yml --ref research/579-recording \
  -f nodes=500 -f idle_seconds=600 -f run_load=false
```

Workflow jobs use `ubuntu-24.04-arm`, `GOFLAGS=-p=2`, one heavy build at a time.
They upload sanitized full JSONL, window timestamps, per-window JSON/Markdown
summaries, the apiserver manifest and feature metrics, logs and CL2 variants.
All apiserver flags are preserved in `apiserver.yaml`; no watch-cache or
feature-gate overrides are added by this recorder. Default behavior is that of
the pinned kind image. `kubernetes_feature_enabled` metrics, where exported,
provide evidence of feature settings, not just an assumption about flags.

## Counting definitions and limits

- The shape-share denominator is **request messages**, including watch
  create/cancel/progress and keepalive messages on persistent streams. New
  transport RPC starts are reported separately; streams begun before the
  window do not count as new RPCs inside it.
- In-flight concurrency is the exact time-weighted start/end state distribution;
  persistent Watch/KeepAlive RPCs are included in `all_rpc`, excluded in the
  legacy-named `unary` statistic. That statistic includes finite RangeStream
  requests; it means nonpersistent requests, not literally unary-only RPCs.
  The JSON includes both as well as connection concurrency.
- Resource-prefix call counts count each distinct primary key prefix once per
  request; a transaction's compare key (or success-op key if no compare) is
  primary. Write rates count successful put/delete **operations**, not attempted
  failed-CAS writes. DeleteRange is not expanded into its affected key count.
- Value-size distributions and key popularity use successful **puts only**,
  including puts in successful transaction branches. Popularity ranks keys by
  put count, uses `ceil(1% or 10% of distinct keys)`, and reports their share of
  puts. Repeated-key intervals use consecutive successful puts within the
  window; they are left/right censored at the boundaries. No object-value
  contents or logical-object decompression are involved.
- Read sizes are whole Range response protobuf bytes, or the sum of all
  RangeStream chunk bytes in a completed finite RPC; nested transaction range
  replies are also included, attributed to their requested resource key.
- Prefix ranges at limit one are labeled `probe` in summaries as a call shape;
  attribution to a particular HTTP LIST is not proven by the recording alone.
  Other prefix Ranges (excluding count-only) are LIST-shaped storage reads,
  **not** a count of all HTTP LISTs. Caller attribution beyond the resource
  prefix needs apiserver/source evidence. Cache-served LISTs are invisible here.
- Watch events/s are decoded KV events by resource prefix. Event bytes exclude
  the surrounding WatchResponse/header; raw records also have total reply bytes.
  Active watches/streams are reconstructed from startup, not guessed from the
  number of creates in a steady window. Empty non-created/non-canceled watch
  replies are counted as progress notifications.
- Proxy instrumentation adds unquantified overhead. The recording measures this
  configuration, not production request-rate or latency capacity. No experiment
  here proves an uninstrumented throughput number or a multi-member raft mix.

Local checks: `GOFLAGS=-p=2 go test ./tools/etcdrec` (unary/stream forwarding,
headers/trailers and nested redaction), and
`python3 -B -m unittest discover -s tools/etcdrec -p 'test_*.py'` (windowing,
CAS outcome accounting, exact concurrency and persistent watch reconstruction).
