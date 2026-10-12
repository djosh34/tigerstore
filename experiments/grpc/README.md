# Client transport experiment

Throwaway pre-work for #47. Do not merge. Base `main` was `91c4f974a6661ac02c2e7747781ea215353c643b`. This compares client transports, not databases. There is no Raft, disk, MVCC history, lease expiry or durable acknowledgement.

## What ran

Host `lima-opencode-vm`, Linux `5.14.0-611.26.1.el9_7.aarch64`, eight Apple-reported ARM64 CPUs, 11 GiB RAM. Shared host with other agents and occasional builds. CPU affinity is not exclusive CPU reservation. No host settings changed. No GitHub runner results are claimed.

- Go 1.27.2 linux/arm64, `GOTOOLCHAIN=local`.
- grpc-go v1.84.0, the latest stable release reported by GitHub on 2026-10-12. Release date 2026-09-17.
- etcd API and clientv3 v3.7.2.
- protobuf-go v1.36.11; x/net v0.61.0.
- Stock etcdctl v3.7.2 from the upstream Linux ARM64 release. Also tested the installed v3.5.17.
- TLS 1.3 only, ALPN h2, verified server certificate and required verified client certificate. Go crypto/tls in every server and both generators. A short-lived self-signed localhost test certificate is generated locally, never committed.
- Server GOGC=100, GOMEMLIMIT=1GiB. Client uses the same settings, GOMAXPROCS=4 and affinity 4-7.
- Main layout: server CPU 2, GOMAXPROCS=1. Second layout: server CPUs 1-2, GOMAXPROCS=2.
- Two established connections, 128 outstanding unary streams per connection. Closed-loop arrivals, 1 KiB value, 60-byte Kubernetes-shaped key. Each slot overwrites its own key. All servers decode the real Put protobuf, update the same mutex-protected in-memory map, increment revision, and encode a real PutResponse. No watch is active during Put benchmarks.
- One-second warmup, five-second timed window plus drain of outstanding requests. Fresh server per case. Five repetitions, case order shuffled with seed 47. Each table cell is the median of the five per-run measurements, not a pooled percentile.
- CPU is the server process's user plus system time from syscall.Getrusage, read through a separate local control endpoint before and after the measured phase, divided by completed RPCs. Runtime, GC, TLS and kernel CPU belong to this count. The control calls add a negligible fixed cost. Client CPU uses its own Getrusage counter. Counts include every completed request, with no RPC errors accepted.
- Latency runs from client request submission to successful gRPC status trailers. Histogram resolution is 1 microsecond below 10 ms and 100 microseconds above it. Mean and maximum use nanoseconds. Closed-loop p99 does not describe an open-loop overload workload.

`grpc-default` uses only transport credentials. In v1.84.0 shared write buffers already default to true. `grpc-shared` explicitly sets SharedWriteBuffer(true), so it is the same configuration, not an independent optimization. `grpc-workers` adds NumStreamWorkers(64). `grpc-tuned` adds shared buffers, 64 stream workers, 1024 max concurrent streams, 1 MiB stream window, 16 MiB connection window and 64 KiB read/write buffers. The worker-count pilot tries 1, 8, 64 and 256 workers and SharedWriteBuffer(false), with two repetitions. It is too noisy to rank worker counts.

`handler` is a gRPC adapter on x/net/http2.Server.ServeConn and net/http.Handler. It uses the same 1024 stream, 1 MiB stream-window and 16 MiB connection-window limits. It is a straightforward reference, not a tuned optimum for ServeConn.

`raw` uses x/net/http2.Framer and hpack with a reader and writer per connection. Stream state is a descriptor rather than a unary goroutine. It coalesces ready responses into a 64 KiB buffered TLS writer without a batching timer. The same supported protobuf dispatch and backend run for both custom servers. It advertises the same flow-control limits as `handler` and `grpc-tuned`. It does not replace crypto/tls, HPACK or protobuf.

## Primary results with the lean generator

The Put-only lean generator pre-encodes each slot's request, reuses warm HPACK blocks and coalesces writes. It verifies every response protobuf and status trailer. It has a reader and an owner goroutine per connection, no goroutine per RPC.

| Server | Server CPU us/Put, 1 core | Put/s, 1 core | p50/p99 us, 1 core | Server CPU us/Put, 2 cores | Put/s, 2 cores | p50/p99 us, 2 cores |
|---|---:|---:|---:|---:|---:|---:|
| grpc-go default | 9.78 | 101,374 | 2,328 / 5,040 | 11.83 | 155,599 | 1,443 / 3,943 |
| grpc-go explicit shared | 10.11 | 97,473 | 2,378 / 5,311 | 12.08 | 154,979 | 1,448 / 3,914 |
| grpc-go workers 64 | 9.52 | 103,166 | 2,333 / 5,144 | 11.65 | 160,820 | 1,480 / 3,774 |
| grpc-go tuned | 9.39 | 105,464 | 2,249 / 4,876 | 11.74 | 156,662 | 1,457 / 3,861 |
| HTTP/2 Handler adapter | 39.87 | 25,032 | 9,805 / 20,100 | 37.87 | 50,894 | 5,028 / 9,391 |
| Custom Framer path | 3.53 | 267,580 | 729 / 2,535 | 4.77 | 328,853 | 595 / 2,075 |

On one server core, custom CPU ranged 3.36 to 3.63 us/Put and tuned grpc-go ranged 8.44 to 9.41. Throughput ranged 251,931 to 293,867/s for custom and 104,531 to 118,414/s for tuned. The medians imply 62% lower server CPU and 2.54x throughput. On two cores, custom CPU ranged 4.33 to 5.01 and grpc-workers ranged 11.18 to 11.84. Throughput is noisier than CPU on this shared host.

Lean client CPU medians were 2.79 us/Put against custom and 3.51 against tuned grpc-go on the one-core server layout. The server consumed about 0.94 to 0.99 CPU-seconds per wall second in those cases. This is not a grpc-go client throughput ceiling.

## Stock grpc-go generator as a second view

Same backend, concurrency, affinity, warmup and five repetitions. The generator calls the generated etcd KV client directly, not the higher-level retrying clientv3 wrapper.

| Server | CPU us/Put, 1 core | Put/s, 1 core | p50/p99 us, 1 core | CPU us/Put, 2 cores | Put/s, 2 cores |
|---|---:|---:|---:|---:|---:|
| grpc-go default | 12.03 | 82,010 | 2,895 / 6,745 | 14.71 | 114,532 |
| grpc-go workers 64 | 10.80 | 89,360 | 2,827 / 6,285 | 13.59 | 123,607 |
| grpc-go tuned | 10.84 | 90,322 | 2,753 / 6,265 | 14.51 | 111,448 |
| Custom Framer path | 5.02 | 164,847 | 1,343 / 4,385 | 6.50 | 174,959 |

The server's CPU/op changes with the generator because request arrival and coalescing change. Do not compare a custom lean-generator row with a grpc-go-server stock-generator row. Client CPU medians here were 13.71 us/Put against custom and 15.32 against tuned on one server core.

A separate one-connection, one-outstanding-RPC run used the stock grpc-go generator and three repetitions. Default grpc-go was 8,957/s, mean 111 us, p50/p99 108/201 us, 48.19 server CPU us/Put. Workers 64 was 9,265/s, mean 108 us, p50/p99 104/198 us, 44.12 CPU us/Put. Custom was 9,719/s, mean 103 us, p50/p99 99/182 us, 36.27 CPU us/Put. The saturated CPU improvement is not a comparable single-client latency improvement.

## Interoperability and normal framing tests

`smoke.log` proves stock clientv3 v3.7.2, etcdctl v3.7.2 and etcdctl v3.5.17 each passed Put, Range and a live update Watch with prev_kv against default grpc-go, tuned grpc-go, Handler and custom Framer servers. Clientv3 waits for the watch-created response before updating the key. The etcdctl smoke checks the actual JSON event and previous value, then terminates the watcher. These are verified mTLS connections, not plaintext tests.

Tests passed three ordinary runs and one race run. They cover 256 concurrent streams over two connections with 5,120 successful Puts and the exact final revision, fragmented 1 MiB Put and Range across 64 KiB client flow-control windows, 256 KiB watch values with previous values, cancellation while unary calls continue on the same connection, Unimplemented status trailers and a warmed lean generator. They do not certify HTTP/2 conformance.

## Code size and remaining work

`raw.go` is 352 Go lines including comments and blank lines. It contains connection framing, stream descriptors, incoming message reassembly, response coalescing, HPACK, basic flow control, SETTINGS, PING, RST_STREAM and Watch dispatch. Shared `backend.go` is 174 lines, and `main.go` is 293 lines including certificate generation, TLS setup, grpc-go reference server, control endpoint and Handler adapter. The total server-side experiment is 819 lines. That is an upper count for this prototype, not an estimate for a production implementation. Generated protobuf and dependency code are excluded.

Generators are 208 plus 355 lines. Tests are 212 lines. All Go source totals 1,594 lines. The Python runner is separate.

The backend only implements Put, current Range and one Watch ID per stream. Other methods return Unimplemented. Prefix Range order, historical reads, Txn, DeleteRange, Compact, leases and production watch semantics are not implemented. No full etcd server benchmark ran, so none of these rates is a durable etcd replacement rate.

The custom transport also lacks grpc-timeout, require-leader metadata, complete HTTP/2 state validation, negotiated HPACK table resizing, graceful GOAWAY/drain, bounded queued bytes, slow-consumer isolation, robust late-completion handling after RST_STREAM, compressed messages and multiple Watch IDs with progress/replay/compaction errors. It accepts the measured clients' normal wire shapes. `/health` and `/version` and an HTTP/1.1 TLS path are absent. These omissions matter to #47 and kubeadm.

Recommendation: keep crypto/tls, x/net Framer/HPACK and stock protobuf. There is evidence to try the per-connection custom server beside grpc-go, with both using the same clean core adapter and compatibility suites. Merely replacing grpc-go with a generic HTTP Handler did not help in this prototype. Keep grpc-go as the fallback until all required API semantics and ordinary protocol lifecycle tests pass.

Proven: the lean custom path beats both current grpc-go default and the tested tuned options for this steady-state unary workload on this host, and the requested stock-client smoke works. Not proven: full Kubernetes compatibility, production-safe HTTP/2, watch throughput, 3-node performance or a durable-store speedup. The same gain on free runners is a guess. Full transport code size is unknown; counting this happy-path prototype as the finished scope would be wrong.

## Reproduce

From the repository root, choose an absolute directory you own on the work mount. All caches, temporary files, certificates and binaries stay there.

```sh
export ROOT=/absolute/owned/experiment-output
mkdir -p "$ROOT"/cache/build "$ROOT"/cache/gopath "$ROOT"/tmp
export GOCACHE="$ROOT/cache/build" GOPATH="$ROOT/cache/gopath"
export GOTMPDIR="$ROOT/tmp" TMPDIR="$ROOT/tmp" GOTOOLCHAIN=local
cd experiments/grpc
go version
go list -m all
go vet ./...
go test -count=3 -v ./...
go test -race -count=1 -v ./...
go build -o "$ROOT/tmp/grpcbench" .
python3 run.py --root "$ROOT/tmp/lean-1core" --binary "$ROOT/tmp/grpcbench" --generator lean --suite main --repetitions 5
python3 run.py --root "$ROOT/tmp/lean-2core" --binary "$ROOT/tmp/grpcbench" --generator lean --suite main --repetitions 5 --server-cpus 1-2 --server-procs 2
python3 run.py --root "$ROOT/tmp/grpc-1core" --binary "$ROOT/tmp/grpcbench" --generator grpc --suite main --repetitions 5
python3 run.py --root "$ROOT/tmp/grpc-2core" --binary "$ROOT/tmp/grpcbench" --generator grpc --suite main --repetitions 5 --server-cpus 1-2 --server-procs 2
python3 run.py --root "$ROOT/tmp/latency" --binary "$ROOT/tmp/grpcbench" --generator grpc --suite latency --repetitions 3
```

The runner refuses to start a case after 2026-10-13T22:00:00Z. To reproduce later, explicitly replace that experiment deadline. It does not change the host's CPU governor, networking or system limits.

For the smoke, download `https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-arm64.tar.gz`, extract beneath `$ROOT/tmp`, then run:

```sh
python3 run.py --root "$ROOT/tmp/smoke" --binary "$ROOT/tmp/grpcbench" --suite smoke
```

The runner records the exact taskset commands in every JSON row. Expanded primary commands are:

```sh
GOMAXPROCS=1 GOGC=100 GOMEMLIMIT=1GiB taskset -c 2 "$ROOT/tmp/grpcbench" -cert-dir "$ROOT/tmp/lean-1core/certs" -mode server -kind raw -workers 64
GOMAXPROCS=4 GOGC=100 GOMEMLIMIT=1GiB taskset -c 4-7 "$ROOT/tmp/grpcbench" -cert-dir "$ROOT/tmp/lean-1core/certs" -mode load -generator lean -kind raw -connections 2 -streams 128 -warmup 1s -duration 5s
```

Stop the server between cases. Delete generated certificates, the downloaded etcd archive and binaries, and the built benchmark when finished. Source and raw logs in `results/` are the persistent evidence.

## Source references

- [grpc-go v1.84.0 release](https://github.com/grpc/grpc-go/releases/tag/v1.84.0).
- [grpc-go server defaults and SharedWriteBuffer](https://github.com/grpc/grpc-go/blob/v1.84.0/server.go#L188-L256).
- [NumStreamWorkers and worker reuse](https://github.com/grpc/grpc-go/blob/v1.84.0/server.go#L626-L709). This option does not remove the grpc-go transport and stream machinery.
- [gRPC over HTTP/2 wire protocol](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md).
- [x/net HTTP/2 v0.61.0](https://github.com/golang/net/tree/v0.61.0/http2).
- [etcd v3.7.2 RPC definitions](https://github.com/etcd-io/etcd/blob/v3.7.2/api/etcdserverpb/rpc.proto).
