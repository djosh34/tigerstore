# Local allocation census

Production source is `0d406cb09aa99dea7ef1488476d6ebc6b032a6b8`, the head of PR 77 at experiment start. The added test and these artifacts are in `824f719`. No production source changed for this capture.

This is a correctness-sized allocation test, not a local throughput benchmark. It starts three real in-process nodes with disk WALs and client/peer TLS through `newCluster` in `internal/server/server_test.go`. It discovers the leader, establishes a raw HTTP/2 TLS connection, warms 32 Puts, then counts 256 successful 1 KiB Puts. Each response must advance the revision by exactly one. All replicas' final values are checked outside the counter interval.

The raw client reuses request and frame buffers and indexed HPACK headers. It does not send `grpc-timeout`. The unchanged `exp/bench-75` benchmark client does send a timeout for every Put. Deadline infrastructure therefore appears in the runner results but not this canonical local fixture.

`runtime.MemStats` is process-wide. The reported 326.05859375 allocations and 19,757.90625 allocated bytes per Put include the client, leader, two followers and background work. They are not leader-only counts. The ARM experiment records each leader process separately.

`MemProfileRate=1` records allocation blocks. Two forced GCs precede each profile. Subtract `before.pb.gz` from `after.pb.gz`; ignore stacks containing `runtime/pprof` to exclude the earlier profile's serialization. Tiny allocations can share profile blocks, so profile object counts do not equal `MemStats.Mallocs`. The source audit explains the fixed and batch-dependent sites.

Commands used, sequentially with no other local test run:

```sh
GOFLAGS=-p=2 GOMEMLIMIT=2GiB go test -c -o /tmp/alloc50-base.test ./internal/server
GOFLAGS=-p=2 GOMEMLIMIT=2GiB ALLOC50_TEST_OUT="$PWD/evidence/alloc50/local-base" \
  /tmp/alloc50-base.test -test.parallel=2 -test.run '^TestAllocationPath$' -test.count=1 -test.v
GOFLAGS=-p=2 GOMEMLIMIT=2GiB go test -parallel 2 -short ./internal/server
GOFLAGS=-p=2 GOMEMLIMIT=2GiB go vet ./internal/server
GOFLAGS=-p=2 GOMEMLIMIT=2GiB golangci-lint run ./internal/server
```

The targeted capture, short server suite, vet and lint passed. The symbol binary is gzip-compressed in `server.test.gz`; `binary.sha256` is its uncompressed hash. These local results make no CPU, throughput, latency, GC-share or steady-state retained-memory claim.
