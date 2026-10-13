# Lean Put generator adaptation

This is a standalone replacement binary for `experiments/pre50next/load` in the current Put-only runner. It is experimental client code, not a server change or a complete implementation of #50.

The transport comes from `experiments/grpc/lean.go` on `exp/pre-47-grpc` at `d2408301966b99471bfed06d4c2dc1cd1e5af462`. That source file's SHA-256 is `9553545389d7ae3d246af1ecf03acc26b7a3f167a7f33883c046054c6ed1f07e`. The worktree base is `4abf26eb4a28e60b037d2d7aee6ea6badbb1ec3b`. The parent owns rebasing, binary manifests and script integration with main `549e0c9c9918a80a320eb886ab1b3480ae73fa69` and #80 if it merges. Nothing here selects or changes a server source.

## Runner interface

All existing flag names and defaults remain. `-op put` is supported; `-op range` fails explicitly rather than silently using grpc-go for measured operations. The current `run.py` invokes only Put. Workers are divided across connections with the same total concurrency, including uneven divisions. The supported bounds are 1 to 4096 workers, 1 to 256 connections, and no more connections than workers.

The parent builds this directory as its existing `load` binary. Example invocation on the disposable ARM runner, with the parent's normal environment and CPU placement:

```sh
GOMAXPROCS=1 GOFLAGS=-p=2 GOMEMLIMIT=2GiB taskset -c 3 "$BIN/load" \
  -endpoint "$LEADER_ENDPOINT" -certdir "$CERT_DIR" \
  -pids "$LEADER_PID,$FOLLOWER1_PID,$FOLLOWER2_PID" -hz 100 \
  -op put -total 60000 -workers 64 -conns 16 \
  -barrier-dir "$BARRIER_DIR" -out "$RESULT_JSON"
```

Use 256 workers for the other cell. Finite totals such as 30000 also work exactly. Profiles use `-total 0 -duration 12s`; the parent's separate 15-second capture includes its existing lead and tail margins. If both total and duration are positive, the first limit stops admissions. A finite total that does not complete is invalid.

The runner still issues separate `-total 4096` seed and `-total 8000` warmup invocations. This binary adds no hidden seed or warmup. Every invocation uses the same 4096 keys, `/registry/pods/ns-%03d/pod-%08d`, and the same 1024-byte values as the existing load client. Each value contains 32 SHA-256 blocks derived from `pre50-independent-%d-%d`. The global ticket sequence selects keys with `(ticket * 2654435761) % 4096`.

The `ready/start` and `drained/release` files and the existing JSON fields are unchanged. JSON adds `generator_cpu_us_op`, `all_servers_cpu_us_op`, `admitted`, duration/total, the original lean source SHA and transport differences. When `-pids` is supplied it must identify three distinct server processes in leader/follower order. `cpu_us_op`, `occupied_cores`, `cpu_before` and `cpu_after` then contain leader, follower 1, follower 2, generator. CPU includes user plus system ticks from `/proc/PID/stat`, checks process start identity and tick monotonicity, and uses the supplied `CLK_TCK`.

## Transport and measurement changes

The Put path keeps one reader and one owner per connection, immutable pre-encoded request envelopes, pre-encoded HPACK and coalesced writes without a fill timer. It has no per-Put goroutine, context, reply channel or timer. Slot state is reused. Framer events, response decoding and latency/revision samples still allocate; this is not a zero-allocation claim.

TLS must negotiate TLS 1.3 and h2 with the configured CA and client certificate. Connections finish SETTINGS negotiation and an initial PING before `ready`. One stock grpc-go Range probe runs before `ready` and another after `release`. Their calls and connection setup are outside the measured CPU window. Each probe transport closes after its call, although grpc-go may finish asynchronous cleanup afterward. All measured Put traffic is lean.

Compared with the current grpc-go client, Put latency starts at stream admission before HEADERS and stops after validated response trailers. It excludes per-request context construction, cancellation and grpc-go scheduling. The original lean generator also omitted `grpc-timeout`; this port keeps that behavior. It sends no per-Put deadline or require-leader metadata. One reusable connection-owner timer enforces a 10-second request age or admission stall, with a separate 10-second completion-fence bound. Socket writes and TLS dialing also have 10-second bounds. Any timeout fails the cell and aborts its socket; it does not retry an ambiguous write. Raw socket close avoids waiting for TLS close_notify on a nonreading peer.

Duration is one shared absolute admission cutoff, not a separate timer started by each owner. An already admitted request drains even if its DATA is still waiting for credit when the cutoff passes. The finite ticket limit cannot overshoot across owners. The success denominator covers the entire admitted cohort through drain. Throughput elapsed time includes that drain.

Each connection ends with a PING round trip inside elapsed time and the CPU window. The owner still checks response frames while waiting for the ACK, so already queued duplicates cannot escape the last-request check. This fence orders a conforming server's preceding writes before its ACK. It cannot prove that a server will never send a new invalid response after the fence. The fixed PING overhead differs from grpc-go and is reported in JSON. No Put starts after the campaign's `2026-10-13T22:00:00Z` cutoff.

The generator samples all three servers and itself before releasing owners, and again after all owners finish their fences, before `drained`. This is the current client's CPU window, not the parent's wider replica-applied counter window. The generator's `/proc` samples include the sampling work itself. The parent still fences replica application and controls CPU affinity. This binary does not repin processes or claim that follower tail work is entirely inside the CPU samples.

## Validation and bounds

Each stream must have HTTP status 200, a gRPC content type, exactly one uncompressed gRPC message containing a valid PutResponse header, and exactly one terminal `grpc-status: 0`. Missing messages, malformed protobuf/envelopes, extra message bytes, duplicate status/trailers, responses on closed or unknown streams, resets and GOAWAY fail the cell. Errors never become successes. The final check requires successes equal admitted requests and the finite total, unchanged probe identity/term, response terms equal both probes, an exact revision delta, and every revision in that interval exactly once. Revision and latency sorting happens outside the CPU window.

The owner honors connection and per-stream send credit, active-stream initial-window SETTINGS deltas including negative credit, maximum concurrent streams and maximum frame size. DATA is fragmented if stream credit is smaller than a request. Padding counts toward receive credit. Response streams have a 1 MiB advertised receive window, larger than the bounded unary response, and connection credit is replenished in coalesced 64 KiB groups. Request wire size must be below 2 KiB. Responses are bounded to 16 KiB, decoded headers to 64 KiB, and reader queues are bounded by connection slot count. At most four million latency/revision samples are admitted per invocation; exhaustion fails a duration run rather than reporting a shorter valid result.

HPACK uses static entries and never-indexed literals with a table-size update to zero. Its single prebuilt block remains valid after a peer changes HEADER_TABLE_SIZE, including zero. This differs from the original client's first-block plus dynamic-indexed subsequent blocks. A peer header-list limit below this request's decoded header size fails explicitly. Server push, compressed responses and unsupported response forms also fail. This is a measured-schema client, not a general gRPC/HTTP2 library.

## Tests and handoff

`lean_test.go` covers every response-envelope split, malformed/missing/duplicate responses and status, request/value identity, HPACK table-size changes, SETTINGS deltas, connection/stream credit, padding accounting, exact shared totals, expired duration limits, revision/term checks and `/proc` parsing. A tiny grpc-go KV fixture requires real mutual TLS 1.3, advertises two concurrent streams per connection, exercises 64/256 workers over 16 connections, runs more than the initial connection credit, and checks finite totals and duration drain. The fixture performs no disk or Raft work and proves no durability property.

Only formatting and source inspection were performed locally for this handoff. No Go build, test, vet, lint, benchmark or profile ran. The parent will compile, run bounded tests and review. Its bounded test command can be:

```sh
GOFLAGS=-p=2 GOMEMLIMIT=2GiB go test -count=1 -parallel 2 -timeout 60s \
  ./experiments/pre50next/leanload
```

No scripts, module files or server packages changed. No subagent, push, GitHub comment or merge was made.
