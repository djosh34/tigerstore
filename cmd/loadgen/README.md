# Load shapes experiment

This throwaway extension changes `cmd/loadgen` only. It uses the existing raw HTTP/2 gRPC client and response validators. It does not change server APIs, consensus, persistence or acknowledgement rules.

## Audit of the starting generator

The starting generator assigns connections round robin, not requests. A fast endpoint can receive more requests. Its closed-loop limit is connections multiplied by streams. Each connection draws its own Poisson schedule. Range is a linearizable point read, not LIST. Warmup and measurement share one phase, so warmup requests may complete in the measurement window. There are no external sampling barriers or queue-depth reports.

Existing checks already require one unary response, unique write revisions, successful CAS operations, exact point-read keys and values, and exact delete counts. Latency follows the arrival cohort through drain, rather than dropping late completions. CPU-sampling failures invalidate results. TLS uses the configured CA and client certificate.

The default dispatcher remains available without experiment flags. Two small fixes also apply to that path: reject nonfinite rates, and do not start closed-loop requests before the phase start. Point Range, CAS, Delete and Watch keep their previous CLI.

## Interface

Example flags for a balanced Put window:

```sh
loadgen -workload put -keys 4096 -value-size 1024 \
  -endpoints HOST1:PORT,HOST2:PORT,HOST3:PORT \
  -cacert ca.pem -cert client.pem -key client-key.pem \
  -conns 18 -concurrency 256 -balance round-robin \
  -warmup 1s -duration 3s -drain 30s \
  -barrier-prefix /path/to/unique-stage -pids PID1,PID2,PID3 -out result.json
```

Use `-concurrency 1024` or `4096` for the other closed-loop shapes. This is a global in-flight ceiling, not rounded per-connection quotas. The dispatcher fills it when server flow control and stream limits permit. Ramp-up, drain and scheduling gaps still exist. `shape.peak_inflight` reports the observed peak. `-streams` does not multiply this limit. If the experiment dispatcher is enabled without `-concurrency`, it defaults to connections multiplied by streams.

Use `-arrival fixed -rate R` for open-loop fixed-rate arrivals. Arrival zero is at the start; subsequent arrivals are at start plus the global request number divided by R. Only arrivals strictly before the stop belong to the cohort. Sending late does not move their scheduled timestamp. The scheduler uses constant memory for pending arrival timestamps. `-total N` instead makes a finite closed-loop measured cohort and replaces the duration window. It cannot be combined with an open-loop rate.

Use `-workload range` for point reads. Use `-workload list -keys 1000 -value-size 1024` for a current-revision linearizable prefix Range. LIST returns every value, without a limit, pagination, keys-only, count-only or serializable option. Every response must have exactly 1000 ordered distinct expected keys, Count 1000, More false, and all 1024 bytes of every seeded value. An extra key already present under `/registry/pods/` invalidates the read. Use fresh clusters. The default key count for LIST is 1000; other counts and value sizes are rejected.

Experiment dispatch supports Put, point Range and LIST. Poisson, CAS, Delete and Watch experiments use the default dispatcher. One endpoint gives leader-only ingress. Multiple endpoints always receive request-level round robin in the experiment dispatcher. `-balance round-robin` explicitly selects this dispatcher even without other shape flags. Eighteen connections give six per endpoint with three endpoints.

One writer assigns monotonically increasing request tickets to endpoint ticket modulo endpoint count. It rotates connections within that endpoint and batches frames per connection. If the next endpoint cannot accept a request, it waits rather than favoring another endpoint. This guarantees sent-request counts differ by at most one on a successful run. It does not impose a global socket-write order across independent TCP connections. There is no per-RPC goroutine, channel or global admission mutex.

## Stage barriers

Each run needs a fresh prefix. The generator rejects an existing `.ready`, `.go`, `.done` or `.finish`. Markers contain JSON; the generator publishes its markers with rename.

1. The generator seeds all keys for every experiment workload, probes every connection with its workload, then runs and fully drains warmup. It resets measured statistics and publishes `PREFIX.ready`.
2. The parent fences all replicas, snapshots external counters and creates `PREFIX.go`. The generator services only HTTP/2 controls while waiting. No requests remain in flight at ready.
3. The generator admits the measured cohort and drains it, including open-loop arrivals that were still queued at the arrival stop. It takes its through-drain CPU sample, then publishes `PREFIX.done` with expected and successful cohort counts and an error string.
4. The parent fences replicas and snapshots external counters, then creates `PREFIX.finish`. The generator sends no RPCs between done and finish. It writes the result after finish. Internal CPU samples exclude this wait and external fences.

`-barrier-timeout` defaults to five minutes per wait. `-drain` defaults to 30 seconds for experiment windows. Drain timeout closes connections, accounts outstanding failures and all unsent scheduled arrivals, and invalidates the result. Seed has a ten-minute deadline. A finite closed-loop cohort also has a ten-minute deadline. Barrier or request failure is never a valid measurement.

## Result additions

Existing result fields remain. `ops` and `throughput` count successful completions inside the arrival window. `cohort`, `cohort_failed`, `unsent`, `latency_us` and `service_latency_us` cover the entire measured cohort through drain. On a valid run, `latency_us.count == cohort`, with zero failed and unsent requests. Service latency starts at dispatcher admission before frame construction and batched socket writes, not after socket transmission.

`shape` adds:

- `expected`, `success`, and `expected_revision_delta`. Put expects one revision increment per measured request. Point Range and LIST expect zero. The expected delta excludes seed, connection probes and warmup. It is a workload expectation, not a generator-side replica fence or independent revision-delta check.
- `endpoint_counts`, with sent, successful, failed and unsent counts for each configured endpoint. Failed counts exclude unsent tickets.
- `queue_samples`, normally at 100 ms intervals plus stop and drain end. Each contains elapsed seconds, cumulative exact scheduled arrivals, cumulative admitted requests, queued arrivals and in-flight RPCs. Queued means scheduled but not admitted; it does not mean server queue depth. `queue_max_observed` is the largest dispatcher observation. `queue_mean_sampled` uses trapezoidal integration of reported samples, not an exact continuous-time mean.
- `queue_at_arrival_stop` and `inflight_at_arrival_stop`, derived from timestamped admission and completion counts. They do not confuse drained requests with the arrival-stop state.
- `arrival_window_s`, `through_drain_s`, `drain_s`, `cohort_throughput`, and `cpu_through_drain`. CPU through drain divides process user plus system deltas by successful cohort operations. The original CPU fields keep their completion-window denominator. Finite closed-loop cohorts use the full elapsed window for both views.
- Start, arrival-stop and done Unix nanoseconds for external alignment. Latencies use monotonic time.
- `response_buffers`, with exact aggregate live reassembly byte peak and retained buffer capacity at done and its peak. These count owned partial-message buffers, including reused free-stream buffers. They exclude Framer/TLS buffers, allocator overhead, request buffers, histograms, seeded values and server queues. Retained capacity includes buffers acquired before measurement. Peaks reset at go to that warmed baseline.

## Limits

LIST has no aggregate reply-memory admission bound. A 1000-key response is about 1.1 MiB; Go slice capacity can be larger. At 4096 concurrent LISTs the generator can retain more than 4 GiB, before server reply queues. Start with concurrency 1, 16, 64 or 256 on free 16 GiB ARM runners. Higher LIST concurrency is not established as safe by this extension. `-max-response-bytes 2097152` rejects a message larger than 2 MiB, before unbounded reassembly. This is a per-message limit, not an aggregate memory budget; zero leaves it unlimited.

The single writer can limit offered load, especially with strict round robin and a stalled endpoint. Report generator CPU, scheduled latency, service latency and queue growth. A requested ceiling is not proof that the generator maintained that many simultaneous RPCs continuously. Wire requests still use prebuilt static HPACK headers. Request payloads must fit one 16 KiB DATA frame. No automatic retries hide failures.

CPU sampling retains the original `/proc` implementation, including its lack of process-start identity checks. The parent must verify PID/process identity and replica fences. Constant seeded read values validate bytes and cardinality, but do not establish arbitrary history linearizability or crash durability. Internal CPU through drain does not include replica catch-up after done; the parent's fenced sampler provides that wider view.

Only lightweight fake-server and validator tests ran locally with `GOFLAGS=-p=2 GOMEMLIMIT=2GiB`, `go test -short -parallel 2 ./cmd/loadgen`. No local performance measurement or runner launch belongs to this extension.
