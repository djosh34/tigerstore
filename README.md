# Loopback transport experiment for tigerstore #53

Question: which encrypted transport is cheapest for a leader's two append/ack links?

Throwaway code, never merge. This is a standalone program, not tigerstore code. The checkout read for context was `a2f5b88922b90fcdde1ab289956abe6e53e55e2f`.

## Run

```sh
go build -o transportbench .
./transportbench -reps 5 -duration 2s -procs 4 > results.jsonl 2> run.log
```

The program refuses to start a case at or after 2026-10-13T22:00:00Z. Go 1.27.2, quic-go v0.63.0. `go vet ./...` passed. All 18 cases also passed with `go build -race -o racebench .; ./racebench -smoke`. Race results are not performance measurements.

Measured 2026-10-12 at 21:38 UTC on `lima-opencode-vm`, Apple aarch64 VM, 8 CPUs, Linux 5.14.0-611.26.1.el9_7.aarch64. One benchmark process, GOMAXPROCS=4, no CPU pinning. The host also ran agents and background services. No system or network settings changed. quic-go tried to enlarge its own socket buffer and logged that it got 416 KiB instead of 7168 KiB. Results retain that limit; no sysctl workaround.

## Method

- Two concurrent independent loopback links model a leader sending to two followers. Each endpoint has one application goroutine per link. QUIC's internal goroutines remain enabled and their CPU is counted. There are no per-message goroutines.
- Each link has one outstanding append batch and waits for its ack before sending another. Acks are 32 bytes. This measures per-link append RTT, not quorum latency. There is no synchronization between the two streams' batches, no Raft, disk, fsync, client API, protobuf, serialization or etcd baseline.
- Entries are prebuilt random payloads, 1024 or 10240 bytes, concatenated into batches of 1, 16 or 256. No compression. Stream framing adds 16 bytes. All payload bytes pass through encryption and reception; UDP copies fragments into a reassembly buffer.
- Established connections only, certificate setup and handshakes excluded. TLS 1.3 over Go TCP defaults and QUIC both verify a generated certificate in both directions. QUIC uses one persistent bidirectional reliable stream per link and default congestion control, flow control, MTU discovery and offload settings.
- UDP uses AES-128-GCM with fresh independent directional keys per link, provisioned in memory before timing. Each packet has a monotonic 64-bit nonce and an authenticated 24-byte header. The 1200-byte UDP payload holds 1160 bytes of append data. Each burst of at most 32 packets waits for an encrypted cumulative ack; the final burst's ack completes the message. The receiver reassembles the full batch before that final ack. Thus large UDP batches need extra acks, unlike the application framing on the stream transports. TCP and QUIC have their own transport acks.
- UDP is only an in-order, loss-free prototype. No retransmission, congestion control, key exchange, rekeying or general replay window. A gap, unexpected ack or timeout fails the run. All measured cases completed. Do not treat it as a deployable transport or claim the result excludes better UDP designs.
- Each case warms both links for 250 ms, then measures for 2 seconds. Five repetitions, deterministic shuffled case order per repetition. Every append contributes an RTT sample. Throughput uses wall time through both links' completion. CPU is process user plus system time from getrusage, divided by completed append messages. It includes both senders, both receivers, networking work charged to the process, allocation, GC and sample collection, not all host softirq work.
- One message means one complete append batch delivered to one follower and acknowledged. Counts combine both links, not unique replicated batches or entries. Payload bytes/s equals messages/s times entry size times batch size, excludes headers, acks and duplicate-replica deduplication. Divide by two for the equivalent two-copy batch rate. CPU is not per entry, per node or per committed write.
- The table uses the median of five run-level values for each metric. p50 and p99 are per-run RTT percentiles, then medians across runs, not percentiles pooled across repetitions. Raw data includes each run's message count, mean latency and elapsed time.

## Verdict

Prefer framed TLS 1.3/TCP for the first implementation. It won every batched case here, and it already supplies reliable ordered delivery. UDP's single-entry 1 KiB advantage is small compared with its missing protocol work and run variation. QUIC did not earn its extra CPU in this one-stream, loss-free test.

This is not evidence about cross-host throughput, packet loss, 1 to 10 ms RTT, pipelined appends, multiplexed snapshot traffic or commit latency. Guess: TLS remains the best default on the target runners. Measure there before treating that as settled.

A batching estimate, not a measurement here: with arrival rate lambda entries/s and append cycle time T, self-clocked batches approach lambda*T entries until the byte cap. These stop-and-wait tests cannot select the production in-flight byte window. At nonzero RTT that window must cover the bandwidth-delay product. Keep snapshots off the heartbeat/append stream.

See `summary.md` for medians and throughput ranges and `results.jsonl` for all 90 measurements.
