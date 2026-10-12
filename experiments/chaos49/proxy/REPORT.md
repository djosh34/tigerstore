# Seeded TLS byte-stream fault sketch

Question: can a Go TCP relay apply the requested faults without terminating TLS, and what does that relay cost on this host?

This is a throwaway, standalone, standard-library-only Go module. It is not tigerstore code and is not a candidate for merging. I read issues #46, #1, #49, #20 and #33, including comments, and `docs/design.md` on main. The design snapshot matches commit `91c4f974a6661ac02c2e7747781ea215353c643b`, the skeleton. No real tigerstore server, gRPC workload, cluster, correctness checker or packet fault was exercised. No subagents, comments, pushes, root commands or host configuration changes were made.

## Result for the parent

- All 78 TLS cases passed: 13 modes, seeds 49 and 50, three repetitions each. Every requested fault produced an action trace and its expected endpoint behavior.
- Mutation offsets and delay transitions use source-stream byte offsets, not `Read` lengths. The relay never uses `io.ReadFull` to fill a forwarding buffer. Small TLS records and fragmented raw-stream writes passed separate tests, including the race detector.
- With endpoint and proxy processes sharing CPU 6, median 256-byte echo round-trip latency was 7.292 microseconds direct and 21.458 microseconds through the proxy. The median paired increase was 14.168 microseconds.
- Proxy-only CPU was measured in a separate process. Its median was 0.527 ms per decimal MB of plaintext forwarded, counting both directions. Total endpoint-plus-proxy CPU was 1.271 ms/MB, versus 0.662 ms/MB direct. The median paired total increase was 0.604 ms/MB.
- Other workers were busy. These are shared-host observations, not quiet-host limits or GitHub runner results.

## What the relay does

`proxy.go` accepts TCP, opens a second TCP connection to the upstream, and copies the ordered streams. It has no TLS keys or TLS implementation. TLS runs end to end between the local client and server in `main.go`. The relay terminates TCP, not TLS.

A per-connection, per-direction hash of the seed and connection ID selects a source offset between 32 and 127 bytes. Deleting or inserting a byte does not change the later source offsets. Source offsets count bytes consumed from the sender, not bytes forwarded. Delays change at fixed 4,096-byte milestones. A partial `Read` can cross a milestone, but the relay does not wait for the rest of that block to arrive.

| Mode | Sketch policy | Observed TLS endpoint result |
| --- | --- | --- |
| `none` | Forward each ordinary socket read, up to 32 KiB, without fault logic or trace entries. | Exact plaintext echo. |
| `drop` | Delete one byte at the seeded client-to-server source offset. | Remote TLS `bad record MAC`. |
| `short` | Wait 1 ms at each 4 KiB source milestone, in both directions. | Exact plaintext echo, delayed. |
| `long` | Wait 25 ms at each milestone, in both directions. | Exact plaintext echo, delayed. |
| `switching` | Alternate 1 ms and 25 ms at milestones. The seed selects each direction's starting phase. | Exact plaintext echo, with both delay settings in the trace. |
| `stall` | Forward the prefix before the target, then stop forwarding and consuming client-to-server bytes. Keep sockets and the reverse pump open. | Echo read timed out. An independent reverse TLS record still arrived. Saturation also made client writes time out. |
| `reset` | Set zero linger and close both relay TCP sockets at the target. | Client received `ECONNRESET`, not a generic accepted failure. |
| `corrupt` | Flip one bit at the target. | Remote TLS `bad record MAC`. |
| `duplicate` | Insert a second copy of one source byte at the target. | Remote TLS `bad record MAC`. |
| `one-way-cut` | Forward the prefix, then consume and discard client-to-server bytes. Keep the reverse pump open. | Echo timed out while an independent reverse TLS record arrived intact. |
| `both-cut` | Apply that discard policy independently to both directions. | Read timed out, with both directions' cuts recorded. |
| `fast` | Pace both directions at an average 100,000,000 source bytes/s. | Exact plaintext echo. |
| `slow` | Pace both directions at an average 1,000,000 source bytes/s. | Exact plaintext echo, slower than `fast`. |

The delay modes are milestone waits, not a constant propagation delay per record or a claimed RTT. The rate model uses cumulative byte-count deadlines. It permits credit after idle time and is not a strict token bucket. Timers and endpoint record buffering limit its accuracy. For seed 49 the fast echo took 2.286 to 2.380 ms and the slow echo took 54.071 to 54.888 ms. The configured rate ratio is 100, but this short request/echo workload did not produce a 100-fold wall-time ratio.

In the TLS exercises, faults arm only after a verified TLS handshake and a 64-byte echo. They therefore act on encrypted application records. The CLI `serve` command starts armed and can affect the unencrypted TLS handshake too. Its policy is fixed for its lifetime. It has no remote arming endpoint, reconnection replay machinery, campaign scheduler or stable logical node identities. Mutation and stall modes target client-to-server; delay and rate modes affect both directions. `both-cut` targets each direction separately.

Streams have a 30-second lifetime deadline in this sketch. Each pump owns one 32 KiB read buffer. `stall` may consume that one buffer before it stops reading; TCP buffers can accept more data before the sender sees backpressure. Trace storage is not capped. These are deliberate sketch limits, not production resource guarantees.

## Encrypted streams, TLS termination and packets

RFC 8446 section 5.2 requires TLS 1.3 AEAD authentication and termination with `bad_record_mac` when record decryption fails. Ciphertext mutation cannot stand in for a valid corrupted Raft message. The tests observed authentication errors for deletion, duplication and a flipped bit. The relay cannot inspect keys, revisions, HTTP/2 streams or peer frames.

A TLS-terminating relay could decrypt a request, change or duplicate a decoded frame, and re-encrypt it as valid TLS. That requires test trust configuration and two TLS sessions. It changes the authenticated path and tests a different failure class. It was not implemented or measured here. TLS record boundaries could also be parsed from the outer record headers without decrypting, but this sketch uses fixed byte offsets instead.

TCP packet loss, duplication and reordering are different again. RFC 9293 section 3.8 describes retransmission and sequence-number processing. TCP normally repairs lost segments and rejects duplicate sequence space before applications see the stream. Deleting an already received stream byte in a relay does not trigger TCP retransmission of that byte. Inserting an application-stream byte does not model a duplicated TCP packet. Packet corruption may be caught by checksums rather than TLS. Packet loss and reordering also affect congestion control and kernel retransmission timing, which this relay does not reproduce.

`tc netem` operates on packets and complements a byte relay. No netem, packet capture, namespace, cgroup, firewall or loopback configuration change was run on this host. There is no claim that packet faults passed.

## Seed replay and its limits

The seed controls the mutation target and switching-delay phase. Short/long waits and rate settings are fixed policies. Connection IDs follow accept order. For seed 49, the mutation target was byte 86 client-to-server and byte 96 server-to-client. For seed 50, those targets were 51 and 113.

Each action records connection ID, direction, source offset, action, requested delay or rate, and monotonic time since relay creation. `policy_trace_sha256` removes timestamps and sorts by direction and offset. All three canonical policy hashes matched for every mode under each seed. New certificates and TLS randomness were generated for each case; this does not mean their ciphertext matched.

A seed replays the policy, not process execution. Kernel reads, scheduler order, TLS record layout, reconnect order, client timing, CPU load and timer wakeups can change. Accept-order IDs are especially weak under concurrent connections. The atomic post-handshake arm is synchronized by this one-client exercise, not by a general distributed controller. Absolute wall times and the chronological interleaving of direction traces differed. Preserve explicit offsets, traces, seed, source hashes and workload when diagnosing a real-process failure. Do not claim bit-for-bit execution replay or infer a cluster correctness guarantee from these traces.

## TLS exercise details and raw results

The client trusts an ephemeral self-signed ECDSA P-256 server certificate with DNS name `localhost`. Certificate verification stays enabled. Both endpoints force TLS 1.3 and disable dynamic record sizing. The observed cipher was `TLS_AES_128_GCM_SHA256`. This is server-authenticated TLS, not mutual TLS. The same relay would forward mTLS bytes, but mTLS was not exercised.

Each case sends a 32,768-byte request after arming. Success cases compare the complete plaintext echo. Damage cases require `bad record MAC`; reset requires `ECONNRESET`; stalls and cuts require a timeout. One-way cut and stall also require receipt of a separate 64-byte server record while the request is withheld. Both-cut sends independent server data too and requires trace entries for both directions. Healthy cases have a three-second deadline; fault cases use 250 ms.

| Mode | Seed 49 wall ms, three runs | Seed 50 wall ms, three runs |
| --- | --- | --- |
| none | 0.105 / 0.086 / 0.088 | 0.154 / 0.092 / 0.094 |
| drop | 0.103 / 0.089 / 0.090 | 0.106 / 0.132 / 0.092 |
| short | 16.430 / 15.712 / 15.686 | 15.998 / 16.291 / 16.178 |
| long | 353.194 / 353.916 / 353.298 | 353.371 / 353.221 / 355.365 |
| switching | 160.353 / 160.121 / 162.519 | 208.827 / 209.005 / 210.723 |
| stall | 250.207 / 250.494 / 250.583 | 250.501 / 250.785 / 250.650 |
| reset | 0.175 / 0.059 / 0.066 | 0.183 / 0.061 / 0.062 |
| corrupt | 0.388 / 0.103 / 0.082 | 0.417 / 0.094 / 0.083 |
| duplicate | 0.095 / 0.085 / 0.134 | 0.080 / 0.088 / 0.114 |
| one-way-cut | 250.477 / 250.520 / 250.438 | 250.432 / 250.731 / 250.640 |
| both-cut | 250.550 / 250.809 / 250.221 | 250.596 / 251.123 / 250.373 |
| fast | 2.380 / 2.354 / 2.286 | 2.350 / 2.370 / 2.412 |
| slow | 54.888 / 54.149 / 54.071 | 54.289 / 55.029 / 54.811 |

Full JSON output, errors, action offsets, timestamps and hashes are in `raw/exercise-seed49.jsonl` and `raw/exercise-seed50.jsonl`. The exercise's `process_cpu_ns` is combined endpoint-plus-relay CPU because that exercise runs them in one process. It is not the proxy-only benchmark below.

`TestSmallTLSRecords` completes 32 sequential one-byte plaintext echoes for each healthy mode. `TestMutationOffsetsIgnoreReadFragmentation` verifies exact output bytes for drop, corrupt and duplicate with raw TCP sender writes of 1, 7, 113, 4,096 and 8,192 bytes. Those supplemental raw-stream tests do not replace the TLS exercises. `TestStallBackpressure` made client TLS writes time out after 4,194,304 plaintext bytes on this run. Its TLS graceful-close teardown adds about five seconds after the 250 ms blocked-write observation. `go vet`, ordinary tests and race-detector tests passed. See `raw/vet.txt`, `raw/test.txt` and `raw/test-race.txt`.

## Host, placement and workload

The final run was 2026-10-12T22:26:35Z through 22:27:07Z, before the deadline of 2026-10-13T22:00:00Z. Per-stage UTC checks are in `raw/run.stdout`.

- Go `go1.27.2 linux/arm64`, normal optimized `go build`, no race instrumentation in the measured executable. Default GC and standard-library TLS.
- Kernel `5.14.0-611.26.1.el9_7.aarch64`, build `#1 SMP PREEMPT_DYNAMIC Thu Jan 29 15:47:59 UTC 2026`.
- Guest `lima-opencode-vm`; `systemd-detect-virt` reports `apple`. Eight online ARM64 CPUs, one thread per exposed core, one cluster, Apple vendor, model 0, implementer `0x61`, CPU part `0x000`, revision 0, BogoMIPS 48.00. AES and PMULL are exposed. The guest does not expose the physical Apple machine model or CPU frequency, so neither is claimed.
- Guest RAM was exactly 12,178,378,752 bytes. Swap in use was 1,972,985,856 bytes before and after the final run. Hardware, memory and feature output is preserved in `raw/host-before.txt`, `raw/cpuinfo.txt` and `raw/meminfo.txt`.
- `taskset -c 6`, `GOMAXPROCS=1`. Client and server share the parent process. The separate relay child inherits affinity and environment. The benchmark verifies `Cpus_allowed_list=6` for both parent and proxy in every row. All are on the same guest core, not three dedicated cores. Other tasks are not excluded from CPU 6.
- Five paired repetitions, alternating direct/proxy order. Fresh endpoints and a fresh connection per path. TLS handshake, certificate creation and process startup are outside the measured intervals. There is no TLS resumption cache.
- Latency workload is one connection, one outstanding request, 100 warmup echoes, then 2,000 measured 256-byte request/echo operations. Quantiles are per-run nearest lower order statistics.
- Bulk workload is 16 warmup echoes, then 7,813 sequential 32,768-byte request/echo operations. Each sample requests 256 decimal MB, rounded up to 256,016,384 request bytes. Matching responses make 512,032,768 bidirectional plaintext bytes. It is a serialized echo workload, not a unidirectional saturation stream.
- The relay is fault-free for the cost comparison. It does ordinary userspace read/write forwarding with a 32 KiB buffer, not splice or kernel zero-copy. No fault trace entries are emitted in `none` mode.

### CPU accounting

Endpoint process CPU is the delta of `getrusage RUSAGE_SELF`, user plus system time. It includes client, server, TLS and benchmark work, but excludes the child. Proxy-only CPU is the child process's `/proc/PID/stat` fields 14 and 15 sampled around each interval. `getconf CLK_TCK` returned 100. Raw tick counts and derived nanoseconds are in every benchmark row.

The proxy consumed 28, 27, 28, 27 and 27 bulk ticks in chronological proxy-run order. One tick is 10 ms. Since user and system counters are quantized separately, a conservative delta quantization error is less than 20 ms, or 0.0391 ms per bidirectional MB in these samples. Fine-grained latency-phase proxy CPU is too short for this accounting precision; use the bulk numbers. Total CPU sums parent and child process CPU, not whole-host or hypervisor CPU. Work charged to independent kernel threads is not included.

The denominator is plaintext bytes in both directions, excluding TLS record overhead. For a denominator of request MB only, double the reported CPU/MB values. The median proxy-only cost would then be 1.055 ms per request MB. Throughput below counts request MB only over the whole echo interval; it is not a NIC line-rate measurement.

### Measured costs

All CPU columns use decimal bidirectional plaintext MB. All ten raw rows are in `raw/benchmark.jsonl`.

| Rep | Path | p50 us | p95 us | p99 us | Endpoint CPU ms/MB | Proxy CPU ms/MB | Total CPU ms/MB | Echo request MB/s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | direct | 7.334 | 8.083 | 14.334 | 0.678 | 0 | 0.678 | 732.976 |
| 1 | proxy | 22.083 | 48.750 | 79.792 | 0.735 | 0.547 | 1.282 | 391.565 |
| 2 | proxy | 21.292 | 35.125 | 40.500 | 0.744 | 0.527 | 1.271 | 391.026 |
| 2 | direct | 7.292 | 7.750 | 15.584 | 0.667 | 0 | 0.667 | 747.833 |
| 3 | direct | 7.291 | 7.625 | 8.083 | 0.657 | 0 | 0.657 | 757.597 |
| 3 | proxy | 21.459 | 36.792 | 52.667 | 0.726 | 0.547 | 1.272 | 397.732 |
| 4 | proxy | 21.458 | 35.666 | 39.583 | 0.731 | 0.527 | 1.259 | 397.524 |
| 4 | direct | 7.250 | 7.667 | 13.084 | 0.657 | 0 | 0.657 | 759.230 |
| 5 | direct | 7.292 | 7.792 | 9.667 | 0.662 | 0 | 0.662 | 753.710 |
| 5 | proxy | 21.334 | 36.625 | 46.167 | 0.724 | 0.527 | 1.251 | 397.134 |

Medians across runs were direct/proxy p95 of 7.750/36.625 us, p99 of 13.084/46.167 us, and echo request throughput of 753.710/397.134 MB/s. The total CPU increase exceeds proxy-only CPU because endpoint scheduling and socket work change too. This sketch shows that a userspace relay is measurable in a shared one-core budget. It does not establish an appropriate transport or CPU layout for tigerstore.

### Concurrent-worker noise

The run began with load average 4.73/4.31/2.98 and ended with 4.87/4.38/3.05. Process snapshots showed `sim.test` at 163 to 171 percent lifetime CPU and other workers' `grpcbench` processes near or above one full core. One initial `sim.test` snapshot had current processor 6, the same CPU used here. A snapshot's current processor is not an affinity guarantee.

During the benchmark, the five one-second `vmstat` intervals showed 49 to 52 percent user CPU, 12 to 18 percent system CPU, and 29 to 37 percent idle across the guest. Those intervals showed no swap-in or swap-out; existing swap usage remained. The first `vmstat` data row is the since-boot average and is excluded from those ranges. There was no quiet-host reservation, and no other worker was stopped. See `raw/vmstat-bench.txt` and the before/after process snapshots.

## Commands and files

From this directory, before the deadline:

```sh
export TMPDIR="$PWD/.tmp" GOCACHE="$PWD/.gocache" GOMODCACHE="$PWD/.gomodcache"
./run.sh > raw/run.stdout 2> raw/run.stderr
python3 summarize.py > raw/summary.txt
```

`run.sh` checks the UTC deadline before each build, test and exercise stage. It places all Go caches, module caches, temporary files and binaries under this directory. The core run commands are:

```sh
go build -o build/sketch .
go vet ./...
taskset -c 6 env GOMAXPROCS=1 go test -v -count=1 ./...
taskset -c 6 env GOMAXPROCS=1 go test -race -v -count=1 ./...
taskset -c 6 env GOMAXPROCS=1 ./build/sketch -command exercise -seed 49 -reps 3
taskset -c 6 env GOMAXPROCS=1 ./build/sketch -command exercise -seed 50 -reps 3
taskset -c 6 env GOMAXPROCS=1 ./build/sketch -command bench -seed 49 -reps 5 -ops 2000 -mb 256
```

The executable also supports an ordinary TCP listener:

```sh
# The upstream must already exist. This command starts armed.
./build/sketch -command serve -listen 127.0.0.1:9443 -upstream 127.0.0.1:8443 -mode corrupt -seed 49
```

It prints the bound address, then emits its trace after SIGINT or SIGTERM. This external-server command was not used to claim real-server integration. The benchmark itself did exercise `serve` as a separate process in `none` mode.

An initial full-run attempt stopped before tests because `cp /proc/cpuinfo` preserved a read-only file mode and a second copy could not overwrite it. `raw/run-aborted.stderr` contains the error. The script now uses `cp --remove-destination` for its owned metadata snapshots; the complete rerun passed. Earlier exploratory outputs are retained as `raw/exercise-initial.jsonl` and `raw/bench-smoke.jsonl`; they predate the final source snapshot and are not the tables above.

Source identity is in `raw/source-sha256.txt`; downloaded-source identity is in `raw/sources-sha256.txt`. The report and raw results remain with the source. Build output and the owned `.tmp`, `.gocache` and `.gomodcache` directories are removed for handoff. A rerun recreates them. No artifacts were installed elsewhere.

## Sources

1. [Issue #49](https://github.com/djosh34/tigerstore/issues/49), [#20](https://github.com/djosh34/tigerstore/issues/20), [#33](https://github.com/djosh34/tigerstore/issues/33), [#46](https://github.com/djosh34/tigerstore/issues/46) and [#1](https://github.com/djosh34/tigerstore/issues/1). Full issue bodies and comments were fetched with `gh issue view N --repo djosh34/tigerstore --json number,title,body,url,comments` into `sources/issue-N.json`. Issues 46 and 33 also have readable `.md` copies. No comments were posted.
2. [Design at the checked main commit](https://github.com/djosh34/tigerstore/blob/91c4f974a6661ac02c2e7747781ea215353c643b/docs/design.md). `gh api repos/djosh34/tigerstore/contents/docs/design.md?ref=main --jq .content | base64 -d` produced `sources/design.md`. A pinned ref fetch matches byte for byte; see `raw/design-identity.txt`.
3. [RFC 8446 section 5.2](https://www.rfc-editor.org/rfc/rfc8446.html#section-5.2), TLS 1.3 record authentication. Full text is `sources/rfc8446.txt`.
4. [RFC 9293 section 3.8](https://www.rfc-editor.org/rfc/rfc9293.html#section-3.8), TCP segment retransmission and sequence processing. Full text is `sources/rfc9293.txt`.
5. Installed Go 1.27.2 sources under `/home/joshazimullah.linux/.local/opt/go1.27.2/src`. Copied TLS record code and `net.TCPConn.SetLinger` documentation are in `sources/go1.27.2-tls-conn.go.txt` and `sources/go1.27.2-tcpsock.go.txt`. Zero linger discards unsent/unacknowledged data; the exercised Linux path returned `ECONNRESET`.
6. [Linux proc_pid_stat documentation](https://man7.org/linux/man-pages/man5/proc_pid_stat.5.html), fields 14 and 15, and [tcp documentation](https://man7.org/linux/man-pages/man7/tcp.7.html). Saved under `sources/`.
7. [tc-netem documentation](https://man7.org/linux/man-pages/man8/tc-netem.8.html), packet impairments and timer limits. Saved as `sources/tc-netem.8.html`. Read for the distinction above; netem was not run.
