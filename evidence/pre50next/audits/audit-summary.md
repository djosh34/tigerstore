# Audit disposition

Only final findings and test output are published, not helper reasoning transcripts.

- The original harness audit is retained in `harness-audit.txt`. Its actual-sampler-bounds and complete-cohort findings are addressed by `check_profiles.py`, the stricter summarizer and `verify.py`. Zero syncs are represented as undefined. No measured cohort had that case.
- The etcd committed gauge is checked after the replica revision fence, then a fresh counter snapshot is taken. At the pinned etcd source the gauge follows WAL.Save. Directory-sync differences are disclosed in the report.
- The conditional-credit-wake change keeps the queue-length decision under the existing connection lock. Tests cover threshold crossings, no-frame credit and existing control frames.
- The peer buffer is bounded, remains below TLS, does not buffer the handshake, flushes both layers before waiting, and closes the underlying socket to interrupt blocked writes. Focused buffer, TLS, close and stall tests pass. This is not a full failure-mode acceptance run.
- The lean draft's literal-only headers were corrected before any benchmark. Every published lean job uses first/warmed indexed HPACK blocks. A non-default HPACK table limit is rejected explicitly rather than measured incorrectly.
- Lean response checks require valid HTTP/gRPC status and exactly one framed PutResponse with a positive revision and term. Duplicate/unknown streams, malformed responses, protocol failures and stalls fail the run. Unit and race tests cover the bounded workload, flow control and barriers.
- Range probes do not read back all values. Revision accounting and PING drain checks are not durability proofs. The PING can only catch invalid frames processed before completion.
- The lean duration can be clipped by the absolute campaign deadline. All reported duration cells actually lasted at least 12 seconds through drain, far before that deadline. The archive verifier checks elapsed time and recorded argv, so a truncated cell cannot pass this campaign's offline audit.
- Thread placement and generator/process CPU arithmetic are rechecked from archived data, not inferred solely from the launcher arguments.
- `validation.json` covers 250 runner windows, eight host windows and 120 CPU profiles. All recorded Actions jobs succeeded. The late grpc-go peer regression and the noisy lean 64-client screen remain included.
- Local Go tests used hard memory-bounded systemd units, GOFLAGS=-p=2, GOMEMLIMIT=2GiB and test parallelism 2. Host benchmark trees used a verified 1 GiB cgroup cap with no swap. Systemd's implausibly low printed memory peaks are not reported as actual peaks.
