# Issue 539 diagnosis

Diagnosis-only branch. Do not merge it.

Base: `6d2cd05105a44efa6dcf4c093f212bb81f51289f` (`origin/main` after fetch).
Instrumentation: `ef3749dd656559312e8a26532b955858819d1dff`.
Prototype: `7ee2ad746a05c0195373489004197c7f0dbdfb4c`.
Retry guard tests: `2980aa641b1b4de411e6e48e686ddb77b7266dd7`.

All runs were on this shared Linux host, using Go 1.27.2,
`GOFLAGS=-p=2` and `GOMAXPROCS=1`. One heavy test command ran at a time.
No peers were killed or disconnected by the test.

## Cause

The same-term leader step-down is Raft's CheckQuorum. In diagnostic run 88,
node 1 processed its last heartbeat response at 2575.862 ms. At tick 128
(2577.774 ms), the prior quorum check still had active peers. Both peers
are inactive in the trace before tick 129. No further peer response is
processed before tick 138 (2846.121 ms). Raft then logs:

```
2846.088ms node=1 turn at=279 due=280 now=281 waited=279 ready=280 dueIn=1 held=false inputs=0 backlog=0
2846.121ms node=1 Tick now=138 role=StateLeader term=2 lead=1 ...
2846.190ms node=1 raft: WARN: 1 stepped down to follower since quorum is not active
2846.222ms node=1 raft: INFO: 1 became follower at term 2
```

The ten ticks after the previous check were spread over about 268 ms.
This was not a burst of ten catch-up ticks from a starved clock. Followers
queued heartbeat responses during that interval, but none had been
processed by the leader. At the step-down turn, its inbox was empty and
its disk backlog was zero. The trace shows delayed peer-message progress,
not a new Raft term forcing this step-down. CPU scheduling is the likely
source of the delays; the trace does not isolate a particular runtime
pause or transport goroutine.

Run 103 shows an additional ordering effect. The leader takes one Tick
before handling its 22 queued inputs. That Tick runs CheckQuorum and drops
leadership in term 2. It then consumes heartbeat responses that have waited
about 54 ms in its inbox. Processing those replies first might prevent
that particular step-down. It would not prevent run 88, where no inputs
were waiting at the check.

Run 39 has a different legitimate Raft path: node 1 receives a higher-term
MsgAppResp from node 2 and becomes a follower at term 3. Six of the nine
failed diagnostic runs instead show the serving node 1 temporarily losing
its known leader while node 2 remains leader. These failures are not all
instances of the issue's same-term leader step-down.

## Decision and primary sources

Keep quorum checks and the clock behavior. Let this range-pressure test
retry transient election errors like an etcd client. An alive process has
no guarantee of retaining leadership when peer replies are delayed longer
than an election window. The prototype changes only the test's Range
calls. It keeps the same key, revision, limit, TLS connection, gzip options
and original 20-second context. It waits 10 ms between retries. It retries
only `ErrLeaderChanged` and `ErrNoLeader`. Other errors and bad replies
still fail the test.

Sources:

- [Raft v3.7.0 tickHeartbeat and becomeFollower](https://github.com/etcd-io/raft/blob/v3.7.0/raft.go#L861-L900): every election interval, the leader checks quorum. A follower transition need not increment the term.
- [Raft v3.7.0 MsgCheckQuorum](https://github.com/etcd-io/raft/blob/v3.7.0/raft.go#L1281-L1294): no active quorum causes exactly the warning in the trace and `becomeFollower(r.Term, None)`.
- [etcd v3.7.2 bootstrap](https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/bootstrap.go#L535-L550): etcd enables CheckQuorum too.
- [etcd v3.7.2 read-index handling](https://github.com/etcd-io/etcd/blob/v3.7.2/server/etcdserver/read/read.go#L164-L194): leadership changes return a retryable `ErrLeaderChanged`.
- [etcd v3.7.2 read retries](https://github.com/etcd-io/etcd/blob/v3.7.2/client/v3/retry.go#L47-L74): immutable reads retry Unavailable. The prototype is narrower than this policy.
- Tigerstore's `heartbeatSent` in `internal/node/node.go` only warns and increments a counter. That warning cannot make Raft step down. It measures outgoing heartbeats, not the time until their responses are processed.

## Runs, in execution order

All package commands were `go test -race -run '^TestRangeReplyClients$' ./internal/server`,
with the flags below added. Full output is in the matching `.log.gz` file.

| Commit | Added flags / environment | Result | Package time | Log |
| --- | --- | --- | --- | --- |
| `6d2cd051` | `-count=40` | 1 failed / 40 | 170.753 s | `baseline-40.log.gz` |
| `ef3749dd` | `TIGERSTORE_DIAG_539=1`, `-count=40` | 0 failed / 40 | 200.672 s | `diagnostic-40.log.gz` |
| `ef3749dd` | `TIGERSTORE_DIAG_539=1`, `-v -count=120` | 9 failed / 120; two traced CheckQuorum losses | 468.110 s | `diagnostic-120.log.gz` |
| `7ee2ad74` | `-v -count=160` | package timed out at 10 min; 158 completed passes, no completed test failures | 600.174 s | `prototype-160.log.gz` |
| `ef3749dd` | `-v -timeout=20m -count=120`, tracing off | 4 failed / 120 | 454.852 s | `without-prototype-120.log.gz` |
| `7ee2ad74` | `-v -timeout=20m -count=120`, tracing off | 0 failed / 120; 18 retries in four runs | 453.316 s | `with-prototype-120.log.gz` |

The 160-run package timeout is not a passing batch. The completed 120-run
comparison is the main with/without result. Run 39 of the passing batch
still loses leadership in term 2, enters term 3, retries eight page calls
at revision 31, validates all pages and passes in 3.76 s. The prototype
does not suppress elections. A finite passing batch is not proof that
future retries can never exhaust the deadline.

Additional checks on the guard-test commit:

```
GOFLAGS=-p=2 GOMAXPROCS=1 go test -race -timeout=20m -count=10 -run '^TestRangeWithLeaderRetry$' ./internal/server
# PASS, 1.236 s
GOFLAGS=-p=2 GOMAXPROCS=1 go test -race -timeout=20m ./internal/core ./internal/node
# PASS, core 42.255 s, node 63.430 s
```

The guard tests check that retries preserve context, request and compression,
that compaction errors are not retried, and that cancellation stops retries.

## Evidence files

- `diagnostic-run-088.log`: full failed test output and the last 600 trace events at the same-term quorum loss, with an empty inbox.
- `diagnostic-run-103.log`: same-term quorum loss immediately before consuming queued replies.
- `diagnostic-run-039.log`: higher-term response step-down, not CheckQuorum.
- `without-prototype-run-096.log` and `without-prototype-run-097.log`: failures in the tracing-off replay.
- `prototype-run-039.log`: a completed, passing test despite leader loss and a term change.
- `*.log.gz`: original full command output, including the timeout batch. Use `gzip -cd FILE.log.gz` to inspect it. Run numbers above are one-based within a command's `-count` sequence.

Tracing changes timing. Do not treat its 9/120 failure rate as the uninstrumented
rate. The tracing-off replay failed 4/120, close to the issue's reported rate.
