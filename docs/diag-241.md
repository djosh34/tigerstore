# Diagnostic reference for #241

Branch: `diag/241-heartbeat-after-corrupt`. Never merge or cherry-pick.

## Finding

The demonstrated bug is in the chaos harness, not a missing peer write
deadline. `tc qdisc change ... netem delay ...` does not clear omitted
corruption and reordering settings. `chaos/env/netem.go:71-78` used that
command to Heal. The diagnostic fix explicitly sets loss, reorder and
corrupt to zero. Loss already resets implicitly on the tested Linux kernel;
corruption and reordering do not.

`cmd/chaos/envfaults.go:392-396` reads qdisc state before healing and marks
`Healed` from the command's exit status alone. It never checks post-heal
settings. A successful command is therefore reported as a healed link even
while corruption/reordering remain active.

## Original evidence

Downloaded using:

```
gh run download 39031253198 -n chaos-campaign-plain-4 -D /tmp/diag241/old
gh run download 39119748549 -n chaos-campaign-plain-3 -D /tmp/diag241/new
gh api repos/djosh34/tigerstore/actions/jobs/117391988590/logs
gh api repos/djosh34/tigerstore/actions/jobs/117652749378/logs
```

`chaos-campaign-plain-4/seed-2000204686/faults.json`, step 20, says:

- Requested only `netem-loss` on `node2-node1`.
- Actual qdisc: `delay 2ms loss 20% corrupt 5%`.
- Requested at 127.441 s, reported healed at 134.664 s.

The corruption is left over from an earlier netem-corrupt step. node1.log
loses leader 2 at 12:14:13.559 and regains it at 12:14:33.522. node2.log
has no peer send error during that gap. chaos.log reports the next step
failing to agree on a leader within 15 s. All six consistency checkers pass.

`chaos-campaign-plain-3/seed-7663907147/faults.json`, step 16, says:

- Requested only `netem-loss` on `node3-node1`.
- Actual qdisc: `delay 5ms loss 20% reorder 50% 50% corrupt 5%`.
- Requested at 99.703 s, reported healed at 107.676 s.

The reordering and corruption are left over from earlier steps.
chaos.log and job-log lines 566-630 report step 17 failing at 125.124 s:
node1 has leader 0, term 2, commit/applied 2341; node3 leads term 2 at
2727 and node2 follows it. node1.log last loses leader 3 at 10:51:49.734.

Neither artifact saves post-heal qdisc state, TCP retransmission timers,
a packet capture or per-message send/receive timestamps. Continued
corruption is proven by the setter and regression, and the inherited
settings are present in both artifacts. The exact TCP sequence responsible
for the original 20-second gap is not proven. TCP retransmission backoff
under the still-corrupting link is a plausible contributor, not a measured
fact. A real transport-only failure after a genuinely healed link remains
unestablished.

## Transport and etcd comparison

`internal/peer/peer.go:1033` sets a write deadline before each framed write;
`:1045-1053` defaults it to 5 s. `sender.run`, `:891-919`, closes/untracks
a failed connection and retries. `isData`, `:788-801`, sends heartbeats
through the control queue ahead of data. However the queues share one TCP
stream and write success only means the socket accepted bytes. It does
not prove delivery. `receive`, `:529`, clears the message stream's read
deadline. There is no streaming idle read deadline or independent link
heartbeat. The existing `TestStalledPeerIsDropped` passes in both diagnostic
Actions runs, checking that a genuinely blocked write fails and reconnects.

etcd v3.7.2 `etcdserver/api/rafthttp/stream.go:168-205` sends and flushes
link heartbeats every read-timeout/3. Its stream reader closes on decode
failure and redials (`:406-465`, `:495-502`).
`client/pkg/transport/timeout_conn.go:37-43` refreshes the read deadline
before each socket read; rafthttp defaults read/write timeouts to 5 s.
That is stronger idle-stream detection, but it cannot justify removing
corruption from the harness's claimed-healed state.

## Short reproduction and evidence

```
go test -c -tags chaosenv -o /tmp/diag241.test ./chaos/env
sudo -n unshare --net bash -c 'ip link set lo up; exec /tmp/diag241.test -test.run "^TestDiag241NetemHeal" -test.count=20 -test.timeout=90s -test.v'
```

The namespace isolates qdisc changes from the host. Tests cover production
corruption/reordering percentages, loss as a negative control, and the real
peer TLS/framing/MsgHeartbeat path. The heartbeat case uses 100% corruption
to shorten the failure, sends no messages during the active fault, calls
Heal, then waits up to 2 s for a heartbeat. The unchanged socket accepts
small writes without guaranteeing delivery.

Without the fix, commit `9618e4a8a3c5f687a816e0b46e05c47b90435c0c`:
https://github.com/djosh34/tigerstore/actions/runs/39146500991

Corrupt/reorder checks fail 20/20 each. The post-heal heartbeat fails 18/20;
2/20 deliver a heartbeat despite retained corruption. The loss-only control
passes 20/20. The targeted job takes about 80 s including setup/build.

The fix changes only Netem.Heal. The same workflow and tests run on push.
The issue comment records the green run and fix SHA.

For phase 2, also capture and validate post-heal qdisc state in fault records
and node-failure diagnostics. If healthy-state liveness still fails, record
TCP_INFO/ss retransmission state and last heartbeat sent/received times so
that buffered writes, link loss and a blocked Raft loop can be distinguished.
