# Runner results: Go io_uring experiment (#576)

**Conclusion:** batching explains nearly all of the small-message improvement over a naive Go net server. The bounded raw-Go ring works with zero measured Go allocations, but its incremental CPU saving versus equally batched Go net is -0.073 to +0.139 us/request in this plaintext toy (one cell regresses). It is not a measured route from 36 us to 10 us for TLS/H2/Raft/WAL.

- Final run: https://github.com/djosh34/tigerstore/actions/runs/42697593922
- Measured commit: `37490b99c1f016a32fe941fff610b5aab7b2f6bf` on `research/576-gouring`.
- Preliminary corrected run: https://github.com/djosh34/tigerstore/actions/runs/42696798206 (per-connection net payloads, superseded by the equal-footprint final run).
- Neoverse-N2, ubuntu-24.04-arm, Linux 7.0.0-1017-azure, Go 1.27.2, memlock 8192 KiB, io_uring_disabled=0, no seccomp, CapEff=0.
- Server CPU 0/GOMAXPROCS=2; client CPU 3/GOMAXPROCS=1. 100-byte requests in 32-request bursts, then drain 32 replies. 500 ms warmup, 2 s measured interval, three trials/cell; table shows independent medians. No TLS.
- CPU is whole-server-process getrusage user+system CPU, not whole-machine CPU. Throughput may be client/transport limited. Identical shared immutable transmit payloads for all modes.

## CPU and throughput

Each cell: **server CPU us/request / million completed requests/s**.

| Connections | Reply | Go net | Go net batched | io_uring SEND | io_uring SEND_ZC fixed |
|---:|---:|---:|---:|---:|---:|
| 6 | 256 | 4.4748 / 0.223 | 0.2260 / 4.424 | 0.1963 / 4.453 | 0.2126 / 4.516 |
| 64 | 256 | 4.5229 / 0.221 | 0.2257 / 4.430 | 0.2091 / 4.226 | 0.2229 / 4.401 |
| 6 | 4096 | 5.2226 / 0.191 | 0.7583 / 1.319 | 0.8315 / 1.200 | 1.0247 / 0.975 |
| 64 | 4096 | 5.2842 / 0.189 | 0.7360 / 1.358 | 0.7114 / 1.404 | 0.9682 / 1.031 |
| 6 | 16384 | 5.8317 / 0.171 | 2.7790 / 0.360 | 2.7746 / 0.360 | 3.8400 / 0.260 |
| 64 | 16384 | 5.9073 / 0.169 | 2.7786 / 0.359 | 2.6398 / 0.378 | 3.6865 / 0.270 |

## Increment over batching

Positive numbers are CPU saved by ordinary io_uring SEND, not by SEND_ZC. These small residuals are not evidence of a meaningful improvement in every cell.

| Connections | Reply B | net -> batch saved us | batch -> ring saved us |
|---:|---:|---:|---:|
| 6 | 256 | 4.2488 | 0.0297 |
| 64 | 256 | 4.2972 | 0.0166 |
| 6 | 4096 | 4.4644 | -0.0732 |
| 64 | 4096 | 4.5482 | 0.0246 |
| 6 | 16384 | 3.0527 | 0.0045 |
| 64 | 16384 | 3.1287 | 0.1388 |

## System calls per request

Separate strace -f -c runs, 500 ms, no warmup. Counts include startup/teardown. **Do not use traced timings or assume these batch sizes equal untraced or real-service batching**: strace severely changes scheduling/throughput. Raw counts of read/write are close to 1+1 per naive RPC, 1/32+1/32 per small batched RPC; large batched replies need two writes. Ring entry can submit sends and process many recv/send CQEs together.

| Connections | Reply B | net all | batch all | ring all (enter only) | SEND_ZC all |
|---:|---:|---:|---:|---:|---:|
| 6 | 256 | 2.3264 | 0.0753 | 0.01223 (0.00762) | 0.01057 |
| 64 | 256 | 2.3546 | 0.0760 | 0.00263 (0.00082) | 0.00498 |
| 6 | 4096 | 2.3619 | 0.0842 | 0.02081 (0.00676) | 0.01910 |
| 64 | 4096 | 2.4585 | 0.0885 | 0.00474 (0.00083) | 0.00791 |
| 6 | 16384 | 2.3908 | 0.1378 | 0.03584 (0.01184) | 0.04780 |
| 64 | 16384 | 2.4285 | 0.1449 | 0.01185 (0.00169) | 0.03973 |

## Allocations and zero-copy verification

- **All 42 untraced io_uring trials (21 ordinary SEND + 21 fixed SEND_ZC) measured exactly zero Mallocs and zero TotalAlloc bytes in the interval.** This is runtime.MemStats for the entire server, not a hidden per-handler counter. Startup buffers/ring/slots are excluded; JSON output is after the snapshot.
- Across the 42 net/batch trials, total interval Mallocs range 0..168; these are whole-process/runtime transients, not one application allocation/request. See `results/trials.json` for exact per-trial allocation rates.
- **3,092,875 / 3,092,875 observed SEND_ZC notifications reported copy fallback** across the 21 untraced ZC trials. Thus the successful opcode and registered buffer did not deliver true zero-copy in this loopback setup. It cost 0.014..1.065 additional us/request versus ordinary ring SEND.
- All 108 client/server count comparisons (84 untraced + 24 traced cases) matched. Clients also checked first/last response bytes. ABI layout unit tests passed locally and on the runner.

## GOMAXPROCS subset: 6 connections, 256 B

| Mode | CPU us/req, P=1 | CPU us/req, P=2 | Mreq/s, P=1 | Mreq/s, P=2 |
|---|---:|---:|---:|---:|
| net | 4.3833 | 4.4748 | 0.228 | 0.223 |
| batch | 0.2129 | 0.2260 | 4.686 | 4.424 |
| uring | 0.1993 | 0.1963 | 4.431 | 4.453 |
| uringzc | 0.2152 | 0.2126 | 4.488 | 4.516 |

No WAL exists in this prototype. Do not use this subset to remove tigerstore's deliberate two-P minimum: `cmd/tigerstore/main.go:37-57` documents its fsync/P-retaking reason. Moving networking only does not remove that reason.

## Interpretation for the parent ticket

1. Start with batching and buffer ownership. Tigerstore already has 64 KiB client and 256 KiB peer batching, so the dramatic naive-net win cannot be counted again.
2. The kernel/runtime integration is feasible without cgo or a Go runtime fork. A locked owner plus bounded mmap pools fits SINGLE_ISSUER/DEFER_TASKRUN. Normal Syscall6 still has entersyscall/exitsyscall; idle waiting can relinquish its P. The runtime source analysis and library audit are in `README.md`.
3. Do not choose fixed SEND_ZC for this loopback path based on its name. Here it always copied, with extra notification/accounting overhead. Physical NIC/TLS costs were not measured.
4. Owning TLS/H2/Raft/WAL as nonblocking state machines is the real architectural cost. A ring under blocking net.Conn/crypto/tls retains goroutine scheduling and likely adds a ciphertext copy into registered buffers; it is not this prototype's shape.
5. Given 36 us/op and 21% syscall CPU, eliminating every syscall CPU cycle would still leave 28.44 us/op. io_uring cannot by itself close the target gap. Any prediction of service-wide us saved remains an explicitly unmeasured guess.

See `README.md` for primary-source links, limits, library SHAs/maintenance, and exact commands. `results/summary.csv`, `results/syscalls.txt`, `results/trials.json`, and `results/runner.txt` preserve the final run's summaries. Raw per-process output and strace reports are in the Actions artifact `research576gouring` (30-day retention).
