# #485 retained-stack measurements

Measured commit: `fe430a4c1298b256370026cce4b3240c8eef0fda`.
Actions run: [42020177093](https://github.com/djosh34/tigerstore/actions/runs/42020177093),
2026-11-05. Both jobs passed:
[ARM job](https://github.com/djosh34/tigerstore/actions/runs/42020177093/job/127220656948),
[x86 job](https://github.com/djosh34/tigerstore/actions/runs/42020177093/job/127220656650).
Artifacts `exp485-stack-ubuntu-24.04-arm` and `exp485-stack-ubuntu-24.04`
contain all raw repetitions, fio JSON, hardware, and per-platform median reports
(including x86 purego). No local timing is used below.

All Go results: Go 1.27.2, five 1-second repetitions, independent medians of
ns/op, B/op, allocs/op. Serial operations, GOMAXPROCS=2, warmed persistent TLS
connections over 256 KiB-per-direction bufconn, except new handshakes.
See [README.md](README.md) for exact boundaries and [overlay.py](overlay.py) for
the explicit immediate-answer fixture implementation.

## Per primitive

**Each cell is `ns/op / B/op / allocs/op`.** These are observed serial per-unit
costs on these runners, not universal minimums or additive simulator forecasts.

| Primitive | ARM native | ARM purego | x86 native |
|---|---:|---:|---:|
| Put 1 KiB, immediate KV stub | 40,824 / 10,425 / 119 | 91,887 / 10,497 / 120 | 14,791 / 10,416 / 119 |
| Range 1 key (1 KiB value), stub | 44,492 / 11,353 / 126 | 96,272 / 11,425 / 128 | 15,237 / 11,335 / 126 |
| Compare-and-put 1 KiB, stub | 48,895 / 12,285 / 153 | 84,361 / 12,289 / 153 | 16,560 / 12,267 / 153 |
| Watch event 1 KiB, node output queue to clientv3 | 11,989 / 2,992 / 15 | 47,295 / 3,060 / 17 | 6,441 / 2,981 / 14 |
| Put 1 KiB, real single-member node/tmpfs | 148,551 / 16,771 / 200 | 188,672 / 16,751 / 200 | 36,087 / 16,798 / 200 |
| Range 1 key, real node/tmpfs | 83,434 / 13,769 / 146 | 126,416 / 13,793 / 147 | 19,066 / 13,709 / 145 |
| Compare-and-put 1 KiB, real node/tmpfs | 168,342 / 20,033 / 253 | 214,820 / 19,961 / 253 | 42,007 / 20,083 / 253 |
| Put + corresponding watch delivery, real node | 170,039 / 21,371 / 223 | 240,229 / 21,344 / 223 | 44,830 / 21,417 / 223 |
| Peer Send-to-Deliver, 64 B entry data | 3,491 / 560 / 13 | 8,584 / 560 / 13 | 2,730 / 560 / 13 |
| Peer Send-to-Deliver, 1 KiB entry data | 4,793 / 1,520 / 13 | 40,528 / 1,520 / 13 | 3,358 / 1,520 / 13 |
| Peer Send-to-Deliver, 16 KiB entry data | 25,435 / 16,903 / 13 | 456,760 / 16,949 / 13 | 19,093 / 16,902 / 13 |
| Client TLS handshake | 744,246 / 160,767 / 886 | 1,079,285 / 158,253 / 886 | 467,727 / 160,715 / 886 |
| Peer mutual TLS 1.3 handshake | 964,375 / 182,362 / 1,111 | 1,567,575 / 179,825 / 1,111 | 616,116 / 182,402 / 1,111 |
| Scenario CA + leaf key/cert generation | 332,654 / 56,386 / 830 | 665,394 / 56,380 / 830 | 208,108 / 56,384 / 830 |

The stub is at concrete `node.Node.SubmitBatch`: the real tigerstore server's
HTTP/2, admission, alias decode, completions, encoding, flow control and metrics
are retained. Range's reply uses compact MVCC KVs, Put uses wire-form replies.
The standalone watch measurement injects an event at the node's outbound
WatchStream queue; real tigerstore watch encoding, gRPC/TLS and clientv3 remain.
It does not time a Put. The real-node Put+watch row is deliberately a composite.

## Hardware and throughput

| Runner property | ubuntu-24.04-arm | ubuntu-24.04 |
|---|---|---|
| nproc / visible CPUs | 4 / 4 | 4 / 4 |
| CPU | ARM Neoverse-N2 | AMD EPYC 9V45 96-Core Processor (host model) |
| Exposed topology | 4 cores, 1 thread/core | 2 cores, 2 threads/core |
| L3 | 128 MiB | 32 MiB |
| RAM, bytes | 16,715,927,552 | 16,760,344,576 |
| Swap, bytes | 3,221,221,376 | 3,221,221,376 |
| Root disk / filesystem | /dev/sda1, ext4 | /dev/nvme0n1p1, ext4 |
| df root size / available, before tests | 145G / 104G | 145G / 85G |
| tmpfs /dev/shm size | 7.8G | 7.9G |
| Kernel | 7.0.0-1017-azure | 7.0.0-1017-azure |

| Useful copy throughput (decimal GB/s) | ARM native | x86 native |
|---|---:|---:|
| 1 worker | 15.171 | 26.608 |
| 2 workers | 27.778 | 29.958 |
| 4 workers | 48.565 | 29.503 |

Each worker copies 64 MiB source into a separate 64 MiB destination; maximum
working set 512 MiB, GOMAXPROCS=4. Counts bytes copied, not read+write traffic.
The ARM LLC is as large as a single worker's working set, so these numbers are
not guaranteed DRAM-only bandwidth. Round/barrier channel overhead is included.

| Per-core throughput (decimal MB/s, 1 MiB input) | ARM native | ARM purego | x86 native |
|---|---:|---:|---:|
| SHA-256 | 2,119.18 | 258.81 | 2,101.99 |
| AES-128-GCM encryption | 4,145.78 | 77.24 | 6,977.68 |

Both crypto benchmarks allocate 0 B/op and 0 allocs/op. Corresponding ns/op:
SHA256 494,802 / 4,051,585 / 498,849; AES-GCM 252,926 / 13,575,786 / 150,276.

| Disk median (5 x 5 seconds, 1-second ramp each) | ARM | x86 |
|---|---:|---:|
| Sequential direct write, 1 MiB QD1, MB/s | 360.56 | 279.73 |
| Sequential direct read, 1 MiB QD1, MB/s | 413.27 | 412.64 |
| Buffered 4 KiB write + fsync every write, QD1, ops/s | 13,178.96 | 5,446.51 |
| Random direct 4 KiB read, libaio QD32, IOPS | 19,563.95 | 19,698.06 |

Fio's entire 1 GiB test file was prewritten, no holes. Root was explicitly
remounted `barrier=1` before measurements; post-remount mount reports have no
`nobarrier`. Initial runner mount options included `nobarrier`; the initial
hardware dump alone is not the measurement mount state. `data=writeback`,
`journal_async_commit`, `commit=30` remain runner defaults. These are virtual
runner disk measurements, not claims about physical media durability.

## Implications / qualifications

* On ARM the retained immediate-answer client stack alone is 40.8 us Put,
44.5 us Range, 48.9 us Txn; a single 1 KiB peer delivery is 4.79 us. These
include real crypto and are substantially above the bare Apply/protobuf work
from #448 before simulator scheduler, fault model, checking and quorum work.
* Measured ARM purego/native ratios: stub Put 2.25x, standalone watch 3.94x,
1 KiB peer 8.46x, 16 KiB peer 17.96x, peer handshake 1.63x; real-node Put only
1.27x. Assembly recovery therefore does not imply an equal end-to-end speedup.
* `purego` is a tag contrast, not exact gosim emulation. It leaves runtime
memmove, scheduling, GC and assembly not guarded by that tag native.
* The real single-member node uses tmpfs, not simfs: simfs requires the
simulator's external locking and is not safe to attach directly to a native
concurrent node. Syncs are enabled, but tmpfs is not physical persistence.
* TLS handshakes use simulator P-256 ECDSA certificates and Go's default key
exchange (not forced X25519; Go can choose hybrid ML-KEM). They include TLS
object and two 16 KiB pipe-buffer allocations, but exclude peer framing preface
and client HTTP/2 setup. These are TLS handshakes, not full connection startup.
* Measurements are serial operation wall time, not aggregate CPU-ns across two
processors. Do not treat client roundtrip plus every peer roundtrip plus bare
Raft as strictly additive: work can overlap, batching changes cost, and peer
transport already includes protobuf marshal/unmarshal. No extrapolated simulator
speed or additive ceiling is asserted here.
