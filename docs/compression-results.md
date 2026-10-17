# Compression result: do not merge

The experiment fails #132's rule. Real Kubernetes values gain, but the
random-value maximum-throughput run regresses under #144's comparison:
candidate median 64,757 Put/s is below main's minimum 65,478.3 Put/s.
It is also 2.2% below main's median of 66,222 Put/s. This does not prove
which allocation or CPU cost caused the loss. There was no tuning run.

[Bench run 38996627100](https://github.com/djosh34/tigerstore/actions/runs/38996627100)
passed all 60 measurements, five repetitions of two value sets, two load
modes and three stores. A successful workflow means the measurements
completed, not that the performance gate passed.

- Candidate, generator and scripts: `5f1e172ff4ca19faf5433e65910e78b573c414d2`.
- Pinned main: `0b9cf0c2b1fd478012f5cb7482d2f700a5090a19`.
- Reference: etcd v3.7.2.
- Runner: four Neoverse-N2 cores, 16.72 GB RAM, one ext4 Azure disk,
  Linux 6.17.0-1022-azure, Go 1.27.2. The bench enabled disk barriers.
- Real corpus: 7,702 Kubernetes 1.37 stored values, 10,492,337 bytes,
  mean 1,362.29 bytes. No values were excluded. The paired random case
  used exactly these lengths, independently generated per connection.
- Each cluster had three TLS replicas, each pinned to one core, and a
  generator on the fourth core. Each replica had GOMEMLIMIT 3 GiB.
  Clusters were fresh per measurement. Target order alternated.

## Measurements

Each cell is minimum / median / maximum over five repetitions. Each
operation is one Put RPC. Both byte columns sum all three replicas.

| Values | Store | Mode | Put/s | p99 ms | Physical B/Put | WAL B/Put |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| real | etcd | max | 3329.700 / 3678.500 / 3884.700 | 786.432 / 840.958 / 945.816 | 11659.542 / 11838.557 / 12277.175 | not sampled |
| real | etcd | rate | 1827.200 / 1838.200 / 1851.700 | 14.713 / 31.916 / 34.996 | 23282.877 / 23807.065 / 24278.753 | not sampled |
| real | main | max | 59635.000 / 64152.000 / 69860.400 | 38.404 / 41.026 / 48.628 | 4517.265 / 4525.278 / 4561.193 | 4274.811 / 4281.022 / 4284.946 |
| real | main | rate | 1827.200 / 1835.900 / 1848.800 | 3.564 / 3.629 / 5.194 | 29268.423 / 29785.458 / 30959.860 | 4281.709 / 4346.172 / 4396.479 |
| real | candidate | max | 69745.100 / 71608.400 / 72022.100 | 39.977 / 40.501 / 47.055 | 809.678 / 812.331 / 829.800 | 558.414 / 559.380 / 560.833 |
| real | candidate | rate | 1826.900 / 1835.700 / 1848.900 | 3.744 / 3.908 / 5.521 | 23707.649 / 24390.795 / 25446.139 | 2085.696 / 2093.681 / 2116.881 |
| random | etcd | max | 3688.200 / 3851.000 / 4054.000 | 736.100 / 769.655 / 857.735 | 11520.227 / 11751.795 / 11859.139 | not sampled |
| random | etcd | rate | 1907.300 / 1918.700 / 1928.000 | 41.288 / 48.628 / 53.871 | 19604.472 / 21181.488 / 21640.959 | not sampled |
| random | main | max | 65478.300 / 66222.000 / 66943.800 | 38.666 / 39.453 / 43.647 | 4518.046 / 4519.301 / 4535.926 | 4274.625 / 4276.425 / 4284.090 |
| random | main | rate | 1910.100 / 1917.900 / 1929.100 | 4.243 / 4.440 / 4.440 | 29162.990 / 31432.426 / 32652.632 | 4274.679 / 4343.611 / 4383.380 |
| random | candidate | max | 62359.800 / 64757.000 / 67053.000 | 38.404 / 40.239 / 64.618 | 4518.966 / 4522.390 / 4535.190 | 4277.383 / 4279.123 / 4283.119 |
| random | candidate | rate | 1910.000 / 1918.200 / 1928.700 | 3.940 / 4.407 / 5.194 | 29401.217 / 31524.930 / 32064.857 | 4273.951 / 4346.987 / 4380.680 |

At maximum load, real-value median throughput rose 11.6%, WAL bytes fell
86.9%, and physical writes fell 82.0%. Random bytes did not shrink.
The random maximum-throughput row is the only row that fails the stated
median-versus-baseline-extremes rule. Some p99 medians rose within that
envelope. In particular, real-value rate p99 rose from 3.629 to 3.908 ms.

WAL bytes come from each replica's `etcd_disk_wal_write_bytes_total`
counter at the measurement boundaries. They include record framing and
hard state, but not checkpoints, cleaning packs or filesystem overhead.
The three scrapes are sequential, not atomic. Physical writes come from
`/proc/diskstats`, including the filesystem journal and other writes on
that device. Low-rate physical bytes per Put include frequent small
syncs. They are not the compression ratio of a stored entry.

Rate runs offer half etcd's median maximum throughput: 1,839 Put/s for
real values and 1,925 Put/s for random values. They are low-load latency
measurements, not near-saturation latency. Maximum runs use 6 connections
with 256 streams each. Every run has 3 seconds of warmup and a 10-second
measurement window. Original JSON, logs and the aggregate report are in
the run's artifact for 30 days.

## Scope and checks

This is log-only compression. Peer traffic and the memory window stay
raw. It uses format 4, the existing cluster-version activation protocol,
pure-Go zstd level 1 on the disk writer, a quick incompressibility sample
and raw fallback. Cold reads decode on helpers. Cleaning preserves
decoded offsets. The method is in [compression-experiment.md](compression-experiment.md).

The corpus is a new live snapshot of #135's synthetic application fleet.
It is not a production trace or the old mixed-k8s histogram. Random
values repeat when a connection samples the same corpus template again.
The run measures hot Put traffic, not data beyond RAM. Since the write
gate already fails, no memory-limited cold point-read, LIST or watch
prev_kv performance comparison was run. Those remain unproved, not
passing. Nothing from this branch should land as the compression feature.

[Checks 38997381337](https://github.com/djosh34/tigerstore/actions/runs/38997381337)
passed on `89db2f0a`: WAL, core, node and load-generator tests under
`-short -race`, vet and a cgo-disabled server build. The disabled-code
control fails on the intended assertion, storing 45,003 bytes of 45,003
instead of compressing. Tests cover gating, raw fallback, corruption,
reopen and cleaning after compressed originals are removed. The real
concurrent-cleaner and metadata recovery fixtures pass too.

Earlier failures remain recorded:

- [38996370130](https://github.com/djosh34/tigerstore/actions/runs/38996370130),
  `68f76d82`: the first implementation shared a zstd encoder across
  synctest bubbles, which Go rejects. `7e34241f` makes codecs local to each
  WAL. The same run invoked external-etcd load-generator tests without
  their binary; the branch check now uses their documented `-short` mode.
- [38996617405](https://github.com/djosh34/tigerstore/actions/runs/38996617405),
  `5f1e172f`: two cleaning fixtures' repeated-zero padding compressed
  enough to avoid the required removals. `58cad426` uses incompressible
  churn while keeping cold values compressible and every removal and
  recovery assertion. It also applies formatting generated on Actions.

The benchmark ran once. Later changes are test fixtures, formatting and
documentation, not a performance retune. All execution was on Actions.
No agents, PR, merge or issue closure. No new defect on main was proved.
