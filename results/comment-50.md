## Unpinned multicore results

tigerstore's throughput advantage survives unrestricted multicore. It won all 27 requested median-throughput comparisons. Its advantage was 3.64 to 11.98 times on GitHub arm64, 1.98 to 11.00 times on GitHub amd64, and 1.56 to 10.43 times on the memory-capped host.

A tigerstore node can use more than one core. Its Range leader averaged 2.41 cores on the four-vCPU amd64 runner and 3.32 cores on this eight-vCPU host. Etcd also used multiple cores, including 3.47 cores for its runner Range leader and 4.99 on the host. Neither store had a GOMAXPROCS setting or CPU pinning.

Higher throughput did not mean a better tail in every case. At 1024 clients on the host, tigerstore Put p99 was 286 ms versus etcd's 79 ms. The host reached its memory cap. These host results do not describe an uncapped eight-core machine.

[Primary GitHub run](https://github.com/djosh34/tigerstore/actions/runs/38418222318), all six jobs passed. [Higher-concurrency probe](https://github.com/djosh34/tigerstore/actions/runs/38419612456), both Range jobs passed; the four write jobs stopped on etcd errors at 8192 clients. [Raw measurements, scripts and runner records](https://github.com/djosh34/tigerstore/tree/exp/multicore/results) remain on `exp/multicore`. Nothing is merged.

### Method

- Server source was main `36eda3cd874e5a5aa81aca41fdf70c7b9373bec2`, including the PR #86 generator. No server, internal package or module edits. The generator gained machine CPU counters. Etcd was the downloaded, SHA256-checked stock v3.7.2 release binary. Tigerstore used Go 1.27.2; the release etcd binary used Go 1.26.8.
- Each run started a fresh three-node cluster with client and peer mTLS. The generator ran on the same machine and targeted the leader. Etcd kept safe persistence defaults, with bench/'s 8 GiB quota. No unsafe fsync option. GitHub disks used the bench/ `remount,barrier=1` step. Host data was on the local XFS disk under `/tmp`, not the virtiofs checkout.
- Put and successful mod-revision CAS Txn used 1 KiB values and 10,000 keys. Range was linearizable, point-only, and checked the preloaded 1 KiB value. Clients means concurrently outstanding RPC slots, multiplexed over eight HTTP/2 connections, not one TCP connection per client. Closed loop, 64/256/1024 slots, three repetitions. Store order reversed in repetition 2. All 162 accepted primary runs had zero request errors, a stable leader and term, and valid CPU samples.
- Runners used 3 seconds of warmup and 10 seconds of measurement. The host used 1 and 3 seconds for both stores to limit retained history. All local Go builds used `GOFLAGS=-p=2 GOMEMLIMIT=2GiB` inside a 3 GiB scope. Servers and generator had those settings unset.
- GitHub arm64 exposed four Neoverse-N2 cores. GitHub amd64 exposed four vCPUs, reported as two EPYC 9V74 cores with two threads each. Each workload got a separate runner, but both stores ran back to back within that job. The primary amd64 Put job had an NVMe-backed root; Txn and Range had SCSI-backed roots. Do not treat differences between workload jobs as a controlled disk or architecture comparison.

The host ran inside `systemd-run --user --scope -p MemoryMax=3G -p MemorySwapMax=0`, without a CPU quota. It peaked at 3.00 GiB, recorded 11,686 memory-cap events, and had no OOM kills or scope swap use. Before each run, five consecutive samples had to show less than 0.8 background core in total and no process above 0.4 core. Ten noisy measurement windows were excluded and retried; their raw results remain under `host/excluded`. Other services remained on the host. This gate is evidence against sustained competing heavy work, not proof that no short competing activity escaped it. Treat the host numbers as memory-capped, background-gated results.

### Where scaling flattens

At the requested points, etcd Range was already nearly flat between 256 and 1024 clients on all three venues. Tigerstore Range still gained throughput through 1024 clients.

The separate runner sweep, shown below, compared 1024, 4096 and 8192 clients on the same runner within each workload. Tigerstore writes gained only 3 to 12 percent from 1024 to 4096 clients. Range gained only 3 to 6 percent from 4096 to 8192. These are observed throughput plateaus with more concurrent requests in this setup, not a measured maximum core count per node. The host's limit was not found beyond 1024 clients.

At 8192 clients, etcd Put and Txn returned `etcdserver: too many requests` on both architectures. Each affected job stopped on that first failed repetition, before tigerstore's 8192-client write runs. Those points are invalid, not throughput comparisons. Every reported supplementary median has three valid repetitions.

The [committer loop](https://github.com/djosh34/tigerstore/blob/36eda3cd874e5a5aa81aca41fdf70c7b9373bec2/internal/node/node.go#L302-L319) is the only goroutine that touches tigerstore's core. Disk work and connection handling can run in parallel. That serial core ownership is code evidence; blaming it for a particular measured plateau is a guess. I did not collect CPU profiles, vary connection count, move the generator off the machine, or run a matched core-count sweep. These measurements prove the unpinned throughput advantage and per-node parallel CPU use here. They do not prove linear scaling with cores, identify the exact bottleneck, or establish a general production or crash-durability advantage.

### Reading the tables

All entries are medians of three per-run statistics. Throughput also shows the lowest and highest run. Quantiles are not pooled. L is the leader, the two F entries are individual followers, and G is the generator. CPU seconds/op is user plus system CPU time divided by successful operations. Average cores is CPU seconds divided by measurement wall time.

Machine CPU includes all processes and kernel work. Its busy seconds and percentage come from `/proc/stat`; idle, I/O wait and steal are excluded from busy. Raw files retain I/O wait, steal, machine CPU seconds and sampling duration. Independent medians and VM CPU accounting need not sum exactly. Latency is closed-loop send-to-completion latency at the stated concurrency, not latency at the same offered request rate.

### GitHub ubuntu-24.04-arm, 4 cores

| Workload | Clients | Store | ops/s, median and min to max | p50 / p99 ms | CPU seconds/op, L / F / F / G | Average cores, L / F / F / G | Machine cores / busy % |
|---|---:|---|---|---|---|---|---|
| put | 64 | etcd | 9,410, 9,127 to 9,465 | 6.144 / 18.285 | 2.04e-04 / 4.43e-05 / 4.54e-05 / 3.52e-05 | 1.91 / 0.42 / 0.43 / 0.33 | 3.21 / 81.2% |
| put | 64 | tigerstore | 34,461, 32,028 to 34,876 | 1.831 / 3.023 | 3.08e-05 / 1.41e-05 / 1.40e-05 / 8.84e-06 | 1.04 / 0.48 / 0.47 / 0.30 | 2.57 / 65.1% |
| put | 256 | etcd | 13,507, 12,827 to 13,507 | 18.153 / 36.307 | 1.69e-04 / 2.92e-05 / 2.98e-05 / 2.55e-05 | 2.28 / 0.39 / 0.39 / 0.34 | 3.46 / 87.4% |
| put | 256 | tigerstore | 66,622, 65,068 to 67,483 | 3.809 / 5.980 | 1.47e-05 / 5.81e-06 / 5.70e-06 / 4.38e-06 | 0.99 / 0.39 / 0.38 / 0.29 | 2.10 / 55.5% |
| put | 1024 | etcd | 15,712, 15,198 to 15,995 | 63.046 / 112.460 | 1.64e-04 / 2.42e-05 / 2.48e-05 / 2.10e-05 | 2.55 / 0.38 / 0.39 / 0.33 | 3.66 / 92.3% |
| put | 1024 | tigerstore | 91,977, 89,588 to 92,742 | 11.174 / 16.482 | 9.52e-06 / 3.36e-06 / 3.38e-06 / 2.90e-06 | 0.87 / 0.31 / 0.31 / 0.27 | 1.83 / 45.8% |
| txn | 64 | etcd | 8,029, 7,084 to 8,066 | 7.291 / 18.285 | 2.47e-04 / 5.70e-05 / 5.68e-05 / 3.92e-05 | 1.97 / 0.46 / 0.44 / 0.31 | 3.26 / 83.1% |
| txn | 64 | tigerstore | 29,224, 29,076 to 30,137 | 2.085 / 3.514 | 3.85e-05 / 1.67e-05 / 1.64e-05 / 9.62e-06 | 1.13 / 0.49 / 0.48 / 0.28 | 2.59 / 67.5% |
| txn | 256 | etcd | 11,146, 10,934 to 11,163 | 22.217 / 40.239 | 2.11e-04 / 3.93e-05 / 3.94e-05 / 3.01e-05 | 2.34 / 0.43 / 0.44 / 0.33 | 3.60 / 91.0% |
| txn | 256 | tigerstore | 60,424, 60,120 to 60,698 | 4.104 / 6.996 | 2.00e-05 / 7.70e-06 / 7.56e-06 / 4.77e-06 | 1.21 / 0.47 / 0.46 / 0.29 | 2.66 / 66.1% |
| txn | 1024 | etcd | 12,350, 12,104 to 12,458 | 79.954 / 146.276 | 2.09e-04 / 3.39e-05 / 3.39e-05 / 2.54e-05 | 2.58 / 0.42 / 0.42 / 0.31 | 3.76 / 94.5% |
| txn | 1024 | tigerstore | 78,864, 77,740 to 78,981 | 12.812 / 20.251 | 1.39e-05 / 4.81e-06 / 4.73e-06 / 3.16e-06 | 1.09 / 0.38 / 0.37 / 0.25 | 2.14 / 54.3% |
| range | 64 | etcd | 19,501, 18,989 to 20,179 | 3.072 / 7.520 | 1.52e-04 / 7.85e-06 / 7.59e-06 / 2.49e-05 | 2.98 / 0.16 / 0.15 / 0.50 | 3.80 / 95.7% |
| range | 64 | tigerstore | 97,244, 96,801 to 99,150 | 0.616 / 1.470 | 1.84e-05 / 3.17e-06 / 3.43e-06 / 7.67e-06 | 1.80 / 0.31 / 0.34 / 0.74 | 3.18 / 81.7% |
| range | 256 | etcd | 24,684, 24,408 to 25,232 | 9.929 / 19.595 | 1.33e-04 / 2.29e-06 / 2.22e-06 / 2.15e-05 | 3.28 / 0.06 / 0.06 / 0.54 | 3.93 / 98.6% |
| range | 256 | tigerstore | 199,004, 196,815 to 201,259 | 1.167 / 2.843 | 1.03e-05 / 1.01e-06 / 1.12e-06 / 4.34e-06 | 2.05 / 0.20 / 0.22 / 0.87 | 3.33 / 84.5% |
| range | 1024 | etcd | 24,810, 24,306 to 25,412 | 39.715 / 78.905 | 1.38e-04 / 7.87e-07 / 8.06e-07 / 1.98e-05 | 3.41 / 0.02 / 0.02 / 0.50 | 3.96 / 99.1% |
| range | 1024 | tigerstore | 297,313, 296,835 to 302,013 | 3.138 / 7.029 | 7.68e-06 / 3.18e-07 / 3.74e-07 / 3.31e-06 | 2.29 / 0.10 / 0.11 / 0.99 | 3.46 / 87.6% |

### GitHub ubuntu-24.04, amd64, 4 vCPUs

| Workload | Clients | Store | ops/s, median and min to max | p50 / p99 ms | CPU seconds/op, L / F / F / G | Average cores, L / F / F / G | Machine cores / busy % |
|---|---:|---|---|---|---|---|---|
| put | 64 | etcd | 5,146, 3,371 to 6,453 | 5.816 / 144.179 | 1.89e-04 / 4.68e-05 / 4.70e-05 / 2.93e-05 | 0.96 / 0.24 / 0.24 / 0.15 | 1.62 / 41.2% |
| put | 64 | tigerstore | 16,068, 9,316 to 18,006 | 2.077 / 60.948 | 2.74e-05 / 1.19e-05 / 1.18e-05 / 8.12e-06 | 0.44 / 0.19 / 0.19 / 0.13 | 1.01 / 27.0% |
| put | 256 | etcd | 7,709, 7,090 to 8,063 | 19.595 / 243.794 | 1.76e-04 / 3.46e-05 / 3.50e-05 / 2.19e-05 | 1.38 / 0.27 / 0.27 / 0.17 | 2.11 / 53.4% |
| put | 256 | tigerstore | 15,280, 14,235 to 40,084 | 4.506 / 211.288 | 1.46e-05 / 5.89e-06 / 5.82e-06 / 4.48e-06 | 0.22 / 0.09 / 0.09 / 0.07 | 0.44 / 11.3% |
| put | 1024 | etcd | 9,534, 8,926 to 10,115 | 77.332 / 418.382 | 1.77e-04 / 3.05e-05 / 2.98e-05 / 1.91e-05 | 1.65 / 0.28 / 0.28 / 0.18 | 2.41 / 60.8% |
| put | 1024 | tigerstore | 34,511, 27,388 to 40,823 | 17.760 / 244.842 | 1.01e-05 / 3.63e-06 / 3.53e-06 / 3.27e-06 | 0.34 / 0.12 / 0.12 / 0.11 | 0.64 / 16.4% |
| txn | 64 | etcd | 6,816, 6,790 to 6,874 | 8.946 / 17.891 | 3.23e-04 / 8.13e-05 / 7.99e-05 / 5.05e-05 | 2.20 / 0.55 / 0.55 / 0.34 | 3.73 / 95.1% |
| txn | 64 | tigerstore | 28,418, 27,876 to 28,726 | 2.220 / 3.695 | 4.47e-05 / 2.02e-05 / 1.98e-05 / 1.26e-05 | 1.28 / 0.57 / 0.56 / 0.36 | 3.05 / 81.1% |
| txn | 256 | etcd | 8,192, 8,063 to 8,284 | 30.343 / 54.657 | 2.96e-04 / 6.40e-05 / 6.38e-05 / 4.16e-05 | 2.40 / 0.52 / 0.53 / 0.34 | 3.85 / 97.2% |
| txn | 256 | tigerstore | 56,183, 55,389 to 56,964 | 4.473 / 8.143 | 2.65e-05 / 1.07e-05 / 1.05e-05 / 6.74e-06 | 1.49 / 0.60 / 0.60 / 0.38 | 3.21 / 83.2% |
| txn | 1024 | etcd | 8,718, 8,710 to 8,769 | 113.508 / 208.142 | 2.94e-04 / 5.79e-05 / 5.82e-05 / 3.37e-05 | 2.56 / 0.50 / 0.51 / 0.29 | 3.90 / 98.1% |
| txn | 1024 | tigerstore | 78,714, 77,987 to 79,080 | 12.747 / 22.741 | 2.05e-05 / 7.42e-06 / 7.45e-06 / 5.31e-06 | 1.62 / 0.59 / 0.58 / 0.42 | 3.23 / 82.9% |
| range | 64 | etcd | 23,337, 23,251 to 23,409 | 2.548 / 6.308 | 1.26e-04 / 6.15e-06 / 6.04e-06 / 2.15e-05 | 2.95 / 0.14 / 0.14 / 0.50 | 3.71 / 94.2% |
| range | 64 | tigerstore | 123,364, 121,472 to 124,129 | 0.476 / 1.307 | 1.47e-05 / 2.55e-06 / 2.88e-06 / 6.71e-06 | 1.81 / 0.31 / 0.35 / 0.83 | 3.24 / 84.0% |
| range | 256 | etcd | 28,304, 28,095 to 28,378 | 8.749 / 17.367 | 1.16e-04 / 1.96e-06 / 1.87e-06 / 1.77e-05 | 3.29 / 0.06 / 0.05 / 0.50 | 3.90 / 97.9% |
| range | 256 | tigerstore | 222,394, 222,232 to 226,275 | 1.010 / 2.925 | 9.30e-06 / 8.09e-07 / 9.40e-07 / 4.05e-06 | 2.07 / 0.18 / 0.21 / 0.90 | 3.29 / 84.7% |
| range | 1024 | etcd | 28,237, 27,814 to 28,319 | 35.521 / 67.895 | 1.23e-04 / 6.73e-07 / 6.36e-07 / 1.55e-05 | 3.47 / 0.02 / 0.02 / 0.44 | 3.95 / 98.9% |
| range | 1024 | tigerstore | 310,489, 307,505 to 311,823 | 2.925 / 7.127 | 7.80e-06 / 2.60e-07 / 3.05e-07 / 3.28e-06 | 2.41 / 0.08 / 0.09 / 1.02 | 3.57 / 90.8% |

### Host arm64, 8 vCPUs, 11 GiB RAM, 3 GiB scope

| Workload | Clients | Store | ops/s, median and min to max | p50 / p99 ms | CPU seconds/op, L / F / F / G | Average cores, L / F / F / G | Machine cores / busy % |
|---|---:|---|---|---|---|---|---|
| put | 64 | etcd | 12,534, 11,034 to 13,700 | 4.243 / 24.314 | 1.54e-04 / 3.23e-05 / 3.08e-05 / 3.39e-05 | 1.93 / 0.41 / 0.39 / 0.42 | 3.43 / 44.9% |
| put | 64 | tigerstore | 19,525, 18,309 to 25,299 | 3.187 / 7.225 | 2.36e-05 / 9.39e-06 / 9.39e-06 / 7.72e-06 | 0.46 / 0.18 / 0.18 / 0.15 | 1.35 / 17.9% |
| put | 256 | etcd | 21,173, 20,679 to 22,114 | 9.142 / 51.249 | 1.29e-04 / 2.08e-05 / 2.13e-05 / 2.49e-05 | 2.75 / 0.44 / 0.46 / 0.54 | 4.59 / 59.0% |
| put | 256 | tigerstore | 55,496, 36,449 to 58,365 | 4.342 / 11.502 | 1.18e-05 / 4.62e-06 / 4.44e-06 / 3.61e-06 | 0.65 / 0.25 / 0.25 / 0.20 | 1.71 / 21.9% |
| put | 1024 | etcd | 30,182, 27,219 to 31,461 | 31.523 / 78.905 | 1.25e-04 / 1.78e-05 / 1.80e-05 / 1.76e-05 | 3.60 / 0.53 / 0.54 / 0.53 | 5.23 / 68.4% |
| put | 1024 | tigerstore | 74,287, 51,875 to 88,494 | 7.946 / 286.261 | 7.95e-06 / 2.98e-06 / 2.87e-06 / 2.39e-06 | 0.59 / 0.22 / 0.21 / 0.18 | 1.54 / 19.7% |
| txn | 64 | etcd | 14,004, 13,613 to 14,429 | 4.006 / 14.123 | 1.66e-04 / 3.52e-05 / 3.49e-05 / 3.44e-05 | 2.32 / 0.49 / 0.48 / 0.48 | 4.04 / 52.8% |
| txn | 64 | tigerstore | 23,163, 22,259 to 23,563 | 2.793 / 5.784 | 2.79e-05 / 1.06e-05 / 1.06e-05 / 7.48e-06 | 0.65 / 0.24 / 0.25 / 0.17 | 1.67 / 21.6% |
| txn | 256 | etcd | 22,087, 16,725 to 22,943 | 10.125 / 32.702 | 1.49e-04 / 2.58e-05 / 2.66e-05 / 2.60e-05 | 3.25 / 0.57 / 0.57 / 0.57 | 5.13 / 66.7% |
| txn | 256 | tigerstore | 42,763, 32,220 to 59,525 | 3.908 / 24.183 | 1.53e-05 / 5.53e-06 / 5.77e-06 / 3.55e-06 | 0.65 / 0.24 / 0.25 / 0.15 | 1.89 / 24.2% |
| txn | 1024 | etcd | 23,029, 22,668 to 25,802 | 37.618 / 92.537 | 1.49e-04 / 2.36e-05 / 2.35e-05 / 1.96e-05 | 3.42 / 0.54 / 0.54 / 0.47 | 5.08 / 64.8% |
| txn | 1024 | tigerstore | 98,605, 90,789 to 116,951 | 9.339 / 21.692 | 1.08e-05 / 3.82e-06 / 3.97e-06 / 2.37e-06 | 1.09 / 0.39 / 0.39 / 0.24 | 2.33 / 29.6% |
| range | 64 | etcd | 41,095, 40,309 to 45,196 | 1.307 / 5.095 | 1.00e-04 / 4.06e-06 / 3.91e-06 / 2.90e-05 | 4.12 / 0.17 / 0.17 / 1.19 | 6.30 / 80.2% |
| range | 64 | tigerstore | 170,299, 167,620 to 172,886 | 0.359 / 0.723 | 1.36e-05 / 2.56e-06 / 2.64e-06 / 6.33e-06 | 2.32 / 0.44 / 0.45 / 1.08 | 4.11 / 56.7% |
| range | 256 | etcd | 53,959, 47,203 to 56,006 | 4.309 / 11.502 | 9.06e-05 / 1.31e-06 / 1.30e-06 / 2.22e-05 | 4.89 / 0.07 / 0.07 / 1.24 | 6.37 / 81.0% |
| range | 256 | tigerstore | 386,245, 296,711 to 386,812 | 0.604 / 1.438 | 7.37e-06 / 7.51e-07 / 7.76e-07 / 3.16e-06 | 2.84 / 0.28 / 0.29 / 1.22 | 4.49 / 60.3% |
| range | 1024 | etcd | 53,034, 51,028 to 57,385 | 17.760 / 46.268 | 9.40e-05 / 5.66e-07 / 5.23e-07 / 1.75e-05 | 4.99 / 0.03 / 0.03 / 0.95 | 6.27 / 81.5% |
| range | 1024 | tigerstore | 553,347, 548,398 to 556,572 | 1.642 / 3.891 | 6.05e-06 / 2.17e-07 / 2.19e-07 / 2.42e-06 | 3.32 / 0.12 / 0.12 / 1.34 | 4.88 / 63.3% |


### Supplementary runner sweep

| Runner | Workload | etcd ops/s at 1024 / 4096 / 8192 | tigerstore ops/s at 1024 / 4096 / 8192 | Tiger leader cores at 1024 / 4096 / 8192 |
|---|---|---|---|---|
| arm64 | put | 13,200 / 12,974 / invalid | 85,638 / 92,071 / not run | 0.92 / 0.81 / not run |
| arm64 | txn | 12,550 / 11,964 / invalid | 66,137 / 71,416 / not run | 0.91 / 0.86 / not run |
| arm64 | range | 24,179 / 21,302 / 20,278 | 292,284 / 358,380 / 378,429 | 2.28 / 2.43 / 2.45 |
| amd64 | put | 12,725 / 12,252 / invalid | 59,858 / 61,702 / not run | 0.73 / 0.71 / not run |
| amd64 | txn | 9,779 / 9,213 / invalid | 45,469 / 50,991 / not run | 0.87 / 0.91 / not run |
| amd64 | range | 21,644 / 19,025 / 18,279 | 241,542 / 263,566 / 271,541 | 2.47 / 2.56 / 2.57 |
