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
