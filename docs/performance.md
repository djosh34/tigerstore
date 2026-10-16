# Performance

What a request costs in tigerstore, the ceilings that follow from that and from the disk, and how tigerstore compares with etcd v3.7.2. Measured at the end of [At the measured speed ceilings](https://github.com/djosh34/tigerstore/issues/146), on `main` at `ac8c7a70`. Every number here comes from a run that can be repeated; the commands are in each section.

## How it is measured

The `bench` workflow (`.github/workflows/bench.yml`, started by hand) runs on a free `ubuntu-24.04-arm` runner: 4 Neoverse-N2 cores, 16 GB of RAM, Linux 6.17, one ext4 disk remounted with write barriers on. Each store runs as 3 nodes on 127.0.0.1 with client and peer TLS and client certificates, a fresh data directory per run, the leader pinned to CPU 0, the followers to CPUs 1 and 2, and the generator (`cmd/loadgen`) to CPU 3. Every target runs 5 times per benchmark, in alternating order. Statistics are min / median / max over the repetitions. The design document's Benchmarks section has the details of the generator and the workloads.

- **max**: 6 connections with 256 streams each, every stream busy, against the leader.
- **rate**: open-loop Poisson arrivals at half of etcd's median max, the same load for every store; latency counts from the scheduled send time.
- Keys look like Kubernetes keys (`/registry/pods/bench-007/web-...`), 10,000 of them, with 1 KiB or 10 KiB random values. A LIST asks for the first 500 keys of a 1,000-key namespace; watch adds one watcher of the written prefix per run at 10 KiB, three at 1 KiB; mixed-k8s replays a Kubernetes cluster's mix of reads, writes and watches over 1,000,000 keys with the value sizes measured on #50.
- The generator encodes request headers as grpc-go, which kube-apiserver uses, does: each field goes into the HPACK table the first time and is sent as a one-byte reference after that. Numbers measured before that change (#191) are not comparable with these.

The runners are different VMs, and the same build measures up to 20% apart on two of them. Comparisons are therefore always paired: the stores and builds being compared run back to back in one job.

## The cost of each step of a request

`bench/profile.sh` (benchmark `profile`) runs a workload at max with all three nodes on tmpfs, so no step waits for the disk, takes a CPU profile of the leader, which has one CPU, and splits the leader's CPU per operation by the goroutine and calls each step runs on. Microseconds of leader CPU per operation, run [38845596998](https://github.com/djosh34/tigerstore/actions/runs/38845596998):

| Step | Put 1 KiB | CAS Txn 1 KiB | point Range 1 KiB | LIST 500 × 1 KiB |
|---|---:|---:|---:|---:|
| Client TLS read and decrypt | 0.52 | 0.61 | 0.14 | — |
| HTTP/2 frames, HPACK and the request body | 1.50 | 1.29 | 1.03 | — |
| Request decode and batch | 0.39 | 1.09 | 0.40 | — |
| Committer: Raft and the rest of its loop | 0.30 | 0.35 | 0.03 | 2.9 |
| Committer: checks and queue | 0.18 | 0.79 | 0.12 | — |
| Committer: entry encode | 0.10 | 0.22 | 0 | — |
| Committer: apply, and reads with their values | 1.33 | 2.35 | 1.25 | 535.6 |
| Committer: completions | 0.15 | 0.20 | 0.16 | 4.4 |
| Log write and sync | 0.34 | 0.42 | 0 | — |
| Peer send (marshal, TLS, write) to both followers | 1.52 | 1.51 | 0.16 | 5.8 |
| Peer receive | 0.08 | 0.09 | 0.03 | 1.5 |
| Reply encode | 0.36 | 0.40 | 0.82 | 145.9 |
| Reply TLS encrypt and write | 0.32 | 0.39 | 0.91 | 413.0 |
| Garbage collection | 0.21 | 0.53 | 0.34 | 26.3 |
| Everything else | 0.04 | 0.10 | 0.06 | 7.3 |
| **Total** | **7.35** | **10.35** | **5.44** | **1,142.7** |
| Rate at one leader CPU | 135,592/s | 96,441/s | 183,773/s | 875/s |

— : at 875 LISTs/s, below what the profile can tell apart (one sample is 10 ms of 7.8 s).

A Put spends a third of its leader CPU reading and decoding the request, a fifth sending the entry to the two followers and a fifth applying it. A CAS costs 3 µs more than a Put: decoding and checking the transaction, and the compare in apply. A LIST is the committer walking the index and copying 500 values, then the writer encoding and sealing more than 500 KiB.

## Ceilings

**Disk.** `cmd/diskbench` measures the runner's disk as #134 did: O_DIRECT writes into a file already written, each followed by fdatasync, 256 KiB to 4 MiB at a time. It took 344.5 MiB/s at most in the Put job and 343.3 MiB/s in the CAS job. `loadgen -disk` counts the device bytes written per operation from `/proc/diskstats`, the ext4 journal and checkpoints included. With three nodes on the one disk a 1 KiB Put writes 3,519 bytes, so the disk cannot take more than 102,670 Puts/s; a CAS writes 3,918 bytes, so 91,892/s. With only the leader on the disk, a Put writes 1,180 bytes there and a CAS 1,288.

**CPU.** The same build with all three nodes on tmpfs is the CPU reference: 129,689 Puts/s and 91,220 CAS/s, with the leader at one full CPU.

**Against the lower of the two**, in the same job ([38845596998](https://github.com/djosh34/tigerstore/actions/runs/38845596998), 5 repetitions, medians):

| Benchmark | Layout | Outcome start `ca9e037b` | `main` `ac8c7a70` | Disk ceiling | CPU reference | Share | Leader CPU |
|---|---|---:|---:|---:|---:|---:|---:|
| Put 1 KiB max | 3 nodes on one disk | 90,502 | 92,178 | 102,670 | 129,689 | 89.8% | 0.77 |
| Put 1 KiB max | leader alone on the disk | 118,560 | 124,918 | 306,120 | 129,689 | 96.3% | 0.99 |
| CAS 1 KiB max | 3 nodes on one disk | 60,107 | 66,347 | 91,892 | 91,220 | 72.7% | 0.81 |
| CAS 1 KiB max | leader alone on the disk | 73,374 | 80,717 | 279,426 | 91,220 | 88.5% | 0.94 |

The target of #146, 95% of the lower number on one shared disk, is not reached on this runner: Put is at 89.8%, CAS at 72.7%. With 4 times as many requests in flight, 6 connections × 1,024 streams instead of 256 (run [38845621287](https://github.com/djosh34/tigerstore/actions/runs/38845621287), another runner, same builds):

| Benchmark | Layout | `main` | Disk ceiling | CPU reference | Share | Leader CPU |
|---|---|---:|---:|---:|---:|---:|
| Put 1 KiB max | 3 nodes on one disk | 88,107 | 105,176 | 128,461 | 83.8% | 0.74 |
| Put 1 KiB max | leader alone on the disk | 115,802 | 318,184 | 128,461 | 90.1% | |
| CAS 1 KiB max | 3 nodes on one disk | 73,548 | 95,130 | 88,312 | 83.3% | 0.88 |
| CAS 1 KiB max | leader alone on the disk | 86,259 | 286,902 | 88,312 | 97.7% | |

Where the time goes:

- **CAS: the requests in flight.** The bench keeps 1,536 requests in flight, and a CAS takes 22 ms at the median on the shared disk, so about 70,000/s is all the bench can see (Little's law); the leader is at 0.81 of its CPU and the disk below its ceiling. With 6,144 in flight, CAS rises to 83.3% of the lower number on the shared disk and to 97.7% with the leader alone. A Kubernetes cluster with many clients has more writes in flight than one bench client; the bench keeps 1,536 so its numbers stay comparable with the earlier ones.
- **Put on the shared disk: the disk as three nodes use it.** More requests in flight do not help Put: 83.8% of that runner's ceiling, with the leader at 0.74. The device writes 89.8% of what diskbench gets out of it. diskbench is one writer of 256 KiB to 4 MiB into blocks already written; the nodes are three writers, each syncing groups of a few hundred Puts into segments that grow, and every sync of a growing file also commits the ext4 journal the three share, about 5% of the bytes here ([below](#journal-bytes-with-cleaning)). Which of these costs the rest was not measured.
- **Leader alone: the leader's CPU.** With one disk per node, which the leader-alone layout stands in for, Put reaches 96.3% of the CPU reference and the leader's one CPU is the limit: 7.35 µs per Put, step by step in the table above.

## Results against etcd v3.7.2

Run [38845596998](https://github.com/djosh34/tigerstore/actions/runs/38845596998): etcd v3.7.2, tigerstore at the start of this outcome (`ca9e037b`) and at its end (`ac8c7a70`), one job per benchmark, 5 repetitions, medians. Max is throughput with every stream busy; rate p99 is at half of etcd's max.

| Benchmark | etcd max | start max | end max | etcd rate p99 ms | start rate p99 | end rate p99 | end server CPU µs/op at max |
|---|---:|---:|---:|---:|---:|---:|---:|
| put-1k | 5,216 | 90,502 | 92,178 | 20.12 | 2.65 | 2.79 | 15.0 |
| put-10k | 1,400 | 10,636 | 10,689 | 12.88 | 7.23 | 8.49 | 68.5 |
| txn-1k | 4,224 | 60,107 | 66,347 | 17.24 | 2.65 | 2.66 | 21.4 |
| txn-10k | 1,220 | 9,400 | 9,193 | 11.50 | 3.87 | 3.97 | 80.9 |
| delete-1k | 5,535 | 113,272 | 118,661 | 23.13 | 1.99 | 2.15 | 15.3 |
| delete-10k ¹ | 5,167 | 119,467 | 130,533 | 15.50 | 2.04 | 2.27 | 14.2 |
| range-1k | 6,638 | 166,310 | 173,246 | 8.04 | 1.29 | 1.29 | 6.0 |
| range-10k | 5,805 | 68,128 | 69,221 | 7.95 | 1.35 | 1.34 | 14.8 |
| list-1k | 375 | 898 | 909 | 34.47 | 6.21 | 5.42 | 1,112.9 |
| list-10k | 554 | 822 | 820 | 13.80 | 6.80 | 7.00 | 1,240.8 |
| watch-1k (puts) | 4,088 | 56,645 | 57,559 | 21.43 | 2.73 | 2.63 | 24.0 |
| watch-1k (events/s, event p99) | 12,170 | 169,837 | 173,736 | 22.09 | 2.76 | 2.70 | |
| watch-10k (puts) | 1,428 | 10,618 | 10,606 | 10.58 | 2.47 | 2.50 | 88.9 |
| watch-10k (events/s, event p99) | 1,433 | 10,618 | 10,542 | 10.52 | 2.48 | 2.53 | |
| mixed-k8s ² | 2,972 | 10,124 | 8,990 | 46.79 | 11.76 | 13.07 | 239.8 |
| mixed-k8s watch (event p99) | | | | 47.32 | 12.75 | 13.01 | |

¹ Run [38846602546](https://github.com/djosh34/tigerstore/actions/runs/38846602546), the same builds: in the run above the preload of 10,000 values of 10 KiB failed on etcd, and loadgen now preloads with 64 streams per connection.

² Max is one calibration run per store here; mixed-k8s drives all three nodes, with 1,000,000 keys of the sizes of a Kubernetes 1.37 dump. Measured with 5 runs of max for each PR of the outcome, the end was 8% slower than the start, all from #191 (run [38852160844](https://github.com/djosh34/tigerstore/actions/runs/38852160844): 8,598 against 9,380, below the minimum of 8,808, with 9% more leader CPU per operation). #191 copied the values of an answer with several keys into one buffer made with `make`, which zeroes it: one more pass over every LIST's bytes, as one large allocation, on every node and close to the memory limit. Without that buffer, mixed-k8s max is back above `main`: 8,076 against 7,344 in run [38858018781](https://github.com/djosh34/tigerstore/actions/runs/38858018781), every run of the one above every run of the other; the change is in the PR with this document.

By #146's rule (a build regresses when its median throughput is below the other's minimum or its median p99 above the other's maximum), the end is not slower than the start in throughput in this table; mixed-k8s is below. Two rate p99 values were above the start's maximum here, delete-1k (2.15 against 2.01 ms) and put-10k (8.49 against 8.04 ms); paired again in run [38850983674](https://github.com/djosh34/tigerstore/actions/runs/38850983674), both are within the start's spread: 2.48 against 2.25 to 3.32 ms, and 2.84 against 2.56 to 5.55 ms.

### etcd's own benchmark tool

The `etcd-tool` benchmark runs etcd v3.7.2's `tools/benchmark`, built from its source tag, with 100 connections and 1,000 clients and 1 KiB values, the setup of etcd's performance documentation, as a second view. Requests/s, medians of 5:

| Test | etcd | start | end |
|---|---:|---:|---:|
| put | 5,092 | 37,697 | 36,230 |
| txn-put | 4,023 | 34,823 | 34,273 |
| range, linearizable | 6,593 | 45,896 | 48,993 |
| range, serializable | 7,586 | 49,695 | 49,438 |

The tool runs on the generator's one CPU, as loadgen does, and loadgen drives the same builds 1.9 to 3.5 times as fast; for tigerstore this table says more about the tool's client than about the store.

### Kubernetes

The `kubeburner` workflow (`bench/kind`) runs kube-burner 2.8.6 on a one-node kind cluster (Kubernetes 1.37) whose API server uses a 3-node store on the same runner as external etcd, pinned as in the benchmarks: create 200 namespaces of 10 ConfigMaps, 10 Secrets and 5 Deployments, patch every ConfigMap once, then delete the namespaces and wait until they are gone, all without client rate limits (QPS and burst 100,000). Run [38853853021](https://github.com/djosh34/tigerstore/actions/runs/38853853021), 3 runs per store in alternating order, all valid, medians:

| | etcd v3.7.2 | tigerstore `ac8c7a70` |
|---|---:|---:|
| Whole run | 70.9 s | 66.7 s |
| Create | 8 s | 7 s |
| Update | 5 s | 5 s |
| Mass delete | 42 s | 38 s |

The phases take about as long with either store, since kind's API server and its namespace controller set their pace. The API server's own view of the store, `etcd_request_duration_seconds`, mean per call:

| Call | etcd v3.7.2 ms | tigerstore ms |
|---|---:|---:|
| create | 10.63 | 7.61 |
| update | 41.94 | 29.57 |
| get | 2.39 | 1.45 |
| delete | 2.59 | 2.26 |
| listStream | 1.81 | 1.09 |
| getCurrentResourceVersion | 1.80 | 0.99 |

Larger runs measure kind instead of the store: with 500 namespaces, or three patches per ConfigMap, thousands of the patches sent at once run into kube-burner's 60 s request timeout or lose their connection, with either store; a run where kube-burner logs an error counts as invalid.

### This host

One run on a different machine, labelled as such: `bench/run.sh` on the 8-CPU arm64 VM (`lima-opencode-vm`, 11 GB of RAM, xfs on a virtio disk, Linux 5.14) that ran the agents of this plan, with other agents running at the same time, `main` at `ac8c7a70` against etcd v3.7.2, 3 repetitions, the same pinning. Medians:

| Benchmark | etcd max | tigerstore max | etcd rate p99 ms | tigerstore rate p99 ms |
|---|---:|---:|---:|---:|
| put-1k | 7,670 | 100,903 | 41.03 | 4.12 |
| txn-1k | 7,086 | 101,604 | 31.78 | 3.94 |
| range-1k | 13,577 | 283,935 | 6.57 | 1.55 |
| list-1k | 597 | 1,392 | 26.94 | 14.91 |

## Journal bytes with cleaning

The bench never removes a segment, so it never reuses one. A long run with cleaning measures that ([#146](https://github.com/djosh34/tigerstore/issues/146#issuecomment-6207262793), script `exp146/journal.sh` on branch `exp/146-journal`): 3 nodes on the runner disk with a 3 GiB disk budget, Put of 1 KiB values over 200,000 keys at 15,000/s for 600 s, compaction every 10 s. Per Put across the nodes, with the journal counted from the kernel's `jbd2_run_stats` tracepoint:

| | Growing segments, deleted on removal | Reused removed segments (#190) |
|---|---:|---:|
| Device bytes | 9,690 to 9,932 | 9,052 to 9,079 |
| Log records | 3,285 to 3,289 | 3,299 |
| Packs | 284 to 289 | 293 to 294 |
| ext4 journal | 4,632 to 4,862 | 3,915 to 3,953 |
| Journal commits | 0.194 to 0.199 | 0.143 to 0.144 |

## What is left

- Per request on the way in: the stream object and the copy of the body into a buffer of its own, about 4% and 7% of a Put's leader CPU.
- TLS: sealing the reply is 36% of a LIST's leader CPU, and Go's `crypto/tls` writes each record with its own system call (collecting them under the socket made LIST slower, #110).
- The CAS request is decoded into a protobuf tree twice, at admission and at apply (9.4% of its leader CPU in the profile).
- On one shared disk: the requests in flight for CAS, and for Put the part of the disk diskbench gets and three nodes do not, above.
