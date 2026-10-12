# Kubernetes compression pre-work for #50 and #52

Two free `ubuntu-24.04-arm` jobs passed. Nothing merged.

- [Experiment branch](https://github.com/djosh34/tigerstore/tree/exp/pre-50-compress).
- Reported run [38319776822](https://github.com/djosh34/tigerstore/actions/runs/38319776822), measured code [`8dbff21`](https://github.com/djosh34/tigerstore/tree/8dbff21670d0fd9d43bc24804a68ed7203ceaf9b/experiments/compress50), job duration 5m39s.
- Earlier successful run [38319154255](https://github.com/djosh34/tigerstore/actions/runs/38319154255), code `ef1add6`, duration 5m18s. It used the same measurement method before lint and timeout cleanup, but a different live snapshot. The tables below use only the reported run.
- [Archived aggregate CSV, versions and workload counters](https://github.com/djosh34/tigerstore/tree/exp/pre-50-compress/evidence/compress50). Each run has 1,550 timing rows, 310 configurations and five repetitions per configuration. Raw values and trained dictionaries were never uploaded or committed. They can contain cluster credentials and were deleted with the disposable runner's temporary files.

Read #46, #1, #50, #52 and `docs/design.md` on main `9c1b938` before the experiment.

## Versions and commands

Runner image `ubuntu24-arm64 20261004.142.1`, Linux `6.17.0-1022-azure`, four Neoverse-N2 cores and 16,722,010,112 bytes RAM. Compression used CPU 0 only, `taskset -c 0`, `GOMAXPROCS=1`. The script deleted kind before timing.

- kind `v0.33.0`, built with Go `1.26.7`.
- Node image `kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5`.
- Kubernetes server `v1.37.0`, commit `f54c212e3a2f75d674b717a9b29052b20b60aefc`; kubectl `v1.37.1`, commit `f78e722310e50bcaca9276be22276d9e91d91308`.
- Stock kind etcd image `registry.k8s.io/etcd:3.7.0-0`, server `3.7.0`, commit `4d71f7c`, built with Go `1.26.5`. This is the corpus source, not an etcd 3.7.2 performance baseline.
- Benchmark Go `1.27.2`, linux/arm64; `github.com/klauspost/compress v1.20.3`; `github.com/pierrec/lz4/v4 v4.1.33`.

Reproduce from the measured commit on a fresh disposable runner before the deadline:

```sh
git clone https://github.com/djosh34/tigerstore.git
cd tigerstore
git checkout 8dbff21670d0fd9d43bc24804a68ed7203ceaf9b
timeout 780 bash experiments/compress50/run.sh
```

The [script](https://github.com/djosh34/tigerstore/blob/8dbff21670d0fd9d43bc24804a68ed7203ceaf9b/experiments/compress50/run.sh) records versions, creates kind, runs `python3 workload.py`, and dumps every current key with:

```sh
kubectl -n kube-system exec etcd-compression-control-plane -- etcdctl \
  --endpoints=https://127.0.0.1:2379 \
  --cacert=/etc/kubernetes/pki/etcd/ca.crt \
  --cert=/etc/kubernetes/pki/etcd/healthcheck-client.crt \
  --key=/etc/kubernetes/pki/etcd/healthcheck-client.key \
  get '' --prefix --write-out=json > "$PRIVATE/dump.json"

GOMAXPROCS=1 taskset -c 0 timeout --kill-after=2s 235s \
  "$PRIVATE/compressbench" -input "$PRIVATE/dump.json" -out "$OUT"
```

The Go module passed gofmt, vet, golangci-lint and short tests locally. Both runner jobs passed its tests and all corpus roundtrip checks.

## Corpus and dominant resources

The workload creates 120 applications across 12 namespaces with Deployments, ConfigMaps, fake Secrets, Services, Events, Leases and EndpointSlices. Four apply generations update configuration, roll Deployment templates and renew Leases. It deletes and recreates one application per namespace between generations. Kubernetes creates the Pods, ReplicaSets and controller Events. Server-side apply avoids client-side last-applied JSON duplication. Containers run pause, not a business application.

The atomic etcd snapshot contains **6,261 values and 9,217,907 value bytes**, plus 353,675 key bytes. Values remain exactly as stored. Of these, 6,239 have the Kubernetes protobuf prefix and total 9,198,502 bytes. The other 22 are APIService JSON values, 19,405 bytes. They remain in the measurements rather than being silently excluded or converted.

| Resource | Values | Value bytes | Share |
| --- | ---: | ---: | ---: |
| Events | 3,803 | 2,807,967 | 30.46% |
| ConfigMaps | 385 | 2,681,580 | 29.09% |
| ReplicaSets | 430 | 1,386,485 | 15.04% |
| Pods | 129 | 655,067 | 7.11% |
| EndpointSlices | 362 | 458,784 | 4.98% |
| Deployments | 122 | 414,004 | 4.49% |
| Secrets | 121 | 203,225 | 2.20% |
| Leases | 244 | 161,383 | 1.75% |

Events and ConfigMaps dominate this workload, not necessarily a production cluster. The pre-dump Pod listing had 122 Running and six Pending Pods. Controllers continued reconciling until the dump, so those earlier list counts differ from the atomic snapshot. This is a live-state dump after churn, not MVCC history or a chronological write trace.

## Compression and single-core cost

The table compares the same 4,915 held-out values. A deterministic SHA-256 key split reserved 1,346 values, 1,986,161 bytes, for training. Every value was also measured with every non-dictionary codec in the separate `all` dataset. Training and evaluation keys are disjoint, but their application templates are shared; this is not cross-cluster validation.

Each cell is **compressed bytes as a percentage of original value bytes; encode/decode CPU microseconds per codec call**. For batch 1, a call handles one value. Other calls handle a packed batch with four-byte lengths. Lower is better. Costs are medians of five complete-corpus timing loops, each at least 40ms. CPU means process user plus system time. CSV also contains wall time, per-value cost and allocations.

| Codec | One value | Target 16 values | Target 64 values | Target 256 values |
| --- | ---: | ---: | ---: | ---: |
| zstd 1 | 47.02%; 10.02/3.98 | 25.30%; 63.60/18.46 | 17.51%; 193.97/52.94 | 13.52%; 665.33/175.23 |
| zstd 3 | 46.48%; 13.33/3.99 | 24.95%; 106.03/18.90 | 17.52%; 308.42/50.49 | 13.72%; 1067.62/168.00 |
| LZ4 | 58.36%; 2.06/0.21 | 34.22%; 28.72/4.00 | 25.79%; 104.32/14.69 | 23.55%; 432.11/55.24 |
| Snappy | 55.85%; 2.92/0.35 | 33.28%; 45.32/8.12 | 25.42%; 168.02/28.48 | 21.84%; 595.96/101.96 |
| zstd 1, dictionary | 25.50%; 18.16/3.88 | 19.21%; 74.11/31.80 | 15.54%; 204.77/90.16 | 13.06%; 653.49/276.91 |
| zstd 3, dictionary | 26.67%; 49.59/3.78 | 19.40%; 153.50/30.44 | 15.62%; 373.76/85.60 | 13.22%; 1194.60/267.64 |

Packing uses a fixed shuffle. Final batches can be short: 308, 77 and 20 calls cover all 4,915 values at targets 16, 64 and 256. Per-call costs above average over those calls, not only full batches. For the entire 6,261-value dump at target 64, plain zstd 1/3, LZ4 and Snappy use 17.48%, 17.49%, 25.75% and 25.38% of original bytes.

Resource-sorted batches compress more: at target 64, held-out plain zstd 1 uses 10.27% rather than shuffled 17.51%. Neither order reproduces Raft arrival order. The code measures both and reports them separately.

The trained zstd 1 and 3 dictionaries occupy 32,897 and 32,913 bytes. Training cost 0.421 and 0.505 CPU seconds, outside timing. Ratios above exclude storing the dictionary. Charging one dictionary to this held-out corpus changes zstd 1's target-64 ratio from 15.54% to 16.00%. At target 256, dictionary storage almost erases its byte advantage: 13.52% including the dictionary versus 13.52% without one. Dictionary distribution, versioning and restart costs are unmeasured.

Codec state and output buffers are warm and reused; every input roundtrips byte-for-byte. Packing, unpacking, dictionary training, keys, commands and Raft metadata are outside timings. Zstd includes frame checksums. LZ4 includes a five-byte raw-length/fallback wrapper. Snappy uses `klauspost/compress/snappy.Encode`, which calls `s2.EncodeSnappyBetter`, not Google's encoder. Formats and checksum overhead differ. Full [methodology](https://github.com/djosh34/tigerstore/blob/exp/pre-50-compress/experiments/compress50/codec/README.md) accompanies the code.

## Does it pay at 1 KiB and 10 KiB?

For real-shaped values, batching makes plain zstd 1 and LZ4 worth testing in the engine. It does not establish an end-to-end win.

The small held-out band contains 621 values of 896 to 1,152 bytes, mean 910.86 bytes, mostly Events. The large band contains 97 ConfigMaps, each 10,834 bytes. No value was resized. At target 64:

| Real size band | Codec | Compressed/original | Encode CPU us/value | Decode CPU us/value | Serial break-even MB/s |
| --- | --- | ---: | ---: | ---: | ---: |
| Near 1 KiB | zstd 1 | 8.70% | 1.001 | 0.334 | 623 |
| Near 1 KiB | LZ4 | 13.43% | 0.425 | 0.081 | 1,560 |
| Near 10 KiB | zstd 1 | 8.47% | 15.331 | 4.245 | 507 |
| Near 10 KiB | LZ4 | 18.21% | 8.367 | 1.292 | 917 |

These bands have ten batches and two batches respectively at target 64, including their short tails. The target-256 large-band test has only one 97-value batch and is not evidence for a full 256-value large-object batch. The complete mixed corpus and exact-size controls do contain full 256-value batches.

The final column is a conditional model: bytes saved per value divided by encode plus decode microseconds, in decimal MB/s. Below that serial transfer bandwidth, byte-transfer time saved exceeds these measured CPU costs. This assumes one encode, one decode and no overlap. It is not a measured network or disk speed, and it omits Raft's fanout, fsync fixed cost, pipelining and read amplification.

For **independent random benchmark values**, compression does not pay. Exact 1,024-byte and 10,240-byte random controls expand under every tested codec at every batch size. At target 64, plain zstd 1 stores 100.420% and 100.046% of original value bytes; LZ4 stores 100.398% and 100.040%. Both also expand slightly relative to already-framed input. The repeated-pattern controls compress extremely well, so repeated or overlapping benchmark buffers cannot justify a production gain. Both controls are archived for every codec and batch size.

An uncached point read must decode a whole packed block. On the mixed held-out corpus, plain zstd 1 costs 52.94 CPU us per target-64 block and 175.23 us per target-256 block, compared with 3.98 us for one separately compressed value. That cost is not amortized if a read needs only one member.

## Proven and unproven

Proven here: actual kube-apiserver storage bytes from Kubernetes 1.37, byte ownership by resource, exact codec sizes, successful roundtrips, and warmed single-core compression/decompression costs on two short free-runner jobs. No dictionary result evaluates a training key. Raw corpus and dictionaries did not leave the runner.

My recommendation is to compare plain zstd 1 and LZ4 on packed entries in a later tournament. Zstd 3 adds CPU without a useful mixed-batch size gain here. A dictionary helps small individual objects but usually costs more CPU, and its advantage shrinks with batching. Keep an uncompressed fallback.

The engine speedup remains a guess. No tigerstore integration, three-replica durability comparison, production resource mix, future-cluster dictionary validation, cold-cache read test or capacity gain was measured. Do not close #50 or #52 from this pre-work. The Sol helper was deleted; the experiment branch and aggregate evidence remain.
