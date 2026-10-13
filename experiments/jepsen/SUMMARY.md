## Jepsen comparison and etcd robustness audit

Tested tigerstore main `90870876b6459334c7eddd67c0f9d184267f2eba` without product changes. [Throwaway branch](https://github.com/djosh34/tigerstore/tree/exp/jepsen), [full report](https://github.com/djosh34/tigerstore/blob/exp/jepsen/experiments/jepsen/REPORT.md), [every job's duration and result](https://github.com/djosh34/tigerstore/blob/exp/jepsen/experiments/jepsen/CASES.md), and [permanent histories and logs](https://github.com/djosh34/tigerstore/tree/exp/jepsen/experiments/jepsen/evidence).

Versions:
- Stock etcd v3.7.2, `68c065e562994b89e333e77b039ad066f933c586`.
- Latest `jepsen-io/etcd`, `b47cc401f27bfb7c28afd9ac088b5f0dca4501b8`. Its dependencies are Jepsen 0.3.11 and jetcd 0.8.5.
- Jepsen's linked community Docker setup, `4b5707813a0a24966082ae4fd2dd223da447e499`, with pinned image digests. OpenJDK 25.0.4.1, Leiningen 2.12.0, 4 GiB JVM heap. Tigerstore built with Go 1.27.2 on the runner.

Each free `ubuntu-24.04` runner hosted one control container and three DB containers. Client endpoints were plain gRPC; peers used mTLS. Only DB setup/startup, primary endpoint lookup, and heap sizing changed. Workloads, clients, checkers, and nemesis implementations stayed unchanged.

Stock etcd ran before each matching tigerstore configuration. These commands each represent a separate fresh runner job:

```sh
bash experiments/jepsen/run.sh etcd register partition 90 10
bash experiments/jepsen/run.sh tigerstore register partition 90 10
```

The inner command was:

```sh
lein run test --nodes n1,n2,n3 --username root \
  --ssh-private-key /root/.ssh/id_rsa --version 3.7.2 \
  --workload register --nemesis partition --concurrency 2n \
  --rate 30 --time-limit 90 --nemesis-interval 5 --ops-per-key 10
```

The adapter selects tigerstore through `STORE=tigerstore`; upstream's test name still says etcd 3.7.2. Every artifact records the actual binary hash. Set, append, and watch used 200 ops/key. Register also ran at 200 and 50. Every exact command is retained.

### Results

Ran 71 jobs from 16:00:41Z to 16:59:51Z on 2026-10-13. Jobs took 1m01s to 6m17s, including setup and upload, below the 20-minute limit. Aggregate runner time was 4h07m48s.

| Store | Passed | Inconclusive | Setup failed | Clock blocked |
| --- | --- | --- | --- | --- |
| etcd | 37 | 0 | 0 | 1 |
| tigerstore | 20 | 8 | 4 | 1 |

Passing matched coverage exists for register, set, append, and watch under no faults, partitions, kills, and pauses. Offline checks also required observed DROP rules with packet hits, stopped processes, or replacement PIDs, plus completed fault/healing operations.

The qualifications matter:

- Tigerstore register kill/partition histories at 200 ops/key returned `:valid? :unknown, :cause :out-of-memory`. Each command was repeated twice; all repeats had the same checker limit, while etcd passed. Two 50-op/key runs were also inconclusive. At 10 ops/key, both faults passed twice on each store. Those shorter histories do not certify the eight larger histories.
- Four tigerstore set attempts failed before traffic because MemberList succeeded before the first election and SetClient's setup put did not retry. DB setup now waits for a successful linearizable read. All four set cases then passed on both stores. The earlier failed attempts remain in the evidence.
- Independent clock skew is unavailable in this Docker topology. Containers share the runner/control realtime clock. Both clock preflights failed explicitly; no clock test is claimed. Lock workloads did not run because tigerstore lacks their Lock RPC.

No definite consistency violation was confirmed, so no product `bug` issue was filed. This does not close #49.

### Etcd robustness and CI

The [source-only robustness audit](https://github.com/djosh34/tigerstore/blob/exp/jepsen/experiments/jepsen/robustness.md) identifies the exact reusable functions. The robustness suite itself was not executed.

- Recording clients and lower-level `SimulateKeyValueTraffic`, `SimulateWatchTraffic`, and `SimulateCompactionTraffic` can use external endpoints. They need a new runner, supported API profiles, and a TLS configuration adapter where required.
- The nondeterministic Porcupine model and public validator can check external-client histories. Fresh storage, revision assumptions, and uncertain-write handling must be preserved.
- Watch validation needs recorded responses and, for full replay-dependent checks, a trustworthy ordered request history. Passing nil persisted requests skips watch and historical-read checks without necessarily returning an error. Require explicit checker statuses and bounded delivery through a final sentinel.
- Gofail hooks, etcd process management, WAL decoding/repair, and WAL-assisted history patching are etcd-specific. External process/network faults and tigerstore's own file-fault layer must replace those mechanisms.

For maintained CI, select the candidate SHA instead of the experiment's fixed SHA, remove the experiment deadline, add the desired triggers, run controls first, and wire the offline fault checks into the job. Keep unknown/missing verdicts and missing faults as failures, retain histories, and use bounded register histories explicitly. Independent clock skew needs VM kernels. All helpers are deleted; testing has stopped.
