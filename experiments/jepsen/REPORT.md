# Jepsen results for tigerstore #49

Ran on 2026-10-13 from 16:00:41Z to 16:59:51Z. Product code is unchanged. The throwaway branch is [exp/jepsen](https://github.com/djosh34/tigerstore/tree/exp/jepsen).

There is a passing matched comparison for register, set, append, and watch with no faults, partitions, kills, and pauses. Register under kills and partitions needed short per-key histories. Eight larger tigerstore register histories remain inconclusive. Independent clock skew cannot run in this Docker topology, and tigerstore lacks the Lock service. This is not completion of every requested check or of #49.

No definite consistency violation was reported. No product `bug` issue was filed. The inconclusive histories are retained and are not certified by the smaller passing runs.

## Versions and setup

| Component | Version |
| --- | --- |
| Tigerstore | Main at experiment start, `90870876b6459334c7eddd67c0f9d184267f2eba` |
| Etcd control | v3.7.2, `68c065e562994b89e333e77b039ad066f933c586` |
| Latest Jepsen etcd test | `b47cc401f27bfb7c28afd9ac088b5f0dca4501b8`, dated 2026-04-17 |
| Jepsen library / jetcd | 0.3.11 / 0.8.5 |
| Docker setup | `nurturenature/jepsen-docker` at `4b5707813a0a24966082ae4fd2dd223da447e499`, linked by Jepsen's README |
| Control runtime | OpenJDK 25.0.4.1, Leiningen 2.12.0, JVM heap capped at 4 GiB |
| Antithesis dependency | `0.1.0-20260116.160922-21`, SDK 1.4.4, resolved by the unchanged upstream dependency declaration |
| Tigerstore build | Go 1.27.2, `GOFLAGS=-p=2 GOMEMLIMIT=2GiB CGO_ENABLED=0 go build ./cmd/tigerstore` |
| Runner | Free public `ubuntu-24.04`, x86-64, Linux `6.17.0-1022-azure`; first runner image `20261004.327.1` |

Each job used one control container and three DB containers on one runner. The image digests are pinned in `run.sh` and recorded in the artifacts. Client gRPC endpoints are plain HTTP on 2379, as upstream expects. Both stores use peer mTLS on 2380 with a fresh test CA per job.

Only DB installation/startup, primary endpoint lookup, and the JVM heap limit changed in the Jepsen project. `upstream.patch` records every change. Workloads, clients, checkers, and nemesis implementations stayed unchanged. `README.md` explains the adapter and its startup-readiness correction.

Every workload/fault configuration ran against stock etcd before its tigerstore comparison. Jepsen chooses random fault schedules, so matched commands do not mean identical schedules. The realized schedules are in the histories.

## Commands

On a GitHub runner, from the experiment checkout:

```sh
bash experiments/jepsen/run.sh etcd register partition 90 10
bash experiments/jepsen/run.sh tigerstore register partition 90 10
```

The arguments are store, workload, nemesis, traffic duration in seconds, and operations per key. Each job runs one case. Replace `partition` with `kill` for the other small-history case. Both small-history configurations ran twice on each store. For set, append, and watch, use 200 operations per key. Register without faults and with pauses passed at both 200 and 50.

Inside the control container:

```sh
lein run test --nodes n1,n2,n3 --username root \
  --ssh-private-key /root/.ssh/id_rsa --version 3.7.2 \
  --workload register --nemesis partition --concurrency 2n \
  --rate 30 --time-limit 90 --nemesis-interval 5 --ops-per-key 10
```

`STORE=tigerstore` selects the tigerstore binary. Upstream's `--version` still names the test `etcd 3.7.2`; it does not identify the actual server. The artifact name, build SHA, and `server.sha256` do. The first etcd smoke test used 60 seconds; every subsequent traffic test used 90. Early commands omitted `--ops-per-key`, which means upstream's default 200.

The exact command for each case is in its `command.txt` and summary JSON. No clocks were changed on the agent host, no containers ran there, and no Go build ran there.

## Results

| Store | Passed | Inconclusive | Setup failed | Clock blocked | Total jobs |
| --- | --- | --- | --- | --- | --- |
| Stock etcd | 37 | 0 | 0 | 1 | 38 |
| Tigerstore | 20 | 8 | 4 | 1 | 33 |

The 65 histories with checker results contain 74,404 etcd operations and 57,806 tigerstore operations, as counted by Jepsen's stats checker. That includes operations in inconclusive histories and operations that failed or had unknown outcomes. It is not a count of successful writes.

Final matched coverage:

| Workload | No fault | Partition | Kill/restart | Pause/resume |
| --- | --- | --- | --- | --- |
| Register | Both passed | Both passed twice at 10 ops/key; larger tigerstore histories remain inconclusive | Both passed twice at 10 ops/key; larger tigerstore histories remain inconclusive | Both passed |
| Set | Both passed after readiness correction | Both passed after readiness correction | Both passed after readiness correction | Both passed after readiness correction |
| Append | Both passed | Both passed | Both passed | Both passed |
| Watch | Both passed | Both passed | Both passed | Both passed |

The observer recorded DROP rules with positive packet counters, stopped process states, and replacement PIDs. The offline summary requires those observations for the corresponding faults, plus completed injection and healing operations in the history. Every completed faulted history met these checks, including the inconclusive ones. These checks prove that each requested fault class took effect at least once, not that every randomly selected target did. Upstream can select an empty primary set during an election.

### Register checker memory limit

The first tigerstore register kill and partition runs returned:

```clojure
{:linear {:valid? :unknown, :cause :out-of-memory}}
```

Each command was repeated twice at the same 200-operations-per-key setting. All four repeats also returned `:unknown` with `:out-of-memory`; all four stock etcd repeats passed. One further tigerstore run of each fault at 50 operations per key had the same result. That makes eight inconclusive histories, not eight consistency counterexamples.

The histories contain many ambiguous mutations. For example, the first partition history contains:

```clojure
{:index 552, :time 13759761298, :type :info, :process 10, :f :write, :value [1 [nil 1]], :error [:unavailable "etcdserver: no leader"]}
{:index 566, :time 14064559196, :type :info, :process 16, :f :cas, :value [1 [nil [4 3]]], :error [:unavailable "etcdserver: no leader"]}
```

In the 50-operation partition run, some keys had all 50 operations return unknown outcomes. The test still treats these as ambiguous, as upstream does. No errors were relabelled as definite aborts and no operations were removed.

Using upstream's `--ops-per-key 10` option bounded each independent history. Both faults then passed twice on each store. These shorter histories provide narrower evidence. They do not establish what the checker could not decide about the earlier histories.

Evidence runs are [first attempt](https://github.com/djosh34/tigerstore/actions/runs/38428716698), [two repetitions](https://github.com/djosh34/tigerstore/actions/runs/38430056885), [50-operation tigerstore runs](https://github.com/djosh34/tigerstore/actions/runs/38432262482), [10-operation etcd controls](https://github.com/djosh34/tigerstore/actions/runs/38433448661), and [10-operation tigerstore runs](https://github.com/djosh34/tigerstore/actions/runs/38434478844).

### Set startup failure

All four first tigerstore set cases stopped before workload history collection. The error was:

```text
java.util.concurrent.ExecutionException:
  io.grpc.StatusRuntimeException: UNAVAILABLE: etcdserver: no leader
at jepsen.etcd.set.SetClient.setup_BANG_(set.clj:15)
```

Upstream DB readiness only calls MemberList, which tigerstore can answer before the initial election. SetClient immediately performs a setup put without retrying. The DB adapter now also waits for one successful linearizable read, with a 30-second retry deadline. It does not write data or change the workload. All four set cases then passed on each store with that adapter.

The original four failed runs remain failed setup attempts, not passes. Their logs and server data are retained in run `38429841266`. The corrected control is `38430904489`; the corrected tigerstore run is `38432262482`.

### Missing clock and lock coverage

The [Docker setup's own documentation](https://github.com/nurturenature/jepsen-docker/blob/4b5707813a0a24966082ae4fd2dd223da447e499/README.md#containers-dont-have-real-clocks) says it cannot test independent clock skew. The containers and control reported the same time namespace, `time:[4026531834]`. Docker shares the host's realtime clock. Running upstream's clock nemesis here would change the runner/control clock as well, not isolate one DB node.

Both clock preflights therefore failed explicitly before the workload ran. Clock faults remain untested. Independent VM kernels are needed for that coverage; no clock nemesis was replaced with a no-op.

Tigerstore does not implement the Lock service used by `lock-set` and `lock-etcd-set`, so neither ran. The older `lock` workload is already disabled upstream. These are unsupported cases, not passing tests.

## Durations and retained evidence

All 71 jobs finished in 1m01s to 6m17s, below their 20-minute limit. The final small-history tigerstore jobs took 3m35s to 4m11s. Aggregate runner job time was 4h07m48s; jobs overlapped. [CASES.md](CASES.md) lists every job's exact duration, outcome, operation count, and Actions link.

[evidence/](evidence/) retains compressed histories, Jepsen's binary test records, checker results, server logs/data, fault observations, exact commands, image metadata, and DB patches. It also has Actions job metadata and offline summaries. These files do not depend on the 14-day Actions artifact retention. Plot images are omitted; their source histories remain. `SHA256SUMS` identifies the archives.

## Etcd robustness audit

The separate [source audit](robustness.md) pins etcd v3.7.2 at `68c065e562994b89e333e77b039ad066f933c586`. It includes exact exported functions and line-level links. No etcd robustness test was executed in this experiment.

- The recording client and lower-level traffic starters accept endpoint strings. `SimulateKeyValueTraffic`, `SimulateWatchTraffic`, and `SimulateCompactionTraffic` can drive external tigerstore endpoints with a new runner and explicit lifecycle management. The top-level `SimulateTraffic` is coupled to `EtcdProcessCluster` and failpoint reports. TLS needs a config-taking recording-client adapter. Only use API operations tigerstore supports.
- The exported nondeterministic Porcupine model can check recorded external-client histories. The public validator prepares uncertain writes and invokes it. Fresh storage, an initial revision-1 read, compatible request semantics, and bounded histories are required. Etcd's full model is not a general-purpose MVCC or lease-expiry oracle.
- Watch validation can consume external-client reports, but its detailed validators are private. Full reliability, resumability, previous-value, and historical-read checks need a trustworthy ordered replay history. Passing nil persisted requests to the public validator skips watch and historical-read validation while its error accessor can still return nil. An adapter must require explicit checker statuses and bounded delivery through a final sentinel.
- Gofail hook names, HTTP/environment activation, etcd's process manager, WAL decoding/repair, and WAL-assisted history patching are etcd-specific. Use external partition/process faults for tigerstore. Its own file-fault layer or future named hooks would replace etcd's internal fault mechanisms. Do not parse tigerstore storage as an etcd WAL.

## What remains for maintained CI

Promote the experiment through a reviewed CI change, not by merging this throwaway branch wholesale. Build the candidate commit instead of the fixed SHA, remove the experiment deadline, and keep the upstream commit, runtime dependencies, and image digests pinned. Cache the downloads if needed.

Run the stock control before each candidate configuration. Keep one bounded case per job and always upload evidence. Gate on a definite Jepsen verdict plus the fault-observation checks; currently those observation checks ran offline. A missing result, `:unknown`, or missing fault must fail. Start register CI with explicitly small histories, while retaining a separate larger-history check whose inconclusive results remain failures. Add an aggregate check that cannot turn skipped dependencies green.

Clock skew needs a separate VM-based job. Locks need product API support before that workload can run. For etcd robustness, use the endpoint-only adapter described in the audit and explicitly gate every claimed checker; importing the top-level suite is insufficient.

SIGKILL does not test power-loss durability. These runs did not validate every #49 guarantee, and #49 remains open.
