# Kubernetes 1.37 conformance research

Research date: 2026-10-12. Work stays under this directory. No subagents or issue comments. Throwaway code is only on `exp/pre-48-conformance`; nothing merges. Deadline: 2026-10-13T22:00:00Z.

The committed mirror is `experiments/pre48-conformance/REPORT.md` on `exp/pre-48-conformance`. Its `evidence/` directory preserves both runner measurements, original JUnit/logs, parsed exact-name outcomes, the shard inventory proof and the separately labeled Talos reference. Large local source/API snapshots and downloaded binaries are not committed.

## Answer for the parent

Use Hydrophone v0.8.0 with the official `registry.k8s.io/conformance:v1.37.0` image. Its `--parallel=4` passes Ginkgo `--procs=4` without silently dropping serial tests. Full selection is `--conformance --skip=''`. This runs all 462 conformance specs, including 29 serial specs, 6 slow specs and 2 disruptive specs. The underlying suite has 3206 specs; skipping the other 2744 is normal, not missing conformance coverage.

Two bounded full-selection discoveries completed on both free runner labels but did not complete conformance. The corrected rerun passed 112 specs on ARM and 94 on AMD before the eight-minute suite timeout. Both interrupted four active specs and left all 29 serial specs unrun. Neither run establishes full conformance on a free runner. Do not interpret a green discovery workflow as a conformance pass. Its script deliberately preserves the failing test exit code in `timing.json`, then exits zero so Actions can finish collecting artifacts.

For the eventual full nightly or release gate, propose a 150-minute job, Ginkgo `--timeout=120m`, and a 130-minute outer bound on Hydrophone. Preserve the full selection and require a real pass. This is a proposed budget, not proof of completion on either free runner. No long job was started in this experiment.

I found no documented certified-mode split across independent short jobs or an approved merge procedure in CNCF's certification instructions, Hydrophone or Sonobuoy. Diagnostic shards are possible and their union can cover all 462 names. They change the required focus, so they are not evidence of an accepted certified run. Do not combine their reports and call that certification without CNCF approval.

## Runnable bounded discovery

The parent provides kind v0.33.0, one control plane and two workers, with three stock external etcd v3.7.2 members and client/peer TLS. The probe uses parent `boot.sh` from commit `617e1063ce8c65e889377f5facbc887f2e9f1411`.

On-push workflow: [`exp/pre-48-conformance`, commit `1331190e4569b0f918d0b62ac928b39b22a809e6`](https://github.com/djosh34/tigerstore/tree/1331190e4569b0f918d0b62ac928b39b22a809e6).

First measured run: https://github.com/djosh34/tigerstore/actions/runs/38316322709

Corrected reporting run: https://github.com/djosh34/tigerstore/actions/runs/38317470141, commit `ac76fc8a8c563e15c07bb5ade7b58c569737fd7f`.

Each matrix job has a hard 15-minute Actions limit. Boot has a four-minute outer limit. The suite has an eight-minute Ginkgo limit and a ten-minute Hydrophone outer limit.

```bash
export PATH="$PWD/.pre48/bin:$PATH"
timeout -k 20s 10m hydrophone \
  --conformance --skip='' --parallel=4 \
  --conformance-image=registry.k8s.io/conformance:v1.37.0 \
  --startup-timeout=2m --disable-progress-status \
  --extra-ginkgo-args='--timeout=8m,--seed=48' \
  --output-dir="$PWD/.pre48/results/conformance"
```

The script downloads and verifies Hydrophone's v0.8.0 release checksum. ARM uses `hydrophone_Linux_arm64.tar.gz`; AMD uses `hydrophone_Linux_x86_64.tar.gz`.

For a future full gate, not executed here, use the same cluster and tool with a 150-minute Actions job:

```bash
timeout -k 30s 130m hydrophone \
  --conformance --skip='' --parallel=4 \
  --conformance-image=registry.k8s.io/conformance:v1.37.0 \
  --startup-timeout=2m --disable-progress-status \
  --extra-ginkgo-args='--timeout=120m,--seed=48' \
  --output-dir="$PWD/.pre48/results/conformance"
```

The full gate must fail if Hydrophone exits nonzero, any expected name is absent or duplicated, or any selected case is skipped, pending, failed or interrupted. Keep the original JUnit and log, not just a synthesized summary. Only a completed run establishes whether this budget is enough.

The first pushed run, [38316287822](https://github.com/djosh34/tigerstore/actions/runs/38316287822), used the parent's old raw-etcd proof-count command. The parent found that `etcdctl --count-only` needed `-w fields`. I canceled that run and updated the boot script before the active run. It is not a conformance measurement.

Hydrophone v0.8.0 fetches only `e2e.log` and `junit_01.xml`, then deletes its pod. The script copies the entire result directory if the outer timeout leaves the pod alive. JUnit is the expected machine report when Hydrophone fetches normally.

The first measured run also used `--json-report=/tmp/results/ginkgo.json`. Ginkgo v2.32.0 resolved this under `/usr/local/bin/tmp/results/ginkgo.json`; both runners recorded an extra `[ReportAfterSuite]` failure, `mkdir /usr/local/bin/tmp: permission denied`. This was an experiment reporting defect, not a Kubernetes behavior failure. The original logs retain it. I removed the unnecessary JSON flag and replaced the parser with JUnit parsing for the corrected bounded rerun. A local dry-run verified that `--output-dir=/tmp/results --json-report=ginkgo.json` places JSON in the intended directory, but Hydrophone would still not fetch that extra file before deleting its pod.

## Exact inventory and prototype

Local files:

- `evidence/conformance-names.txt`: all 462 exact names.
- `sources/kubernetes/test/conformance/testdata/conformance.yaml`: the v1.37.0 source inventory, also 462 entries.
- `evidence/inventory-dryrun.json`: upstream Ginkgo dry-run report from the official v1.37.0 ARM64 test binary.
- `evidence/kubernetes-test-checksum-verification.txt` and `evidence/local-binary-sha256.txt`: verified release archive and extracted binary checksums.
- `shards.py`, `evidence/shards/manifest.json`, `evidence/shard-dryrun-verification.json`: diagnostic shard prototype.

The official binary's `--list-tests` prints all tests even with a focus argument. I used Ginkgo dry-run JSON instead:

```bash
ginkgo --dry-run --focus='\[Conformance\]' \
  --output-dir="$PWD" --json-report=inventory.json --no-color ./e2e.test
```

A dry-run prints selected specs as passed without executing them. None of these local dry-runs proves Kubernetes behavior. For this Ginkgo version, use a filename for `--json-report` and set `--output-dir` to the desired directory. The local inventory succeeded despite an unintended duplicate output directory; the non-root conformance image cannot write such a directory.

| SIG | Specs |
| --- | ---: |
| node | 104 |
| api-machinery | 100 |
| storage | 91 |
| apps | 60 |
| network | 47 |
| auth | 27 |
| cli | 17 |
| scheduling | 11 |
| instrumentation | 4 |
| architecture | 1 |
| Total | 462 |

The prototype creates 32 shards of 14 or 15 specs and places at most one serial spec in each. I ran 32 upstream Ginkgo dry-runs to verify the generated regexes. Their disjoint union has 462 names, zero duplicates and zero missing names. No actual tests ran. These are not duration-balanced shards; names alone cannot prove that a shard fits the job budget.

```bash
python3 shards.py evidence/conformance-names.txt evidence/shards --count=32
focus=$(< evidence/shards/00.focus)
hydrophone --focus="$focus" --skip='' --parallel=4 \
  --conformance-image=registry.k8s.io/conformance:v1.37.0 \
  --extra-ginkgo-args='--timeout=8m,--seed=48' \
  --output-dir=shard-00
```

An exact anchored focus must include `Kubernetes e2e suite ` before the spec name. Ginkgo v2.32.0 applies the regex to the suite description plus the spec text. `^full-spec-name$` selected zero specs in a local probe; `^Kubernetes e2e suite full-spec-name$` selected one. The generator escapes Go RE2 metacharacters and verifies the names against the real binary.

## Certification and splitting

[CNCF instructions](https://github.com/cncf/k8s-conformance/blob/master/instructions.md#running) define the set by `[Conformance]`, require `E2E_FOCUS=\[Conformance\]` and empty `E2E_SKIP` for manually configured certification runs, and say a valid certification run may not skip any conformance tests. They accept both Hydrophone and Sonobuoy.

A submission needs reproducible instructions, `e2e.log`, `junit_01.xml` and product metadata. The review checks every required name in JUnit and checks that all tests pass in the log. Mere inventory counts are weaker than exact-name equality. Full-mode execution evidence is also different from formal certification, which needs participation and CNCF acceptance under the [program terms](https://github.com/cncf/k8s-conformance/blob/master/terms-conditions/Certified_Kubernetes_Terms.md).

Hydrophone's [`--conformance` implementation](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/cmd/root.go) is an alias for the required focus. Its [deployment code](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/pkg/conformance/deploy.go) sets empty skip by default and passes `--procs=N` for parallel execution. Ginkgo keeps serial-decorated specs and runs them after the parallel specs on process one. That is parallel execution inside one suite, not separate independent short jobs.

`hydrophone --continue` reconnects to an already-running pod. It is not checkpoint/resume support. Actions jobs get different disposable VMs, so a kind cluster on one VM does not persist into the next job. A persistent cluster could outlive monitoring jobs, but it would still run the full suite continuously and would not satisfy this probe's all-short-lived-runner arrangement.

Sonobuoy v0.57.5's [certified mode](https://github.com/vmware-tanzu/sonobuoy/blob/v0.57.5/cmd/sonobuoy/app/modes.go) also uses the full focus and no skip. Its default is `non-disruptive-conformance`, not full mode. `conformance-lite` is explicitly unofficial and removes slow cases.

A full sequential alternative is:

```bash
sonobuoy run --mode=certified-conformance \
  --kubernetes-version=v1.37.0 \
  --kube-conformance-image=registry.k8s.io/conformance:v1.37.0
# After sonobuoy status reports completed:
outfile=$(sonobuoy retrieve)
sonobuoy results "$outfile"
```

Do not use `sonobuoy --e2e-parallel=true` as a full certified run. It sets `E2E_PARALLEL=true`, and the upstream v1.37.0 [go-runner](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/conformance/image/go-runner/cmd.go) inserts a serial skip when the original skip is empty. Passing `--procs=4` through `E2E_EXTRA_GINKGO_ARGS` while leaving `E2E_PARALLEL=false` avoids that source-level trap, but Hydrophone already does this without extra orchestration.

The inspected tool source does not expose a shard index/count or checkpoint option. A focused shard and hand-merged XML are not a documented certification path. I found no source that authorizes them. This is not a claim that CNCF could never approve an exception.

## Runtime evidence

Keep these categories separate:

1. Hydrophone's [maintainer documentation](https://github.com/kubernetes-sigs/hydrophone/blob/v0.8.0/docs/understanding-e2e-results.md) estimates one to two hours. CNCF says an hour or more. Neither estimate measures our runner topology.
2. A recorded [Talos v1.37 report](https://github.com/cncf/k8s-conformance/blob/master/v1.37/talos/e2e.log) in the CNCF repository ran all 462 specs in 6789.861 seconds, 113.164 minutes, with 462 passed and zero failed/pending selected tests. Its [reproduction instructions](https://github.com/cncf/k8s-conformance/blob/master/v1.37/talos/README.md) use QEMU with three control planes and two workers. It is not this free runner setup. Its JUnit's 29 serial case durations sum to 1026.530156461 seconds, 17.109 minutes. That is observed work on Talos, not a universal lower bound.
3. Our bounded free-runner probes provide elapsed time and how much of the suite finished. The first timed out at eight minutes. This attempt did not complete within eight minutes; it cannot establish an actual full-suite duration or passing completion time.

Fixed waits give only a conservative serial lower bound. The selected [taint-removal test](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/e2e/node/taints.go#L284-L336) requires a 75-second observation before success. The selected [orphan-RC test](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/e2e/apimachinery/garbage_collector.go#L388-L445) sleeps 30 seconds. The selected [preemption-path test](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/e2e/scheduling/preemption.go#L759-L913) sleeps five seconds. Thus those three contribute at least 110 seconds to a passing serial phase, before all other work.

The selected host-port conflict test calls `WaitForPodNotPending` for a pod that should remain pending. The default PodStart timeout is five minutes, and the accepted Talos sample takes 304.248 seconds. That describes healthy intended behavior, but summing arbitrary timeout constants is not a valid runtime proof. I have not proven an unavoidable total greater than 15 minutes.

Upstream [kind test-infra configuration](https://github.com/kubernetes/test-infra/blob/master/config/jobs/kubernetes-sigs/kind/kind.yaml) gives full conformance 150 minutes and describes its parallel variant as approximately 20 minutes with serial specs skipped. Those are job budgets and a source comment, not timings for v1.37.0 on our runners. Copying that serial skip would invalidate full certified selection.

## Architecture and runner facts

[kind v0.33.0](https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0) publishes the parent's exact node digest:

```
kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
```

The kind release explicitly supports native AMD64 and ARM64. The official conformance v1.37.0 registry index lists both architectures. Saved primary manifest: `evidence/conformance-image-manifest.json`.

- AMD64 child digest: `sha256:962b4bd5002b34a2bd7b2b63bed3aec1800a57209484b57b0a1792a080053c1a`.
- ARM64 child digest: `sha256:a308614b20795ed57586bbe929721909ba4524d7babfe44b70b18b56f5edb375`.

GitHub [runner documentation](https://docs.github.com/en/actions/reference/runners/github-hosted-runners) lists both `ubuntu-24.04` and `ubuntu-24.04-arm` as standard public-repository Linux runners. Record actual CPU, memory, free disk and boot durations from artifacts rather than treating advertised specifications as measurements. The parent owns the boot and 50k-object measurements.

## Failures and flakes

There is no measured flake rate for this topology. Our observed selected-test failures all say `[TIMEDOUT] A suite timeout occurred`; they are not evidence of intermittent test defects. Do not call a first failure a flake merely because a rerun passes. Keep the original failure, exact spec name, duration, logs, image/tool version, random seed and rerun result.

Three primary-source examples show what to investigate if those same behaviors fail with adequate time:

- [Kubernetes #137393](https://github.com/kubernetes/kubernetes/issues/137393) reports `ReplicaSet Replicaset should have a working scale subresource [Conformance]` flakes in the kind beta-enabled lane in February and March 2026. [Fix #137403](https://github.com/kubernetes/kubernetes/pull/137403) says waiting for pods to become ready did not imply the controller had updated its status. It changed the test to wait for controller status and merged on March 4. The [v1.37.0 source](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/e2e/apps/replica_set.go#L419-L428) uses that status-wait approach. Treat this as a fixed historical test race, not an expected current failure.
- [Kubernetes #120570](https://github.com/kubernetes/kubernetes/issues/120570) reports `HostPort validates that there is no conflict between pods with same hostPort but different hostIP and protocol [LinuxOnly] [Conformance]` in parallel kind conformance. A maintainer traced recorded failures to `OutOfpods`, 110 pods used out of capacity 110, after another garbage-collector test exhausted the node. The issue closed in April 2024. This explains how a network-test failure can come from pod capacity. It does not prove the old defect still applies to kind v0.33.0 or v1.37.0.
- [Kubernetes #94931](https://github.com/kubernetes/kubernetes/issues/94931) reports `NoExecuteTaintManager Multiple Pods ... evicts pods with minTolerationSeconds ... [Conformance]` flakes in kind and GCE conformance, with `Failed to evict all Pods. 1 pod(s) is not evicted.` It closed in March 2021 when the linked dashboards were green. Current applicability is unknown, and our bounded probe never reached this serial case.

All three behaviors are in the current inventory. Their history is useful for triage, not permission to skip them or a measured flake forecast for these runners.

Expected failure classes to investigate, not established causes, are cold image pulls, shared-runner resource pressure, node-level timing checks, DNS/network checks and disruption from serial taint/scheduler tests. The kind topology supplies two workers because several tests need distinct nodes. Ginkgo serial decorations prevent those tests from running concurrently with the rest; skipping them is not a fix.

For a strict project gate, require every conformance name to pass, no selected skips/pending/interruption, a successful suite exit, and complete reports from the chosen main commit. Our stock-etcd experiment can only establish the baseline tooling and cluster environment. It cannot establish that tigerstore passes.

## First measured run

Run [38316322709](https://github.com/djosh34/tigerstore/actions/runs/38316322709) used `1331190e4569b0f918d0b62ac928b39b22a809e6`, Hydrophone v0.8.0, seed 48, full focus, no skip, four Ginkgo processes and the parent's external stock-etcd TLS cluster.

| Measurement | ubuntu-24.04-arm | ubuntu-24.04 |
| --- | ---: | ---: |
| Boot setup and ready, seconds | 46.942 | 61.585 |
| Conformance specs passed | 110 | 94 |
| Specs interrupted by suite timeout | 4 | 4 |
| Selected specs skipped after timeout | 348 | 364 |
| Ginkgo suite runtime, seconds | 498.208 | 498.117 |
| Hydrophone invocation elapsed, seconds | 545.135 | 546.362 |
| Hydrophone exit code | 1 | 1 |
| Serial specs actually executed | 0 | 0 |

Each JUnit contains all 462 expected conformance names exactly once, with no unexpected or missing names. The skipped counts above concern conformance only; the larger Ginkgo skipped totals also include the 2744 non-conformance specs. A skipped spec after timeout is not a pass and is not the same state as Ginkgo `Pending`, which remained zero.

The extra JSON reporting defect described above adds one non-spec `[ReportAfterSuite]` failure. No selected-test failure before the deliberate timeout appeared in this run. That narrow observation does not mean unrun specs would pass.

ARM's interrupted specs were API chunking with expired continuation, the 500-updates Pod Generation test, projected-secret optional updates, and fail-closed AdmissionWebhook during namespace cleanup. AMD's were API chunking with expired continuation, ResourceQuota status updates, GRPC liveness restart, and the 500-updates Pod Generation test. Exact full names and failure messages are in each runner's `conformance/junit-analysis.json`, `failed-names.txt` and `pending-names.txt`.

Measured real costs:

- The expired-continuation API chunking case occupied a process until the suite timeout on both runners. JUnit reports 497.778 seconds ARM and 497.758 seconds AMD, including timeout cleanup. The failure progress report places it in `retrieving the second page until the token expires` after approximately 462 seconds in that step. [The v1.37.0 source](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/e2e/apimachinery/chunking.go#L144-L193) polls every 20 seconds for up to twice `storagebackend.DefaultCompactInterval`. [That default is five minutes](https://github.com/kubernetes/kubernetes/blob/v1.37.0/staging/src/k8s.io/apiserver/pkg/storage/storagebackend/config.go#L38). Thus the source allows a ten-minute poll budget. It is not a mandatory ten-minute sleep, and our timeout does not prove compaction is broken.
- `CronJob should not schedule jobs when suspended [Conformance] [Slow]` passed in 300.132 seconds ARM and 300.125 seconds AMD. [The source](https://github.com/kubernetes/kubernetes/blob/v1.37.0/test/e2e/apps/cronjob.go#L99-L110) uses a five-minute `Consistently` observation. This case really costs five minutes even on a healthy fast cluster.
- The failing-subpath Variable Expansion case passed in 154.740 seconds ARM and 154.957 seconds AMD. That is observed cost on these runners, not a universal lower bound.

The ARM runner reported four Neoverse-N2 CPUs, 16,722,010,112 bytes RAM and 115,112,218,624 bytes free on its root filesystem before boot. AMD reported four CPUs with an AMD EPYC 9V74 model, 16,766,414,848 bytes RAM and 91,249,053,696 bytes free. Post-run Docker stats are instantaneous snapshots, not peak-memory measurements.

Raw artifacts and local analysis are under `evidence/run-38316322709/`. The original experiment reporting error remains in `e2e.log` and `hydrophone.log`.

## Corrected bounded rerun

Run [38317470141](https://github.com/djosh34/tigerstore/actions/runs/38317470141) used `ac76fc8a8c563e15c07bb5ade7b58c569737fd7f`. It removed the invalid JSON report flag and parsed Hydrophone's preserved JUnit directly. All other suite settings stayed the same.

| Measurement | ubuntu-24.04-arm | ubuntu-24.04 |
| --- | ---: | ---: |
| Boot setup and ready, seconds | 53.394 | 53.416 |
| Conformance specs passed | 112 | 94 |
| Specs interrupted by suite timeout | 4 | 4 |
| Selected specs skipped after timeout | 346 | 364 |
| Ginkgo suite runtime, seconds | 498.112 | 498.312 |
| Hydrophone invocation elapsed, seconds | 544.992 | 546.246 |
| Hydrophone exit code | 1 | 1 |
| Serial specs actually executed | 0 | 0 |

There was no extra reporting failure in this rerun. All four selected failures on each architecture explicitly report the deliberate suite timeout. JUnit again contains all 462 expected names exactly once. The original first-run reporting failure remains evidence of that failed attempt; the rerun does not erase it.

ARM's interrupted specs were API chunking with expired continuation, the 500-updates Pod Generation test, projected-secret optional updates and Secrets optional updates. AMD's were API chunking with expired continuation, ResourceQuota status updates, GRPC liveness restart and the 500-updates Pod Generation test. Each artifact's `summary.json` and local `junit-analysis.json` contain the exact full names, runtime and failure/skip text. `failed-names.txt` and `pending-names.txt` preserve the failed and unrun selected names.

CronJob's five-minute observation passed again, in 300.130 seconds ARM and 300.035 seconds AMD. The API chunking case again occupied a process until timeout, with JUnit runtimes 497.764 seconds ARM and 497.755 seconds AMD. This repeat is evidence of real suite cost, not a passing full run or a flake rate.

Raw results are under `evidence/run-38317470141/`. The GitHub workflow is green only because this is a collection experiment; both Hydrophone exits are 1. Full conformance remains unproven on either free runner and completely untested against tigerstore here. The supported next step is a separately authorized bounded full nightly run with the proposed 150-minute job budget, not an invented certified shard merge.

The downloaded release test binaries, test archive, temporary probe outputs and incomplete source-clone data have been deleted. Source excerpts, scripts and evidence remain. No long job, full shard campaign, issue comment or merged code was created.
