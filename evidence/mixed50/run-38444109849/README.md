# Mixed benchmark after the PR 95 reviews

[Run 38444109849](https://github.com/djosh34/tigerstore/actions/runs/38444109849) passed all 18 windows: one max calibration per store, then five open-loop repetitions per store. The bench job took 33m21s. All windows are valid with zero failed cohort requests and zero unsent arrivals. Final modeled live keys range from 1,000,812 to 1,008,207.

Generator/scripts/report: `92f644a3a1a8dd76d5a4675c3f35012bd8f7f379`. Both main controls: `b5f2e28ef2edd94393d62952b1568763ebc12fe2`. Stock etcd v3.7.2. Both stores ran with `GOMEMLIMIT=3GiB`.

The etcd max calibration measured 2,838.7 ops/s. Its median is that single sample. The five rate repetitions offered `floor(2838.7/2) = 1419` Poisson arrivals/s to every store. Main baseline and candidate max samples were 12,148.8 and 12,711.8 ops/s. They did not set the rate. Max results have only one sample and are calibration points, not five-repetition estimates.

Free ubuntu-24.04-arm runner, four Neoverse-N2 cores, client and peer TLS with client certificates, disk write barriers on, no injected delay. Generator CPU 3, leader CPU 0, followers CPUs 1 and 2. Fresh three-node clusters, one million preloaded keys, up to 128 puts and 60 KiB per preload Txn at 300 Txns/s. Six request connections, 16 slots each, all endpoints. Warmup three seconds, measurement ten seconds, latency from scheduled arrivals through drain. Rate repetitions alternate target order.

Default GET/LIST/update/create/delete shares are 25/10/40/15/10, plus 27 resource-prefix watches. Ten percent of GETs use known absent keys when available. LIST limit is 500, with count, more and full KV checks. Selection skips listing groups and skips LIST groups with active calls. Cluster-scoped resources omit the namespace segment. Duplicate unary messages are rejected before changing the model. Matching is rechecked after watch readers stop.

Counts, bounds and means come from the 43 resource/bin rows of the Kubernetes 1.37 inventory on #50. The inventory has no finer within-bin histogram. Nonconstant bins use a scaled beta model on the exact observed min/max: alpha=8*p, beta=8*(1-p), p=(mean-min)/(max-min). Stochastic integer rounding preserves the exact mean in expectation. Constant bins retain their size. This is a fitted smooth model, not empirical resampling. Values are random pool bytes, not dump contents.

`summary.md` contains min/median/max over five repetitions for rate results. Calibration triples repeat the one sample and explicitly show Runs=1. CPU per watch event is omitted from mixed JSON and printed as a dash: total CPU also covers GETs, LISTs and writes. Raw JSONs, placements and post-run resource samples are retained here; node logs remain in the workflow artifact. Earlier runs are separate evidence, not pooled with this one.

From the `work/50-mixed` code checkout, report without running a benchmark (replace `<evidence-worktree>` with the evidence branch checkout):

```sh
GOFLAGS=-p=2 GOMEMLIMIT=2GiB go run ./cmd/benchreport <evidence-worktree>/evidence/mixed50/run-38444109849
```

Dispatch:

```sh
gh workflow run bench.yml --ref work/50-mixed \
  -f candidate=main -f baseline=main -f benchmarks=mixed-k8s -f modes=rate -f reps=5
```

This evidence is on `exp/50-mixed-evidence`, outside PR 95's diff. No local benchmark or million-key preload ran.
