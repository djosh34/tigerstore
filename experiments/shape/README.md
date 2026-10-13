# Pre-#50 experiment

Read [REPORT.md](REPORT.md) for findings, [campaign.json](campaign.json) for exact dispatch inputs/source pins and [report-data/](report-data/) for full results. Nothing here is merged into main. All performance measurements used free `ubuntu-24.04-arm` Actions runners. The deadline was 2026-10-13 at 22:00 UTC.

## Evidence

[evidence/archives.json](evidence/archives.json) identifies 67 complete per-run archives, about 145 MiB compressed and 23 GiB extracted. They retain failures and all 28 superseded fixed-arrival runs. Two earlier pilots are separately identified and excluded in the campaign manifest; their Actions run links remain the record of those attempts.

Archives use Zstandard with a 128 MiB window. Every part and combined archive has a SHA-256. No archive part exceeds 95 MiB. All 67 archives were extracted and all 35,213 archived files were byte-compared with the original downloads; see [verification.json](evidence/verification.json). Generated summaries are not duplicated inside the archives. Explicit `expected-plan.json` files are offline additions from the dispatch manifest, distinct from the original runner `plan.json`.

Extract one run with Python 3.12+ and `zstd` installed:

```sh
python3 experiments/shape/extract-evidence.py /tmp/pre50-results \
  --run 38427584978
```

Omit `--run` to extract everything. Allow at least 25 GiB free space. The script verifies compressed bytes before extraction and uses the safe tar data filter. It adds a local completion marker after successful extraction.

Measured artifacts contain:

- `runner.txt`, build identities, hashes, mount/CPU/memory metadata and placement.
- `commands.json`, the exact cluster and loadgen commands.
- `load.json`, `audit.json`, before/after snapshots, saved queue/device samples and server logs.
- `plan.json` and any `failures.json`; missing or invalid windows are not silently dropped.
- Matching binaries and raw captures for the CPU/trace diagnostics.
- Exact source overlays and test logs for the cap/count controls.

A build failure can have only test/overlay files and the offline expected plan. The original Range C4096 startup failures lack measured load records. Neither case is converted into a passing measurement.

## Recalculate offline

After extracting all runs:

```sh
python3 experiments/shape/expected-plans.py /tmp/pre50-results
python3 experiments/shape/analyze-campaign.py /tmp/pre50-results
python3 experiments/shape/build-report-data.py /tmp/pre50-results
sha256sum -c experiments/shape/evidence/report-data.sha256
python3 experiments/shape/open-calculations.py \
  --results /tmp/pre50-results \
  --campaign experiments/shape/campaign.json \
  --output /tmp/pre50-open-audit
python3 -m unittest discover -s experiments/shape -p 'test_*.py'
```

The campaign analyzer deliberately excludes `superseded-arrival-bug` runs from current results. It records parser/missing-window failures in `summary-status.json`; a per-run summarizer can return nonzero for missing records. The closed headline table explicitly substitutes the successful port-only retry for the ten original Range leader-C4096 startup failures. Both attempts remain in the per-window data and archives.

`build-report-data.py` computes CPU utilization per window from the recorded cohort CPU clock before aggregating. It withholds numerical cell aggregates unless every planned window is valid. The CSV retains invalid and missing rows. Its `path` column is relative to the extracted results root. A failed paired job can still contain five valid windows for one store; the tables mark that distinction.

The independent profile and observation-control decoders have their own reproduction commands in [profile-analysis/](profile-analysis/) and [observation-control-analysis/](observation-control-analysis/). Local Go commands must use `GOFLAGS=-p=2 GOMEMLIMIT=2GiB`; tests also use `-parallel 2`. Offline profile decoding does not execute the benchmark binary. Recorded raw `go tool pprof` text retains its original trailing spaces and hashes; a narrowly scoped `.gitattributes` disables whitespace diagnostics for those raw outputs only. Source and report whitespace checks remain enabled.

## Historical measurement commands

The exact historical source and complete input set for each dispatch are in `campaign.json`. The workflow at that source is `.github/workflows/bench.yml`. These examples show the recorded input grammar, not permission to start jobs after the deadline.

Closed leader Put cells:

```sh
gh api repos/djosh34/tigerstore/actions/workflows/bench.yml/dispatches \
  -f ref=exp/pre-50-shape \
  -f 'inputs[cases]=["put-1k-leader-c256","put-1k-leader-c1024","put-1k-leader-c4096"]' \
  -f 'inputs[reps]=5' -f 'inputs[duration]=3s' -f 'inputs[warmup]=1s' \
  -f 'inputs[rates]={}' -f 'inputs[targets]=tigerstore etcd' \
  -f 'inputs[profile]=none' -f 'inputs[conns]=18'
```

A corrected same-rate point-read comparison, recorded at commit `223f8f6aad7923ff186ac77a2c9fc29b9f140c86`:

```sh
gh api repos/djosh34/tigerstore/actions/workflows/bench.yml/dispatches \
  -f ref=exp/pre-50-shape \
  -f 'inputs[cases]=["range-1k-leader-open"]' \
  -f 'inputs[reps]=5' -f 'inputs[duration]=3s' -f 'inputs[warmup]=1s' \
  -f 'inputs[rates]={"tigerstore":[1000,3000,5000],"etcd":[1000,3000,5000]}' \
  -f 'inputs[targets]=tigerstore etcd' \
  -f 'inputs[profile]=none' -f 'inputs[conns]=18'
```

The archived `commands.json` is authoritative for each individual cohort. It records TLS files, endpoint order, PIDs, concurrency, value/key counts, seed, warmup, duration, arrival rate, balance/dispatch mode, response limit and ready/go/done/finish barriers. `bench/cluster.sh` records all server flags and reuses the same CPU placement for both stores. `run.sh` records disk barriers and guards the deadline.

To repeat a historical measurement, first obtain authorization for a new deadline and use a new `exp/` branch from the recorded source. The hard deadline guards intentionally reject later measurement starts. Do not remove durability barriers or reuse an old source's timing bug. Offline evidence extraction and recalculation do not need a runner.

## Controls and limits

`build-caps.sh` applies one exact source replacement to an archived source tree. It builds either byte-target variants or the count-window variant used in the campaign, not arbitrary combinations. Recorded `.diff`, source/binary hashes and test output identify each binary. A 1 MiB packing target is not a new request-size limit: oversized legal requests remain intact.

The pristine/disabled/enabled observer control is separate from physical-counter measurements. It uses the common native applied/revision checks and cooldown described in the report. The cooldown is not an all-disk-job proof. Its summaries omit disk/fsync/owner-queue claims.

The client assigns balanced request tickets round robin, with endpoint totals differing by at most one. Socket chronology can differ because writes are buffered per connection. Fixed arrivals are independent of replies, but a finite concurrency limit can queue admission. The latency called `service` begins at admission before flush; it is not isolated server execution time.

No result here establishes a Kubernetes SLO, a three-machine capacity limit or a full crash/linearizability campaign. Read the validity flags and full ranges before comparing medians.
