# Offline observation-control analysis

The script reads saved artifacts from runs 38424937611 and 38424941260. It does not invoke Go, subprocesses, servers, tests, benchmarks or network calls. It makes no measurements.

```sh
python3 observation-control-analysis/analyze.py \
  --results-root /home/joshazimullah.linux/work_mounts/scratch/pre-50-shape/results/observation-controls \
  --output-dir /tmp/tigerstore-observation-controls
```

To regenerate the committed archive, use `--output-dir observation-control-analysis/archive`. The script rejects output paths inside the input artifact tree.

## Evidence

- `archive/computed-tables.md` contains absolute median/min/max results for every target and paired percentage changes for all three Tiger contrasts. It includes p50, p99, p99.9, cohort and window CPU, leader CPU and generator CPU.
- `archive/computed.json.gz` preserves all 120 raw load results and audits, derived integer server CPU deltas for both windows, wider external CPU deltas, reconstructed window clocks, PID identities and pre-cohort revisions. Every summary includes the five individual values and every paired change includes the five individual repetition-number ratios.
- `archive/input-manifest.json` hashes all 630 consumed files and the analysis script. It records that no subprocesses ran and no physical claims are made. It hashes the original before/after snapshots, but the computed archive extracts no file/fsync/IO/disk or owner-queue counters from them.

```python
import gzip
import json

with gzip.open("observation-control-analysis/archive/computed.json.gz", "rt") as stream:
    data = json.load(stream)
print(data["paired_percent_changes"]["put-1k-leader-c256"])
```

## Checks and formulas

The script checks six plans with five repetitions and four ordered targets each, 120 valid complete closed cohorts, expected revisions, balanced admission where requested, stable PID/starttime identities, placement, disabled versus enabled invocation and binary manifest equality for on/off. It checks the common native-applied status and 0.3-second cooldown proof for all 90 Tiger windows. This is not a persistence-completion proof.

Cohort CPU uses `shape.cpu_through_drain.server_us_per_op_by_node` and the successful arrival cohort denominator. Window CPU uses the legacy completion-window values and `load.ops`. Multiplication by the respective denominator recovers integer user-plus-system CPU nanoseconds, each a multiple of Linux's 10 ms USER_HZ tick. The script checks both against the wider external process snapshots and reconstructs the window elapsed time from per-node CPU deltas and reported cores. Client CPU is loadgen only; the scraper's CPU3 cost is unrecorded.

Throughput is successful cohort operations divided by dispatch time through drain. Quantiles describe complete successful cohorts. Changes are `100 * (numerator_rep / denominator_rep - 1)`, followed by the median and minimum/maximum of the five changes. The median paired ratio can differ from the ratio of separate medians. No quantiles are pooled and the ranges are not confidence intervals.

The archive includes all five pre-cohort revisions as evidence of unmatched time-based Put warmup. They include seeded/probe/warmup state, not exact warmup counts. Read warmup counts are not recorded. The order alternates: pristine/off/observed/etcd in repetitions 1, 3 and 5, reversed in repetitions 2 and 4. This deterministic alternating order and unmatched warmup remain limitations.

Neither the report nor these tables use physical counters. A native applied fence plus cooldown cannot justify file/fsync or disk attribution. The enabled treatment combines server observation, debug HTTP service and external scraping/sampling on CPU3; it does not isolate in-server hook cost. All old fixed-arrival claims remain retired. Corrected-generator results are reported separately.
