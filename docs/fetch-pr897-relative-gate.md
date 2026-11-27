# #897 option-A relative measurement gate

Part of #833. This implements the manager's #897-specific ruling, separately
from the unchanged absolute-target diagnostic and #833's done-when evidence.

## Predeclared rule (before the new Actions runs)

Use five fresh pairs on an isolated runner, in order **AB, BA, AB, BA, AB**.
A is immutable main, B immutable candidate; matched workload, budget, routing,
cache preparation and instrumentation remain required. Retain all runs and all
RPC samples; never discard a losing pair or retry it as noise.

1. **Every pair** must have successful-throughput ratio `B/A >= 0.98`, not
   merely a passing median. A lower pair stops immediately for the manager.
2. Measure pair-to-pair p95 noise from the **five main p95 values only**:
   - `center = median(main_p95)`;
   - `MAD = median(abs(main_p95 - center))`;
   - `noise_ms = 1.4826 * MAD` (one robust sigma, in milliseconds).
   Each pair must satisfy `candidate_p95 - main_p95 <= noise_ms`.
   Candidate variability cannot enlarge its own allowance. There is no
   post-hoc floor, outlier removal, or borrowed 2% p95 allowance. Zero observed
   main noise means zero allowance. All five baseline values, MAD, multiplier,
   noise and individual deltas are emitted in the artifact/report.
3. Preserve the existing independent busy-refusal non-regression check. Report
   p99, maximum, cohort wall, reads/bytes, refusal totals/max, every pair's ratio,
   and median/IQR/min/max. P99/max are not hidden absolute cutoffs in this
   #897 option-A policy; #833's slow-tail/race/5-ms evidence remains separate.

P95 is the unchanged nearest-rank per-RPC quantile over 80 RPCs in the original
fixture, including busy retries. Five-pair noise cannot be asserted from fewer
than five pairs. A throughput failure does not wait for noise to be estimated.

## Deliberately separate implementation

- `scripts/fetch-pr897-gate.py`: the option-A policy above.
- `scripts/fetch-pr897-measure.py`: Actions-only execution and artifact assembly.
  It reuses the existing matched overlay builder and strict raw-sample adapter,
  **not** the absolute diagnostic's runner or decision function.
- Existing `bench.yml` opt-in `fetch_pr897=true`, with immutable full baseline /
  candidate SHAs, selects the separate relative job. `fetch_race` and
  `fetch_fsync_ms` retain their meanings. Do not also select `fetch_measure`.
- `scripts/fetch-original-measure.py`, its 1,000-ms/3,000-ms rules, its tests,
  the original workload overlay/assertions/deadlines, and the original
  `fetch-original` workflow job are **unchanged**. `fetch_measure=true` still
  selects that distinct absolute diagnostic. The manager's investigation of
  its absolute target is not pre-empted by this new #897 gate.

The original workload run is only the first campaign cell, not evidence that
named/Kubernetes/reuse/ceilings or the added simulator gates have passed.
Full-budget/long runs remain Actions-only. The final head must also pass the
complete planted inventory. No earlier cancelled simulator run counts as the
required completed aged images 1–8 or long seed.
