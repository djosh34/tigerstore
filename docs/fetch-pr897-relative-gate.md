# #897 option-A measurement gate and #913 design information

Part of #833. The manager's added ruling assigns the original 1 s / 3 s
per-range design figures to [#913](https://github.com/djosh34/tigerstore/issues/913).
They are **information in #897's report, not a merge gate**.

## Predeclared gate (unchanged)

Use five fresh pairs on an isolated runner, in order **AB, BA, AB, BA, AB**.
A is immutable main, B immutable candidate; matched workload, budget, routing,
cache preparation and instrumentation remain required. Retain every run and
RPC sample; never discard a losing pair or retry it as noise.

1. **Every pair** must have successful-throughput ratio `B/A >= 0.98`, not
   merely a passing median. A lower pair stops immediately for the manager.
2. Measure p95 noise from the **five main p95 values only**:
   - `center = median(main_p95)`;
   - `MAD = median(abs(main_p95 - center))`;
   - `noise_ms = 1.4826 * MAD` (one robust sigma, in milliseconds).
   Each pair must satisfy `candidate_p95 - main_p95 <= noise_ms`.
   Candidate variability cannot enlarge its own allowance. There is no
   post-hoc floor, outlier removal, or borrowed 2% p95 allowance. Zero observed
   main noise means zero allowance. Publish all baseline values and deltas.

Those are the **only performance-verdict conditions**. Preserve p99/max and
refusal regression checks as reported information, not additional gates.
Correctness, complete samples, routing, source/share assertions and preparation
remain prerequisites; failed or unqualified tests never count as performance
wins. Report cohort wall, reads/bytes, refusals and median/IQR/min/max as before.

## Original campaign and design information

`scripts/fetch-original-measure.py` uses `scripts/fetch-pr897-gate.py` for the
gate, evaluates each pair, and finishes all five unless a real gate or
correctness/qualification failure stops it. There is no extra-five rule.
`scripts/fetch-pr897-measure.py` uses the same original decision function;
both existing workflow opt-ins (`fetch_measure`, `fetch_pr897`) agree.

The literal `limit = 3000 if race else 1000` stays unchanged. Each completed
pair and report records both sides' p95/p99/max, the applicable limit and
whether each side exceeds it. A candidate miss is labeled:

> information: over #913's 1 s / 3 s design figure (main: …)

It does **not** add a failing reason, emit `RULING`, or stop more pairs.
A slower p95 can still fail ruling A independently; a faster candidate above
an absolute figure can pass. Original overlays, values, fetch/share checks,
retry sleeps and actual test deadlines are unchanged. P95 is the unchanged
nearest-rank quantile over the original 80 RPCs, including busy retries.

## #833's separate close condition

The slow-share test must stay reliably inside **its own deadline with headroom
under race**, plus the race and **5-ms-fsync** campaigns must show the tail cut
against main. The #913 design figures neither replace that condition nor define
#897's merge gate. Do not infer #833 closure from synthetic policy tests.

The PR report lists main's five baseline p95s with immutable refs and Actions
links for #913. Historical stopped runs remain historical; this ruling does
not waive an actual sub-0.98 pair, simulator failure, or incomplete campaign.
The original workload is not named/Kubernetes/reuse/ceiling evidence. All
full-budget/long runs stay on Actions, and the final merge head must catch the
complete planted inventory.
