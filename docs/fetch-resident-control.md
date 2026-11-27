# #897 named resident control (measurement scratch branch)

This tooling branch starts from PR head `2f61154a` and does not change that
measured PR head. Node binaries are built from explicitly pinned main/head
checkouts; artifact source overlays expose only existing statistics at scrape
boundaries. No new hook, map, counter or retained value is added on an RPC,
record lookup, fetch or cache-hit path. The generator adds p95 from its existing
histogram and preserves both existing synchronized `/metrics` snapshots.

First cell: named **range-1k**, RTT **0**, **3 GiB** actual memory budget on
all three nodes; 10,000 keys of 1 KiB; fixed initial max-load step of **6
connections x 256 streams**, rather than independently selected ramp steps;
3-second warmup and 10-second measured windows; seed **94125874**; placement
(generator/leader/followers) CPUs **3/0/1/2**. Both use a fresh 512-MiB safe
log ring, ext4/barriers, identical TLS and same-member leader routing. Full
original main/candidate source patches, immutable refs, runner data, node logs,
placements, raw JSON, raw hashes and synchronized source samples are retained.

A cell qualifies **only as a resident control** if, for every node:

- actual observed budget is exactly 3 GiB and routing matches the pinned side;
- total worker fetch jobs/values **since boot** are zero at both boundaries;
- the fetched-value cache is empty at both boundaries;
- there are no measured value reloads; the ordinary slab is populated.

Together the cumulative producer counter and cache samples prove that the
legacy fetch cache was never populated: originating fulfillment, same-consumer
cache reuse and genuine cross-request cache reuse are therefore all **zero**,
not unmeasured positive claims. No cold/cache-removal result is inferred.
Unknown/missing/positive fetch/cache samples fail qualification, not pass.

Both sides must retain the exact workload/load/value/complete-RPC cohort,
valid results and no errors; generator CPU must be <=0.8 core (the existing
benchreport rule), and the actual fixed leader must use >=0.9 core. A fresh
nullstore diagnostic is archived, but its own server can be the bottleneck;
it is not an upper bound on generator capacity. Generator-limited, unsaturated
or invalid rows cannot count as wins or regressions. This is **not** the #602
put/txn/Kubernetes measured-ceiling campaign.

The initial attempt 46046218629 ran **only main**, no candidate and no pair.
The adapter incorrectly equated Ops with latency sample count. Existing
`stats.complete` counts Ops by completion window and latency by intended-send
cohort: validate histogram Count == Cohort instead, keeping CohortFailed=0,
Unsent=0, no errors and native validity. No driver/checker assertion changes.
That attempt's native main made 579,005.5 ops/s with 0.5402 client core and
1.0000 leader core, while nullstore made 172,202.8 with 0.3455 client core and
2.8420 server cores: the latter is server-limited, not the generator ceiling.
Both facts are retained rather than counting the unpaired attempt or inventing
a perf regression/win. Fresh complete pairs use the corrected source-grounded
qualification; the option-A throughput/noise rules are unchanged.

Apply the previously declared option-A policy unchanged: AB/BA/AB/BA/AB,
**every** throughput ratio >=0.98; each p95 delta <=1.4826*MAD of the five
main p95s. Stop immediately for a lower pair. P95 uses loadgen's existing
log-linear histogram (at most 0.8% bucket width); its new summary field does
not change collection, workloads or assertions. P99/max and raw samples remain
visible. Absolute diagnostic scripts/jobs/targets are unchanged.

Actions only. This initial control does not complete named/Kubernetes/cold/
refused/evicted/reuse or #602 gates; those remain separately required.
