# First runner attempt, excluded from the final comparison

Run: https://github.com/djosh34/tigerstore/actions/runs/38378542755
Source: ae0c2ecc639bb49c723dc1efd6260424a9028a82.

- The targeted ARM lifetime race gate passed.
- The 64-client job stopped in the ring-environment correctness gate: `TestAllocationValueLifetimes/base` hit the ordinary test fixture's 200ms leader disk fence. The preceding ring census succeeded. No timed 64-client cohort ran.
- The 256-client job passed all four ordinary gates and measured the first pristine-base cohort. Ring seed then stopped with `invalid GC CPU delta`: the copied generator requires a positive runtime CPU-class delta, but these short cohorts can have no intervening GC. The incomplete cohort is not included in the final five-repetition comparison.

Follow-up changes do not alter production disk-fencing policy or the pristine production source. Only allocation fixtures use the production 100ms tick, making the same watchdog approximately 2s; other fixtures retain 10ms. Census profiling runs in isolated test processes. The new test fixtures are also installed on the pristine archive. Only primary 60000-Put cohorts use the generator's runtime allocation/GC metrics; seed, warmup and separate exact-count profile cohorts retain success/revision/CPU validation without that optional metric gate. No forced GC was added to timed cohorts.

Raw gate logs, census profiles, the incomplete base result, ring seed failure, race log and run metadata are retained here. Matching first-attempt symbol binaries remain in the Actions artifacts (downloaded locally under `/tmp/alloc50-first-artifacts`). Final-run symbols will be retained with the final evidence.
