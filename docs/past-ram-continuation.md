# Past-RAM continuation for #148

This continues `74e08fe1` on `prep/148-past-ram-2`. The earlier measurements
and their limits remain in [past-ram-tests.md](past-ram-tests.md). All builds,
formatting, tests and process execution run in GitHub Actions. No agent ran
product code on the shared host.

## Recorded memory proof

The three servers still have separate 640 MiB cgroups, including their page
cache, no swap, and `GOMEMLIMIT=560MiB`. The large preload still uses the
measured Kubernetes size histogram and reaches 2,080,401,455 live value bytes.
Each member must observe the entire preload before the controller starts
load. Member 2 is killed after 30 seconds and restarted after five seconds.

The workload now separates two key ranges:

- `/registry/` holds the large, immutable preload. A bulk client issues cold
  point reads and bounded LISTs. It checks every returned value against the
  generator, then checks every key and byte through all three members after
  load. No bulk key is mutated during the measured interval.
- `/proof/` starts empty. Three concurrent clients, each pinned to a member,
  send Put, Delete, Get, limited LIST and CAS Txn requests. Values follow the
  same size histogram. A fourth client grants, uses and revokes leases.
  Three watches request `prev_kv`, resume through the restart, and must reach
  the terminal revision. Compaction runs after that completeness fence.

All mutations after preload are recorded. Each RPC records its own send and
receive times. Failed mutations remain ambiguous. The driver never retries
one and never changes an uncertain result to a rejection. The healthy suffix
waits beyond the server request timeout, reads the proof range through each
member, and uses failed CAS operations with mutating branches to advance the
durable commit hint. Empty Txns do not provide this fence on etcd.

After every process stops, the driver independently decodes each member's
committed WAL suffix. It requires the initial `/proof/start` request and
agreement between the three committed prefixes. The longest prefix supplies
the log to the checkers. No log is reconstructed from successful replies or
watch events. A lost start marker, unsupported request or missing log fails.

The proof range starts at a later global revision because of preload. The
driver subtracts that fixed offset from revision fields in the history and
log. It preserves values, key names, versions, lease IDs, errors and times.
Tests check this conversion and require a missing watch event to fail after
conversion. `recorded.json` retains the unmodified observations;
`history.json` retains the normalized checker input with the decoded log.

Porcupine, both witness modes, Watches, Durable and Revisions run on the
complete proof-range history. Each search checker has a two-minute limit;
a timeout fails. This is a range-isolated check, not a claim that the six
checkers modeled millions of immutable preload values. The independent
bulk-value oracle and final scans cover those values. No unrecorded mutation
can create a revision gap during the checked interval.

The controller samples CPU and storage reads before load and after all
writers stop, before verification. `summary.json` reports bulk-client rates
and latency; `recorded-load.json` reports concurrent RPC rates, latency and
failure counts separately. Neither is the previous sequential workload's
throughput, nor a production benchmark. The small etcd case uses a 16 MiB
preload solely to check the same recorder and all six checkers against the
reference. It is not the required past-RAM population.

## Production cleaner simulation

`TestConcurrentCleanerSimulation` runs the actual `Node.Run`, persistence
queues, checkpoint writer, readers and paced `cleaner.run` on a locked
`simfs.Disk` inside `testing/synctest`. Production still uses `osfs` and its
mappings. The private `newOnDisk` constructor lets the test use `File.ReadAt`
instead of OS mappings; it does not replace the cleaner algorithm.

Eight seed values vary yields at filesystem calls. Cold readers wait two
virtual milliseconds, snapshot streams wait 30 milliseconds per write, and
clients overwrite and compact while the cleaner runs. Every schedule must
copy packs, remove segments, serve snapshots and complete cold reads. The
final crash copy is taken while the driver is live, before orderly shutdown.
Recovery compares its values with the client's acknowledged values.

Test-only observations in the production cleaner capture durable copies
after pack sync, after buffering the replacement root, after root sync and
after removal. The filesystem also captures its first durable segment
removal. Each copy must reopen and resolve the chosen durable checkpoint's
value references with valid checksums. The earlier sixteen distributed
simulator histories still check recovered values and retained references
against their committed history after physical segment removal.

These are concurrent node-driver simulations with a virtual clock, not
bit-for-bit replay of the distributed simulator. They exercise snapshot
sending, not a new three-node snapshot-install campaign. Existing distributed
and real-file tests remain enabled.

Two Actions controls mutate `internal/node/clean.go` itself. One removes
victims before installing and syncing the replacement root. The other omits
one live checkpoint value from the pack. Both must fail seed 1 with lost-value
evidence. The prior stale-cache mutation remains required too. The controls
run in the reusable workflow. Integration assigns B29 to early removal, B30
to an omitted checkpoint value and B31 to the stale cache lookup. They are
source mutations, not production build switches. Each must fail its one fixed
scenario with the named data-loss assertion; a build error is not a catch.

## Etcd index allocation

The workflow checks out etcd v3.7.2 at
`68c065e562994b89e333e77b039ad066f933c586`. It copies the measurement test into
upstream's `server/storage/mvcc` package and calls the real `newTreeIndex`
and `Put`. Keys have the exact resource histogram and byte lengths used by
the preload, with one revision per key and 64 keys per transaction revision.

Two forced collections bracket the population, with the index kept live
through the second heap sample. The difference includes owned key bytes,
key indexes, generations, revisions and B-tree allocations. It excludes
values, the backend, Raft, network buffers and page cache. Three repetitions
at 100,000 keys, at the earlier capacity counts of 457,472 and 463,680, and
at this continuation's counts of 463,744 and 470,144 measure the size effect. This is isolated live Go heap allocation, not an
attribution of RSS from a running etcd server. Tigerstore's earlier metric
estimate and this heap measurement use different methods.

## Results

[Measurement run 38949987426](https://github.com/djosh34/tigerstore/actions/runs/38949987426)
used `009888cd`. Tigerstore's proof passed all six checkers on 5,931 recorded
operations and 3,690 decoded committed requests. Every watch reached the
target. All three members passed the full bulk-value scan after one kill and
restart; no member had an OOM kill. The largest checker time was 26 ms.

The bulk client completed 30,605 operations in 180.010 seconds, 170.018/s,
with p50 0.827 ms and p99 49.465 ms. Concurrent recorded clients completed
5,920 calls, including 136 failed calls retained in the history, at 32.887/s,
p50 1.633 ms and p99 43.419 ms. Aggregate server CPU was 186.987 CPU-seconds.
These are separate workload streams, not single-RPC latency for the bulk
LISTs plus concurrent writes combined.

Etcd's required proof failed during preload after 470,016 acknowledged
objects and 686,350,721 value bytes; a member was OOM-killed. There is still
no etcd mixed-load result at the required population. The small etcd control
passed all six checkers on 1,930 operations and 1,192 committed requests,
including its kill, restart, watches, leases and compaction.

The same run repeated the capacity cases:

| Product and quota | Acknowledged objects | Live value bytes | First limit |
| --- | ---: | ---: | --- |
| Tigerstore, uncapped | 2,773,248 | 4,045,368,848 | Cgroup OOM |
| Etcd, 8 GiB | 470,144 | 687,787,211 | Cgroup OOM |
| Etcd, effectively uncapped | 463,744 | 678,348,675 | Cgroup OOM |

One more 64-Put transaction may have an unknown outcome in each case.
Tigerstore's sampled index estimate was about 164.55 bytes/key. The upstream
index test measured median 195.075 live heap bytes/key at 470,144 keys and
195.076 at 463,744 keys, three repetitions each, in
[38951006186](https://github.com/djosh34/tigerstore/actions/runs/38951006186)
on `c18b1303`. The earlier populations gave about 195.07 too. This does not
establish which allocation caused the process OOMs.

Both cache-allocation cases passed full value verification:

| Allocation | Reads/s | p99, ms | Server CPU-seconds | Cache hits / misses | Physical read bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Normal | 1,628.595 | 1.123 | 251.988 | 193,716 / 97,414 | 366,637,056 |
| Disabled | 1,612.416 | 1.124 | 249.787 | 0 / 288,238 | 403,374,080 |

Normal caching reduced physical reads. This pair was slightly faster with
the cache, unlike the first handoff's pair. Neither pair measures a kernel
hit percentage or supports changing the default. The repeated churn test
again reported 157,600 copied pack bytes / 10,051 acknowledged Puts, or
15.68 bytes/Put. It excludes WAL, checkpoint, root and journal writes.

[Storage run 38950429153](https://github.com/djosh34/tigerstore/actions/runs/38950429153),
`34184a61`, passed all eight concurrent production-cleaner schedules under
race, all five durable cuts per schedule, the sixteen earlier distributed
histories, driver regressions, selected storage suites, vet, lint and
formatting. Schedules served 16-17 snapshots and 514-571 cold reads each,
and removed 25-29 segments each. Both production mutations failed at seed 1
on a lost checkpoint value, as required. The stale-cache control passed too.
Run 38951006186 repeats these tests with an explicit assertion that original
segment 1 is absent and a positive durable checkpoint at every cut.

The measurement matrix remains red because maximum-capacity cases stop at
OOM and etcd cannot finish the proof preload. No new tigerstore defect was
proved. These capacity boundaries, deliberate mutation failures and the
following test-setup failures are not product bug issues.

### Earlier attempts

- [38948536184](https://github.com/djosh34/tigerstore/actions/runs/38948536184)
  measured the index successfully. Storage failed lint. The small etcd
  checker adapter returned `invalid argument` when closing an upstream
  read-only WAL whose directory handle was nil. Tigerstore's adapter mistook
  its `member` file for an etcd directory. Neither checker run passed.
- [38948864037](https://github.com/djosh34/tigerstore/actions/runs/38948864037)
  timed out after ten minutes in the new simulated disk. It slept while a
  caller held a production mutex. Synctest cannot advance time through
  mutex contention. Filesystem calls now yield without sleeping; delayed
  readers and snapshot writers sleep outside that lock. This was a test
  adapter deadlock, not a product deadlock.
- [38949206183](https://github.com/djosh34/tigerstore/actions/runs/38949206183)
  passed storage. The small etcd history failed its log witness and Durable
  check because the empty terminal Txns were read-only and the final
  compaction lay beyond the WAL's durable commit hint. The other four
  checkers passed. Failed CAS barriers fixed that test fence. Tigerstore
  still hit the directory-detection mistake in this attempt.
- [38949617963](https://github.com/djosh34/tigerstore/actions/runs/38949617963)
  passed both complete process histories, storage and production mutation
  controls. Etcd's large preload still failed at OOM. The later measurement
  uses histogram-sized mutation values and stops writers before the ending
  CPU sample; keep the earlier throughput numbers separate.

## Integration

`prep/int-8-past-ram` takes `74e08fe1` and `5395c610` onto main at
`e5b29278`, without either prep-only commit. The soak already supplied the
identical `internal/k8ssize` files. The required CI gate now includes storage,
the B29-B31 mutation controls and the upstream index measurement. Manual,
main and nightly CI also run every capacity case; pull requests omit only
those process measurements. The planted workflow runs the source mutations
too, and its inventory names their workflow instead of a Go package.

Run the memory proof on main after integration. Compression, the measured
two-second three-member cold-read election scenario and the final release
gate remain outside this continuation's new coverage.

### Capacity outcomes

The four red capacity jobs in run
[38951406613](https://github.com/djosh34/tigerstore/actions/runs/38951406613)
record measured limits, not a demonstrated product defect. This follows the
max-objects requirement in #144: grow the live population until something
fails, then name that limit and count. A green capacity job now means it
recorded that limit, not that the server survived it.

| Case | Acknowledged objects | Live value bytes | Final cgroup OOM kills |
| --- | ---: | ---: | --- |
| Tigerstore, uncapped max | 2,794,304 | 4,076,217,555 | 0, 1, 0 |
| Etcd, 8 GiB max | 463,744 | 678,348,675 | 0, 0, 1 |
| Etcd, uncapped max | 457,984 | 670,027,745 | 1, 0, 0 |
| Etcd, proof preload | 463,680 | 677,194,644 | 0, 1, 0 |

All four used separate 640 MiB cgroups, swap disabled and `GOMEMLIMIT=560MiB`.
The driver log in the first and fourth cases ends with an unavailable Txn
and EOF during preload. The final cgroup counters caught kills that the
last two-second sample missed. The other two controllers observed the OOM
in their polling loop. No mixed load had started. Etcd's proof population
was below the required 2,080,374,784 bytes, so there is no mixed-load rate or
checker result at that population. The small etcd adapter control remains
required and is never accepted at OOM.

These observations prove the cgroup boundary, not which allocation exhausted
memory. Tigerstore keeps its index in RAM by decision #130; its sampled
index estimate near this boundary is about 164.55 bytes/key. Etcd's isolated
index measurement is about 195.08 bytes/key. Neither number attributes the
whole process's OOM to the index. No quota or disk-full error preceded the
recorded boundary. One additional 64-Put transaction may be ambiguous.

`scripts/past-ram.py` now emits `capacity-limit` or
`reference-preload-limit` only with three final cgroup reports, a positive
acknowledged preload, a recorded OOM kill and a matching termination. It
keeps the termination text and counters. Unknown driver errors, checker
failures, time limits, failures after preload, and OOMs in tigerstore's proof
or either cache case stay red. The classifier tests include the four earlier
observations and these rejection cases. Every measurement writes its outcome
and counts to the Actions summary as well as the retained JSON artifacts.
