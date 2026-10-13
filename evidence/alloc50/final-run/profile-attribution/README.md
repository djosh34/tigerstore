# FINAL profile attribution — run 38386296161

**Confirmed: both ring and arena eliminate exactly 15 managed workspace objects and the canonical 2416-byte allocation signature per serial Put.** The sixteenth audit site, owned Raft batch encoding, remains one allocation/serial Put. This independently reproduces the second-run attribution using the **FINAL** profiles and **FINAL** production symbols, not the prior binaries.

Evidence: [successful Actions run](https://github.com/djosh34/tigerstore/actions/runs/38386296161), [source identities](../c64/sources.txt), [binary versions/hashes](../c64/versions.txt). Base `0d406cb09aa99dea7ef1488476d6ebc6b032a6b8`; experiment `da1581def021bc01df4ca32690b937be3dfa8232`.

| Matching final symbol executable | Profile build ID | Inflated executable SHA256 (matches final versions.txt) |
|---|---|---|
| `c64/symbols/base.gz` | `59693393254ee7d474d0c2a349105502cdcf69c1` | `d263d5d0824fb591ce30c0a29bcb287a4988716e5462c51d85f915c501b63b7c` |
| `c64/symbols/experiment.gz` (ring + arena) | `1f5f7bcfd93da894224e1bcae2661f17f7201aac` | `ada2362a9810b3f0296de207ee0dcdc72e43cd8f69f95264769e3f43fef76227` |

## Scope and method

Each final c64 diagnostic profile cohort validated **2000 successful, serial, 1KiB Puts**, one worker, rate 1, zero RPC errors. These are **leader-process** profiles, not the audit's client + three-replica process totals. The interval also contains 16 fresh TLS/gRPC connections and **18 generic Range RPCs** (16 initial probes + two revision probes), plus timers, GC and background work. Profiling is diagnostic, not a timed primary cohort.

[Full original pprof text views](tops.txt) contain all six final object/byte line views. [Full TSV views](fullviews.tsv) preserve every retained line's flat/cumulative count and classification. Analysis used matching symbols, `-top -lines -nodefraction=0 -edgefraction=0`, `-ignore=runtime/pprof|main.bench75Observe|net/http\.`; byte views add `-unit=bytes`. **No dropped nodes after filtering.** The filter excludes whole matching stacks, not every consequence of observation. Printed percentages/total retain the unfiltered denominator; only the complete flat sums below are filtered totals. These are exact profile-record deltas, not MemStats Mallocs or a universal zero-allocation guarantee; tiny packing/compiler attribution still apply.

Only offline `go tool pprof` analysis was executed, with `GOFLAGS=-p=2 GOMEMLIMIT=2GiB`. No test, benchmark, build, or production/harness edit was performed. Go analysis slot released.

## Exact 15-site result

The [pristine source audit](../../../../experiments/alloc50/source-audit.md) identified ten server, two node and three core transient sites, plus one owned encoding site. Objects below include the generic probes; ring and arena have identical managed-site results.

| Managed subgroup | Sites | Base objects | Each candidate objects | Exact suppressed objects | Canonical suppressed bytes/Put |
|---|---:|---:|---:|---:|---:|
| Server stream/body/arrays/reply/output | 10 | 20162 | 162 | 20000 | 2040 |
| Node batch + Completion array | 2 | 4036 | 36 | 4000 | 160 |
| Core wire pending + proposal array + scalar Reply array | 3 | 6000 | 0 | 6000 | 216 |
| **Total** | **15** | **30198** | **198** | **30000 = 15/Put** | **2416** |

**The 198 survivors are 18 generic RPCs × 11 shared sites**, not managed heap fallbacks. Site pairs refer to each final binary's source lines; zero means no flat contribution from the corresponding managed wire allocation, not a cutoff omission.

| Base site → candidate shared/default site | Workspace | Base objects | Ring/arena objects |
|---|---|---:|---:|
| `server/h2.go:433` → `:504` | stream | 2018 | 18 |
| `server/stream.go:122` → `:130` | body | 2018 | 18 |
| `server/batch.go:73` → `:77` | req array | 2018 | 18 |
| `server/batch.go:74` → `:78` | tag array | 2018 | 18 |
| `server/batch.go:159` → `:171` | outer completion array | 2018 | 18 |
| `server/batch.go:194` → `:206` | finished array | 2018 | 18 |
| `server/batch.go:243` → zero | reply bytes | 2000 | 0 |
| `server/stream.go:275` → `:284` | HEADERS descriptors | 2018 | 18 |
| `server/stream.go:278` → `:287` | DATA descriptors | 2018 | 18 |
| `server/stream.go:298` → `:307` | trailers descriptors | 2018 | 18 |
| `node/node.go:186` → `:191` | batch | 2018 | 18 |
| `node/node.go:408` → `:424` | Completion array | 2018 | 18 |
| `core/node.go:376` → zero | wire pending | 2000 | 0 |
| `core/node.go:511` → zero | proposal-pointer array | 2000 | 0 |
| `core/node.go:905` → zero | scalar wire Reply array | 2000 | 0 |

**Exact mixed-site bytes:** base 4850064, each candidate 18640; reduction **4831424 bytes**. Canonical managed suppression is **4832000 / 2000 = 2416 bytes/Put**. The difference is **576 bytes = 18 × (352 − 320)** added by generic stream objects moving size class in the prototype. The other generic remnants match; generic body sizes are not 1KiB Put sizes. This removes request-path churn, not startup-pool memory.

**Retained sixteenth site:** `core/codec.go:37`, `encodeBatch`, is **2000 objects / 2304000 bytes in every mode**: one owned allocation and 1152 allocated bytes/serial Put. Immutable Entry.Data/clone/history semantics were not pooled away.

## Whole-profile totals and non-managed allocations

| Complete filtered flat sum | Base | Ring | Arena |
|---|---:|---:|---:|
| Objects | 247774 | 217933 | 217974 |
| Bytes | 24398288 | 19731000 | 19743816 |
| Whole-profile object reduction | — | 29841 | 29800 |
| Whole-profile byte reduction | — | 4667288 | 4654472 |

Whole-profile object savings are slightly **below** 30000 because other net allocations offset the exact managed saving by 159/200 objects. Do not substitute those background/infrastructure differences for the managed aggregate, or infer primary 64-/256-worker savings from serial batching.

**No noisy lookup allocation was counted as managed quota/workspace:** `core/state.go:92` and `mvcc/ops.go:234` each remain **2000 objects / 96000 bytes in all three modes**, and are explicitly labeled `MVCC_bookkeeping_not_managed_quota` in the TSV. They are key bookkeeping, not slot capacity or body allocations. Generic Range pending/read arrays and response objects likewise remain separate. Quota/live-byte gauges come from `Server.AllocationStats()`, not heap samples: final ring/arena retention snapshots report held_slots=0, held_bytes=8388608 driver reserve, denied_count=0 and fallback_allocations=0 for these normal profile cohorts.

Deadline timers/bound callbacks, HTTP2/HPACK/TLS, Raft/storage messages and scalars, peer decoding, WAL staging, MVCC/history and retained Entry.Data allocations remain **not addressed**, not fundamentally unpoolable. Their further reuse requires independent lifetime/capacity/progress proofs. This attribution alone neither proves lifetime safety nor establishes a hard total-heap bound; final correctness/race/slow-client evidence is separate.

### Reproduction

Inflate the final c64 `symbols/{base,experiment}.gz` into `/tmp/alloc50-final-pprof-symbols`. For each `profile-{base,ring,arena}`, choose matching `base` or `experiment` and run:

```sh
GOFLAGS=-p=2 GOMEMLIMIT=2GiB go tool pprof -top -lines \
  -nodefraction=0 -edgefraction=0 -sample_index=alloc_objects \
  -base="$DIR/allocs-before.pb.gz" \
  '-ignore=runtime/pprof|main.bench75Observe|net/http\.' \
  "$SYMBOL_BINARY" "$DIR/allocs-after.pb.gz"
# Byte views: replace sample index with alloc_space and add -unit=bytes.
```
