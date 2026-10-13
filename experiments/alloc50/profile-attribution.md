# Allocation attribution — second runner attempt

Evidence: [Actions run 38381281559](https://github.com/djosh34/tigerstore/actions/runs/38381281559), `alloc50-64/c64/profile-{base,ring,arena}`. Original function-level views: [base](../../evidence/alloc50/attempt-38381281559/c64/profile-base/alloc_objects-attribution.txt), [ring](../../evidence/alloc50/attempt-38381281559/c64/profile-ring/alloc_objects-attribution.txt), [arena](../../evidence/alloc50/attempt-38381281559/c64/profile-arena/alloc_objects-attribution.txt). Source/lifetime reference: [pristine audit](source-audit.md).

**Final matching-symbol, zero-drop line views:** [base objects](attribution-second-run/base-alloc_objects-lines.txt), [ring objects](attribution-second-run/ring-alloc_objects-lines.txt), [arena objects](attribution-second-run/arena-alloc_objects-lines.txt); [base bytes](attribution-second-run/base-alloc_space-lines.txt), [ring bytes](attribution-second-run/ring-alloc_space-lines.txt), [arena bytes](attribution-second-run/arena-alloc_space-lines.txt). [Managed-site counts and bytes TSV](attribution-second-run/managed-sites.tsv).

Base source `0d406cb09aa99dea7ef1488476d6ebc6b032a6b8`, experiment source `191e12ca104c78f1646ee0481cec29ca209118b8`; profile build IDs respectively `a9a96873a2506671ca0d1ec3728dbc3f952ca53c` and `b5661be76f476b97a4fd67a56a4a8878bf39bb2b` (shared ring/arena executable).

## Scope and exact filtered totals

Each fresh diagnostic cluster profiled **2000 successful serial 1KiB Puts**, one worker, rate 1, after seed/warmup. Profiles are **leader-process only**, unlike the audit's in-process client + three-replica census. The load program creates **16 fresh TLS/gRPC connections** inside the interval, with 16 initial Range probes and two revision probes: **18 generic RPCs** alongside 2000 Puts. Handshakes, deadlines, GC and background peer/Raft work remain.

The final views use matching production symbols, `-top -lines -nodefraction=0 -edgefraction=0`, and `-ignore=runtime/pprof|main.bench75Observe|net/http\.`. Bytes additionally use `-unit=bytes`. **Zero nodes dropped after filtering**: all flat entries, including 18-object generic remnants, are included. The filter excludes whole matching stacks, not every consequence of observation. Pprof's percentages/printed total retain the unfiltered denominator; the correct filtered sums are:

| Flat sum over every retained line | Base | Ring | Arena |
|---|---:|---:|---:|
| Objects | 248075 | 217916 | 217862 |
| Bytes | 24408000 | 19734280 | 19732432 |
| Object reduction vs base | — | 30159 | 30213 |
| Byte reduction vs base | — | 4673720 | 4675568 |

Counts are exact **profile-record deltas**, not logical allocation-expression invocation counts or MemStats Mallocs; tiny packing and compiler/inlining attribution still matter. Analysis executed only `go tool pprof` on supplied profiles/symbols, no build, test or benchmark. The original truncated function views are superseded for small-function counts below.

## Exact managed-path function changes

Names abbreviated to package/type/function; counts are flat, never cumulative.

| Flat function | Base | Ring | Arena |
|---|---:|---:|---:|
| `server.conn.newStream` | 4052 | 2036 | 2036 |
| `server.stream.receive` | 2018 | 18 | 18 |
| `server.conn.admit` | 4036 | 36 | 36 |
| `node.Node.SubmitBatch` | 2018 | 18 | 18 |
| `core.Node.Submit` | 2091 | 36 | 36 |
| `core.Node.queueWrite` | 2000 | 0 | 0 |
| `core.Node.applyPut` | **2000** | **0** | **0** |
| `node.completeAll` | 2018 | 18 | 18 |
| `server.conn.Complete` | 2018 | 18 | 18 |
| `server.conn.complete` | 2018 | 18 | 18 |
| `server.conn.putResponse` | 2000 | 0 | 0 |
| `server.stream.pushLocked` | 6054 | 54 | 54 |
| `core.encodeBatch` | **2000** | **2000** | **2000** |

`newStream` candidates split exactly into **2018 bound deadline callbacks** (`h2.go:547`) and **18 generic heap streams** (`h2.go:504`). Base also has 16 stream-map growth blocks (`h2.go:454`), explaining its reduction of 2016 rather than 2000. Candidate `Submit` retains 18 generic pending objects and 18 read-list arrays; base additionally has 55 timeout-list growth blocks. Those conditional contributions are outside the fixed 15-site count below.

The legacy server functions retain **only their generic contribution at these workspace sites**, not a per-Put contribution. In particular, `admit` has two 18-object array sites and `pushLocked` three 18-object descriptor-growth sites. `applyPut`'s zero flat confirms that the scalar wire Reply array contribution was removed too.

## 15-of-16 exact site check, including bytes

The original audit's stable explicit subset was 16 objects/serial Put. Each pristine site below contributed one object/Put in that audit. Line numbers refer to the corresponding production binary, not the current worktree. Ring and arena have identical candidate site counts/bytes in this table. A candidate dash means no sampled allocating wire-path counterpart, with **zero flat objects/bytes**, not an omitted low-count node.

| Pristine site → candidate shared/default site | Workspace | Base objects | Each candidate objects | Base bytes | Each candidate bytes |
|---|---|---:|---:|---:|---:|
| `server/h2.go:433` → `:504` | stream | 2018 | 18 | 645760 | 6336 |
| `server/stream.go:122` → `:130` | body | 2018 | 18 | 2304352 | 352 |
| `server/batch.go:73` → `:77` | request array | 2018 | 18 | 258304 | 2304 |
| `server/batch.go:74` → `:78` | tag array | 2018 | 18 | 32288 | 288 |
| `node/node.go:186` → `:191` | node batch | 2018 | 18 | 161440 | 1440 |
| `core/node.go:376` → — | wire pending | 2000 | 0 | 288000 | 0 |
| `core/node.go:511` → — | proposal array | 2000 | 0 | 16000 | 0 |
| `core/node.go:905` → — | wire Reply array | 2000 | 0 | 128000 | 0 |
| `node/node.go:408` → `:424` | Completion array | 2018 | 18 | 161440 | 1440 |
| `server/batch.go:159` → `:171` | completion queue array | 2018 | 18 | 48432 | 432 |
| `server/batch.go:194` → `:206` | finished array | 2018 | 18 | 96864 | 864 |
| `server/batch.go:243` → — | reply bytes | 2000 | 0 | 128000 | 0 |
| `server/stream.go:275` → `:284` | output HEADERS growth | 2018 | 18 | 96864 | 864 |
| `server/stream.go:278` → `:287` | output DATA growth | 2018 | 18 | 161440 | 1440 |
| `server/stream.go:298` → `:307` | output trailers growth | 2018 | 18 | 322880 | 2880 |
| **15-site sum** | | **30198** | **198** | **4850064** | **18640** |
| **`core/codec.go:37` → same** | **owned encoding, retained** | **2000** | **2000** | **2304000** | **2304000** |

**Exact managed aggregate suppression: 30198 − 198 = 30000 flat objects = 15 per successful Put**, in both candidates. The 198 survivors are precisely **18 generic RPCs × 11 shared sites**. Thus 15 workspace contributions were replaced; the sixteenth, owned proposal encoding, remains.

The 15-site raw byte reduction is **4831424 bytes**. Its canonical managed-Put signature is **4832000 bytes = 2416 bytes/Put**: eleven generic sites otherwise match, but 18 generic stream objects move from the 320-byte to the 352-byte size class, adding **576 bytes**, hence 4832000 − 576. Generic body sizes differ from 1KiB Put bodies, so do not obtain byte savings by multiplying each entire mixed-site average by 2000. This is removal of churn, **not a claim that startup pool memory was saved**.

Whole-profile suppression is 30159/30213 objects, not exactly 30000. The extra net 159/213 comes from conditional capacity/infrastructure and background differences outside this managed aggregate; it must not be credited as further per-Put allocator savings. The whole-profile byte reduction likewise includes new connection workspace and other non-managed differences. Several of the 15 sites are per batch/turn: serial batches normally contain one Put. **Do not extrapolate 15 saved allocations/Put to the primary 64-/256-worker cohorts.**

## Residual work, not addressed by these prototypes

| Exact flat function | Base | Ring | Arena |
|---|---:|---:|---:|
| `time.newTimer` | 2034 | 2034 | 2034 |
| HTTP2 `readMetaFrame.func1` | 8072 | 8072 | 8072 |
| HTTP2 `readMetaFrame` | 7058 | 7015 | 7039 |
| HTTP2 DATA frame cache allocation | 2018 | 2018 | 2018 |
| HTTP2 `parseHeadersFrame` | 2018 | 2018 | 2018 |
| HTTP2 `parsePingFrame` | 2018 | 2018 | 2018 |
| HTTP2 `parseWindowUpdateFrame` | 2018 | 2018 | 2018 |
| Raft `maybeSendAppend` | 24170 | 24168 | 24174 |
| Raft `send` | 20306 | 20312 | 20314 |
| `core.Node.handleReady` | 7090 | 7059 | 7056 |
| `core.Node.send` | 8153 | 8156 | 8156 |
| `peer.TCP.receive` | 8153 | 8156 | 8158 |
| `core.State.track` | 2000 | 2000 | 2000 |
| `mvcc.writer.latest` | 2000 | 2000 | 2000 |
| protobuf `mergeBytes` | 2000 | 2000 | 2000 |
| `node.Node.commit` | 4000 | 4000 | 4000 |

- **Timers/callbacks:** 2034 timer blocks (227808 bytes) plus **2018 bound callbacks** (32288 bytes) remain in every mode: 4052 objects / 2000 Puts = **2.026/Put including cohort overhead**. Of these, 2000 timers + 2000 callbacks correspond to the timed Puts. This is distinct from the audit's no-timeout raw client.
- **HTTP2/HPACK/TLS:** metadata/frame allocation and fresh handshakes remain. Connection setup was excluded from the original warmed audit but included here; writer-local/connection capacity setup must not be confused with per-request workspace churn.
- **Raft/peers/WAL:** message/scalar/storage-protocol/effects allocations and peer protobuf decoding remain; small count differences reflect scheduling/tiny packing/background work, not a new pooling result. `node.commit`'s 4000 blocks are persist-backlog array appends (`node.go:351` → `:366`), not surviving managed Completion arrays. WAL staging/transport copies are not equivalent to fresh allocations.
- **MVCC/Entry.Data:** key bookkeeping remains exactly 2000 at each displayed state/MVCC function, and proposal encoding/clone bytes remain 2000 each. The audit's three-replica 6/Put bookkeeping subset becoming 2/Put here is a **scope difference**, not an optimization. Committed Entry.Data still backs current/historical values; no entry-byte recycling or early acknowledgment was introduced.

These are allocations **not addressed**, not fundamentally unpoolable objects. Further reuse requires separate ownership, retained-alias, capacity and progress proofs. This serial allocation attribution establishes neither high-concurrency CPU/GC benefit nor a hard total-heap bound. Its successful profile cohorts also do not turn this attempt's failed slow-case fixtures into passing isolation evidence.

### Reproduction (analysis only)

Inflate `symbols/base.gz` and `symbols/experiment.gz` from this attempt into `/tmp/alloc50-pprof-symbols`. For each mode use the matching binary (`base` for base, `experiment` otherwise), and that mode's before/after profiles:

```sh
GOFLAGS=-p=2 GOMEMLIMIT=2GiB go tool pprof -top -lines \
  -nodefraction=0 -edgefraction=0 -sample_index=alloc_objects \
  -base="$DIR/allocs-before.pb.gz" \
  '-ignore=runtime/pprof|main.bench75Observe|net/http\.' \
  "$SYMBOL_BINARY" "$DIR/allocs-after.pb.gz"
# Exact bytes: replace sample index with alloc_space and add -unit=bytes.
```
