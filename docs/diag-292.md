# #292 recovery-memory diagnosis (diagnostic branch only)

Base: main `2fef0b5d`; toolchain: Go 1.27.2 linux/arm64. This branch is
not a proposed production patch.

## Cause and profile

The suspicion in #292 is correct, with a more precise mechanism than repeated
append copying: this Go version's `io.ReadAll` keeps exponentially growing
**chunks**, then allocates a contiguous result while those chunks are still
live. `wal.Open -> openPacks -> openPack -> osfs.file.ReadAll -> io.ReadAll`
therefore requires substantially more than the pack size at the final copy.
The 64 MiB reproducer allocates **93.69 MiB in chunks + 64.09 MiB in the final
result**, excluding the small map/reader allocations. Neither collection nor
shrinking the value cache can make both live buffers fit in a 128 MiB cgroup
(or its 112 MiB Go limit). The ~63.7–64.0 MiB packs in the issue fall in the
same chunk-growth step. This is recovery failure, not evidence of lost writes.

Measured with a real osfs directory (not simfs), GC disabled only during the
recovery under test. Thus the heap increase is the high-water mark including
transient buffers, not a goroutine sampling lottery. It is also a conservative
bound across successive files: garbage from an earlier file is not collected.
For a single pack the final-copy overlap itself accounts for almost all of it.

| Test | Original pack reader | Bounded reader |
|---|---:|---:|
| 16 MiB pack, 32 MiB node budget | 36,342,176 B, FAIL | 215,424 B, PASS |
| 64 MiB pack, 128 MiB node budget | 165,751,200 B, FAIL | 436,480 B, PASS |
| 48 MiB live values, three ~16 MiB packs + checkpoint, 32 MiB budget | 108,984,760 B, FAIL | 679,136 B, PASS |

Small measurement variations (a few KiB) are runtime bookkeeping.

Red test originally committed as `6a6790bb`; bounded reader `fae33bea`;
expanded tests `417bb941`. Commands:

```sh
go test ./internal/wal -run '^TestPackRecoveryMemory$' -v -count=1
go test ./internal/core -run '^TestCheckpointRecoveryMemory$' -v -count=1
go test ./internal/wal -count=1
```

To run the expanded tests against precisely the original production reader,
without modifying the worktree:

```sh
git show 2fef0b5d:internal/wal/pack.go > /tmp/292-main-pack.go
python3 - <<'PY'
import json, os
json.dump({'Replace': {os.path.abspath('internal/wal/pack.go'):
                     '/tmp/292-main-pack.go'}},
          open('/tmp/292-red-overlay.json', 'w'))
PY
go test -overlay /tmp/292-red-overlay.json ./internal/wal \
  -run '^TestPackRecoveryMemory$' -v -count=1
go test -overlay /tmp/292-red-overlay.json ./internal/core \
  -run '^TestCheckpointRecoveryMemory$' -v -count=1
# Attribution, not just a heap total:
go test -overlay /tmp/292-red-overlay.json ./internal/wal \
  -run '^TestPackRecoveryMemory/64MiB$' -v -count=1 \
  -memprofile /tmp/292-red.pprof -memprofilerate 1
go tool pprof -alloc_space -list io.ReadAll /tmp/292-red.pprof
```

The profile assigns 98.68% of allocation space to io.ReadAll: 64.09 MiB on
its final-result allocation and 93.69 MiB on chunk allocations. Fixture writing
accounts for most of the rest. The test measures recovery only.

## Recovery-path allocation inventory

* `cmd/tigerstore.run` sets `debug.SetMemoryLimit(7/8 * budget)` before
  `node.New`; `Core.MemoryBytes` is 3/4 of that. At 128 MiB these are **112
  MiB and 84 MiB**, not a separate 84 MiB allowance for values. The limit is
  soft and is not an allocation/admission limit.
* `wal.Open` first lists filenames and sorts segment names. Directory lists,
  open reader/sizes maps and run objects scale with file/entry count, not
  value bytes: approximately 80 B/run, 8 B/entry for offset+CRC arrays (plus
  slice growth), 32 B/sealed segment. Snapshot value-record locations add
  approximately 48 B/index + 24 B/value record; roots hold 8 B/pack number.
  Append-capacity overhead is additional, modest for the issue's record counts.
* **Segments still use File.ReadAll** one at a time. A normal default-size
  64 MiB segment has essentially the same ~158 MiB whole-file-read problem,
  even with this pack-only fix. The #292 replay overrides segment size to
  1 MiB (and window to 1 MiB); one such read is only a few MiB, while cleaning
  aggregates many segments into a ~64 MiB pack. The passing rotation control
  thus does not establish safety for production-default 64 MiB segments.
* Originally `openPack` has the chunk+result peak above, then appends one
  24 B position per value; sorting does not copy the payload. A 64 MiB pack
  containing 96 KiB cold values has roughly 683 positions (~16 KiB), or
  roughly 4096 positions (~96 KiB) at 16 KiB/value. Append growth is metadata
  only. `MapBytes` is an estimate; clipping slice capacity does not release
  its underlying allocation. Our 4096-value fixture reports 98,448 B for
  the WAL map and allocates <427 KiB in the entire fixed WAL recovery.
* `nextRun` separately calls File.ReadAll on the run-number file, normally
  **8 bytes**. It accepts a zero-filled tail for the LazyFS stale-size case;
  an abnormally large run file would still incur a whole-file allocation.
* `wal.Checkpoint` returns a ReaderAt-backed sequential reader, not ReadAll.
  `core.loadCheckpoint/readCheckpoint` uses a 64 KiB bufio reader plus a
  trailer CRC buffer of about one read. `mvcc.Store.Load/readKey` allocates
  key bytes, key records, B-tree nodes and **64 B/version**, not value.Len
  bytes. Its estimate is 64 B/key + key length + 64 B/version; for 2048
  `/pressure/NNNNNN` keys with one version each that is **294,912 B**,
  irrespective of the **192 MiB** of live value payloads. Version arrays
  grow with retained history; lease key sets duplicate leased key strings.
  Header members/users/roles/leases scale with their metadata, not packs.
* Committed replay uses `wal.Entry`: one allocation of **entry payload +
  18 B**, bounded by MaxEntryBytes (64 MiB), plus its protobuf/request/response
  metadata. The usual issue puts are 96 KiB cold or 16 KiB hot values;
  decoding aliases their entry and MVCC retains references, not the entry.
  Temporary operation results/cold reads depend on the replayed request
  (compare reads are batched at 1 MiB). TrackWritten metadata scales with
  replayed puts until the final Leaving call. Checkpoint effects produced
  while replaying retain serialized index chunks, not live value payloads.
  Record mode, off in tigerstore, would retain Applied entry bytes.
* `core.New` replays committed entries directly from disk, with no applied
  entry window. `fillCache` cannot populate the cache from those entries:
  they never entered storage. Checkpoint Load never reads a value. The
  initial cache is empty and its initial limit is zero. Uncommitted entries
  after HardState.Commit **are retained** by `storage.add` until Raft deals
  with them: O(uncommitted payload bytes), not a recovery-specific hard
  MemoryBytes bound. Large entries/tails or expensive replayed operations
  remain independent risks; this experiment does not prove arbitrary
  restart workloads fit any budget.

## Own memory accounting after the fix

`TestCheckpointRecoveryMemory` builds real files with 512 x 96 KiB live values
(48 MiB), one checkpoint, and three ~16 MiB packs. It executes both wal.Open
and core.New, then checks exactly the memoryGauge formula:

```
other = live heap + total - released - free heap spans - heap objects
cache limit = MemoryBytes - other
```

The cache is empty; the applied window is zero; index accounting is **73,728
B** and WAL maps **12,560 B**. Recovery allocates **679,136 B**. After GC the
whole test process (not just recovery's delta) accounts for **8,387,496 B**
against **22,020,096 B** of ProcessMemory (the scaled 32 MiB budget). This is
not a claim that the real server's baseline is 8 MiB: the Actions replay is
needed for gRPC, FUSE/page-cache/cgroup and catch-up effects.

## Other whole-file / snapshot paths

Current main already streams snapshot receive: `snapReader` has a 256 KiB
reader and one reusable item buffer; checkpoint items are 1 MiB. A value/item
can be larger (up to MaxEntryBytes+64), and decoded entries plus the WAL
append buffer temporarily duplicate data. `receiveCheckpoint` computes CRC
as chunks arrive and rereads only the checkpoint header; it does **not**
read the whole checkpoint into memory. Snapshot serving similarly streams
checkpoint chunks and walks references in a second pass.

Checkpoint header auth messages use io.ReadAll on each individual length-limited
protobuf, so a huge role/user can have a comparable transient multiplier.
`trailerReader.check` uses io.ReadAll on the **unconsumed remainder**: normally
4 bytes, but arbitrarily large trailing garbage in a malformed checkpoint can
allocate the rest of that file. `RestoredCheckpoint` builds a whole checkpoint
in memory for explicit restore, not normal restart. The other normal-start
whole-file reads are WAL segments and the run-number file, as noted above.

## Fix sketch and limits

```
- data := f.ReadAll(); p.read(data)
+ bufio.NewReaderSize(io.NewSectionReader(f, 0, MaxInt64), 64 KiB)
+ read fixed header; for each record:
+   read record/value metadata
+   update chained CRC over payload in reusable 64 KiB pieces
+   retain only (index, offset, length, file position)
+ verify end count, CRC and exact EOF
```

No file format changes; a large individual value is also streamed. The named
pack must still verify fully before it is accepted. All wal tests, including
every-byte pack corruption and pack replacement/removal, pass. This is a
semi-proven **pack-only** fix, not a general startup memory-bound proof; WAL
segment ReadAll remains explicitly unfixed.

## Actions replay

Copied the storage-only workflow/harness from exp/148-chaos-1 unchanged
(commit `51d73863`). Dispatched once with the bounded reader:

```
gh workflow run chaos-campaign.yml --repo djosh34/tigerstore \
  --ref diag/292-recovery-pack-oom -f seeds=148108 \
  -f scenarios='["clean-rename-power"]' -f pressure_keys=2048 \
  -f memory=134217728
```

Run: https://github.com/djosh34/tigerstore/actions/runs/39293927104

**PASS**, head `51d73863`, job
https://github.com/djosh34/tigerstore/actions/runs/39293927104/job/118201431717.
Three leader-node power-loss/restart cycles hit clean-rename, cleared LazyFS,
caught up, and checked an acknowledged canary through every node. The first
cleaning formed **66,566,089 B** from 69 files; the others formed 66,467,737 B
and 66,369,412 B packs. Recovery completed in about 0.50 / 0.56 / 0.63 seconds.
The fault record reports installed/effect/overlapped/healed all true.

All three cgroups report **zero OOM kills** with 201,326,592 B live values
and memory.max 134,217,728 B. Their memory.peak values are 134,361,088 /
134,221,824 / 134,221,824 B: near the limit (the kernel's peak accounting can
slightly overshoot it), not a demonstration of spare cgroup headroom. Both
witness modes, watches, durability and revisions pass. Porcupine is explicitly
**skipped**, not passed: 8,178 operations exceeds its existing 4,000 limit.
No additional no-fix Actions replay was needed: the two existing issue runs
are red, the deterministic test/profile isolates the cause, and this replay
is green with the same storage experiment plus only the pack-reader change.

The branch includes the small JSON evidence and local test/profile summaries
under `docs/diag-292-evidence/`; complete node logs and overlay are in
[the Actions artifact](https://github.com/djosh34/tigerstore/actions/runs/39293927104/artifacts/12207176984).
