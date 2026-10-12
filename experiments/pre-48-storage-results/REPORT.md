# Kubernetes storage adapter prework for #48

Question: can the unchanged Kubernetes etcd3 storage tests and their shared storage contract helpers run through external etcd API endpoints, without embedded-etcd internals?

Yes. Stock etcd 3.7.2 passed the full selected suites for Kubernetes 1.35.0, 1.36.0 and 1.37.0. There were no per-test skips. A fresh-process factory replaces embedded server setup. No upstream assertion, shared contract helper, feature gate, recorder or timeout changed.

This is throwaway prework, not tigerstore validation. Nothing merged. All pushes went to `exp/pre-48-storage`. No issue comment was posted. The parent owns the combined issue comment. The deadline is 2026-10-13T22:00:00Z; both experiment scripts refuse to start after it.

## Sources and versions

I read #46, #1, #48, #5, #18, #20 and `docs/design.md` on main through `gh`. Their snapshots are under the local `sources/` directory. The clone started at main commit `91c4f974a6661ac02c2e7747781ea215353c643b`.

| Kubernetes | apiserver module | Source commit | Pinned etcd client |
| --- | --- | --- | --- |
| 1.35.0 | v0.35.0 | `5c93bee068027068364c6b373496ac7d83508c7c` | v3.6.5 |
| 1.36.0 | v0.36.0 | `4275b1386716339093435e3a84f313d0f311d912` | v3.6.8 |
| 1.37.0 | v0.37.0 | `4d85a6646b05a5a592882f0ac459b2de87431219` | v3.7.0 |

These are the pinned `.0` releases of the requested minors, not claims about every later patch release. Sources came from `https://proxy.golang.org/k8s.io/apiserver/@v/v0.MINOR.0.zip`. Each result directory preserves the module Origin commit and archive hashes.

The server was the stock `etcd-v3.7.2-linux-arm64.tar.gz` release asset, downloaded from `https://github.com/etcd-io/etcd/releases/download/v3.7.2/etcd-v3.7.2-linux-arm64.tar.gz`. Its SHA-256 is `5e3ff99d428fabfcefdc95348f6560d1efe835f7b2d1ab3334fe0831076966b7`. `etcd --version` reported 3.7.2, Git SHA `68c065e`, built with Go 1.26.8. The source commit is `68c065e562994b89e333e77b039ad066f933c586`.

All test builds used Go 1.27.2. Its Linux arm64 archive SHA-256 is `94f3e30b8e374bc285e7dadc11e0865726b9bc6e85b841ccceaabc0214c6b7c8`.

The free runner was `ubuntu-24.04-arm`, with four Neoverse-N2 cores and Linux `6.17.0-1022-azure`. No CPU affinity was applied. Every setup used one external stock etcd process with fsync enabled, plain HTTP loopback client/peer endpoints, default 100 ms heartbeat and 1000 ms election timeout. The upstream embedded helper instead sets `UnsafeNoFsync=true`; the measured external baseline does not. This experiment does not measure TLS storage-test runtime or multi-member behavior.

## Adapter and isolated state

Code is in `experiments/pre-48-storage/` on the experiment branch. `prepare.py` replaces `pkg/storage/etcd3/testserver/test_server.go` and changes the one `*embed.Config` type in `store_test.go` to `*testserver.Config`. The original recorder wrapping is copied verbatim for each version. Each result directory contains the exact `adapter.diff`.

`STORAGE_TEST_FACTORY` points to an executable. Each `RunEtcd` call invokes it with a fresh data directory and the requested progress-notify interval in environment variables. It prints one comma-separated endpoint line, stays alive, then stops on SIGINT. The stock factory allocates client/peer ports and execs the downloaded etcd. The adapter uses an ordinary KV.Range call for readiness, requires an empty store at revision 1, and closes its client before stopping the process. Exit zero or termination by the requested SIGINT is expected; another exit or a shutdown timeout fails the test.

The factory boundary can later launch tigerstore or provision remote isolated endpoints. A static `STORAGE_TEST_ENDPOINTS` mode exists for one isolated setup. `STORAGE_TEST_CA`, `STORAGE_TEST_CERT` and `STORAGE_TEST_KEY` supply client TLS files, but that mode was not exercised here. The factory must honor the requested progress interval. Reusing one static endpoint across the suite is deliberately rejected rather than clearing keys or changing expectations.

Deleting all keys does not reset the global revision, historical versions, compaction boundary or leases. Prefix isolation also cannot reset a cluster-wide compaction boundary. For example, `TestCompact` expects its first compaction cycle to precede the first write; watch semantics contain revision 1 requests; `TestTooLargeResourceVersionErrorForWatchList` uses revision 102 as a future revision; list and watch tests intentionally compact history. A used endpoint can turn those expectations into different tests. The baseline therefore provisions fresh state for every upstream setup call, including subtests that construct their own store.

Only these cases change a server setting:

- 1.35 and 1.36: `TestProgressNotify`, `TestWatchDispatchBookmarkEvents`.
- 1.37: `TestWatch/ProgressNotify`, `TestWatch/WatchDispatchBookmarkEvents`.

They request `WatchProgressNotifyInterval=1s`. The stock factory passes `--watch-progress-notify-interval=1s`. This setting requires lifecycle configuration, not an internal assertion or Membership RPC.

## Every test and shared helper

The full declared inventory is [INVENTORY.md](https://github.com/djosh34/tigerstore/blob/exp/pre-48-storage/experiments/pre-48-storage-results/INVENTORY.md). It lists every top-level test and benchmark, every shared `Run` helper, all recursive subpackages, dependencies, and adjacent feature/factory tests that were inspected but are outside this baseline command.

Every executed dynamic name, including parent test groups, appears in each version's `test-results.tsv`:

- [v0.35.0 results](https://github.com/djosh34/tigerstore/blob/exp/pre-48-storage/experiments/pre-48-storage-results/v0.35.0/test-results.tsv)
- [v0.36.0 results](https://github.com/djosh34/tigerstore/blob/exp/pre-48-storage/experiments/pre-48-storage-results/v0.36.0/test-results.tsv)
- [v0.37.0 results](https://github.com/djosh34/tigerstore/blob/exp/pre-48-storage/experiments/pre-48-storage-results/v0.37.0/test-results.tsv)

| Top-level Go tests by package | 1.35 | 1.36 | 1.37 |
| --- | ---: | ---: | ---: |
| `pkg/storage/etcd3` | 74 | 74 | 70 |
| `pkg/storage/etcd3/metrics` | 5 | 7 | 7 |
| `pkg/storage/etcd3/preflight` | 8 | 8 | 8 |
| `pkg/storage/testing` standalone utility tests | 0 | 0 | 2 |
| Total | 87 | 89 | 87 |

`pkg/storage/etcd3/testing`, `testing/testingcert` and `testserver` have no test files. `pkg/storage/testing` also has no standalone Go tests in 1.35/1.36. Its store/watch contract helpers execute through the etcd3 wrappers in all three versions. There is no separate standalone shared contract suite to run. Go emits package-level `skip` JSON events for `[no test files]`; these are not per-test skips. The summaries distinguish them.

The sole unreferenced shared contract helper is `RunTestOptionalWatchBookmarksWithCorrectResourceVersion`. Its source says the optional periodic bookmark behavior is implemented in the watch cache layer. Its callers are in `pkg/storage/cacher/cacher_test.go`, not etcd3. It asks for `AllowWatchBookmarks` without `ProgressNotify`. Etcd3's `RunTestWatchDispatchBookmarkEvents(..., false)` explicitly checks that this alone does not enable periodic bookmarks. This helper is outside the upstream etcd3 selection, not an adapter skip or an embedded-etcd dependency. The inventory includes it and its actual callers. Benchmarks are inventoried but were not run.

### Internals and member APIs

No test in the selected tree intrinsically requires an embedded etcd object, Raft/WAL inspection, or a Membership RPC. No MemberList, MemberAdd, MemberRemove or MemberUpdate call occurs in these tests or the adapter.

The original helper uses `embed.NewConfig`, `embed.StartEtcd`, readiness/stop/error/close methods and `Server.Cluster().ClientURLs`. Those are setup details, all replaced by the factory and ordinary endpoint readiness. `go list -deps -test` for every selected package contains zero `go.etcd.io/etcd/server/v3` imports after adaptation. The original module graph still lists its upstream etcd server dependency; that does not mean the test executable uses it.

Maintenance.Status is different from Membership. The apiserver store constructor starts a version check through `pkg/storage/feature/feature_support_checker.go`. The inventory marks those setups `API, Status`. It needs the externally visible version behavior, not an etcd member implementation. Compaction tests use KV.Compact. Metric tests fake their monitor, preflight tests use invalid/empty addresses, and `TestEtcdHealthCheck` parses supplied JSON rather than calling a real health endpoint. There are no member-API or internal-only skips to propose for these versions.

## Measured runtimes and Actions evidence

The passing suite run is [38316931864](https://github.com/djosh34/tigerstore/actions/runs/38316931864), on experiment commit `204f36a702fbdf2f7ff34bb5e85bc44cd4df407c`.

| Version | Cold build wall seconds | Full test command wall seconds | etcd3 package seconds | Fresh servers | Pass result events | Leaf passes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1.35.0 | 54.96 | 98.69 | 97.302 | 75 | 357 | 327 |
| 1.36.0 | 59.06 | 98.81 | 97.314 | 75 | 361 | 329 |
| 1.37.0 | 64.22 | 131.31 | 129.709 | 101 | 537 | 490 |

Result events include top-level tests, nested groups and leaf subtests, so they are not a count of distinct leaf scenarios. All events with a `Test` field passed. Failures and per-test skips were zero.

Jobs took 164, 169 and 207 seconds respectively, including tool downloads, evidence upload and cleanup. Job links and exact timestamps are in `storage-jobs.json` on the branch. Raw JSON output, diffs, dependency lists, versions, checksums and times are in the workflow artifacts and under local `evidence/run-38316931864/`. Measured summaries and inventories are committed under `experiments/pre-48-storage-results/v0.MINOR.0/`.

The failed attempts stay visible:

- [38316089694](https://github.com/djosh34/tigerstore/actions/runs/38316089694), `d073157`: the adapter accidentally called Kubernetes.Get instead of KV.Get for readiness, so the build failed. Cleanup also met read-only module-cache directories. Fixed the call, enabled writable module cache, and made failure cleanup chmod the owned cache.
- [38316342186](https://github.com/djosh34/tigerstore/actions/runs/38316342186), `e071fba`: the adapter incorrectly failed expected stock-etcd SIGINT shutdowns. These occurred for all 75, 75 and 101 factory instances. The only subsequent suite change accepted that requested signal while retaining failures for other exits. The green run is the evidence that the corrected adapter passed, not a claim that either failed run passed.

## RangeStream contract and probe

The architect's [decision on #48](https://github.com/djosh34/tigerstore/issues/48#issuecomment-6133737025) now requires RangeStream implementation in #48. No stream test was skipped here.

The focused probe passed in [38317357827](https://github.com/djosh34/tigerstore/actions/runs/38317357827), on `c934084dc8ce53033ba0c46da7773dde734567ab`. It used stock server 3.7.2 and client/API modules 3.7.2. Cold build took 15.66 seconds; the complete probe took 1.25 seconds. All 25 requested cases produced their expected statuses, and all 18 successful streams merged with `proto.Merge` to the same response as unary Range.

[wire.jsonl](https://github.com/djosh34/tigerstore/blob/exp/pre-48-storage/experiments/pre-48-storage-results/rangestream/wire.jsonl) records requests, every chunk, KV key/revision/value-length fields, terminal status, and response-message hashes. The workflow artifact and local `evidence/run-38317357827/rangestream-1/` also preserve each received protobuf response reserialized with `proto.Marshal` into `.pb` files. These are message captures, not an HTTP/2 packet trace.

### Stock source rules

Pinned sources at `68c065e562994b89e333e77b039ad066f933c586`:

- [server/etcdserver/v3_server.go, lines 157-299](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/v3_server.go#L157-L299).
- [server/etcdserver/api/v3rpc/key.go, lines 62-87 and 154-180](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/api/v3rpc/key.go#L62-L180).
- [api/etcdserverpb/rpc.proto, lines 1442-1457](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/api/etcdserverpb/rpc.proto#L1442-L1457).
- [server/embed/config.go, default request size](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/embed/config.go#L67).
- [server/etcdserver/txn/range.go, default ordering and revision filters](https://github.com/etcd-io/etcd/blob/68c065e562994b89e333e77b039ad066f933c586/server/etcdserver/txn/range.go#L97-L108).

The initial chunk limit is 10 keys, not a fixed 10-key size for every chunk. Subsequent limits double if the preceding serialized RangeResponse is smaller than half the target, or halve if larger than twice the target, with a minimum of one. The target is MaxRequestBytes, stock default 1,572,864 bytes. Remaining request limits cap the next chunk.

Nonfinal chunks carry only KVs. Header, More and Count occur on the final chunk. The final Count is the full matching count, including keys omitted by a request limit. EOF means the stream finished, not that More is false. CountOnly returns a single metadata-only chunk. Empty ranges also return a final header-bearing chunk.

A non-serializable request first performs a linearizable read barrier. An unpinned request is pinned to the first range response's store revision for subsequent chunks. An explicitly historical request keeps its requested KV snapshot, but its final header reports the current store revision at the first range read, not the requested historical revision. Kubernetes already handles that distinction in [store.go, lines 973-988](https://github.com/kubernetes/apiserver/blob/4d85a6646b05a5a592882f0ac459b2de87431219/pkg/storage/etcd3/store.go#L973-L988).

The source performs a separate historical range per chunk. Midstream compaction can therefore terminate a stream after earlier chunks; that behavior is source-inspected, not forced by this probe. Concurrent mutation during a slow active stream was also not independently forced here. The unchanged upstream tests and source pinning rules remain the evidence for those paths.

### Recorded responses

The probe created 25 small keys at revisions 2 through 26, then one additional key at revision 27. Header below means the final header revision. Every nonfinal chunk had no header and default Count=0, More=false.

| Request | Chunk KV counts | Final Count | Final More | Header | Status |
| --- | --- | ---: | --- | ---: | --- |
| Latest 25 keys before extra write | 10, 15 | 25 | false | 26 | OK |
| Limit 1 / 5 / 10 after extra write | 1 / 5 / 10 | 26 | true | 27 | OK |
| Limit 15 | 10, 5 | 26 | true | 27 | OK |
| Limit 25 | 10, 15 | 26 | true | 27 | OK |
| Limit 26 or 30 | 10, 16 | 26 | false | 27 | OK |
| Revision 26 after write 27 | 10, 15 | 25 | false | 27 | OK |
| KeysOnly | 10, 16, values empty | 26 | false | 27 | OK |
| CountOnly with Limit 5 | 0 | 26 | false | 27 | OK |
| Serializable, KEY ascending, or revision -1 | 10, 16 | 26 | false | 27 | OK |
| Missing prefix | 0 | 0 | false | 27 | OK |
| Compact at 26, read 26 | 10, 15 | 25 | false | 27 | OK |
| Compact at 26, read 27 | 10, 16 | 26 | false | 27 | OK |
| 25 values of 400 KiB | 10, 5, 5, 5 | 25 | false | 52 | OK |

Errors occurred before any response chunk:

| Request | gRPC status | Exact description |
| --- | --- | --- |
| KEY descending or VALUE ascending | Unimplemented | `RangeStream does not support custom sort orders` |
| MinModRevision=2 | Unimplemented | `RangeStream does not support revision filters` |
| SortOrder=99 | InvalidArgument | `etcdserver: invalid sort option` |
| Empty key | InvalidArgument | `etcdserver: key is not provided` |
| Future revision 1000 | OutOfRange | `etcdserver: mvcc: required revision is a future revision` |
| Compact at 26, read 25 | OutOfRange | `etcdserver: mvcc: required revision has been compacted` |

Source rules reject any nonzero Min/MaxCreateRevision or Min/MaxModRevision. NONE ordering is allowed; KEY ascending is also allowed. Valid nondefault sorting is Unimplemented, while invalid enum values are InvalidArgument.

The two earlier probe runs, [38316838325](https://github.com/djosh34/tigerstore/actions/runs/38316838325) and [38317019967](https://github.com/djosh34/tigerstore/actions/runs/38317019967), captured successful streams but failed the final large unary comparison at gRPC's 4 MiB receive limit. Setting the etcd client Config limit did not change the raw protobuf client's call options. The passing probe sets a 32 MiB receive limit directly on both raw RPC calls. This is a client limit, not a RangeStream error or a server change.

### Exact Kubernetes stream tests

Only the inspected 1.37 suite has these stream-specific cases:

- `TestList/rangeStream=true` runs shared list semantics with streaming enabled and checks that a stream was attempted. Its recorder increments before the RPC result, so this check alone does not prove stream success.
- `TestGetListStreamMultiChunk` uses 25 keys, a later write, an exact historical revision and a minimum revision. It checks streaming use on successive calls, object counts, no continuation metadata, and the requested exact resource version.
- `TestWatchChanSync/RangeStream` has three cases: an empty result, one page of matching objects, and a mutation before returning another page. It calls the streaming synchronization implementation directly and checks its stream-call count.
- `TestWatchChanSyncStreamMatchesPaginated` compares the complete streamed and paginated event sets over 20 keys.
- `TestWatchChanSyncStreamCompactionError` pins a read to the earlier of two writes, compacts at the later write, and requires ResourceExpired plus an error metric.
- `TestWatchChanSyncStreamMetrics/success_records_a_listStream_request` requires a successful direct stream and the listStream request metric.
- `TestGetListStreamFallsBackToPaginated`, `TestWatchChanSyncStreamFallsBackToPaginated`, and `TestWatchChanSyncStreamMetrics/unimplemented_is_not_recorded` inject a client-side Unimplemented response and check fallback/feature-cache/metric behavior. These already provide negative controls without changing the external stock endpoint.
- `TestListMetrics/rangeStream=true`, `TestConsistentList/rangeStream=true`, and `TestWatch/RangeStream=true/...` also exercise the enabled path. Every nested name is in the 1.37 TSV.

The source comment saying the server chunks at 10 keys describes the initial limit only. The measured 10+15 and 10+5+5+5 responses show why the worker must not assume a fixed chunk size. These cases require public RangeStream semantics, not embedded objects or Membership APIs.

## Exact commands

All local operations used `/home/joshazimullah.linux/work_mounts/scratch/pre-48-k8s/storage`. No local test build was needed.

```sh
mkdir -p tmp cache sources evidence
export TMPDIR="$PWD/tmp"
for n in 46 1 48 5 18 20; do
  gh issue view "$n" --repo djosh34/tigerstore \
    --json number,title,body,comments,url,state > "sources/issue-$n.json"
done
gh api 'repos/djosh34/tigerstore/contents/docs/design.md?ref=main' \
  --jq .content | base64 -d > sources/design.md
gh repo clone djosh34/tigerstore repo
cd repo
git switch -c exp/pre-48-storage
```

The Actions jobs ran:

```sh
experiments/pre-48-storage/run.sh v0.35.0
experiments/pre-48-storage/run.sh v0.36.0
experiments/pre-48-storage/run.sh v0.37.0
experiments/pre-48-rangestream/run.sh
```

The storage script downloads the pinned module, applies the adapter, sets TMPDIR/GOCACHE/GOPATH/GOMODCACHE under its own `.work/v0.MINOR.0` directory, and executes these commands inside the copied apiserver module:

```sh
/usr/bin/time -p -o "$root/evidence/build.time" \
  go test -run '^$' -count=1 ./pkg/storage/etcd3/... ./pkg/storage/testing
go list -deps -test ./pkg/storage/etcd3/... ./pkg/storage/testing
go test -list . ./pkg/storage/etcd3/... ./pkg/storage/testing
/usr/bin/time -p -o "$root/evidence/test.time" \
  go test -json -count=1 -timeout=10m \
  ./pkg/storage/etcd3/... ./pkg/storage/testing
```

The probe builds its separate Go module with `go build -mod=mod`, then runs the resulting executable under `/usr/bin/time -p`. Its final module graph and Go checksum file are committed with the response summaries.

Both workflows trigger only on the owned experiment branch and their respective code paths. The storage jobs have a 15-minute timeout; the probe has a 10-minute timeout. All actual jobs finished in under four minutes. Artifacts were downloaded with `gh run download RUN_ID --repo djosh34/tigerstore --dir evidence/run-RUN_ID`.

## Files and cleanup

Local report: `/home/joshazimullah.linux/work_mounts/scratch/pre-48-k8s/storage/REPORT.md`.

Committed report and inventories: `experiments/pre-48-storage-results/` on `exp/pre-48-storage`. Complete local evidence remains in `evidence/`; pinned source snapshots remain in `sources/`. Workflow artifacts have seven-day retention, so the committed measured summaries and full test-name inventories are the durable links.

Runner scripts and always-run cleanup steps delete tool binaries, test build caches, module caches, temporary data and copied patched modules. Local temporary directories, smoke copies, caches and the extracted stock binary were removed. Source archives and captured protobuf messages are evidence, not build outputs. No subagent was created, no main/work branch was pushed, and no merge or issue comment was made.
