# PR 97 review probes

These are throwaway review tests, not proposed production changes. Base is PR 97 `7b48320b75ad58b3a9f77491cafb38adffb7d7fc`, compared only with PR 96 `1704af0b528dbcb4f85cbc1c793fbca364c83b2f`.

The local wire probes use the downloaded stock etcd v3.7.2 ARM64 release, SHA256 `5e3ff99d428fabfcefdc95348f6560d1efe835f7b2d1ab3334fe0831076966b7`. The etcd fixture is one loopback HTTP member; tigerstore uses the repository's three-member TLS fixture at 100 ms ticks. They test API behavior, not comparative performance.

```
REVIEW_ETCD=/absolute/path/to/etcd-v3.7.2-linux-arm64/etcd \
  GOFLAGS=-p=2 GOMEMLIMIT=2GiB \
  go test ./internal/server -parallel 2 -run '^TestReview97' -count=1 -v
```

- `TestReview97Boundary` writes 30 keys, tunes the first value so etcd's internal first response is 786431 bytes, one byte below half its target. Etcd emits 10+20 keys. Tigerstore emits 10+10+10 because its chunk-size input includes extra header metadata. Repeated three times with the same result.
- `TestReview97Compaction` writes 80 values of 400 KiB, receives the first chunk, then writes revision 82 and compacts through it while the stream is reading revision 81. Etcd ends with OutOfRange and its compacted-revision message. Tigerstore ends with Internal and unexpected EOF. Its stream error path drops the unsent remainder of a partially sent gRPC message. Repeated three times with the same result.
- `TestReview97SnapshotCancel` writes 80 values of 160 KiB, requests 60, then updates, deletes and inserts keys after receiving the first chunk. Both servers return 60 old values, terminal count 80, More true and the original header revision 81. Cancellation returns Canceled and a subsequent write succeeds.

`wire.log` and `snapshot.log` retain the local output. The first two tests intentionally fail on the reviewed tigerstore head.

## TestCompact comparison

[Run 38438356753](https://github.com/djosh34/tigerstore/actions/runs/38438356753) uses source `63c930f7`, the same unchanged Kubernetes v0.37.0 TestCompact, and three members with client and peer TLS for both tigerstore and stock etcd v3.7.2. Both jobs use ubuntu-24.04-arm, no CPU pinning and normal durable writes. They are separate VMs of the same runner type, not a paired latency benchmark on one VM.

Each job first runs the test while the other selected test packages compile, matching the original job structure, then repeats the same test 30 times against the built packages. Artifacts retain actual Go test exit codes, JSON output and runner CPU/filesystem details. Workflow success only means evidence was collected; the recorded test exit codes decide whether tests passed. No upstream assertion, timeout or clock was changed.

Tigerstore passed the cold run and all 30 warm repetitions. The etcd attempt is **not a pass**. Its compaction cycles completed without `No waiters`, but the experiment reused tigerstore's shutdown handling and each test failed in cleanup. It treated etcd's requested SIGINT exit as an error and killed a member after four seconds. These are adapter failures, not evidence of an etcd commit-latency failure. This experiment does not establish the latency comparison.

The worker independently added a correct etcd shutdown adapter. I downloaded and checked every log from [run 38437822216](https://github.com/djosh34/tigerstore/actions/runs/38437822216), source `b1ca4e7071244957fd4bb19e58ed88c7008994ce`. It ran unchanged TestCompact 20 times per store, with the packages compiled first, in one ubuntu-24.04-arm job, three members and client/peer TLS for both. All 20 tigerstore and all 20 etcd tests passed; none contains `No waiters` or a failing test. This is a matched functional comparison, not a per-commit latency distribution. The original pre-work run 38316931864 also passed TestCompact against stock etcd on this runner type, with one HTTP member.

## Commit-latency source audit

The failed PR 96 test's log shows a real 135 ms persist batch for one 81-byte entry, with earlier 120–178 ms hard-state and election appends. It does not isolate write, fsync, scheduler delay or compilation contention. The later passes do not establish why that failure happened.

Tigerstore's persist loop drains already queued jobs and calls WAL.Sync once. At low concurrency there may be only one write to amortize each mandatory sync, and it also syncs hard-state-only writes. Stock etcd's WAL.Save uses raft.MustSync and fdatasync on its preallocated WAL, so commit-only hard-state updates need not each force a sync. Both still require durable entry and term/vote writes. These are implementation differences worth measuring if the latency gap recurs, not a proven attribution of the original failure.

The first compactor transaction has 81 bytes and runs in a fresh database, far below tigerstore's 64 MiB rotation threshold. Rotation is not a credible cause for that append. It also does not exercise the near-1.5-MiB write path in #88; sharing a symptom is not evidence of a common cause. The original log and the successful comparisons do not establish a persistent tigerstore-versus-etcd commit-latency difference.
