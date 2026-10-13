# Append-clocked group commit pre-work

Question: does holding client submissions and Ready behind one local append plus fsync make durable groups grow with load and improve Put throughput?

This is throwaway code for #47. Do not merge it. An Opus worker must write the production change. The branch starts at main `b8bac8b371c94f6d17136ad14aaa04354bbff01e`.

[Results and limits](RESULTS.md) report both complete runner attempts. The scheduling rule works for controlled queued arrivals. The final real-client run does not establish reliably large leader groups or a general performance win.

## Code

- `81053bcc54ac315b6ac9302dbe09b5eb591c18db` is the baseline. It copies the pre50 profiling client and observability onto current main. There is no driver, core, Raft, peer or WAL behavior change.
- `07a986e8789131c19ae71c88f3fbe88760c86f2d` adds the driver prototype and focused tests. Only `internal/node/node.go` changes production behavior. Its patch is 49 additions and 29 deletions. The helper's equivalent source commit was `a33f15a946caf20ae8751c44d6ec4682da3d8d81`.
- First measured source `ae9abb82394e47799003fc62afae2b68fde558a5` adds measurement fences but makes no later server change.
- Final prototype `fad17fe3fe64c73075da484c773f4859bc4e8e18` also drains the inbox after append completion before taking the next Ready. The owner snapshots the inbox length and drains that many queued inputs. Continuous arrivals cannot prevent the next disk submission. The complete behavioral patch against the baseline is 53 additions and 35 deletions in `internal/node/node.go`.
- Stock etcd v3.7.2 is the release binary, upstream `68c065e562994b89e333e77b039ad066f933c586`. The script checks ARM64 SHA-256 `17122293e6713e3d8b14b33964af8f6eae0a83af03e87487f34731ee0e28112d`.

The copied client is unchanged from `exp/pre-50-profile`. `experiments/pre47groupcommit` replaces only the orchestration and summary. Every complete executable argument vector is in `c*/commands.json`. Each cluster directory includes placement, affinities, counters, before/after terms and revisions, logs and result JSON.

## Mechanism

The driver marks a group in flight as soon as `Process` returns persist jobs, before the disk goroutine accepts it. It holds client requests outside `core.Submit` and does not call `Process` again until that group's fsync succeeds. Holding Submit matters because `queueWrite` can propose immediately when a request crosses the entry byte cap.

Incoming peer messages still reach `Step`, and ticks still run, until the count-bound shared inbox backpressures both peers and clients. After successful fsync, the owner calls `Persisted` on the original jobs in their original order, submits the held requests and calls `Process`. Followers use the same gate, so several incoming appends can become one physical group.

The first candidate submitted only requests already moved to its deferred list when disk completion won the select. It could leave existing inbox requests for a later append. The final candidate also drains inputs already in that inbox on completion. The first run is retained in `first-run/raw.tar.gz`, `first-run/runs.csv` and `first-run/summary.csv`.

The disk worker appends every job's entries in order, including replacement suffixes. It appends only the last non-nil hard-state tuple after those entries, then calls `WAL.Sync` once. No timer is added. A normal nonempty group gives one file Write and one file Sync. Segment rotation still adds header and directory persistence operations. Empty response-only jobs still get their completion even when Sync has no bytes to flush.

The leader waits for local durability, not for the previous batch's quorum commit, before proposing the next batch. The pipeline can therefore have several batches awaiting remote commit.

## Method

Three free `ubuntu-24.04-arm` jobs, one per concurrency of 16, 64 and 256. Each job measures baseline, prototype and etcd on the same allocated VM, sequentially. Five fresh clusters per store. Odd repetitions run baseline, prototype, etcd; even repetitions reverse that order. Different concurrency cells use different VMs. The runs isolate the store comparison within a cell, not VM effects between cells.

The layout matches pre50. Leader CPU 0, followers CPUs 1 and 2, generator CPU 3, each with `GOMAXPROCS=1`. All server threads are repinned after leader discovery and checked after load. Client and peer traffic use certificate-authenticated TLS 1.3. No proxy, added delay, profile capture, watchers, compaction or compression. Etcd keeps its normal safe persistence settings and gets an 8 GiB backend quota. Tigerstore has no quota. All three data directories share the runner's root ext4 disk.

The script remounts the disposable runner root with `barrier=1`, rejects `nobarrier` or `barrier=0`, and records the mount. Guest barriers and successful fsync do not prove host power-loss protection. Three processes on one virtual disk do not model independent failure domains.

The client uses 16 established gRPC connections and closed-loop workers against the leader. It overwrites 4,096 keys shaped as `/registry/pods/ns-%03d/pod-%08d` with 1,024-byte SHA-256-derived values. Values differ between keys but repeat on overwrites. Each cluster gets 4,096 seed Puts, 20,000 warmup Puts, a one-second pause and 60,000 measured Puts. Seed concurrency is 64; warmup and measurement use the cell's concurrency.

CPU is process user plus system ticks sampled from `/proc/PID/stat`, with PID start-time checks. The generator samples immediately around its worker interval, including drain. CPU excludes I/O wait and work charged elsewhere. It excludes the later follower catch-up fences. One tick over 60,000 Puts is 0.1667 CPU microseconds per operation on these runners. The aggregate server value sums the three replicas, not the generator. Independent median columns need not sum.

The client requires no errors, 60,000 successes, exactly 60,000 new revisions and one response term. Extra serializable probes before and after the measured client require every replica to reach the expected revision and require the same term as all measured responses. Startup and final leader IDs must agree. No samples are dropped after seeing their performance.

Disk counters bracket the client subprocess, including its connection probes and setup. The before and after snapshots follow all-replica applied-revision fences. Tigerstore's async Raft never applies unstable local entries. Etcd uses a different apply loop, so the script also captures each replica's committed Raft index and waits for `etcd_server_proposals_committed_total` to reach it. Etcd updates that gauge only after successful `storage.Save`. A fresh metrics scrape after this fence supplies the histograms, since Prometheus collectors do not form an atomic snapshot.

Puts per sync means 60,000 divided by the counter delta. Tigerstore counts every file Sync through osfs, including hard-state-only and segment-header calls, but not directory syncs. Etcd's stock WAL histogram counts WAL syncs, usually fdatasync, including its instrumented WAL installation sync sites. Its backend commit histogram is archived separately and is not included in the WAL denominator. These are gross sync densities, not a histogram of logical Raft entry sizes, and not all-etcd-fsync counts.

## Safety and limits

The library contract and source citations are in `audit/contracts.md`. The helper's code review of the first gate is in `audit/driver-review.md`, and its measurement review is in `audit/benchmark-review.md`. The parent made the final inbox-drain correction after that review. `audit/inbox-drain-tests.txt` records fmt, vet, lint, all short tests and 50 race repetitions for that correction.

Required invariants:

- Never acknowledge a leader write, apply its unstable entries or release their watch events before successful local fsync. Remote-majority persistence alone does not bypass async Raft's stable-local apply frontier.
- Do not call Ready and discard it while blocked. Ready accepts work, clears messages and marks entries in progress.
- Preserve every original append job and response in order, including hard-state-only and response-only jobs. Call Persisted only after the whole group succeeds. Install entries in MemoryStorage before delivering the original stability response. Do not invent a combined response or call Advance with async storage.
- Keep entry records in order. A repeated index replaces the suffix. Coalesce the whole last non-nil hard state, including vote zero, never fieldwise maxima. Its commit index cannot precede the entries it names on disk.
- Preserve entry/message buffers until all asynchronous consumers finish. A term change must not let an old completion stabilize a replacement suffix.
- A write, fsync, rotation or directory-sync error fences the node and releases no successful completion. Do not retry a failed fsync.

The prototype is not production-ready. Blanket Process gating delays heartbeats, votes, ReadIndex, apply, timeout replies, watch output and the externally observed leader. Long disk stalls can cause elections. Deferred request deadlines begin when Submit finally runs. The 8,192-request deferred cap is a count limit, about 12 GiB at the largest valid request, before another inbox and upstream RPC buffers. Multiple capped Raft entries can share one physical group, so there is no physical group-byte cap. Full inbox backpressure also stops incoming peer handling. These are explicit gaps, not measured gains or satisfied liveness rules.

The current simulator drives core directly and does not execute the changed driver. Passing its seeds is baseline regression evidence, not proof of the new gate. Focused driver tests cover a held append, byte-cap Submit bypass, completion/error ordering, follower grouping and acknowledgements, final-hard-state coalescing and suffix replacement. Pinned-raft contract tests cover unstable apply suppression, Ready acceptance, term-change entry-free completions and entry lifetime. The logs are archived here.

The real change should share the scheduling rule with the simulator and add these fault schedules, with assertions that each fault actually ran:

- Hold leader fsync while both followers acknowledge. Assert no leader apply, reply or watch event before local completion. Repeat on a follower.
- Cross the proposal byte cap during a held append. Assert no premature proposal, no second disk group and no lost or duplicated request.
- Change terms and replace a suffix while old append work is pending. Deliver old completions and a later entry-free completion. Check stable index/term and exact apply once.
- Coalesce term/vote/commit tuples, including a reset to vote zero. Crash at record, file-sync and directory-sync boundaries and cold-recover the exact safe tuple and suffix.
- Fail Write/Sync with EIO or ENOSPC. Assert node fencing, no positive completion and healthy-majority progress. Inject short writes in the file adapter too.
- Stall a gray leader past an election timeout, fill its queue, pause/restart it and deliver stale-generation completions. Check bounded memory, control-path progress, no ID reuse and a stated healthy-majority service bound.
- After acknowledged writes, erase each disk in turn and cold-crash/restart the other two before any live-memory repair. Require every acknowledged value and revision.

The audit lists 16 detailed schedules and marks what already exists. Full gray-node, erase-one-disk, poisoning and changed-driver simulator coverage remain unproven.

## Reproduction

On a fresh disposable ARM Actions runner, before the experiment deadline:

```sh
git checkout <measured commit from environment.txt>
CLIENTS=16 timeout --kill-after=5s 820s bash experiments/pre47groupcommit/run.sh
# Repeat in separate jobs with CLIENTS=64 and CLIENTS=256.
python3 experiments/pre47groupcommit/summarize.py
```

The script executes:

```sh
sudo mount -o remount,barrier=1 /
GOMAXPROCS=1 taskset -c 3 "$RUNNER_TEMP/pre47groupcommit/bin/load" \
  -endpoint 127.0.0.1:<leader-port> -certdir <certdir> \
  -pids <leader>,<follower1>,<follower2> -hz 100 \
  -op put -total 60000 -workers <16-or-64-or-256> -out <result.json>
```

`versions.txt` records every binary hash, module version and build identity. The baseline is built from `git archive 81053bcc54ac315b6ac9302dbe09b5eb591c18db`. The candidate's Go build may record `vcs.modified=true` because untracked evidence exists before build; `git diff --exit-code` and `build-status.txt` distinguish that from edited source.

Local validation before the code push used:

```sh
test -z "$(gofmt -l .)"
go vet ./...
golangci-lint run ./...
go test -short ./...
go test -race -count=10 ./internal/node
```

The helper also ran 50 race repetitions, and the independent audit ran five. Only the parent's logged commands and the audit's logged commands are archived; the helper's 50-repeat claim is not an additional preserved log.

Copying the candidate node tests onto the observability-only baseline made three test families fail. The baseline emits another group before the held one completes, followers do not hold their next Ready, and the disk writes two hard-state records rather than one. `audit/baseline-regression.txt` preserves that expected failure. It proves those tests distinguish the scheduling change. The combined test error text does not prove that the old driver acknowledged an unsynced write.

Two early runner attempts were cancelled to repair the counter fences, not counted as measurements. Run `38329668773` at `07a986e` lacked the etcd post-Save fence. Run `38329779873` at `b2458f2` used the first successful gauge scrape for its histogram, which can race Prometheus collection. The final source starts a new scrape after the gauge fence. No server code changed between these attempts.
