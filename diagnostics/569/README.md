# Deliberate fence after a failed snapshot serve

The prototype is commit `6c7604155ad5b11c0684214820727ba618e6b975`, based on PR #626 head `2d8d84cf9ca880e7436cff79e4471ff217eeec91`.
The same patch on main `c2fa2e586d9438b1cfd947844ba63a2523563d4d` is commit `8ddf74a6f2bfe98eafa8ebbad55940443bae64c5`, on `diag/569-fence-on-purpose-main`.
Neither branch has a PR.

`TestFenceCorruptLeader` uses `simrun.Seeds`, with eight seeds starting at 1 by default. Every individual run must have exactly one `FencedAfterServe`, including a single-seed replay and a race run. It does not accept a skip or a successful search on another seed.

The scenario waits for an elected leader and stops one follower. It writes 40 distinct keys through the normal recorded client path. Checkpoints are disabled so that no checkpoint or cleaner reads the damage first. The tiny Raft window remains enabled. The scenario waits for the old entry to leave that window and for the healthy follower to catch up. It flips a byte of the old entry in a sealed WAL segment with `simfs.Disk.Rot`, then starts the lagging follower. That follower must fetch a snapshot. The stream reads the old WAL record, fails its CRC, and stops the sender. Recovery refuses the disk and the simulator counts the fence. This uses a raw-record snapshot, not a checkpoint snapshot. No startup on the damaged disk happens before the failed serve.

All standard run checks and `checkers` stay enabled. The scenario's writes are in the history and the committed-log checks. Clients continue after the fence, and the remaining quorum must recover and pass liveness. Successful runs report one cold restart too.

## Local runs

Every command used `GOFLAGS=-p=2`. Only one test command ran at a time. Only seed 1 ran locally.

On the PR-based prototype:

```sh
SIM_SEED=1 SIM_SEEDS=1 scripts/sim.sh ./internal/sim '^TestFenceCorruptLeader$' -test.v
SIM_SEED=1 SIM_SEEDS=1 go test -race -count=1 -run '^TestFenceCorruptLeader$' -v ./internal/sim
```

Both passed with one fence after serve and one failed serve. The output is in `pr626-seed1-second.log` and `pr626-race-seed1.log`.

The same commands on the main-based prototype passed with the same counters. Their output is in `main-seed1.log` and `main-race-seed1.log`. The test file is identical on both prototype commits.

The control removed the four scenario configuration overrides from the PR-based test, leaving `cfg := DefaultConfig(seed)`, the normal checkers, and the per-seed fence assertion. Seed 1 failed because `FencedAfterServe` was zero. See `pr626-default-control-seed1.log`. The change was restored and was never committed.

The first draft reached the desired fence but failed durability because consecutive scenario calls shared an end/start timestamp. A one-nanosecond gap fixes the recorded history. See `pr626-seed1-first.log`. The final version also waits for the healthy follower before introducing the damage.

On both final source trees, gofmt, `go vet ./...`, `golangci-lint run --build-tags difftest,planted ./...`, and `go test -short ./...` passed. The first PR-based short-test command was killed by the tool's 120-second timeout. Its incomplete output is `pr626-short.log`. The complete rerun is `pr626-short-complete.log`. Empty vet and gofmt logs mean those commands emitted no output and exited zero.

## Eight-seed campaigns

Both dispatches used:

```sh
gh workflow run sim-campaign.yml --repo djosh34/tigerstore --ref <diag-branch> \
  -f test=TestFenceCorruptLeader -f seeds=8 -f first=1
```

The PR-based campaign is https://github.com/djosh34/tigerstore/actions/runs/43097656245 on `6c760415`.
The main-based campaign is https://github.com/djosh34/tigerstore/actions/runs/43097986823 on `8ddf74a6`.

Both first attempts ended in `startup_failure`, with no jobs and no test logs. Their annotations say "An unexpected error has occurred and we've been automatically notified." The annotation text is saved beside this file. The GitHub status API reported `Actions: major_outage` on 2026-11-11. Its response is saved as `github-status.json`.

Each campaign was retried once. Their reruns are still pending as of this evidence commit. No eight-seed result is claimed. Actions must recover before that part of #569 can be proved. The campaign runs retain the exact code commits and will upload every seed's output if they start.
