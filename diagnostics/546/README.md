# Issue 546 diagnosis

This branch is throwaway. It starts at origin/main, 71ba6d4f. No production code is changed. No PR is intended.

All runs used GOFLAGS=-p=2. Builds and tests ran one at a time.

## Main result

The test assumes cleaning wins a race against quota enforcement. It need not. Checkpoints and transient packs count too. The cluster-version handshake hides quota enforcement in many fast runs.

The unmodified TestSharedValues passed 10 plain runs and 10 node-only coverage runs. See baseline.log and baseline-coverage.log.

Adding only alarmsAllowed(t, r.n) after runClean made the original test fail 10 of 20 runs, at rounds 39 to 41. No artificial sync delay was needed. See version-ready-original.patch and version-ready-original.log. The patch is NOT applied in this branch.

Apply and replay that experiment:

```sh
git apply diagnostics/546/version-ready-original.patch
GOFLAGS=-p=2 go test -short -count=20 -run '^TestSharedValues$' ./internal/node
git apply -R diagnostics/546/version-ready-original.patch
```

## Scratch tests

```sh
GOFLAGS=-p=2 go test -short -count=1 -v -run '^TestDiag546SharedValues$' ./internal/node
GOFLAGS=-p=2 DIAG546_STRICT=1 go test -short -count=1 -v -run '^TestDiag546SharedValues$/held-pack$' ./internal/node
GOFLAGS=-p=2 go test -short -count=3 -v -run '^TestDiag546FocusedSharedValues$' ./internal/node
GOFLAGS=-p=2 go test -short -count=1 -v -run '^TestDiag546CutWaitingWithoutWrites$' ./internal/core
```

The strict run is expected to fail. It restores the original test's no-NOSPACE assertion. held-pack makes it impossible for a pack to finish before a refusal. The ordinary run stops generating keys on NOSPACE and waits for cleaning without more writes. It checks the original shared-value size bound and version 101. Early-refusal cases also check that a small probe write succeeds afterward. Late checkpoint-held cases legitimately remain over budget after cleaning; that is logged, not treated as a recovery failure.

node.log has every-round file counts, byte counts, durable roots, and eligible files. The vector order is segments, checkpoints, packs, spares, other. Snapshots are not atomic with file changes. Rounds 1000 and 1001 are samples after writing stops, not new write rounds. The main node.log was captured before adding the focused test's padding write, which only shifted log line numbers in the other scratch test.

startup.log uses the original startup order, with no version wait. All three samples first cleaned at cluster version 1. startup-slow.log adds a 368 ms first WAL sync and five 75 ms syncs. Those three runs also stayed at version 1 through the first cleaning. These delays alone did not reproduce CI. Other modes wait for version 3 so the quota is actually enforced.

The focused test creates a durable checkpoint with 101 references to one value. A physical compaction at revision 1 drops none of its versions. A 40 KiB padding put seals the second segment; WAL.keep protects the newest two segments. Defragment then uses the node's real cleaning path. It copies 32,826 bytes and preserves version 101. This is a deterministic alternative to the original test's new-key flood.

## Negative controls

Neither negative control is left applied.

- core-negative.log: disable the single n.cutWaiting() call in internal/core/node.go. The waiting checkpoint stays at index 18 instead of advancing to applied index 102. With the call restored, core.log shows progress without another request.
- focused-negative.log: disable the consecutive-reference skip in cleaner.walk, internal/node/clean.go. The focused test copies 3,311,726 bytes instead of 32,826 and fails its size assertion.

focused.log contains three passing focused runs. strict.log contains an expected failing strict run. final-node-check.log and final-core-check.log contain passing checks after restoring both production changes. The original-startup modes were added later and checked separately.
