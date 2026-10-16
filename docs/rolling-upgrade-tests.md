# Rolling upgrade tests

`rolling-upgrade.yml` runs the real-process tests in `cmd/chaos/rolling_test.go`. It builds two copies of the checked-out server with Go overlays. The old copy supports entry format 1 and peer range 1-1. The new copy supports entry formats 1-2 and peer range 1-2. Only those constants differ. These are synthetic version ranges, not previous release binaries.

The tests reuse the chaos cluster, TLS links, workload, history recorder and six checker verdicts. Each upgrade stage must contain successful writes. The evidence includes the histories, WALs, server logs, captured announcements and old-binary refusal. Checker timeouts fail the run.

`TestRollingUpgrade` runs seeds 148 and 174. It upgrades nodes 1 and 2, rolls node 2 back, restarts it, and upgrades node 3. It then sends node 2's earlier announcement through a real peer connection. The announcement keeps its original origin, boot and run fields. The test requires those bytes to commit after the rollback announcement, requires version 1 to remain active, and checks that the committed entries still use format 1. It upgrades node 2 last, checks for format-2 entries, tries the old binary against that data directory, and requires a clear refusal. The new binary must then recover under load.

`TestRollingRollbackBeforeAnnouncement` covers the other order named in #174. It stops the node selected for rollback and upgrades the last old node before the rollback can announce. This is a regression test, not an expected-success downgrade. If the remaining nodes activate version 2, it fails.

The workflow also removes the stale-run guard in a separate build. That job passes only if `TestRollingUpgrade/148` detects activation after the delayed announcement. This checks that the delayed-announcement test reaches the gate it claims to test.

The normal scenario also runs with the server and test executable built with `-race`. Each stage checks the binary ranges in `/metrics` and probes fresh TLS peer connections in both directions. ALPN and the preface reply must choose the highest shared version. The existing core and peer version tests run alongside it, including overlapping, touching and disjoint peer ranges and refusal from a checkpoint.

To reproduce in GitHub Actions, dispatch `rolling-upgrade.yml` on the candidate branch. While the workflow exists only on the prework branch, its prep-only `ci.yml` calls it:

```
gh workflow run ci.yml --repo djosh34/tigerstore --ref prep/148-rolling-upgrade
```

No build or execution belongs on the shared agent host. The task worker should drop the prep-only CI change and add the rolling job to the required CI gate. When a previous release exists, the release workflow still needs the previous-tag-to-candidate run required by #127. These synthetic builds do not replace that check.

The prework starts from the harness on main. On `work/148-chaos` at `821ac5e1`, `newCluster` also takes data directories and extra arguments. When combining the branches, change the call in `startRolling` to `newCluster(dir, oldBin, seed, [3]string{}, nil)`. The upgrade run does not need the privileged environment or LazyFS.
