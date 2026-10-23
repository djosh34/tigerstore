# Rolling upgrade tests

`rolling-upgrade.yml` runs the real-process tests in `cmd/chaos/rolling_test.go`. It builds two copies of the checked-out server with Go overlays. The old copy supports entry format 1 and peer range 2-2. The new copy supports entry formats 1-2 and peer range 2-3. Only those constants differ. These are synthetic version ranges, not previous release binaries.

The tests reuse the chaos cluster, TLS links, workload, history recorder and six checker verdicts. Each upgrade stage must contain successful writes. The evidence includes the histories, WALs, server logs, captured announcements and old-binary refusal. Checker timeouts fail the run.

`TestRollingUpgrade` runs seeds 148 and 174. It upgrades nodes 1 and 2, rolls node 2 back, restarts it, and upgrades node 3. It then sends node 2's earlier announcement through a real peer connection. The announcement keeps its original origin, boot and run fields. The test requires those bytes to commit after the rollback announcement, requires version 1 to remain active, and checks that the committed entries still use format 1. It upgrades node 2 last, checks for format-2 entries, tries the old binary against that data directory, and requires a clear refusal. The new binary must then recover under load.

`TestRollingRollbackBeforeAnnouncement` covers the other order named in #174. It stops the node selected for rollback and upgrades the last old node before the rollback can announce. This is a regression test, not an expected-success downgrade. If the remaining nodes activate version 2, it fails.

The workflow also removes the confirmation check in a separate build. That job passes only if `TestRollingRollbackBeforeAnnouncement` detects activation while the rollback node is down and the old binary refuses to restart. The component tests separately check rejection of superseded runs.

The normal scenario also runs with the server and test executable built with `-race`. Each stage checks the binary ranges in `/metrics` and probes fresh TLS peer connections in both directions. ALPN and the preface reply must choose the highest shared version. The existing core and peer version tests run alongside it, including overlapping, touching and disjoint peer ranges and refusal from a checkpoint.

The required `ci` gate calls `rolling-upgrade.yml` on pull requests, pushes to main, nightly runs and manual dispatches. The rolling workflow also accepts a separate manual dispatch. To run the full CI set on a candidate branch:

```
gh workflow run ci.yml --repo djosh34/tigerstore --ref <candidate-branch>
```

No build or execution belongs on the shared agent host. When a previous release exists, the release workflow still needs the previous-tag-to-candidate run required by #127. These synthetic builds do not replace that check.

The upgrade run uses the chaos cluster without extra data directories or arguments. It does not need the privileged environment.
