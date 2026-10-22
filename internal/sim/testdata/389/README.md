# Issue 389 diagnosis images

These are the unmodified cold-start images from `sim-campaign-8` of
run 39842177115, `TestMembershipFaults_84216/cold/copy-at-49602ms`.
`node3-erased` holds the original disk, for the control that restores it.
The test erases it in memory, as the campaign did.

Question: can the other disks elect a leader without node 3?

Run on this diagnosis branch:

```sh
GOMAXPROCS=2 GOFLAGS=-p=1 scripts/sim.sh ./internal/sim '^TestDiag389' -test.v
```

Without the diagnosis change to `internal/sim/faults.go`, erasing node 3
fails after 25.52 simulated seconds. Keeping all disks, or erasing node 4
instead, converges after about 6.2 simulated seconds. The same controls
have the same results with the production Go files from `a5d2026d`.

Node 4's last entry is 1351 at term 8, but it still considers itself a
learner. Its uncommitted entry 1342 at term 7 promotes itself. Node 1 has
applied that promotion, but its last entry is only 1343 at term 7. Node 2
ends at 1307 at term 7. No surviving candidate can obtain an up-to-date
voter quorum. Node 4 can vote, but cannot campaign, and cannot apply its
promotion without a leader committing it.

The prototype excuses only the unavailable cold restart's liveness
requirement, not a store failure. It does not change production code or
make this cluster elect. It is diagnosis code, not a patch to merge.
The fixtures compare the survivors with each other, not with a complete
external history. The original seed rerun supplies that history check.
