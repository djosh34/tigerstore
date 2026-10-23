# Cold restart images of membership seed 84216

The disks that the cold restart of `TestMembershipFaults` seed 84216 started
from, copied at 49,602 ms, unmodified: `cold/copy-at-49602ms` of the
`sim-campaign-8` artifact of [run 39842177115](https://github.com/djosh34/tigerstore/actions/runs/39842177115),
on `ee59e495` of PR #384 (#389). `node3-erased` holds node 3's disk as it
was; the campaign erased it, and so does `TestColdRestartNoElection`.

With node 3 erased, no node can win an election. Node 1 has applied node
4's promotion, so it needs 3 of 4 voters, and node 4's log is newer than
its own. Node 2 is behind node 1. Node 4 has the newest log but is still a
learner in its own configuration, so it does not campaign.
