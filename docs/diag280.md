# #280 diagnosis (reference only)

Base: PR #275 head `9d230880`. Do not merge/cherry-pick this branch.

## Evidence

Run 39185668816, artifact sim-campaign-11, sim-out/TestSeeds_8255809:

- trace.txt:49,89: after torn first appends, node 2 recovers only entries
  1–2 and node 3 only entry 1, all term 1; both commit/applied 0.
- trace.txt:251: node 1's last recovery before the copy: entries 1–5,
  last term 2, hard state term 2 / vote 1 / commit 3, applied 3.
- trace.txt:269: disk copy at 18819.523ms. Node 1 does not become a
  leader or advance term again until 20066ms. Its last-crash image has
  bootstrap 1–3, term-2 no-op 4, and an uncommitted data entry 5.
- trace.txt:370,374: the failed cold restart directly reports node 2
  last 2/term 1 and node 3 last 1/term 1, both hard state term 2 / vote 1 /
  commit 0, applied 0, voters []. No elections/messages follow.

The three small WAL files in internal/sim/testdata/diag280 are the actual
last-crash images from that artifact, not invented entries. WAL recovery
checks their CRCs and truncates the two damaged suffixes. The reproduction
adds the later durable term-2 vote to nodes 2/3 (as proven by their cold
restart lines). The full-copy images themselves are absent: runInspect
copies `s.cold` to `res.cold` **before** `s.check`, but only `s.check` can
set it. Thus saveRun's cold-image branch is never reached. The per-node
4000-call rings also no longer cover the initial appends. This is an
additional evidence/tooling gap, not the source of the election failure.

## Cause

core.bootstrapped correctly declines an incomplete bootstrap. core.New
also skips RawNode.Bootstrap for a nonempty recovered log. Replaying commit
0 supplies no ConfState to Raft. Both survivors are absent from their own
progress trackers and cannot campaign (raft.promotable); an empty-config
node may vote but nobody asks. Node 1 was the only survivor with all three
bootstrap entries and was erased. #247's full-bootstrap replay is not
enough; this is its torn-prefix startup-liveness case, not #261's unknown
peer admission failure. Real node startup is used by the simulator, and
the short test drives that same product code, with open links/no faults.
The liveness defect is in the product, not the simulator's verdict.

## Short reproduction

```
go test -v -count=50 -timeout=3m ./internal/sim -run '^TestDiag280ColdRestart$'
```

It boots only nodes 2 and 3 for 25 simulated seconds. Without the repair,
both stay followers, leader 0, applied 0, members []. A control adds only
the missing bootstrap entry 3 on node 2: it elects and brings node 3 up.

## Semi-proven repair

At startup, before publishing the WAL or creating a core, compare the
entire surviving prefix to the exact canonical term-1 bootstrap derived
from the saved initial identity. Only an exact match, root 0, commit 0,
nonempty log shorter than the initial peer list, and an initial member
qualify. Append and sync the missing suffix, preserving term/vote/commit;
then #275's existing full-bootstrap recovery supplies all three members.
Never expose a partial member list. The repair is idempotent and a failed
sync prevents starting a core. Tests cover prefix lengths 1/2, preserving
hard state through reopen, retry after interrupted sync, and refusal to
repair different terms, contexts, remove entries or a non-initial member.

The helper and driver hookup are diagnostic reference only. A production
fix needs its own implementation/review of the durable-identity contract
(including exact original contexts/order) and exhaustive recovery-crash
coverage; these runs prove this failure mechanism, not general safety.
All execution is on Actions. The workflow also disables only the repair
on the fixed head and requires all 50 failures to return, while all 50
complete-prefix controls continue to pass. No campaign is used.
