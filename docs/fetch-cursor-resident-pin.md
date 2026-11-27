# Resident option-A trace isolation

Part of #833. Manager ruling: https://github.com/djosh34/tigerstore/pull/897#issuecomment-7230173612 . All controls are disposable diagnostics over `b8707d801b14943ea4cec40badd5b0d28fea09bd` plus the option-A work. Production cursor routing remains enabled. No checker, witness limit, deadline, disk implementation or planted expectation is changed.

## Two independent reversions

| Variant | Fork trace | Fork history | Image SHA-256 |
| --- | --- | --- | --- |
| Current-pin baseline | `cee1eb73ec6ac127` | `df529d402093fa52` | `ec4427fb64ce2ef3c84e9a99057ac9290d83205e5c8cd1a3ada90b1a40456f20` |
| Full resident option A | `ec26ff5f37150752` | `ab3e074948e3f9cb` | `5d85b8ad8dc250071447492a86ba0ac077950516c4cb240fa3d2ad48a687645f` |
| Only resident routing reverted | `cee1eb73ec6ac127` | `df529d402093fa52` | `ec4427fb64ce2ef3c84e9a99057ac9290d83205e5c8cd1a3ada90b1a40456f20` |
| Only P/window-bounded pieces reverted | `ec26ff5f37150752` | `ab3e074948e3f9cb` | `5d85b8ad8dc250071447492a86ba0ac077950516c4cb240fa3d2ad48a687645f` |

All variants pass aging and fork semantic checks. The diagnostic variants retaining the new route intentionally fail only the unchanged old pin. The new piece funding is not the cause of the pin change in this fixture.

Routing reversion restores the pre-option-A dispatch decisions for multi-key/loan resident ranges, live/catch-up watches and large resident value pieces; it preserves the point shortcut that predates option A. New P-funded openings, window bounds and ownership remain. Piece reversion restores the previous large-range/watch buffering/owner and raw-piece allocation implementations, retaining resident-versus-cold dispatch and the new small resident route. Its unreserved buffers exist only in the never-merge diagnostic tree.

Full node hashes, baseline/routing-reverted:

```text
node1 22c234a5caf18514
node2 4b3cf8996425e8bd
node3 2b73d15de6cc5d4a
```

Full node hashes, full-option-A/pieces-reverted:

```text
node1981124615 610b3040ed394e24
node2          3d88e415633d2610
node3          0d5832bba5f1631e
```

The later replacement node ID is part of the genuinely different execution, not a renamed hash. Aging trace/history are `d004875be2eefca0 / a0e0bb8197fe2e8b` for the baseline and `9dfa7f747c69f34f / 1ee3edbe996593c7` for option A. Their image sizes are 41,387,769 and 39,260,603 bytes respectively.

## First divergence

The first internal output divergence is **1847.136018 ms into aging, node 2, revision 2**. Before that point, the output observations match. Both paths see the same live watches:

| Watcher | Stream | Response body bytes |
| --- | --- | --- |
| 10 | 49 | 2712 |
| 11 | 29 | 3211 |
| 9 | 25 | 1443 |

The current-pin route defers these resident deliveries to `startWatchFill` and cursor queue commits. Its first client-socket flush has **153 buffered bytes**, the already-present Txn reply. The full option-A route sends them through main's direct live writer with no reply stage/source plan; the first flush has **7561 buffered bytes**, including those watch responses. The old cursor ACKs have 1/4/2 descriptors and **zero source leases**. The first old-route cold-worker dispatch is later, at **2495.890317 ms**; the new route has none before 3 seconds. Thus the initial difference is hot-watch queue/flush packetization, not a cold disk read or a P-piece allocation.

**All 10,789 disk-call records through that output divergence are identical**, including write paths, offsets, byte counts and CRCs. SHA-256 of that common textual prefix is `865cb45b81ee705ea1a1a362ea427128c125d21aa34b210ef42bc0d32dad1a2f`. The first later disk-log difference is only timing: node 2's same 4096-byte write at offset 8192, CRC `125eca2f`, at 1850.605966 ms (baseline) versus 1850.596869 ms (option A). It follows the output divergence.

The first client completion ordering difference confirms the mechanism. Client 12's Txn on node 2, called at 1.812 s, returns at **1.847 s on the current-pin route versus 1.852 s on option A**. Its entire request/reply text is identical: 11,973 bytes, SHA-256 `cbf43865cd337e1ac7c54baca1b458409ff458cfdee4e3fa1bf7dd2effaeca27`. A different client's LeaseGrant now completes before it. Follow-up calls and later fault/RNG scheduling then differ; subsequent histories and disk images are genuinely different.

The first ordinary aging trace difference is line 81, after the common `1753.185 ms watch 4: stream on node 2 from 1`:

```text
current pin: 2101.216 ms watch 3: stream on node 1 from 2
option A:    2365.567 ms watch 24: stream on node 2 from 1
```

These observations are not added to the simulation's trace, scheduler or disk state. Diagnostic-only observers use `simrun.Aside` and retain the existing trace prefix before its ring drops it. Forwarding the already-present SIM_KEEP variable changes no environment key set/count. Observed and unobserved runs reproduce their exact image SHA, fork/history and node hashes, proving the observers do not move the executions.

## Semantic checks and reproduction

The fixture invokes durability, revision and watch checks, plus revision witnesses from the log and from revisions. It does not claim a separate Porcupine run. All of those checks pass for aging and fork in all controls. Weighted witness steps (unchanged 360,000,000 budget):

- Current pin / routing reverted: **10,480 aging; 15,348 fork**.
- Option A / pieces reverted: **7,250 aging; 12,224 fork**.

Each short single-image/fork test ran sequentially, with Go 1.27.2 linux/arm64, in the 6-GiB/no-swap scope with GOMEMLIMIT=1536MiB. GOFLAGS=-p=2 limited the cold build's parallelism; it changes no simulated environment variable or workload.

```sh
systemd-run --user --scope -q -p MemoryMax=6G -p MemorySwapMax=0 \
 env GOMEMLIMIT=1536MiB GOFLAGS=-p=2 \
 scripts/sim.sh ./internal/sim '^TestLeaseFreeForkTrace$' -test.v -test.timeout 2m
```

The approved re-pin records the routing cause and the first event in the test comment. Both reversions and every observer remain out of production. The prior pin failure and diagnostic negative pin comparisons remain evidence; no semantic assertion is weakened and no expected failure is added. Every final-head gate remains required, including the long seed's verdict, witness steps and complete whole-command-tree peak.
