# FINAL — ready for parent archive / PR80 post

Actions **38397810888**, parent-confirmed resolve **2026-10-13T12:30:50Z**:
- main **aa9d9b6eea79c73f12fe0153aefa809841967e88**, NOT preliminary 549e0c9.
- PR80 **67ff792a72f78e83cb6905563dcb6bc0e9bb2764**.
- Generator/harness **e3bc29f5de3b9184b8aae5f64ed290681d6e1d6f**, engine `experiments/bench75/load/newlean.go`.
- Stock etcd v3.7.2 unchanged release **68c065e562994b89e333e77b039ad066f933c586**; previous verified source audit reused by reference.

Final main vs preliminary main changes only two peer test files; audited production sources and dependencies are unchanged. All source tables/sites were rebound and reverified to **aa9d9b6**, not the preliminary measurement ref. Final generator committed source was re-read and its actual conditional flow-control DATA fragmentation, coalesced 64KiB receive credits, and cached <=5-second-remaining write-deadline renewal documented.

Conclusions:
- Tiger known leader new-value stages: **Put 4→4; CAS 6→5**, either comparison outcome.
- CAS failure separately adds **one old stored-value reply encode** in both targets.
- Newly persisted/applied follower: Put **2→2**, CAS **3→2**; sender encoding **+1 per actual transmitted occurrence**, no fanout/batch assumption.
- Fixed private request sends/receives/closes **0/0/0**, fresh private reply channels **0**; shared admission/disk/peer/wake/tick populations remain symbolic.
- Etcd old explicit grpc deadline-sync close is absent for timed raw RPCs (no grpc-timeout); core 3s/3r/2 closes/2 fresh replies unchanged. **Codec C conditional**. Etcd Put 8 immediate includes PageWriter; deferred bbolt pages excluded/separate.

Deliverables: `audit.md`, `verified-sites.md/json`, `workload-sites.md/json`, `lean-etcd-scope.md`, immutable bindings, selected source/dependency/workload files, manifests, `verification.json`, `artifact-check.json`, `SHA256SUMS`. No dependency ZIPs, whole exports or bare object repo. No mutable-source warnings in final report.

No repo modifications, subagents, Go execution, builds, benchmarks/tests or GitHub writes performed. Benchmark work is ARM-only and parent/workflow-owned. No profiles requested. Actual generator CPU/runtime evidence and failure counts belong to the parent's benchmark report; this audit makes no performance claim and does not reuse prior zero failures.

Parent alone archives final and posts to PR80. All audit writes are confined to `/home/joshazimullah.linux/work_mounts/scratch/bench80-lean-source-audit`.
