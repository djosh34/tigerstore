# FINAL — PR84 source audit ready for parent archive/post

Run **38400142994**, resolved **2026-10-13T12:49:30Z**:
- main `aa9d9b6eea79c73f12fe0153aefa809841967e88`
- PR84 `d5681651cd554b8804ed3a77b9ff019ade6815bf` (provenance retained `refs/pull/84/head`, immutable object binding)
- harness/generator `6148beab08e91a02c243ccd2d2b788ecdacd45fd`; `newlean.go` byte-identical to e3bc29f.

Only Put64 and Put256, three stores, five reps. Source conclusions: leader input stages **4→4**; follower **2→2**; sender **+1 per actual occurrence**. Read-clock does not add an owning request-value buffer. It adds **one Taken channel per connection**, conditional/coalesced successful K_s sends and selected K_r receipts; not fixed per request and not a reply channel. Normal private request channels/closes remain zero. No assumed batch sizes or source-derived CPU/syscall outcomes.

Etcd release/pins reused by reference: **8+C** immediate Put stages including PageWriter, deferred bbolt extra/separate, C conditional; fixed core **3s/3r/2 closes/2 fresh reply channels**, no inapplicable grpc deadline-sync close for raw measured requests. Untimed control deadlines/warm writes outside cohort.

Deliverables: `audit.md`, `verified-sites.md/json`, `workload-sites.md/json`, immutable bindings, selected source files, small manifests, `verification.json`, `artifact-check.json`, `SHA256SUMS`.

Writes ONLY under this new scratch. Sealed PR80 scratch and repository untouched. No subagents, GitHub writes, Go executions, builds, tests or benchmarks. Parent alone reports actual validated ARM leader CPU/Put, leader proc IO syscr/syscw deltas/Put and generator CPU, then archives/posts PR84. Prior PR80 numbers and assumed 2.8µs are not substituted for this run.
