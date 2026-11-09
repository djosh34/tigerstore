# #577: least leader CPU per mixed-k8s op

Research only. Reusable for a new workload:

1. `cmd/research577` measures independent primitives on the runner (dispatch
   `bench.yml` on this branch; artifacts `research-577-*`). Its `fixtures()`
   builds the workload's requests, replies, durable bytes and watch events from
   `cmd/loadgen/mixed.go` and `internal/k8ssize`; change it for a new workload.
2. `cmd/research577index` on branch `research/577-index-raft` measures the key
   index, DRAM latency and etcd Raft per batch. Its numbers are constants in
   `model.py` (`LOOKUP`, `BT_INS`, ...).
3. `python3 -I model.py <primitives.jsonl>... --turn N --leader-node K [--role follower]`
   prints bytes per op and the floor per part.
4. `go tool pprof -raw <leader.pprof> > raw; python3 -I classify.py raw <us/op> x`
   splits a leader profile into the same parts, for the "today" column.
