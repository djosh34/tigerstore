# Whole-batch write path

Question: how much leader CPU does a batch admission and completion path remove after replacing the HTTP handler frontend, and how much more does a wire-backed command codec remove?

This is throwaway code for issue #74, experiment 2. Nothing on this branch is a proposed merge. The starting main commit is `eb8a20eda7b1436e3319b924753469f2d8c0ee99`.

Sources:

- Custom x/net HTTP/2 Framer and HPACK experiment, `exp/pre-47-grpc` at `d2408301966b99471bfed06d4c2dc1cd1e5af462`.
- Profile harness, `exp/pre-50-profile` at `1d90dcebf4ff9c910f7053dc0634d251767fd92b`.
- Group commit comparison, `exp/bench-68` at `32ceb80fb36fd4229c2e2e7594b40da197fe4eaf`.

## Controls

The default server remains unchanged. The prototype uses `TIGERSTORE_FRONTEND=wholebatch` and three planned modes:

| Mode | Admission and completion | Command codec |
| --- | --- | --- |
| `rpc` | One request at a time, legacy request handlers | Generated protobuf |
| `admit` | One admission per batch, completions grouped per connection | Generated protobuf |
| `full` | One admission per batch, completions grouped per connection | Wire-backed descriptors |

The `rpc` ablation intentionally retains per-RPC goroutines so a blocked write cannot stop the connection reader from admitting other streams. The full path has one reader and one writer per connection. Admission and completion change together in this experiment. Their separate CPU contributions cannot be inferred from that contrast.

The existing Raft owner still decides order, revisions and transaction outcomes. Replies still originate in committed application. This prototype does not execute writes speculatively or release successful replies before durable application. The asynchronous Raft persistence response dependencies remain unchanged.

## Measurement requirements

All performance measurements run on free `ubuntu-24.04-arm` runners. Each client-count job runs main, the prototype variants and etcd v3.7.2 back to back for five repetitions. The client counts are 16, 64 and 256. The generator uses 16 connections, 1 KiB values and 4,096 keys. Every process has a pinned physical CPU. The leader uses CPU 0, followers use 1 and 2, and the generator uses 3. Servers receive two Go processors. Filesystem barriers stay enabled. TLS and client certificates remain enabled.

Record process CPU per successful Put on every node, allocation count and bytes per Put, file fsyncs per Put, throughput and latency percentiles. Label instrumented samples and missing counters. Compare paired repetitions within each job. Do not treat a CPU profile percentage as a measured end-to-end reduction.

No performance result is recorded yet.
